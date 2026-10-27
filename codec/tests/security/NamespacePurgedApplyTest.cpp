// Namespaces Phase 2 Task 6a: the __security NamespacePurged event (kind 11, formatVer 2) - erases a namespace's users
// and roles and every grant naming it.
#include <rune/security/SecurityQueries.h>
#include "SecurityScript.h"
#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>

using rs::Operation;
using rs::PatternKind;
using rs::ResourceType;

namespace {
constexpr uint32_t kA = 1, kB = 2, kAb = 3;

rs::SecurityEvent purgeEvent(uint32_t nsId, std::string name) {
    rs::SecurityEvent e;
    e.formatVer = rs::kFormatV2; e.kind = rs::EventKind::NamespacePurged; e.ops = rs::NamespacePurgedOps{};
    e.nsId = nsId; e.objectName = std::move(name);
    return e;
}
std::expected<rs::Applied, rs::ApplyError> purge(Script& sc, uint32_t nsId, std::string name) {
    auto e = purgeEvent(nsId, std::move(name));
    auto f = rs::previewFingerprint(sc.s, e);
    if (!f) return std::unexpected(f.error());
    e.fingerprint = *f;
    return rs::apply(sc.s, e, sc.offset++);
}
bool has(const std::set<rs::GrantSpec>& gs, const rs::GrantSpec& x) { return gs.count(x) == 1; }

const rs::GrantSpec kAdminA = g(Operation::Admin, ResourceType::Namespace, PatternKind::Exact, "a");
const rs::GrantSpec kReadAPrefix = g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "a/");
const rs::GrantSpec kReadBt = g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "b/t");
const rs::GrantSpec kReadT = g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "t");
const rs::GrantSpec kReadAbx = g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "ab/x");
const rs::GrantSpec kReadAt = g(Operation::Read, ResourceType::View, PatternKind::Exact, "a/v");
const rs::GrantSpec kReadAExact = g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "a");

/// ns 1 "a": users u1, u2 and role r1 (grants, u1 a member); ns 2 "b": user v1; ns 3 "ab": user w1; default: root
/// (cross-namespace grants), role dr (a grant naming a/..) with member root2.
void build(Script& sc) {
    ASSERT_TRUE(sc.runIn(kA, rs::CreateUserOps{v(1)}, "u1").has_value());
    ASSERT_TRUE(sc.runIn(kA, rs::CreateUserOps{v(2)}, "u2").has_value());
    ASSERT_TRUE(sc.runIn(kA, rs::CreateRoleOps{}, "r1").has_value());
    ASSERT_TRUE(sc.runIn(kA, rs::GrantOps{rs::GranteeKind::Role, kAdminA}, "r1").has_value());
    ASSERT_TRUE(sc.runIn(kA, rs::GrantRoleOps{sc.s.roleByName(kA, "r1")->objectId, "r1"}, "u1").has_value());
    ASSERT_TRUE(sc.runIn(kA, rs::GrantOps{rs::GranteeKind::User, kAdminA}, "u2").has_value());
    ASSERT_TRUE(sc.runIn(kB, rs::CreateUserOps{v(3)}, "v1").has_value());
    ASSERT_TRUE(sc.runIn(kAb, rs::CreateUserOps{v(4)}, "w1").has_value());
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(5)}, "root").has_value());
    ASSERT_TRUE(sc.run(rs::CreateUserOps{v(6)}, "root2").has_value());
    ASSERT_TRUE(sc.run(rs::CreateRoleOps{}, "dr").has_value());
    ASSERT_TRUE(sc.run(rs::GrantRoleOps{sc.s.roleByName(0, "dr")->objectId, "dr"}, "root2").has_value());
    // Namespace "a" grants exist only for namespace a's own users and roles (validGrant); a default user holds
    // QUALIFIED grants into it instead.
    for (const auto& gs : {kReadAPrefix, kReadBt, kReadT, kReadAbx, kReadAt, kReadAExact})
        ASSERT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::User, gs}, "root").has_value());
    ASSERT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, kReadAPrefix}, "dr").has_value());
    ASSERT_TRUE(sc.run(rs::GrantOps{rs::GranteeKind::Role, kReadT}, "dr").has_value());
    ASSERT_TRUE(sc.runIn(kB, rs::GrantOps{rs::GranteeKind::User, g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "a/x")}, "v1").has_value());
}
} // namespace

TEST(NamespacePurgedApplyRegression, ErasesUsersRolesGrantsRegression) {
    Script sc;
    build(sc);
    ASSERT_TRUE(rs::hasNamespaceRecords(sc.s, kA, "a"));
    const uint64_t u1 = sc.s.userByName(kA, "u1")->objectId, root = sc.s.userByName(0, "root")->objectId,
                   root2 = sc.s.userByName(0, "root2")->objectId, v1 = sc.s.userByName(kB, "v1")->objectId;
    auto r = purge(sc, kA, "a");
    ASSERT_TRUE(r.has_value()) << r.error().detail;
    EXPECT_EQ(r->kind, rs::EventKind::NamespacePurged);
    for (const auto& [id, u] : sc.s.users()) EXPECT_NE(u.nsId, kA);
    for (const auto& [id, ro] : sc.s.roles()) EXPECT_NE(ro.nsId, kA);
    EXPECT_EQ(sc.s.userByName(kA, "u1"), nullptr);
    EXPECT_EQ(sc.s.userByName(kA, "u2"), nullptr);
    EXPECT_EQ(sc.s.roleByName(kA, "r1"), nullptr);
    const auto* rootU = sc.s.userByName(0, "root");
    ASSERT_NE(rootU, nullptr);
    EXPECT_EQ(rootU->grants, (std::set<rs::GrantSpec>{kReadBt, kReadT, kReadAbx, kReadAExact}));
    EXPECT_GT(rootU->version, 7u) << "a changed user's version advances";
    EXPECT_EQ(sc.s.roleByName(0, "dr")->grants, (std::set<rs::GrantSpec>{kReadT}));
    EXPECT_TRUE(sc.s.userByName(kB, "v1")->grants.empty()) << "v1's grant on 'a/x...' names namespace a";
    ASSERT_NE(sc.s.userById(v1), nullptr);
    ASSERT_NE(sc.s.userById(root2), nullptr);
    EXPECT_EQ(sc.s.userById(u1), nullptr);
    EXPECT_EQ(sc.s.userById(root)->nsId, 0u);
}

TEST(NamespacePurgedApplyRegression, EffectsRegression) {
    Script sc;
    build(sc);
    const uint64_t u1 = sc.s.userByName(kA, "u1")->objectId, u2 = sc.s.userByName(kA, "u2")->objectId,
                   root = sc.s.userByName(0, "root")->objectId, root2 = sc.s.userByName(0, "root2")->objectId,
                   v1 = sc.s.userByName(kB, "v1")->objectId;
    auto r = purge(sc, kA, "a");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->effect, rs::SessionEffect::Revoke);
    EXPECT_EQ(r->affectedUsers, (std::vector<uint64_t>{u1, u2}));
    std::vector<uint64_t> want{root, root2, v1};
    std::sort(want.begin(), want.end());
    EXPECT_EQ(r->refreshedUsers, want);
    // a purge that only strips grants refreshes them
    Script sc2;
    ASSERT_TRUE(sc2.run(rs::CreateUserOps{v(1)}, "x").has_value());
    ASSERT_TRUE(sc2.run(rs::GrantOps{rs::GranteeKind::User, kReadAPrefix}, "x").has_value());
    auto r2 = purge(sc2, kA, "a");
    ASSERT_TRUE(r2.has_value());
    EXPECT_EQ(r2->effect, rs::SessionEffect::Refresh);
    EXPECT_EQ(r2->affectedUsers, (std::vector<uint64_t>{sc2.s.userByName(0, "x")->objectId}));
    EXPECT_TRUE(r2->refreshedUsers.empty());
}

TEST(NamespacePurgedApplyRegression, IdempotentRegression) {
    Script sc;
    build(sc);
    ASSERT_TRUE(purge(sc, kA, "a").has_value());
    EXPECT_FALSE(rs::hasNamespaceRecords(sc.s, kA, "a"));
    const uint64_t d1 = sc.s.digest();
    const int64_t off1 = sc.s.appliedOffset();
    auto again = purge(sc, kA, "a");
    ASSERT_TRUE(again.has_value()) << again.error().detail;
    EXPECT_EQ(again->effect, rs::SessionEffect::None);
    EXPECT_TRUE(again->affectedUsers.empty());
    EXPECT_GT(sc.s.appliedOffset(), off1);
    // only appliedOffset moved: a fresh state at the same offset has the same digest of objects
    EXPECT_NE(sc.s.digest(), d1) << "the digest covers appliedOffset";
    // and a purge of a namespace that never existed is Ok too
    EXPECT_TRUE(purge(sc, 99, "zz").has_value());
    EXPECT_FALSE(rs::hasNamespaceRecords(sc.s, 99, "zz"));
}

TEST(NamespacePurgedApplyRegression, OtherNamespaceUntouchedRegression) {
    Script sc;
    build(sc);
    const uint64_t w1Digest = rs::fingerprintOf(*sc.s.userByName(kAb, "w1"), sc.s);
    const uint64_t v1Id = sc.s.userByName(kB, "v1")->objectId;
    ASSERT_TRUE(purge(sc, kA, "a").has_value());
    const auto* root = sc.s.userByName(0, "root");
    EXPECT_TRUE(has(root->grants, kReadAbx)) << "'ab/x' does not start with 'a/': the '/' boundary";
    EXPECT_TRUE(has(root->grants, kReadBt));
    ASSERT_NE(sc.s.userByName(kAb, "w1"), nullptr);
    EXPECT_EQ(rs::fingerprintOf(*sc.s.userByName(kAb, "w1"), sc.s), w1Digest);
    ASSERT_NE(sc.s.userById(v1Id), nullptr);
    EXPECT_EQ(sc.s.userById(v1Id)->nsId, kB);
    EXPECT_FALSE(rs::hasNamespaceRecords(sc.s, 99, "zz")) << "no records of an unrelated name";
    EXPECT_TRUE(rs::hasNamespaceRecords(sc.s, kB, "b"));
    EXPECT_TRUE(rs::hasNamespaceRecords(sc.s, kAb, "ab"));
}

TEST(NamespacePurgedApplyRegression, ExactNameWithoutSlashSurvivesRegression) {
    // A default-namespace topic literally called "a" is not in namespace "a" (only "a/..." is).
    Script sc;
    build(sc);
    ASSERT_TRUE(purge(sc, kA, "a").has_value());
    EXPECT_TRUE(has(sc.s.userByName(0, "root")->grants, kReadAExact));
}

TEST(NamespacePurgedApplyRegression, DefaultRefusedRegression) {
    Script sc;
    build(sc);
    auto r = purge(sc, 0, "default");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().code, rs::ApplyError::Code::Precondition);
    EXPECT_EQ(r.error().refusal, rs::Refusal::NamespaceMismatch);
    auto bad = purge(sc, kA, "");
    ASSERT_FALSE(bad.has_value());
}

TEST(NamespacePurgedApplyRegression, TruncatedAtEveryOffsetRegression) {
    auto e = purgeEvent(7, "arena");
    e.fingerprint = 0x1122334455667788ull;
    const auto b = rs::encode(e).value();
    for (std::size_t n = 0; n < b.size(); ++n) {
        auto d = rs::decode(std::span<const uint8_t>(b.data(), n));
        ASSERT_FALSE(d.has_value()) << "prefix " << n;
    }
    auto d = rs::decode(b);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(*d, e);
    EXPECT_EQ(d->kind, rs::EventKind::NamespacePurged);
    EXPECT_EQ(static_cast<uint16_t>(d->kind), 11u);
    EXPECT_EQ(rs::kindName(d->kind), "NamespacePurged");
}

TEST(NamespacePurgedApplyRegression, FormatV1RefusedRegression) {
    auto e = purgeEvent(0, "x");
    e.formatVer = rs::kFormatV1;
    auto enc = rs::encode(e);
    ASSERT_FALSE(enc.has_value());
    EXPECT_EQ(enc.error(), rs::EncodeError::UnknownKind);
    // a v2 encoding with the version patched to 1 decodes as an unknown kind
    auto b = rs::encode(purgeEvent(0, "x")).value();
    b[0] = 1;
    auto d = rs::decode(b);
    ASSERT_FALSE(d.has_value());
    EXPECT_EQ(d.error(), rs::DecodeError::UnknownKind);
    Script sc;
    auto a = rs::apply(sc.s, e, 0);
    ASSERT_FALSE(a.has_value());
}

TEST(NamespacePurgedApplyRegression, NonZeroObjectOrVersionRefusedRegression) {
    Script sc;
    build(sc);
    auto e = purgeEvent(kA, "a");
    e.objectId = 5;
    auto r = rs::apply(sc.s, e, sc.offset++);
    ASSERT_FALSE(r.has_value());
    EXPECT_TRUE(rs::hasNamespaceRecords(sc.s, kA, "a"));
}
