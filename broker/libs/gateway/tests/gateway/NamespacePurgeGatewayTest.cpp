// NamespacePurgeGatewayTest.cpp - namespaces Phase 2 Task 6a at the gateway (no Aeron): what ClientGateway::onSecurityApplied
// does with a NamespacePurged (kind 11) event.
//   * a session of an ERASED user ends (revoked, reason "namespace deleted" - not "password changed");
//   * a session of a SURVIVING user whose cross-namespace grant (Topic Prefix "a/") was stripped stays open and
//     recompiles its permissions (`refreshedUsers`), even though the event's main effect is Revoke.
//
// Interaction matrix (user x effect):
//   | user                               | purge of `a`                 | expected                           | test
//   |------------------------------------|------------------------------|------------------------------------|-----
//   | a/u (erased)                       | Revoke                       | session gone, "namespace deleted"  | PurgedUserSessionRevoked
//   | default d (grant a/ + grant t)     | Revoke (a/u erased) + refresh| open, a/ grant gone, t grant kept  | CrossNamespaceGrantRefreshed
//   | default d (grant a/), a has no user| Refresh                      | open, a/ grant gone                | RefreshOnlyPurgeRefreshed
//   | b/v (untouched)                    | Revoke of a                  | session open, grants unchanged     | OtherNamespaceUserUntouched
#include <rune/gateway/ClientGateway.h>
#include <rune/gateway/SecurityView.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/security/Permissions.h>
#include <rune/security/SecurityEvent.h>
#include <rune/security/SecurityFingerprint.h>
#include <rune/security/SecurityState.h>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <string>

using namespace rune::gateway;
namespace pc = rune::protocol::client;
namespace rs = rune::security;

namespace {

constexpr uint32_t kA = 1, kB = 2;

rs::ScramVerifier verifier(uint8_t seed) {
    rs::ScramVerifier x; x.salt.assign(16, seed); x.iterations = 4096;
    x.storedKey.fill(seed); x.serverKey.fill(static_cast<uint8_t>(seed + 1)); return x;
}
rs::GrantSpec grant(rs::Operation op, rs::ResourceType t, rs::PatternKind k, std::string n) { return {op, {t, k, std::move(n)}}; }

/// Builds valid events against `s` the way the __security leader does.
struct Script {
    rs::SecurityState s;
    int64_t offset = 0;
    rs::SecurityEvent make(uint32_t nsId, rs::Ops ops, std::string name) {
        rs::SecurityEvent e; e.kind = rs::kindOf(ops); e.objectName = std::move(name); e.ops = std::move(ops); e.nsId = nsId;
        const rs::UserObject* u = s.userByName(nsId, e.objectName);
        const bool create = e.kind == rs::EventKind::CreateUser;
        e.objectId = create ? s.nextObjectId() : (u ? u->objectId : 0);
        e.prevVersion = create ? 0 : (u ? u->version : 0);
        e.newVersion = e.prevVersion + 1;
        if (auto f = rs::previewFingerprint(s, e)) e.fingerprint = *f;
        return e;
    }
    void run(uint32_t nsId, rs::Ops ops, std::string name) {
        auto r = rs::apply(s, make(nsId, std::move(ops), std::move(name)), offset++);
        if (!r) throw std::logic_error("Script: " + r.error().detail);
    }
};

struct Rig {
    SecurityView view;
    ClientGateway gw;
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> logSink;
    std::shared_ptr<spdlog::logger> previous;
    explicit Rig(rs::SecurityState booted)
        : view(std::move(booted)),
          gw(ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1)) {
        gw.setNodeIdForTest(1);
        gw.setAuthMode(GatewayAuthMode::Required, &view);
        logSink = std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(256);
        previous = spdlog::default_logger();
        auto capture = std::make_shared<spdlog::logger>("ns-purge-gw-capture", logSink);
        capture->set_level(spdlog::level::info);
        spdlog::set_default_logger(capture);
    }
    ~Rig() { spdlog::set_default_logger(previous); }
    [[nodiscard]] bool logContains(const std::string& text) const {
        for (const std::string& l : logSink->last_formatted()) if (l.find(text) != std::string::npos) return true;
        return false;
    }
    /// An authenticated session of `userId` in `nsId` holding exactly `grants`.
    int64_t session(uint64_t userId, uint32_t nsId, std::vector<rs::GrantSpec> grants) {
        const int64_t sid = gw.injectTestSession(0, pc::kClientVersionAuth);
        std::sort(grants.begin(), grants.end());
        gw.authenticateWithGrantsForTest(sid, userId, std::move(grants), nsId);
        return sid;
    }
    /// Applies NamespacePurged(nsId, name) to the view and hands the result to the gateway.
    rs::Applied purge(uint32_t nsId, const std::string& name) {
        rs::SecurityEvent e;
        e.formatVer = rs::kFormatV2; e.kind = rs::EventKind::NamespacePurged; e.ops = rs::NamespacePurgedOps{};
        e.nsId = nsId; e.objectName = name;
        auto f = rs::previewFingerprint(view.state(), e);
        if (!f) throw std::logic_error("purge preview: " + f.error().detail);
        e.fingerprint = *f;
        const auto bytes = rs::encode(e);
        if (!bytes) throw std::logic_error("purge encode");
        const int64_t off = view.appliedOffset() + 1;
        auto applied = view.onApplied(off, *bytes);
        if (!applied) throw std::logic_error("purge apply: " + applied.error().detail);
        gw.onSecurityApplied(off, *applied);
        return *applied;
    }
};

const rs::GrantSpec kReadAPrefix = grant(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Prefix, "a/");
const rs::GrantSpec kReadT = grant(rs::Operation::Read, rs::ResourceType::Topic, rs::PatternKind::Exact, "t");
const uint8_t kReadBit = rs::opBit(rs::Operation::Read);

struct Ids { uint64_t au, bv, d; };
/// a/u (ns a), b/v (ns b), d (default) holding `defaultGrants`.
Ids build(Script& sc, std::vector<rs::GrantSpec> defaultGrants, bool withUserInA = true) {
    Ids ids{};
    if (withUserInA) sc.run(kA, rs::CreateUserOps{verifier(1)}, "u");
    sc.run(kB, rs::CreateUserOps{verifier(2)}, "v");
    sc.run(0, rs::CreateUserOps{verifier(3)}, "d");
    for (const auto& g : defaultGrants) sc.run(0, rs::GrantOps{rs::GranteeKind::User, g}, "d");
    sc.run(kB, rs::GrantOps{rs::GranteeKind::User, kReadT}, "v");
    if (withUserInA) ids.au = sc.s.userByName(kA, "u")->objectId;
    ids.bv = sc.s.userByName(kB, "v")->objectId;
    ids.d  = sc.s.userByName(0, "d")->objectId;
    return ids;
}

}  // namespace

TEST(NamespacePurgeGatewayRegression, PurgedUserSessionRevokedRegression) {
    Script sc;
    const Ids ids = build(sc, {kReadAPrefix});
    Rig rig(sc.s);
    const int64_t sid = rig.session(ids.au, kA, {});
    ASSERT_TRUE(rig.gw.hasSessionForTest(sid));
    const rs::Applied a = rig.purge(kA, "a");
    EXPECT_EQ(a.effect, rs::SessionEffect::Revoke);
    EXPECT_FALSE(rig.gw.hasSessionForTest(sid)) << "the erased user's session is closed";
    EXPECT_TRUE(rig.logContains("namespace deleted")) << "the revoke reason of a purge";
    EXPECT_FALSE(rig.logContains("password changed")) << "NamespacePurged must not fall through to the password reason";
}

TEST(NamespacePurgeGatewayRegression, CrossNamespaceGrantRefreshedRegression) {
    Script sc;
    const Ids ids = build(sc, {kReadAPrefix, kReadT});
    Rig rig(sc.s);
    const int64_t sid = rig.session(ids.d, 0, {kReadAPrefix, kReadT});
    const PermissionTable* before = rig.gw.sessionPermissionsForTest(sid);
    ASSERT_NE(before, nullptr);
    ASSERT_TRUE(before->allowsName(rs::ResourceType::Topic, "a/t", kReadBit)) << "before the purge d may read a/t";
    const rs::Applied a = rig.purge(kA, "a");
    ASSERT_EQ(a.effect, rs::SessionEffect::Revoke) << "a/u was erased: the main effect is Revoke, d is only in refreshedUsers";
    ASSERT_EQ(a.refreshedUsers, std::vector<uint64_t>{ids.d});
    ASSERT_TRUE(rig.gw.hasSessionForTest(sid)) << "d survives";
    const PermissionTable* after = rig.gw.sessionPermissionsForTest(sid);
    ASSERT_NE(after, nullptr);
    EXPECT_FALSE(after->allowsName(rs::ResourceType::Topic, "a/t", kReadBit)) << "the same request is refused after the purge";
    EXPECT_TRUE(after->allowsName(rs::ResourceType::Topic, "t", kReadBit)) << "d's other grant is kept";
}

// No user of `a` exists: the purge only strips d's grant (effect Refresh, affectedUsers = the refresh set).
TEST(NamespacePurgeGatewayRegression, RefreshOnlyPurgeRefreshedRegression) {
    Script sc;
    const Ids ids = build(sc, {kReadAPrefix, kReadT}, /*withUserInA=*/false);
    Rig rig(sc.s);
    const int64_t sid = rig.session(ids.d, 0, {kReadAPrefix, kReadT});
    const rs::Applied a = rig.purge(kA, "a");
    ASSERT_EQ(a.effect, rs::SessionEffect::Refresh);
    ASSERT_TRUE(rig.gw.hasSessionForTest(sid));
    EXPECT_FALSE(rig.gw.sessionPermissionsForTest(sid)->allowsName(rs::ResourceType::Topic, "a/t", kReadBit));
    EXPECT_TRUE(rig.gw.sessionPermissionsForTest(sid)->allowsName(rs::ResourceType::Topic, "t", kReadBit));
}

TEST(NamespacePurgeGatewayRegression, OtherNamespaceUserUntouchedRegression) {
    Script sc;
    const Ids ids = build(sc, {kReadAPrefix});
    Rig rig(sc.s);
    const int64_t sid = rig.session(ids.bv, kB, {kReadT});
    (void)rig.purge(kA, "a");
    ASSERT_TRUE(rig.gw.hasSessionForTest(sid)) << "b/v is neither erased nor refreshed";
    EXPECT_TRUE(rig.gw.sessionPermissionsForTest(sid)->allowsName(rs::ResourceType::Topic, "t", kReadBit));
}
