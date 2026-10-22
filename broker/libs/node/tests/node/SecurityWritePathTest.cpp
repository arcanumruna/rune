// SecurityWritePathTest.cpp — authentication Task 10: the `__security` write path (SecurityAdmin, spec §3, D36):
// the leader validates a security DDL against its applied state with the shared library, dedupes on requestId,
// appends acks=all and answers once the event is committed AND applied locally, gated on its offset (D24).
// One SecurityAdmin + a real leader replica + the real SecurityApplier (support/SecurityDdlRig.h).
#include "support/SecurityDdlRig.h"

#include <rune/security/Permissions.h>

#include <gtest/gtest.h>

#include <algorithm>

using rune::node::test::SecurityDdlRig;
namespace sec = rune::security;
namespace pc  = rune::protocol::client;
using Op = pc::SecurityOpKind;
using Ch = pc::UserChangeKind;

namespace {
constexpr uint16_t kNotLeader = 2, kInternal = 6, kInvalidRequest = 39;

bool contains(const std::vector<uint8_t>& hay, const std::string& needle) {
    return std::search(hay.begin(), hay.end(), needle.begin(), needle.end()) != hay.end();
}
}  // namespace

TEST(SecurityWritePathRegression, EveryOpKindCommitsAndAnswersItsObject) {
    SecurityDdlRig rig("all_kinds");
    rig.becomeServingLeader();
    // 1 CreateRole
    auto a = rig.ddl(rig.createRole("world_server"));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_EQ(a.version, 1u);
    EXPECT_EQ(a.gateOffset, 0) << "an OK is gated on the event's offset (D24)";
    const uint64_t roleId = a.objectId;
    ASSERT_NE(roleId, 0u);
    // 2 CreateUser
    a = rig.ddl(rig.createUser("ws1", "a-long-password-1"));
    ASSERT_EQ(a.code, 0) << a.detail;
    const uint64_t userId = a.objectId;
    EXPECT_NE(userId, roleId) << "users and roles share one id counter";
    EXPECT_EQ(a.gateOffset, 1);
    // 3 Grant (to the role)
    a = rig.ddl(rig.grant(false, sec::GranteeKind::Role, "world_server", sec::Operation::Write, sec::ResourceType::Topic,
                          sec::PatternKind::Prefix, "world."));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_EQ(a.objectId, roleId);
    EXPECT_EQ(a.version, 2u);
    // 4 GrantRole
    a = rig.ddl(rig.grantRole(false, "ws1", "world_server"));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_EQ(a.objectId, userId);
    EXPECT_EQ(rig.state().user("ws1")->roles.count(roleId), 1u);
    // 5 AlterUser (AddPassword, then Disable, Enable, DropPassword)
    {
        auto m = rig.alterUser("ws1", Ch::AddPassword);
        m.verifier = SecurityDdlRig::verifierFor("another-long-pw-2");
        a = rig.ddl(std::move(m));
        ASSERT_EQ(a.code, 0) << a.detail;
        EXPECT_EQ(rig.state().user("ws1")->verifiers.size(), 2u);
    }
    a = rig.ddl(rig.alterUser("ws1", Ch::Disable));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_FALSE(rig.state().user("ws1")->enabled);
    a = rig.ddl(rig.alterUser("ws1", Ch::Enable));
    ASSERT_EQ(a.code, 0) << a.detail;
    {
        auto m = rig.alterUser("ws1", Ch::DropPassword);
        m.passwordIndex = 1;
        a = rig.ddl(std::move(m));
        ASSERT_EQ(a.code, 0) << a.detail;
        EXPECT_EQ(rig.state().user("ws1")->verifiers.size(), 1u);
    }
    {
        auto m = rig.alterUser("ws1", Ch::SetPassword);
        m.verifier = SecurityDdlRig::verifierFor("third-long-pw-3");
        m.terminateSessions = 1;
        a = rig.ddl(std::move(m));
        ASSERT_EQ(a.code, 0) << a.detail;
    }
    // 6 Revoke, 7 RevokeRole
    a = rig.ddl(rig.grant(true, sec::GranteeKind::Role, "world_server", sec::Operation::Write, sec::ResourceType::Topic,
                          sec::PatternKind::Prefix, "world."));
    ASSERT_EQ(a.code, 0) << a.detail;
    a = rig.ddl(rig.grantRole(true, "ws1", "world_server"));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_TRUE(rig.state().user("ws1")->roles.empty());
    // 8 DropRole, 9 DropUser
    a = rig.ddl(rig.simple(Op::DropRole, "world_server", "DROP ROLE world_server;"));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_EQ(rig.state().role("world_server"), nullptr);
    a = rig.ddl(rig.simple(Op::DropUser, "ws1", "DROP USER ws1;"));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_EQ(rig.state().user("ws1"), nullptr);
    // Every event is in the log, in offset order, and started == answered.
    const auto events = rig.recordedEvents();
    EXPECT_EQ(events.size(), 13u);
    for (std::size_t i = 0; i < events.size(); ++i) EXPECT_EQ(events[i].offset, static_cast<int64_t>(i));
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
}

TEST(SecurityWritePathRegression, RefusalsAnswerInvalidRequestNamingTheRuleAndAppendNothing) {
    SecurityDdlRig rig("refusals");
    rig.becomeServingLeader();
    ASSERT_EQ(rig.ddl(rig.createRole("r1")).code, 0);
    ASSERT_EQ(rig.ddl(rig.createUser("u1", "a-long-password-1")).code, 0);
    ASSERT_EQ(rig.ddl(rig.createUser("u2", "a-long-password-2")).code, 0);
    const std::size_t logged = rig.recordedEvents().size();

    auto expect = [&](SecurityDdlRig::Answer a, sec::Refusal want, const char* what) {
        EXPECT_EQ(a.code, kInvalidRequest) << what << ": " << a.detail;
        EXPECT_NE(a.detail.find(std::string{sec::refusalName(want)}), std::string::npos)
            << what << ": the detail names the rule (" << sec::refusalName(want) << "): " << a.detail;
        EXPECT_EQ(rig.recordedEvents().size(), logged) << what << ": nothing appended";
        EXPECT_EQ(a.gateOffset, -1) << what << ": a refusal is not gated";
    };
    expect(rig.ddl(rig.createUser("__sys", "a-long-password-1")), sec::Refusal::InvalidName, "InvalidName");
    expect(rig.ddl(rig.createUser("u1", "a-long-password-1")), sec::Refusal::UserExists, "UserExists");
    expect(rig.ddl(rig.createRole("r1")), sec::Refusal::RoleExists, "RoleExists");
    expect(rig.ddl(rig.simple(Op::DropUser, "nobody", "DROP USER nobody;")), sec::Refusal::UnknownUser, "UnknownUser");
    expect(rig.ddl(rig.simple(Op::DropRole, "norole", "DROP ROLE norole;")), sec::Refusal::UnknownRole, "UnknownRole");
    expect(rig.ddl(rig.grantRole(false, "u1", "norole")), sec::Refusal::UnknownRole, "UnknownRole (GRANT ROLE)");
    // Grants.
    auto g = [&] { return rig.grant(false, sec::GranteeKind::User, "u1", sec::Operation::Read, sec::ResourceType::Topic,
                                    sec::PatternKind::Exact, "t"); };
    ASSERT_EQ(rig.ddl(g()).code, 0);
    const std::size_t logged2 = rig.recordedEvents().size();
    EXPECT_EQ(logged2, logged + 1);
    auto a = rig.ddl(g());
    EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::AlreadyGranted)}), std::string::npos) << a.detail;
    a = rig.ddl(rig.grant(true, sec::GranteeKind::User, "u2", sec::Operation::Read, sec::ResourceType::Topic,
                          sec::PatternKind::Exact, "t"));
    EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::NotGranted)}), std::string::npos) << a.detail;
    a = rig.ddl(rig.grant(false, sec::GranteeKind::User, "u1", sec::Operation::Read, sec::ResourceType::Cluster,
                          sec::PatternKind::Exact, ""));
    EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::InvalidGrant)}), std::string::npos) << a.detail;
    // Membership.
    ASSERT_EQ(rig.ddl(rig.grantRole(false, "u1", "r1")).code, 0);
    a = rig.ddl(rig.grantRole(false, "u1", "r1"));
    EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::AlreadyMember)}), std::string::npos) << a.detail;
    a = rig.ddl(rig.grantRole(true, "u2", "r1"));
    EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::NotMember)}), std::string::npos) << a.detail;
    // Passwords: a second is fine, a third is refused; the last can not be dropped; an index that is not there.
    {
        auto m = rig.alterUser("u2", Ch::AddPassword);
        m.verifier = SecurityDdlRig::verifierFor("second-long-pw-22");
        ASSERT_EQ(rig.ddl(std::move(m)).code, 0);
        auto m3 = rig.alterUser("u2", Ch::AddPassword);
        m3.verifier = SecurityDdlRig::verifierFor("third-long-pw-33");
        a = rig.ddl(std::move(m3));
        EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::TooManyPasswords)}), std::string::npos) << a.detail;
        auto d = rig.alterUser("u1", Ch::DropPassword);
        d.passwordIndex = 1;
        a = rig.ddl(std::move(d));
        EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::LastPassword)}), std::string::npos) << a.detail;
        auto d3 = rig.alterUser("u2", Ch::DropPassword);
        d3.passwordIndex = 3;
        a = rig.ddl(std::move(d3));
        EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::NoSuchPassword)}), std::string::npos) << a.detail;
    }
    // Enable / disable.
    a = rig.ddl(rig.alterUser("u1", Ch::Enable));
    EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::AlreadyEnabled)}), std::string::npos) << a.detail;
    ASSERT_EQ(rig.ddl(rig.alterUser("u1", Ch::Disable)).code, 0);
    a = rig.ddl(rig.alterUser("u1", Ch::Disable));
    EXPECT_EQ(a.code, kInvalidRequest); EXPECT_NE(a.detail.find(std::string{sec::refusalName(sec::Refusal::AlreadyDisabled)}), std::string::npos) << a.detail;
    // AlreadyBootstrapped is covered by SecurityBootstrapRegression; VersionMismatch cannot arise: the leader builds
    // every version from the applied state it serializes on.
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
}

TEST(SecurityWritePathRegression, DuplicateRequestIdAnswersTheOriginalResultRegression) {
    SecurityDdlRig rig("dup");
    rig.becomeServingLeader();
    const auto rid = rune::protocol::RequestId{0xAB, 7};
    auto m1 = rig.createUser("u1", "a-long-password-1");
    m1.meta = rig.meta("CREATE USER u1 WITH PASSWORD '<redacted>';", "admin", rid);
    const auto first = rig.ddl(m1);
    ASSERT_EQ(first.code, 0) << first.detail;
    const std::size_t logged = rig.recordedEvents().size();
    // The retry (a different correlation id, the same statement): the ORIGINAL result, no second event.
    auto m2 = m1;
    m2.corr = rig.nextCorr();
    const auto second = rig.ddl(m2);
    EXPECT_EQ(second.code, 0);
    EXPECT_EQ(second.objectId, first.objectId);
    EXPECT_EQ(second.version, first.version);
    EXPECT_EQ(second.gateOffset, first.gateOffset) << "gated on the ORIGINAL event's offset";
    EXPECT_EQ(rig.recordedEvents().size(), logged) << "applied once";
}

TEST(SecurityWritePathRegression, ReusedRequestIdWithAnotherStatementIsInvalidRequest) {
    SecurityDdlRig rig("reuse");
    rig.becomeServingLeader();
    const auto rid = rune::protocol::RequestId{0xAB, 9};
    auto m1 = rig.createUser("u1", "a-long-password-1");
    m1.meta = rig.meta("CREATE USER u1 WITH PASSWORD '<redacted>';", "admin", rid);
    ASSERT_EQ(rig.ddl(m1).code, 0);
    auto other = rig.createUser("u2", "a-long-password-2");
    other.meta = rig.meta("CREATE USER u2 WITH PASSWORD '<redacted>';", "admin", rid);
    const auto a = rig.ddl(other);
    EXPECT_EQ(a.code, kInvalidRequest);
    EXPECT_NE(a.detail.find("requestId reused for a different statement"), std::string::npos) << a.detail;
    EXPECT_EQ(rig.state().user("u2"), nullptr);
}

TEST(SecurityWritePathRegression, DemotionAnswersTheInFlightDdlNotLeaderRegression) {
    SecurityDdlRig rig("demote");
    rig.becomeServingLeader({1, 2, 3});
    rig.silence(2); rig.silence(3);   // the event cannot commit
    const int64_t corr = rig.submitNoWait(rig.createUser("u1", "a-long-password-1"));
    EXPECT_FALSE(rig.answerFor(corr).has_value()) << "awaiting its commit";
    EXPECT_TRUE(rig.admin().hasInFlight());
    rig.becomeFollower(2, /*drive=*/false);
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value()) << "answered inside the demotion, not on a later tick";
    EXPECT_EQ(a->code, kNotLeader) << "retriable";
    EXPECT_FALSE(rig.admin().hasInFlight());
}

TEST(SecurityWritePathRegression, CatchingUpLeaderAnswersNotLeader) {
    SecurityDdlRig rig("catchup");
    rig.becomeServingLeader({1, 2, 3});
    rig.silence(2); rig.silence(3);
    (void)rig.submitNoWait(rig.createUser("u1", "a-long-password-1"));   // appended, never committed
    rig.becomeFollower(2, false);
    rig.assumeLeadership();   // a new leadership: LEO > HW - catching up
    rig.pump();
    ASSERT_EQ(rig.admin().roleName(), "LeaderCatchingUp");
    const int64_t corr = rig.submitNoWait(rig.createUser("u2", "a-long-password-2"));
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->code, kNotLeader);
    EXPECT_EQ(rig.state().user("u2"), nullptr);
}

TEST(SecurityWritePathRegression, FollowerAnswersNotLeader) {
    SecurityDdlRig rig("follower");
    rig.becomeFollower(2);
    const auto a = rig.ddl(rig.createUser("u1", "a-long-password-1"));
    EXPECT_EQ(a.code, kNotLeader);
}

TEST(SecurityWritePathRegression, CommitTimeoutAnswersNotLeaderAndTheRetryDedupes) {
    SecurityDdlRig rig("timeout");
    rig.becomeServingLeader({1, 2, 3});
    rig.silence(2);   // 3 still fetches: ISR size stays 3 but HW needs all of ISR -> never commits
    rig.silence(3);
    const auto rid = rune::protocol::RequestId{0xAB, 11};
    auto m = rig.createUser("u1", "a-long-password-1");
    m.meta = rig.meta("CREATE USER u1 WITH PASSWORD '<redacted>';", "admin", rid);
    const int64_t corr = rig.submitNoWait(m);
    EXPECT_FALSE(rig.answerFor(corr).has_value());
    rig.advanceClockPast(rune::node::SecurityAdmin::kDdlTimeoutNs);
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->code, kNotLeader);
    // The followers return: the event commits and applies; the retry answers its original result.
    rig.unsilence(2); rig.unsilence(3);
    for (int i = 0; i < 20; ++i) rig.pump();
    ASSERT_NE(rig.state().user("u1"), nullptr);
    m.corr = rig.nextCorr();
    const auto retry = rig.ddl(m);
    EXPECT_EQ(retry.code, 0) << retry.detail;
    EXPECT_EQ(rig.recordedEvents().size(), 1u);
}

TEST(SecurityWritePathRegression, HaltedBrokerAnswersInternal) {
    SecurityDdlRig rig("halted");
    rig.becomeServingLeader();
    rig.applier().tamperNextFingerprintForTest();
    const auto a = rig.ddl(rig.createUser("u1", "a-long-password-1"));   // appended; its apply halts this broker
    EXPECT_EQ(a.code, kInternal) << a.detail;
    EXPECT_TRUE(rig.applier().halted());
    const auto b = rig.ddl(rig.createUser("u2", "a-long-password-2"));
    EXPECT_EQ(b.code, kInternal);
    EXPECT_NE(b.detail.find("halted"), std::string::npos) << b.detail;
    EXPECT_EQ(rig.admin().roleName(), "Halted");
}

TEST(SecurityWritePathRegression, NoEventEverCarriesAPasswordLiteralRegression) {
    SecurityDdlRig rig("nopw");
    rig.becomeServingLeader();
    const std::string pw1 = "hunter2-correct-horse", pw2 = "battery-staple-9", pw3 = "xyzzy-plugh-1234";
    ASSERT_EQ(rig.ddl(rig.createUser("u1", pw1)).code, 0);
    auto add = rig.alterUser("u1", Ch::AddPassword);
    add.verifier = SecurityDdlRig::verifierFor(pw2);
    add.meta = rig.meta("ALTER USER u1 ADD PASSWORD '<redacted>';");
    ASSERT_EQ(rig.ddl(std::move(add)).code, 0);
    auto set = rig.alterUser("u1", Ch::SetPassword);
    set.verifier = SecurityDdlRig::verifierFor(pw3);
    set.meta = rig.meta("ALTER USER u1 SET PASSWORD '<redacted>' TERMINATE SESSIONS;");
    ASSERT_EQ(rig.ddl(std::move(set)).code, 0);
    for (const auto& r : rig.recordedEvents()) {
        for (const auto& pw : {pw1, pw2, pw3}) EXPECT_FALSE(contains(r.bytes, pw)) << "offset " << r.offset << " carries '" << pw << "'";
        if (r.event.kind != sec::EventKind::DropUser)
            EXPECT_NE(r.event.statement.find("'<redacted>'"), std::string::npos) << r.event.statement;
    }
}

// Review C I1 (D10): the leader refuses a statement carrying a password literal - the log is never compacted.
TEST(SecurityWritePathRegression, AnUnredactedPasswordLiteralIsRefusedAndAppendsNothingRegression) {
    SecurityDdlRig rig("unredacted");
    rig.becomeServingLeader();
    const std::vector<std::string> bad = {
        "CREATE USER u1 WITH PASSWORD 'hunter2-correct-horse';",
        "create user u1 with password \"x-long-password-1\";",
        "ALTER USER u1 SET PASSWORD 'p4ssword-long-1' TERMINATE SESSIONS;",
        "CREATE USER u1 WITH Password\t$$dollar-quoted$$;",
        "CREATE USER u1 WITH PASSWORD '<redacted>' ; ALTER USER u1 ADD PASSWORD 'leak-this-1234';",
    };
    for (const auto& stmt : bad) {
        auto m = rig.createUser("u1", "a-long-password-1");
        m.meta = rig.meta(stmt);
        const auto a = rig.ddl(std::move(m));
        EXPECT_EQ(a.code, kInvalidRequest) << stmt;
        EXPECT_NE(a.detail.find("password literal"), std::string::npos) << a.detail;
    }
    EXPECT_TRUE(rig.recordedEvents().empty()) << "nothing was appended";
    // The redacted forms (and statements that merely mention the keyword) still pass.
    EXPECT_EQ(rig.ddl(rig.createUser("u1", "a-long-password-1")).code, 0);
    auto drop = rig.alterUser("u1", Ch::AddPassword);
    drop.verifier = SecurityDdlRig::verifierFor("another-long-pw-1");
    drop.meta = rig.meta("ALTER USER u1 ADD PASSWORD '<redacted>';");
    EXPECT_EQ(rig.ddl(std::move(drop)).code, 0);
    auto dp = rig.alterUser("u1", Ch::DropPassword);
    dp.passwordIndex = 1;
    dp.meta = rig.meta("ALTER USER u1 DROP PASSWORD 1;");
    EXPECT_EQ(rig.ddl(std::move(dp)).code, 0);
    EXPECT_EQ(rig.recordedEvents().size(), 3u);
}

TEST(SecurityWritePathRegression, PrincipalIsTheSessionsAuthenticatedUser) {
    SecurityDdlRig rig("principal");
    rig.becomeServingLeader();
    const auto rid = rune::protocol::RequestId{0xCD, 3};
    auto m = rig.createUser("u1", "a-long-password-1", "alice");
    m.meta = rig.meta("CREATE USER u1 WITH PASSWORD '<redacted>';", "alice", rid);
    ASSERT_EQ(rig.ddl(m).code, 0);
    const auto events = rig.recordedEvents();
    ASSERT_EQ(events.size(), 1u);
    const auto& p = events[0].event.principal;
    EXPECT_EQ(p.authenticated, "alice");
    EXPECT_EQ(p.clientName, "rig");
    EXPECT_EQ(p.sessionId, SecurityDdlRig::kSession);
    EXPECT_EQ(p.requestId, rid);
    EXPECT_NE(p.requestFingerprint, 0u);
    EXPECT_GT(events[0].event.timestampMs, 1'700'000'000'000) << "the leader's wall clock";
}

// ── Bootstrap (D3 / D4): the first event, principal 'bootstrap', refused a second time ─────────────────────────────
TEST(SecurityBootstrapRegression, FirstBootstrapCreatesAMustChangeAdminWithAdminCluster) {
    SecurityDdlRig rig("boot1");
    rig.becomeServingLeader();
    EXPECT_FALSE(rig.state().hasSecurityAdmin());
    const auto a = rig.bootstrap(rig.bootstrapMsg("admin", "initial-one-time-pw"));
    ASSERT_EQ(a.code, 0) << a.detail;
    EXPECT_EQ(a.gateOffset, 0);
    const auto* u = rig.state().user("admin");
    ASSERT_NE(u, nullptr);
    EXPECT_TRUE(u->mustChangePassword) << "the one-time password must be changed first";
    EXPECT_TRUE(rig.state().hasSecurityAdmin()) << "ADMIN ON CLUSTER, directly granted";
    ASSERT_TRUE(rig.state().fakeSaltSeed().has_value());
    EXPECT_EQ((*rig.state().fakeSaltSeed())[0], 0x70) << "the fake-salt seed is set once, from the request (D8)";
    const auto grants = sec::effectiveGrants(rig.state(), u->objectId);
    EXPECT_FALSE(grants.empty());
}

TEST(SecurityBootstrapRegression, SecondBootstrapIsRefusedExplicitly) {
    SecurityDdlRig rig("boot2");
    rig.becomeServingLeader();
    ASSERT_EQ(rig.bootstrap(rig.bootstrapMsg("admin", "initial-one-time-pw", 1)).code, 0);
    const auto a = rig.bootstrap(rig.bootstrapMsg("admin2", "another-one-time-pw", 2));
    EXPECT_EQ(a.code, kInvalidRequest);
    EXPECT_EQ(a.detail, "security is already bootstrapped (a security admin exists)");
    EXPECT_EQ(rig.recordedEvents().size(), 1u);
    EXPECT_EQ(rig.state().user("admin2"), nullptr);
}

TEST(SecurityBootstrapRegression, BootstrapIsTheFirstEventWithPrincipalBootstrap) {
    SecurityDdlRig rig("boot3");
    rig.becomeServingLeader();
    ASSERT_EQ(rig.bootstrap(rig.bootstrapMsg("admin", "initial-one-time-pw")).code, 0);
    const auto events = rig.recordedEvents();
    ASSERT_EQ(events.size(), 1u);
    EXPECT_EQ(events[0].offset, 0);
    EXPECT_EQ(events[0].event.kind, sec::EventKind::Bootstrap);
    EXPECT_EQ(events[0].event.principal.authenticated, "bootstrap");
    EXPECT_NE(events[0].event.statement.find("<redacted>"), std::string::npos);
    EXPECT_EQ(contains(events[0].bytes, "initial-one-time-pw"), false);
}

TEST(SecurityBootstrapRegression, ARetriedBootstrapAnswersItsOriginalResult) {
    SecurityDdlRig rig("boot4");
    rig.becomeServingLeader();
    auto msg = rig.bootstrapMsg("admin", "initial-one-time-pw", 5);
    const auto first = rig.bootstrap(msg);
    ASSERT_EQ(first.code, 0);
    // The same frame again (the nonce is the request's identity): the original OK, not 'already bootstrapped'.
    msg.corr = rig.nextCorr();
    const auto retry = rig.bootstrap(msg);
    EXPECT_EQ(retry.code, 0) << retry.detail;
    EXPECT_EQ(retry.objectId, first.objectId);
    EXPECT_EQ(rig.recordedEvents().size(), 1u);
}

// Issue #228: the recorded statement is rendered by the leader from the op - a client cannot forge the audit text
// (nor the replayedFrom provenance) by sending its own.
TEST(SecurityWritePathRegression, TheRecordedStatementIsRenderedFromTheOpNotTakenFromTheClientRegression) {
    SecurityDdlRig rig("forged_statement");
    rig.becomeServingLeader();
    const std::string forged = "GRANT ADMIN ON CLUSTER TO ws1;";
    const auto forge = [&](rune::node::SecurityDdlMsg m) {
        m.meta.statement = forged;
        m.meta.replayedFrom = rune::catalog::ReplayedFrom{"x", 7, "mallory"};
        return m;
    };
    ASSERT_EQ(rig.ddl(rig.createUser("u", "a-long-password-1")).code, 0);
    ASSERT_EQ(rig.ddl(rig.createRole("r")).code, 0);
    // self ALTER USER u SET PASSWORD
    auto set = rig.alterUser("u", Ch::SetPassword);
    set.verifier = SecurityDdlRig::verifierFor("second-long-pw-2");
    set.meta.authenticated = "u";
    ASSERT_EQ(rig.ddl(forge(std::move(set))).code, 0);
    // GRANT
    auto g = rig.grant(false, sec::GranteeKind::Role, "r", sec::Operation::Write, sec::ResourceType::Topic,
                       sec::PatternKind::Prefix, "world.");
    ASSERT_EQ(rig.ddl(forge(std::move(g))).code, 0);
    // DROP USER
    ASSERT_EQ(rig.ddl(forge(rig.simple(Op::DropUser, "u", "DROP USER u;"))).code, 0);

    const auto events = rig.recordedEvents();
    ASSERT_EQ(events.size(), 5u);
    EXPECT_EQ(events[2].event.statement, "ALTER USER u SET PASSWORD '<redacted>';");
    EXPECT_EQ(events[3].event.statement, "GRANT WRITE ON TOPIC PREFIX 'world.' TO ROLE r;");
    EXPECT_EQ(events[4].event.statement, "DROP USER u;");
    for (std::size_t i = 2; i < events.size(); ++i) {
        EXPECT_EQ(events[i].event.statement.find("GRANT ADMIN"), std::string::npos) << events[i].event.statement;
        EXPECT_FALSE(events[i].event.replayedFrom.has_value()) << "offset " << events[i].offset;
    }
}
