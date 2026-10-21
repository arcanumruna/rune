// AuthLiveChangeTest.cpp - authentication Task 14 (spec §6, D24-D26) against ONE real authenticating BrokerNode: a GRANT /
// REVOKE / DISABLE / DROP / SET PASSWORD applies to the sessions that are ALREADY connected - explicitly (NotAuthorized
// / SessionRevoked on the correlation ids), never silently - and the DDL's OK means "this broker already enforces it".
#include "support/AuthRig.h"

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;
using SP = rune::protocol::SecurityPatternKind;
using namespace std::chrono_literals;

namespace {
constexpr int kRevoked = static_cast<int>(pc::ErrorCode::SessionRevoked);

struct Live {
    AuthRig r;
    explicit Live(const std::string& tag, AuthBroker::Options o = {}) : r(tag, std::move(o)) {}

    AuthBroker& b() { return r.broker(); }
    int64_t adminSid() { return b().adminSession(); }
    const std::string& ch() { return r.ch(); }

    // ── admin DDL (as the bootstrap admin; the answer is the gated OK) ──
    int revoke(const std::string& who, bool role, SO op, ST t, SP k, const std::string& pattern) {
        return b().admin().revoke(ch(), adminSid(), who, role, op, t, k, pattern).errorCode;
    }
    int revokeExact(const std::string& who, SO op, ST t, const std::string& name) { return revoke(who, false, op, t, SP::Exact, name); }
    int grantExact(const std::string& who, SO op, ST t, const std::string& name) {
        return b().admin().grant(ch(), adminSid(), who, false, op, t, SP::Exact, name).errorCode;
    }
    int createRole(const std::string& name) { return b().admin().createRole(ch(), adminSid(), name).errorCode; }
    int grantRoleOp(const std::string& role, SO op, ST t, SP k, const std::string& pattern) {
        return b().admin().grant(ch(), adminSid(), role, true, op, t, k, pattern).errorCode;
    }
    int grantRole(const std::string& user, const std::string& role) { return b().admin().grantRole(ch(), adminSid(), user, role).errorCode; }
    int dropRole(const std::string& role) { return b().admin().dropRole(ch(), adminSid(), role).errorCode; }
    int dropUser(const std::string& user) { return b().admin().dropUser(ch(), adminSid(), user).errorCode; }
    int alter(const std::string& user, pc::UserChangeKind k, const std::string& pw = {}, bool terminate = false) {
        return b().admin().alterPassword(ch(), adminSid(), user, k, pw, terminate).errorCode;
    }

    // ── a live subscription as one session ──
    struct Sub { int64_t corr = 0; int64_t id = 0; };
    Sub subscribe(int64_t sid, int32_t topic) {
        Sub s;
        s.corr = r.c().sendSubscribe(ch(), r.stream(), sid, topic, 0);
        const auto code = r.c().awaitSubscribeCode(s.corr, AuthRig::dl());
        if (!code || *code != 0) { s.corr = -1; return s; }
        s.id = r.c().subscriptionIdOf(s.corr).value_or(0);
        return s;
    }
    std::size_t pushes(int64_t subId) {
        std::size_t n = 0;
        for (const auto& p : r.c().records()) n += p.subscriptionId == subId ? 1 : 0;
        return n;
    }
    bool awaitPushes(int64_t subId, std::size_t atLeast) {
        return r.c().pollUntil([&] { return pushes(subId) >= atLeast; });
    }
    /// The code of the first ErrorResponse on `corr` seen after the subscribe (bounded wait); -1 = none.
    int errorOn(int64_t corr, int64_t sessionId = 0) {
        int code = -1;
        r.c().pollUntil([&] {
            for (const auto& e : r.c().errors())
                if (e.correlationId == corr && (sessionId == 0 || e.sessionId == sessionId)) { code = e.code; return true; }
            return false;
        });
        return code;
    }
    std::string errorTextOn(int64_t corr, int64_t sessionId) {
        for (const auto& e : r.c().errors())
            if (e.correlationId == corr && e.sessionId == sessionId) return e.message;
        return {};
    }
    /// Nothing more arrives for `subId` within `ms`.
    bool quiet(int64_t subId, std::chrono::milliseconds ms) {
        const std::size_t before = pushes(subId);
        r.c().pollFor(ms, [] { return false; });
        return pushes(subId) == before;
    }
};
}  // namespace

TEST(AuthLiveChangeRegression, RevokedReadEndsLiveSubscriptionsRegression) {
    Live t("liveread");
    std::string reader;
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const auto sub = t.subscribe(rs, t.r.orders);
    ASSERT_GT(sub.id, 0);
    ASSERT_EQ(t.r.produce(ws, t.r.orders, /*untilOk=*/true), 0);
    ASSERT_TRUE(t.awaitPushes(sub.id, 1)) << "the live subscription delivers";
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0);   // returns after the answer gate (D24)
    EXPECT_EQ(t.errorOn(sub.corr), 45) << "the end is explicit: NotAuthorized on the subscription's correlation id";
    EXPECT_NE(t.errorTextOn(sub.corr, rs).find("READ on TOPIC"), std::string::npos) << t.errorTextOn(sub.corr, rs);
    const std::size_t seen = t.pushes(sub.id);
    ASSERT_EQ(t.r.produce(ws, t.r.orders), 0);
    EXPECT_TRUE(t.quiet(sub.id, 500ms)) << "nothing is delivered after the end";
    EXPECT_EQ(t.pushes(sub.id), seen);
    EXPECT_EQ(t.r.subscribe(rs, t.r.orders), 45) << "a new Subscribe is refused";
}

TEST(AuthLiveChangeRegression, RevokedReadKeepsTheUsersOtherSubscriptionsRegression) {
    Live t("liveother");
    std::string reader;
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders"), exact(SO::Read, ST::Topic, "secret")}, &reader);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders"), exact(SO::Write, ST::Topic, "secret")});
    const auto a = t.subscribe(rs, t.r.orders);
    const auto b = t.subscribe(rs, t.r.secret);
    ASSERT_GT(a.id, 0);
    ASSERT_GT(b.id, 0);
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0);
    EXPECT_EQ(t.errorOn(a.corr), 45);
    ASSERT_EQ(t.r.produce(ws, t.r.secret, /*untilOk=*/true), 0);
    EXPECT_TRUE(t.awaitPushes(b.id, 1)) << "the topic that kept its READ keeps delivering";
}

TEST(AuthLiveChangeRegression, RevokedWriteFailsTheNextGatewayProduceRegression) {
    Live t("livewrite");
    std::string writer;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    ASSERT_EQ(t.r.produce(ws, t.r.orders, /*untilOk=*/true), 0);
    ASSERT_EQ(t.revokeExact(writer, SO::Write, ST::Topic, "orders"), 0);
    EXPECT_EQ(t.r.produce(ws, t.r.orders), 45) << "the very next produce after the OK";
}

TEST(AuthLiveChangeRegression, RevokedWriteFailsTheNextShardProduceRegression) {
    AuthBroker::Options o;
    o.shard = true;
    Live t("liveshard", o);
    std::string writer;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    auto shardProduce = [&](int attempts) {
        int code = kNoAnswer;
        for (int a = 0; a < attempts; ++a) {
            const int64_t corr = t.r.c().nextCorrelationId();
            if (!t.r.c().produce(t.b().workerIngressChannel(), AuthRig::stream(), ws, corr, t.r.orders, 0, 1, makeBatch(1)))
                return kNoAnswer;
            const auto ack = t.r.c().awaitProduceResponseUntil(corr, AuthRig::dl(10));
            code = ack ? ack->errorCode : kNoAnswer;
            if (code != static_cast<int>(pc::ErrorCode::NotLeader) && code != static_cast<int>(pc::ErrorCode::UnknownTopic) &&
                code != kNoAnswer)
                return code;
            std::this_thread::sleep_for(25ms);
        }
        return code;
    };
    ASSERT_EQ(shardProduce(200), 0);
    ASSERT_EQ(t.revokeExact(writer, SO::Write, ST::Topic, "orders"), 0);
    EXPECT_EQ(shardProduce(1), 45) << "the worker's bitmap was replaced before the DDL's OK (D24)";
    ASSERT_EQ(t.grantExact(writer, SO::Write, ST::Topic, "orders"), 0);
    EXPECT_EQ(shardProduce(1), 0) << "and a re-grant opens it again, without a new login";
}

TEST(AuthLiveChangeRegression, RoleRevokeRefreshesEveryMember) {
    Live t("liverole");
    ASSERT_EQ(t.createRole("readers"), 0);
    ASSERT_EQ(t.grantRoleOp("readers", SO::Read, ST::Topic, SP::Exact, "orders"), 0);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    std::vector<int64_t> sessions;
    std::vector<Live::Sub> subs;
    for (int i = 0; i < 3; ++i) {
        std::string name;
        sessions.push_back(t.r.newUser({}, &name));
        ASSERT_EQ(t.grantRole(name, "readers"), 0);
    }
    // Members were granted the role AFTER login: the grant itself reaches the live sessions.
    for (const int64_t s : sessions) { subs.push_back(t.subscribe(s, t.r.orders)); ASSERT_GT(subs.back().id, 0); }
    ASSERT_EQ(t.r.produce(ws, t.r.orders, true), 0);
    for (const auto& s : subs) ASSERT_TRUE(t.awaitPushes(s.id, 1));
    ASSERT_EQ(t.revoke("readers", true, SO::Read, ST::Topic, SP::Exact, "orders"), 0);
    for (const auto& s : subs) EXPECT_EQ(t.errorOn(s.corr), 45);
    for (const int64_t s : sessions) EXPECT_EQ(t.r.subscribe(s, t.r.orders), 45);
}

TEST(AuthLiveChangeRegression, DropRoleRefreshesMembers) {
    Live t("livedroprole");
    ASSERT_EQ(t.createRole("readers"), 0);
    ASSERT_EQ(t.grantRoleOp("readers", SO::Read, ST::Topic, SP::Exact, "orders"), 0);
    std::string name;
    const int64_t s = t.r.newUser({}, &name);
    ASSERT_EQ(t.grantRole(name, "readers"), 0);
    const auto sub = t.subscribe(s, t.r.orders);
    ASSERT_GT(sub.id, 0);
    ASSERT_EQ(t.dropRole("readers"), 0);
    EXPECT_EQ(t.errorOn(sub.corr), 45) << "the member loses what the dropped role gave";
    EXPECT_EQ(t.r.subscribe(s, t.r.orders), 45);
}

TEST(AuthLiveChangeRegression, GrantArrivesWithoutReconnect) {
    Live t("livegrant");
    std::string name;
    const int64_t s = t.r.newUser({}, &name);
    EXPECT_EQ(t.r.subscribe(s, t.r.orders), 45);
    ASSERT_EQ(t.grantExact(name, SO::Read, ST::Topic, "orders"), 0);
    EXPECT_EQ(t.r.subscribe(s, t.r.orders), 0) << "the same session, no reconnect";
}

TEST(AuthLiveChangeRegression, PrefixGrantCoversATopicCreatedAfterLogin) {
    Live t("liveprefix");
    const int64_t s = t.r.newUser({prefix(SO::Read, ST::Topic, "w.")});
    const int32_t late = t.r.defineTopic(t.r.builder, "w.late");
    ASSERT_GE(late, 0);
    ASSERT_TRUE(t.r.awaitListed(late));
    EXPECT_EQ(t.r.subscribe(s, late), 0);
    const int32_t other = t.r.defineTopic(t.r.builder, "x.late");
    ASSERT_GE(other, 0);
    EXPECT_EQ(t.r.subscribe(s, other), 45) << "a topic outside the prefix";
}

TEST(AuthLiveChangeRegression, DisableRevokesEnableAllowsLogin) {
    Live t("livedisable");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &name);
    const auto sub = t.subscribe(s, t.r.orders);
    ASSERT_GT(sub.id, 0);
    ASSERT_EQ(t.alter(name, pc::UserChangeKind::Disable), 0);
    EXPECT_EQ(t.errorOn(sub.corr), kRevoked) << "the live subscription completes SessionRevoked";
    EXPECT_EQ(t.errorOn(0, s), kRevoked) << "then the session-level frame";
    EXPECT_EQ(t.errorTextOn(0, s), "user disabled");
    // The session is gone: a request on it is dropped as an unknown session (this gateway never answers those).
    EXPECT_FALSE(t.r.c().pingError(t.ch(), t.r.stream(), s, AuthRig::dl(1)).has_value());
    const int64_t s2 = t.r.wire().connect(t.ch());
    ASSERT_GT(s2, 0);
    EXPECT_EQ(t.r.wire().login(t.ch(), s2, name, {AuthRig::kPw, AuthRig::kPw}).errorCode(), 44) << "login while disabled";
    EXPECT_TRUE(t.b().awaitCounter(kSessionsRevokedCounterTypeId, 1)) << "counter 232";
    {
        const auto en = t.b().admin().alterPassword(t.ch(), t.adminSid(), name, pc::UserChangeKind::Enable);
        ASSERT_EQ(en.errorCode, 0) << en.detail;
    }
    EXPECT_NO_THROW((void)t.r.login(name)) << "ENABLE allows login again";
}

TEST(AuthLiveChangeRegression, DropUserRevokesEverySession) {
    Live t("livedropuser");
    std::string name;
    const int64_t s1 = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t s2 = t.r.login(name);
    const auto a = t.subscribe(s1, t.r.orders);
    const auto b = t.subscribe(s2, t.r.orders);
    ASSERT_GT(a.id, 0);
    ASSERT_GT(b.id, 0);
    ASSERT_EQ(t.dropUser(name), 0);
    EXPECT_EQ(t.errorOn(a.corr), kRevoked);
    EXPECT_EQ(t.errorOn(b.corr), kRevoked);
    EXPECT_EQ(t.errorOn(0, s1), kRevoked);
    EXPECT_EQ(t.errorOn(0, s2), kRevoked);
    EXPECT_EQ(t.errorTextOn(0, s1), "user dropped");
    EXPECT_TRUE(t.b().awaitCounter(kSessionsRevokedCounterTypeId, 2));
}

TEST(AuthLiveChangeRegression, SetPasswordKeepsSessions) {
    Live t("livesetpw");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const auto sub = t.subscribe(s, t.r.orders);
    ASSERT_GT(sub.id, 0);
    ASSERT_EQ(t.alter(name, pc::UserChangeKind::SetPassword, "a-new-password-98765"), 0);
    ASSERT_EQ(t.r.produce(ws, t.r.orders, true), 0);
    EXPECT_TRUE(t.awaitPushes(sub.id, 1)) << "the live session keeps receiving";
    const int64_t s2 = t.r.wire().connect(t.ch());
    EXPECT_EQ(t.r.wire().login(t.ch(), s2, name, {AuthRig::kPw, AuthRig::kPw}).errorCode(), 44) << "the old password";
    const int64_t s3 = t.r.wire().connect(t.ch());
    EXPECT_TRUE(t.r.wire().login(t.ch(), s3, name, {"a-new-password-98765", "a-new-password-98765"}).ok()) << "the new one";
}

TEST(AuthLiveChangeRegression, SetPasswordTerminateRevokes) {
    Live t("liveterminate");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &name);
    const auto sub = t.subscribe(s, t.r.orders);
    ASSERT_GT(sub.id, 0);
    ASSERT_EQ(t.alter(name, pc::UserChangeKind::SetPassword, "a-new-password-98765", /*terminate=*/true), 0);
    EXPECT_EQ(t.errorOn(sub.corr), kRevoked);
    EXPECT_EQ(t.errorOn(0, s), kRevoked);
    EXPECT_EQ(t.errorTextOn(0, s), "password changed (TERMINATE SESSIONS)");
}

TEST(AuthLiveChangeRegression, CatalogReadEndsWhenDescribeClusterIsRevoked) {
    Live t("livecatalog");
    std::string name;
    const int64_t s = t.r.newUser({cluster(SO::Describe)}, &name);
    const int64_t corr = t.r.c().sendCatalogSubscribe(t.ch(), t.r.stream(), s, 0);
    const auto code = t.r.c().awaitSubscribeCode(corr, AuthRig::dl());
    ASSERT_TRUE(code && *code == 0);
    ASSERT_EQ(t.revoke(name, false, SO::Describe, ST::Cluster, SP::Exact, ""), 0);
    EXPECT_EQ(t.errorOn(corr), 45);
    EXPECT_EQ(t.r.catalogSubscribe(s), 45) << "and a new catalog read is refused";
}

TEST(AuthLiveChangeRegression, SecurityReadEndsOnlyWhenAdminClusterIsRevoked) {
    Live t("livesecurity");
    std::string name;
    const int64_t s = t.r.newUser({cluster(SO::Admin), cluster(SO::Describe)}, &name);
    const int64_t catalogCorr = t.r.c().sendCatalogSubscribe(t.ch(), t.r.stream(), s, 0);
    ASSERT_TRUE(t.r.c().awaitSubscribeCode(catalogCorr, AuthRig::dl()).value_or(-1) == 0);
    const int64_t secCorr = t.r.c().sendSubscribe(t.ch(), t.r.stream(), s, pc::kSecurityTopicId, 0);
    ASSERT_TRUE(t.r.c().awaitSubscribeCode(secCorr, AuthRig::dl()).value_or(-1) == 0);
    ASSERT_EQ(t.revoke(name, false, SO::Admin, ST::Cluster, SP::Exact, ""), 0);
    EXPECT_EQ(t.errorOn(secCorr), 45);
    for (const auto& e : t.r.c().errors()) EXPECT_NE(e.correlationId, catalogCorr) << "the catalog read (DESCRIBE kept) stays";
}

TEST(AuthLiveChangeRegression, GroupMemberLosingGroupReadIsRemovedWithNotAuthorized) {
    Live t("livegroup");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")}, &name);
    std::string member;
    ASSERT_EQ(t.r.joinGroup(s, "cg", {t.r.orders}, &member), 0);
    const int before = t.r.heartbeat(s, "cg", member);
    ASSERT_NE(before, 45);
    ASSERT_NE(before, static_cast<int>(pc::ErrorCode::UnknownMember)) << "a live member";
    ASSERT_EQ(t.revokeExact(name, SO::Read, ST::Group, "cg"), 0);
    EXPECT_EQ(t.r.heartbeat(s, "cg", member), 45) << "the next Heartbeat answers NotAuthorized";
    EXPECT_EQ(t.r.offsetCommit(s, "cg", t.r.orders), 45);
    EXPECT_EQ(t.r.offsetFetch(s, "cg", t.r.orders), 45);
    // The coordinator removed the member: after a re-grant the same member id is unknown, not still a member.
    ASSERT_EQ(t.grantExact(name, SO::Read, ST::Group, "cg"), 0);
    const int hb = t.r.heartbeat(s, "cg", member);
    EXPECT_EQ(hb, static_cast<int>(pc::ErrorCode::UnknownMember)) << "removed from the group by the revoke";
}

TEST(AuthLiveChangeRegression, TheGrantOkIsEnforcedByTheAnsweringBrokerImmediately) {
    Live t("liveimmediate");
    std::string name;
    const int64_t s = t.r.newUser({}, &name);
    ASSERT_EQ(t.grantExact(name, SO::Write, ST::Topic, "orders"), 0);
    ASSERT_EQ(t.r.produce(s, t.r.orders, /*untilOk=*/true), 0);   // warm: leadership settled
    ASSERT_EQ(t.revokeExact(name, SO::Write, ST::Topic, "orders"), 0);
    for (int i = 0; i < 100; ++i) {
        ASSERT_EQ(t.grantExact(name, SO::Write, ST::Topic, "orders"), 0) << i;
        ASSERT_EQ(t.r.produce(s, t.r.orders), 0) << "GRANT then, with zero delay, a produce: OK (iteration " << i << ")";
        ASSERT_EQ(t.revokeExact(name, SO::Write, ST::Topic, "orders"), 0) << i;
        ASSERT_EQ(t.r.produce(s, t.r.orders), 45) << "REVOKE then, with zero delay, a produce: refused (iteration " << i << ")";
    }
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// PR #441 adversarial review (issue #259 area): pooled tables, binds, patches and drops - enforced on the SHARD worker
// (a real authenticating broker, producing straight to the worker's ingress).
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
/// The worker-ingress produce of `sid` to `topic`: retried through NotLeader / UnknownTopic / no answer while the
/// partition settles (bounded), else the first definitive code.
int shardProduceCode(Live& t, int64_t sid, int32_t topic, int attempts = 1) {
    int code = kNoAnswer;
    for (int a = 0; a < attempts; ++a) {
        const int64_t corr = t.r.c().nextCorrelationId();
        if (!t.r.c().produce(t.b().workerIngressChannel(), AuthRig::stream(), sid, corr, topic, 0, 1, makeBatch(1)))
            return kNoAnswer;
        const auto ack = t.r.c().awaitProduceResponseUntil(corr, AuthRig::dl(10));
        code = ack ? ack->errorCode : kNoAnswer;
        if (code != static_cast<int>(pc::ErrorCode::NotLeader) && code != static_cast<int>(pc::ErrorCode::UnknownTopic) &&
            code != kNoAnswer)
            return code;
        std::this_thread::sleep_for(25ms);   // the partition settling (pre-existing pattern of this file)
    }
    return code;
}
AuthBroker::Options shardOptions() { AuthBroker::Options o; o.shard = true; return o; }
}  // namespace

// Concurrency / auth: one role's members share ONE table on the worker; flipping the role's WRITE repeatedly is enforced
// on every member's worker slot at the DDL's OK, and a bystander with its own grant set is never touched.
TEST(AuthLiveChangeAdversarialRegression, ARoleFlippedRepeatedlyIsEnforcedOnEveryMembersWorkerSlotAdversarialRegression) {
    Live t("adv259flip", shardOptions());
    ASSERT_EQ(t.createRole("writers"), 0);
    ASSERT_EQ(t.grantRoleOp("writers", SO::Write, ST::Topic, SP::Exact, "orders"), 0);
    std::vector<int64_t> members;
    for (int i = 0; i < 3; ++i) {
        std::string name;
        members.push_back(t.r.newUser({}, &name));
        ASSERT_EQ(t.grantRole(name, "writers"), 0);
    }
    const int64_t bystander = t.r.newUser({exact(SO::Write, ST::Topic, "orders"), exact(SO::Write, ST::Topic, "secret")});
    for (const int64_t s : members) ASSERT_EQ(shardProduceCode(t, s, t.r.orders, 200), 0);
    ASSERT_EQ(shardProduceCode(t, bystander, t.r.orders, 200), 0);
    for (int flip = 0; flip < 3; ++flip) {
        ASSERT_EQ(t.revoke("writers", true, SO::Write, ST::Topic, SP::Exact, "orders"), 0) << flip;
        for (const int64_t s : members) EXPECT_EQ(shardProduceCode(t, s, t.r.orders), 45) << "flip " << flip;
        EXPECT_EQ(shardProduceCode(t, bystander, t.r.orders), 0) << "another grant set's table is untouched";
        ASSERT_EQ(t.grantRoleOp("writers", SO::Write, ST::Topic, SP::Exact, "orders"), 0) << flip;
        for (const int64_t s : members) EXPECT_EQ(shardProduceCode(t, s, t.r.orders), 0) << "flip " << flip;
    }
}

// Lifetime: a grant set whose last session leaves is freed and its id reused by ANOTHER set; the new holder never
// gets the old set's WRITE on the worker, and the old user logging in again gets its own back.
TEST(AuthLiveChangeAdversarialRegression, AFreedTableIdReusedByAnotherSetNeverGrantsTheOldWritesAdversarialRegression) {
    Live t("adv259reuse", shardOptions());
    std::string a;
    const int64_t sa = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &a);
    ASSERT_EQ(shardProduceCode(t, sa, t.r.orders, 200), 0);
    ASSERT_TRUE(t.r.c().disconnect(t.ch(), t.r.stream(), sa));
    // The Disconnect rides the same client publication as the next user's Connect: the gateway closes `sa` (its table's
    // last holder: freed) before that login acquires a table - the id is reused.
    const int64_t sb = t.r.newUser({exact(SO::Write, ST::Topic, "secret")});
    EXPECT_EQ(shardProduceCode(t, sb, t.r.orders, 1), 45) << "the reused table holds the NEW set only";
    EXPECT_EQ(shardProduceCode(t, sb, t.r.secret, 200), 0);
    const int64_t sa2 = t.r.login(a);
    EXPECT_EQ(shardProduceCode(t, sa2, t.r.orders, 200), 0);
    EXPECT_EQ(shardProduceCode(t, sa2, t.r.secret, 1), 45);
}

// Prefix extension through the PATCH path: two sessions of one user share a table; a topic the prefix covers, created
// after login, becomes writable on the worker for both; one outside the prefix stays refused.
TEST(AuthLiveChangeAdversarialRegression, ATopicCreatedAfterLoginReachesEverySharerOnTheShardAdversarialRegression) {
    Live t("adv259patch", shardOptions());
    std::string w;
    const int64_t s1 = t.r.newUser({prefix(SO::Write, ST::Topic, "w.")}, &w);
    const int64_t s2 = t.r.login(w);
    const int32_t late = t.r.defineTopic(t.r.builder, "w.late");
    ASSERT_GE(late, 0);
    ASSERT_TRUE(t.r.awaitListed(late));
    EXPECT_EQ(shardProduceCode(t, s1, late, 200), 0);
    EXPECT_EQ(shardProduceCode(t, s2, late, 200), 0);
    const int32_t other = t.r.defineTopic(t.r.builder, "x.late");
    ASSERT_GE(other, 0);
    ASSERT_TRUE(t.r.awaitListed(other));
    EXPECT_EQ(shardProduceCode(t, s1, other, 200), 45);
}

// Crash and restart (issue #117 Hold gate): after a restart the control thread's mirror is the booted state plus the
// live re-applies; an admin reading `__security` from the start must get every event and the caught-up marker (never
// held forever behind the gate), and a live event after it arrives too.
TEST(AuthLiveChangeAdversarialRegression, ASecurityHistoryReadAfterARestartIsNeverHeldForeverAdversarialRegression) {
    Live t("adv117restart");
    for (int i = 0; i < 3; ++i) (void)t.r.newUser({exact(SO::Read, ST::Topic, "orders")});
    t.r.restart();
    auto& c = t.b().admin().raw();
    const int64_t corr = c.sendSubscribe(t.ch(), AuthRig::stream(), t.adminSid(), pc::kSecurityTopicId, 0);
    ASSERT_EQ(c.awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0);
    const int64_t sub = c.subscriptionIdOf(corr).value_or(0);
    ASSERT_NE(sub, 0);
    auto markerSeen = [&] {
        for (const auto& p : c.records()) if (p.subscriptionId == sub && p.bytes.size() == 8 && p.baseOffset >= 0) return true;
        return false;
    };
    auto newest = [&] {
        int64_t n = -1;
        for (const auto& p : c.records()) {
            if (p.subscriptionId != sub || p.bytes.size() <= 8) continue;
            std::size_t pos = 0;
            while (pos + 61 <= p.bytes.size()) {
                rune::protocol::RecordBatchReader rd(p.bytes.data() + pos, p.bytes.size() - pos);
                if (!rd.ensureValid()) break;
                n = std::max(n, rd.baseOffset() + static_cast<int64_t>(rd.lastOffsetDelta()));
                pos += static_cast<std::size_t>(rd.totalLength());
            }
        }
        return n;
    };
    ASSERT_TRUE(c.pollUntil(markerSeen)) << "the history read reached its caught-up marker after the restart";
    const int64_t before = newest();
    EXPECT_GE(before, 3) << "the bootstrap, the builder and the three users at least";
    (void)t.r.newUser({});
    EXPECT_TRUE(c.pollUntil([&] { return newest() > before; })) << "a live event after the restart is not held forever";
}

// Auth, revoked mid-stream (issue #117 end-to-end): a `__security` reader that loses ADMIN gets 45 and NOT ONE frame of
// the verifier-bearing events after its revoke, over repeated revoke / re-grant / re-subscribe rounds.
TEST(AuthLiveChangeAdversarialRegression, ARevokedSecurityReaderGetsNoEventAfterItsRevokeOverRoundsAdversarialRegression) {
    Live t("adv117rounds");
    std::string name;
    const int64_t s = t.r.newUser({cluster(SO::Admin), cluster(SO::Describe)}, &name);
    for (int round = 0; round < 3; ++round) {
        const int64_t corr = t.r.c().sendSubscribe(t.ch(), t.r.stream(), s, pc::kSecurityTopicId, 0);
        ASSERT_EQ(t.r.c().awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0) << round;
        const int64_t sub = t.r.c().subscriptionIdOf(corr).value_or(0);
        ASSERT_NE(sub, 0);
        ASSERT_TRUE(t.r.c().pollUntil([&] {
            for (const auto& p : t.r.c().records()) if (p.subscriptionId == sub && p.bytes.size() == 8 && p.baseOffset >= 0) return true;
            return false;
        })) << "caught up, round " << round;
        ASSERT_EQ(t.revoke(name, false, SO::Admin, ST::Cluster, SP::Exact, ""), 0) << round;
        EXPECT_EQ(t.errorOn(corr), 45) << round;
        const std::size_t seen = t.pushes(sub);
        (void)t.r.newUser({});   // a CREATE USER: a new verifier in `__security`
        EXPECT_TRUE(t.quiet(sub, 300ms)) << "no frame after the revoke, round " << round;
        EXPECT_EQ(t.pushes(sub), seen);
        ASSERT_EQ(t.b().admin().grant(t.ch(), t.adminSid(), name, false, SO::Admin, ST::Cluster, SP::Exact, "").errorCode, 0);
    }
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issues #196 / #109: the D24 OK on the SHARD path (the default runtime) is released only once the worker enforces the
// change - its lifecycle ring installed the revoke (bitmap / bind / close), its op ring ran the ending Unsubscribe - and
// a worker-pushed subscription's terminal NotAuthorized comes from the worker after its last Records.  The worker is made
// slow on purpose (its ring is not drained for a while: a test seam on its own thread); nothing is dropped.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
constexpr int64_t kSlowWorkerNs = 400'000'000;   // longer than an OK takes when nothing waits for the worker
void holdLifecycle(Live& t) {
    (void)t.b().node().onWorkerThreadForTest(0, [](Worker& w) { w.holdSessionOpsForTest(kSlowWorkerNs); return 0; });
}
void holdOpRing(Live& t) {
    (void)t.b().node().onWorkerThreadForTest(0, [](Worker& w) { w.holdOpRingForTest(kSlowWorkerNs); return 0; });
}
int64_t leoOf(Live& t, int32_t topic) {
    return t.b().node().onOwnerThreadForTest(topic, 0, [](rune::replication::PartitionReplica* r) {
        return r ? r->logEndOffset() : int64_t{-1};
    });
}
/// One worker-ingress produce, waiting at most `seconds` for its answer (kNoAnswer: none).
int shardProduceOnce(Live& t, int64_t sid, int32_t topic, int seconds = 3) {
    const int64_t corr = t.r.c().nextCorrelationId();
    if (!t.r.c().produce(t.b().workerIngressChannel(), AuthRig::stream(), sid, corr, topic, 0, 1, makeBatch(1)))
        return kNoAnswer;
    const auto ack = t.r.c().awaitProduceResponseUntil(corr, AuthRig::dl(seconds));
    return ack ? ack->errorCode : kNoAnswer;
}
}  // namespace

// Issue #196 (RED before the fix: the REVOKE's OK was released once the shipper had HANDED the new bind to the worker's
// lifecycle ring; the worker had not installed it, and the very next produce on the old permission slot was appended and
// acked).  Repeated: every round's OK is followed by a refused produce, every re-grant's by an accepted one.
TEST(AuthLiveChangeRegression, ShardRevokeOkMeansTheWorkerAlreadyRefusesRegression) {
    Live t("shardfence", shardOptions());
    std::string writer;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
    for (int round = 0; round < 4; ++round) {
        holdLifecycle(t);
        ASSERT_EQ(t.revokeExact(writer, SO::Write, ST::Topic, "orders"), 0) << "round " << round;
        const int64_t leo = leoOf(t, t.r.orders);
        EXPECT_EQ(shardProduceOnce(t, ws, t.r.orders), 45) << "round " << round << ": the worker refuses at the OK (D24)";
        EXPECT_EQ(leoOf(t, t.r.orders), leo) << "round " << round << ": nothing was appended after the revoke's OK";
        holdLifecycle(t);
        ASSERT_EQ(t.grantExact(writer, SO::Write, ST::Topic, "orders"), 0);
        EXPECT_EQ(shardProduceOnce(t, ws, t.r.orders), 0) << "round " << round << ": the re-grant is enforced at its OK";
    }
}

// Issue #196, the session-ending kinds: DISABLE / TERMINATE SESSIONS revoke the session; with the worker slow to drain
// its lifecycle ring the OK still means the worker refuses that session's direct produce (it is bound to nothing ahead
// of the fence).  RED before the fix: the produce after the OK was appended.
TEST(AuthLiveChangeRegression, ShardDisableAndTerminateOkMeanTheWorkerAlreadyRefusesRegression) {
    Live t("shardfencerevoke", shardOptions());
    for (const bool terminate : {false, true}) {
        std::string writer;
        const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
        ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
        holdLifecycle(t);
        if (terminate)
            ASSERT_EQ(t.alter(writer, pc::UserChangeKind::SetPassword, "a brand new password 77", /*terminate=*/true), 0);
        else
            ASSERT_EQ(t.alter(writer, pc::UserChangeKind::Disable), 0);
        const int64_t leo = leoOf(t, t.r.orders);
        EXPECT_NE(shardProduceOnce(t, ws, t.r.orders, 2), 0) << (terminate ? "TERMINATE" : "DISABLE");
        EXPECT_EQ(leoOf(t, t.r.orders), leo) << (terminate ? "TERMINATE" : "DISABLE") << ": nothing appended after the OK";
    }
}

// Issue #109 (RED before the fix: the gateway sent the 45 at once on its own egress while the ending Unsubscribe waited
// in the worker's op ring; the worker kept pushing on the subscription's own record egress, so records produced AFTER
// the REVOKE's OK reached the revoked consumer after its 45).  The worker ends it: the 45 follows its last Records, the
// OK follows the end, and nothing is delivered afterwards.
TEST(AuthLiveChangeRegression, NoRecordAfterTheEndOnPerWorkerEgressRegression) {
    Live t("shardend", shardOptions());
    std::string reader;
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const auto sub = t.subscribe(rs, t.r.orders);
    ASSERT_GT(sub.id, 0);
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
    ASSERT_TRUE(t.awaitPushes(sub.id, 1)) << "the worker pushes the live subscription";
    holdOpRing(t);
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(shardProduceOnce(t, ws, t.r.orders), 0) << "the writer keeps its WRITE";
    EXPECT_EQ(t.errorOn(sub.corr, rs), 45) << "the end is explicit, on the subscription's correlation id";
    EXPECT_NE(t.errorTextOn(sub.corr, rs).find("READ on TOPIC"), std::string::npos) << t.errorTextOn(sub.corr, rs);
    EXPECT_TRUE(t.quiet(sub.id, 500ms));
    uint64_t endSeq = 0;
    for (const auto& e : t.r.c().errors()) if (e.correlationId == sub.corr && e.sessionId == rs) { endSeq = e.seq; break; }
    ASSERT_GT(endSeq, 0u);
    for (const auto& p : t.r.c().records())
        EXPECT_FALSE(p.subscriptionId == sub.id && p.seq > endSeq)
            << "a Records frame (offset " << p.baseOffset << ") arrived after the subscription's 45";
    EXPECT_EQ(t.r.subscribe(rs, t.r.orders), 45) << "a new Subscribe is refused";
}
