// AuthLiveChangeTest.cpp - authentication Task 14 (spec §6, D24-D26) against ONE real authenticating BrokerNode: a GRANT /
// REVOKE / DISABLE / DROP / SET PASSWORD applies to the sessions that are ALREADY connected - explicitly (NotAuthorized
// / SessionRevoked on the correlation ids), never silently - and the DDL's OK means "this broker already enforces it".
#include "support/AuthRig.h"

#include <limits>

#include <rune/node/AdminChannels.h>

#include <functional>

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
    EXPECT_EQ(t.r.subscribeDenied(rs, t.r.orders), 45) << "a new Subscribe is refused";
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
    EXPECT_EQ(t.r.produceDenied(ws, t.r.orders), 45) << "the very next produce after the OK";
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
            if (!t.r.c().produce(t.b().workerIngressChannel(), AuthRig::stream(), ws, corr, t.r.orders, 0, 1, makeCharBatch(1)))
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
    const int64_t before = t.r.logEnd(t.r.orders);
    EXPECT_EQ(shardProduce(1), 45) << "the worker's bitmap was replaced before the DDL's OK (D24)";
    EXPECT_EQ(t.r.logEnd(t.r.orders), before) << "#278: the refused produce appended nothing";
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
    for (const int64_t s : sessions) EXPECT_EQ(t.r.subscribeDenied(s, t.r.orders), 45);
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
    EXPECT_EQ(t.r.subscribeDenied(s, t.r.orders), 45);
}

TEST(AuthLiveChangeRegression, GrantArrivesWithoutReconnect) {
    Live t("livegrant");
    std::string name;
    const int64_t s = t.r.newUser({}, &name);
    EXPECT_EQ(t.r.subscribeDenied(s, t.r.orders), 45);
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
    EXPECT_EQ(t.r.subscribeDenied(s, other), 45) << "a topic outside the prefix";
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
    EXPECT_TRUE(t.r.awaitSessionGone(s)) << "#138: the gateway ended the session";
    const int64_t s2 = t.r.wire().connect(t.ch());
    ASSERT_GT(s2, 0);
    EXPECT_EQ(t.r.wire().login(t.ch(), s2, name, {AuthRig::kPw, AuthRig::kPw}).errorCode(), 44) << "login while disabled";
    EXPECT_TRUE(t.b().awaitCounter(typeIdOf(RuneMetric::SessionsRevoked), 1)) << "counter 232";
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
    EXPECT_TRUE(t.b().awaitCounter(typeIdOf(RuneMetric::SessionsRevoked), 2));
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

// ── Issue #212: a member that loses READ on its topic, or whose session is revoked, leaves its group at once ──────────
// Not at the heartbeat timeout (30 s here; no test waits for it): its partitions are consumed by nobody until then.
namespace {
constexpr int kUnknownMember = static_cast<int>(pc::ErrorCode::UnknownMember);
/// The remaining member's Heartbeat at the old generation once its group rebalanced: RebalanceInProgress, or
/// StaleGeneration when the coordinator already moved the group to a new generation - never the live OK.
bool rebalanced(int hb) {
    return hb == static_cast<int>(pc::ErrorCode::RebalanceInProgress) || hb == static_cast<int>(pc::ErrorCode::StaleGeneration);
}
/// Members on sessions `s1` and `s2` of group "cg" over `topics`, both Stable at one generation.
struct Pair { std::string m1, m2; int32_t gen = -1; };
Pair joinPair(Live& t, int64_t s1, int64_t s2, const std::vector<int32_t>& topics) {
    Pair p;
    const auto a = t.r.c().joinGroup(t.ch(), AuthRig::stream(), s1, "cg", {}, topics);
    const auto b = t.r.c().joinGroup(t.ch(), AuthRig::stream(), s2, "cg", {}, topics);
    if (!a || a->errorCode != 0 || !b || b->errorCode != 0) return p;
    p.m1 = a->memberId; p.m2 = b->memberId;
    const auto ra = t.r.c().joinGroup(t.ch(), AuthRig::stream(), s1, "cg", p.m1, topics);
    const auto rb = t.r.c().joinGroup(t.ch(), AuthRig::stream(), s2, "cg", p.m2, topics);
    if (!ra || ra->errorCode != 0 || !rb || rb->errorCode != 0) return p;
    p.gen = std::max(ra->generation, rb->generation);
    // The coordinator assigns on its next duty pass: bounded by the number of round trips, never by wall time.
    for (int i = 0; i < 200; ++i)
        if (t.r.heartbeatAt(s1, "cg", p.m1, p.gen) == 0 && t.r.heartbeatAt(s2, "cg", p.m2, p.gen) == 0) return p;
    p.gen = -1;
    return p;
}
}  // namespace

TEST(AuthLiveChangeRegression, RevokedTopicReadEvictsTheGroupMemberRegression) {
    Live t("livegrouptopic");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
    const Pair p = joinPair(t, s, s2, {t.r.orders});
    ASSERT_GT(p.gen, 0) << "precondition: two live members, Stable";
    ASSERT_EQ(t.revokeExact(name, SO::Read, ST::Topic, "orders"), 0);
    EXPECT_EQ(t.r.heartbeatAt(s, "cg", p.m1, p.gen), kUnknownMember) << "removed from the group by the topic revoke";
    EXPECT_PRED1(rebalanced, t.r.heartbeatAt(s2, "cg", p.m2, p.gen)) << "the group rebalances at once";
    // The other member takes the freed partitions: it re-joins alone and is Stable (a lone member holds every
    // partition of orders) - the assignment itself is not visible to this wire rig, the lone Stable member is.
    const auto again = t.r.c().joinGroup(t.ch(), AuthRig::stream(), s2, "cg", p.m2, {t.r.orders});
    ASSERT_TRUE(again.has_value());
    ASSERT_EQ(again->errorCode, 0);
    EXPECT_GT(again->generation, p.gen);
    int hb = -1;
    for (int i = 0; i < 200 && hb != 0; ++i) hb = t.r.heartbeatAt(s2, "cg", p.m2, again->generation);
    EXPECT_EQ(hb, 0) << "the remaining member is Stable alone";
}

TEST(AuthLiveChangeRegression, RevokedTopicReadOfAnotherTopicKeepsTheMemberRegression) {
    Live t("livegroupother");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders"),
                                   exact(SO::Read, ST::Topic, "secret")}, &name);
    const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
    const Pair p = joinPair(t, s, s2, {t.r.orders});
    ASSERT_GT(p.gen, 0);
    ASSERT_EQ(t.revokeExact(name, SO::Read, ST::Topic, "secret"), 0);   // a topic the member never subscribed to
    EXPECT_EQ(t.r.heartbeatAt(s, "cg", p.m1, p.gen), 0) << "the member stays";
    EXPECT_EQ(t.r.heartbeatAt(s2, "cg", p.m2, p.gen), 0) << "no rebalance";
}

TEST(AuthLiveChangeRegression, DisabledUserLeavesItsGroupsAtOnceRegression) {
    Live t("livegroupdisable");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
    const Pair p = joinPair(t, s, s2, {t.r.orders});
    ASSERT_GT(p.gen, 0);
    ASSERT_EQ(t.alter(name, pc::UserChangeKind::Disable), 0);
    EXPECT_EQ(t.errorOn(0, s), kRevoked);
    EXPECT_PRED1(rebalanced, t.r.heartbeatAt(s2, "cg", p.m2, p.gen))
        << "the revoked session's member left at once (no heartbeat timeout): the group rebalances";
}

TEST(AuthLiveChangeRegression, DropUserLeavesItsGroupsAtOnceRegression) {
    Live t("livegroupdrop");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
    const Pair p = joinPair(t, s, s2, {t.r.orders});
    ASSERT_GT(p.gen, 0);
    ASSERT_EQ(t.dropUser(name), 0);
    EXPECT_EQ(t.errorOn(0, s), kRevoked);
    EXPECT_PRED1(rebalanced, t.r.heartbeatAt(s2, "cg", p.m2, p.gen))
        << "the dropped user's member left at once (no heartbeat timeout): the group rebalances";
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
        ASSERT_EQ(t.r.produceDenied(s, t.r.orders), 45) << "REVOKE then, with zero delay, a produce: refused (iteration " << i << ")";
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
        if (!t.r.c().produce(t.b().workerIngressChannel(), AuthRig::stream(), sid, corr, topic, 0, 1, makeCharBatch(1)))
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
    if (!t.r.c().produce(t.b().workerIngressChannel(), AuthRig::stream(), sid, corr, topic, 0, 1, makeCharBatch(1)))
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
    // Issue #273 item 1: 100 more rounds with no hold - the natural window between the OK and the worker installing the
    // bind.  Every OK is followed at once by the refused (or accepted) produce, never an append after a revoke's OK.
    for (int round = 0; round < 100; ++round) {
        ASSERT_EQ(t.revokeExact(writer, SO::Write, ST::Topic, "orders"), 0) << "unheld round " << round;
        const int64_t leo = leoOf(t, t.r.orders);
        ASSERT_EQ(shardProduceOnce(t, ws, t.r.orders), 45) << "unheld round " << round << ": refused at the revoke's OK";
        ASSERT_EQ(leoOf(t, t.r.orders), leo) << "unheld round " << round << ": appended after the revoke's OK";
        ASSERT_EQ(t.grantExact(writer, SO::Write, ST::Topic, "orders"), 0) << "unheld round " << round;
        ASSERT_EQ(shardProduceOnce(t, ws, t.r.orders), 0) << "unheld round " << round << ": accepted at the grant's OK";
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

// Issue #206: a halted broker ends its live sessions with the retriable AuthUnavailable (corr 0), never SessionRevoked,
// and binds each one to nothing first (#196): a direct produce the client sends after its 48 is refused by the worker
// and nothing is appended - the halt stays fail-closed.  (No lifecycle hold here: unlike a DDL, a halt has no OK that
// waits for the workers' enforcement fence, so a held ring would delay the bind itself.)
TEST(AuthLiveChangeRegression, AHaltedBrokersWorkersRefuseTheEndedSessionsProduceRegression) {
    Live t("shardhalt", shardOptions());
    std::string writer;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
    // A malformed halt frame halts the mirror (#203) - the cheapest real halt, through the broker's own fail-closed path.
    t.b().node().injectSecurityFrameOnControlForTest(static_cast<uint16_t>(kA2cSecurityHalted), std::vector<uint8_t>{9});
    EXPECT_EQ(t.errorOn(0, ws), static_cast<int>(pc::ErrorCode::AuthUnavailable)) << "ended retriably, never revoked";
    const int64_t leo = leoOf(t, t.r.orders);
    EXPECT_NE(shardProduceOnce(t, ws, t.r.orders, 2), 0) << "the worker refuses the ended session's produce";
    EXPECT_EQ(leoOf(t, t.r.orders), leo) << "nothing appended after the halt";
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
    EXPECT_EQ(t.r.subscribeDenied(rs, t.r.orders), 45) << "a new Subscribe is refused";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Adversarial review of PR #459 (issues #109 / #196 / #110 / #120): break the ordering of a subscription's end with the
// worker's pushes, and the D24 OK on the shard path - a revoke racing the Subscribe, a backlog still being pushed, a
// revoked session (46) instead of a revoked READ (45), many subscriptions ended by one event.
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
/// `batches` x `perBatch` records produced to `topic` by `ws` (gateway produce, acked).
void backlog(Live& t, int64_t ws, int32_t topic, int batches, int perBatch) {
    for (int i = 0; i < batches; ++i) {
        const int64_t corr = t.r.c().nextCorrelationId();
        ASSERT_TRUE(t.r.c().produce(t.ch(), AuthRig::stream(), ws, corr, topic, 0, 1, makeCharBatch(perBatch)));
        const auto a = t.r.c().awaitProduceResponseUntil(corr, AuthRig::dl());
        ASSERT_TRUE(a.has_value() && a->errorCode == 0) << "backlog batch " << i;
    }
}
uint64_t firstErrorSeq(Live& t, int64_t corr, int64_t sid, int code) {
    for (const auto& e : t.r.c().errors()) if (e.correlationId == corr && e.sessionId == sid && e.code == code) return e.seq;
    return 0;
}
int errorsOn(Live& t, int64_t corr, int64_t sid) {
    int n = 0;
    for (const auto& e : t.r.c().errors()) n += e.correlationId == corr && e.sessionId == sid ? 1 : 0;
    return n;
}
std::size_t recordsAfter(Live& t, int64_t subId, uint64_t seq) {
    std::size_t n = 0;
    for (const auto& p : t.r.c().records()) n += p.subscriptionId == subId && p.seq > seq ? 1 : 0;
    return n;
}
}  // namespace

// Ordering: the REVOKE READ arrives while the subscription's own Subscribe still waits in the worker's (held) op ring.
// The Unsubscribe that ends it and the event's fence queue behind it: exactly one 45 (from the worker - never a second
// one from the gateway), nothing pushed after it, and the OK is released.
TEST(AuthLiveChangeAdversarialRegression, ARevokeRacingTheSubscribeOnAHeldOpRingEndsItExactlyOnceAdversarialRegression) {
    Live t("adv109race", shardOptions());
    std::string reader;
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
    holdOpRing(t);
    const int64_t corr = t.r.c().sendSubscribe(t.ch(), AuthRig::stream(), rs, t.r.orders, 0);
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0) << "the OK comes once the worker ran the end";
    EXPECT_EQ(t.errorOn(corr, rs), 45);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(shardProduceOnce(t, ws, t.r.orders), 0);
    t.r.c().pollFor(500ms, [] { return false; });
    EXPECT_EQ(errorsOn(t, corr, rs), 1) << "one terminal answer on the subscription's correlation id";
    const auto subId = t.r.c().subscriptionIdOf(corr);
    if (subId) {
        const uint64_t endSeq = firstErrorSeq(t, corr, rs, 45);
        EXPECT_EQ(recordsAfter(t, *subId, endSeq), 0u) << "Records after the 45";
    }
}

// Ordering under a backlog: the worker is still pushing a long backlog when the READ is revoked (its op ring held so
// the end waits): no Records frame of the subscription follows its 45.
TEST(AuthLiveChangeAdversarialRegression, NoRecordAfterTheEndWhileABacklogIsPushedAdversarialRegression) {
    Live t("adv109backlog", shardOptions());
    std::string reader;
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
    backlog(t, ws, t.r.orders, 60, 1000);
    const auto sub = t.subscribe(rs, t.r.orders);
    ASSERT_GT(sub.id, 0);
    holdOpRing(t);
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0);
    EXPECT_EQ(t.errorOn(sub.corr, rs), 45);
    t.r.c().pollFor(500ms, [] { return false; });
    const uint64_t endSeq = firstErrorSeq(t, sub.corr, rs, 45);
    ASSERT_GT(endSeq, 0u);
    EXPECT_EQ(recordsAfter(t, sub.id, endSeq), 0u) << "Records after the 45";
    EXPECT_EQ(errorsOn(t, sub.corr, rs), 1);
}

// The same bug class through the sibling end: DISABLE revokes the reader's whole session (SessionRevoked, 46, on each
// subscription's correlation id).  With the worker still pushing a backlog and its op ring held, no Records frame of the
// subscription may follow its 46 - the session's end is terminal.
TEST(AuthLiveChangeAdversarialRegression, NoRecordAfterTheSessionRevokedWhileABacklogIsPushedAdversarialRegression) {
    Live t("adv109revoked", shardOptions());
    std::string reader;
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
    backlog(t, ws, t.r.orders, 60, 1000);
    const auto sub = t.subscribe(rs, t.r.orders);
    ASSERT_GT(sub.id, 0);
    holdOpRing(t);
    ASSERT_EQ(t.alter(reader, pc::UserChangeKind::Disable), 0);
    EXPECT_EQ(t.errorOn(sub.corr, rs), kRevoked);
    t.r.c().pollFor(500ms, [] { return false; });
    const uint64_t endSeq = firstErrorSeq(t, sub.corr, rs, kRevoked);
    ASSERT_GT(endSeq, 0u);
    EXPECT_EQ(recordsAfter(t, sub.id, endSeq), 0u) << "Records after the subscription's SessionRevoked";
}

// Resources: one REVOKE ends many worker-pushed subscriptions at once (one session, 32 subscriptions): each gets exactly
// one 45, the OK is released (no hang, no in-doubt answer), and the worker owes nothing afterwards.
TEST(AuthLiveChangeAdversarialRegression, ARevokeEndingManySubscriptionsEndsEachExactlyOnceAdversarialRegression) {
    Live t("adv109many", shardOptions());
    std::string reader;
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
    std::vector<Live::Sub> subs;
    for (int i = 0; i < 32; ++i) {
        subs.push_back(t.subscribe(rs, t.r.orders));
        ASSERT_GT(subs.back().id, 0) << i;
    }
    holdOpRing(t);
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0);
    t.r.c().pollUntil([&] {
        for (const auto& s : subs) if (errorsOn(t, s.corr, rs) == 0) return false;
        return true;
    });
    t.r.c().pollFor(300ms, [] { return false; });
    for (const auto& s : subs) {
        EXPECT_EQ(errorsOn(t, s.corr, rs), 1) << "sub " << s.id;
        EXPECT_EQ(firstErrorSeq(t, s.corr, rs, 45) > 0, true) << "sub " << s.id;
    }
    EXPECT_EQ(t.b().node().onWorkerThreadForTest(0, [](Worker& w) { return static_cast<int>(w.subscriptionEndsOwedForTest()); }), 0);
}

// ── PR #567 adversarial review (#212): every sibling path to "a session's member may no longer read its topic" ──────
// Auth: the topic READ held through a ROLE is lost by revoking it from the role, or by dropping the role - the member
// leaves its group at once like a direct revoke.  Both the default (gateway) and the shard configuration.
TEST(AuthLiveChangeAdversarialRegression, ATopicReadLostThroughARoleEvictsTheGroupMemberAdversarialRegression) {
    for (const bool shard : {false, true})
        for (const bool dropTheRole : {false, true}) {
            SCOPED_TRACE(std::string(shard ? "shard" : "gateway") + (dropTheRole ? " DROP ROLE" : " REVOKE FROM ROLE"));
            Live t(dropTheRole ? "adv212droprole" : "adv212rolerevoke", shard ? shardOptions() : AuthBroker::Options{});
            ASSERT_EQ(t.createRole("readers"), 0);
            ASSERT_EQ(t.grantRoleOp("readers", SO::Read, ST::Topic, SP::Exact, "orders"), 0);
            std::string name;
            const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg")}, &name);
            ASSERT_EQ(t.grantRole(name, "readers"), 0);
            const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
            const Pair p = joinPair(t, s, s2, {t.r.orders});
            ASSERT_GT(p.gen, 0) << "precondition: two live members, Stable (READ on orders through the role)";
            if (dropTheRole) ASSERT_EQ(t.dropRole("readers"), 0);
            else ASSERT_EQ(t.revoke("readers", /*role=*/true, SO::Read, ST::Topic, SP::Exact, "orders"), 0);
            EXPECT_EQ(t.r.heartbeatAt(s, "cg", p.m1, p.gen), kUnknownMember) << "removed by the role change";
            EXPECT_PRED1(rebalanced, t.r.heartbeatAt(s2, "cg", p.m2, p.gen)) << "the group rebalances at once";
            EXPECT_EQ(t.r.joinGroup(s, "cg", {t.r.orders}), 45) << "and a re-join naming the topic is refused";
        }
}

// Auth (no false eviction): a READ grant swapped for a WIDER prefix trips the conservative grant-lost check, but the
// re-check against the new table keeps the member and its group Stable (no rebalance).
TEST(AuthLiveChangeAdversarialRegression, ATopicReadSwappedForAWiderPrefixKeepsTheMemberAdversarialRegression) {
    Live t("adv212widen");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")}, &name);
    const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
    const Pair p = joinPair(t, s, s2, {t.r.orders});
    ASSERT_GT(p.gen, 0);
    ASSERT_EQ(t.b().admin().grant(t.ch(), t.adminSid(), name, false, SO::Read, ST::Topic, SP::Prefix, "ord").errorCode, 0);
    ASSERT_EQ(t.revokeExact(name, SO::Read, ST::Topic, "orders"), 0);   // READ still held through the prefix
    EXPECT_EQ(t.r.heartbeatAt(s, "cg", p.m1, p.gen), 0) << "the member stays";
    EXPECT_EQ(t.r.heartbeatAt(s2, "cg", p.m2, p.gen), 0) << "no rebalance";
}

// Concurrency / scope: a session that is a member of two groups loses READ on the topic of one: only that group's
// member leaves; the member of the other group (another topic) stays Stable at its generation.
TEST(AuthLiveChangeAdversarialRegression, ATopicReadLossEvictsOnlyTheMemberOnThatTopicAdversarialRegression) {
    Live t("adv212twogroups");
    std::string name;
    const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Group, "cg2"),
                                   exact(SO::Read, ST::Topic, "orders"), exact(SO::Read, ST::Topic, "secret")}, &name);
    const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
    const Pair p = joinPair(t, s, s2, {t.r.orders});
    ASSERT_GT(p.gen, 0);
    const auto [other, otherGen] = t.r.joinStable(s, "cg2", {t.r.secret});
    ASSERT_GT(otherGen, 0);
    int hb = -1;
    for (int i = 0; i < 200 && hb != 0; ++i) hb = t.r.heartbeatAt(s, "cg2", other, otherGen);
    ASSERT_EQ(hb, 0) << "precondition: the cg2 member is Stable";
    ASSERT_EQ(t.revokeExact(name, SO::Read, ST::Topic, "orders"), 0);
    EXPECT_EQ(t.r.heartbeatAt(s, "cg", p.m1, p.gen), kUnknownMember) << "the orders member leaves";
    EXPECT_EQ(t.r.heartbeatAt(s, "cg2", other, otherGen), 0) << "the secret member of cg2 stays, no rebalance there";
}

// Auth: TERMINATE SESSIONS (SET PASSWORD ... terminate) is the third revoke path: the member leaves at once.  A plain
// SET PASSWORD (sessions kept) leaves the member in place.
TEST(AuthLiveChangeAdversarialRegression, TerminateSessionsLeavesItsGroupsAtOnceAdversarialRegression) {
    for (const bool terminate : {false, true}) {
        SCOPED_TRACE(terminate ? "terminate" : "keep sessions");
        Live t(terminate ? "adv212terminate" : "adv212setpw");
        std::string name;
        const int64_t s = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")}, &name);
        const int64_t s2 = t.r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
        const Pair p = joinPair(t, s, s2, {t.r.orders});
        ASSERT_GT(p.gen, 0);
        ASSERT_EQ(t.alter(name, pc::UserChangeKind::SetPassword, "another-password-24680", terminate), 0);
        if (terminate) {
            EXPECT_EQ(t.errorOn(0, s), kRevoked);
            EXPECT_PRED1(rebalanced, t.r.heartbeatAt(s2, "cg", p.m2, p.gen)) << "the terminated member left at once";
        } else {
            EXPECT_EQ(t.r.heartbeatAt(s, "cg", p.m1, p.gen), 0) << "the member stays";
            EXPECT_EQ(t.r.heartbeatAt(s2, "cg", p.m2, p.gen), 0) << "no rebalance";
        }
    }
}

// Adversarial review of PR #583 (#138, the whole bug class): every session-ending change - DISABLE, DROP USER, SET
// PASSWORD ... TERMINATE SESSIONS - removes EACH of the user's sessions from the gateway, the changes that keep
// sessions (a REVOKE of one grant, a plain SET PASSWORD) keep both, and another user's session is never touched.  A
// produce sent afterwards on an ended session id (protocol state: a request after the close) appends nothing.
TEST(AuthLiveChangeAdversarialRegression, EverySessionEndingChangeRemovesExactlyItsSessionsAdversarialRegression) {
    Live t("adv138ends");
    const int64_t bystander = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(t.r.produce(bystander, t.r.orders, /*untilOk=*/true), 0);
    struct Kind { const char* what; bool ends; std::function<int(const std::string&)> apply; };
    const std::vector<Kind> kinds = {
        {"REVOKE WRITE", false, [&](const std::string& u) { return t.revokeExact(u, SO::Write, ST::Topic, "orders"); }},
        {"SET PASSWORD", false, [&](const std::string& u) { return t.alter(u, pc::UserChangeKind::SetPassword, "kept-sessions-pw-1"); }},
        {"DISABLE", true, [&](const std::string& u) { return t.alter(u, pc::UserChangeKind::Disable); }},
        {"TERMINATE SESSIONS", true,
         [&](const std::string& u) { return t.alter(u, pc::UserChangeKind::SetPassword, "terminated-pw-12", true); }},
        {"DROP USER", true, [&](const std::string& u) { return t.dropUser(u); }},
    };
    for (const Kind& k : kinds) {
        SCOPED_TRACE(k.what);
        std::string name;
        const int64_t s1 = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &name);
        const int64_t s2 = t.r.login(name);
        ASSERT_EQ(t.r.produce(s1, t.r.orders, /*untilOk=*/true), 0);
        ASSERT_EQ(k.apply(name), 0);
        if (!k.ends) {
            EXPECT_TRUE(t.b().node().gatewayHasSessionForTest(s1)) << "the session is kept";
            EXPECT_TRUE(t.b().node().gatewayHasSessionForTest(s2)) << "the session is kept";
            continue;
        }
        EXPECT_TRUE(t.r.awaitSessionGone(s1)) << "the gateway ended the first session";
        EXPECT_TRUE(t.r.awaitSessionGone(s2)) << "the gateway ended the second session";
        EXPECT_TRUE(t.b().node().gatewayHasSessionForTest(bystander)) << "another user's session is untouched";
        // A produce on the ended id, then an allowed one on the same stream (ordered after it): exactly one record.
        const int64_t before = t.r.logEnd(t.r.orders);
        ASSERT_TRUE(t.r.c().produce(t.ch(), t.r.stream(), s1, t.r.c().nextCorrelationId(), t.r.orders, 0, 1, makeCharBatch(1)));
        ASSERT_EQ(t.r.produce(bystander, t.r.orders), 0);
        EXPECT_EQ(t.r.logEnd(t.r.orders), before + 1) << "the ended session's produce appended nothing";
    }
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// Issue #273 residuals (shard rig: per-worker ingress AND per-worker egress, the default runtime).
// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
namespace {
/// Worker 0's cursors as (session, has its OWN record egress) - read on the worker's thread.
struct WorkerCursor { int64_t session = 0; bool ownEgress = false; };
std::vector<WorkerCursor> workerCursors(Live& t) {
    return t.b().node().onWorkerThreadForTest(0, [](Worker& w) {
        std::vector<WorkerCursor> out;
        for (const auto& s : w.subscriptionsForTest()) out.push_back({s.sessionId, s.egressPub != nullptr});
        return out;
    });
}
/// No cursor on worker 0 pushes on the worker's SHARED dynamic-MDC egress (a cursor without its own record egress).
bool noSharedEgressCursor(Live& t) {
    for (const WorkerCursor& c : workerCursors(t)) if (!c.ownEgress) return false;
    return true;
}
bool workerHasCursorOf(Live& t, int64_t sid) {
    for (const WorkerCursor& c : workerCursors(t)) if (c.session == sid) return true;
    return false;
}
/// How many ErrorResponses with `code` arrived for (`corr`, `sid`).
int errorsWith(Live& t, int64_t corr, int64_t sid, int code) {
    int n = 0;
    for (const auto& e : t.r.c().errors()) n += e.correlationId == corr && e.sessionId == sid && e.code == code ? 1 : 0;
    return n;
}
}  // namespace

// Issue #273 (MDC fallback): a shard client has two ways to a partition - the worker's own ingress / record egress, and
// the gateway's control stream it falls back to (the routed produce; under authentication the shared dynamic-MDC record
// egress fallback is retired, issue #118).  A REVOKE is enforced on EVERY path at its OK: the revoked WRITE is refused
// on the worker ingress AND on the control-stream fallback, nothing is appended on either; a revoked READ ends the
// subscription, and no cursor - before, during or after the end, also for a Subscribe that waited on its record-egress
// resolve - ever pushes on the worker's shared MDC egress.  RED: delete the WRITE check of ClientGateway::handleProduce
// (the control-stream fallback appends after the revoke).
TEST(AuthLiveChangeRegression, RevokeIsEnforcedOnTheMdcFallbackPathRegression) {
    Live t("mdcfallback", shardOptions());
    std::string writer, reader;
    const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writer);
    const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0) << "worker ingress";
    ASSERT_EQ(t.r.produce(ws, t.r.orders), 0) << "the control-stream fallback (gateway-routed produce)";
    const auto sub = t.subscribe(rs, t.r.orders);
    ASSERT_GT(sub.id, 0);
    ASSERT_TRUE(t.awaitPushes(sub.id, 1));
    EXPECT_TRUE(workerHasCursorOf(t, rs)) << "the subscription is served by the worker";
    EXPECT_TRUE(noSharedEgressCursor(t)) << "a cursor pushes on the shared MDC egress under authentication";

    // WRITE revoked: refused on both paths at the OK, nothing appended.
    ASSERT_EQ(t.revokeExact(writer, SO::Write, ST::Topic, "orders"), 0);
    const int64_t leo = leoOf(t, t.r.orders);
    EXPECT_EQ(shardProduceOnce(t, ws, t.r.orders), 45) << "worker ingress after the revoke's OK";
    EXPECT_EQ(t.r.produceDenied(ws, t.r.orders), 45) << "the control-stream fallback after the revoke's OK";
    EXPECT_EQ(leoOf(t, t.r.orders), leo) << "a revoked WRITE was appended through a fallback path";

    // READ revoked: the live subscription ends; no shared-egress cursor at any point.
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0);
    EXPECT_EQ(t.errorOn(sub.corr, rs), 45);
    EXPECT_FALSE(workerHasCursorOf(t, rs)) << "the worker still holds the revoked reader's cursor after the OK";
    EXPECT_TRUE(noSharedEgressCursor(t));

    // The deferred window: READ granted back, a fresh session's Subscribe parked on its record-egress resolve, READ
    // revoked while it waits.  Released, it is answered 45/46 and never reaches the worker - not even on the MDC egress.
    ASSERT_EQ(t.grantExact(reader, SO::Read, ST::Topic, "orders"), 0);
    const int64_t rs2 = t.r.login(reader);
    t.b().node().holdRecordEgressResolveForTest(true);
    const int64_t corr2 = t.r.c().sendSubscribe(t.ch(), AuthRig::stream(), rs2, t.r.orders, 0);
    ASSERT_NE(corr2, 0);
    ASSERT_EQ(t.revokeExact(reader, SO::Read, ST::Topic, "orders"), 0);
    t.b().node().holdRecordEgressResolveForTest(false);
    const auto code = t.r.c().awaitSubscribeCode(corr2, AuthRig::dl());
    ASSERT_TRUE(code.has_value()) << "the held Subscribe was answered";
    EXPECT_TRUE(*code == 45 || *code == 46) << "the held Subscribe answered " << *code;
    EXPECT_FALSE(workerHasCursorOf(t, rs2)) << "the revoked reader's held Subscribe reached the worker";
    EXPECT_TRUE(noSharedEgressCursor(t));
    // Re-granted WRITE: both paths open again, without a new login (the revoke was not a session end).
    ASSERT_EQ(t.grantExact(writer, SO::Write, ST::Topic, "orders"), 0);
    EXPECT_EQ(shardProduceOnce(t, ws, t.r.orders), 0);
    EXPECT_EQ(t.r.produce(ws, t.r.orders), 0);
}

// Issue #273 / #136 (46 under back-pressure): a session ended by a live change (DISABLE) while the worker that serves it
// is back-pressured - its lifecycle ring not drained, its op ring not drained with a backlog still being pushed, or the
// session's record-egress resolve held - still gets its SessionRevoked (46): the session-level frame exactly once and,
// on a live subscription, exactly one 46 on the subscription's correlation id with no Records after it.  The DDL's OK is
// released (no hang) and the session is gone.  (44 - AuthenticationFailed - is a login / pre-login answer the control
// thread sends synchronously; no worker ring or egress resolve sits on its path, so there is no back-pressured 44.)
// RED: drop the session-level SessionRevoked frame of ClientGateway's session end (the 46 never arrives).
TEST(AuthLiveChangeRegression, EndedSessionAnswerSurvivesBackPressureRegression) {
    enum class Pressure { LifecycleRing, OpRingWithBacklog, RecordEgressResolve };
    for (const Pressure p : {Pressure::LifecycleRing, Pressure::OpRingWithBacklog, Pressure::RecordEgressResolve}) {
        const char* what = p == Pressure::LifecycleRing ? "lifecycle ring held"
                         : p == Pressure::OpRingWithBacklog ? "op ring held with a backlog" : "record-egress resolve held";
        SCOPED_TRACE(what);
        Live t(p == Pressure::LifecycleRing ? "bp46life" : p == Pressure::OpRingWithBacklog ? "bp46op" : "bp46egress",
               shardOptions());
        std::string reader;
        const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
        const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
        ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
        if (p == Pressure::OpRingWithBacklog) backlog(t, ws, t.r.orders, 60, 1000);
        int64_t corr = 0, subId = 0;
        if (p == Pressure::RecordEgressResolve) {
            t.b().node().holdRecordEgressResolveForTest(true);
            corr = t.r.c().sendSubscribe(t.ch(), AuthRig::stream(), rs, t.r.orders, 0);
            ASSERT_NE(corr, 0);
        } else {
            const auto sub = t.subscribe(rs, t.r.orders);
            ASSERT_GT(sub.id, 0);
            corr = sub.corr;
            subId = sub.id;
            if (p == Pressure::LifecycleRing) holdLifecycle(t); else holdOpRing(t);
        }
        ASSERT_EQ(t.alter(reader, pc::UserChangeKind::Disable), 0) << "the DISABLE's OK is released (no hang)";
        if (p == Pressure::RecordEgressResolve) t.b().node().holdRecordEgressResolveForTest(false);
        EXPECT_EQ(t.errorOn(0, rs), kRevoked) << "the session-level SessionRevoked arrived";
        EXPECT_TRUE(t.r.awaitSessionGone(rs)) << "the gateway ended the session";
        t.r.c().pollFor(300ms, [] { return false; });   // anything still owed lands now
        EXPECT_EQ(errorsWith(t, 0, rs, kRevoked), 1) << "exactly one session-level 46";
        if (subId != 0) {
            EXPECT_EQ(errorsWith(t, corr, rs, kRevoked), 1) << "exactly one 46 on the subscription";
            const uint64_t endSeq = firstErrorSeq(t, corr, rs, kRevoked);
            ASSERT_GT(endSeq, 0u);
            EXPECT_EQ(recordsAfter(t, subId, endSeq), 0u) << "Records after the subscription's 46";
        } else {
            // The parked Subscribe never reached a worker: it is answered 46 (or not at all - the session-level 46 above
            // ended it), never OK.
            const auto code = t.r.c().awaitSubscribeCode(corr, AuthRig::dl(1));
            EXPECT_TRUE(!code.has_value() || *code == kRevoked || *code == 45) << "the parked Subscribe answered " << *code;
        }
        EXPECT_EQ(t.b().node().onWorkerThreadForTest(0, [](Worker& w) { return static_cast<int>(w.subscriptionEndsOwedForTest()); }),
                  0) << "the worker still owes an end";
    }
}

// Issue #273 (session spoof over the wire on the shard worker ingress): B (a logged-in writer of its own) sends Produce
// frames straight to the worker's ingress naming session ids that are not its own: A's id after A's session was ended by
// a DISABLE, A's id after A disconnected, an id never issued, and 0.  Each is refused - never acked OK - and nothing is
// appended; B's own produce under its own id still works.  (A LIVE session's id: on this rig's plaintext test transport
// the session id is the frame's only identity, so a live id is a bearer token by design (#10: an authenticating broker
// serves TLS only); on TLS a frame naming another session's id never opens -
// AuthSessionBindingRegression.APlaintextProduceNamingAnEncryptedSessionIsNeverAppendedAdversarialRegression.)
// RED: keep a session's worker grant slot bound after its end (the ended id's produce is appended).
TEST(AuthLiveChangeRegression, ShardWorkerIngressRefusesASpoofedSessionIdRegression) {
    Live t("shardspoof", shardOptions());
    std::string aName, a2Name;
    const int64_t a = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &aName);
    const int64_t a2 = t.r.newUser({exact(SO::Write, ST::Topic, "orders")}, &a2Name);
    const int64_t b = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(shardProduceCode(t, a, t.r.orders, 200), 0);
    ASSERT_EQ(shardProduceCode(t, a2, t.r.orders, 200), 0);
    ASSERT_EQ(shardProduceCode(t, b, t.r.orders, 200), 0);
    ASSERT_EQ(t.alter(aName, pc::UserChangeKind::Disable), 0);          // A's session ended by a live change
    ASSERT_TRUE(t.r.awaitSessionGone(a));
    ASSERT_TRUE(t.r.c().disconnect(t.ch(), AuthRig::stream(), a2));     // A2's session ended by its own Disconnect
    ASSERT_TRUE(t.r.awaitSessionGone(a2));
    for (const int64_t spoofed : {a, a2, b + 1'000, int64_t{0}}) {
        SCOPED_TRACE("spoofed session id " + std::to_string(spoofed));
        const int64_t leo = leoOf(t, t.r.orders);
        EXPECT_NE(shardProduceOnce(t, spoofed, t.r.orders, 2), 0) << "a produce naming a session id that is not B's";
        EXPECT_EQ(leoOf(t, t.r.orders), leo) << "the spoofed produce was appended";
    }
    EXPECT_EQ(shardProduceOnce(t, b, t.r.orders), 0) << "B's own session is untouched";
}

// ═════════════════════════════════════════════════════════════════════════════════════════════════════════════════════
// PR #604 adversarial review (session spoofing on the worker ingress; input bounds).  The worker finds a session's grant
// slot by an open-addressed probe on the session id (SessionAckSink: Fibonacci hash, linear probe).  B sends worker-ingress
// Produce frames naming ids that are NOT a live session but land on a live session's probe slot - an id that hashes to
// B's own slot, one that hashes to a fully granted bystander's slot - and the edge ids -1, INT64_MIN and INT64_MAX.  Each
// is refused (never acked OK) and nothing is appended; B and the bystander keep producing under their own ids.
// RED: let SessionAckSink::find_ match a slot by its hash instead of the full session id (the colliding id inherits the
// slot's grants and is appended).
namespace {
/// SessionAckSink's slot hash (kMaxSessions = 4096: 12 bits), reproduced to build colliding ids.
std::size_t sinkSlotOf(int64_t sid) { return static_cast<std::size_t>((static_cast<uint64_t>(sid) * 0x9E3779B97F4A7C15ULL) >> (64 - 12)); }
/// The smallest id above `sid` + 1 that hashes to `sid`'s slot (bounded search).
int64_t collidingId(int64_t sid) {
    for (int64_t x = sid + 2; x < sid + (int64_t{1} << 24); ++x)
        if (sinkSlotOf(x) == sinkSlotOf(sid)) return x;
    return 0;
}
}  // namespace

TEST(AuthLiveChangeAdversarialRegression, AWorkerIngressIdThatCollidesWithALiveSlotIsRefusedAdversarialRegression) {
    Live t("advspoofhash", shardOptions());
    const int64_t b = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t bystander = t.r.newUser({prefix(SO::Write, ST::Topic, "")});
    ASSERT_EQ(shardProduceCode(t, b, t.r.orders, 200), 0);
    ASSERT_EQ(shardProduceCode(t, bystander, t.r.orders, 200), 0);
    const int64_t onB = collidingId(b), onBystander = collidingId(bystander);
    ASSERT_NE(onB, 0);
    ASSERT_NE(onBystander, 0);
    ASSERT_NE(onB, bystander);
    ASSERT_NE(onBystander, b);
    for (const int64_t spoofed : {onB, onBystander, int64_t{-1}, std::numeric_limits<int64_t>::min(),
                                  std::numeric_limits<int64_t>::max()}) {
        SCOPED_TRACE("spoofed session id " + std::to_string(spoofed));
        const int64_t leo = leoOf(t, t.r.orders);
        EXPECT_NE(shardProduceOnce(t, spoofed, t.r.orders, 2), 0) << "a worker-ingress produce naming a non-session id";
        EXPECT_EQ(leoOf(t, t.r.orders), leo) << "the spoofed produce was appended";
    }
    EXPECT_EQ(shardProduceOnce(t, b, t.r.orders), 0) << "B's own session is untouched";
    EXPECT_EQ(shardProduceOnce(t, bystander, t.r.orders), 0) << "the bystander's session is untouched";
}

// PR #604 adversarial review (46 under back-pressure, the sibling session ends): EndedSessionAnswerSurvivesBackPressure
// covers DISABLE; a DROP USER and a password change that terminates sessions end the session through the same gateway
// end.  With the worker still pushing a backlog and its op ring held, each one: the DDL's OK is released (no hang), the
// session-level SessionRevoked (46) arrives exactly once, the live subscription gets exactly one 46 and no Records after
// it, and the worker owes nothing afterwards.  RED: drop the session-level SessionRevoked frame of ClientGateway's
// session end.
TEST(AuthLiveChangeAdversarialRegression, EverySessionEndingChangeUnderAHeldOpRingAnswersExactlyOnceAdversarialRegression) {
    enum class End { DropUser, PasswordTerminate };
    for (const End e : {End::DropUser, End::PasswordTerminate}) {
        SCOPED_TRACE(e == End::DropUser ? "DROP USER" : "ALTER PASSWORD ... TERMINATE");
        Live t(e == End::DropUser ? "advbp46drop" : "advbp46pw", shardOptions());
        std::string reader;
        const int64_t rs = t.r.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
        const int64_t ws = t.r.newUser({exact(SO::Write, ST::Topic, "orders")});
        ASSERT_EQ(shardProduceCode(t, ws, t.r.orders, 200), 0);
        backlog(t, ws, t.r.orders, 60, 1000);
        const auto sub = t.subscribe(rs, t.r.orders);
        ASSERT_GT(sub.id, 0);
        holdOpRing(t);
        const int ok = e == End::DropUser ? t.dropUser(reader)
                                          : t.alter(reader, pc::UserChangeKind::SetPassword, "a-new-password-98765", /*terminate=*/true);
        ASSERT_EQ(ok, 0) << "the session-ending change's OK is released (no hang)";
        EXPECT_EQ(t.errorOn(0, rs), kRevoked) << "the session-level SessionRevoked arrived";
        EXPECT_TRUE(t.r.awaitSessionGone(rs)) << "the gateway ended the session";
        t.r.c().pollFor(500ms, [] { return false; });   // anything still owed lands now
        EXPECT_EQ(errorsWith(t, 0, rs, kRevoked), 1) << "exactly one session-level 46";
        EXPECT_EQ(errorsWith(t, sub.corr, rs, kRevoked), 1) << "exactly one 46 on the subscription";
        const uint64_t endSeq = firstErrorSeq(t, sub.corr, rs, kRevoked);
        ASSERT_GT(endSeq, 0u);
        EXPECT_EQ(recordsAfter(t, sub.id, endSeq), 0u) << "Records after the subscription's 46";
        EXPECT_EQ(t.b().node().onWorkerThreadForTest(0, [](Worker& w) { return static_cast<int>(w.subscriptionEndsOwedForTest()); }),
                  0) << "the worker still owes an end";
    }
}
