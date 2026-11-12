// AuthDdlAnswerStallRegressionTest.cpp - authentication Task 18: the seeded stress (seed 0) found two broker bugs.
//  1. The security DDL answers stalled for good: the control thread spun in AssignmentMap::get (tombstone exhaustion after
//     topic create/drop churn); its regression lives in libs/metadata/tests/PartitionAssignmentTest.cpp
//     (AssignmentMapRegression).  The first two tests here are the end-to-end shape of the racing DDLs that exposed it.
//  2. A revoke-ended subscription whose Subscribe still waited for its session's record-egress pub became a zombie worker
//     cursor (it pushed after the revoke, and again after a re-grant).  The deterministic gateway-level regression is
//     RecordEgressLifetimeRegression.UnsubscribeCancelsASubscribeStillAwaitingItsEgressPub; the tests below are its
//     end-to-end shape.
#include "support/AuthRig.h"

#include <algorithm>
#include <random>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;
using SP = rune::protocol::SecurityPatternKind;
using namespace std::chrono_literals;

namespace {
struct Async {
    AuthRig& r;
    int64_t send(pc::SecurityDdl m, const char* stmt) {
        auto& raw = r.broker().admin().raw();
        m.sessionId = r.broker().adminSession();
        m.correlationId = raw.nextCorrelationId();
        m.ext.clientName = "stall-test"; m.ext.statement = stmt;
        m.ext.requestId = {0x57A1'1000'0000'0000ull, ++n};
        std::vector<uint8_t> b(m.encodedSize());
        b.resize(m.encode(b.data()));
        return raw.send(r.ch(), AuthWireClient::kClientStream, b.data(), b.size()) ? m.correlationId : 0;
    }
    int64_t revokeWrite(const std::string& who, const std::string& topic) {
        pc::SecurityDdl m; m.kind = pc::SecurityOpKind::Revoke; m.name = who; m.granteeKind = 0;
        m.op = static_cast<uint8_t>(SO::Write); m.resourceType = static_cast<uint8_t>(ST::Topic);
        m.patternKind = static_cast<uint8_t>(SP::Exact); m.pattern = topic;
        return send(m, "REVOKE ...;");
    }
    int64_t grantWrite(const std::string& who, const std::string& topic) {
        pc::SecurityDdl m; m.kind = pc::SecurityOpKind::Grant; m.name = who; m.granteeKind = 0;
        m.op = static_cast<uint8_t>(SO::Write); m.resourceType = static_cast<uint8_t>(ST::Topic);
        m.patternKind = static_cast<uint8_t>(SP::Exact); m.pattern = topic;
        return send(m, "GRANT ...;");
    }
    int64_t createRole(const std::string& name) {
        pc::SecurityDdl m; m.kind = pc::SecurityOpKind::CreateRole; m.name = name;
        return send(m, "CREATE ROLE ...;");
    }
    int64_t grantReadToRole(const std::string& role, const std::string& topic) {
        pc::SecurityDdl m; m.kind = pc::SecurityOpKind::Grant; m.name = role; m.granteeKind = 1;
        m.op = static_cast<uint8_t>(SO::Read); m.resourceType = static_cast<uint8_t>(ST::Topic);
        m.patternKind = static_cast<uint8_t>(SP::Exact); m.pattern = topic;
        return send(m, "GRANT ...;");
    }
    int64_t defineTopic(const std::string& name) {
        auto& adm = r.broker();
        return adm.admin().raw().sendDefineTopic(r.ch(), AuthWireClient::kClientStream, adm.adminSession(), name, 1,
                                                 authRigCharBlob(), 0, {}, 0, authRigTopicConfig());
    }
    int64_t alter(const std::string& who, pc::UserChangeKind k) {
        pc::SecurityDdl m; m.kind = pc::SecurityOpKind::AlterUser; m.name = who; m.change = k;
        return send(m, "ALTER USER ...;");
    }
    /// The answer code of `corr` within 5 s, or kNoAnswer.
    int answer(int64_t corr) {
        auto& raw = r.broker().admin().raw();
        const auto a = raw.awaitAnyDdlAnswer(corr, AuthRig::dl(5));
        if (!a) return kNoAnswer;
        if (a->responseTemplate == static_cast<uint16_t>(pc::TemplateId::DdlResponse))
            if (auto d = raw.awaitDdl(corr, AuthRig::dl(5))) return d->errorCode;
        return a->code;
    }
    uint64_t n = 0;
};
}  // namespace

TEST(AuthDdlAnswerStallRegression, RevokeWriteThenDisableOfTheSameUserBothAnswer) {
    AuthBroker::Options o; o.shard = true;
    AuthRig rig("stall1", o);
    Async a{rig};
    for (int round = 0; round < 20; ++round) {
        std::string name;
        std::vector<int64_t> sids;
        sids.push_back(rig.newUser({exact(SO::Write, ST::Topic, "orders")}, &name));
        for (int i = 0; i < 2; ++i) sids.push_back(rig.login(name));
        const int64_t c1 = a.revokeWrite(name, "orders");
        const int64_t c2 = a.alter(name, pc::UserChangeKind::Disable);
        ASSERT_NE(c1, 0); ASSERT_NE(c2, 0);
        EXPECT_EQ(a.answer(c1), 0) << "REVOKE answered (round " << round << ")";
        EXPECT_EQ(a.answer(c2), 0) << "DISABLE answered (round " << round << ")";
    }
}

TEST(AuthDdlAnswerStallRegression, DisableThenEnableThenGrantAllAnswer) {
    AuthBroker::Options o; o.shard = true;
    AuthRig rig("stall2", o);
    Async a{rig};
    std::string name;
    rig.newUser({exact(SO::Write, ST::Topic, "orders")}, &name);
    for (int round = 0; round < 10; ++round) {
        const int64_t c1 = a.alter(name, pc::UserChangeKind::Disable);
        const int64_t c2 = a.revokeWrite(name, "orders");
        const int64_t c3 = a.alter(name, pc::UserChangeKind::Enable);
        const int64_t c4 = a.grantWrite(name, "orders");
        EXPECT_EQ(a.answer(c1), 0); EXPECT_EQ(a.answer(c2), 0); EXPECT_EQ(a.answer(c3), 0); EXPECT_EQ(a.answer(c4), 0);
        rig.login(name);   // a fresh session for the next round
    }
}

// Issues #218 / #67: the staleness fence stales only the principals a change could affect.  An admin that pipelines a
// grant to a ROLE (the admin is no member) followed by more DDL about other users and roles and a catalog CREATE sees
// every answer OK, never the retriable NotLeader.
TEST(AuthDdlAnswerStallRegression, PipelinedRoleDdlNeverRetriesANonMemberAdminRegression) {
    AuthBroker::Options o; o.shard = true;
    AuthRig rig("stallrole", o);
    Async a{rig};
    std::string other;
    rig.newUser({exact(SO::Write, ST::Topic, "orders")}, &other);
    for (int round = 0; round < 10; ++round) {
        const std::string role = "pipe_role_" + std::to_string(round);
        ASSERT_EQ(a.answer(a.createRole(role)), 0);
        const int64_t c1 = a.grantReadToRole(role, "orders");
        const int64_t c2 = a.alter(other, pc::UserChangeKind::Disable);
        const int64_t c3 = a.createRole(role + "_b");
        const int64_t c4 = a.alter(other, pc::UserChangeKind::Enable);
        ASSERT_NE(c1, 0); ASSERT_NE(c2, 0); ASSERT_NE(c3, 0); ASSERT_NE(c4, 0);
        const int64_t c5 = a.defineTopic("pipe_topic_" + std::to_string(round));
        ASSERT_NE(c5, 0);
        EXPECT_EQ(a.answer(c5), 0) << "a catalog CREATE pipelined behind role DDL (round " << round << ")";
        EXPECT_EQ(a.answer(c1), 0) << "GRANT TO ROLE (round " << round << ")";
        EXPECT_EQ(a.answer(c2), 0) << "DISABLE (round " << round << ")";
        EXPECT_EQ(a.answer(c3), 0) << "CREATE ROLE (round " << round << ")";
        EXPECT_EQ(a.answer(c4), 0) << "ENABLE (round " << round << ")";
    }
}

// Seed 0 of the stress, second finding: a subscription ended by a revoke (ErrorResponse 45) must stay ended - a later
// re-grant of READ must not make the worker push to it again.
namespace {
void endedSubscriptionStaysEnded(bool shard, const char* tag) {
    AuthBroker::Options o; o.shard = shard;
    AuthRig rig(tag, o);
    std::string reader;
    const int64_t rs = rig.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const int64_t ws = rig.newUser({exact(SO::Write, ST::Topic, "orders")});
    auto& c = rig.c();
    const int64_t corr = c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0);
    ASSERT_EQ(c.awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0);
    const int64_t subId = c.subscriptionIdOf(corr).value_or(0);
    ASSERT_GT(subId, 0);
    auto pushes = [&] { std::size_t n = 0; for (const auto& p : c.records()) n += p.subscriptionId == subId ? 1 : 0; return n; };
    ASSERT_EQ(rig.produce(ws, rig.orders, true), 0);
    ASSERT_TRUE(c.pollUntil([&] { return pushes() >= 1; }));
    auto& adm = rig.broker().admin();
    ASSERT_EQ(adm.revoke(rig.ch(), rig.broker().adminSession(), reader, false, SO::Read, ST::Topic, SP::Exact, "orders").errorCode, 0);
    ASSERT_TRUE(c.pollUntil([&] { for (const auto& e : c.errors()) if (e.correlationId == corr) return true; return false; }));
    c.pollFor(300ms, [] { return false; });
    const std::size_t atEnd = pushes();
    ASSERT_EQ(adm.grant(rig.ch(), rig.broker().adminSession(), reader, false, SO::Read, ST::Topic, SP::Exact, "orders").errorCode, 0);
    for (int i = 0; i < 5; ++i) ASSERT_EQ(rig.produce(ws, rig.orders), 0);
    c.pollFor(1000ms, [] { return false; });
    EXPECT_EQ(pushes(), atEnd) << "an ended subscription received records after a re-grant of READ";
    // the same session subscribes the same topic again: the NEW subscription delivers, the ended one stays silent
    const int64_t corr2 = c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0);
    ASSERT_EQ(c.awaitSubscribeCode(corr2, AuthRig::dl()).value_or(-1), 0);
    const int64_t sub2 = c.subscriptionIdOf(corr2).value_or(0);
    ASSERT_NE(sub2, subId);
    for (int i = 0; i < 5; ++i) ASSERT_EQ(rig.produce(ws, rig.orders), 0);
    c.pollFor(1000ms, [] { return false; });
    std::size_t n2 = 0;
    for (const auto& p : c.records()) n2 += p.subscriptionId == sub2 ? 1 : 0;
    EXPECT_GT(n2, 0u) << "the new subscription delivers";
    EXPECT_EQ(pushes(), atEnd) << "the ended subscription stayed silent after the same session re-subscribed";
}
}  // namespace

TEST(AuthDdlAnswerStallRegression, EndedSubscriptionStaysEndedAfterReGrantShard) { endedSubscriptionStaysEnded(true, "endshard"); }
TEST(AuthDdlAnswerStallRegression, EndedSubscriptionStaysEndedAfterReGrantGateway) { endedSubscriptionStaysEnded(false, "endgw"); }

namespace {
// The revoke races the Subscribe that is still being bound by the worker.
void subscribeRacingRevokeStaysEnded(bool shard, const char* tag) {
    AuthBroker::Options o; o.shard = shard;
    AuthRig rig(tag, o);
    Async a{rig};
    auto& c = rig.c();
    auto& adm = rig.broker().admin();
    const int64_t ws = rig.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(rig.produce(ws, rig.orders, true), 0);
    int subscribedRounds = 0;
    int swept45 = 0, sweptOk = 0;   // the swept rounds (not the two deterministic modes): answered 45/46 / answered OK
    std::string codes;              // one "<round>:<mode>:<code>" entry per round, printed once at the end
    std::vector<int64_t> okIds;
    enum class Mode { SubscribeThenRevoke, SubscribeOkThenRevoke, RevokeThenSubscribe, RevokeAnsweredThenSubscribe };
    // Polls `c` until a steady_clock deadline (pollFor takes whole milliseconds; a zero gap polls once).
    auto pollForMicros = [&](std::chrono::microseconds gap) {
        const auto deadline = std::chrono::steady_clock::now() + gap;
        do { c.pollFor(0ms, [] { return false; }); } while (std::chrono::steady_clock::now() < deadline);
    };
    auto runRound = [&](int round, Mode mode, std::chrono::microseconds gap) {
        std::string reader;
        const int64_t rs = rig.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
        pc::SecurityDdl m; m.kind = pc::SecurityOpKind::Revoke; m.name = reader; m.granteeKind = 0;
        m.op = static_cast<uint8_t>(SO::Read); m.resourceType = static_cast<uint8_t>(ST::Topic);
        m.patternKind = static_cast<uint8_t>(SP::Exact); m.pattern = "orders";
        int64_t corr = 0;
        if (mode == Mode::RevokeThenSubscribe || mode == Mode::RevokeAnsweredThenSubscribe) {
            // The revoke goes first: the Subscribe races the revoke's propagation, a window BEFORE "Subscribe sent".
            const int64_t rc = a.send(m, "REVOKE ...;");
            ASSERT_NE(rc, 0);
            if (mode == Mode::RevokeThenSubscribe) {
                pollForMicros(gap);
                corr = c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0);
                ASSERT_EQ(a.answer(rc), 0);
            } else {
                ASSERT_EQ(a.answer(rc), 0);
                c.pollFor(300ms, [] { return false; });   // the denial has reached the gateway and the workers
                corr = c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0);
            }
        } else {
            corr = c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0);
            // The Subscribe OK waits for the session's record-egress pub (~20 ms to resolve) in shard mode, longer than
            // a revoke round trip: sweep the revoke across that window, and let every third round wait for the OK so
            // the OK-then-revoked path is always taken, whatever the timing.
            if (mode == Mode::SubscribeOkThenRevoke)
                ASSERT_TRUE(c.pollUntil([&] { return c.subscriptionIdOf(corr).has_value(); }));
            else c.pollFor(std::chrono::duration_cast<std::chrono::milliseconds>(gap), [] { return false; });
            const int64_t rc = a.send(m, "REVOKE ...;");
            ASSERT_EQ(a.answer(rc), 0);
        }
        const auto code = c.awaitSubscribeCode(corr, AuthRig::dl());
        ASSERT_TRUE(code.has_value());
        const bool swept = mode == Mode::SubscribeThenRevoke || mode == Mode::RevokeThenSubscribe;
        if (swept) { if (*code == 45 || *code == 46) ++swept45; else if (*code == 0) ++sweptOk; }
        codes += " " + std::to_string(round) + ":" + std::to_string(static_cast<int>(mode)) + ":"
                 + std::to_string(*code);
        ASSERT_TRUE(*code == 0 || *code == 45 || *code == 46)
            << "round " << round << ": unexpected Subscribe answer " << *code;
        if (mode == Mode::RevokeAnsweredThenSubscribe)
            ASSERT_TRUE(*code == 45 || *code == 46) << "round " << round
                << ": a Subscribe sent after the revoke was answered, answered " << *code << " (expected 45/46)";
        const int64_t subId = c.subscriptionIdOf(corr).value_or(0);
        c.pollFor(300ms, [] { return false; });
        auto pushes = [&] { std::size_t n = 0; for (const auto& p : c.records()) n += subId != 0 && p.subscriptionId == subId ? 1 : 0; return n; };
        const std::size_t atEnd = pushes();
        ASSERT_EQ(adm.grant(rig.ch(), rig.broker().adminSession(), reader, false, SO::Read, ST::Topic, SP::Exact, "orders").errorCode, 0);
        for (int i = 0; i < 3; ++i) ASSERT_EQ(rig.produce(ws, rig.orders), 0);
        c.pollFor(400ms, [] { return false; });
        if (*code == 0) ++subscribedRounds;
        EXPECT_EQ(pushes(), atEnd) << "round " << round << ": subscribe answered " << *code << ", the sub (id " << subId
                                   << ") delivered after the revoke ended it and a re-grant";
        // A Subscribe answered 45/46 has no id, so pushes() cannot see its zombie cursor: every Record must carry an id
        // some Subscribe OK named.
        if (subId != 0) okIds.push_back(subId);
        const auto zombies = std::count_if(c.records().begin(), c.records().end(), [&](const auto& p) {
            return std::find(okIds.begin(), okIds.end(), p.subscriptionId) == okIds.end();
        });
        EXPECT_EQ(zombies, 0) << "round " << round << ": subscribe answered " << *code
                              << ", a Record carried a subscription id no Subscribe OK named (a zombie cursor)";
    };
    int round = 0;
    for (; round < 15; ++round) {
        runRound(round, round % 3 == 2 ? Mode::SubscribeOkThenRevoke : Mode::SubscribeThenRevoke,
                 std::chrono::milliseconds(round * 4));
        if (::testing::Test::HasFatalFailure()) return;
    }
    // Revoke first: the Subscribe is sent after the REVOKE, 0 .. 8 ms later (the gateway variant answers its Subscribe
    // OK at once, so only a revoke sent before the Subscribe can race it).
    for (const auto gap : {0us, 0us, 250us, 500us, 1000us, 2000us, 4000us, 8000us}) {
        runRound(round++, Mode::RevokeThenSubscribe, gap);
        if (::testing::Test::HasFatalFailure()) return;
    }
    runRound(round++, Mode::RevokeAnsweredThenSubscribe, 0us);
    if (::testing::Test::HasFatalFailure()) return;
    std::cout << "RACECODES " << tag << " (round:mode:code; mode 0 sub-then-revoke, 1 sub-OK-then-revoke, "
              << "2 revoke-then-sub, 3 revoke-answered-then-sub):" << codes
              << " | swept45=" << swept45 << " sweptOk=" << sweptOk << std::endl;
    EXPECT_GT(subscribedRounds, 0) << "at least one round took the path under test (Subscribe OK, then revoked)";
    // The sweep must straddle the race window in this variant: among the swept rounds some Subscribes lost the race
    // (45/46) and some won it (OK). All of one kind means the latency moved and the sweep no longer covers the race.
    EXPECT_GT(swept45, 0) << tag << ": no swept round was answered 45/46 (swept45=" << swept45
                          << ", sweptOk=" << sweptOk << "): the sweep sits after the window";
    EXPECT_GT(sweptOk, 0) << tag << ": no swept round was answered OK (swept45=" << swept45
                          << ", sweptOk=" << sweptOk << "): the sweep sits before the window";
}
}  // namespace

TEST(AuthDdlAnswerStallRegression, SubscribeRacingRevokeStaysEndedShard) { subscribeRacingRevokeStaysEnded(true, "racesh"); }
TEST(AuthDdlAnswerStallRegression, SubscribeRacingRevokeStaysEndedGateway) { subscribeRacingRevokeStaysEnded(false, "racegw"); }

namespace {
// Issue #188: a Subscribe still AwaitingEgress (the gateway's record-egress resolve is HELD) is ended by a REVOKE READ; once
// the hold is released, that subscription must never deliver a Record, whatever its offset (a zombie replays from 0, below
// any log-end bound).  Gateway mode has no worker record egress to wait for: the Subscribe is served at once, so there the
// check is that nothing arrives after the end (also with new records produced).
void endedSubscriptionNeverPushesAgain(bool shard, const char* tag) {
    AuthBroker::Options o; o.shard = shard;
    AuthRig rig(tag, o);
    Async a{rig};
    auto& c = rig.c();
    const int64_t ws = rig.newUser({exact(SO::Write, ST::Topic, "orders")});
    for (int i = 0; i < 3; ++i) ASSERT_EQ(rig.produce(ws, rig.orders, i == 0), 0);
    std::string reader;
    const int64_t rs = rig.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
    const std::size_t recordsBefore = c.records().size();
    rig.broker().node().holdRecordEgressResolveForTest(true);
    const int64_t corr = c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0);
    ASSERT_NE(corr, 0);
    c.pollFor(100ms, [] { return false; });
    if (shard) EXPECT_FALSE(c.subscriptionIdOf(corr).has_value()) << "the Subscribe OK waits for the held record egress";
    pc::SecurityDdl m; m.kind = pc::SecurityOpKind::Revoke; m.name = reader; m.granteeKind = 0;
    m.op = static_cast<uint8_t>(SO::Read); m.resourceType = static_cast<uint8_t>(ST::Topic);
    m.patternKind = static_cast<uint8_t>(SP::Exact); m.pattern = "orders";
    const int64_t rc = a.send(m, "REVOKE ...;");
    ASSERT_EQ(a.answer(rc), 0);
    rig.broker().node().holdRecordEgressResolveForTest(false);
    const auto code = c.awaitSubscribeCode(corr, AuthRig::dl());
    ASSERT_TRUE(code.has_value()) << "the Subscribe was answered";
    c.pollFor(200ms, [] { return false; });
    const int64_t subId = c.subscriptionIdOf(corr).value_or(0);
    const auto delivered = [&] {   // no other subscription lives on `c`: with no sub id, every new Record counts
        std::size_t n = 0;
        for (std::size_t i = recordsBefore; i < c.records().size(); ++i) n += (subId == 0 || c.records()[i].subscriptionId == subId) ? 1 : 0;
        return n;
    };
    const std::size_t atEnd = delivered();
    if (shard) {
        // the broker ends the race by answering the held Subscribe itself (PR #596 review: asserted, not only printed)
        EXPECT_TRUE(*code == 45 || *code == 46) << "the held Subscribe answered " << *code << ", expected 45/46";
        EXPECT_EQ(atEnd, 0u) << "the subscription never reached a worker, yet delivered (subscribe answered " << *code << ")";
    }
    for (int i = 0; i < 3; ++i) ASSERT_EQ(rig.produce(ws, rig.orders), 0);
    c.pollFor(400ms, [] { return false; });
    EXPECT_EQ(delivered(), atEnd) << "the revoked subscription (subscribe answered " << *code << ", sub id " << subId
                                  << ") delivered records after its end";
}
}  // namespace

TEST(AuthStressRegression, EndedSubscriptionNeverPushesAgainRegression) {
    for (const bool shard : {true, false}) {
        SCOPED_TRACE(shard ? "shard" : "gateway");
        endedSubscriptionNeverPushesAgain(shard, shard ? "nevsh" : "nevgw");
        if (HasFatalFailure()) return;
    }
}

// ── Adversarial review of PR #596 (issues #188 / #5): a Subscribe parked AwaitingEgress (the gateway's record-egress
// resolve held) against every way its principal can lose READ before the resolve completes, in a seeded order.  The
// invariant for every round: the Subscribe is answered; an answer other than OK is 45/46; no Record ever arrives for a
// subscription id the client never got an OK for (a zombie worker cursor); a subscription answered OK then ended
// (ErrorResponse 45/46) delivers nothing after its end, also with new records produced.  At the end an unheld
// Subscribe of a fresh reader delivers (the path stays live, so the silence above is not vacuous).
namespace {
enum class HeldVariant { RevokeAnswered, ReleaseBeforeRevokeAnswer, RevokeThenReGrant, SubscribeTwice, Disable, Disconnect };
const char* variantName(HeldVariant v) {
    switch (v) {
        case HeldVariant::RevokeAnswered: return "revoke-answered";
        case HeldVariant::ReleaseBeforeRevokeAnswer: return "release-before-revoke-answer";
        case HeldVariant::RevokeThenReGrant: return "revoke-then-regrant";
        case HeldVariant::SubscribeTwice: return "subscribe-twice";
        case HeldVariant::Disable: return "disable";
        case HeldVariant::Disconnect: return "disconnect";
    }
    return "?";
}
pc::SecurityDdl readDdl(pc::SecurityOpKind k, const std::string& who) {
    pc::SecurityDdl m; m.kind = k; m.name = who; m.granteeKind = 0;
    m.op = static_cast<uint8_t>(SO::Read); m.resourceType = static_cast<uint8_t>(ST::Topic);
    m.patternKind = static_cast<uint8_t>(SP::Exact); m.pattern = "orders";
    return m;
}
}  // namespace

TEST(AuthDdlAnswerStallRegression, HeldSubscribeLosingReadEveryWayNeverPushesAdversarialRegression) {
    constexpr uint64_t kSeed = 0x596A'D5E1ull;
    SCOPED_TRACE("seed " + std::to_string(kSeed));
    std::mt19937_64 rng(kSeed);
    std::vector<HeldVariant> plan;
    for (int rep = 0; rep < 2; ++rep)
        for (HeldVariant v : {HeldVariant::RevokeAnswered, HeldVariant::ReleaseBeforeRevokeAnswer, HeldVariant::RevokeThenReGrant,
                              HeldVariant::SubscribeTwice, HeldVariant::Disable, HeldVariant::Disconnect})
            plan.push_back(v);
    std::shuffle(plan.begin(), plan.end(), rng);

    AuthBroker::Options o; o.shard = true;
    AuthRig rig("advheld", o);
    Async a{rig};
    auto& c = rig.c();
    auto& node = rig.broker().node();
    const int64_t ws = rig.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(rig.produce(ws, rig.orders, true), 0);
    const auto zombies = [&] {   // Records for an id no Subscribe OK of this client named
        std::size_t n = 0;
        for (const auto& p : c.records()) n += c.knowsSubscriptionId(p.subscriptionId) ? 0 : 1;
        return n;
    };
    for (std::size_t round = 0; round < plan.size(); ++round) {
        const HeldVariant v = plan[round];
        SCOPED_TRACE("round " + std::to_string(round) + " " + variantName(v));
        std::string reader;
        const int64_t rs = rig.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
        node.holdRecordEgressResolveForTest(true);
        std::vector<int64_t> corrs{c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0)};
        if (v == HeldVariant::SubscribeTwice) corrs.push_back(c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0));
        for (int64_t corr : corrs) ASSERT_NE(corr, 0);
        c.pollFor(50ms, [] { return false; });
        const bool sessionEnds = v == HeldVariant::Disable || v == HeldVariant::Disconnect;
        switch (v) {
            case HeldVariant::RevokeAnswered:
            case HeldVariant::SubscribeTwice:
                ASSERT_EQ(a.answer(a.send(readDdl(pc::SecurityOpKind::Revoke, reader), "REVOKE ...;")), 0);
                node.holdRecordEgressResolveForTest(false);
                break;
            case HeldVariant::ReleaseBeforeRevokeAnswer: {
                const int64_t rc = a.send(readDdl(pc::SecurityOpKind::Revoke, reader), "REVOKE ...;");
                node.holdRecordEgressResolveForTest(false);   // the resolve completes while the revoke is in flight
                ASSERT_EQ(a.answer(rc), 0);
                break;
            }
            case HeldVariant::RevokeThenReGrant:
                ASSERT_EQ(a.answer(a.send(readDdl(pc::SecurityOpKind::Revoke, reader), "REVOKE ...;")), 0);
                ASSERT_EQ(a.answer(a.send(readDdl(pc::SecurityOpKind::Grant, reader), "GRANT ...;")), 0);
                node.holdRecordEgressResolveForTest(false);
                break;
            case HeldVariant::Disable:
                ASSERT_EQ(a.answer(a.alter(reader, pc::UserChangeKind::Disable)), 0);
                node.holdRecordEgressResolveForTest(false);
                EXPECT_TRUE(rig.awaitSessionGone(rs)) << "a disabled user's session is closed";
                break;
            case HeldVariant::Disconnect:
                ASSERT_TRUE(c.disconnect(rig.ch(), rig.stream(), rs));
                EXPECT_TRUE(rig.awaitSessionGone(rs)) << "the disconnected session is gone";
                node.holdRecordEgressResolveForTest(false);
                break;
        }
        std::vector<int64_t> endedIds;
        for (int64_t corr : corrs) {
            const auto code = c.awaitSubscribeCode(corr, sessionEnds ? AuthRig::dl(1) : AuthRig::dl());
            if (!sessionEnds) ASSERT_TRUE(code.has_value()) << "the held Subscribe " << corr << " was answered";
            if (!code) continue;
            if (*code != 0) {
                EXPECT_TRUE(*code == 45 || *code == 46) << "the held Subscribe answered " << *code << ", expected 45/46";
                continue;
            }
            // answered OK: the release won the race; the end must follow, and it must be the last word
            if (!sessionEnds) {
                EXPECT_TRUE(c.pollUntil([&] { for (const auto& e : c.errors()) if (e.correlationId == corr && (e.code == 45 || e.code == 46)) return true; return false; }))
                    << "a subscription answered OK after its READ was revoked was never ended";
            }
            endedIds.push_back(c.subscriptionIdOf(corr).value_or(0));
        }
        c.pollFor(200ms, [] { return false; });
        const auto pushesOf = [&](int64_t id) { std::size_t n = 0; for (const auto& p : c.records()) n += p.subscriptionId == id ? 1 : 0; return n; };
        std::vector<std::size_t> atEnd;
        for (int64_t id : endedIds) atEnd.push_back(pushesOf(id));
        for (int i = 0; i < 3; ++i) ASSERT_EQ(rig.produce(ws, rig.orders), 0);
        c.pollFor(300ms, [] { return false; });
        EXPECT_EQ(zombies(), 0u) << "Records for a subscription the client never got an OK for (a zombie worker cursor)";
        for (std::size_t k = 0; k < endedIds.size(); ++k)
            EXPECT_EQ(pushesOf(endedIds[k]), atEnd[k]) << "ended subscription " << endedIds[k] << " delivered after its end";
        if (HasFailure()) return;
    }
    // liveness (non-vacuity): with nothing held, a fresh reader's subscription is bound and delivers
    const int64_t live = rig.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t corr = c.sendSubscribe(rig.ch(), rig.stream(), live, rig.orders, 0);
    ASSERT_EQ(c.awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0);
    const int64_t id = c.subscriptionIdOf(corr).value_or(0);
    EXPECT_TRUE(c.pollUntil([&] { for (const auto& p : c.records()) if (p.subscriptionId == id) return true; return false; }))
        << "an unheld subscription delivers";
}

// Adversarial review of PR #596 (#134, the GetViewRow READ gate): READ on a view revoked while the session is open is
// enforced on that same session from the revoke's answer on, a re-grant restores it, and a DISABLE closes the session -
// in shard and gateway modes.  (The alloc gate swaps a test-only permission table; this drives the real revoke.)
namespace {
void viewReadRevokedMidSession(bool shard, const char* tag) {
    AuthBroker::Options o; o.shard = shard;
    AuthRig rig(tag, o);
    auto& adm = rig.broker().admin();
    std::string name;
    const int64_t s = rig.newUser({exact(SO::Read, ST::View, "board")}, &name);
    for (int round = 0; round < 3; ++round) {
        SCOPED_TRACE("round " + std::to_string(round));
        const int before = rig.getViewRow(s, rig.board);
        ASSERT_NE(before, kNoAnswer);
        EXPECT_NE(before, kNotAuthorized) << "READ VIEW held: the read is served";
        ASSERT_EQ(adm.revoke(rig.ch(), rig.broker().adminSession(), name, false, SO::Read, ST::View, SP::Exact, "board").errorCode, 0);
        EXPECT_EQ(rig.getViewRow(s, rig.board), kNotAuthorized) << "the read after an answered REVOKE READ VIEW is refused";
        ASSERT_EQ(adm.grant(rig.ch(), rig.broker().adminSession(), name, false, SO::Read, ST::View, SP::Exact, "board").errorCode, 0);
    }
    EXPECT_NE(rig.getViewRow(s, rig.board), kNotAuthorized) << "re-granted READ VIEW: the read is served again";
    ASSERT_EQ(adm.alterPassword(rig.ch(), rig.broker().adminSession(), name, pc::UserChangeKind::Disable).errorCode, 0);
    EXPECT_TRUE(rig.awaitSessionGone(s)) << "a disabled user's session is closed";
}
}  // namespace

TEST(AuthDdlAnswerStallRegression, ViewReadRevokedMidSessionIsRefusedShardAdversarialRegression) { viewReadRevokedMidSession(true, "advvrsh"); }
TEST(AuthDdlAnswerStallRegression, ViewReadRevokedMidSessionIsRefusedGatewayAdversarialRegression) { viewReadRevokedMidSession(false, "advvrgw"); }
