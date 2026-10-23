// AuthDdlAnswerStallRegressionTest.cpp - authentication Task 18: the seeded stress (seed 0) found two broker bugs.
//  1. The security DDL answers stalled for good: the control thread spun in AssignmentMap::get (tombstone exhaustion after
//     topic create/drop churn); its regression lives in libs/metadata/tests/PartitionAssignmentTest.cpp
//     (AssignmentMapRegression).  The first two tests here are the end-to-end shape of the racing DDLs that exposed it.
//  2. A revoke-ended subscription whose Subscribe still waited for its session's record-egress pub became a zombie worker
//     cursor (it pushed after the revoke, and again after a re-grant).  The deterministic gateway-level regression is
//     RecordEgressLifetimeRegression.UnsubscribeCancelsASubscribeStillAwaitingItsEgressPub; the tests below are its
//     end-to-end shape.
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
    for (int round = 0; round < 15; ++round) {
        std::string reader;
        const int64_t rs = rig.newUser({exact(SO::Read, ST::Topic, "orders")}, &reader);
        const int64_t corr = c.sendSubscribe(rig.ch(), rig.stream(), rs, rig.orders, 0);
        pc::SecurityDdl m; m.kind = pc::SecurityOpKind::Revoke; m.name = reader; m.granteeKind = 0;
        m.op = static_cast<uint8_t>(SO::Read); m.resourceType = static_cast<uint8_t>(ST::Topic);
        m.patternKind = static_cast<uint8_t>(SP::Exact); m.pattern = "orders";
        const int64_t rc = a.send(m, "REVOKE ...;");
        ASSERT_EQ(a.answer(rc), 0);
        const auto code = c.awaitSubscribeCode(corr, AuthRig::dl());
        ASSERT_TRUE(code.has_value());
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
    }
    EXPECT_GT(subscribedRounds, 0) << "at least one round took the path under test (Subscribe OK, then revoked)";
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
