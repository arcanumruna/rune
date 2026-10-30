// ShardAuthProduceTest.cpp - authentication Task 13 (spec §6, D30) against ONE real authenticating broker in SHARD
// mode: a client produces straight to the owning worker's ingress endpoint, bypassing the gateway.  The worker's WRITE
// bitmap (shipped over the lifecycle ring on login) is the only thing standing between an unauthorized client and the log.
#include "support/AuthRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;

namespace {
// One produce straight to the worker, retried while leadership / assignment settles (NotLeader, UnknownTopic).
// NotAuthorized (45) is final: it is the answer under test, never a transient.
int shardProduce(AuthRig& rig, int64_t sid, int32_t topicId, int attempts = 200) {
    int code = kNoAnswer;
    for (int a = 0; a < attempts; ++a) {
        const int64_t corr = rig.c().nextCorrelationId();
        if (!rig.c().produce(rig.broker().workerIngressChannel(), AuthRig::stream(), sid, corr, topicId, 0, 1, makeCharBatch(1)))
            return kNoAnswer;
        const auto ack = rig.c().awaitProduceResponseUntil(corr, AuthRig::dl(10));
        code = ack ? ack->errorCode : kNoAnswer;
        if (code != static_cast<int>(pc::ErrorCode::NotLeader) && code != static_cast<int>(pc::ErrorCode::UnknownTopic) &&
            code != kNoAnswer)
            return code;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return code;
}
AuthBroker::Options shardOptions() {
    AuthBroker::Options o;
    o.shard = true;
    return o;
}
}  // namespace

TEST(ShardAuthProduceRegression, GrantedProduceSucceedsAndUngrantedIsNotAuthorized) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shardauth", shardOptions());
    const int64_t writer = rig.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t reader = rig.newUser({exact(SO::Read, ST::Topic, "orders")});
    EXPECT_EQ(shardProduce(rig, writer, rig.orders), 0) << "the granted topic is appended";
    // #278: a refusal is not only the code - the worker consumed the frame and appended nothing.
    const int64_t orders = rig.logEnd(rig.orders), secret = rig.logEnd(rig.secret);
    EXPECT_GE(orders, 1) << "the granted produce was appended";
    EXPECT_EQ(shardProduce(rig, writer, rig.secret), kNotAuthorized) << "an ungranted topic is refused at the worker";
    EXPECT_EQ(shardProduce(rig, reader, rig.orders), kNotAuthorized) << "READ is not WRITE";
    EXPECT_EQ(rig.logEnd(rig.secret), secret) << "#278: the refused produce appended nothing";
    EXPECT_EQ(rig.logEnd(rig.orders), orders) << "#278: the refused produce appended nothing";
}

TEST(ShardAuthProduceRegression, ASessionThatNeverLoggedInIsDeniedAtTheWorker) {
    AuthRig rig("shardanon", shardOptions());
    const int64_t sid = rig.wire().connect(rig.ch());                     // connected, SCRAM not even started
    ASSERT_GT(sid, 0);
    const int64_t before = rig.logEnd(rig.orders);
    EXPECT_EQ(shardProduce(rig, sid, rig.orders, /*attempts=*/3), kNotAuthorized)
        << "default deny: an announced, unauthenticated session has an empty slot";
    EXPECT_EQ(rig.logEnd(rig.orders), before) << "#278: the refused produce appended nothing";
}

TEST(ShardAuthProduceRegression, NoProduceCanPrecedeTheLoginsPermissions) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shardrace", shardOptions());
    std::string user;
    (void)rig.newUser({exact(SO::Write, ST::Topic, "orders")}, &user);
    ASSERT_EQ(shardProduce(rig, rig.login(user), rig.orders), 0) << "warm-up: leadership settled";
    rig.wire().cacheSaltedPasswords(true);
    for (int i = 0; i < 200; ++i) {
        const int64_t sid = rig.wire().connect(rig.ch());
        ASSERT_GT(sid, 0) << i;
        const auto login = rig.wire().login(rig.ch(), sid, user, {AuthRig::kPw, AuthRig::kPw});
        ASSERT_TRUE(login.ok()) << i;
        // The instant AuthServerFinal{OK} arrives: produce.  The grants were enqueued on every worker's lifecycle
        // ring BEFORE the OK was sent, so this is always accepted - never a stale NotAuthorized.
        const int64_t corr = rig.c().nextCorrelationId();
        ASSERT_TRUE(rig.c().produce(rig.broker().workerIngressChannel(), AuthRig::stream(), sid, corr, rig.orders, 0, 1, makeCharBatch(1)));
        const auto ack = rig.c().awaitProduceResponseUntil(corr, AuthRig::dl(10));
        ASSERT_TRUE(ack.has_value()) << "iteration " << i;
        EXPECT_EQ(ack->errorCode, 0) << "iteration " << i;
    }
}

namespace {
// Issue #197: DefineTopic `name` as `sid`, then (hold optional) a direct worker produce straight after the OK.
struct DefineThenProduce { int ddl = kNoAnswer; int produce = kNoAnswer; bool answeredWhileHeld = false; };
DefineThenProduce defineThenProduce(AuthRig& rig, int64_t sid, const std::string& name, bool hold) {
    DefineThenProduce out;
    if (hold) rig.broker().node().holdPermissionShippingForTest(true);
    const int64_t corr = rig.c().sendDefineTopic(rig.ch(), AuthRig::stream(), sid, name, 1, authRigCharBlob());
    std::optional<pc::DefineTopicResponse> early;
    if (hold) {
        early = rig.c().awaitDefine(corr, AuthRig::Clock::now() + std::chrono::milliseconds(300));
        out.answeredWhileHeld = early.has_value();
        rig.broker().node().holdPermissionShippingForTest(false);
    }
    const auto a = early ? early : rig.c().awaitDefine(corr, AuthRig::dl(20));
    out.ddl = a ? a->errorCode : kNoAnswer;
    if (out.ddl != 0) return out;
    // The new topic's id: the DefineTopicResponse carries it.
    out.produce = kNoAnswer;
    const int32_t id = a->topicId;
    for (int attempt = 0; attempt < 200; ++attempt) {
        const int64_t pc2 = rig.c().nextCorrelationId();
        if (!rig.c().produce(rig.broker().workerIngressChannel(), AuthRig::stream(), sid, pc2, id, 0, 1, makeCharBatch(1))) break;
        const auto ack = rig.c().awaitProduceResponseUntil(pc2, AuthRig::dl(10));
        out.produce = ack ? ack->errorCode : kNoAnswer;
        // Only the leadership / assignment settling is retried - NEVER 45 (the answer under test).
        if (out.produce != static_cast<int>(pc::ErrorCode::NotLeader) && out.produce != static_cast<int>(pc::ErrorCode::UnknownTopic) &&
            out.produce != kNoAnswer)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return out;
}
}  // namespace

// Issue #197 (RED before the fix: the DefineTopic OK left while the new topic's WRITE-bit patch still sat in the
// gateway's permission shipper, so a direct worker produce straight after the OK was refused 45).
TEST(ShardAuthProduceRegression, ATopicCreatedAfterLoginIsWritableAtTheWorkerOnItsOkRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shardddl197", shardOptions());
    const int32_t warm = rig.defineTopic(rig.builder, "w.warm");
    ASSERT_GE(warm, 0);
    const int64_t user = rig.newUser({prefix(SO::Write, ST::Topic, "w."), prefix(SO::Create, ST::Topic, "w.")});
    ASSERT_EQ(shardProduce(rig, user, warm), 0) << "warm-up: login shipped, leadership settled";
    for (int i = 0; i < 5; ++i) {
        const auto r = defineThenProduce(rig, user, "w.n" + std::to_string(i), /*hold=*/true);
        EXPECT_FALSE(r.answeredWhileHeld) << i << ": the OK must wait for the permission patch to reach the workers";
        EXPECT_EQ(r.ddl, 0) << i;
        EXPECT_EQ(r.produce, 0) << i << ": acked, never 45";
    }
}

// Issue #197 guard: with nothing backed up the gate opens at once (a gate that never opens fails this).
TEST(ShardAuthProduceRegression, WithoutABackedUpShipperTheDdlOkIsNotDelayedRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shardddl197b", shardOptions());
    const int32_t warm = rig.defineTopic(rig.builder, "w.warm");
    ASSERT_GE(warm, 0);
    const int64_t user = rig.newUser({prefix(SO::Write, ST::Topic, "w."), prefix(SO::Create, ST::Topic, "w.")});
    ASSERT_EQ(shardProduce(rig, user, warm), 0);
    for (int i = 0; i < 3; ++i) {
        const auto r = defineThenProduce(rig, user, "w.m" + std::to_string(i), /*hold=*/false);
        EXPECT_EQ(r.ddl, 0) << i;
        EXPECT_EQ(r.produce, 0) << i;
    }
}

// ── Adversarial review of PR #528 (issue #197) ──────────────────────────────────────────────────────────────────────

// Never a hang: a shipper that NEVER drains (a worker ring stuck full) does not hold the DefineTopic answer forever -
// the gate answers Internal (in doubt) once kDdlAnswerMaxHoldNs passes, naming the unshipped permissions.  Once the
// backlog ships, a retry with the SAME requestId is answered OK for the same topic and a direct worker produce is
// acked, never 45.  RED (mutation): revert the gate's objectPermissionsShipped term - the first answer is an
// immediate OK.
TEST(ShardAuthProduceRegression, APermanentlyBackedUpShipperAnswersTheDdlInDoubtNeverHangsAdversarialRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shardddl197adv", shardOptions());
    const int32_t warm = rig.defineTopic(rig.builder, "w.warm");
    ASSERT_GE(warm, 0);
    const int64_t user = rig.newUser({prefix(SO::Write, ST::Topic, "w."), prefix(SO::Create, ST::Topic, "w.")});
    ASSERT_EQ(shardProduce(rig, user, warm), 0);
    const rune::protocol::RequestId rid{0x5197, 0x528};
    rig.broker().node().holdPermissionShippingForTest(true);
    const int64_t corr = rig.c().sendDefineTopic(rig.ch(), AuthRig::stream(), user, "w.stuck", 1, authRigCharBlob(), 0, rid);
    ASSERT_NE(corr, 0);
    EXPECT_FALSE(rig.c().awaitAnyDdlAnswer(corr, AuthRig::Clock::now() + std::chrono::milliseconds(500)).has_value())
        << "answered while the new topic's permissions were still queued";
    const auto a = rig.c().awaitAnyDdlAnswer(corr, AuthRig::dl(20));
    ASSERT_TRUE(a.has_value()) << "the DDL answer was held forever";
    EXPECT_NE(a->code, 0) << "an OK left while no worker held the topic's WRITE bits";
    if (!a->message.empty())
        EXPECT_NE(a->message.find("permissions have not reached every worker"), std::string::npos) << a->message;
    EXPECT_EQ(rig.c().answersFor(corr), 1u);

    rig.broker().node().holdPermissionShippingForTest(false);
    const int64_t retry = rig.c().sendDefineTopic(rig.ch(), AuthRig::stream(), user, "w.stuck", 1, authRigCharBlob(), 0, rid);
    const auto ok = rig.c().awaitDefine(retry, AuthRig::dl(20));
    ASSERT_TRUE(ok.has_value());
    ASSERT_EQ(ok->errorCode, 0) << "the retry with the same requestId must be answered OK once shipped";
    EXPECT_EQ(shardProduce(rig, user, ok->topicId), 0) << "acked at the worker, never 45";
}

// Drop while held: an OK held on the shipper is released as soon as its topic is DROPPED (a dropped object never
// serves and owes nothing), while the shipper is still held - not left to time out into an in-doubt Internal.
// RED (mutation): remove the erase in onCatalogObjectDropped - the OK is held to the 5 s deadline and answered Internal.
TEST(ShardAuthProduceRegression, ADropWhileTheOkIsHeldReleasesItAdversarialRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shardddl197drop", shardOptions());
    const int32_t warm = rig.defineTopic(rig.builder, "w.warm");
    ASSERT_GE(warm, 0);
    const int64_t user = rig.newUser({prefix(SO::Write, ST::Topic, "w."), prefix(SO::Create, ST::Topic, "w.")});
    ASSERT_EQ(shardProduce(rig, user, warm), 0);
    rig.broker().node().holdPermissionShippingForTest(true);
    const auto t0 = AuthRig::Clock::now();
    const int64_t corr = rig.c().sendDefineTopic(rig.ch(), AuthRig::stream(), user, "w.dropme", 1, authRigCharBlob());
    ASSERT_NE(corr, 0);
    EXPECT_FALSE(rig.c().awaitAnyDdlAnswer(corr, t0 + std::chrono::milliseconds(300)).has_value());
    const int64_t dropCorr = rig.c().sendDropTopic(rig.ch(), AuthRig::stream(), rig.builder, "w.dropme");
    ASSERT_NE(dropCorr, 0);
    const auto a = rig.c().awaitAnyDdlAnswer(corr, AuthRig::dl(20));
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->code, 0) << "the held OK of a dropped topic must pass, not time out in doubt: " << a->message;
    EXPECT_LT(AuthRig::Clock::now() - t0, std::chrono::milliseconds(4500)) << "released only by the hold deadline";
    const auto d = rig.c().awaitAnyDdlAnswer(dropCorr, AuthRig::dl(20));
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->code, 0) << d->message;
    rig.broker().node().holdPermissionShippingForTest(false);
}
