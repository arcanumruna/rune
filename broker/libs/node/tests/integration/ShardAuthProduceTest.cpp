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
        if (!rig.c().produce(rig.broker().workerIngressChannel(), AuthRig::stream(), sid, corr, topicId, 0, 1, makeBatch(1)))
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
    EXPECT_EQ(shardProduce(rig, writer, rig.secret), kNotAuthorized) << "an ungranted topic is refused at the worker";
    EXPECT_EQ(shardProduce(rig, reader, rig.orders), kNotAuthorized) << "READ is not WRITE";
}

TEST(ShardAuthProduceRegression, ASessionThatNeverLoggedInIsDeniedAtTheWorker) {
    AuthRig rig("shardanon", shardOptions());
    const int64_t sid = rig.wire().connect(rig.ch());                     // connected, SCRAM not even started
    ASSERT_GT(sid, 0);
    EXPECT_EQ(shardProduce(rig, sid, rig.orders, /*attempts=*/3), kNotAuthorized)
        << "default deny: an announced, unauthenticated session has an empty slot";
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
        ASSERT_TRUE(rig.c().produce(rig.broker().workerIngressChannel(), AuthRig::stream(), sid, corr, rig.orders, 0, 1, makeBatch(1)));
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
        if (!rig.c().produce(rig.broker().workerIngressChannel(), AuthRig::stream(), sid, pc2, id, 0, 1, makeBatch(1))) break;
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
