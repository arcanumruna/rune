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
