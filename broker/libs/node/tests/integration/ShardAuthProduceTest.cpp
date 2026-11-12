// ShardAuthProduceTest.cpp - authentication Task 13 (spec §6, D30) against ONE real authenticating broker in SHARD
// mode: a client produces straight to the owning worker's ingress endpoint, bypassing the gateway.  The worker's WRITE
// bitmap (shipped over the lifecycle ring on login) is the only thing standing between an unauthorized client and the log.
#include "support/AuthRig.h"

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::test;
namespace pc = rune::protocol::client;

namespace {
// Waits (bounded, the client's own pump; no sleep here) until the partition accepts produces: the replica's own gate.
bool awaitServes(AuthRig& rig, int32_t topicId) {
    const bool ok = rig.c().pollFor(std::chrono::seconds(10), [&] { return rig.partitionAcceptsProduce(topicId); });
    EXPECT_TRUE(ok) << "topic " << topicId << " partition 0 does not accept produces after 10 s";
    return ok;
}
// One produce straight to the worker once the partition serves.  The worker's own registry can still lag the replica's
// gate (UnknownTopic / NotLeader), so at most `attempts` immediate re-sends, no sleep.
// NotAuthorized (45) is final: it is the answer under test, never a transient.
int shardProduce(AuthRig& rig, int64_t sid, int32_t topicId, int attempts = 3) {
    if (!awaitServes(rig, topicId)) return kNoAnswer;
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
    EXPECT_EQ(shardProduce(rig, sid, rig.orders, /*attempts=*/3), static_cast<int>(pc::ErrorCode::AuthenticationFailed))
        << "default deny: an announced, unauthenticated session has an empty slot (#216: answered 44, as the gateway)";
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
    if (!awaitServes(rig, id)) return out;
    for (int attempt = 0; attempt < 3; ++attempt) {   // immediate re-sends only (the worker's registry can lag the gate)
        const int64_t pc2 = rig.c().nextCorrelationId();
        if (!rig.c().produce(rig.broker().workerIngressChannel(), AuthRig::stream(), sid, pc2, id, 0, 1, makeCharBatch(1))) break;
        const auto ack = rig.c().awaitProduceResponseUntil(pc2, AuthRig::dl(10));
        out.produce = ack ? ack->errorCode : kNoAnswer;
        // Only the leadership / assignment settling is retried - NEVER 45 (the answer under test).
        if (out.produce != static_cast<int>(pc::ErrorCode::NotLeader) && out.produce != static_cast<int>(pc::ErrorCode::UnknownTopic) &&
            out.produce != kNoAnswer)
            break;
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

// Issue #500: a produce straight to the owning worker for a topic this broker has not applied yet.  The session holds
// WRITE ON TOPIC PREFIX 'w.'; the next catalog topic id is not created yet, so the worker's bitmap cannot cover it.
// The worker answers the retriable NotLeader (never the terminal 45) and appends nothing; once the topic is created
// and applied the re-sent produce - the same producer sequence - is appended, and the next sequence follows it.
// Real broker: the catalog apply cannot be held here, so the window is the id of a topic that does not exist yet; the
// frontier the gateway ships behind the apply's permission patches is what the worker compares against.
// RED before the fix: the worker answered 45.
TEST(ShardAuthProduceRegression, AProduceToATopicNotAppliedHereIsRetriableOnTheWorkerStreamRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shard500", shardOptions());
    const int32_t warm = rig.defineTopic(rig.builder, "w.warm");
    ASSERT_GE(warm, 0);
    const int64_t user = rig.newUser({prefix(SO::Write, ST::Topic, "w."), prefix(SO::Create, ST::Topic, "w.")});
    ASSERT_EQ(shardProduce(rig, user, warm), 0) << "warm-up: login shipped, leadership settled";
    const int32_t next = warm + 1;   // the catalog's next topic id: not created, so not applied on this broker
    const auto ids = rig.initProducer(user);
    ASSERT_EQ(ids.code, 0);

    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.setProducerId(ids.pid); b.setProducerEpoch(ids.epoch); b.setBaseSequence(0);
    b.append(charKey(), charValue("v"), 1000);
    const std::vector<uint8_t> batch0 = b.build();
    auto direct = [&](int32_t topicId, int32_t seq, const std::vector<uint8_t>& batch) -> int {
        const int64_t corr = rig.c().nextCorrelationId();
        if (!rig.c().produceIdempotent(rig.broker().workerIngressChannel(), AuthRig::stream(), user, corr, topicId, 0,
                                       1, ids.pid, ids.epoch, seq, batch))
            return kNoAnswer;
        const auto ack = rig.c().awaitProduceResponseUntil(corr, AuthRig::dl(10));
        return ack ? ack->errorCode : kNoAnswer;
    };
    // The frontier follows the warm topic's apply on the same FIFO as its permission patch: a bounded number of
    // immediate re-sends until the worker has it (45 is only the not-yet-shipped state; it must turn into NotLeader).
    int code = kNoAnswer;
    for (int i = 0; i < 200 && (code == kNoAnswer || code == kNotAuthorized); ++i) code = direct(next, 0, batch0);
    EXPECT_EQ(code, static_cast<int>(pc::ErrorCode::NotLeader)) << "not applied here: retriable";

    const int32_t created = rig.defineTopic(user, "w.z");
    ASSERT_EQ(created, next) << "the topic got the id the produce addressed";
    ASSERT_TRUE(awaitServes(rig, created));
    int after = kNoAnswer;
    // transient: the worker's registry can lag the gate
    for (int i = 0; i < 20 && after != 0; ++i) after = direct(created, 0, batch0);
    EXPECT_EQ(after, 0) << "after the apply the re-sent produce, same sequence, is appended";
    EXPECT_EQ(rig.logEnd(created), 1) << "appended exactly once";
    rune::protocol::RecordBatchBuilder b1(0, 1000);
    b1.setProducerId(ids.pid); b1.setProducerEpoch(ids.epoch); b1.setBaseSequence(1);
    b1.append(charKey(), charValue("v"), 1000);
    EXPECT_EQ(direct(created, 1, b1.build()), 0) << "the next sequence follows: the refused produce never advanced it";
    EXPECT_EQ(rig.logEnd(created), 2);
}

// ── #216: the worker path answers what the gateway path answers ──
namespace {
// One produce on `channel` as `sid` over `w`'s own client (its answers come back on the session's own publication).
int produceOn(AuthWireClient& w, const std::string& channel, int64_t sid, int32_t topic, int attempts = 3) {
    int code = kNoAnswer;
    for (int a = 0; a < attempts; ++a) {
        const int64_t corr = w.raw().nextCorrelationId();
        if (!w.raw().produce(channel, AuthRig::stream(), sid, corr, topic, 0, 1, makeCharBatch(1))) return kNoAnswer;
        const auto ack = w.raw().awaitProduceResponseUntil(corr, AuthRig::dl(10));
        code = ack ? ack->errorCode : kNoAnswer;
        if (code != static_cast<int>(pc::ErrorCode::NotLeader) &&
            code != static_cast<int>(pc::ErrorCode::UnknownTopic) && code != kNoAnswer)
            return code;
    }
    return code;
}
}  // namespace

// Issue #216 (RED before: the worker answered 45 for all three states).  Sessions in AwaitingFirst, AwaitingFinal and
// PasswordChangeRequired produce through the worker ingress and through the gateway: the same codes (44, 44, 47), the
// unauthenticated sessions are ended on both paths, the password-change session stays - and once its password is
// changed it produces on the worker path.
TEST(ShardAuthProduceRegression, WorkerPathAnswersTheSameCodesAsTheGatewayRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    AuthRig rig("shard216", shardOptions());
    AuthBroker& b = rig.broker();
    AuthWireClient& w = b.wire();
    const std::string& gw = b.channel();
    const std::string& wk = b.workerIngressChannel();
    // A password-change session needs a bootstrap-created user: reopen the bootstrap window (both administrators
    // disabled), then bootstrap a one-time-password administrator.
    b.createUser("adm2", AuthBroker::kAdminPassword, {cluster(SO::Admin)});
    const int64_t a2 = w.connect(gw);
    ASSERT_GT(a2, 0);
    ASSERT_TRUE(w.login(gw, a2, "adm2", {AuthBroker::kAdminPassword, AuthBroker::kAdminPassword}).ok());
    ASSERT_EQ(w.alterPassword(gw, a2, "admin", pc::UserChangeKind::Disable).errorCode, 0);
    {   // adm2 disables itself: its own session is revoked with the change, so the answer may never arrive
        pc::SecurityDdl self; self.sessionId = a2; self.kind = pc::SecurityOpKind::AlterUser; self.name = "adm2";
        self.change = pc::UserChangeKind::Disable;
        self.flags = pc::kSecurityDdlFlagForce;   // the last enabled administrator: FORCE reopens the bootstrap window
        (void)w.securityDdl(gw, self, "ALTER USER adm2 DISABLE FORCE;", std::chrono::seconds(3));
    }
    int64_t s0 = 0;
    bool reopened = false;
    std::string lastDetail;
    for (int attempt = 0; attempt < 20 && !reopened; ++attempt) {   // bounded: reopens once the view applied both
        s0 = w.connect(gw);
        ASSERT_GT(s0, 0);
        const auto boot = w.bootstrap(gw, s0, b.keyContents(), "root2", AuthBroker::kBootstrapPassword);
        reopened = boot.errorCode == 0;
        if (!reopened) lastDetail = std::to_string(boot.errorCode) + ": " + boot.detail;
        if (!reopened) (void)w.raw().disconnect(gw, AuthWireClient::kClientStream, s0);
    }
    ASSERT_TRUE(reopened) << "the bootstrap window reopened: " << lastDetail;
    (void)w.raw().disconnect(gw, AuthWireClient::kClientStream, s0);
    const int64_t pcr = w.connect(gw);
    ASSERT_GT(pcr, 0);
    const auto pl = w.login(gw, pcr, "root2", {AuthBroker::kBootstrapPassword, AuthBroker::kBootstrapPassword});
    ASSERT_TRUE(pl.ok() && pl.mustChangePassword());
    ASSERT_TRUE(awaitServes(rig, rig.orders));
    // The unauthenticated sessions: one pair per path (a 44 ends the session it answers).  Created last: the end must
    // come from the 44, not from an idle or authentication deadline.
    const int64_t firstW = w.connect(gw), firstG = w.connect(gw), finalW = w.connect(gw), finalG = w.connect(gw);
    ASSERT_GT(std::min({firstW, firstG, finalW, finalG}), 0);
    for (const int64_t sid : {finalW, finalG}) {   // AwaitingFinal: the first SCRAM leg answered, the final never sent
        const auto l = w.login(gw, sid, "root2", {AuthBroker::kBootstrapPassword, AuthBroker::kBootstrapPassword}, 0,
                               /*sendFinal=*/false);
        ASSERT_TRUE(l.first.has_value());
    }
    const int kFailed = static_cast<int>(pc::ErrorCode::AuthenticationFailed);
    const int kPwChange = static_cast<int>(pc::ErrorCode::PasswordChangeRequired);
    const int64_t before = rig.logEnd(rig.orders);

    EXPECT_EQ(produceOn(w, wk, firstW, rig.orders), kFailed) << "AwaitingFirst, worker path";
    EXPECT_EQ(produceOn(w, gw, firstG, rig.orders), kFailed) << "AwaitingFirst, gateway path";
    EXPECT_EQ(produceOn(w, wk, finalW, rig.orders), kFailed) << "AwaitingFinal, worker path";
    EXPECT_EQ(produceOn(w, gw, finalG, rig.orders), kFailed) << "AwaitingFinal, gateway path";
    EXPECT_EQ(produceOn(w, wk, pcr, rig.orders), kPwChange) << "PasswordChangeRequired, worker path";
    EXPECT_EQ(produceOn(w, gw, pcr, rig.orders), kPwChange) << "PasswordChangeRequired, gateway path";
    EXPECT_EQ(rig.logEnd(rig.orders), before) << "nothing was appended";

    // The same client-visible end: the unauthenticated sessions are gone on both paths, the password-change one stays.
    for (const int64_t sid : {firstW, firstG, finalW, finalG})
        EXPECT_TRUE(rig.awaitSessionGone(sid, std::chrono::seconds(3))) << "session " << sid << " is ended by the 44";
    EXPECT_TRUE(b.node().gatewayHasSessionForTest(pcr)) << "the password-change session stays";
    EXPECT_EQ(produceOn(w, wk, pcr, rig.orders), kPwChange) << "and is answered again";

    // The password change lifts it: the ordinary bind replaces the password-change state on the worker.
    ASSERT_EQ(w.alterPassword(gw, pcr, "root2", pc::UserChangeKind::SetPassword, AuthBroker::kAdminPassword).errorCode,
              0);
    EXPECT_EQ(produceOn(w, wk, pcr, rig.orders), 0) << "the same session, once its password is changed";
    const int64_t again = w.connect(gw);
    ASSERT_GT(again, 0);
    ASSERT_TRUE(w.login(gw, again, "root2", {AuthBroker::kAdminPassword, AuthBroker::kAdminPassword}).ok());
    EXPECT_EQ(produceOn(w, wk, again, rig.orders), 0) << "and after a re-login";
}

// Issue #216, the race to rule out: a login's OK waits until its bind reached every worker, so the first direct produce
// after the OK is never answered 44 (the slot is still Unbound only before the bind).  The shipper is held: the OK is
// not sent; released: OK, and the first produce is served.  Then 200 plain logins, each producing at once.
TEST(ShardAuthProduceRegression, ALoginNeverSeesAnUnauthenticatedAnswerAfterItsOkRegression) {
    using namespace grants;
    using SO = rune::protocol::SecurityOperation;
    using ST = rune::protocol::SecurityResourceType;
    AuthRig rig("shard216race", shardOptions());
    AuthBroker& b = rig.broker();
    AuthWireClient& w = b.wire();
    std::string user;
    (void)rig.newUser({exact(SO::Write, ST::Topic, "orders")}, &user);
    ASSERT_TRUE(awaitServes(rig, rig.orders));
    const std::string& gw = b.channel();
    const std::string& wk = b.workerIngressChannel();
    for (int round = 0; round < 3; ++round) {
        const int64_t sid = w.connect(gw);
        ASSERT_GT(sid, 0);
        auto l = w.login(gw, sid, user, {AuthRig::kPw, AuthRig::kPw}, 0, /*sendFinal=*/false);
        ASSERT_TRUE(l.first.has_value()) << round;
        b.node().holdPermissionShippingForTest(true);
        w.finish(gw, l, user, {AuthRig::kPw, AuthRig::kPw}, std::chrono::milliseconds(300));
        EXPECT_FALSE(l.fin.has_value()) << "round " << round << ": the OK is not sent while its bind is queued";
        b.node().holdPermissionShippingForTest(false);
        const auto fin = w.raw().awaitAuthFinal(l.correlationId, AuthRig::dl(10));
        ASSERT_TRUE(fin.has_value()) << round;
        l.fin = pc::AuthServerFinal::decode(fin->bytes.data(), fin->bytes.size());
        ASSERT_TRUE(l.ok()) << round;
        EXPECT_EQ(produceOn(w, wk, sid, rig.orders, 1), 0) << "round " << round << ": first direct produce served";
    }
    w.cacheSaltedPasswords(true);
    for (int i = 0; i < 200; ++i) {
        const int64_t sid = w.connect(gw);
        ASSERT_GT(sid, 0) << i;
        ASSERT_TRUE(w.login(gw, sid, user, {AuthRig::kPw, AuthRig::kPw}).ok()) << i;
        const int code = produceOn(w, wk, sid, rig.orders, 1);
        ASSERT_NE(code, static_cast<int>(pc::ErrorCode::AuthenticationFailed)) << "iteration " << i;
        ASSERT_EQ(code, 0) << "iteration " << i;
    }
}

// Issue #216 follow-up: a session that never authenticated, on a broker with anonymous read on, is the anonymous reader
// as soon as its first produce reaches the gateway - on the worker path too.  The worker answers that first produce 44
// (it held no bind: the one accepted difference), and the gateway, handling the worker's signal, turns the session
// into the anonymous reader exactly as its own produce path does instead of ending it.  Later worker produces answer 45.
TEST(ShardAuthProduceRegression, AnAnonymousReadersFirstWorkerProduceDoesNotEndItRegression) {
    AuthBroker::Options o = shardOptions();
    o.configure = [](NodeConfig& c) { c.anonymousRead = true; };
    AuthRig rig("shard216anon", std::move(o));
    AuthBroker& b = rig.broker();
    AuthWireClient& w = b.wire();
    const std::string& gw = b.channel();
    const std::string& wk = b.workerIngressChannel();
    ASSERT_TRUE(awaitServes(rig, rig.orders));
    const int64_t sid = w.connect(gw);
    ASSERT_GT(sid, 0);
    const int64_t before = rig.logEnd(rig.orders);
    EXPECT_EQ(produceOn(w, wk, sid, rig.orders, /*attempts=*/1), static_cast<int>(pc::ErrorCode::AuthenticationFailed))
        << "the worker held no bind: the first produce is answered 44 (accepted difference)";
    // The signal is handled on the gateway's control thread: drive the client until the session is the anonymous reader.
    EXPECT_TRUE(rig.c().pollFor(std::chrono::seconds(10), [&] { return b.node().anonymousSessionsForTest() == 1; }))
        << "the worker's signal made the session the anonymous reader";
    EXPECT_TRUE(b.node().gatewayHasSessionForTest(sid)) << "an anonymous reader is not ended";
    const auto md = w.raw().metadata(gw, AuthWireClient::kClientStream, sid);
    EXPECT_FALSE(md.empty()) << "the session still reads (Describe on `default`)";
    EXPECT_EQ(produceOn(w, wk, sid, rig.orders, /*attempts=*/1), kNotAuthorized)
        << "its next worker produce is NotAuthorized, as on the gateway path";
    EXPECT_TRUE(b.node().gatewayHasSessionForTest(sid)) << "and the session stays";
    EXPECT_EQ(b.node().anonymousSessionsForTest(), 1u);
    EXPECT_EQ(rig.logEnd(rig.orders), before) << "nothing was appended";
}
