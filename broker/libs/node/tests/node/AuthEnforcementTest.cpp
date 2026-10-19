// AuthEnforcementTest.cpp - authentication Task 12 against ONE real authenticating BrokerNode (real Aeron UDP, raw
// frames): every row of the §2.1 enforcement map, granted -> the request's normal answer, not granted -> code 45 in the
// request's OWN response type.  A user per test, holding exactly the grants the row names.
#include "support/AuthRig.h"

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;

namespace {
constexpr int kOk = 0;
constexpr int kUnknownTopic = static_cast<int>(pc::ErrorCode::UnknownTopic);
}  // namespace

TEST(AuthEnforcementRegression, ProduceAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t onlyRead = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.produce(may, r.orders, /*untilOk=*/true), kOk);
    EXPECT_EQ(r.produce(onlyRead, r.orders), kNotAuthorized) << "READ is not WRITE";
    EXPECT_EQ(r.produce(none, r.orders), kNotAuthorized) << "default deny";
    EXPECT_EQ(r.produce(may, r.secret), kNotAuthorized) << "WRITE on one topic is not WRITE on another";
}

TEST(AuthEnforcementRegression, SubscribeAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t none = r.newUser({});
    ASSERT_TRUE(r.c().subscribeUntilBound(r.ch(), r.stream(), may, r.orders, 0, 0, AuthRig::dl(15)));
    EXPECT_EQ(r.subscribe(none, r.orders), kNotAuthorized);
    EXPECT_EQ(r.subscribe(may, r.secret), kNotAuthorized);
}

TEST(AuthEnforcementRegression, CatalogReadsNeedDescribeOnClusterRegression) {
    AuthRig r;
    const int64_t may = r.newUser({cluster(SO::Describe)});
    const int64_t readAll = r.newUser({prefix(SO::Read, ST::Topic, "")});
    EXPECT_EQ(r.catalogSubscribe(may), kOk);
    EXPECT_EQ(r.catalogSubscribe(readAll), kNotAuthorized) << "READ on every topic is not DESCRIBE ON CLUSTER (D21)";
}

TEST(AuthEnforcementRegression, SecurityReadsNeedAdminOnClusterRegression) {
    AuthRig r;
    const int64_t admin = r.newUser({cluster(SO::Admin)});
    const int64_t describe = r.newUser({cluster(SO::Describe)});
    EXPECT_EQ(r.subscribe(admin, rune::protocol::client::kSecurityTopicId), kOk);
    EXPECT_EQ(r.subscribe(describe, rune::protocol::client::kSecurityTopicId), kNotAuthorized);
}

TEST(AuthEnforcementRegression, GroupRowsAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Read, ST::Group, "cg"), exact(SO::Read, ST::Topic, "orders")});
    const int64_t groupOnly = r.newUser({exact(SO::Read, ST::Group, "cg")});
    const int64_t topicOnly = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t none = r.newUser({});
    std::string member;
    EXPECT_EQ(r.joinGroup(may, "cg", {r.orders}, &member), kOk);
    EXPECT_NE(r.heartbeat(may, "cg", member), kNotAuthorized);
    EXPECT_NE(r.offsetCommit(may, "cg", r.orders), kNotAuthorized);
    EXPECT_NE(r.offsetFetch(may, "cg", r.orders), kNotAuthorized);

    EXPECT_EQ(r.joinGroup(groupOnly, "cg", {r.orders}), kNotAuthorized) << "the group alone does not open the topic";
    EXPECT_EQ(r.joinGroup(topicOnly, "cg", {r.orders}), kNotAuthorized) << "the topic alone does not open the group";
    EXPECT_EQ(r.joinGroup(none, "cg", {r.orders}), kNotAuthorized);
    EXPECT_EQ(r.heartbeat(none, "cg", member), kNotAuthorized);
    EXPECT_EQ(r.offsetCommit(groupOnly, "cg", r.orders), kNotAuthorized);
    EXPECT_EQ(r.offsetCommit(topicOnly, "cg", r.orders), kNotAuthorized);
    EXPECT_EQ(r.offsetFetch(groupOnly, "cg", r.secret), kNotAuthorized) << "every topic the request names needs READ";
    EXPECT_EQ(r.offsetFetch(none, "cg", r.orders), kNotAuthorized);
}

TEST(AuthEnforcementRegression, LeaveGroupNeedsNoGrant) {
    AuthRig r;
    const int64_t none = r.newUser({});
    EXPECT_NE(r.leaveGroup(none, "anything"), kNotAuthorized) << "D18: LeaveGroup needs nothing";
}

TEST(AuthEnforcementRegression, TransactionRowsAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Write, ST::TransactionalId, "tx1"), exact(SO::Write, ST::Topic, "orders"),
                                   exact(SO::Read, ST::Group, "cg")});
    const int64_t txOnly = r.newUser({exact(SO::Write, ST::TransactionalId, "tx1")});
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.initProducerId(may, "tx1"), kOk);
    EXPECT_NE(r.addPartitions(may, "tx1", r.orders), kNotAuthorized);
    EXPECT_NE(r.addOffsets(may, "tx1", "cg"), kNotAuthorized);
    EXPECT_NE(r.txnOffsetCommit(may, "tx1", "cg", r.orders), kNotAuthorized);
    EXPECT_NE(r.endTxn(may, "tx1"), kNotAuthorized);

    EXPECT_EQ(r.initProducerId(none, "tx1"), kNotAuthorized);
    EXPECT_EQ(r.initProducerId(may, "tx2"), kNotAuthorized) << "the id is matched, not just any transactional id";
    EXPECT_EQ(r.addPartitions(txOnly, "tx1", r.orders), kNotAuthorized) << "WRITE on every partition's topic too";
    EXPECT_EQ(r.addOffsets(txOnly, "tx1", "cg"), kNotAuthorized) << "READ on the group too (D18)";
    EXPECT_EQ(r.txnOffsetCommit(txOnly, "tx1", "cg", r.orders), kNotAuthorized);
    EXPECT_EQ(r.endTxn(none, "tx1"), kNotAuthorized);
}

TEST(AuthEnforcementRegression, NonTransactionalInitProducerIdNeedsNoGrantRegression) {
    AuthRig r;
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.initProducerId(none, ""), kOk) << "D18: its produces are checked, not the id";
}

TEST(AuthEnforcementRegression, ViewReadAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Read, ST::View, "board")});
    const int64_t topicReader = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    EXPECT_NE(r.getViewRow(may, r.board), kNotAuthorized);
    EXPECT_EQ(r.getViewRow(topicReader, r.board), kNotAuthorized) << "READ on the source topic is not READ on the view";
    EXPECT_EQ(r.getViewRow(may, r.orders), kNotAuthorized) << "READ VIEW does not cover a topic id";
}

TEST(AuthEnforcementRegression, GetTopicSchemaAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Describe, ST::Topic, "orders")});
    const int64_t viewReader = r.newUser({exact(SO::Read, ST::View, "board")});
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.getTopicSchema(may, "orders"), kOk);
    EXPECT_EQ(r.getTopicSchema(viewReader, "board"), kOk) << "a view is addressed by name too";
    EXPECT_EQ(r.getTopicSchema(none, "orders"), kNotAuthorized);
    EXPECT_EQ(r.getTopicSchema(may, "secret"), kNotAuthorized);
}

TEST(AuthEnforcementRegression, DefineTopicAndCreateSchemaAllowedAndDenied) {
    AuthRig r;
    const int64_t named = r.newUser({exact(SO::Create, ST::Topic, "fresh1")});
    const int64_t cl = r.newUser({cluster(SO::Create)});
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.defineTopicCode(named, "fresh1"), kOk);
    EXPECT_EQ(r.defineTopicCode(named, "fresh2"), kNotAuthorized);
    EXPECT_EQ(r.defineTopicCode(cl, "fresh3"), kOk) << "CREATE ON CLUSTER covers any name";
    EXPECT_EQ(r.defineTopicCode(none, "fresh4"), kNotAuthorized);
}

TEST(AuthEnforcementRegression, CreateViewAndAlterViewAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Create, ST::View, "v1"), exact(SO::Alter, ST::View, "board"),
                                   exact(SO::Read, ST::Topic, "orders")});
    const int64_t noCreate = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    EXPECT_EQ(r.createViewCode(may, "v1", r.orders), kOk);
    EXPECT_EQ(r.createViewCode(may, "v2", r.orders), kNotAuthorized);
    EXPECT_EQ(r.createViewCode(noCreate, "v1b", r.orders), kNotAuthorized);
    EXPECT_NE(r.alterViewCode(may, "board", r.orders), kNotAuthorized);
    EXPECT_EQ(r.alterViewCode(noCreate, "board", r.orders), kNotAuthorized);
}

TEST(AuthEnforcementRegression, CreateViewOverAnUnreadableSourceIsNotAuthorizedRegression) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Create, ST::View, "leak"), exact(SO::Read, ST::Topic, "orders")});
    EXPECT_EQ(r.createViewCode(may, "leak", r.secret), kNotAuthorized) << "D19: a view reads its sources";
    EXPECT_EQ(r.createViewCode(may, "leak", r.orders), kOk);
    const int64_t alterer = r.newUser({exact(SO::Alter, ST::View, "board"), exact(SO::Read, ST::Topic, "orders")});
    EXPECT_EQ(r.alterViewCode(alterer, "board", r.secret), kNotAuthorized) << "AlterView likewise";
}

TEST(AuthEnforcementRegression, AlterTopicAndAlterTopicConfigAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Alter, ST::Topic, "orders")});
    const int64_t none = r.newUser({});
    EXPECT_NE(r.alterTopicCode(may, "orders"), kNotAuthorized);
    EXPECT_EQ(r.alterConfigCode(may, "orders"), kOk);
    EXPECT_EQ(r.alterTopicCode(may, "secret"), kNotAuthorized);
    EXPECT_EQ(r.alterConfigCode(may, "secret"), kNotAuthorized);
    EXPECT_EQ(r.alterTopicCode(none, "orders"), kNotAuthorized);
    EXPECT_EQ(r.alterConfigCode(none, "orders"), kNotAuthorized);
}

TEST(AuthEnforcementRegression, DropTopicAndDropViewAllowedAndDenied) {
    AuthRig r;
    ASSERT_GE(r.defineTopic(r.builder, "doomed"), 0);
    ASSERT_GE(r.createView(r.builder, "doomedview", r.orders), 0);
    const int64_t may = r.newUser({exact(SO::Drop, ST::Topic, "doomed"), exact(SO::Drop, ST::View, "doomedview")});
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.dropTopicCode(none, "doomed"), kNotAuthorized);
    EXPECT_EQ(r.dropViewCode(none, "doomedview"), kNotAuthorized);
    EXPECT_EQ(r.dropTopicCode(may, "orders"), kNotAuthorized);
    EXPECT_EQ(r.dropTopicCode(may, "doomed"), kOk);
    EXPECT_EQ(r.dropViewCode(may, "doomedview"), kOk);
}

TEST(AuthEnforcementRegression, SecurityDdlAllowedAndDenied) {
    AuthRig r;
    const int64_t admin = r.newUser({cluster(SO::Admin)});
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.wire().createRole(r.ch(), admin, "r_ok").errorCode, 0u);
    const auto denied = r.wire().createRole(r.ch(), none, "r_no");
    EXPECT_EQ(denied.errorCode, 45u);
    EXPECT_EQ(denied.detail, "not authorized: ADMIN on CLUSTER") << "D23: the DdlResponse carries the text";
}

TEST(AuthEnforcementRegression, SelfPasswordChangeNeedsNoAdmin) {
    AuthRig r;
    std::string name;
    const int64_t self = r.newUser({}, &name);
    EXPECT_EQ(r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::AddPassword, "second-password-123").errorCode, 0u);
    EXPECT_EQ(r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::SetPassword, "third-password-1234").errorCode, 0u);
    EXPECT_EQ(r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::Disable).errorCode, 45u)
        << "only password changes are the user's own";
    EXPECT_EQ(r.wire().alterPassword(r.ch(), self, "builder", pc::UserChangeKind::SetPassword, "hijack-password-123").errorCode, 45u)
        << "another user's password needs ADMIN";
}

TEST(AuthEnforcementRegression, AdminClusterGrantsNoDataAccess) {
    AuthRig r;
    const int64_t admin = r.newUser({cluster(SO::Admin)});
    EXPECT_EQ(r.produce(admin, r.orders), kNotAuthorized);
    EXPECT_EQ(r.subscribe(admin, r.orders), kNotAuthorized);
    EXPECT_EQ(r.getTopicSchema(admin, "orders"), kNotAuthorized);
    EXPECT_EQ(r.catalogSubscribe(admin), kNotAuthorized);
    EXPECT_TRUE(r.metadataTopics(admin).empty());
}

TEST(AuthEnforcementRegression, DescribeOnClusterTurnsAMissingIdIntoUnknownTopicRegression) {   // D22
    AuthRig r;
    const int64_t describe = r.newUser({cluster(SO::Describe)});
    const int64_t none = r.newUser({});
    constexpr int32_t kMissing = 99'999;
    EXPECT_EQ(r.subscribe(describe, kMissing), kUnknownTopic);
    EXPECT_EQ(r.produce(describe, kMissing), kUnknownTopic);
    EXPECT_EQ(r.subscribe(none, kMissing), kNotAuthorized);
    EXPECT_EQ(r.produce(none, kMissing), kNotAuthorized);
    EXPECT_EQ(r.subscribe(describe, r.secret), kNotAuthorized) << "an existing topic it may not READ is still refused";
}

TEST(AuthEnforcementRegression, AnObjectCreatedAfterLoginIsCoveredByAPrefixRegression) {
    AuthRig r;
    const int64_t world = r.newUser({prefix(SO::Write, ST::Topic, "world.")});
    EXPECT_EQ(r.defineTopicCode(r.builder, "world.zone1"), kOk);
    const int32_t id = [&] {
        const auto a = r.c().awaitSchema(r.c().sendGetTopicSchema(r.ch(), r.stream(), r.builder, "world.zone1"), AuthRig::dl());
        return a ? a->topicId : -1;
    }();
    ASSERT_GE(id, 0);
    EXPECT_EQ(r.produce(world, id, /*untilOk=*/true), kOk) << "the table was extended when the topic was applied";
    EXPECT_EQ(r.defineTopicCode(r.builder, "other.zone"), kOk);
    const auto other = r.c().awaitSchema(r.c().sendGetTopicSchema(r.ch(), r.stream(), r.builder, "other.zone"), AuthRig::dl());
    ASSERT_TRUE(other.has_value());
    EXPECT_EQ(r.produce(world, other->topicId), kNotAuthorized) << "a non-matching name is not covered";
}

TEST(AuthEnforcementRegression, ADroppedTopicsIdLosesItsBitsRegression) {
    AuthRig r;
    ASSERT_GE(r.defineTopic(r.builder, "world.gone"), 0);
    const int64_t world = r.newUser({prefix(SO::Write, ST::Topic, "world.")});
    const auto a = r.c().awaitSchema(r.c().sendGetTopicSchema(r.ch(), r.stream(), r.builder, "world.gone"), AuthRig::dl());
    ASSERT_TRUE(a.has_value());
    const int32_t id = a->topicId;
    EXPECT_EQ(r.produce(world, id, /*untilOk=*/true), kOk);
    EXPECT_EQ(r.dropTopicCode(r.builder, "world.gone"), kOk);
    EXPECT_NE(r.produce(world, id), kOk);
    EXPECT_EQ(r.produce(world, id), kNotAuthorized) << "a dropped id is no longer live: D22, no DESCRIBE ON CLUSTER";
}

TEST(AuthEnforcementRegression, TheUnauthorizedAnswerCounterAndLogNameTheTextRegression) {
    LogCapture log;
    AuthRig r;
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.subscribe(none, r.orders), kNotAuthorized);
    EXPECT_GE(log.count("not authorized: READ on TOPIC"), 1) << "the broker says why in its own log (the wire says only the D23 text)";
}

TEST(AuthEnforcementRegression, ObjectsThatExistedAtBootAreCoveredAfterARestartRegression) {
    AuthRig r;
    std::string reader, world;
    (void)r.newUser({exact(SO::Read, ST::Topic, "orders"), exact(SO::Create, ST::Topic, "newone")}, &reader);
    r.broker().createUser("worlder", AuthRig::kPw, {prefix(SO::Write, ST::Topic, "orders")});
    r.restart();   // the catalog replays at boot: "orders", "secret" and the view "board" are boot objects
    const int64_t sid = r.login(reader);
    ASSERT_TRUE(r.c().subscribeUntilBound(r.ch(), r.stream(), sid, r.orders, 0, 0, AuthRig::dl(20)))
        << "the boot ObjectIndex reached the gateway: the READ bit of a topic created before the restart";
    EXPECT_EQ(r.subscribe(sid, r.secret), kNotAuthorized);
    const int64_t w = r.login("worlder");
    EXPECT_EQ(r.produce(w, r.orders, /*untilOk=*/true), kOk) << "a prefix grant covers a boot topic";
    EXPECT_EQ(r.produce(w, r.secret), kNotAuthorized);
    const int64_t viewReader = [&] { r.broker().createUser("vr", AuthRig::kPw, {exact(SO::Read, ST::View, "board")}); return r.login("vr"); }();
    EXPECT_NE(r.getViewRow(viewReader, r.board), kNotAuthorized) << "a boot view sink is typed as a VIEW";
}
