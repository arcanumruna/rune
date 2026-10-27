// AuthEnforcementTest.cpp - authentication Task 12 against ONE real authenticating BrokerNode (real Aeron UDP, raw
// frames): every row of the §2.1 enforcement map, granted -> the request's normal answer, not granted -> code 45 in the
// request's OWN response type.  A user per test, holding exactly the grants the row names.
#include "support/AuthRig.h"
#include <rune/protocol/ProducerIdBinding.h>

#include <algorithm>
#include <sys/stat.h>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;

namespace {
constexpr int kOk = 0;
constexpr int kUnknownTopic = static_cast<int>(pc::ErrorCode::UnknownTopic);
constexpr int kUnknownMember = static_cast<int>(pc::ErrorCode::UnknownMember);
}  // namespace

TEST(AuthEnforcementRegression, ProduceAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t onlyRead = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.produce(may, r.orders, /*untilOk=*/true), kOk);
    EXPECT_EQ(r.produceDenied(onlyRead, r.orders), kNotAuthorized) << "READ is not WRITE";
    EXPECT_EQ(r.produceDenied(none, r.orders), kNotAuthorized) << "default deny";
    EXPECT_EQ(r.produceDenied(may, r.secret), kNotAuthorized) << "WRITE on one topic is not WRITE on another";
}

TEST(AuthEnforcementRegression, SubscribeAllowedAndDenied) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t none = r.newUser({});
    ASSERT_TRUE(r.c().subscribeUntilBound(r.ch(), r.stream(), may, r.orders, 0, 0, AuthRig::dl(15)));
    EXPECT_EQ(r.subscribeDenied(none, r.orders), kNotAuthorized);
    EXPECT_EQ(r.subscribeDenied(may, r.secret), kNotAuthorized);
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
    EXPECT_EQ(r.subscribeDenied(describe, rune::protocol::client::kSecurityTopicId), kNotAuthorized);
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

// Issue #123: LeaveGroup needs READ on the group and evicts only the caller's own member.  A zero-grant user is refused
// 45; a user holding READ on the group but naming another session's member is answered UnknownMember - exactly as for
// a member that does not exist - and the victim stays a member.
TEST(AuthEnforcementRegression, LeaveGroupByAnotherSessionOrWithoutReadIsRefusedRegression) {
    AuthRig r;
    const int64_t ws1 = r.newUser({exact(SO::Read, ST::Group, "world-7"), exact(SO::Read, ST::Topic, "orders")});
    const int64_t m = r.newUser({});
    const int64_t n = r.newUser({exact(SO::Read, ST::Group, "world-7")});
    const auto [member, gen] = r.joinStable(ws1, "world-7", {r.orders});
    ASSERT_FALSE(member.empty());
    EXPECT_EQ(r.leaveGroup(m, "world-7", member), kNotAuthorized) << "LeaveGroup needs READ on the group";
    EXPECT_EQ(r.leaveGroup(n, "world-7", member), kUnknownMember) << "another session's member is not the caller's";
    EXPECT_EQ(r.leaveGroup(n, "world-7", "world-7-1-00000000000000000000000000000000"), kUnknownMember)
        << "the same answer as for a member that does not exist";
    EXPECT_EQ(r.heartbeatAt(ws1, "world-7", member, gen), kOk) << "the victim is still a member at its generation";
    EXPECT_EQ(r.leaveGroup(ws1, "world-7", member), kOk) << "the owner leaves";
}

// Issue #124: users u1 and u2 share READ on group g; u1 is member M.  u2 naming M on Heartbeat, Sync and OffsetCommit
// is answered UnknownMember (as for a member that does not exist) and changes nothing; a JoinGroup naming M joins u2
// as a NEW member.
TEST(AuthEnforcementRegression, AnotherPrincipalsMemberIdIsRefusedOnEveryGroupRequestRegression) {
    AuthRig r;
    const std::vector<AuthGrant> g{exact(SO::Read, ST::Group, "g"), exact(SO::Read, ST::Topic, "orders")};
    const int64_t u1 = r.newUser(g);
    const int64_t u2 = r.newUser(g);
    const auto [member, gen] = r.joinStable(u1, "g", {r.orders});
    ASSERT_FALSE(member.empty());
    ASSERT_EQ(r.commitAs(u1, "g", member, gen, r.orders, 100), kOk);
    EXPECT_EQ(r.heartbeatAt(u2, "g", member, gen), kUnknownMember);
    EXPECT_EQ(r.syncGroup(u2, "g", member, gen), kUnknownMember) << "another member's assignment is not handed out";
    EXPECT_EQ(r.commitAs(u2, "g", member, gen, r.orders, 500), kUnknownMember);
    EXPECT_EQ(r.committedOffset(u1, "g", r.orders), 100) << "the foreign commit moved the group's position";
    EXPECT_EQ(r.syncGroup(u1, "g", member, gen), kOk);
    const auto [code, got] = r.rejoin(u2, "g", member, {r.orders});
    EXPECT_EQ(code, kOk);
    EXPECT_FALSE(got.empty());
    EXPECT_NE(got, member) << "another principal took over the member";
    EXPECT_NE(r.heartbeatAt(u1, "g", member, gen), kUnknownMember) << "u1 still owns its member (now rebalancing)";
}

// Issue #276: SyncGroup needs READ on the group (D18), like every other group request - READ on the topic alone does
// not hand out a member's assignment, whosever member id the request names.  RED: delete the SyncGroup case of
// ClientGateway::denyGroupRequest_ (the request reaches the coordinator: UnknownMember / OK, never 45).
TEST(AuthEnforcementRegression, SyncGroupIsDeniedWithoutGroupReadRegression) {
    AuthRig r;
    const int64_t may = r.newUser({exact(SO::Read, ST::Group, "cg276"), exact(SO::Read, ST::Topic, "orders")});
    const int64_t topicOnly = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t otherGroup = r.newUser({exact(SO::Read, ST::Group, "cg-other"), exact(SO::Read, ST::Topic, "orders")});
    const auto [member, gen] = r.joinStable(may, "cg276", {r.orders});
    ASSERT_FALSE(member.empty());
    EXPECT_EQ(r.syncGroup(topicOnly, "cg276", member, gen), kNotAuthorized) << "topic READ is not group READ";
    EXPECT_EQ(r.syncGroup(topicOnly, "cg276", "nobody", gen), kNotAuthorized) << "denied before any member lookup";
    EXPECT_EQ(r.syncGroup(otherGroup, "cg276", member, gen), kNotAuthorized) << "READ on another group";
    EXPECT_EQ(r.syncGroup(may, "cg276", member, gen), kOk) << "the owner with group READ is served";
}

// Issue #276 (X036 over an authenticated wire): a client-built CONTROL batch (a forged COMMIT/ABORT marker) is refused
// InvalidRequest even for a principal holding WRITE on the topic, and nothing is appended - the next ordinary batch
// lands at the offset right after the previous one.  RED: delete the refusesClientBatches check in
// ClientGateway::handleProduce (the marker is appended: answer 0, and the next batch's offset moves by 2).
// PR #569 adversarial review (#276, input bounds on the group name): group READ is matched on the request's OWN group
// name for every group request - a prefix grant opens only names under it (case-sensitive, no shorter name, no name
// that merely contains it), and an empty group name is never served.  The owner's own requests stay served.
// RED: delete the SyncGroup case of ClientGateway::denyGroupRequest_ (the out-of-grant SyncGroups reach the
// coordinator and are answered UnknownMember, not 45).
TEST(AuthEnforcementRegression, GroupReadIsMatchedOnTheRequestsOwnGroupNameForEveryGroupRequestAdversarialRegression) {
    AuthRig r;
    const int64_t owner = r.newUser({prefix(SO::Read, ST::Group, "cg27"), exact(SO::Read, ST::Topic, "orders")});
    const auto [member, gen] = r.joinStable(owner, "cg276", {r.orders});
    ASSERT_FALSE(member.empty());
    for (const char* g : {"cg2", "CG276", "Cg276", "xcg276", "c", "cg2 76"}) {
        EXPECT_EQ(r.syncGroup(owner, g, member, gen), kNotAuthorized) << "SyncGroup " << g;
        EXPECT_EQ(r.heartbeatAt(owner, g, member, gen), kNotAuthorized) << "Heartbeat " << g;
        EXPECT_EQ(r.leaveGroup(owner, g, member), kNotAuthorized) << "LeaveGroup " << g;
        EXPECT_EQ(r.commitAs(owner, g, member, gen, r.orders, 3), kNotAuthorized) << "OffsetCommit " << g;
        EXPECT_EQ(r.offsetFetch(owner, g, r.orders), kNotAuthorized) << "OffsetFetch " << g;
        EXPECT_EQ(r.joinGroup(owner, g, {r.orders}), kNotAuthorized) << "JoinGroup " << g;
    }
    EXPECT_NE(r.syncGroup(owner, "", member, gen), kOk) << "an empty group name was served";
    EXPECT_NE(r.heartbeatAt(owner, "", member, gen), kOk) << "an empty group name was served";
    EXPECT_EQ(r.syncGroup(owner, "cg276", member, gen), kOk) << "the owner's own group stays served";
    EXPECT_EQ(r.heartbeatAt(owner, "cg276", member, gen), kOk);
}

TEST(AuthEnforcementRegression, AClientControlBatchIsRefusedForAnAuthorizedWriterRegression) {
    AuthRig r;
    const int64_t w = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(r.produce(w, r.orders, /*untilOk=*/true), kOk);
    auto produceBatch = [&](const std::vector<uint8_t>& batch) -> std::pair<int, int64_t> {
        const int64_t corr = r.c().nextCorrelationId();
        if (!r.c().produce(r.ch(), r.stream(), w, corr, r.orders, 0, 1, batch)) return {kNoAnswer, -1};
        const auto a = r.c().awaitProduceResponseUntil(corr, AuthRig::dl());
        return a ? std::pair<int, int64_t>{a->errorCode, a->baseOffset} : std::pair<int, int64_t>{kNoAnswer, -1};
    };
    const auto first = produceBatch(makeBatch(1));
    ASSERT_EQ(first.first, kOk);
    rune::protocol::RecordBatchBuilder ctl(0, 1000);
    ctl.setControl(true);
    ctl.setProducerId(7); ctl.setProducerEpoch(0); ctl.setBaseSequence(0);
    ctl.append("", "", 1000);
    EXPECT_EQ(produceBatch(ctl.build()).first, static_cast<int>(pc::ErrorCode::InvalidRequest))
        << "a client never writes a control batch, whatever its grants";
    const auto next = produceBatch(makeBatch(1));
    ASSERT_EQ(next.first, kOk);
    EXPECT_EQ(next.second, first.second + 1) << "the refused control batch was appended";
}

TEST(AuthEnforcementRegression, TransactionRowsAllowedAndDenied) {
    AuthRig r;
    // Issue #125 correction (not a weakening): TxnOffsetCommit needs READ on every listed topic, exactly as
    // OffsetCommit does - this row used to pin the hole (WRITE alone let `may` commit offsets of `orders`).  `may`
    // now holds READ on `orders` too; TxnOffsetCommitNeedsReadOnEveryTopicRegression pins the refusal without it.
    const int64_t may = r.newUser({exact(SO::Write, ST::TransactionalId, "tx1"), exact(SO::Write, ST::Topic, "orders"),
                                   exact(SO::Read, ST::Topic, "orders"), exact(SO::Read, ST::Group, "cg")});
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

// #211 (Kafka's rule; replaces the #277 pin "needs no grant"): a non-transactional InitProducerId needs WRITE on at
// least one topic - literal, prefixed or wildcard.  A zero-grant (or READ-only) principal is refused 45 and mints no
// producer id (each id costs pid space; a replicated block commit every 1000), and a live REVOKE of its last WRITE
// refuses it again at once.  RED: drop the anyTopicWrite check in ClientGateway::denyTxnRequest_ (every call is OK).
TEST(AuthTxnRegression, NonTransactionalInitProducerIdNeedsAWriteGrantRegression) {
    using SP = rune::protocol::SecurityPatternKind;
    AuthRig r;
    const int64_t none = r.newUser({});
    const int64_t reader = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    std::string writerName;
    const int64_t writer = r.newUser({exact(SO::Write, ST::Topic, "orders")}, &writerName);
    const int64_t prefixed = r.newUser({prefix(SO::Write, ST::Topic, "ord")});

    const auto first = r.initProducer(writer);
    ASSERT_EQ(first.code, kOk) << "WRITE on one topic";
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(r.initProducerId(none, ""), kNotAuthorized) << "zero grants";
        EXPECT_EQ(r.initProducerId(reader, ""), kNotAuthorized) << "READ is not WRITE";
    }
    const auto second = r.initProducer(prefixed);
    ASSERT_EQ(second.code, kOk) << "a prefixed WRITE";
    EXPECT_EQ(rune::protocol::producerIdCounter(second.pid), rune::protocol::producerIdCounter(first.pid) + 1)
        << "a refused InitProducerId allocated a producer id";

    auto& admin = r.broker().admin();
    ASSERT_EQ(admin.revoke(r.ch(), r.broker().adminSession(), writerName, false, SO::Write, ST::Topic, SP::Exact,
                           "orders").errorCode, 0u);
    EXPECT_EQ(r.initProducerId(writer, ""), kNotAuthorized) << "after a live REVOKE of the last WRITE";
    EXPECT_EQ(r.initProducer(prefixed).code, kOk) << "the other principal is untouched";
}

// PR #609 adversarial review (#211, auth class): WRITE on some topic is what counts, however it is held.  Grants of
// every other kind - WRITE on a transactional id, READ on a group, CREATE/ALTER/DESCRIBE on CLUSTER, CREATE on a
// topic - never mint a non-transactional producer id (ADMIN on CLUSTER is root, spec §2: AdminClusterIsRootRegression);
// WRITE held only through a ROLE does, and revoking that role mid-session refuses the very next one.  RED: drop the #211 check in ClientGateway::denyTxnRequest_.
TEST(AuthTxnRegression, NonTransactionalInitProducerIdCountsOnlyTopicWriteHoweverHeldAdversarialRegression) {
    using SP = rune::protocol::SecurityPatternKind;
    AuthRig r;
    const int64_t others = r.newUser({exact(SO::Write, ST::TransactionalId, "tx1"), exact(SO::Read, ST::Group, "cg"),
                                      exact(SO::Create, ST::Topic, "orders"), exact(SO::Read, ST::Topic, "orders"),
                                      exact(SO::Create, ST::Cluster, ""), exact(SO::Alter, ST::Cluster, ""),
                                      exact(SO::Describe, ST::Cluster, "")});
    EXPECT_EQ(r.initProducerId(others, ""), kNotAuthorized) << "no grant of WRITE on a topic";

    AuthWireClient& aw = r.broker().admin();
    const int64_t as = r.broker().adminSession();
    ASSERT_EQ(aw.createRole(r.ch(), as, "writers211").errorCode, 0u);
    ASSERT_EQ(aw.grant(r.ch(), as, "writers211", true, SO::Write, ST::Topic, SP::Prefix, "ord").errorCode, 0u);
    std::string name;
    const int64_t viaRole = r.newUser({}, &name);
    EXPECT_EQ(r.initProducerId(viaRole, ""), kNotAuthorized) << "precondition: no grant yet";
    ASSERT_EQ(aw.grantRole(r.ch(), as, name, "writers211").errorCode, 0u);
    EXPECT_EQ(r.initProducerId(viaRole, ""), kOk) << "WRITE held through a role";
    ASSERT_EQ(aw.revokeRole(r.ch(), as, name, "writers211").errorCode, 0u);
    EXPECT_EQ(r.initProducerId(viaRole, ""), kNotAuthorized) << "the role revoked mid-session";
}

// Issue #122: a producer id is bound to the principal it was issued to.  Users m and ws2 both hold WRITE on the topic;
// ws2 runs an idempotent producer P.  m naming P is refused 45 whatever it sends - a higher epoch (a fence), ws2's next
// sequence (a batch that would make ws2's real batch a "duplicate" and silently drop it), or a transactional batch
// (pinning the partition's LSO) - and ws2's producer carries on untouched.
namespace {
struct ForeignPidRig {
    AuthRig r;
    int64_t ws2 = 0, m = 0;
    AuthRig::ProducerIds p;
    int64_t firstOffset = -1;
    ForeignPidRig() {
        ws2 = r.newUser({exact(SO::Write, ST::Topic, "orders")});
        m = r.newUser({exact(SO::Write, ST::Topic, "orders")});
        p = r.initProducer(ws2);
        EXPECT_EQ(p.code, kOk);
        const auto a = r.produceAs(ws2, r.orders, p.pid, p.epoch, 0);
        EXPECT_EQ(a.code, kOk);
        firstOffset = a.baseOffset;
    }
};
}  // namespace

TEST(AuthEnforcementRegression, ForeignPrincipalCannotFenceAProducerRegression) {
    ForeignPidRig f;
    EXPECT_EQ(f.r.produceAs(f.m, f.r.orders, f.p.pid, static_cast<int16_t>(f.p.epoch + 1), 0).code, kNotAuthorized);
    const auto next = f.r.produceAs(f.ws2, f.r.orders, f.p.pid, f.p.epoch, 1);
    EXPECT_EQ(next.code, kOk) << "the owner's producer was fenced by another principal";
    EXPECT_GT(next.baseOffset, f.firstOffset);
}

TEST(AuthEnforcementRegression, ForeignPrincipalCannotSwallowAProducersBatchRegression) {
    ForeignPidRig f;
    EXPECT_EQ(f.r.produceAs(f.m, f.r.orders, f.p.pid, f.p.epoch, 1, false, "junk").code, kNotAuthorized);
    // A plain record in between: a batch answered as a duplicate of an EARLIER one gets an offset below it.
    const auto plain = f.r.produceAs(f.m, f.r.orders, -1, 0, -1);
    ASSERT_EQ(plain.code, kOk);
    const auto real = f.r.produceAs(f.ws2, f.r.orders, f.p.pid, f.p.epoch, 1, false, "real");
    EXPECT_EQ(real.code, kOk);
    EXPECT_GT(real.baseOffset, plain.baseOffset) << "the owner's batch was answered as a duplicate and never stored";
}

TEST(AuthEnforcementRegression, ForeignPrincipalCannotPinTheLsoWithAnotherPrincipalsPidRegression) {
    ForeignPidRig f;
    EXPECT_EQ(f.r.produceAs(f.m, f.r.orders, f.p.pid, f.p.epoch, 1, /*transactional=*/true).code, kNotAuthorized);
    // m's own id is m's to use (an unregistered transactional batch under one's OWN id is issue #112).
    const auto own = f.r.initProducer(f.m);
    ASSERT_EQ(own.code, kOk);
    EXPECT_NE(own.pid, f.p.pid);
    EXPECT_EQ(f.r.produceAs(f.m, f.r.orders, own.pid, own.epoch, 0).code, kOk);
    EXPECT_EQ(f.r.produceAs(f.ws2, f.r.orders, own.pid, own.epoch, 1).code, kNotAuthorized) << "binding is mutual";
    EXPECT_EQ(f.r.produceAs(f.ws2, f.r.orders, f.p.pid, f.p.epoch, 1).code, kOk);
}

// Issue #122 on a sharded broker: the worker that receives a Produce directly (per-worker ingress) checks the id
// against the principal shipped with the session's grants; the gateway-routed path checks it at the gateway.
TEST(AuthEnforcementRegression, ForeignPidIsRefusedOnWorkerIngressAndTheRoutedPathRegression) {
    AuthBroker::Options o;
    o.shard = true;
    AuthRig r("pidshard", o);
    const int64_t ws2 = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t m = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const std::string worker = r.broker().workerIngressChannel();
    const auto p = r.initProducer(ws2);
    ASSERT_EQ(p.code, kOk);
    const auto first = r.produceAs(ws2, r.orders, p.pid, p.epoch, 0, false, "v", &worker);
    ASSERT_EQ(first.code, kOk) << "the owner produces over worker ingress";
    EXPECT_EQ(r.produceAs(m, r.orders, p.pid, static_cast<int16_t>(p.epoch + 1), 0, false, "v", &worker).code, kNotAuthorized);
    EXPECT_EQ(r.produceAs(m, r.orders, p.pid, p.epoch, 1, false, "junk", &worker).code, kNotAuthorized);
    EXPECT_EQ(r.produceAs(m, r.orders, p.pid, p.epoch, 1, /*transactional=*/true).code, kNotAuthorized) << "gateway-routed";
    const auto next = r.produceAs(ws2, r.orders, p.pid, p.epoch, 1, false, "real", &worker);
    EXPECT_EQ(next.code, kOk) << "the owner's producer was fenced or its batch swallowed";
    EXPECT_GT(next.baseOffset, first.baseOffset);
    const auto own = r.initProducer(m);
    ASSERT_EQ(own.code, kOk);
    EXPECT_EQ(r.produceAs(m, r.orders, own.pid, own.epoch, 0, false, "v", &worker).code, kOk) << "m's own id is m's";
}

// Issue #122 on the transaction requests: a principal holding WRITE on a shared transactional id gets an id of its
// OWN at InitProducerId (the previous holder is fenced), and naming the other principal's id on AddPartitions /
// EndTxn is answered exactly like any wrong producer id.
TEST(AuthEnforcementRegression, ASharedTransactionalIdIsReboundToEachPrincipalRegression) {
    AuthRig r;
    const std::vector<AuthGrant> g{exact(SO::Write, ST::TransactionalId, "tx-shared"), exact(SO::Write, ST::Topic, "orders")};
    const int64_t a = r.newUser(g);
    const int64_t b = r.newUser(g);
    const auto pa = r.initProducer(a, "tx-shared");
    ASSERT_EQ(pa.code, kOk);
    const auto pa2 = r.initProducer(a, "tx-shared");
    ASSERT_EQ(pa2.code, kOk);
    EXPECT_EQ(pa2.pid, pa.pid) << "the same principal keeps its id (epoch bump)";
    EXPECT_EQ(pa2.epoch, pa.epoch + 1);
    const auto pb = r.initProducer(b, "tx-shared");
    ASSERT_EQ(pb.code, kOk);
    EXPECT_NE(pb.pid, pa.pid) << "another principal was handed the first principal's producer id";
    EXPECT_EQ(pb.epoch, 0);
    EXPECT_EQ(r.produceAs(b, r.orders, pa.pid, pa2.epoch, 0).code, kNotAuthorized);
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

// Issue #273 item 4: GetTopicSchema across topic kinds - a data topic, a view (its sink) and the system topics.  Only the
// object's OWN kind's grant answers it (#128): READ on a view named like the topic, or on a topic named like the view,
// opens nothing; READ on a view's source topic does not open the view.  A system topic is never reachable through a
// topic / view grant (even READ TOPIC prefix ''), and DESCRIBE ON CLUSTER sees it as missing (UnknownTopic), as any
// name that is no catalog object.  RED: drop the own-kind branch (#128) of authorizeTopicOrViewName - the wrong-kind
// grants are served.
TEST(AuthEnforcementRegression, GetTopicSchemaAnswersPerTheObjectsOwnKindAcrossTopicKindsRegression) {
    AuthRig r;
    const int kUnknown = static_cast<int>(pc::ErrorCode::UnknownTopic);
    const int64_t readOrders = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t viewNamedOrders = r.newUser({exact(SO::Read, ST::View, "orders")});
    const int64_t readBoard = r.newUser({exact(SO::Read, ST::View, "board")});
    const int64_t topicNamedBoard = r.newUser({exact(SO::Read, ST::Topic, "board")});
    const int64_t allTopics = r.newUser({prefix(SO::Read, ST::Topic, "")});
    const int64_t allViews = r.newUser({prefix(SO::Read, ST::View, "")});
    const int64_t describeCluster = r.newUser({cluster(SO::Describe)});
    const int64_t adminCluster = r.newUser({cluster(SO::Admin)});

    // A data topic.
    EXPECT_EQ(r.getTopicSchema(readOrders, "orders"), kOk);
    EXPECT_EQ(r.getTopicSchema(allTopics, "orders"), kOk);
    EXPECT_EQ(r.getTopicSchema(viewNamedOrders, "orders"), kNotAuthorized) << "a VIEW grant on a topic's name";
    EXPECT_EQ(r.getTopicSchema(allViews, "orders"), kNotAuthorized);
    // A view (its sink topic is addressed by the view's name).
    EXPECT_EQ(r.getTopicSchema(readBoard, "board"), kOk);
    EXPECT_EQ(r.getTopicSchema(allViews, "board"), kOk);
    EXPECT_EQ(r.getTopicSchema(topicNamedBoard, "board"), kNotAuthorized) << "a TOPIC grant on a view's name";
    EXPECT_EQ(r.getTopicSchema(allTopics, "board"), kNotAuthorized);
    EXPECT_EQ(r.getTopicSchema(readOrders, "board"), kNotAuthorized) << "READ on the source is not READ on the view";
    // The system topics.
    for (const char* sys : {"__security", "__catalog", "__consumer_offsets", "__transaction_state"}) {
        EXPECT_EQ(r.getTopicSchema(allTopics, sys), kNotAuthorized) << sys;
        EXPECT_EQ(r.getTopicSchema(allViews, sys), kNotAuthorized) << sys;
        EXPECT_EQ(r.getTopicSchema(adminCluster, sys), kUnknown) << sys << ": root, but no catalog object (I6 keeps no data grant)";
        EXPECT_EQ(r.getTopicSchema(describeCluster, sys), kUnknown) << sys << ": no catalog object";
    }
    EXPECT_EQ(r.getTopicSchema(describeCluster, "orders"), kOk);
    EXPECT_EQ(r.getTopicSchema(describeCluster, "board"), kOk);
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

// Issue #129: a DefineTopic with a descriptor on an EXISTING untyped topic is a new schema version of a live object
// (CreateSchemaOps), not a creation: it needs ALTER on that topic (which implies DESCRIBE, D15), not only CREATE on
// the name.
TEST(AuthEnforcementRegression, CreateSchemaOnAnExistingTopicNeedsAlterRegression) {
    AuthRig r;
    ASSERT_GE(r.defineTopic(r.builder, "world.chat", /*typed=*/false), 0);   // untyped, owned by the builder
    auto schemaVersion = [&] {
        const auto a = r.c().awaitSchema(r.c().sendGetTopicSchema(r.ch(), r.stream(), r.builder, "world.chat"), AuthRig::dl());
        return a ? static_cast<int>(a->version) : kNoAnswer;
    };
    const int before = schemaVersion();
    const int64_t creator = r.newUser({prefix(SO::Create, ST::Topic, "world.")});
    const int64_t describer = r.newUser({prefix(SO::Create, ST::Topic, "world."), exact(SO::Describe, ST::Topic, "world.chat")});
    const int64_t alterer = r.newUser({prefix(SO::Create, ST::Topic, "world."), exact(SO::Alter, ST::Topic, "world.chat")});
    EXPECT_EQ(r.defineTopicCode(creator, "world.chat"), kNotAuthorized) << "CREATE on the name does not rewrite a live topic's schema";
    EXPECT_EQ(r.defineTopicCode(describer, "world.chat"), kNotAuthorized) << "DESCRIBE is not ALTER";
    EXPECT_EQ(schemaVersion(), before) << "the schema is unchanged";
    EXPECT_EQ(r.defineTopicCode(creator, "world.fresh"), 0) << "CREATE alone still creates a new typed topic";
    EXPECT_EQ(r.defineTopicCode(creator, "world.fresh"), static_cast<int>(pc::ErrorCode::TopicExists))
        << "a typed topic is still TopicExists, not an authorization question";
    EXPECT_EQ(r.defineTopicCode(alterer, "world.chat"), 0) << "ALTER adopts the schema";
    EXPECT_NE(schemaVersion(), before);
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

TEST(AuthEnforcementRegression, CreateAndAlterViewOverAnIdNotYetIndexedIsNotAuthorizedRegression) {
    AuthRig r;
    // The in-flight CREATE TOPIC window: the id is unknown to the gateway's index but may be live on the admin thread.
    const int64_t u = r.newUser({cluster(SO::Describe), exact(SO::Create, ST::View, "leak2"),
                                 exact(SO::Alter, ST::View, "board"), exact(SO::Read, ST::Topic, "orders")});
    constexpr int32_t kNext = 99'999;
    EXPECT_EQ(r.createViewCode(u, "leak2", kNext), kNotAuthorized) << "D22 must not apply to view sources";
    EXPECT_EQ(r.alterViewCode(u, "board", kNext), kNotAuthorized);
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

// Issue #114: a user's own ADD / DROP / SET PASSWORD (no ADMIN) needs a fresh proof of a CURRENT password, bound to the
// session's login and to the request.  The session alone - what a hijacked session can send - changes nothing.
TEST(AuthEnforcementRegression, SelfPasswordChangeNeedsAFreshProofOfTheCurrentPasswordRegression) {
    AuthRig r;
    std::string name;
    const int64_t self = r.newUser({}, &name);
    // The user itself (a proof of its current password, as the real client sends) may.
    EXPECT_EQ(r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::AddPassword, "second-password-123").errorCode, 0u);
    EXPECT_TRUE(r.broker().wire().connectAndLogin(r.ch(), name, "second-password-123").ok()) << "the proven change applied";
    // The attacker holding only the session: every own-credential change is refused and nothing reaches __security.
    const int64_t before = r.broker().node().securityViewForTest().appliedOffset;
    r.wire().signOwnCredentialChanges(false);
    for (const auto change : {pc::UserChangeKind::AddPassword, pc::UserChangeKind::SetPassword, pc::UserChangeKind::DropPassword}) {
        const auto a = r.wire().alterPassword(r.ch(), self, name, change,
                                              change == pc::UserChangeKind::DropPassword ? std::string{} : "attacker-password-123");
        EXPECT_EQ(a.errorCode, 45u) << static_cast<int>(change);
        EXPECT_NE(a.detail.find("fresh proof of a current password"), std::string::npos) << a.detail;
    }
    EXPECT_EQ(r.broker().node().securityViewForTest().appliedOffset, before) << "no event was appended";
}

// Namespaces spec §2 / plan: "the cluster admin is root" - ADMIN ON CLUSTER alone is every operation on every user
// object, data and DDL included.  Driven for a user granted only ADMIN ON CLUSTER and for the bootstrap admin itself
// (which holds nothing else), on a real broker over the wire.
TEST(AuthEnforcementRegression, AdminClusterIsRootRegression) {
    AuthRig r;
    const int64_t granted = r.newUser({cluster(SO::Admin)});
    // The bootstrap admin itself, logged in on the rig's data client (its admin-channel session answers elsewhere).
    const int64_t bootstrap = r.login("admin", AuthBroker::kAdminPassword);
    int n = 0;
    for (const int64_t admin : {granted, bootstrap}) {
        SCOPED_TRACE(admin == bootstrap ? "bootstrap admin" : "ADMIN ON CLUSTER");
        const std::string t = "root_t" + std::to_string(n), v = "root_v" + std::to_string(n), tx = "root_tx" + std::to_string(n),
                          cg = "root_cg" + std::to_string(n);
        ++n;
        EXPECT_EQ(r.produce(admin, r.orders), kOk);
        EXPECT_EQ(r.produce(admin, r.secret), kOk);
        EXPECT_EQ(r.subscribe(admin, r.secret), kOk);
        EXPECT_EQ(r.getTopicSchema(admin, "orders"), kOk);
        EXPECT_EQ(r.getTopicSchema(admin, "board"), kOk);
        EXPECT_NE(r.getViewRow(admin, r.board), kNotAuthorized);
        EXPECT_EQ(r.catalogSubscribe(admin), kOk);
        const auto topics = r.metadataTopics(admin);
        EXPECT_NE(std::find(topics.begin(), topics.end(), r.secret), topics.end()) << "metadata lists every user topic";
        EXPECT_EQ(r.defineTopicCode(admin, t), kOk) << "CREATE TOPIC";
        EXPECT_EQ(r.alterTopicCode(admin, t), kOk) << "ALTER TOPIC";
        EXPECT_EQ(r.createViewCode(admin, v, r.orders), kOk) << "CREATE VIEW";
        EXPECT_EQ(r.dropViewCode(admin, v), kOk) << "DROP VIEW";
        EXPECT_EQ(r.dropTopicCode(admin, t), kOk) << "DROP TOPIC";
        EXPECT_EQ(r.joinGroup(admin, cg, {r.orders}), kOk) << "groups";
        EXPECT_EQ(r.initProducerId(admin, tx), kOk) << "transactional ids";
    }
    // Issue #626: root is root in EVERY namespace - a foreign (non-default) namespace's objects, by id and by
    // qualified name, data and DDL alike, with no grant naming that namespace.
    r.createNamespace("rootns");
    const int32_t foreign = r.defineTopic(bootstrap, "rootns/owned");
    ASSERT_GE(foreign, 0) << "root defines a topic in a foreign namespace";
    const int32_t foreignView = r.createView(bootstrap, "rootns/ownedv", foreign);
    ASSERT_GE(foreignView, 0) << "root creates a view in a foreign namespace";
    n = 0;
    for (const int64_t admin : {granted, bootstrap}) {
        SCOPED_TRACE(admin == bootstrap ? "bootstrap admin, foreign ns" : "ADMIN ON CLUSTER, foreign ns");
        const std::string t = "rootns/root_t" + std::to_string(n), v = "rootns/root_v" + std::to_string(n);
        ++n;
        EXPECT_EQ(r.produce(admin, foreign, /*untilOk=*/true), kOk);
        EXPECT_EQ(r.subscribe(admin, foreign), kOk);
        EXPECT_EQ(r.getTopicSchema(admin, "rootns/owned"), kOk);
        EXPECT_EQ(r.getTopicSchema(admin, "rootns/ownedv"), kOk);
        EXPECT_NE(r.getViewRow(admin, foreignView), kNotAuthorized);
        const auto topics = r.metadataTopics(admin);
        EXPECT_NE(std::find(topics.begin(), topics.end(), foreign), topics.end()) << "metadata lists the foreign topic";
        EXPECT_EQ(r.defineTopicCode(admin, t), kOk) << "CREATE TOPIC ns/x";
        EXPECT_EQ(r.alterTopicCode(admin, t), kOk) << "ALTER TOPIC ns/x";
        EXPECT_EQ(r.alterConfigCode(admin, t), kOk) << "ALTER TOPIC ns/x CONFIG";
        EXPECT_EQ(r.createViewCode(admin, v, foreign), kOk) << "CREATE VIEW ns/v";
        EXPECT_EQ(r.dropViewCode(admin, v), kOk) << "DROP VIEW ns/v";
        EXPECT_EQ(r.dropTopicCode(admin, t), kOk) << "DROP TOPIC ns/x";
    }
}

TEST(AuthEnforcementRegression, DescribeOnClusterTurnsAMissingIdIntoUnknownTopicRegression) {   // D22
    AuthRig r;
    const int64_t describe = r.newUser({cluster(SO::Describe)});
    const int64_t none = r.newUser({});
    constexpr int32_t kMissing = 99'999;
    EXPECT_EQ(r.subscribe(describe, kMissing), kUnknownTopic);
    EXPECT_EQ(r.produce(describe, kMissing), kUnknownTopic);
    EXPECT_EQ(r.subscribeDenied(none, kMissing), kNotAuthorized);
    EXPECT_EQ(r.produceDenied(none, kMissing), kNotAuthorized);
    EXPECT_EQ(r.subscribeDenied(describe, r.secret), kNotAuthorized) << "an existing topic it may not READ is still refused";
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
    EXPECT_EQ(r.produceDenied(world, other->topicId), kNotAuthorized) << "a non-matching name is not covered";
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
    EXPECT_EQ(r.produceDenied(world, id), kNotAuthorized) << "a dropped id is no longer live: D22, no DESCRIBE ON CLUSTER";
}

TEST(AuthEnforcementRegression, TheUnauthorizedAnswerCounterAndLogNameTheTextRegression) {
    LogCapture log;
    AuthRig r;
    const int64_t none = r.newUser({});
    EXPECT_EQ(r.subscribeDenied(none, r.orders), kNotAuthorized);
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
    EXPECT_EQ(r.subscribeDenied(sid, r.secret), kNotAuthorized);
    const int64_t w = r.login("worlder");
    EXPECT_EQ(r.produce(w, r.orders, /*untilOk=*/true), kOk) << "a prefix grant covers a boot topic";
    EXPECT_EQ(r.produceDenied(w, r.secret), kNotAuthorized);
    const int64_t viewReader = [&] { r.broker().createUser("vr", AuthRig::kPw, {exact(SO::Read, ST::View, "board")}); return r.login("vr"); }();
    EXPECT_NE(r.getViewRow(viewReader, r.board), kNotAuthorized) << "a boot view sink is typed as a VIEW";
}

// ── Adversarial: DefineTopic on existing names (the area of #129) ───────────────────────────────────────────────────
// A CREATE-only user (CREATE ON CLUSTER: every name) cannot reach a live object's schema through any spelling: the
// same name with no descriptor, a malformed descriptor, a transient define over the disk topic, a name that differs
// only in case, a view's name - and after a restart (objects known at boot).  None of them changes the schema.
TEST(AuthEnforcementRegression, ACreateOnlyUserReachesNoLiveSchemaThroughAnyNameFormAdversarialRegression) {
    AuthRig r;
    ASSERT_GE(r.defineTopic(r.builder, "world.chat", /*typed=*/false), 0);
    auto version = [&] {
        const auto a = r.c().awaitSchema(r.c().sendGetTopicSchema(r.ch(), r.stream(), r.builder, "world.chat"), AuthRig::dl());
        return a ? static_cast<int>(a->version) : kNoAnswer;
    };
    const int before = version();
    std::string name;
    int64_t creator = r.newUser({cluster(SO::Create)}, &name);
    auto define = [&](const std::string& n, std::vector<uint8_t> blob, uint8_t persistence = 0) {
        return r.ddlCode(r.c().sendDefineTopic(r.ch(), r.stream(), creator, n, 1, blob, persistence));
    };
    const int kExists = static_cast<int>(pc::ErrorCode::TopicExists);
    const int kInvalidSchema = static_cast<int>(pc::ErrorCode::InvalidSchema);
    EXPECT_EQ(define("world.chat", authRigCharBlob()), kNotAuthorized);
    EXPECT_EQ(define("world.chat", {}), kExists) << "no descriptor: nothing to adopt";
    EXPECT_EQ(define("world.chat", {0xFF, 0x00, 0x13}), kInvalidSchema) << "a malformed descriptor never reaches adoption";
    EXPECT_EQ(define("world.chat", authRigCharBlob(), /*transient=*/1), kNotAuthorized);
    EXPECT_EQ(define("World.Chat", authRigCharBlob()), kExists) << "a fold-equal name is a collision, not the topic";
    EXPECT_EQ(define("board", authRigCharBlob()), kExists) << "a view's name";
    EXPECT_EQ(version(), before);
    r.restart();
    creator = r.login(name);
    EXPECT_EQ(define("world.chat", authRigCharBlob()), kNotAuthorized) << "after a restart";
    EXPECT_EQ(version(), before);
}

// A CREATE-only user's typed define racing the builder's untyped define of the same new name: whichever lands
// first, the creator never ends up having ADOPTED a schema onto the builder's topic (both cannot succeed).
TEST(AuthEnforcementRegression, ARacingCreateNeverAdoptsAdversarialRegression) {
    AuthRig r;
    const int64_t creator = r.newUser({prefix(SO::Create, ST::Topic, "race.")});
    for (int i = 0; i < 6; ++i) {
        const std::string n = "race.t" + std::to_string(i);
        const int64_t cb = r.c().sendDefineTopic(r.ch(), r.stream(), r.builder, n, 1, std::vector<uint8_t>{});
        const int64_t cc = r.c().sendDefineTopic(r.ch(), r.stream(), creator, n, 1, authRigCharBlob());
        const int builderCode = r.ddlCode(cb), creatorCode = r.ddlCode(cc);
        EXPECT_FALSE(builderCode == 0 && creatorCode == 0) << n << ": the creator adopted the builder's topic";
        EXPECT_TRUE((builderCode == 0 && creatorCode == kNotAuthorized) ||
                    (creatorCode == 0 && builderCode == static_cast<int>(pc::ErrorCode::TopicExists)))
            << n << " builder " << builderCode << " creator " << creatorCode;
    }
}

// ── Adversarial: a user's own credential change (the area of #114) ───────────────────────────────────────────────
// The exact signed frame replayed (same request id, same bytes) applies once: the leader dedupes it.
TEST(AuthEnforcementRegression, AReplayedSignedOwnChangeAppliesOnceAdversarialRegression) {
    AuthRig r;
    std::string name;
    const int64_t self = r.newUser({}, &name);
    const AuthWireClient::Verifier v = AuthWireClient::verifierOf("replayed-password-1");
    pc::SecurityDdl m; m.sessionId = self; m.kind = pc::SecurityOpKind::AlterUser; m.name = name;
    m.change = pc::UserChangeKind::AddPassword; m.verifier = v.wire();
    r.wire().pinRequestId(rune::protocol::RequestId{0x5151, 0x7777});
    const auto first = r.wire().securityDdl(r.ch(), m, "ALTER USER " + name + " ADD PASSWORD '<redacted>';");
    ASSERT_EQ(first.errorCode, 0u) << first.detail;
    const int64_t applied = r.broker().node().securityViewForTest().appliedOffset;
    const auto again = r.wire().securityDdl(r.ch(), m, "ALTER USER " + name + " ADD PASSWORD '<redacted>';");
    r.wire().pinRequestId(std::nullopt);
    EXPECT_EQ(again.errorCode, 0u) << "the original result: " << again.detail;
    EXPECT_EQ(r.broker().node().securityViewForTest().appliedOffset, applied) << "applied once";
    EXPECT_TRUE(r.broker().wire().connectAndLogin(r.ch(), name, "replayed-password-1").ok());
}

// The proof is of a CURRENT password: once the password a session logged in with is gone (an admin dropped it), that
// session can no longer change its own credential - a new login with a current password can.
TEST(AuthEnforcementRegression, AnOwnChangeProvenWithADroppedPasswordIsRefusedAdversarialRegression) {
    AuthRig r;
    std::string name;
    const int64_t oldSession = r.newUser({}, &name);   // logged in with AuthRig::kPw
    r.broker().addPassword(name, "the-newer-password-1");
    r.broker().dropOldestPassword(name);               // kPw is gone
    const auto refused = r.wire().alterPassword(r.ch(), oldSession, name, pc::UserChangeKind::AddPassword, "third-password-123");
    EXPECT_EQ(refused.errorCode, 45u) << refused.detail;
    const int64_t fresh = r.login(name, "the-newer-password-1");
    EXPECT_EQ(r.wire().alterPassword(r.ch(), fresh, name, pc::UserChangeKind::AddPassword, "third-password-123").errorCode, 0u);
}

// Across a restart: the old session is gone with its login nonce; the new login's proof works, and a proof made for
// the OLD session's nonce does not carry over (the session id it named no longer exists, and a new session has a new
// nonce).
TEST(AuthEnforcementRegression, OwnChangesAcrossARestartNeedTheNewLoginAdversarialRegression) {
    AuthRig r;
    std::string name;
    (void)r.newUser({}, &name);
    r.restart();
    const int64_t s = r.login(name);
    EXPECT_EQ(r.wire().alterPassword(r.ch(), s, name, pc::UserChangeKind::AddPassword, "after-restart-pw-1").errorCode, 0u);
    r.wire().signOwnCredentialChanges(false);
    EXPECT_EQ(r.wire().alterPassword(r.ch(), s, name, pc::UserChangeKind::SetPassword, "unsigned-pw-12345").errorCode, 45u);
}

// ALTER revoked while the session is open: the gateway's verdict follows the live permission table, so the very next
// adoption attempt is refused - granted again, it applies.
TEST(AuthEnforcementRegression, AdoptionFollowsAnAlterRevokedMidSessionAdversarialRegression) {
    AuthRig r;
    ASSERT_GE(r.defineTopic(r.builder, "live.t", /*typed=*/false), 0);
    std::string name;
    const int64_t s = r.newUser({prefix(SO::Create, ST::Topic, "live."), exact(SO::Alter, ST::Topic, "live.t")}, &name);
    const auto revoked = r.broker().admin().revoke(r.ch(), r.broker().adminSession(), name, false, SO::Alter, ST::Topic,
                                                  rune::protocol::SecurityPatternKind::Exact, "live.t");
    ASSERT_EQ(revoked.errorCode, 0u) << revoked.detail;
    EXPECT_EQ(r.defineTopicCode(s, "live.t"), kNotAuthorized) << "ALTER revoked mid-session";
    const auto granted = r.broker().admin().grant(r.ch(), r.broker().adminSession(), name, false, SO::Alter, ST::Topic,
                                                 rune::protocol::SecurityPatternKind::Exact, "live.t");
    ASSERT_EQ(granted.errorCode, 0u) << granted.detail;
    EXPECT_EQ(r.defineTopicCode(s, "live.t"), 0) << "granted again";
}

// Input bounds on the adoption path: a 249-byte topic name (the maximum) - the NotAuthorized detail the gateway hands
// the admin thread fits the admin ring and the refusal is the same; with ALTER it adopts.
TEST(AuthEnforcementRegression, AdoptionOfAMaxLengthNameIsGuardedAdversarialRegression) {
    AuthRig r;
    const std::string longName = "m." + std::string(247, 'n');
    ASSERT_EQ(longName.size(), 249u);
    ASSERT_GE(r.defineTopic(r.builder, longName, /*typed=*/false), 0);
    const int64_t creator = r.newUser({prefix(SO::Create, ST::Topic, "m.")});
    const int64_t alterer = r.newUser({prefix(SO::Create, ST::Topic, "m."), prefix(SO::Alter, ST::Topic, "m.")});
    EXPECT_EQ(r.defineTopicCode(creator, longName), kNotAuthorized);
    EXPECT_EQ(r.defineTopicCode(alterer, longName), 0);
}

// ── PR #410 review, adversarial pass (the area of #114: retries and replays of an own credential change) ────────
// The client re-sends a DDL with the SAME request id after a NotLeader / lost session (the leader's dedupe answers the
// original result).  An own SET PASSWORD (or DROP PASSWORD of the password the session logged in with) that already
// APPLIED, retried: the retry is still signed with the login's password, which is gone.  It must get its ORIGINAL
// result (OK), not a 45 "log in again" for a change that applied - the client would then keep the old password and
// never log in again.
TEST(AuthEnforcementRegression, ARetriedOwnSetOrDropPasswordAnswersItsOriginalResultAdversarialRegression) {
    AuthRig r;
    // SET PASSWORD: the retry's proof is over the replaced password.
    {
        std::string name;
        const int64_t self = r.newUser({}, &name);
        const AuthWireClient::Verifier v = AuthWireClient::verifierOf("the-set-password-123");
        pc::SecurityDdl m; m.sessionId = self; m.kind = pc::SecurityOpKind::AlterUser; m.name = name;
        m.change = pc::UserChangeKind::SetPassword; m.verifier = v.wire();
        const std::string st = "ALTER USER " + name + " SET PASSWORD '<redacted>';";
        r.wire().pinRequestId(rune::protocol::RequestId{0x5152, 0x8888});
        const auto first = r.wire().securityDdl(r.ch(), m, st);
        ASSERT_EQ(first.errorCode, 0u) << first.detail;
        const int64_t applied = r.broker().node().securityViewForTest().appliedOffset;
        const auto retry = r.wire().securityDdl(r.ch(), m, st);
        r.wire().pinRequestId(std::nullopt);
        EXPECT_EQ(retry.errorCode, 0u) << "a retry of an applied SET PASSWORD answers its original result: " << retry.detail;
        EXPECT_EQ(r.broker().node().securityViewForTest().appliedOffset, applied) << "applied once";
    }
    // DROP PASSWORD of the password the session logged in with (slot 1): the retry's proof is over the dropped one.
    {
        std::string name;
        const int64_t self = r.newUser({}, &name);   // logged in with AuthRig::kPw (slot 1)
        r.broker().addPassword(name, "the-second-password-1");
        pc::SecurityDdl m; m.sessionId = self; m.kind = pc::SecurityOpKind::AlterUser; m.name = name;
        m.change = pc::UserChangeKind::DropPassword; m.passwordIndex = 1;
        const std::string st = "ALTER USER " + name + " DROP PASSWORD 1;";
        r.wire().pinRequestId(rune::protocol::RequestId{0x5153, 0x9999});
        const auto first = r.wire().securityDdl(r.ch(), m, st);
        ASSERT_EQ(first.errorCode, 0u) << first.detail;
        const auto retry = r.wire().securityDdl(r.ch(), m, st);
        r.wire().pinRequestId(std::nullopt);
        EXPECT_EQ(retry.errorCode, 0u) << "a retry of an applied DROP PASSWORD answers its original result: " << retry.detail;
    }
}

// The FORCE / CredentialProof trailer from a hostile client on statements it does not change: FORCE on a GRANT or a
// CREATE USER changes nothing, a proof on another user's change does not stand in for ADMIN, and FORCE never lets a
// non-admin remove anyone.
TEST(AuthEnforcementRegression, TrailerFlagsOnStatementsTheyDoNotGovernChangeNothingAdversarialRegression) {
    AuthRig r;
    std::string victim;
    (void)r.newUser({}, &victim);
    std::string name;
    const int64_t self = r.newUser({}, &name);
    pc::SecurityDdl dis; dis.sessionId = self; dis.kind = pc::SecurityOpKind::AlterUser; dis.name = victim;
    dis.change = pc::UserChangeKind::Disable; dis.flags = pc::kSecurityDdlFlagForce;
    EXPECT_EQ(r.wire().securityDdl(r.ch(), dis, "ALTER USER " + victim + " DISABLE FORCE;").errorCode, 45u)
        << "FORCE is not ADMIN";
    pc::SecurityDdl drop; drop.sessionId = self; drop.kind = pc::SecurityOpKind::DropUser; drop.name = victim;
    drop.flags = pc::kSecurityDdlFlagForce;
    EXPECT_EQ(r.wire().securityDdl(r.ch(), drop, "DROP USER " + victim + " FORCE;").errorCode, 45u);
    // An admin's GRANT with FORCE: the normal result.
    const int64_t admin = r.newUser({cluster(SO::Admin)});
    pc::SecurityDdl g; g.sessionId = admin; g.kind = pc::SecurityOpKind::Grant; g.name = name;
    g.granteeKind = static_cast<uint8_t>(rune::protocol::SecurityGranteeKind::User);
    g.op = static_cast<uint8_t>(SO::Read); g.resourceType = static_cast<uint8_t>(ST::Topic);
    g.patternKind = static_cast<uint8_t>(rune::protocol::SecurityPatternKind::Exact); g.pattern = "orders";
    g.flags = pc::kSecurityDdlFlagForce;
    EXPECT_EQ(r.wire().securityDdl(r.ch(), g, "GRANT READ ON TOPIC orders TO USER " + name + ";").errorCode, 0u);
    EXPECT_EQ(r.produceDenied(self, r.orders), kNotAuthorized) << "only READ was granted";
}

// #129 through a ROLE: ALTER held only through a role adopts the schema; the role revoked mid-session, the very next
// adoption is refused and the schema stays.
TEST(AuthEnforcementRegression, AdoptionThroughARoleFollowsTheRoleAdversarialRegression) {
    AuthRig r;
    ASSERT_GE(r.defineTopic(r.builder, "role.t1", /*typed=*/false), 0);
    ASSERT_GE(r.defineTopic(r.builder, "role.t2", /*typed=*/false), 0);
    auto version = [&](const char* t) {
        const auto a = r.c().awaitSchema(r.c().sendGetTopicSchema(r.ch(), r.stream(), r.builder, t), AuthRig::dl());
        return a ? static_cast<int>(a->version) : kNoAnswer;
    };
    AuthWireClient& aw = r.broker().admin();
    const int64_t as = r.broker().adminSession();
    ASSERT_EQ(aw.createRole(r.ch(), as, "alterers").errorCode, 0u);
    ASSERT_EQ(aw.grant(r.ch(), as, "alterers", true, SO::Alter, ST::Topic, rune::protocol::SecurityPatternKind::Prefix, "role.").errorCode, 0u);
    std::string name;
    const int64_t s = r.newUser({prefix(SO::Create, ST::Topic, "role.")}, &name);
    ASSERT_EQ(aw.grantRole(r.ch(), as, name, "alterers").errorCode, 0u);
    EXPECT_EQ(r.defineTopicCode(s, "role.t1"), 0) << "ALTER through the role adopts";
    ASSERT_EQ(aw.revokeRole(r.ch(), as, name, "alterers").errorCode, 0u);
    const int before = version("role.t2");
    EXPECT_EQ(r.defineTopicCode(s, "role.t2"), kNotAuthorized) << "the role revoked mid-session";
    EXPECT_EQ(version("role.t2"), before);
}

// PR #410 review F1, the throttle side: retries of an applied own SET PASSWORD (proven with the replaced password) are
// answered their original result and are NOT password guesses - after three of them the user still logs in at once
// from the same host (three charged failures would have started a delay).
TEST(AuthEnforcementRegression, RetriesOfAnAppliedOwnChangeAreNotChargedOnTheThrottleRegression) {
    AuthRig r;
    std::string name;
    const int64_t self = r.newUser({}, &name);
    const AuthWireClient::Verifier v = AuthWireClient::verifierOf("the-new-password-123");
    pc::SecurityDdl m; m.sessionId = self; m.kind = pc::SecurityOpKind::AlterUser; m.name = name;
    m.change = pc::UserChangeKind::SetPassword; m.verifier = v.wire();
    const std::string st = "ALTER USER " + name + " SET PASSWORD '<redacted>';";
    r.wire().pinRequestId(rune::protocol::RequestId{0x5154, 0xAAAA});
    ASSERT_EQ(r.wire().securityDdl(r.ch(), m, st).errorCode, 0u);
    for (int i = 0; i < 3; ++i) EXPECT_EQ(r.wire().securityDdl(r.ch(), m, st).errorCode, 0u) << "retry " << i;
    r.wire().pinRequestId(std::nullopt);
    const auto login = r.broker().wire().connectAndLogin(r.ch(), name, "the-new-password-123");
    EXPECT_TRUE(login.ok()) << "not rate limited: code " << login.errorCode() << " retryAfter "
                            << (login.fin ? login.fin->retryAfterMs : 0u);
}

// PR #410 review F1, the whole bug class: any security DDL whose gateway check reads state its own first attempt
// changed.  An admin revoking ITS OWN ADMIN (another admin remains) and retrying with the same request id: the gateway
// now denies it ADMIN, but the retry gets its original result; a NEW request from it is refused 45.
TEST(AuthEnforcementRegression, ARetriedRevokeOfOnesOwnAdminAnswersItsOriginalResultRegression) {
    AuthRig r;
    std::string name;
    const int64_t self = r.newUser({cluster(SO::Admin)}, &name);
    pc::SecurityDdl m; m.sessionId = self; m.kind = pc::SecurityOpKind::Revoke; m.name = name;
    m.granteeKind = static_cast<uint8_t>(rune::protocol::SecurityGranteeKind::User);
    m.op = static_cast<uint8_t>(SO::Admin); m.resourceType = static_cast<uint8_t>(ST::Cluster);
    m.patternKind = static_cast<uint8_t>(rune::protocol::SecurityPatternKind::Exact);
    const std::string st = "REVOKE ADMIN ON CLUSTER FROM USER " + name + ";";
    r.wire().pinRequestId(rune::protocol::RequestId{0x5155, 0xBBBB});
    ASSERT_EQ(r.wire().securityDdl(r.ch(), m, st).errorCode, 0u);
    const int64_t applied = r.broker().node().securityViewForTest().appliedOffset;
    const auto retry = r.wire().securityDdl(r.ch(), m, st);
    r.wire().pinRequestId(std::nullopt);
    EXPECT_EQ(retry.errorCode, 0u) << "the retry's original result: " << retry.detail;
    EXPECT_EQ(r.broker().node().securityViewForTest().appliedOffset, applied);
    const auto fresh = r.wire().createRole(r.ch(), self, "after_revoke");
    EXPECT_EQ(fresh.errorCode, 45u) << "a new request needs the ADMIN it gave up";
    EXPECT_EQ(fresh.detail, "not authorized: ADMIN on CLUSTER");
}

// PR #410 re-check, adversarial (the throttle exemption of a "known retry"): a hijacked session (no password) reuses
// the request id of an already-applied request of its user with a DIFFERENT change, so the gateway sees a "known
// retry" and skips the login throttle.  Its proof guesses must still be throttled like logins, and the answer must not
// tell a right proof from a wrong one (a right one would otherwise answer "requestId reused", a wrong one 45).
TEST(AuthEnforcementRegression, AReusedRequestIdIsNoUnthrottledPasswordOracleAdversarialRegression) {
    AuthRig r;
    std::string name;
    const int64_t self = r.newUser({}, &name);
    const rune::protocol::RequestId applied{0x5156, 0xCCCC};
    r.wire().pinRequestId(applied);
    ASSERT_EQ(r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::AddPassword, "first-added-pw-123").errorCode, 0u);
    // The attacker: unsigned (= wrong) guesses under the known request id, well past the free failures.
    r.wire().signOwnCredentialChanges(false);
    for (int i = 0; i < 8; ++i) {
        const auto a = r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::SetPassword, "attacker-pw-12345");
        EXPECT_EQ(a.errorCode, 45u) << i << ": " << a.detail;
    }
    // A RIGHT proof (stands in for the guess that hits) under the same id, at once: throttled -> the same 45, not a
    // distinguishable answer.
    r.wire().signOwnCredentialChanges(true);
    const auto hit = r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::SetPassword, "attacker-pw-12345");
    r.wire().pinRequestId(std::nullopt);
    EXPECT_EQ(hit.errorCode, 45u) << "a right proof right after 8 wrong ones must be throttled, not answered: " << hit.errorCode
                                  << " " << hit.detail;
}

// PR #410 review F2, without the throttle's help: a request id reused for a DIFFERENT own change answers exactly the
// bad-proof refusal whether its proof is right or wrong - the right one first (no failure counted yet, so no delay
// can mask the difference), then a wrong one.  Neither applies anything.
TEST(AuthEnforcementRegression, AReusedRequestIdAnswersTheSameForARightAndAWrongProofRegression) {
    AuthRig r;
    std::string name;
    const int64_t self = r.newUser({}, &name);
    r.wire().pinRequestId(rune::protocol::RequestId{0x5157, 0xDDDD});
    ASSERT_EQ(r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::AddPassword, "first-added-pw-123").errorCode, 0u);
    const int64_t applied = r.broker().node().securityViewForTest().appliedOffset;
    const auto right = r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::SetPassword, "a-different-pw-123");
    r.wire().signOwnCredentialChanges(false);
    const auto wrong = r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::SetPassword, "a-different-pw-123");
    r.wire().signOwnCredentialChanges(true);
    r.wire().pinRequestId(std::nullopt);
    EXPECT_EQ(right.errorCode, 45u) << right.detail;
    EXPECT_EQ(wrong.errorCode, 45u) << wrong.detail;
    EXPECT_EQ(right.detail, wrong.detail) << "the answer never tells a right proof from a wrong one";
    EXPECT_EQ(r.broker().node().securityViewForTest().appliedOffset, applied) << "nothing applied";
}

// ===========================================================================
// Adversarial review of PR #423 (issue #122): attack producer-id ownership through the wire, on both produce paths.
// ===========================================================================

// Input bounds: ids m was never issued but that are in the bound format - another principal's tag on a counter that
// was never handed out, the extreme bound ids (counter 0 / tag 0, INT64_MAX) - are refused 45 on worker ingress and
// on the gateway-routed path.  A legacy id (bit 62 clear) stays usable by anyone (the deliberate version-skew rule).
TEST(AuthEnforcementRegression, ForgedBoundProducerIdsAreRefusedOnBothPathsAdversarialRegression) {
    AuthBroker::Options o;
    o.shard = true;
    AuthRig r("pidforge", o);
    const int64_t ws2 = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t m = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const std::string worker = r.broker().workerIngressChannel();
    const auto p = r.initProducer(ws2);
    ASSERT_EQ(p.code, kOk);
    ASSERT_TRUE(rune::protocol::isBoundProducerId(p.pid));
    const int64_t forged[] = {
        p.pid + (int64_t{1} << rune::protocol::kPidOwnerBits),   // ws2's tag, the next (never issued) counter
        rune::protocol::kBoundPidFlag,                           // counter 0, tag 0 (the "no principal" tag)
        INT64_MAX,                                               // the last counter, tag 0xFFFFFF
    };
    for (const int64_t f : forged) {
        EXPECT_EQ(r.produceAs(m, r.orders, f, 0, 0, false, "v", &worker).code, kNotAuthorized) << "worker, pid " << f;
        EXPECT_EQ(r.produceAs(m, r.orders, f, 0, 0).code, kNotAuthorized) << "routed, pid " << f;
    }
    EXPECT_NE(r.produceAs(m, r.orders, 5, 0, 0, false, "v", &worker).code, kNotAuthorized) << "a legacy id is unowned";
    EXPECT_NE(r.produceAs(m, r.orders, 6, 0, 0).code, kNotAuthorized) << "a legacy id is unowned (routed)";
    const auto own = r.produceAs(ws2, r.orders, p.pid, p.epoch, 0, false, "v", &worker);
    EXPECT_EQ(own.code, kOk) << "the owner is untouched";
}

// Auth (grant changed mid-session): every re-ship of a session's WRITE grants to the workers must carry its
// principal.  The owner keeps producing under its id over worker ingress after a grant is added, after its WRITE is
// revoked (45) and re-granted; the other principal stays refused throughout.
TEST(AuthEnforcementRegression, ProducerIdOwnerSurvivesGrantChangesMidSessionAdversarialRegression) {
    AuthBroker::Options o;
    o.shard = true;
    AuthRig r("pidregrant", o);
    std::string name;
    const int64_t ws2 = r.newUser({exact(SO::Write, ST::Topic, "orders")}, &name);
    const int64_t m = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const std::string worker = r.broker().workerIngressChannel();
    const auto p = r.initProducer(ws2);
    ASSERT_EQ(p.code, kOk);
    int32_t seq = 0;
    ASSERT_EQ(r.produceAs(ws2, r.orders, p.pid, p.epoch, seq++, false, "v", &worker).code, kOk);
    auto& admin = r.broker().admin();
    const int64_t adm = r.broker().adminSession();
    using SP = rune::protocol::SecurityPatternKind;
    ASSERT_EQ(admin.grant(r.ch(), adm, name, false, SO::Write, ST::Topic, SP::Exact, "secret").errorCode, 0u);
    EXPECT_EQ(r.produceAs(ws2, r.orders, p.pid, p.epoch, seq++, false, "v", &worker).code, kOk) << "after a grant";
    ASSERT_EQ(admin.revoke(r.ch(), adm, name, false, SO::Write, ST::Topic, SP::Exact, "orders").errorCode, 0u);
    EXPECT_EQ(r.produceAs(ws2, r.orders, p.pid, p.epoch, seq, false, "v", &worker).code, kNotAuthorized) << "revoked";
    ASSERT_EQ(admin.grant(r.ch(), adm, name, false, SO::Write, ST::Topic, SP::Exact, "orders").errorCode, 0u);
    EXPECT_EQ(r.produceAs(ws2, r.orders, p.pid, p.epoch, seq++, false, "v", &worker).code, kOk) << "re-granted";
    EXPECT_EQ(r.produceAs(m, r.orders, p.pid, p.epoch, seq, false, "v", &worker).code, kNotAuthorized);
}

// Escalation through a sibling op: principal b shares WRITE on a transactional id with a, and READ on a group.  b
// naming a's producer id on AddPartitions, AddOffsets, TxnOffsetCommit and EndTxn (abort) gets exactly the wrong-id
// answer, and a's open transaction is untouched: a commits it afterwards.
TEST(AuthEnforcementRegression, ForeignPidOnEveryTransactionRequestChangesNothingAdversarialRegression) {
    AuthRig r;
    const std::vector<AuthGrant> g{exact(SO::Write, ST::TransactionalId, "tx-s"), exact(SO::Write, ST::Topic, "orders"),
                                   exact(SO::Read, ST::Group, "cg-t"), exact(SO::Read, ST::Topic, "orders")};
    const int64_t a = r.newUser(g);
    const int64_t b = r.newUser(g);
    const auto pa = r.initProducer(a, "tx-s");
    ASSERT_EQ(pa.code, kOk);
    ASSERT_EQ(r.addPartitions(a, "tx-s", r.orders, pa.pid, pa.epoch), kOk) << "a's transaction is open";
    const int64_t wrong = 12345;   // a legacy id nobody holds: the plain wrong-id answer
    const int wrongAdd = r.addPartitions(b, "tx-s", r.orders, wrong, pa.epoch);
    ASSERT_NE(wrongAdd, kOk);
    EXPECT_EQ(r.addPartitions(b, "tx-s", r.orders, pa.pid, pa.epoch), wrongAdd);
    EXPECT_EQ(r.addOffsets(b, "tx-s", "cg-t", pa.pid, pa.epoch), r.addOffsets(b, "tx-s", "cg-t", wrong, pa.epoch));
    EXPECT_EQ(r.txnOffsetCommit(b, "tx-s", "cg-t", r.orders, pa.pid, pa.epoch),
              r.txnOffsetCommit(b, "tx-s", "cg-t", r.orders, wrong, pa.epoch));
    EXPECT_EQ(r.endTxn(b, "tx-s", pa.pid, pa.epoch, /*commit=*/false), r.endTxn(b, "tx-s", wrong, pa.epoch, false));
    EXPECT_NE(r.endTxn(b, "tx-s", pa.pid, pa.epoch, false), kOk);
    EXPECT_EQ(r.endTxn(a, "tx-s", pa.pid, pa.epoch, /*commit=*/true), kOk) << "b's requests touched a's transaction";
}

// Crash and restart: the owner is in the id itself, so a broker restart (and the owner's new session after it) keeps
// the binding - the owner's idempotent producer carries on at its next sequence, another principal is still refused.
TEST(AuthEnforcementRegression, ProducerIdBindingSurvivesABrokerRestartAdversarialRegression) {
    AuthRig r;
    std::string ownerName, otherName;
    (void)r.newUser({exact(SO::Write, ST::Topic, "orders")}, &ownerName);
    (void)r.newUser({exact(SO::Write, ST::Topic, "orders")}, &otherName);
    int64_t owner = r.login(ownerName);
    const auto p = r.initProducer(owner);
    ASSERT_EQ(p.code, kOk);
    const auto first = r.produceAs(owner, r.orders, p.pid, p.epoch, 0);
    ASSERT_EQ(first.code, kOk);
    r.restart();
    owner = r.login(ownerName);
    const int64_t other = r.login(otherName);
    EXPECT_EQ(r.produceAs(other, r.orders, p.pid, p.epoch, 1).code, kNotAuthorized) << "after the restart";
    const auto next = r.produceAs(owner, r.orders, p.pid, p.epoch, 1);
    EXPECT_EQ(next.code, kOk) << "the owner lost its producer id across the restart";
    EXPECT_GT(next.baseOffset, first.baseOffset);
    const auto fresh = r.initProducer(owner);
    ASSERT_EQ(fresh.code, kOk);
    EXPECT_GT(rune::protocol::producerIdCounter(fresh.pid), rune::protocol::producerIdCounter(p.pid))
        << "a recovered bound id's counter was re-issued";
}

// Issue #125: TxnOffsetCommit needs READ on every listed topic, exactly as OffsetCommit does (the gateway checked only
// the transactional id and the group).
TEST(AuthEnforcementRegression, TxnOffsetCommitNeedsReadOnEveryTopicRegression) {
    AuthRig r;
    const std::vector<AuthGrant> base{exact(SO::Write, ST::TransactionalId, "tx1"), exact(SO::Write, ST::Topic, "orders"),
                                      exact(SO::Read, ST::Group, "cg")};
    std::vector<AuthGrant> withRead = base;
    withRead.push_back(exact(SO::Read, ST::Topic, "orders"));
    const int64_t noRead = r.newUser(base);
    const int64_t reader = r.newUser(withRead);
    EXPECT_EQ(r.txnOffsetCommit(noRead, "tx1", "cg", r.orders), kNotAuthorized)
        << "an offset was committed for a topic the caller cannot READ";
    EXPECT_NE(r.txnOffsetCommit(reader, "tx1", "cg", r.orders), kNotAuthorized);
}

// Adversarial review of PR #466 (#125), through the real gateway and BrokerNode's fence wiring: READ is needed on
// EVERY entry's topic (the unreadable one first, last, or after a duplicate; with and without the version-10 member
// block), the member block is fenced (unknown member, stale generation, another session's member), and a legacy
// (version-9, no block) frame is still accepted unfenced.
TEST(AuthEnforcementRegression, TxnOffsetCommitReadOnEveryEntryAndTheMemberFenceOverTheWireAdversarialRegression) {
    AuthRig r;
    std::string name;
    const int64_t u = r.newUser({exact(SO::Write, ST::TransactionalId, "tx1"), exact(SO::Write, ST::Topic, "orders"),
                                 exact(SO::Read, ST::Topic, "orders"), exact(SO::Read, ST::Group, "cg")}, &name);
    const auto p = r.initProducer(u, "tx1");
    ASSERT_EQ(p.code, kOk);
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.orders, r.secret}, p.pid, p.epoch), kNotAuthorized)
        << "the unreadable topic was the last entry";
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.secret, r.orders}, p.pid, p.epoch), kNotAuthorized);
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.orders, r.orders, r.secret}, p.pid, p.epoch, 1, "m"),
              kNotAuthorized) << "the member block changed the authorization of the entries";

    const auto [member, gen] = r.joinStable(u, "cg", {r.orders});
    ASSERT_FALSE(member.empty());
    ASSERT_EQ(r.addOffsets(u, "tx1", "cg", p.pid, p.epoch), kOk);
    const int unknownMember = static_cast<int>(pc::ErrorCode::UnknownMember);
    const int illegalGen    = static_cast<int>(pc::ErrorCode::IllegalGeneration);
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.orders}, p.pid, p.epoch, gen, "cg-nobody"), unknownMember);
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.orders}, p.pid, p.epoch, gen + 1, member), illegalGen);
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.orders}, p.pid, p.epoch, gen - 1, member), illegalGen);
    const int64_t other = r.login(name);   // the same principal, another session: the member is not its own
    EXPECT_EQ(r.txnOffsetCommitMany(other, "tx1", "cg", {r.orders}, p.pid, p.epoch, gen, member), unknownMember);
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.orders}, p.pid, p.epoch), kOk)
        << "a legacy (version-9) frame without a member block was refused";
    EXPECT_EQ(r.txnOffsetCommitMany(u, "tx1", "cg", {r.orders}, p.pid, p.epoch, gen, member), kOk);
}

// #277: a fetch-all (an OffsetFetch with no topic entries) lists only the topics the caller may READ - here a group
// with offsets committed for "orders" and "secret", read by a user holding READ on the group and on "orders" only.
TEST(AuthEnforcementRegression, OffsetFetchAllReturnsOnlyReadableTopicsRegression) {
    AuthRig r;
    const int64_t committer = r.newUser({exact(SO::Read, ST::Group, "g277"), exact(SO::Read, ST::Topic, "orders"),
                                         exact(SO::Read, ST::Topic, "secret")});
    const int64_t reader = r.newUser({exact(SO::Read, ST::Group, "g277"), exact(SO::Read, ST::Topic, "orders")});
    const auto [member, gen] = r.joinStable(committer, "g277", {r.orders, r.secret});
    ASSERT_FALSE(member.empty());
    ASSERT_EQ(r.commitAs(committer, "g277", member, gen, r.orders, 7), kOk);
    ASSERT_EQ(r.commitAs(committer, "g277", member, gen, r.secret, 9), kOk);

    const auto both = r.offsetFetchAll(committer, "g277");
    ASSERT_TRUE(both.has_value()) << "the fetch-all was not answered";
    ASSERT_EQ(both->size(), 2u) << "the committer reads both topics";

    const auto seen = r.offsetFetchAll(reader, "g277");
    ASSERT_TRUE(seen.has_value()) << "the filtered fetch-all was not answered";
    ASSERT_EQ(seen->size(), 1u) << "only the readable topic is listed";
    EXPECT_EQ(seen->front().topicId, r.orders);
    EXPECT_EQ(seen->front().offset, 7);
    EXPECT_EQ(seen->front().errorCode, 0);
}

// #277: a user with two passwords (ADD PASSWORD: rotation) logs in through EITHER verifier slot on its own - the other
// slot's proof is wrong - and a login whose two proofs are both wrong fails 44.
TEST(AuthEnforcementRegression, EachVerifierSlotAuthenticatesOnItsOwnRegression) {
    AuthRig r;
    constexpr const char* kSecond = "second-password-6789";
    std::string name;
    (void)r.newUser({}, &name);
    r.broker().addPassword(name, kSecond);
    // The verifiers are listed oldest first: slot 0 is the original password, slot 1 the added one.
    const auto viaSlot0 = r.loginSlot(name, 0, AuthRig::kPw);
    EXPECT_TRUE(viaSlot0.ok()) << "slot 0 alone: " << viaSlot0.errorCode();
    EXPECT_TRUE(viaSlot0.serverSignatureValid());
    const auto viaSlot1 = r.loginSlot(name, 1, kSecond);
    EXPECT_TRUE(viaSlot1.ok()) << "slot 1 alone: " << viaSlot1.errorCode();
    EXPECT_TRUE(viaSlot1.serverSignatureValid());
    EXPECT_EQ(r.loginSlot(name, 0, "neither-password-1").errorCode(), 44) << "both proofs wrong";
    EXPECT_EQ(r.loginSlot(name, 0, kSecond).errorCode(), 44) << "a password proves only its own slot";
}

// ===========================================================================
// Adversarial review of PR #583 (#278 / #277 / #136): the union of the batch's areas - denial side effects, the
// fetch-all filter, the two verifier slots and the login throttle - attacked through the wire.
// ===========================================================================

// #278 on the DEFAULT runtime (shard workers, CLAUDE.md rule 11): every refused produce routed through the gateway
// appends nothing and every refused Subscribe registers nothing - for no grant, the wrong operation and the wrong
// topic, and for the same refused request sent twice back to back before its first answer.
TEST(AuthEnforcementAdversarialRegression, DenialsOnTheShardRuntimeAppendAndRegisterNothingAdversarialRegression) {
    AuthBroker::Options o;
    o.shard = true;
    AuthRig r("adv278shard", o);
    const int64_t onlyRead = r.newUser({exact(SO::Read, ST::Topic, "orders")});
    const int64_t onlyWrite = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t none = r.newUser({});
    ASSERT_EQ(r.produce(onlyWrite, r.orders, /*untilOk=*/true), kOk) << "the control: WRITE is appended";
    EXPECT_EQ(r.produceDenied(onlyRead, r.orders), kNotAuthorized) << "READ is not WRITE";
    EXPECT_EQ(r.produceDenied(none, r.orders), kNotAuthorized) << "default deny";
    EXPECT_EQ(r.produceDenied(onlyWrite, r.secret), kNotAuthorized) << "WRITE on one topic is not WRITE on another";
    EXPECT_EQ(r.subscribeDenied(onlyWrite, r.orders), kNotAuthorized) << "WRITE is not READ";
    EXPECT_EQ(r.subscribeDenied(none, r.orders), kNotAuthorized) << "default deny";
    EXPECT_EQ(r.subscribeDenied(onlyRead, r.secret), kNotAuthorized) << "READ on one topic is not READ on another";

    // Back to back: two refused Subscribes on one session, the second sent before the first is answered.
    const int64_t c1 = r.c().sendSubscribe(r.ch(), r.stream(), none, r.orders, 0);
    const int64_t c2 = r.c().sendSubscribe(r.ch(), r.stream(), none, r.orders, 0);
    EXPECT_EQ(r.c().awaitSubscribeCode(c1, AuthRig::dl()), std::optional<int16_t>(kNotAuthorized));
    EXPECT_EQ(r.c().awaitSubscribeCode(c2, AuthRig::dl()), std::optional<int16_t>(kNotAuthorized));
    r.expectNothingDelivered(none, c1);
    r.expectSubscribeRegisteredNothing(none, c2, r.orders);
}

// #277 under live grant changes: a fetch-all is filtered by the session's permissions AT THE TIME OF THE FETCH - a
// prefix grant covers its topics only, a user with READ on the group alone gets an empty (not refused) list, and a
// READ revoked mid-session drops that topic from the very next fetch-all (while a listed fetch of it is refused 45).
TEST(AuthEnforcementAdversarialRegression, AFetchAllFollowsPrefixesAndALiveRevokeAdversarialRegression) {
    AuthRig r("adv277fetch");
    const auto g = [](SO op, ST t, const char* n) { return exact(op, t, n); };
    const int64_t committer = r.newUser({g(SO::Read, ST::Group, "gadv"), g(SO::Read, ST::Topic, "orders"),
                                         g(SO::Read, ST::Topic, "secret")});
    std::string readerName;
    const int64_t reader = r.newUser({g(SO::Read, ST::Group, "gadv"), g(SO::Read, ST::Topic, "orders"),
                                      g(SO::Read, ST::Topic, "secret")}, &readerName);
    const int64_t byPrefix = r.newUser({g(SO::Read, ST::Group, "gadv"), prefix(SO::Read, ST::Topic, "sec")});
    const int64_t groupOnly = r.newUser({g(SO::Read, ST::Group, "gadv")});
    const auto [member, gen] = r.joinStable(committer, "gadv", {r.orders, r.secret});
    ASSERT_FALSE(member.empty());
    ASSERT_EQ(r.commitAs(committer, "gadv", member, gen, r.orders, 7), kOk);
    ASSERT_EQ(r.commitAs(committer, "gadv", member, gen, r.secret, 9), kOk);
    const auto topicsOf = [](const std::optional<std::vector<pc::OffsetFetchResult>>& v) {
        std::vector<std::pair<int32_t, int64_t>> out;
        if (v) for (const auto& e : *v) out.emplace_back(e.topicId, e.offset);
        std::sort(out.begin(), out.end());
        return out;
    };
    using TO = std::vector<std::pair<int32_t, int64_t>>;
    const auto both = r.offsetFetchAll(reader, "gadv");
    ASSERT_TRUE(both.has_value());
    EXPECT_EQ(topicsOf(both), (TO{{r.orders, 7}, {r.secret, 9}}));
    const auto pre = r.offsetFetchAll(byPrefix, "gadv");
    ASSERT_TRUE(pre.has_value());
    EXPECT_EQ(topicsOf(pre), (TO{{r.secret, 9}})) << "a prefix grant covers only its topics";
    const auto none = r.offsetFetchAll(groupOnly, "gadv");
    ASSERT_TRUE(none.has_value()) << "READ on the group alone: answered, not refused";
    EXPECT_TRUE(none->empty()) << "and lists no topic";

    ASSERT_EQ(r.broker().admin().revoke(r.ch(), r.broker().adminSession(), readerName, false, SO::Read, ST::Topic,
                                        rune::protocol::SecurityPatternKind::Exact, "secret").errorCode, 0);
    const auto after = r.offsetFetchAll(reader, "gadv");
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(topicsOf(after), (TO{{r.orders, 7}})) << "the revoked topic is gone from the very next fetch-all";
    EXPECT_EQ(r.offsetFetch(reader, "gadv", r.secret), kNotAuthorized) << "and a listed fetch of it is refused";
}

// #277 / #136, input bounds and protocol state of the two verifier slots: a one-password user's slot 1 is a pad that
// no password opens; after the oldest password is dropped the remaining one moves to slot 0 and the dropped one opens
// nothing; a final that carries fewer proofs than the broker listed is refused even with the right password; and a
// password dropped between the two legs of a login never logs in with it.
TEST(AuthEnforcementAdversarialRegression, TheVerifierSlotsAcceptOnlyCurrentPasswordsInTheirOwnSlotAdversarialRegression) {
    AuthRig r("adv277slots");
    constexpr const char* kSecond = "second-password-6789";
    std::string name;
    (void)r.newUser({}, &name);
    // Successes between the failures reset the (user, host) pair: no failure here is ever inside the throttle's delay.
    EXPECT_EQ(r.loginSlot(name, 1, AuthRig::kPw).errorCode(), 44) << "a one-password user's slot 1 is a pad";
    ASSERT_TRUE(r.loginSlot(name, 0, AuthRig::kPw).ok());
    {
        const int64_t sid = r.wire().connect(r.ch());
        ASSERT_GT(sid, 0);
        const auto one = r.wire().login(r.ch(), sid, name, {AuthRig::kPw, AuthRig::kPw}, /*count=*/1);
        ASSERT_TRUE(one.first.has_value());
        EXPECT_EQ(one.first->count, 2);
        EXPECT_EQ(one.errorCode(), 44) << "one proof where the broker listed two";
    }
    ASSERT_TRUE(r.loginSlot(name, 0, AuthRig::kPw).ok());
    r.broker().addPassword(name, kSecond);
    r.broker().dropOldestPassword(name);
    EXPECT_EQ(r.loginSlot(name, 0, AuthRig::kPw).errorCode(), 44) << "the dropped password";
    EXPECT_TRUE(r.loginSlot(name, 0, kSecond).ok()) << "the remaining password moved to slot 0";
    EXPECT_EQ(r.loginSlot(name, 1, kSecond).errorCode(), 44) << "slot 1 is a pad again";
    EXPECT_TRUE(r.loginSlot(name, 0, kSecond).ok());

    // A first leg parked while the user holds two passwords; the one it will prove is dropped before the final.
    std::string other;
    (void)r.newUser({}, &other);
    r.broker().addPassword(other, kSecond);
    const int64_t sid = r.wire().connect(r.ch());
    ASSERT_GT(sid, 0);
    auto parked = r.wire().login(r.ch(), sid, other, {"wrong-password-0", AuthRig::kPw}, 0, /*sendFinal=*/false);
    ASSERT_TRUE(parked.first.has_value());
    r.broker().dropOldestPassword(other);   // drops kPw (slot 0 at the first leg)
    r.wire().finish(r.ch(), parked, other, {AuthRig::kPw, "wrong-password-1"});
    ASSERT_TRUE(parked.fin.has_value());
    EXPECT_NE(parked.fin->errorCode, 0) << "a password dropped between the legs never logs in";
    EXPECT_TRUE(r.loginSlot(other, 0, kSecond).ok()) << "the control: the current password still does";
}

// #136, the throttle through a sibling entry point: a change of one's own password proven with a WRONG password is a
// password guess charged on the same (user, host) throttle as a failed login.  Two failed logins are free; the third
// failure - a bad own-change proof - starts the pair's delay, so the right password, sent at once, is refused with a
// retry-after.  (Not charged, the pair would hold only the two free failures and the login would succeed.)
TEST(AuthEnforcementAdversarialRegression, OwnChangeProofsAndLoginsShareOneThrottleAdversarialRegression) {
    AuthRig r("adv136throttle");
    std::string name;
    const int64_t self = r.newUser({}, &name);
    for (int i = 0; i < 2; ++i) {
        const auto l = r.loginSlot(name, 0, "a wrong guess");
        ASSERT_TRUE(l.first.has_value()) << i << ": a free failure is verified";
        EXPECT_EQ(l.errorCode(), 44) << i;
    }
    const int64_t sid = r.wire().connect(r.ch());   // connected first: the login follows the guess within its delay
    ASSERT_GT(sid, 0);
    r.wire().signOwnCredentialChanges(false);   // the attacker on a hijacked session: no password, so no valid proof
    EXPECT_EQ(r.wire().alterPassword(r.ch(), self, name, pc::UserChangeKind::SetPassword, "attacker-pw-12345").errorCode, 45u);
    r.wire().signOwnCredentialChanges(true);
    const auto right = r.wire().login(r.ch(), sid, name, {AuthRig::kPw, AuthRig::kPw});
    ASSERT_TRUE(right.fin.has_value());
    EXPECT_FALSE(right.ok()) << "the bad own-change proof was the pair's third failure";
    EXPECT_FALSE(right.first.has_value()) << "refused at the first leg, no salt";
    EXPECT_GT(right.fin->retryAfterMs, 0u);
}

// Adversarial review of PR #583 (test fixture segment sizing): an AuthRig broker - bootstrap, users, two topics, a view -
// holds its logs in ~7 MiB.  Every log preallocates its active segment to its roll size, so ONE internal log or
// topic left at the 64 MiB production default (__security, __catalog, __consumer_offsets, transactions, metadata or a
// rig topic) puts the data dir far above the bound.  Aeron's term buffers (the aeron dir) are not logs and are excluded.
// RED before the review fix: BrokerNode built the __transaction_state and __cluster_metadata logs at the storage::Log
// default roll (64 MiB), ignoring NodeConfig::offsetLogSegmentRollBytes that their replicas were configured with.
TEST(AuthEnforcementAdversarialRegression, TheRigsLogsStayWithinTheTestSegmentBoundAdversarialRegression) {
    AuthRig r("adv583disk");
    const std::filesystem::path data = r.broker().dir() / "broker";
    uintmax_t allocated = 0, largest = 0;
    std::string largestPath;
    for (auto it = std::filesystem::recursive_directory_iterator(data); it != std::filesystem::recursive_directory_iterator(); ++it) {
        if (it->is_directory() && it->path().filename() == "aeron") { it.disable_recursion_pending(); continue; }
        if (!it->is_regular_file()) continue;
        struct stat st {};
        if (::stat(it->path().c_str(), &st) != 0) continue;
        const auto bytes = static_cast<uintmax_t>(st.st_blocks) * 512u;
        allocated += bytes;
        if (bytes > largest) { largest = bytes; largestPath = std::filesystem::relative(it->path(), data).string(); }
    }
    EXPECT_LE(largest, static_cast<uintmax_t>(kTestSegmentRollBytes) + (1u << 20))
        << "a file above the 1 MiB test roll (+ index slack): " << largestPath;
    EXPECT_LT(allocated, uintmax_t{24} << 20) << "the rig's logs: " << (allocated >> 20) << " MiB";
}
