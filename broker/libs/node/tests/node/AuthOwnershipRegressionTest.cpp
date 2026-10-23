// AuthOwnershipRegressionTest.cpp - issue #276 residual: the OWNERSHIP dimension of the auth matrix, kept out of the
// 720-cell request x grant x timing table (that table is shared by the stress test, the 3-node slice and the docker
// e2e; a new column would change all of them).  One table, one row per kind of id a request can name.  In every row two
// principals A and B are BOTH fully granted for the request, and B sends it naming an id that belongs to A's session:
// the request is refused with a specific code and A is untouched (A's member still in its group at its generation, A's
// producer sequence and transaction still A's, A's subscription still delivering, A's topic schema unchanged, nothing
// appended).  Against ONE real authenticating BrokerNode over real Aeron UDP (raw frames); the worker rows run on the
// shard runtime (per-worker ingress and egress).
//
// Design notes recorded per row (none is a bearer capability: every id is bound to its owner):
//   * a group member id is bound to the session that joined it (GroupCoordinationService::ownsMember_) - a foreign one
//     is answered UnknownMember, exactly as a member that does not exist (no existence oracle);
//   * a bound producer id carries its principal's tag (Worker / ClientGateway producerIdUsableBy*) - refused 45;
//   * a transactional producer id is checked like any wrong producer id (B's answer equals the answer for an id nobody
//     holds); the transactional ID itself is a grant (WRITE on it), not an ownership - B's own InitProducerId would fence
//     A by design (ASharedTransactionalIdIsReboundToEachPrincipalRegression), so it is not a row here;
//   * a subscription id is removed only by its owning session (ClientGateway::handleUnsubscribe) - Unsubscribe has no
//     answer, so the row asserts the owner's subscription keeps delivering;
//   * schema adoption (a descriptor on an existing untyped topic) needs ALTER on the topic, whoever created it;
//   * a worker-ingress produce of a session that never authenticated has no grant slot on the worker: refused 45.
#include "support/AuthRig.h"

#include <functional>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
using ST = rune::protocol::SecurityResourceType;
using SO = rune::protocol::SecurityOperation;

namespace {
constexpr int kOk = 0;
constexpr int kUnknownMember = static_cast<int>(pc::ErrorCode::UnknownMember);

AuthBroker::Options shardOpts() { AuthBroker::Options o; o.shard = true; return o; }

/// An Unsubscribe frame as `sid` naming `subId` (no answer exists for it).
bool sendUnsubscribe(AuthRig& r, int64_t sid, int64_t subId) {
    pc::Unsubscribe m;
    m.sessionId = sid; m.correlationId = r.c().nextCorrelationId(); m.subscriptionId = subId;
    uint8_t buf[pc::Unsubscribe::ENCODED_SIZE];
    const std::size_t n = m.encode(buf);
    return r.c().send(r.ch(), r.stream(), buf, n);
}
std::size_t pushesFor(AuthRig& r, int64_t subId) {
    std::size_t n = 0;
    for (const auto& p : r.c().records()) n += p.subscriptionId == subId ? 1 : 0;
    return n;
}
/// The schema version of `topic` as the builder sees it (kNoAnswer: none).
int schemaVersion(AuthRig& r, const std::string& topic) {
    const auto a = r.c().awaitSchema(r.c().sendGetTopicSchema(r.ch(), r.stream(), r.builder, topic), AuthRig::dl());
    return a && a->errorCode == 0 ? static_cast<int>(a->version) : kNoAnswer;
}

struct Row {
    const char* name;
    bool shard;
    std::function<void(AuthRig&)> run;   // sets up A and B, sends B's request, asserts the refusal and A's state
};

// ── Row 1: a group member id (SyncGroup, Heartbeat, OffsetCommit, LeaveGroup) ─────────────────────────────────────────
void groupMemberRow(AuthRig& r) {
    const std::vector<AuthGrant> g{exact(SO::Read, ST::Group, "own-g"), exact(SO::Read, ST::Topic, "orders")};
    const int64_t a = r.newUser(g);
    const int64_t b = r.newUser(g);
    const auto [member, gen] = r.joinStable(a, "own-g", {r.orders});
    ASSERT_FALSE(member.empty());
    ASSERT_EQ(r.commitAs(a, "own-g", member, gen, r.orders, 100), kOk);
    EXPECT_EQ(r.syncGroup(b, "own-g", member, gen), kUnknownMember) << "B's SyncGroup naming A's member";
    EXPECT_EQ(r.heartbeatAt(b, "own-g", member, gen), kUnknownMember) << "B's Heartbeat naming A's member";
    EXPECT_EQ(r.commitAs(b, "own-g", member, gen, r.orders, 500), kUnknownMember) << "B's OffsetCommit naming A's member";
    EXPECT_EQ(r.leaveGroup(b, "own-g", member), kUnknownMember) << "B's LeaveGroup naming A's member";
    // A untouched: still the member at its generation, its assignment served, its committed offset unmoved.
    EXPECT_EQ(r.heartbeatAt(a, "own-g", member, gen), kOk) << "A's member left its group or its generation moved";
    EXPECT_EQ(r.syncGroup(a, "own-g", member, gen), kOk);
    EXPECT_EQ(r.committedOffset(a, "own-g", r.orders), 100) << "B's commit moved A's group position";
}

// ── Row 2: a producer id (idempotent produce), routed through the gateway AND straight to the worker ingress ─────────
void producerIdRow(AuthRig& r) {
    const int64_t a = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const int64_t b = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const std::string worker = r.broker().workerIngressChannel();
    const auto p = r.initProducer(a);
    ASSERT_EQ(p.code, kOk);
    const auto first = r.produceAs(a, r.orders, p.pid, p.epoch, 0, false, "a0", &worker);
    ASSERT_EQ(first.code, kOk);
    const int64_t leo = r.logEnd(r.orders);
    EXPECT_EQ(r.produceAs(b, r.orders, p.pid, p.epoch, 1, false, "b-routed").code, kNotAuthorized) << "routed (gateway)";
    EXPECT_EQ(r.produceAs(b, r.orders, p.pid, p.epoch, 1, false, "b-worker", &worker).code, kNotAuthorized) << "worker ingress";
    EXPECT_EQ(r.produceAs(b, r.orders, p.pid, static_cast<int16_t>(p.epoch + 1), 0, false, "b-fence", &worker).code,
              kNotAuthorized) << "a fence (higher epoch) naming A's id";
    EXPECT_EQ(r.logEnd(r.orders), leo) << "a refused foreign-pid batch was appended";
    // A untouched: its next sequence is accepted at the very next offset (never "duplicate", never fenced).
    const auto next = r.produceAs(a, r.orders, p.pid, p.epoch, 1, false, "a1", &worker);
    EXPECT_EQ(next.code, kOk) << "A's producer was fenced or its sequence consumed";
    EXPECT_EQ(next.baseOffset, first.baseOffset + 1);
}

// ── Row 3: a transaction (AddPartitions, AddOffsets, EndTxn) naming A's transactional producer id ────────────────────
void transactionRow(AuthRig& r) {
    const std::vector<AuthGrant> g{exact(SO::Write, ST::TransactionalId, "own-tx"), exact(SO::Write, ST::Topic, "orders"),
                                   exact(SO::Read, ST::Group, "own-txg"), exact(SO::Read, ST::Topic, "orders")};
    const int64_t a = r.newUser(g);
    const int64_t b = r.newUser(g);
    const auto pa = r.initProducer(a, "own-tx");
    ASSERT_EQ(pa.code, kOk);
    ASSERT_EQ(r.addPartitions(a, "own-tx", r.orders, pa.pid, pa.epoch), kOk) << "A's transaction is open";
    const int64_t nobody = 424242;   // a legacy id nobody holds: the plain wrong-id answer
    const int wrongAdd = r.addPartitions(b, "own-tx", r.orders, nobody, pa.epoch);
    ASSERT_NE(wrongAdd, kOk);
    EXPECT_EQ(r.addPartitions(b, "own-tx", r.orders, pa.pid, pa.epoch), wrongAdd) << "AddPartitions naming A's pid";
    const int wrongOffsets = r.addOffsets(b, "own-tx", "own-txg", nobody, pa.epoch);
    ASSERT_NE(wrongOffsets, kOk);
    EXPECT_EQ(r.addOffsets(b, "own-tx", "own-txg", pa.pid, pa.epoch), wrongOffsets) << "AddOffsets naming A's pid";
    const int wrongEnd = r.endTxn(b, "own-tx", nobody, pa.epoch, /*commit=*/false);
    ASSERT_NE(wrongEnd, kOk);
    EXPECT_EQ(r.endTxn(b, "own-tx", pa.pid, pa.epoch, /*commit=*/false), wrongEnd) << "EndTxn(abort) naming A's pid";
    EXPECT_EQ(r.endTxn(a, "own-tx", pa.pid, pa.epoch, /*commit=*/true), kOk) << "B's requests touched A's transaction";
}

// ── Row 4: a subscription id (Unsubscribe), on the gateway and on the per-worker egress ──────────────────────────────
void subscriptionRow(AuthRig& r) {
    const std::vector<AuthGrant> g{exact(SO::Read, ST::Topic, "orders")};
    const int64_t a = r.newUser(g);
    const int64_t b = r.newUser(g);
    const int64_t w = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    ASSERT_EQ(r.produce(w, r.orders, /*untilOk=*/true), kOk);
    const int64_t corr = r.c().sendSubscribe(r.ch(), r.stream(), a, r.orders, 0);
    ASSERT_EQ(r.c().awaitSubscribeCode(corr, AuthRig::dl()).value_or(-1), 0);
    const int64_t subId = r.c().subscriptionIdOf(corr).value_or(0);
    ASSERT_NE(subId, 0);
    ASSERT_TRUE(r.c().pollUntil([&] { return pushesFor(r, subId) >= 1; })) << "A's subscription delivers";
    ASSERT_TRUE(sendUnsubscribe(r, b, subId)) << "B's Unsubscribe naming A's subscription id";
    // Ordered behind B's Unsubscribe on the same publication: a Ping of B answered means the gateway handled it.
    {
        pc::Ping p;
        p.sessionId = b; p.correlationId = r.c().nextCorrelationId();
        uint8_t buf[64];
        const std::size_t n = p.encode(buf);
        ASSERT_TRUE(r.c().send(r.ch(), r.stream(), buf, n));
        ASSERT_TRUE(r.c().awaitRaw(p.correlationId, AuthRig::dl()).has_value()) << "no Pong: B's frames not handled";
    }
    const std::size_t before = pushesFor(r, subId);
    ASSERT_EQ(r.produce(w, r.orders), kOk);
    EXPECT_TRUE(r.c().pollUntil([&] { return pushesFor(r, subId) > before; }))
        << "A's subscription stopped delivering after B's Unsubscribe naming it";
    for (const auto& e : r.c().errors()) EXPECT_NE(e.correlationId, corr) << "A's subscription was ended (code " << e.code << ")";
    EXPECT_TRUE(r.broker().node().gatewayHasSessionForTest(a));
}

// ── Row 5: schema adoption of A's topic by B without ALTER ───────────────────────────────────────────────────────────
void schemaAdoptionRow(AuthRig& r) {
    const int64_t a = r.newUser({prefix(SO::Create, ST::Topic, "own."), prefix(SO::Alter, ST::Topic, "own.")});
    const int64_t b = r.newUser({prefix(SO::Create, ST::Topic, "own.")});
    ASSERT_GE(r.defineTopic(a, "own.t", /*typed=*/false), 0) << "A registers its (untyped) topic";
    const int before = schemaVersion(r, "own.t");
    EXPECT_EQ(r.defineTopicCode(b, "own.t"), kNotAuthorized) << "B's descriptor on A's topic (CREATE without ALTER)";
    EXPECT_EQ(schemaVersion(r, "own.t"), before) << "B changed the schema of A's topic";
    EXPECT_EQ(r.defineTopicCode(a, "own.t"), kOk) << "the control: A, holding ALTER, adopts a schema";
    EXPECT_NE(schemaVersion(r, "own.t"), before);
}

// ── Row 6: a worker-ingress produce of a session that never authenticated (shard runtime) ─────────────────────────────
void unauthenticatedWorkerIngressRow(AuthRig& r) {
    const int64_t w = r.newUser({exact(SO::Write, ST::Topic, "orders")});
    const std::string worker = r.broker().workerIngressChannel();
    ASSERT_EQ(r.produceAs(w, r.orders, -1, 0, -1, false, "warm", &worker).code, kOk) << "the control: an authenticated writer";
    const int64_t anon = r.wire().connect(r.ch());   // connected, never logged in (Anonymous)
    ASSERT_GT(anon, 0);
    const int64_t leo = r.logEnd(r.orders);
    const int64_t corr = r.c().nextCorrelationId();
    ASSERT_TRUE(r.c().produce(worker, r.stream(), anon, corr, r.orders, 0, 1, makeBatch(1)));
    const auto ack = r.c().awaitProduceResponseUntil(corr, AuthRig::dl());
    ASSERT_TRUE(ack.has_value()) << "the unauthenticated produce was not answered";
    EXPECT_EQ(ack->errorCode, kNotAuthorized) << "a worker-ingress produce of an unauthenticated session";
    EXPECT_EQ(r.logEnd(r.orders), leo) << "the unauthenticated produce was appended";
    EXPECT_EQ(r.produceAs(w, r.orders, -1, 0, -1, false, "after", &worker).code, kOk) << "the writer is untouched";
    EXPECT_EQ(r.logEnd(r.orders), leo + 1);
}

}  // namespace

TEST(AuthOwnershipRegression, ForeignIdsAreRefusedAndLeaveTheOwnerUntouchedRegression) {
    const std::vector<Row> rows = {
        {"group member id", false, groupMemberRow},
        {"producer id", true, producerIdRow},
        {"transactional producer id", false, transactionRow},
        {"subscription id (gateway)", false, subscriptionRow},
        {"subscription id (per-worker egress)", true, subscriptionRow},
        {"schema adoption", false, schemaAdoptionRow},
        {"worker-ingress produce without authentication", true, unauthenticatedWorkerIngressRow},
    };
    int n = 0;
    for (const Row& row : rows) {
        SCOPED_TRACE(std::string("row: ") + row.name);
        AuthRig r("own" + std::to_string(n++), row.shard ? shardOpts() : AuthBroker::Options{});
        row.run(r);
    }
}
