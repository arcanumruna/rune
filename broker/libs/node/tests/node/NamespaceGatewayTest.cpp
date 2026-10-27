// NamespaceGatewayTest.cpp — namespaces Task 10 (spec §2, §7; D7, D8, D9, D15) against ONE real BrokerNode
// (authentication ENABLED, real Aeron UDP, raw frames): the gateway resolves every object name against the session's
// namespace, and a namespace the session may not see answers exactly like one that does not exist.
//   * resolveName (rune/gateway/NsResolve.h): table-driven, zero allocation.
//   * A foreign object by ID: the session's permissions are keyed by its own namespace, so a foreign id is 45 even
//     for a user holding prefix-"" grants on every resource type.  Root reaches it.
//   * Listings: metadata lists only the session's own namespace's topics (root: all).
//   * A foreign or unknown namespace in a NAME: the same answer bytes (after the header) for "bravo/x" and
//     "nosuchns/x", and no answer echoes the foreign namespace's name.
//   * D8: a session outside `default` is refused every group and transaction request before the coordinator sees it.
//   * A malformed qualified name is 40 (InvalidTopicName) on every name-taking request, root included.
//
// Interaction matrix (caller × target × operation):
//   | caller          | target                          | operation                                  | expected        | test
//   |-----------------|---------------------------------|--------------------------------------------|-----------------|------
//   | (unit)          | caller ns × root × name shape   | resolveName                                | 18 table rows   | ResolveTable
//   | alpha/alice     | bravo topic / view, by id       | produce, subscribe, getViewRow             | 45              | ForeignIdNoPerms
//   | alpha/alice     | alpha topic, by id              | produce                                    | 0 (control)     | ForeignIdNoPerms
//   | root            | bravo topic / view, by id       | produce, subscribe, getViewRow             | ok              | ForeignIdNoPerms
//   | alpha/alice     | every namespace                 | metadata                                   | alpha only      | ListingsFiltered
//   | root            | every namespace                 | metadata                                   | all             | ListingsFiltered
//   | alpha/alice     | bravo/x vs nosuchns/x           | schema, define, alter, config, drop topic, | identical body, | ErrorMessageNoForeignName
//   |                 |                                 | create, drop, alter view                   | no "bravo"      |
//   | alpha/alice     | bravo ids                       | subscribe, getViewRow                      | no foreign name | ErrorMessageNoForeignName
//   | alpha/alice     | any group / transactional id    | join, sync, heartbeat, leave, commit,      | 45, coordinator | GroupTxnRefusedOutsideDefault
//   |                 |                                 | fetch, init pid, add parts/offsets, txn    | untouched       |
//   |                 |                                 | commit, end txn                            |                 |
//   | alice, root     | a//x, /x, a/, a/b/c             | every name-taking request                  | 40              | MalformedName
#include "support/AllocWindow.h"
#include "support/AuthRig.h"

#include <rune/catalog/NamespaceRegistry.h>
#include <rune/catalog/NsName.h>
#include <rune/catalog/TopicName.h>
#include <rune/gateway/NsResolve.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <string>
#include <string_view>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
using namespace rune::node::test::grants;
namespace pc = rune::protocol::client;
namespace gw = rune::gateway;

namespace {

constexpr int kOk = 0;
constexpr int kInvalidTopicName = static_cast<int>(pc::ErrorCode::InvalidTopicName);

bool contains(const std::vector<uint8_t>& a, std::string_view needle) {
    return std::search(a.begin(), a.end(), needle.begin(), needle.end()) != a.end();
}
std::vector<uint8_t> bodyOf(const std::vector<uint8_t>& a) {
    constexpr std::size_t h = pc::MessageHeader::ENCODED_SIZE;
    return a.size() > h ? std::vector<uint8_t>(a.begin() + h, a.end()) : std::vector<uint8_t>{};
}

/// One broker: namespaces alpha and bravo, a topic in each (alpha/mine, bravo/secretb), a view in bravo
/// (bravo/boardb), root logged in on the rig's data client, and `alpha/alice` holding prefix-"" grants on every
/// resource type a data path checks (so only the namespace keeps her out of bravo).
struct NsGw {
    AuthRig r{"nsgw"};
    uint32_t alpha = 0, bravo = 0;
    int64_t root = 0, alice = 0;
    int32_t mine = -1, secretb = -1, bview = -1;
    NsGw() {
        alpha = r.createNamespace("alpha");
        bravo = r.createNamespace("bravo");
        root = r.login("admin", AuthBroker::kAdminPassword);
        mine = r.defineTopic(root, "alpha/mine");
        secretb = r.defineTopic(root, "bravo/secretb");
        bview = r.createView(root, "bravo/boardb", secretb);
        r.broker().createUser("alpha/alice", AuthRig::kPw,
                              {prefix(SO::Read, ST::Topic, ""), prefix(SO::Write, ST::Topic, ""),
                               prefix(SO::Describe, ST::Topic, ""), prefix(SO::Read, ST::View, ""),
                               prefix(SO::Read, ST::Group, ""), prefix(SO::Write, ST::TransactionalId, ""),
                               prefix(SO::Describe, ST::TransactionalId, "")});
        alice = r.login("alpha/alice");
    }
};

}  // namespace

// ── resolveName: the table (alpha = 1, bravo = 2) ───────────────────────────────────────────────────────────────
TEST(NamespaceGatewayRegression, ResolveTableRegression) {
    rune::catalog::NamespaceRegistry boot;
    rune::catalog::NamespaceMirror m(boot);
    ASSERT_TRUE(m.upsert(rune::catalog::NamespaceRecord{1, "alpha", {}, rune::catalog::NsActive{}, 1, 11}));
    ASSERT_TRUE(m.upsert(rune::catalog::NamespaceRecord{2, "bravo", {}, rune::catalog::NsActive{}, 1, 22}));
    const auto& reg = m.registry();
    using R = gw::Resolved;
    const auto found = [](uint32_t ns) { return gw::Resolution{R::Found, ns, "t"}; };
    const gw::Resolution hidden = gw::hiddenAsMissing("t");
    const gw::Resolution malformed{R::Malformed, gw::kHiddenNsId, {}};
    struct Row { uint32_t caller; bool root; std::string_view name; gw::Resolution want; };
    const Row rows[] = {
        {1, false, "t", found(1)},            // 1  unqualified = the caller's namespace
        {1, false, "default/t", hidden},      // 2  default is foreign to alpha
        {1, false, "alpha/t", found(1)},      // 3  own namespace, qualified
        {0, false, "t", found(0)},            // 4
        {0, false, "default/t", found(0)},    // 5
        {0, false, "alpha/t", hidden},        // 6
        {1, false, "bravo/t", hidden},        // 7  a foreign namespace = a missing one (the mutation target)
        {1, false, "nosuch/t", hidden},       // 8
        {1, true, "bravo/t", found(2)},       // 9  root reaches every namespace
        {1, true, "default/t", found(0)},     // 10
        {1, true, "nosuch/t", hidden},        // 11 root: an unknown namespace is still missing
        {0, true, "alpha/t", found(1)},       // 12
        {1, false, "alpha//t", malformed},    // 13
        {1, false, "/t", malformed},          // 14
        {1, false, "alpha/", malformed},      // 15
        {1, true, "a/b/c", malformed},        // 16
        {1, false, "", malformed},            // 17
        {2, false, "bravo/t", found(2)},      // 18
    };
    int i = 0;
    for (const Row& row : rows) {
        ++i;
        const gw::Resolution got = gw::resolveName(row.caller, row.root, reg, row.name);
        EXPECT_EQ(got.r, row.want.r) << "row " << i << " " << row.name;
        EXPECT_EQ(got.nsId, row.want.nsId) << "row " << i << " " << row.name;
        if (row.want.r != R::Malformed) EXPECT_EQ(got.local, row.want.local) << "row " << i << " " << row.name;
    }
    // Zero allocation: every row again inside the window.
    uint32_t sink = 0;
    {
        rune::testing::AllocWindow w;
        for (const Row& row : rows) sink += gw::resolveName(row.caller, row.root, reg, row.name).nsId;
        EXPECT_EQ(w.count(), 0u) << "resolveName allocated";
    }
    EXPECT_NE(sink, 0u);
}

// ── a foreign object by id: the session's namespace keys its permissions ───────────────────────────────────────
TEST(NamespaceGatewayRegression, ForeignIdNoPermsRegression) {
    NsGw g;
    AuthRig& r = g.r;
    ASSERT_GE(g.mine, 0);
    ASSERT_GE(g.secretb, 0);
    ASSERT_GE(g.bview, 0);
    EXPECT_EQ(r.produce(g.alice, g.mine, /*untilOk=*/true), kOk) << "control: her own namespace's topic";
    EXPECT_EQ(r.subscribeDenied(g.alice, g.secretb, /*reader=*/g.root, /*writer=*/g.root), kNotAuthorized);
    EXPECT_EQ(r.produceDenied(g.alice, g.secretb), kNotAuthorized);
    EXPECT_EQ(r.getViewRow(g.alice, g.bview), kNotAuthorized);
    EXPECT_EQ(r.produce(g.root, g.secretb, /*untilOk=*/true), kOk) << "root";
    EXPECT_EQ(r.subscribe(g.root, g.secretb), kOk) << "root";
    EXPECT_NE(r.getViewRow(g.root, g.bview), kNotAuthorized) << "root";
}

// ── listings: only the session's own namespace (root: every one) ───────────────────────────────────────────────
TEST(NamespaceGatewayRegression, ListingsFilteredRegression) {
    NsGw g;
    AuthRig& r = g.r;
    ASSERT_TRUE(r.awaitListed(r.orders));
    const auto has = [](const std::vector<int32_t>& v, int32_t id) { return std::find(v.begin(), v.end(), id) != v.end(); };
    // Placement settles asynchronously: wait (bounded) until root sees every topic, then compare.
    std::vector<int32_t> rootSees;
    r.c().pollFor(std::chrono::seconds(10), [&] {
        rootSees = r.metadataTopics(g.root);
        return has(rootSees, g.mine) && has(rootSees, g.secretb) && has(rootSees, r.orders);
    });
    EXPECT_TRUE(has(rootSees, g.mine));
    EXPECT_TRUE(has(rootSees, g.secretb));
    EXPECT_TRUE(has(rootSees, r.orders));
    const auto aliceSees = r.metadataTopics(g.alice);
    EXPECT_TRUE(has(aliceSees, g.mine)) << "her own namespace's topic";
    EXPECT_FALSE(has(aliceSees, g.secretb)) << "a bravo topic";
    EXPECT_FALSE(has(aliceSees, r.orders)) << "a default topic";
}

// ── a foreign namespace in a name answers byte-for-byte like an unknown one ─────────────────────────────────────
TEST(NamespaceGatewayRegression, ErrorMessageNoForeignNameRegression) {
    NsGw g;
    AuthRig& r = g.r;
    ShardTestClient& c = r.c();
    const auto ch = r.ch();
    const int32_t st = AuthRig::stream();
    const auto alterOps = [] {
        rune::codec::AlterOp o{};
        o.type = rune::codec::AlterOpType::Add; o.name = "extra"; o.primType = rune::codec::PrimType::Int64;
        return rune::codec::encodeAlterOps({o});
    }();
    const pc::ConfigChangeWire cc{static_cast<uint8_t>(rune::protocol::ConfigKey::RetentionMs), 600'000};
    struct Op { const char* what; std::function<int64_t(const std::string&)> send; };
    const std::vector<Op> ops = {
        {"GetTopicSchema", [&](const std::string& n) { return c.sendGetTopicSchema(ch, st, g.alice, n); }},
        {"DefineTopic", [&](const std::string& n) {
             return c.sendDefineTopic(ch, st, g.alice, n, 1, authRigCharBlob(), 0, {}, 0, authRigTopicConfig()); }},
        {"AlterTopic", [&](const std::string& n) { return c.sendAlterTopic(ch, st, g.alice, n, 0, alterOps); }},
        {"AlterTopicConfig", [&](const std::string& n) { return c.sendAlterTopicConfig(ch, st, g.alice, n, 0, {cc}); }},
        {"DropTopic", [&](const std::string& n) { return c.sendDropTopic(ch, st, g.alice, n); }},
        {"CreateView", [&](const std::string& n) {
             return c.sendCreateView(ch, st, g.alice, n, "id", 0, authRigViewPlan("secretb", {g.mine})); }},
        {"DropView", [&](const std::string& n) { return c.sendDropView(ch, st, g.alice, n); }},
        {"AlterView", [&](const std::string& n) {
             return c.sendAlterView(ch, st, g.alice, n, "id", 0, 0, authRigViewPlan("secretb", {g.mine})); }},
    };
    for (const Op& op : ops) {
        SCOPED_TRACE(op.what);
        const auto foreign = r.rawAnswer(op.send("bravo/secretb"));
        const auto unknown = r.rawAnswer(op.send("nosuchns/secretb"));
        ASSERT_FALSE(foreign.empty()) << "no answer";
        ASSERT_FALSE(unknown.empty()) << "no answer";
        EXPECT_EQ(bodyOf(foreign), bodyOf(unknown)) << "a foreign namespace answers like an unknown one";
        EXPECT_FALSE(contains(foreign, "bravo")) << "the foreign namespace's name is echoed";
        EXPECT_FALSE(contains(unknown, "nosuchns")) << "the asked namespace's name is echoed";
    }
    // By id: nothing names the foreign object or its namespace.
    for (const auto& a : {r.rawAnswer(c.sendSubscribe(ch, st, g.alice, g.secretb, 0)),
                          r.rawAnswer(c.sendGetViewRow(ch, st, g.alice, g.bview, 1))}) {
        ASSERT_FALSE(a.empty());
        EXPECT_FALSE(contains(a, "bravo"));
        EXPECT_FALSE(contains(a, "secretb"));
        EXPECT_FALSE(contains(a, "boardb"));
    }
}

// ── D8: groups and transactions exist only in `default` ────────────────────────────────────────────────────────
TEST(NamespaceGatewayRegression, GroupTxnRefusedOutsideDefaultRegression) {
    NsGw g;
    AuthRig& r = g.r;
    EXPECT_EQ(r.joinGroup(g.alice, "cgA", {g.mine}), kNotAuthorized) << "JoinGroup";
    EXPECT_EQ(r.syncGroup(g.alice, "cgA", "m", 1), kNotAuthorized) << "SyncGroup";
    EXPECT_EQ(r.heartbeat(g.alice, "cgA", "m"), kNotAuthorized) << "Heartbeat";
    EXPECT_EQ(r.leaveGroup(g.alice, "cgA", "m"), kNotAuthorized) << "LeaveGroup";
    EXPECT_EQ(r.offsetCommit(g.alice, "cgA", r.orders), kNotAuthorized) << "OffsetCommit";
    EXPECT_EQ(r.offsetFetch(g.alice, "cgA", r.orders), kNotAuthorized) << "OffsetFetch";
    EXPECT_EQ(r.initProducerId(g.alice, "txA"), kNotAuthorized) << "InitProducerId";
    EXPECT_EQ(r.addPartitions(g.alice, "txA", g.mine), kNotAuthorized) << "AddPartitionsToTxn";
    EXPECT_EQ(r.addOffsets(g.alice, "txA", "cgA"), kNotAuthorized) << "AddOffsetsToTxn";
    EXPECT_EQ(r.txnOffsetCommit(g.alice, "txA", "cgA", r.orders), kNotAuthorized) << "TxnOffsetCommit";
    EXPECT_EQ(r.endTxn(g.alice, "txA"), kNotAuthorized) << "EndTxn";
    // The coordinator never saw them: alice's commit left no offset, and her join no member / generation.
    EXPECT_EQ(r.committedOffset(g.root, "cgA", r.orders), r.committedOffset(g.root, "cgUntouched", r.orders))
        << "alice's OffsetCommit reached the coordinator";
    const auto joined = r.joinStable(g.root, "cgA", {r.orders});
    const auto fresh = r.joinStable(g.root, "cgFresh", {r.orders});
    ASSERT_GE(fresh.second, 0);
    EXPECT_EQ(joined.second, fresh.second) << "alice's JoinGroup reached the coordinator (an extra generation)";
    // Root in `default` keeps groups and transactions (control).
    EXPECT_EQ(r.initProducerId(g.root, "txRoot"), kOk);
}

/// D8 covers transactional ids only: an InitProducerId WITHOUT one mints a cluster-unique producer id for an
/// idempotent producer and touches no (ns, id)-keyed state, so a namespace user gets it (the C++ client's producer
/// always asks for one before its first produce; found by docker/namespace-e2e.sh step 5). A principal with no topic
/// WRITE still gets 45 (#211), and a transactional id stays refused.
/// Mutation: drop the `plainProducerIdRequest` exemption in ClientGateway's txn handler -> alice's first call is 45.
TEST(NamespaceGatewayRegression, IdempotentProducerIdOutsideDefaultRegression) {
    NsGw g;
    AuthRig& r = g.r;
    EXPECT_EQ(r.initProducerId(g.alice, ""), kOk) << "idempotent InitProducerId for alpha/alice";
    EXPECT_EQ(r.initProducerId(g.alice, "txA"), kNotAuthorized) << "a transactional id stays refused outside default";
    r.broker().createUser("alpha/nowrite", AuthRig::kPw, {prefix(SO::Read, ST::Topic, "")});
    const int64_t nowrite = r.login("alpha/nowrite");
    EXPECT_EQ(r.initProducerId(nowrite, ""), kNotAuthorized) << "no topic WRITE mints no producer id (#211)";
}

/// D8, the cluster admin's half (namespaces Task 12 matrix): root logs in to `default`, so the session-nsId check
/// passes it; naming a group or transaction THROUGH another namespace - a non-default topic id, or a group /
/// transactional id qualified by an existing non-default namespace - is still refused 45 before the coordinator.
/// Mutation: drop the `rootOutsideDefault_` term in ClientGateway's group / txn handlers → every refusal below is 0.
TEST(NamespaceGatewayRegression, RootGroupTxnOutsideDefaultRegression) {
    NsGw g;
    AuthRig& r = g.r;
    EXPECT_EQ(r.joinGroup(g.root, "cgR", {g.mine}), kNotAuthorized) << "JoinGroup on an alpha topic";
    EXPECT_EQ(r.offsetCommit(g.root, "cgR", g.mine), kNotAuthorized) << "OffsetCommit on an alpha topic";
    EXPECT_EQ(r.offsetFetch(g.root, "cgR", g.mine), kNotAuthorized) << "OffsetFetch on an alpha topic";
    EXPECT_EQ(r.joinGroup(g.root, "alpha/cg", {r.orders}), kNotAuthorized) << "JoinGroup on an alpha-qualified group";
    EXPECT_EQ(r.initProducerId(g.root, "alpha/tx"), kNotAuthorized) << "InitProducerId on an alpha-qualified txn id";
    EXPECT_EQ(r.addPartitions(g.root, "txR", g.mine), kNotAuthorized) << "AddPartitionsToTxn on an alpha topic";
    EXPECT_EQ(r.addOffsets(g.root, "txR", "bravo/cg"), kNotAuthorized) << "AddOffsetsToTxn on a bravo-qualified group";
    EXPECT_EQ(r.txnOffsetCommit(g.root, "txR", "cgR", g.mine), kNotAuthorized) << "TxnOffsetCommit on an alpha topic";
    // The coordinator never saw the join: a fresh group's first generation equals the refused one's.
    const auto joined = r.joinStable(g.root, "cgR", {r.orders});
    const auto fresh = r.joinStable(g.root, "cgRFresh", {r.orders});
    ASSERT_GE(fresh.second, 0);
    EXPECT_EQ(joined.second, fresh.second) << "root's refused JoinGroup reached the coordinator";
    // Controls: `default` topics, "default/"-qualified and unknown-qualifier ids (opaque in phase 1) stay allowed.
    EXPECT_EQ(r.initProducerId(g.root, "default/tx"), kOk);
    EXPECT_EQ(r.initProducerId(g.root, "nosuchns/tx"), kOk);
    EXPECT_EQ(r.offsetFetch(g.root, "cgR", r.orders), kOk);
}

// ── a malformed qualified name: 40 on every name-taking request, root included ──────────────────────────────────
TEST(NamespaceGatewayRegression, MalformedNameRegression) {
    NsGw g;
    AuthRig& r = g.r;
    for (const int64_t sid : {g.alice, g.root}) {
        SCOPED_TRACE(sid == g.root ? "root" : "alpha/alice");
        for (const char* bad : {"alpha//x", "/x", "alpha/", "a/b/c"}) {
            SCOPED_TRACE(bad);
            EXPECT_EQ(r.getTopicSchema(sid, bad), kInvalidTopicName) << "GetTopicSchema";
            EXPECT_EQ(r.defineTopicCode(sid, bad), kInvalidTopicName) << "DefineTopic";
            EXPECT_EQ(r.alterTopicCode(sid, bad), kInvalidTopicName) << "AlterTopic";
            EXPECT_EQ(r.alterConfigCode(sid, bad), kInvalidTopicName) << "AlterTopicConfig";
            EXPECT_EQ(r.dropTopicCode(sid, bad), kInvalidTopicName) << "DropTopic";
            EXPECT_EQ(r.createViewCode(sid, bad, r.orders), kInvalidTopicName) << "CreateView";
            EXPECT_EQ(r.dropViewCode(sid, bad), kInvalidTopicName) << "DropView";
            EXPECT_EQ(r.alterViewCode(sid, bad, r.orders), kInvalidTopicName) << "AlterView";
        }
    }
}

// ── Adversarial (batch review, Tasks 9–11): attacks through the wire on the same names, internal logs, bounds ────
//   | caller          | target                                   | operation                      | expected          | test
//   |-----------------|------------------------------------------|--------------------------------|-------------------|------
//   | alpha/carol     | "secretb" (bravo has one) / "BOARDB"     | define (unqualified)           | 0: own namespace  | SameLocalNameIsolation
//   | alpha/carol     | "Mine" (folds onto alpha/mine)           | define                         | refused (control) | SameLocalNameIsolation
//   | alpha/carol     | bravo/secretb vs nosuchns/secretb        | drop                           | same code, kept   | SameLocalNameIsolation
//   | alpha/carol     | bravo/newx                               | define                         | refused, absent   | SameLocalNameIsolation
//   | alpha/carol     | "secretb"                                | drop                           | 0, bravo's kept   | SameLocalNameIsolation
//   | alice, ns admin | __catalog                                | catalog subscribe              | 45 (root: 0)      | InternalLogsTenantDenied
//   | root            | alpha/__x vs __x                         | define                         | same refusal      | InternalLogsTenantDenied
//   | root            | 249 / 250-byte local, NUL, 63/64-byte ns | define, schema                 | bounds hold       | NameBounds

/// Mutation: drop the foreign-namespace hide in NsResolve.cpp (`nsId != callerNsId && !root`) → the drop of
/// bravo/secretb answers differently from nosuchns/secretb, and bravo/newx is CREATED (the name-based grant check is
/// namespace-blind: the resolver is the only fence).  Mutation: CatalogState::liveNameFoldingOnto scans every
/// namespace → carol's "BOARDB" is refused (an oracle on bravo's names).
TEST(NamespaceGatewayRegression, SameLocalNameIsolationAdversarialRegression) {
    NsGw g;
    AuthRig& r = g.r;
    r.broker().createUser("alpha/carol", AuthRig::kPw,
                          {prefix(SO::Create, ST::Topic, ""), prefix(SO::Drop, ST::Topic, ""),
                           prefix(SO::Alter, ST::Topic, ""), prefix(SO::Describe, ST::Topic, ""),
                           prefix(SO::Read, ST::Topic, "")});
    const int64_t carol = r.login("alpha/carol");
    ASSERT_GT(carol, 0);
    EXPECT_EQ(r.defineTopicCode(carol, "secretb"), kOk) << "bravo's local name is free in alpha";
    EXPECT_EQ(r.defineTopicCode(carol, "BOARDB"), kOk) << "a case-fold onto a FOREIGN view's name is no collision";
    EXPECT_NE(r.defineTopicCode(carol, "Mine"), kOk) << "control: a case-fold onto her own namespace's topic";
    EXPECT_EQ(r.getTopicSchema(carol, "secretb"), kOk);
    // A drop through the foreign qualifier answers like an unknown namespace and leaves bravo's topic alone.
    const int foreignDrop = r.dropTopicCode(carol, "bravo/secretb");
    EXPECT_NE(foreignDrop, kOk);
    EXPECT_EQ(foreignDrop, r.dropTopicCode(carol, "nosuchns/secretb"));
    EXPECT_EQ(r.getTopicSchema(g.root, "bravo/secretb"), kOk) << "bravo's topic survived";
    // A create through the foreign qualifier is refused and nothing appears in bravo.
    const int foreignCreate = r.defineTopicCode(carol, "bravo/newx");
    EXPECT_NE(foreignCreate, kOk);
    EXPECT_EQ(foreignCreate, r.defineTopicCode(carol, "nosuchns/newx"));
    EXPECT_NE(r.getTopicSchema(g.root, "bravo/newx"), kOk) << "the foreign create landed";
    // Her own unqualified drop takes her topic only.
    EXPECT_EQ(r.dropTopicCode(carol, "secretb"), kOk);
    EXPECT_EQ(r.getTopicSchema(g.root, "bravo/secretb"), kOk) << "her drop of `secretb` took bravo's";
    // Root's unqualified name resolves in ITS namespace (`default`), never by searching the others.
    EXPECT_NE(r.getTopicSchema(g.root, "secretb"), kOk) << "root's bare name found a tenant topic";
}

/// Mutation: the gateway's catalog-subscribe gate (DESCRIBE ON CLUSTER) admits everyone → the 45s turn 0.
/// Mutation: AdminWorker checks the local name only in `default` (`&& m.nsId == 0`) → alpha/__x is created.
TEST(NamespaceGatewayRegression, InternalLogsTenantDeniedAdversarialRegression) {
    NsGw g;
    AuthRig& r = g.r;
    r.broker().createUser("alpha/nsadm", AuthRig::kPw, {exact(SO::Admin, ST::Namespace, "alpha")});
    const int64_t nsadm = r.login("alpha/nsadm");
    ASSERT_GT(nsadm, 0);
    EXPECT_EQ(r.catalogSubscribe(g.alice), kNotAuthorized) << "a tenant user reads the cluster-wide catalog";
    EXPECT_EQ(r.catalogSubscribe(nsadm), kNotAuthorized) << "a namespace admin reads the cluster-wide catalog";
    EXPECT_EQ(r.catalogSubscribe(g.root), kOk) << "control: root";
    // The reserved prefix holds through a qualifier, for root too.
    const int bare = r.defineTopicCode(g.root, "__x");
    EXPECT_NE(bare, kOk);
    EXPECT_EQ(r.defineTopicCode(g.root, "alpha/__x"), bare);
    EXPECT_EQ(r.defineTopicCode(g.root, "default/__x"), bare);
    EXPECT_NE(r.getTopicSchema(g.root, "alpha/__x"), kOk);
}

/// Mutation: AdminWorker checks the local name only in `default` (`&& m.nsId == 0`) → the 250-byte local name under
/// `alpha/` is created.  The NUL rows guard a resolver or encoder that truncates at a NUL.
TEST(NamespaceGatewayRegression, NameBoundsAdversarialRegression) {
    NsGw g;
    AuthRig& r = g.r;
    const std::string max(rune::catalog::kMaxTopicNameBytes, 'a');
    const std::string over(rune::catalog::kMaxTopicNameBytes + 1, 'b');
    EXPECT_EQ(r.defineTopicCode(g.root, "alpha/" + max), kOk) << "a max-length local name under a qualifier";
    EXPECT_EQ(r.getTopicSchema(g.root, "alpha/" + max), kOk);
    EXPECT_EQ(r.getTopicSchema(g.alice, max), kOk) << "the same topic, unqualified, from inside alpha";
    const int tooLong = r.defineTopicCode(g.root, over);
    EXPECT_NE(tooLong, kOk);
    EXPECT_EQ(r.defineTopicCode(g.root, "alpha/" + over), tooLong) << "the qualifier changed the length check";
    // An embedded NUL never truncates either part.
    const std::string nulNs = std::string("alpha\0", 6) + "/nulns";
    const std::string nulLocal = std::string("alpha/x\0y", 9);
    EXPECT_NE(r.defineTopicCode(g.root, nulNs), kOk);
    EXPECT_NE(r.getTopicSchema(g.root, "alpha/nulns"), kOk) << "the NUL-truncated namespace resolved";
    EXPECT_NE(r.defineTopicCode(g.root, nulLocal), kOk);
    EXPECT_NE(r.getTopicSchema(g.root, "alpha/x"), kOk) << "the NUL-truncated local name was created";
    // An unknown namespace at 63 and 64 bytes answers like any unknown namespace (no length oracle, no crash).
    const int unknown = r.defineTopicCode(g.root, "nosuchns/t");
    EXPECT_NE(unknown, kOk);
    EXPECT_EQ(r.defineTopicCode(g.root, std::string(rune::catalog::kMaxNamespaceNameBytes, 'z') + "/t"), unknown);
    EXPECT_EQ(r.defineTopicCode(g.root, std::string(rune::catalog::kMaxNamespaceNameBytes + 1, 'z') + "/t"), unknown);
}
