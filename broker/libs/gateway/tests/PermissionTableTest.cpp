// PermissionTableTest.cpp - authentication Task 12: the per-session permission table (compile, created-after-login,
// write bits) and the Authorizer (the one place a code or a detail text is chosen).
#include <rune/gateway/Authorizer.h>
#include <rune/gateway/ObjectIndex.h>
#include <rune/gateway/PermissionPool.h>
#include <rune/gateway/PermissionTable.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

namespace rg = rune::gateway;
namespace rs = rune::security;
using rs::Operation; using rs::ResourceType; using rs::PatternKind;

namespace {
rs::GrantSpec g(Operation o, ResourceType t, PatternKind k, std::string n) { return {o, {t, k, std::move(n)}}; }
}

TEST(PermissionTableRegression, CompilesExactPrefixClusterAndImpliedDescribe) {
    rg::ObjectIndex idx; idx.onTopic(0, "player_events"); idx.onTopic(1, "world.zone1"); idx.onTopic(2, "secret");
    idx.onView(3, "leaderboard");
    const std::vector<rs::GrantSpec> grants = {g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "player_events"),
                                               g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world."),
                                               g(Operation::Read, ResourceType::View, PatternKind::Exact, "leaderboard")};
    auto t = rg::PermissionTable::compile(grants, idx);
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Describe)));
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Write)));
    EXPECT_TRUE(t.allows(1, rs::opBit(Operation::Write)));
    EXPECT_FALSE(t.allows(2, rs::opBit(Operation::Describe))) << "default deny";
    EXPECT_TRUE(t.allows(3, rs::opBit(Operation::Read)));
    EXPECT_FALSE(t.allows(4, rs::opBit(Operation::Read))) << "an id beyond the table";
    EXPECT_FALSE(t.allows(-1, rs::opBit(Operation::Read))) << "a negative (internal) id";
}

TEST(PermissionTableRegression, AnObjectCreatedAfterLoginIsCoveredByAMatchingPrefix) {
    rg::ObjectIndex idx; idx.onTopic(0, "a");
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "world.")}}, idx);
    EXPECT_FALSE(t.allows(7, rs::opBit(Operation::Write)));
    t.onObjectCreated(7, {"world.zone9", ResourceType::Topic});
    EXPECT_TRUE(t.allows(7, rs::opBit(Operation::Write)));
    t.onObjectCreated(8, {"other", ResourceType::Topic});
    EXPECT_FALSE(t.allows(8, rs::opBit(Operation::Write)));
    t.onObjectDropped(7);
    EXPECT_FALSE(t.allows(7, rs::opBit(Operation::Write))) << "a dropped id never keeps its bits";
    EXPECT_EQ(t.writeBits().size(), 1u);
    EXPECT_EQ(t.writeBits()[0] & (uint64_t{1} << 7), 0u) << "the write bit is cleared with the ops";
}

TEST(PermissionTableRegression, WriteBitsMirrorTheWriteOps) {
    rg::ObjectIndex idx; for (int i = 0; i < 130; ++i) idx.onTopic(i, "t" + std::to_string(i));
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "t129")}}, idx);
    const auto bits = t.writeBits();
    ASSERT_EQ(bits.size(), 3u);
    EXPECT_EQ(bits[2], uint64_t{1} << (129 - 128));
    EXPECT_EQ(bits[0], 0u);
}

TEST(PermissionTableRegression, AllowAllAllowsEverythingIncludingCluster) {
    auto t = rg::PermissionTable::allowAll();
    EXPECT_TRUE(t.allows(12345, rs::opBit(Operation::Write)));
    EXPECT_TRUE(t.allows(-9, rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allowsCluster(rs::opBit(Operation::Admin)));
    EXPECT_TRUE(t.allowsName(ResourceType::Group, "any", rs::opBit(Operation::Read)));
}

TEST(PermissionTableRegression, DenyAllDeniesEverything) {
    auto t = rg::PermissionTable::denyAll();
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Describe)));
    EXPECT_FALSE(t.allowsCluster(rs::opBit(Operation::Describe)));
    EXPECT_FALSE(t.allowsName(ResourceType::Topic, "x", rs::opBit(Operation::Describe)));
}

TEST(PermissionTableRegression, ClusterGrantsReachEveryTopicViewButNeverDataOrInternalNames) {
    rg::ObjectIndex idx; idx.onTopic(0, "a"); idx.onView(1, "v");
    auto t = rg::PermissionTable::compile({{g(Operation::Drop, ResourceType::Cluster, PatternKind::Exact, ""),
                                            g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, "")}}, idx);
    EXPECT_TRUE(t.allows(0, rs::opBit(Operation::Drop)));
    EXPECT_TRUE(t.allows(1, rs::opBit(Operation::Drop)));
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Read))) << "DROP/DESCRIBE on CLUSTER imply no data access";
    EXPECT_TRUE(t.allowsCluster(rs::opBit(Operation::Drop)));
    EXPECT_FALSE(t.allowsCluster(rs::opBit(Operation::Create)));
    EXPECT_FALSE(t.allowsCluster(rs::opBit(Operation::Admin)));
    t.onObjectCreated(5, {"later", ResourceType::Topic});
    EXPECT_TRUE(t.allows(5, rs::opBit(Operation::Drop))) << "a cluster grant covers an object created after login";
}

TEST(PermissionTableRegression, APrefixNeverMatchesAnInternalName) {   // I6
    rg::ObjectIndex idx; idx.onTopic(0, "__catalog_like");
    auto t = rg::PermissionTable::compile({{g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "")}}, idx);
    EXPECT_FALSE(t.allows(0, rs::opBit(Operation::Read)));
    EXPECT_FALSE(t.allowsName(ResourceType::Topic, "__x", rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allowsName(ResourceType::Topic, "x", rs::opBit(Operation::Read)));
}

TEST(PermissionTableRegression, NamePatternsCoverGroupsAndTransactionalIds) {
    auto t = rg::PermissionTable::compile({{g(Operation::Read, ResourceType::Group, PatternKind::Prefix, "cg-"),
                                            g(Operation::Write, ResourceType::TransactionalId, PatternKind::Exact, "tx1")}}, {});
    EXPECT_TRUE(t.allowsName(ResourceType::Group, "cg-a", rs::opBit(Operation::Read)));
    EXPECT_FALSE(t.allowsName(ResourceType::Group, "other", rs::opBit(Operation::Read)));
    EXPECT_TRUE(t.allowsName(ResourceType::TransactionalId, "tx1", rs::opBit(Operation::Write)));
    EXPECT_FALSE(t.allowsName(ResourceType::TransactionalId, "tx2", rs::opBit(Operation::Write)));
}

TEST(ObjectIndexRegression, TracksLiveIdsAndDrops) {
    rg::ObjectIndex idx;
    EXPECT_EQ(idx.at(0), nullptr);
    idx.onTopic(2, "b"); idx.onView(5, "v");
    EXPECT_EQ(idx.idLimit(), 6);
    ASSERT_NE(idx.at(2), nullptr);
    EXPECT_EQ(idx.at(2)->name, "b");
    EXPECT_EQ(idx.at(5)->type, ResourceType::View);
    EXPECT_EQ(idx.at(3), nullptr) << "a gap is not live";
    EXPECT_EQ(idx.at(-6), nullptr);
    int n = 0; idx.forEach([&](int32_t, const rg::ObjectEntry&) { ++n; });
    EXPECT_EQ(n, 2);
    idx.onDropped(2);
    EXPECT_EQ(idx.at(2), nullptr);
    idx.onDropped(99);   // unknown: idempotent
    idx.onDropped(-3);
}

// ── the Authorizer: the only place a code or a detail is chosen ────────────────────────────────────────────────
TEST(AuthorizerRegression, DetailTextIsBuiltOnlyFromTheRequest) {
    rg::ObjectIndex idx; idx.onTopic(0, "hidden");
    auto t = rg::PermissionTable::compile({}, idx);
    const auto hidden  = rg::authorizeTopicId(t, 0, Operation::Read, idx);
    const auto missing = rg::authorizeTopicId(t, 77, Operation::Read, idx);
    ASSERT_TRUE(hidden.has_value());
    ASSERT_TRUE(missing.has_value());
    EXPECT_EQ(hidden->code, 45);
    EXPECT_EQ(hidden->detail, "not authorized: READ on TOPIC 0");
    EXPECT_EQ(missing->detail, "not authorized: READ on TOPIC 77");
    EXPECT_EQ(rg::authorizeName(t, ResourceType::Topic, "t1", Operation::Drop)->detail, "not authorized: DROP on TOPIC t1");
    EXPECT_EQ(rg::authorizeCluster(t, Operation::Admin)->detail, "not authorized: ADMIN on CLUSTER");
}

TEST(AuthorizerRegression, DescribeOnClusterTurnsAMissingIdIntoTheExistingUnknownTopic) {   // D22
    rg::ObjectIndex idx; idx.onTopic(0, "exists");
    auto t = rg::PermissionTable::compile({{g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, "")}}, idx);
    EXPECT_FALSE(rg::authorizeTopicId(t, 77, Operation::Read, idx).has_value()) << "missing id: no denial, UnknownTopic follows";
    EXPECT_TRUE(rg::authorizeTopicId(t, 0, Operation::Read, idx).has_value()) << "a live id it may not read is denied";
}

TEST(AuthorizerRegression, ViewSourceUnknownIdIsDeniedEvenWithDescribeOnClusterRegression) {
    rg::ObjectIndex idx; idx.onTopic(0, "exists");
    const std::vector<rune::security::GrantSpec> grants{g(Operation::Describe, ResourceType::Cluster, PatternKind::Exact, ""),
                                                        g(Operation::Read, ResourceType::Topic, PatternKind::Exact, "exists")};
    auto t = rg::PermissionTable::compile(grants, idx);
    EXPECT_TRUE(rg::authorizeViewSource(t, 77).has_value()) << "no D22 for view sources: an unindexed id may be a CREATE TOPIC in flight";
    EXPECT_FALSE(rg::authorizeTopicId(t, 77, Operation::Read, idx).has_value()) << "D22 unchanged for the other callers";
    EXPECT_FALSE(rg::authorizeViewSource(t, 0).has_value());
}

TEST(AuthorizerRegression, GrantedRequestsAreNotDenied) {
    rg::ObjectIndex idx; idx.onTopic(0, "a");
    auto t = rg::PermissionTable::compile({{g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "a")}}, idx);
    EXPECT_FALSE(rg::authorizeTopicId(t, 0, Operation::Write, idx).has_value());
    EXPECT_FALSE(rg::authorizeName(t, ResourceType::Topic, "a", Operation::Write).has_value());
    EXPECT_FALSE(rg::authorizeCluster(rg::PermissionTable::allowAll(), Operation::Admin).has_value());
}

TEST(AuthorizerRegression, AnOverlongNameIsCutInTheDetailIdenticallyForHiddenAndMissing) {
    auto t = rg::PermissionTable::denyAll();
    const auto d = rg::authorizeName(t, ResourceType::Topic, std::string(5000, 'x'), Operation::Drop);
    ASSERT_TRUE(d.has_value());
    EXPECT_LE(d->detail.size(), 300u);
}

// ── Issue #259 (X108): one compiled table per distinct grant set ──
namespace {
std::vector<rs::GrantSpec> roleSet(const std::string& prefix) {
    std::vector<rs::GrantSpec> v = {g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, prefix),
                                    g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, prefix + "w")};
    std::sort(v.begin(), v.end());
    return v;
}
rg::ObjectIndex manyTopics(int n) {
    rg::ObjectIndex idx;
    for (int i = 0; i < n; ++i) idx.onTopic(i, (i % 2 ? "worldw." : "world.") + std::to_string(i));
    return idx;
}
}  // namespace

// RED before the fix (one compile per session): 1 000 sessions of one role compiled 1 000 tables.
TEST(PermissionPoolRegression, SessionsSharingAGrantSetCompileOneTableRegression) {
    const auto idx = manyTopics(2000);
    rg::PermissionPool pool;
    std::vector<rg::PermissionRef> sessions;
    for (int i = 0; i < 1000; ++i) sessions.push_back(pool.acquire(roleSet("world"), idx));
    EXPECT_EQ(pool.compiles(), 1u) << "one compile for the set, not one per session";
    EXPECT_EQ(pool.liveTables(), 1u);
    EXPECT_EQ(&sessions.front().table(), &sessions.back().table()) << "the very same table";
    EXPECT_TRUE(sessions[7]->allows(1, rs::opBit(Operation::Write)));
    EXPECT_FALSE(sessions[7]->allows(0, rs::opBit(Operation::Write)));
    const auto other = pool.acquire(roleSet("other"), idx);
    EXPECT_EQ(pool.compiles(), 2u);
    EXPECT_NE(other.id(), sessions[0].id()) << "a different set never shares";
}

TEST(PermissionPoolRegression, TheLastHolderFreesTheTableAndItsIdIsReusedRegression) {
    const auto idx = manyTopics(10);
    rg::PermissionPool pool;
    uint32_t id = 0;
    {
        auto a = pool.acquire(roleSet("world"), idx);
        auto b = a;   // a second holder
        id = a.id();
        EXPECT_EQ(pool.refsOf(id), 2u);
        pool.drainEvents([](auto, uint32_t, const rg::PermissionTable&) {});
    }
    EXPECT_EQ(pool.liveTables(), 0u);
    std::vector<rg::PermissionPool::Event> evs;
    pool.drainEvents([&](rg::PermissionPool::Event e, uint32_t i, const rg::PermissionTable&) { evs.push_back(e); EXPECT_EQ(i, id); });
    ASSERT_EQ(evs.size(), 1u);
    EXPECT_EQ(evs[0], rg::PermissionPool::Event::Freed) << "the workers are told to drop it";
    const auto c = pool.acquire(roleSet("other"), idx);
    EXPECT_EQ(c.id(), id) << "ids stay dense (the workers' table store is bounded)";
    EXPECT_FALSE(c->allows(1, rs::opBit(Operation::Write))) << "the reused entry holds the NEW set only";
}

TEST(PermissionPoolRegression, AnObjectCreatedUpdatesEachLiveTableOnceRegression) {
    auto idx = manyTopics(10);
    rg::PermissionPool pool;
    std::vector<rg::PermissionRef> sessions;
    for (int i = 0; i < 100; ++i) sessions.push_back(pool.acquire(roleSet("world"), idx));
    const auto stranger = pool.acquire(roleSet("other"), idx);
    idx.onTopic(10, "worldw.new");
    int calls = 0, writeGained = 0;
    pool.onObjectCreated(10, *idx.at(10), [&](uint32_t, const rg::PermissionTable&, bool before, bool after) {
        ++calls;
        writeGained += !before && after ? 1 : 0;
    });
    EXPECT_EQ(calls, 2) << "two live tables, not 101 sessions";
    EXPECT_EQ(writeGained, 1);
    EXPECT_TRUE(sessions[42]->allows(10, rs::opBit(Operation::Write)));
    EXPECT_FALSE(stranger->allows(10, rs::opBit(Operation::Write)));
}

TEST(PermissionPoolRegression, AnUnpooledRefIsItsOwnTableAndDenyByDefaultRegression) {
    const rg::PermissionRef none;
    EXPECT_EQ(none.id(), rg::PermissionRef::kNoId);
    EXPECT_FALSE(none->allows(0, rs::opBit(Operation::Read)));
    const rg::PermissionRef all(rg::PermissionTable::allowAll());
    EXPECT_TRUE(all->isAllowAll());
    const rg::PermissionRef copy = all;
    EXPECT_TRUE(copy->isAllowAll());
}

// ── PR #441 adversarial review (issue #259 area): the pool's lifetimes, id reuse and bounds ──
namespace {
std::vector<rs::GrantSpec> writeOne(int i) { return {g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "t" + std::to_string(i))}; }
}  // namespace

// Lifetime / ordering: a set freed and its id reused by another set BEFORE the events were drained.  The workers must
// see Freed before the new Created (a drop never lands after the new table), and every Created ships the id's CURRENT
// content (never the freed set's bits).
TEST(PermissionPoolAdversarialRegression, AnIdFreedAndReusedBeforeTheDrainNeverShipsTheOldBitsAdversarialRegression) {
    auto idx = manyTopics(10);   // odd ids: "worldw.N"
    rg::PermissionPool pool;
    uint32_t id = 0;
    {
        const auto a = pool.acquire(roleSet("world"), idx);   // WRITE on the odd topics
        id = a.id();
    }
    const auto b = pool.acquire(roleSet("other"), idx);       // WRITE on nothing here
    ASSERT_EQ(b.id(), id) << "the freed id is reused";
    std::vector<std::pair<rg::PermissionPool::Event, bool>> evs;
    pool.drainEvents([&](rg::PermissionPool::Event e, uint32_t i, const rg::PermissionTable& t) {
        EXPECT_EQ(i, id);
        evs.emplace_back(e, t.allows(1, rs::opBit(Operation::Write)));
    });
    ASSERT_EQ(evs.size(), 3u);
    EXPECT_EQ(evs[0].first, rg::PermissionPool::Event::Created);
    EXPECT_EQ(evs[1].first, rg::PermissionPool::Event::Freed);
    EXPECT_EQ(evs[2].first, rg::PermissionPool::Event::Created) << "the drop precedes the new table";
    EXPECT_FALSE(evs[0].second) << "even the first Created ships the CURRENT (new) set, never the freed one's WRITE";
    EXPECT_FALSE(evs[2].second);
}

// Resources: the pool is bounded; a new set beyond it fails CLOSED (deny-all, never another set's table), an existing set
// still shares, and freeing one makes room again.
TEST(PermissionPoolAdversarialRegression, AFullPoolFailsClosedAndRecoversWhenASetLeavesAdversarialRegression) {
    rg::ObjectIndex idx;
    for (int i = 0; i < 8; ++i) idx.onTopic(i, "t" + std::to_string(i));
    rg::PermissionPool pool;
    std::vector<rg::PermissionRef> held;
    held.reserve(rg::PermissionPool::kMaxTables);
    for (uint32_t i = 0; i < rg::PermissionPool::kMaxTables; ++i) held.push_back(pool.acquire(writeOne(static_cast<int>(i)), idx));
    ASSERT_EQ(pool.liveTables(), rg::PermissionPool::kMaxTables);
    const auto over = pool.acquire(writeOne(999'999), idx);
    EXPECT_EQ(over.id(), rg::PermissionRef::kNoId);
    EXPECT_FALSE(over->allows(0, rs::opBit(Operation::Write)));
    EXPECT_FALSE(over->isAllowAll());
    const auto shared = pool.acquire(writeOne(3), idx);
    EXPECT_EQ(shared.id(), held[3].id()) << "an existing set still shares when the pool is full";
    EXPECT_TRUE(shared->allows(3, rs::opBit(Operation::Write)));
    held[5] = rg::PermissionRef{};   // its last holder leaves
    const auto again = pool.acquire(writeOne(999'999), idx);
    EXPECT_NE(again.id(), rg::PermissionRef::kNoId);
    EXPECT_FALSE(again->allows(5, rs::opBit(Operation::Write))) << "the reused entry is the new set's";
}

// Lifetime: copies, moves, self-assignment and swaps keep the count exact - one Freed when (and only when) the last
// holder goes, never a double release.
TEST(PermissionPoolAdversarialRegression, RefCopiesMovesAndSelfAssignmentKeepTheCountExactAdversarialRegression) {
    const auto idx = manyTopics(4);
    rg::PermissionPool pool;
    auto a = pool.acquire(roleSet("world"), idx);
    const uint32_t id = a.id();
    pool.drainEvents([](auto, uint32_t, const rg::PermissionTable&) {});
    {
        rg::PermissionRef b = a;                 // 2
        rg::PermissionRef c = std::move(b);      // 2 (b empty)
        auto& self = c;
        c = self;                                // 2
        rg::PermissionRef d;
        d.swap(c);                               // 2
        EXPECT_EQ(pool.refsOf(id), 2u);
        a = std::move(d);                        // a's old hold released, d's moved in: 1
        EXPECT_EQ(pool.refsOf(id), 1u);
    }
    EXPECT_EQ(pool.refsOf(id), 1u);
    EXPECT_FALSE(pool.hasEvents()) << "no Freed while a holder remains";
    a = rg::PermissionRef{};
    int freed = 0;
    pool.drainEvents([&](rg::PermissionPool::Event e, uint32_t, const rg::PermissionTable&) {
        freed += e == rg::PermissionPool::Event::Freed ? 1 : 0;
    });
    EXPECT_EQ(freed, 1);
    EXPECT_EQ(pool.liveTables(), 0u);
}

// Schema-ish churn on the object index: a topic dropped and its id reused by a name the set does NOT cover clears the
// WRITE bit in every live table (never a stale allow for the new topic).
TEST(PermissionPoolAdversarialRegression, ADroppedTopicIdReusedOutsideThePrefixIsNotWritableAdversarialRegression) {
    auto idx = manyTopics(4);   // 1, 3 = "worldw.*"
    rg::PermissionPool pool;
    const auto r = pool.acquire(roleSet("world"), idx);
    ASSERT_TRUE(r->allows(3, rs::opBit(Operation::Write)));
    idx.onDropped(3);
    pool.onObjectDropped(3, [](uint32_t, const rg::PermissionTable&, bool, bool) {});
    EXPECT_FALSE(r->allows(3, rs::opBit(Operation::Write)));
    idx.onTopic(3, "elsewhere.3");
    pool.onObjectCreated(3, *idx.at(3), [](uint32_t, const rg::PermissionTable&, bool, bool) {});
    EXPECT_FALSE(r->allows(3, rs::opBit(Operation::Write))) << "the reused id is a different topic";
    EXPECT_FALSE(r->allows(3, rs::opBit(Operation::Read)));
}

// ── issue #128: GetTopicSchema checks only the grants of the object's own kind ──────────────────────────────────
TEST(AuthorizerRegression, AViewGrantNeverDescribesATopicRegression) {
    rg::ObjectIndex idx; idx.onTopic(0, "payments"); idx.onView(1, "payboard");
    const auto viewOnly  = rg::PermissionTable::compile({{g(Operation::Describe, ResourceType::View, PatternKind::Prefix, "pay")}}, idx);
    const auto topicOnly = rg::PermissionTable::compile({{g(Operation::Describe, ResourceType::Topic, PatternKind::Prefix, "pay")}}, idx);
    const auto denied = rg::authorizeTopicOrViewName(viewOnly, "payments", Operation::Describe, idx);
    ASSERT_TRUE(denied.has_value()) << "a VIEW grant must not describe the topic 'payments'";
    EXPECT_EQ(denied->code, 45);
    EXPECT_EQ(denied->detail, "not authorized: DESCRIBE on TOPIC payments") << "the text always says TOPIC (D23)";
    EXPECT_TRUE(rg::authorizeTopicOrViewName(topicOnly, "payboard", Operation::Describe, idx).has_value())
        << "a TOPIC grant must not describe the view 'payboard'";
    EXPECT_FALSE(rg::authorizeTopicOrViewName(viewOnly, "payboard", Operation::Describe, idx).has_value());
    EXPECT_FALSE(rg::authorizeTopicOrViewName(topicOnly, "payments", Operation::Describe, idx).has_value());
}

TEST(AuthorizerRegression, AnUnknownSchemaNameAcceptsEitherKindsGrantRegression) {   // D22 parity: "unknown" follows
    rg::ObjectIndex idx; idx.onTopic(0, "payments");
    const auto viewOnly = rg::PermissionTable::compile({{g(Operation::Describe, ResourceType::View, PatternKind::Prefix, "pay")}}, idx);
    EXPECT_FALSE(rg::authorizeTopicOrViewName(viewOnly, "payroll", Operation::Describe, idx).has_value());
    idx.onDropped(0);
    EXPECT_FALSE(rg::authorizeTopicOrViewName(viewOnly, "payments", Operation::Describe, idx).has_value())
        << "a dropped name is unknown again";
    EXPECT_TRUE(rg::authorizeTopicOrViewName(viewOnly, "orders", Operation::Describe, idx).has_value());
}

// #211: hasAnyGrant(type, op) - any literal, prefixed or wildcard grant of `op` ON that type, kept at compile time
// (O(1) per request); allow-all always, deny-all never; READ is not WRITE, a group / transactional-id / CLUSTER grant
// is not a topic grant.  authorizeAnyGrant denies with 45 and names no resource.
TEST(PermissionTableRegression, HasAnyGrantCountsEveryPatternOfThatTypeAndOpRegression) {
    rg::ObjectIndex idx; idx.onTopic(0, "orders");
    const auto has = [&](std::vector<rs::GrantSpec> gs) {
        return rg::PermissionTable::compile(gs, idx).hasAnyGrant(ResourceType::Topic, Operation::Write);
    };
    EXPECT_TRUE(has({g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "orders")}));
    EXPECT_TRUE(has({g(Operation::Write, ResourceType::Topic, PatternKind::Exact, "not-created-yet")}));
    EXPECT_TRUE(has({g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "ord")}));
    EXPECT_TRUE(has({g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "")})) << "wildcard";
    EXPECT_FALSE(has({}));
    EXPECT_FALSE(has({g(Operation::Read, ResourceType::Topic, PatternKind::Prefix, "")})) << "READ is not WRITE";
    EXPECT_FALSE(has({g(Operation::Write, ResourceType::TransactionalId, PatternKind::Prefix, ""),
                      g(Operation::Read, ResourceType::Group, PatternKind::Prefix, ""),
                      g(Operation::Create, ResourceType::Cluster, PatternKind::Exact, "")}));
    EXPECT_TRUE(has({g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")})) << "ADMIN ON CLUSTER is root";
    EXPECT_FALSE(has({g(Operation::Write, ResourceType::View, PatternKind::Exact, "orders")}));
    EXPECT_TRUE(rg::PermissionTable::allowAll().hasAnyGrant(ResourceType::Topic, Operation::Write));
    EXPECT_FALSE(rg::PermissionTable::denyAll().hasAnyGrant(ResourceType::Topic, Operation::Write));

    const auto none = rg::PermissionTable::compile(std::vector<rs::GrantSpec>{}, idx);
    const auto d = rg::authorizeAnyGrant(none, ResourceType::Topic, Operation::Write);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->code, 45);
    EXPECT_EQ(d->detail, "not authorized: WRITE on any TOPIC");
    const auto w = rg::PermissionTable::compile(std::vector<rs::GrantSpec>{g(Operation::Write, ResourceType::Topic, PatternKind::Prefix, "x")}, idx);
    EXPECT_FALSE(rg::authorizeAnyGrant(w, ResourceType::Topic, Operation::Write).has_value());
}

// Namespaces spec §2 (owner ruling): ADMIN ON CLUSTER is root.  The compiled table gives every op on every user
// object (existing, created after login, by name), every cluster op and every any-grant - and still nothing beyond
// CREATE/ALTER/DROP/DESCRIBE on an internal (`__`) object (I6).  The bootstrap admin holds only this grant.
TEST(PermissionTableRegression, AdminOnClusterIsRootRegression) {
    rg::ObjectIndex idx; idx.onTopic(0, "orders"); idx.onView(1, "board"); idx.onTopic(2, "__catalog");
    auto t = rg::PermissionTable::compile({{g(Operation::Admin, ResourceType::Cluster, PatternKind::Exact, "")}}, idx);
    constexpr uint8_t kAll = 0xFF;
    EXPECT_TRUE(t.allows(0, kAll)) << "READ / WRITE / TRUNCATE / ... on a user topic";
    EXPECT_TRUE(t.allows(1, kAll)) << "every op on a view";
    EXPECT_NE(t.writeBits()[0] & (uint64_t{1} << 0), 0u) << "the produce path's write bit";
    EXPECT_TRUE(t.allowsCluster(rs::opBit(Operation::Create) | rs::opBit(Operation::Describe) | rs::opBit(Operation::Admin)));
    for (ResourceType ty : {ResourceType::Topic, ResourceType::View, ResourceType::Group, ResourceType::TransactionalId}) {
        EXPECT_TRUE(t.allowsName(ty, "anything", kAll)) << static_cast<int>(ty);
        for (Operation op : {Operation::Read, Operation::Write, Operation::Create, Operation::Describe})
            EXPECT_TRUE(t.hasAnyGrant(ty, op)) << static_cast<int>(ty) << "/" << static_cast<int>(op);
    }
    EXPECT_FALSE(t.hasAnyGrant(ResourceType::Namespace, Operation::Admin)) << "a NAMESPACE grant is never implied";
    EXPECT_FALSE(t.allows(2, rs::opBit(Operation::Read))) << "no data on an internal topic (I6)";
    EXPECT_FALSE(t.allowsName(ResourceType::Topic, "__security", rs::opBit(Operation::Write)));
    EXPECT_TRUE(t.allows(2, rs::opBit(Operation::Describe)));
    t.onObjectCreated(7, {"later", ResourceType::Topic});
    EXPECT_TRUE(t.allows(7, kAll)) << "root covers an object created after login";
    EXPECT_NE(t.writeBits()[0] & (uint64_t{1} << 7), 0u);
}
