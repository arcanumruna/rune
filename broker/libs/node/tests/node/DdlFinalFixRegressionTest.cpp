// DdlFinalFixRegressionTest.cpp — DDL conformance final fix wave (final review
// C1 / I1 / I2 / I5(b) / M2 / M3), driven through DdlHarness.
//
//   I1  an orphan view sink that holds bytes is never adopted (and a committed one
//       never matches): adoption implies an unclaimed, unwritten, uncommitted sink.
//   I2  a CreateView in doubt (KL2: timed out / demoted after its record was
//       appended) followed by a DefineTopic of the same name is refused 25 — the two
//       catalogs never both claim one id, and the broker restarts.
//   I5b the in-doubt matrix: {Define, View} x {timeout, demotion} in AwaitingCommit,
//       then every same-name op, then a restart.
//   M2  while the catalog rebuild of the last promotion failed, new DDL is refused.
//   M3  the AwaitingCommit states hold no create ticket and one copy of the plan.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"
#include <rune/protocol/RecordBatch.h>
#include <rune/node/BrokerNode.h>   // AdminWorkerAgent

#include <algorithm>

#include <ostream>
#include <string>
#include <tuple>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;

namespace {

void create(DdlHarness& h, Op op, const std::string& name, const std::string& src = "src") {
    const int64_t c = h.submit(op, name, src);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kOk) << "setup " << opName(op) << " '" << name << "'";
}

/// A create whose control thread made the topic but whose reply failed: an orphan.
void makeOrphan(DdlHarness& h, Op op, const std::string& name) {
    h.setControl(ControlMode::Hold);
    const int64_t c = h.submit(op, name);
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.failCreate(/*afterCreate=*/true);
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_EQ(h.response(c)->err, kInternal);
    ASSERT_GE(h.idOf(name), 0) << "the orphan topic exists";
}

enum class InDoubtFault : uint8_t { Timeout, Demotion };
const char* faultLabel(InDoubtFault f) { return f == InDoubtFault::Timeout ? "Timeout" : "Demotion"; }

/// KL2: `op` appends its catalog record, then times out / is demoted in
/// AwaitingCommit (answered Internal, never applied live); the record then
/// commits.  Needs holdCommits (the HW waits for follower 2).
void makeInDoubt(DdlHarness& h, Op op, const std::string& name, InDoubtFault fault) {
    const int64_t c = h.submit(op, name);
    ASSERT_TRUE(h.tickUntilAppended());
    ASSERT_EQ(h.worker().inFlightStateName(), isView(op) ? "ViewAwaitingCommit" : "DefineAwaitingCommit");
    if (fault == InDoubtFault::Timeout) {
        h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
        h.tick();
    } else {
        h.demote();
        for (int i = 0; i < 5; ++i) h.tick();   // production ticks while demoted
        h.promote();
    }
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kInternal) << "in doubt: answered Internal";
    h.releaseCommits();   // the appended record commits after its op was answered
    h.tick();
    ASSERT_FALSE(h.commitHeld());
    ASSERT_GE(h.idOf(name), 0);
    if (isView(op)) ASSERT_EQ(h.views().byName(name), nullptr) << "never applied live";
    else            ASSERT_EQ(h.schemas().byName(name), nullptr) << "never applied live";
}

} // namespace

// ── I1 ───────────────────────────────────────────────────────────────────────
// Adoption (Task 14) claims only an orphan sink nothing was ever written to.  A sink
// holding bytes (only a raw client write could put them there) is refused 25: the
// bytes would otherwise be read back as the view's output.
TEST(DdlFixRegression, I1_WrittenOrphanSinkIsNeverAdoptedRegression) {
    DdlHarness h;
    makeOrphan(h, Op::ViewPersisted, "w");
    const int32_t sinkId = h.idOf("w");
    rune::protocol::RecordBatchBuilder b(0, 0);
    b.append(std::string_view{}, std::string_view{"foreign"}, 0);
    h.topics().log(sinkId, 0)->append(b.build(), 0);   // bytes in the orphan sink
    const Catalog before = h.catalog();
    const int64_t c = h.submit(Op::ViewPersisted, "w");
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    EXPECT_EQ(h.response(c)->err, kTopicExists) << "a written sink is never adopted";
    EXPECT_EQ(h.views().byName("w"), nullptr);
    EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// I1: the control thread's sinkSpecMatches requires an UNCOMMITTED topic — a sink
// whose record committed (its registries lost, e.g. a failed rebuild) never matches.
TEST(DdlFixRegression, I1_CommittedSinkNeverMatchesForAdoptionRegression) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("rune_i1_committed_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    {
        rune::gateway::TopicRegistry reg({}, root.string());
        CreateTopicReqMsg req;
        req.ticket = 1; req.partitionCount = 1; req.name = "sink"; req.keyField = "id";
        req.cleanupPolicy = static_cast<uint8_t>(TopicSpec::CleanupPolicy::Compact); req.persistence = 0;
        const CreateTopicReplyMsg first = createTopicForAdmin(reg, req, /*firstRuntimeId=*/0);
        ASSERT_GE(first.topicId, 0);
        req.ticket = 2;
        EXPECT_TRUE(createTopicForAdmin(reg, req, 0).sinkSpecMatches) << "uncommitted + unwritten: adoptable";
        ASSERT_TRUE(reg.commitTopic(first.topicId));
        req.ticket = 3;
        EXPECT_FALSE(createTopicForAdmin(reg, req, 0).sinkSpecMatches) << "a committed sink is never adoptable";
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

// ── I2 ───────────────────────────────────────────────────────────────────────
TEST(DdlFixRegression, KL2_ViewInDoubtThenDefineSameNameStillBootsRegression) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    makeInDoubt(h, Op::ViewPersisted, "v", InDoubtFault::Timeout);
    if (HasFatalFailure()) return;
    const int64_t d = h.submit(Op::DefineDisk, "v");
    h.settle();
    ASSERT_TRUE(h.response(d).has_value());
    EXPECT_EQ(h.response(d)->err, kTopicExists) << "a runtime view sink is never adopted by DefineTopic";
    EXPECT_EQ(h.schemas().byName("v"), nullptr);
    EXPECT_NO_THROW(h.restart()) << "the catalogs never both claim one id";
    EXPECT_NE(h.views().byName("v"), nullptr) << "after the restart the in-doubt view is committed";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// I2 (KL1b closed): a DefineTopic never adopts a view's sink — committed or orphan.
TEST(DdlFixRegression, KL1b_DefineNeverAdoptsAnOrphanViewSinkRegression) {
    DdlHarness h;
    makeOrphan(h, Op::ViewPersisted, "osink");
    const int64_t d = h.submit(Op::DefineDisk, "osink");
    h.settle();
    ASSERT_TRUE(h.response(d).has_value());
    EXPECT_EQ(h.response(d)->err, kTopicExists);
    const int64_t v = h.submit(Op::ViewPersisted, "osink");
    h.settle();
    EXPECT_EQ(h.response(v)->err, kOk) << "the view retry still adopts its own sink";
    EXPECT_NO_THROW(h.restart());
    EXPECT_NE(h.views().byName("osink"), nullptr);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── I5(b): the in-doubt matrix ───────────────────────────────────────────────
struct InDoubtCase { Op first; InDoubtFault fault; Op then; };
void PrintTo(const InDoubtCase& c, std::ostream* os) {
    *os << opName(c.first) << "_" << faultLabel(c.fault) << "_" << opName(c.then);
}

class DdlInDoubtRegression : public ::testing::TestWithParam<InDoubtCase> {};

TEST_P(DdlInDoubtRegression, SameNameOpThenRestart) {
    const InDoubtCase& c = GetParam();
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    makeInDoubt(h, c.first, "x", c.fault);
    if (HasFatalFailure()) return;
    const int32_t id = h.idOf("x");
    EXPECT_TRUE(h.topics().awaitingCommit(id)) << "live, the in-doubt topic is not servable";

    // Live, an in-doubt topic is an orphan: a schema-less disk topic (define) or an
    // orphan sink (view) — the expectation table's rows.
    const Occupant live = isView(c.first) ? Occupant::OrphanSink : Occupant::SchemaLessDisk;
    const Cell want = expectedCell(live, c.then);
    const int reqs = h.createRequests();
    const int64_t corr = h.submit(c.then, "x");
    h.settle();
    ASSERT_TRUE(h.response(corr).has_value()) << "every op is answered";
    EXPECT_EQ(h.response(corr)->err, want.code) << opName(c.then) << " on an in-doubt " << opName(c.first);
    EXPECT_EQ(h.createRequests() - reqs, want.createRequests);
    if (want.code == kOk && isCreate(c.then)) {
        EXPECT_EQ(h.response(corr)->id, id) << "adoption keeps the in-doubt topic's id";
        EXPECT_FALSE(h.topics().awaitingCommit(id)) << "the adopting commit makes it servable";
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // After a restart the in-doubt record is committed: the name is the first op's
    // kind (or the adopting op's, which claimed the same id).
    ASSERT_NO_THROW(h.restart());
    const Catalog cat = h.catalog();
    const CatalogEntry* e = findEntry(cat, "x");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->id, id) << "the id survives";
    const bool viewAfter = isView(c.first) || (want.code == kOk && isView(c.then));
    EXPECT_EQ(e->kind, viewAfter ? EntryKind::View : EntryKind::Typed);
    EXPECT_EQ(e->version, 1u);
    EXPECT_FALSE(h.topics().awaitingCommit(id)) << "a recovered topic serves from boot";
    // And the name keeps colliding after the restart.
    const int64_t again = h.submit(c.first, "x");
    h.settle();
    EXPECT_EQ(h.response(again)->err, viewAfter && isView(c.first) ? kViewExists : kTopicExists);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

std::vector<InDoubtCase> inDoubtCases() {
    std::vector<InDoubtCase> v;
    for (Op first : {Op::DefineDisk, Op::ViewPersisted})
        for (InDoubtFault f : {InDoubtFault::Timeout, InDoubtFault::Demotion})
            for (Op then : {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient,
                            Op::AlterAdd, Op::GetSchema})
                v.push_back({first, f, then});
    return v;
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlInDoubtRegression, ::testing::ValuesIn(inDoubtCases()),
    [](const ::testing::TestParamInfo<InDoubtCase>& i) {
        return std::string(opName(i.param.first)) + "_" + faultLabel(i.param.fault) + "_" + opName(i.param.then);
    });

// ── M2 ───────────────────────────────────────────────────────────────────────
// A promotion whose catalog rebuild hit a corrupt record serves no DDL: the
// registries may miss records past it, so a create could adopt a committed topic.
TEST(DdlFixRegression, M2_FailedSchemaRebuildRefusesNewDdlRegression) {
    DdlHarness h;
    h.appendRawCatalogRecordForTest(/*views=*/false, {0xff, 0xff, 0xff, 0xff, 0xff});
    for (int i = 0; i < 3; ++i) h.tick();
    h.demote();
    h.promote();
    ASSERT_TRUE(h.worker().rebuildFailed());
    const int reqs = h.createRequests();
    const int64_t d = h.submit(Op::DefineDisk, "n");
    const int64_t v = h.submit(Op::ViewPersisted, "vn");
    const int64_t a = h.submit(Op::AlterAdd, "src");
    h.settle();
    for (int64_t c : {d, v, a}) {
        ASSERT_TRUE(h.response(c).has_value());
        EXPECT_EQ(h.response(c)->err, kInternal) << "refused while the rebuild is failed";
    }
    EXPECT_EQ(h.createRequests(), reqs) << "nothing requested";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, M2_FailedViewsRebuildRefusesNewDdlRegression) {
    DdlHarness h;
    h.appendRawCatalogRecordForTest(/*views=*/true, {0xff, 0xff, 0xff, 0xff, 0xff});
    for (int i = 0; i < 3; ++i) h.tick();
    h.demote();
    h.promote();
    const int reqs = h.createRequests();
    const int64_t d = h.submit(Op::DefineDisk, "n");
    const int64_t v = h.submit(Op::ViewPersisted, "vn");
    h.settle();
    for (int64_t c : {d, v}) {
        ASSERT_TRUE(h.response(c).has_value());
        EXPECT_EQ(h.response(c)->err, kInternal) << "refused while the views rebuild is failed";
    }
    EXPECT_EQ(h.createRequests(), reqs);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── M3 (hard rule 3): the commit states hold exactly what the commit needs ───
template <typename S> concept HasTicket       = requires(const S& s) { s.ticket; };
template <typename S> concept HasCompiledPlan = requires(const S& s) { s.compiledPlan; };
static_assert(!HasTicket<AdminWorker::DefineAwaitingCommit>, "M3: the commit state needs no create ticket");
static_assert(!HasTicket<AdminWorker::ViewAwaitingCommit>, "M3: the commit state needs no create ticket");
static_assert(!HasCompiledPlan<AdminWorker::ViewAwaitingCommit>,
              "M3: the plan lives once, in viewRecord.compiledPlanBlob");
static_assert(HasTicket<AdminWorker::DefineAwaitingId> && HasTicket<AdminWorker::ViewAwaitingId>,
              "the AwaitingId states match the reply by ticket");

TEST(DdlFixRegression, M3_CommitStatesHoldOnlyWhatTheCommitNeedsRegression) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    ASSERT_TRUE(h.tickUntilAppended());
    h.releaseCommits();
    h.settle();
    ASSERT_EQ(h.response(v)->err, kOk);
    EXPECT_EQ(h.viewPlanPushes(), 1) << "the plan fanned out from the record's blob";
    ASSERT_NO_THROW(h.restart());
    EXPECT_EQ(h.recoveryViewInstalls(), 1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── C1: a view plan never overtakes its sink's commit on the admin→control ring ──
// The control thread builds a committed sink's replicas on kA2cTopicCommitted; the
// view plan that follows must reach it AFTER that commit even when the ring was
// full and the commit sits in the admin-owned backlog (two backlogs, one ring).
TEST(DdlFixRegression, C1_ViewPlanNeverOvertakesItsSinkCommitOnTheFailureRingRegression) {
    SchemaRegistry reg;
    AdminWorkerAgent agent(reg, /*replica=*/nullptr, /*selfId=*/0, {0}, AdminDeps{});
    int posted = 0;
    while (agent.topicCommittedBacklogForTest() == 0 && posted < 10'000'000) agent.postTopicCommitted(posted++);
    ASSERT_GT(agent.topicCommittedBacklogForTest(), 0u) << "precondition: the ring filled, a commit is backlogged";
    std::vector<int32_t> order;   // frame types in ring order
    auto drainAll = [&] {
        for (int n = 1; n > 0;)
            n = agent.failureRing().drain([&](int32_t tid, aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                                              aeron::util::index_t) { order.push_back(tid); }, 1 << 20);
    };
    drainAll();   // the control thread catches up; the backlogged commit is still admin-side
    order.clear();
    ViewPlanUpdateMsg vp;
    vp.viewTopicId = 7;
    vp.planBlob = {1, 2, 3};
    agent.postViewPlan(vp);
    for (int i = 0; i < 4; ++i) { (void)agent.doWork(0); drainAll(); }
    ASSERT_FALSE(order.empty());
    EXPECT_EQ(order.back(), kA2cViewPlanUpdate) << "the plan arrives last";
    EXPECT_EQ(std::count(order.begin(), order.end(), kA2cViewPlanUpdate), 1);
    EXPECT_EQ(order.front(), kA2cTopicCommitted) << "the backlogged commit precedes the plan";
    EXPECT_EQ(agent.topicCommittedBacklogForTest(), 0u);
}

// C1 x KL2: an in-doubt record that a later PROMOTION replays (its HW now covers
// it) makes the topic a live catalog entry — it must serve from then on too, not
// only after a restart.  The promotion's replay posts kA2cTopicCommitted for every
// replayed topic (a no-op for one already servable).
TEST(DdlFixRegression, C1_InDoubtRecordReplayedByAPromotionServesRegression) {
    for (const Op op : {Op::DefineDisk, Op::ViewPersisted}) {
        SCOPED_TRACE(opName(op));
        DdlHarnessOptions o;
        o.holdCommits = true;
        DdlHarness h(o);
        makeInDoubt(h, op, "x", InDoubtFault::Timeout);
        if (HasFatalFailure()) return;
        const int32_t id = h.idOf("x");
        ASSERT_TRUE(h.topics().awaitingCommit(id)) << "live: in doubt, not servable";
        h.demote();
        for (int i = 0; i < 3; ++i) h.tick();
        h.promote();   // replays the (now committed) record
        h.tick();
        if (isView(op)) EXPECT_NE(h.views().byName("x"), nullptr) << "the replay applied the view";
        else            EXPECT_NE(h.schemas().byName("x"), nullptr) << "the replay applied the schema";
        EXPECT_FALSE(h.topics().awaitingCommit(id)) << "and the topic serves";
        EXPECT_TRUE(h.violations().empty()) << h.violationText();
    }
}
