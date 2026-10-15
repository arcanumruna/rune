// DdlLeadershipRegressionTest.cpp — DDL conformance plan: the demotion / commit-timeout / ISR (B4, KL2), schema
// (B8), view-source (B9) and in-flight-state regressions on the __catalog write path
// (catalog history Task 10; split out of DdlHarnessRegressionTest.cpp in fix
// round 1, review M8).  Every suite name contains "Regression".
#include <gtest/gtest.h>
#include "support/DdlHarness.h"
#include "support/DdlHarnessTestHelpers.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/TopicName.h>
#include <rune/codec/AlterOps.h>
#include <rune/gateway/TopicRegistry.h>
#include <rune/protocol/RecordBatch.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
namespace cat = rune::catalog;

// ── B4 / KL2 / re-promotion ──────────────────────────────────────────────────
// Was B4_DemotionAnswersAnInFlightCreateImmediately (AwaitingId): the in-flight
// define is in AwaitingCommit.  Spec §5.2: answered the retriable NotLeader at
// once; its event commits under the re-promoted leader, and the client's retry
// (same requestId) is answered the ORIGINAL result (was: adopts the orphan).
TEST(DdlFixRegression, B4_DemotionAnswersAnInFlightCreateImmediatelyAndTheRetryDedupes) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    DefineTopicMsg m;
    m.name = "n"; m.partitionCount = 1; m.keyField = "id"; m.blob = charBlob();
    m.meta = h.rig().meta("CREATE TOPIC n");
    const DefineTopicMsg retry = m;
    const int64_t a = h.submitRawDefine(std::move(m));
    ASSERT_TRUE(h.tickUntilAppended());
    h.demote();
    ASSERT_TRUE(h.response(a).has_value()) << "answered on demotion, not after the 30 s timeout";
    EXPECT_EQ(h.response(a)->err, kNotLeader);
    EXPECT_FALSE(h.worker().hasInFlight());
    EXPECT_EQ(h.schemas().byName("n"), nullptr) << "not applied off a follower";
    h.promote();
    h.settle();
    const int64_t events = static_cast<int64_t>(h.eventsAppended());
    const int64_t b = h.submitRawDefine(retry);
    h.settle();
    EXPECT_EQ(h.response(b)->err, kOk) << "the retry gets the original result";
    EXPECT_EQ(h.response(b)->id, h.idOf("n"));
    EXPECT_EQ(static_cast<int64_t>(h.eventsAppended()), events) << "deduped: nothing appended twice";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// A follower never commits an op off its own HW; once re-promoted the event
// commits under the new epoch and applies (spec §5.2, F13 — it was answered the
// retriable NotLeader, never applied twice).
TEST(DdlFixRegression, B4_DemotionAnswersAnInFlightCommitAndAFollowerNeverAppliesIt) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    h.demote();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kNotLeader);
    for (int i = 0; i < 10; ++i) h.tick();
    EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "a follower never commits an op off its own HW";
    h.promote();
    h.settle();
    EXPECT_EQ(h.schemas().byName("src")->version, 2u) << "committed under the re-promoted leader, applied once";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B4_RepromotionKeepsTheCatalogExact) {
    DdlHarness h;
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    h.settle();
    ASSERT_EQ(h.response(v)->err, kOk);
    const Catalog before = h.catalog();
    const std::size_t views = h.views().all().size();
    const int installs = h.viewPlanPushes() + h.recoveryViewInstalls();
    h.demote();
    h.promote();
    h.settle();
    EXPECT_EQ(h.catalog(), before) << "a promotion rebuilds nothing: no duplicates, no loss";
    EXPECT_EQ(h.views().all().size(), views);
    EXPECT_EQ(h.viewPlanPushes() + h.recoveryViewInstalls(), installs) << "no view re-installed";
    const int64_t d = h.submit(Op::DefineDisk, "after");
    h.settle();
    EXPECT_EQ(h.response(d)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// KL2 resolved by F13: an op whose commit timed out is answered the retriable
// NotLeader (was Internal, in doubt); the leader catches up (NotLeader to new
// DDL) until its event is applied, and the committed event is applied LIVE (was:
// only on restart).  A restart keeps it.
TEST(DdlFixRegression, KL2_TimeoutInAwaitingCommitIsNotLeaderAndAppliedOnceCommitted) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
    h.tick();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kNotLeader);
    EXPECT_EQ(h.worker().roleName(), "LeaderCatchingUp");
    const int64_t b = h.submit(Op::DefineDisk, "behind");
    h.tick();
    ASSERT_TRUE(h.response(b).has_value());
    EXPECT_EQ(h.response(b)->err, kNotLeader) << "no event is built on a state missing the appended one";
    EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "not applied before it commits";
    h.releaseCommits();
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_EQ(h.schemas().byName("src")->version, 2u) << "the committed event is applied live";
    EXPECT_TRUE(h.worker().isLeader()) << "caught up: serving again";
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->version, 2u) << "and replayed on restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Review M6 (parity with B4's dedupe): the client RETRIES the timed-out ALTER with
// its requestId — NotLeader while the leader catches up on the held event, then
// the ORIGINAL result (schema v2) once it is applied; nothing is appended twice.
TEST(DdlFixRegression, KL2_RetryAfterATimeoutIsNotLeaderWhileCatchingUpThenTheOriginalResult) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    AlterTopicMsg m;
    m.topicName = "src";
    m.opsBlob = alterOpsFor(Op::AlterAdd, 77);
    m.meta = h.rig().meta("ALTER TOPIC src ADD extra77 INT64");
    const AlterTopicMsg retry = m;
    const int64_t a = h.submitRawAlter(std::move(m));
    ASSERT_TRUE(h.tickUntilAppended());
    h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
    h.tick();
    ASSERT_TRUE(h.response(a).has_value());
    ASSERT_EQ(h.response(a)->err, kNotLeader);
    const uint64_t events = h.eventsAppended();

    const int64_t early = h.submitRawAlter(retry);
    h.tick();
    ASSERT_TRUE(h.response(early).has_value());
    EXPECT_EQ(h.response(early)->err, kNotLeader) << "the retry while the leader catches up";

    h.releaseCommits();
    for (int i = 0; i < 5; ++i) h.tick();
    ASSERT_TRUE(h.worker().isLeader());
    const int64_t late = h.submitRawAlter(retry);
    h.settle();
    ASSERT_TRUE(h.response(late).has_value());
    EXPECT_EQ(h.response(late)->err, kOk) << "the retry gets the original result";
    EXPECT_EQ(h.response(late)->version, 2u) << "the original ALTER's schema version";
    EXPECT_EQ(h.response(late)->id, h.idOf("src"));
    EXPECT_EQ(h.eventsAppended(), events) << "deduped: the ALTER was applied once";
    EXPECT_EQ(h.schemas().byName("src")->version, 2u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Review M7: an ISR SHRINK at the same leader epoch (follower 2 leaves the ISR)
// while an op awaits its commit is not a leadership change: the op stays in
// flight, the role keeps serving, and new appends are refused below minIsr.
TEST(DdlFixRegression, IsrShrinkAtTheSameEpochWhileAwaitingCommitIsNotALeadershipChange) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    const int32_t epoch = static_cast<int32_t>(h.rig().replica().epoch());
    h.setIsrInPlace({1});
    for (int i = 0; i < 3; ++i) h.tick();
    EXPECT_EQ(static_cast<int32_t>(h.rig().replica().epoch()), epoch) << "no new epoch";
    EXPECT_EQ(h.worker().roleName(), "LeaderServing") << "the role keeps serving";
    EXPECT_TRUE(h.worker().hasInFlight()) << "the op stays in flight";
    EXPECT_FALSE(h.response(a).has_value()) << "never acked by the leader alone (acks=all, minIsr = a majority)";
    EXPECT_TRUE(h.commitHeld()) << "the HW did not advance past the event";
    EXPECT_EQ(h.schemas().byName("src")->version, 1u);
    // Follower 2 rejoins at the same epoch and fetches: the event commits once.
    h.setIsrInPlace({1, DdlHarness::kFollower});
    h.releaseCommits();
    for (int i = 0; i < 3 && !h.response(a).has_value(); ++i) h.tick();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(a)->version, 2u);
    // Shrunk again at the same epoch, a NEW append is refused below minIsr.
    h.setIsrInPlace({1});
    const uint64_t events = h.eventsAppended();
    const int64_t b = h.submit(Op::DefineDisk, "below");
    for (int i = 0; i < 3; ++i) h.tick();
    ASSERT_TRUE(h.response(b).has_value());
    EXPECT_EQ(h.response(b)->err, kNotLeader) << "an append below minIsr is refused";
    EXPECT_EQ(h.eventsAppended(), events);
    EXPECT_EQ(static_cast<int32_t>(h.rig().replica().epoch()), epoch) << "still the same epoch";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── B4 matrix: demotion while an op awaits its commit ─────────────────────────
// The AwaitingId states (DefineAwaitingId / ViewAwaitingId) are gone with the
// create round-trip (Task 9); every DDL kind now has exactly one async state,
// AwaitingCommit — pinned for Define, View, Alter and two of the new kinds.  For
// each: an immediate NotLeader; nothing applied off a follower; a demoted worker
// starts nothing (every queued op is refused NotLeader at the role gate: no
// append); after re-promotion the event commits and applies once (spec §5.2), and
// the leader serves.  A restart keeps it.
enum class B4State : uint8_t {
    DefineAwaitingCommit, ViewAwaitingCommit, AlterAwaitingCommit, DropTopicAwaitingCommit, AlterViewAwaitingCommit,
};

[[nodiscard]] static const char* b4StateName(B4State s) noexcept {
    switch (s) {
        case B4State::DefineAwaitingCommit:    return "DefineAwaitingCommit";
        case B4State::ViewAwaitingCommit:      return "ViewAwaitingCommit";
        case B4State::AlterAwaitingCommit:     return "AlterAwaitingCommit";
        case B4State::DropTopicAwaitingCommit: return "DropTopicAwaitingCommit";
        case B4State::AlterViewAwaitingCommit: return "AlterViewAwaitingCommit";
    }
    return "?";
}
static void PrintTo(B4State s, std::ostream* os) { *os << b4StateName(s); }

class DdlDemotionRegression : public ::testing::TestWithParam<B4State> {
protected:
    static DdlHarnessOptions options() {
        DdlHarnessOptions o;
        o.holdCommits = true;
        return o;
    }
    /// The setup the state needs (a view to alter, a topic to drop), committed.
    static void prepare(DdlHarness& h, B4State s) {
        if (s == B4State::DropTopicAwaitingCommit) createOk(h, Op::DefineDisk, "n");
        if (s == B4State::AlterViewAwaitingCommit) createOk(h, Op::ViewPersisted, "v");
    }
    /// Submit the op and drive it into AwaitingCommit; returns its corr.
    static int64_t driveInto(DdlHarness& h, B4State s) {
        int64_t c = -1;
        switch (s) {
            case B4State::DefineAwaitingCommit:    c = h.submit(Op::DefineDisk, "n"); break;
            case B4State::ViewAwaitingCommit:      c = h.submit(Op::ViewPersisted, "v"); break;
            case B4State::AlterAwaitingCommit:     c = h.submit(Op::AlterAdd, "src"); break;
            case B4State::DropTopicAwaitingCommit: c = h.submit(Op::DropTopic, "n"); break;
            case B4State::AlterViewAwaitingCommit: c = h.submit(Op::AlterView, "v"); break;
        }
        EXPECT_TRUE(h.tickUntilAppended());
        EXPECT_TRUE(h.worker().hasInFlight());
        EXPECT_FALSE(h.response(c).has_value());
        return c;
    }
    /// Did the in-flight op's effect reach the applied catalog?
    static bool applied(DdlHarness& h, B4State s, int32_t viewSinkBefore) {
        switch (s) {
            case B4State::DefineAwaitingCommit:    return h.schemas().byName("n") != nullptr;
            case B4State::ViewAwaitingCommit:      return h.views().byName("v") != nullptr;
            case B4State::AlterAwaitingCommit:     return h.schemas().byName("src")->version != 1u;
            case B4State::DropTopicAwaitingCommit: return h.idOf("n") == -1;
            case B4State::AlterViewAwaitingCommit: return h.idOf("v") != viewSinkBefore;
        }
        return true;
    }
};

TEST_P(DdlDemotionRegression, DemotionAnswersNotLeaderAtOnceStartsNothingAndAppliesOnlyOnceCommitted) {
    const B4State s = GetParam();
    DdlHarness h(options());
    prepare(h, s);
    if (HasFatalFailure()) return;
    const int32_t sink0 = h.idOf("v");
    const int64_t x = driveInto(h, s);
    const int64_t y = h.submit(Op::AlterAdd, "src2");       // queued behind x
    const int64_t w = h.submit(Op::DefineDisk, "queued");   // queued behind y

    h.demote();
    ASSERT_TRUE(h.response(x).has_value()) << "answered inside onCatalogRole(follower), not on a tick or the timeout";
    EXPECT_EQ(h.response(x)->err, kNotLeader);
    EXPECT_FALSE(h.worker().hasInFlight());
    EXPECT_FALSE(h.response(y).has_value()) << "demotion answers only the in-flight op";

    // A demoted worker starts nothing: every queued op is refused at the role gate
    // (FIFO, checked by the harness), no append.
    const uint64_t events = h.eventsAppended();
    const int64_t logEnd = h.catalogLogEnd();
    for (int i = 0; i < 10; ++i) h.tick();
    ASSERT_TRUE(h.response(y).has_value());
    ASSERT_TRUE(h.response(w).has_value());
    EXPECT_EQ(h.response(y)->err, kNotLeader);
    EXPECT_EQ(h.response(w)->err, kNotLeader);
    EXPECT_EQ(h.eventsAppended(), events) << "no append while demoted";
    EXPECT_EQ(h.catalogLogEnd(), logEnd);
    EXPECT_EQ(h.schemas().byName("src2")->version, 1u);
    EXPECT_EQ(h.schemas().byName("queued"), nullptr);
    EXPECT_FALSE(applied(h, s, sink0)) << "never applied off a follower";

    h.promote();
    h.settle();   // the HW now covers x's event under the new epoch
    EXPECT_TRUE(applied(h, s, sink0)) << "spec §5.2: the event answered NotLeader commits and applies once";

    const int64_t z = h.submit(Op::DefineDisk, "after");
    h.settle();
    ASSERT_TRUE(h.response(z).has_value());
    EXPECT_EQ(h.response(z)->err, kOk) << "the re-promoted leader serves DDL";

    const Catalog before = h.catalog();
    h.restart();
    EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(applied(h, s, sink0));
    EXPECT_NE(h.schemas().byName("after"), nullptr);
    EXPECT_EQ(h.schemas().byName("queued"), nullptr);
    EXPECT_EQ(h.schemas().byName("src2")->version, 1u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Final review I4 (production truth): the admin thread keeps ticking while demoted,
// so the role gate answers every QUEUED op NotLeader on the ticks after the
// demotion — in FIFO order, nothing started.  The re-promoted leader then serves
// a fresh request.
TEST_P(DdlDemotionRegression, DemotionAnswersEveryQueuedOpNotLeaderInFifoOrder) {
    const B4State s = GetParam();
    DdlHarness h(options());
    prepare(h, s);
    if (HasFatalFailure()) return;
    const int64_t x = driveInto(h, s);
    const int64_t y = h.submit(Op::DefineDisk, "q1");
    const int64_t w = h.submit(Op::AlterAdd, "src2");
    const int64_t u = h.submit(Op::ViewTransient, "q3");
    const uint64_t events = h.eventsAppended();

    h.demote();
    ASSERT_TRUE(h.response(x).has_value());
    EXPECT_EQ(h.response(x)->err, kNotLeader);
    for (int i = 0; i < 10; ++i) h.tick();   // the admin thread ticks while demoted
    EXPECT_EQ(h.worker().pendingCount(), 0u) << "the queue drains while demoted";
    for (const int64_t c : {y, w, u}) {
        ASSERT_TRUE(h.response(c).has_value()) << "every queued op is answered while demoted";
        EXPECT_EQ(h.response(c)->err, kNotLeader) << "retry on the new leader";
    }
    std::vector<int64_t> order;
    for (const Response& r : h.responses())
        if (r.corr == x || r.corr == y || r.corr == w || r.corr == u) order.push_back(r.corr);
    EXPECT_EQ(order, (std::vector<int64_t>{x, y, w, u})) << "answered in FIFO order";
    EXPECT_EQ(h.eventsAppended(), events) << "a demoted leader appends nothing";

    h.promote();
    h.settle();
    EXPECT_EQ(h.schemas().byName("q1"), nullptr);
    EXPECT_EQ(h.schemas().byName("src2")->version, 1u);
    EXPECT_EQ(h.views().byName("q3"), nullptr);
    const int64_t z = h.submit(Op::DefineDisk, "q1");
    h.settle();
    ASSERT_TRUE(h.response(z).has_value());
    EXPECT_EQ(h.response(z)->err, kOk) << "the re-promoted leader serves the retry";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(EveryAsyncState, DdlDemotionRegression,
                         ::testing::Values(B4State::DefineAwaitingCommit, B4State::ViewAwaitingCommit,
                                           B4State::AlterAwaitingCommit, B4State::DropTopicAwaitingCommit,
                                           B4State::AlterViewAwaitingCommit),
                         [](const ::testing::TestParamInfo<B4State>& i) { return std::string(b4StateName(i.param)); });

// ── B8 / user decision D3a: renaming the key field moves the schema's keyField ───
TEST(DdlFixRegression, B8_RenamingTheKeyFieldMovesKeyFieldAndSurvivesRestart) {
    DdlHarness h;
    ASSERT_EQ(h.schemas().byName("src")->keyField, "id");
    const int64_t a = h.submit(Op::AlterRenameKey, "src");   // RENAME id -> pk
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "pk") << "live";
    EXPECT_EQ(h.schemas().byName("src")->version, 2u);
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->keyField, "pk") << "replayed from the durable event";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B8_ChainedRenamesEndAtTheLastName) {
    DdlHarness h;
    rune::codec::AlterOp r1{}; r1.type = rune::codec::AlterOpType::Rename; r1.name = "id"; r1.newName = "a";
    rune::codec::AlterOp r2{}; r2.type = rune::codec::AlterOpType::Rename; r2.name = "a";  r2.newName = "b";
    AlterTopicMsg m; m.topicName = "src"; m.opsBlob = rune::codec::encodeAlterOps({r1, r2});
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    ASSERT_EQ(h.response(c)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "b");
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->keyField, "b") << "replayed from the durable event";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B8_RenamingANonKeyFieldKeepsKeyField) {
    DdlHarness h;
    const int64_t c = h.submit(Op::AlterRename, "src");   // RENAME hp -> health
    h.settle();
    ASSERT_EQ(h.response(c)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "id");
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B8_ARejectedAlterNeverMovesKeyField) {
    DdlHarness h;
    rune::codec::AlterOp r{}; r.type = rune::codec::AlterOpType::Rename; r.name = "id"; r.newName = "pk";
    rune::codec::AlterOp d{}; d.type = rune::codec::AlterOpType::Drop; d.name = "nosuch";
    AlterTopicMsg m; m.topicName = "src"; m.opsBlob = rune::codec::encodeAlterOps({r, d});
    const int64_t logEnd = h.catalogLogEnd();
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kInvalidSchema) << "the ALTER is atomic";
    EXPECT_EQ(h.schemas().byName("src")->keyField, "id");
    EXPECT_EQ(h.schemas().byName("src")->version, 1u);
    EXPECT_EQ(h.catalogLogEnd(), logEnd) << "nothing appended";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B8_RenamingTheKeyBackRestoresIt) {
    DdlHarness h;
    const int64_t a = h.submit(Op::AlterRenameKey, "src");   // id -> pk
    h.settle();
    rune::codec::AlterOp r{}; r.type = rune::codec::AlterOpType::Rename; r.name = "pk"; r.newName = "id";
    AlterTopicMsg m; m.topicName = "src"; m.opsBlob = rune::codec::encodeAlterOps({r});
    const int64_t b = h.submitRawAlter(std::move(m));
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    ASSERT_EQ(h.response(b)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "id");
    EXPECT_EQ(h.schemas().byName("src")->version, 3u);
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->keyField, "id");
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── B9 / user decision D3b: a view over a view's output is rejected (it would stay
//    silently empty — a view sink carries no change events until Phase 3) ──────────
TEST(DdlFixRegression, B9_ViewOverAViewIsViewSourceIsViewAndCreatesNothing) {
    DdlHarness h;
    for (const Op first : {Op::ViewPersisted, Op::ViewTransient}) {
        const std::string inner = first == Op::ViewPersisted ? "inner_p" : "inner_t";
        const int64_t c0 = h.submit(first, inner);
        h.settle();
        ASSERT_EQ(h.response(c0)->err, kOk);
        for (const Op outer : {Op::ViewPersisted, Op::ViewTransient}) {
            SCOPED_TRACE(std::string(opName(first)) + " then " + opName(outer));
            const Catalog before = h.catalog();
            const uint64_t events = h.eventsAppended();
            const int64_t logEnd = h.catalogLogEnd();
            const int64_t c = h.submit(outer, "outer", inner);
            h.settle();
            EXPECT_EQ(h.response(c)->err, kViewSourceIsView);
            EXPECT_EQ(h.eventsAppended(), events) << "no event";
            EXPECT_EQ(h.catalogLogEnd(), logEnd);
            EXPECT_EQ(h.catalog(), before);
        }
        // ALTER VIEW onto a view's output is refused the same way.
        const int64_t av = h.submit(Op::AlterView, inner, inner);
        h.settle();
        EXPECT_EQ(h.response(av)->err, kViewSourceIsView) << "ALTER VIEW over a view";
    }
    h.restart();
    const int64_t again = h.submit(Op::ViewPersisted, "outer", "inner_p");
    h.settle();
    EXPECT_EQ(h.response(again)->err, kViewSourceIsView) << "still rejected after a restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B9_ViewOverATypedTopicIsStillAccepted) {
    DdlHarness h;
    const int64_t c = h.submit(Op::ViewPersisted, "plain", "src");
    h.settle();
    EXPECT_EQ(h.response(c)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Every source is checked, not only the first: a plan whose LATER source is a
// view's output is ViewSourceIsView, ahead of any compile / co-partition check.
TEST(DdlFixRegression, B9_AViewOutputAsALaterSourceIsViewSourceIsView) {
    DdlHarness h;
    const int64_t c0 = h.submit(Op::ViewPersisted, "inner");
    h.settle();
    ASSERT_EQ(h.response(c0)->err, kOk);
    CreateViewMsg m;
    m.viewName = "outer"; m.keyField = "id"; m.persistence = 0;
    m.planBlob = viewPlanBlob("outer", std::vector<int32_t>{h.idOf("src"), h.idOf("inner")}, 0);
    const Catalog before = h.catalog();
    const uint64_t events = h.eventsAppended();
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kViewSourceIsView);
    EXPECT_EQ(h.eventsAppended(), events);
    EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Was B4_Missing{Schema,Views}ReplicaInAwaitingCommitAnswersInternal (the two
// per-catalog commit replicas are one __catalog replica now).  The catalog
// counterparts: an AdminWorker with NO catalog answers every DDL kind Internal
// (never a silent drop), and an applier that HALTS while an op awaits its commit
// answers it Internal — and every later DDL Internal (spec §5.3: a halted broker
// accepts no catalog change).
TEST(DdlFixRegression, B4_NoCatalogReplicaAnswersEveryDdlInternal) {
    SchemaRegistry reg;
    ViewRegistry views;
    std::vector<uint16_t> codes;
    AdminDeps d;
    d.respondDefine = [&](int64_t, int64_t, int32_t, uint16_t, uint16_t e) { codes.push_back(e); };
    d.respondAlter  = [&](int64_t, int64_t, int32_t, uint16_t, uint16_t e) { codes.push_back(e); };
    d.respondCreateView = [&](int64_t, int64_t, int32_t, uint64_t, uint16_t e) { codes.push_back(e); };
    d.respondDdl = [&](int64_t, int64_t, uint16_t e, uint64_t, uint32_t, int32_t, std::string_view) { codes.push_back(e); };
    d.respondError = [&](int64_t, int64_t, uint16_t e, std::string_view) { codes.push_back(e); };
    AdminWorker w(reg, 1, std::move(d), &views, /*catalogReplica=*/nullptr);
    (void)w.bootCatalog(BootEffects{}, LiveEffects{});
    w.submit(DdlRequest{DefineTopicMsg{1, 1, 1, false, "t", "id", charBlob(), 0, 0, {}, {}}});
    w.submit(DdlRequest{AlterTopicMsg{1, 2, 0, "t", alterOpsFor(Op::AlterAdd, 1), {}}});
    w.submit(DdlRequest{CreateViewMsg{1, 3, 0, "v", "id", {}, {}}});
    w.submit(DdlRequest{DropTopicMsg{1, 4, "t", {}}});
    w.submit(DdlRequest{DropViewMsg{1, 5, "v", {}}});
    w.submit(DdlRequest{AlterViewMsg{1, 6, "v", "id", 0, 0, {}, {}}});
    w.submit(DdlRequest{AlterTopicConfigMsg{1, 7, "t", 0, configChangesFor(1), {}}});
    for (int i = 0; i < 4; ++i) (void)w.doWork(1'000'000LL * (i + 1));
    EXPECT_EQ(codes, std::vector<uint16_t>(7, kInternal)) << "answered, never silently dropped";
    EXPECT_EQ(w.startedCount(), 7u);
    EXPECT_EQ(w.answeredCount(), 7u);
}

TEST(DdlFixRegression, B4_ApplierHaltInAwaitingCommitAnswersInternalAndRefusesLaterDdl) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    const int64_t next = h.submit(Op::ViewPersisted, "v");
    h.worker().applierForTest().tamperNextFingerprintForTest();   // the event's fingerprint will not verify
    h.releaseCommits();
    for (int i = 0; i < 5; ++i) h.tick();
    ASSERT_TRUE(h.response(a).has_value()) << "answered, never silently dropped";
    EXPECT_EQ(h.response(a)->err, kInternal);
    ASSERT_TRUE(h.response(next).has_value());
    EXPECT_EQ(h.response(next)->err, kInternal) << "a halted applier accepts no catalog change";
    EXPECT_TRUE(h.worker().applier().halted());
    EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "never applied";
    EXPECT_EQ(h.views().byName("v"), nullptr);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── Task 10 fix round 1 (DDL plan): harness restart keeps the replica set ──────
TEST(DdlHarnessRegression, MultiNodeRestartKeepsTheReplicaSetAndHoldsCommitsAgain) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::DefineTransient, "t");
    h.settle();
    ASSERT_TRUE(h.response(a).has_value());
    ASSERT_EQ(h.response(a)->err, kOk);
    const Catalog before = h.catalog();

    h.restart();
    EXPECT_TRUE(h.holdingCommits()) << "replica set {1,2} survives the restart";
    EXPECT_FALSE(h.commitHeld()) << "the rebooted leader learned the committed HW";
    EXPECT_EQ(h.catalog(), before) << "the boot replay rebuilt the catalog";

    // A commit after the restart is held until follower 2 fetches.
    const int64_t b = h.submit(Op::AlterAdd, "t");
    ASSERT_TRUE(h.tickUntilAppended());
    for (int i = 0; i < 20; ++i) h.tick();
    EXPECT_TRUE(h.commitHeld());
    EXPECT_FALSE(h.response(b).has_value()) << "held on the HW gate after a restart";
    h.releaseCommits();
    h.tick();
    ASSERT_TRUE(h.response(b).has_value());
    EXPECT_EQ(h.response(b)->err, kOk);
    EXPECT_EQ(h.response(b)->version, 2u);

    // A leadership flap after the restart keeps the catalog.
    h.demote();
    h.promote();
    h.settle();
    EXPECT_EQ(h.schemas().byName("t")->version, 2u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Install accounting: one live install per committed CREATE VIEW; a restart's boot
// replay re-installs every live view (recovery installs); a leadership flap
// re-installs NOTHING (was +1 per view per promotion: every broker applies each
// event once — Task 7 — and a promotion rebuilds nothing).
TEST(DdlHarnessRegression, RecoveryViewInstallsAreCountedApartFromCommitInstalls) {
    DdlHarness h;
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    const int64_t tv = h.submit(Op::ViewTransient, "tv");
    h.settle();
    ASSERT_EQ(h.response(v)->err, kOk);
    ASSERT_EQ(h.response(tv)->err, kOk);
    EXPECT_EQ(h.viewPlanPushes(), 2) << "one commit install per committed CREATE VIEW";
    EXPECT_EQ(h.recoveryViewInstalls(), 0);

    h.restart();
    EXPECT_EQ(h.viewPlanPushes(), 2) << "a restart adds no commit install";
    EXPECT_EQ(h.recoveryViewInstalls(), 2) << "one recovery install per recovered view";

    h.demote();
    h.promote();
    h.settle();
    EXPECT_EQ(h.recoveryViewInstalls(), 2) << "a promotion re-installs nothing";
    EXPECT_EQ(h.viewPlanPushes(), 2);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── Task 10b (hard rule 3): the in-flight DDL op is ONE std::variant of explicit
//    per-state structs; Idle holds nothing, so no stale op field survives an answer.
static_assert(std::variant_size_v<AdminWorker::InFlight> == 2,
              "Idle + AwaitingCommit (the AwaitingId states left with the create round-trip)");
static_assert(std::is_same_v<std::variant_alternative_t<0, AdminWorker::InFlight>, AdminWorker::Idle>,
              "a default-constructed worker is Idle");
static_assert(std::is_empty_v<AdminWorker::Idle>, "Idle holds no op variables");


TEST(InFlightStateRegression, IdleWorkerHoldsNoOp) {
    DdlHarness h;
    expectIdle(h, "after boot");
}

TEST(InFlightStateRegression, CommittedOpsWalkTheirStatesAndEndIdle) {
    DdlHarness h(heldCommits());
    struct Step { Op op; const char* name; uint32_t version; };
    const Step steps[] = {
        {Op::DefineDisk, "d", 1}, {Op::AlterAdd, "d", 2}, {Op::ViewPersisted, "v", 1},
        {Op::AlterView, "v", 2}, {Op::AlterTopicConfig, "d", 3}, {Op::DropView, "v", 3}, {Op::DropTopic, "d", 4},
    };
    for (const Step& s : steps) {
        SCOPED_TRACE(opName(s.op));
        const int64_t c = h.submit(s.op, s.name);
        ASSERT_TRUE(h.tickUntilAppended());
        EXPECT_EQ(h.worker().inFlightStateName(), "AwaitingCommit");
        for (int i = 0; i < 3; ++i) h.tick();
        EXPECT_FALSE(h.response(c).has_value());
        ASSERT_TRUE(tickUntilAnswered(h, c, /*releaseCommits=*/true));
        EXPECT_EQ(h.response(c)->err, kOk);
        EXPECT_EQ(h.response(c)->version, s.version);
        expectIdle(h, "committed");
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(InFlightStateRegression, EveryUncommittedExitEndsIdle) {
    DdlHarness h(heldCommits());

    // AwaitingCommit -> timeout on the held HW gate (the retriable NotLeader, F13).
    const int64_t dt = h.submit(Op::DefineDisk, "dt");
    ASSERT_TRUE(h.tickUntilAppended());
    h.advance(AdminWorker::ddlTimeoutNs() + 1);
    ASSERT_TRUE(tickUntilAnswered(h, dt, /*releaseCommits=*/false));
    EXPECT_EQ(h.response(dt)->err, kNotLeader);
    expectIdle(h, "define timed out in AwaitingCommit");
    h.settle();   // the event commits; the leader catches up

    // AwaitingCommit -> demotion answers it at once.
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    h.demote();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kNotLeader);
    expectIdle(h, "alter demoted in AwaitingCommit");
    h.promote();
    expectIdle(h, "after re-promotion");
    h.settle();

    // A refused append (below minIsr) never enters AwaitingCommit.
    h.shrinkIsr();
    const int64_t r = h.submit(Op::DefineDisk, "refused");
    for (int i = 0; i < 5; ++i) h.tick();
    ASSERT_TRUE(h.response(r).has_value());
    EXPECT_EQ(h.response(r)->err, kNotLeader);
    expectIdle(h, "append refused below minIsr");
    h.restoreIsr();

    // AwaitingCommit -> the applier halts at the event.
    h.settle();
    const int64_t hx = h.submit(Op::AlterAdd, "src2");
    ASSERT_TRUE(h.tickUntilAppended());
    h.worker().applierForTest().tamperNextFingerprintForTest();
    ASSERT_TRUE(tickUntilAnswered(h, hx, /*releaseCommits=*/true));
    EXPECT_EQ(h.response(hx)->err, kInternal);
    expectIdle(h, "apply halted in AwaitingCommit");
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}
