// Catalog history spec §10 — Event kind x timing, every cell asserts the six
// invariants of the plan's "Every cell asserts" list.
#include "support/CatalogClusterHarness.h"
#include "support/CatalogMatrixExpectations.h"
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <string>
#include <vector>

namespace cm = rune::node::test::cmatrix;
using rune::node::test::CatalogClusterHarness;

TEST(CatalogMatrixCountRegression, CellCountsArePinned) {
    int total = 0, applicable = 0;
    std::array<int, 10> byOutcome{};
    for (int k = 0; k < cm::kKinds; ++k)
        for (int v = 0; v < 2; ++v)
            for (int p = 0; p < 2; ++p)
                for (int t = 0; t < cm::kTimings; ++t) {
                    ++total;
                    const auto o = cm::expect(static_cast<cm::Kind>(k), {static_cast<cm::Via>(v), static_cast<cm::Persist>(p)},
                                              static_cast<cm::Timing>(t));
                    ++byOutcome[static_cast<int>(o)];
                    if (o != cm::Outcome::NotApplicable) ++applicable;
                }
    EXPECT_EQ(total, 324);
    EXPECT_EQ(applicable, 308);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::NotApplicable)], 16);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::HaltedBrokerInternalOthersOk)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::NotLeaderUntilBudgetThenOk)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::NotLeaderThenOkOnce)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::OneWinsOneConflict)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::OkNewObjectId)], 36);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::OkAfterPlacement)], 12);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::OkDroppedBeforePlacement)], 8);   // R10: DropTopic / DropView x CreateRacesPlacement
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::RedirectThenOk)], 54);
    EXPECT_EQ(byOutcome[static_cast<int>(cm::Outcome::Ok)], 54);
}

TEST(CatalogMatrixCountRegression, ExpectedCodesAgreeWithTheOutcomeOfEveryCell) {
    // F15/F16: the code table and the outcome table cannot drift apart.
    for (int k = 0; k < cm::kKinds; ++k)
        for (int v = 0; v < 2; ++v)
            for (int p = 0; p < 2; ++p)
                for (int t = 0; t < cm::kTimings; ++t) {
                    const cm::Kind kind = static_cast<cm::Kind>(k);
                    const cm::Entity e{static_cast<cm::Via>(v), static_cast<cm::Persist>(p)};
                    const cm::Timing tm = static_cast<cm::Timing>(t);
                    const auto o = cm::expect(kind, e, tm);
                    const auto c = cm::expectedCodes(kind, e, tm);
                    if (o == cm::Outcome::NotApplicable) { EXPECT_EQ(c.first, -1); continue; }
                    if (o == cm::Outcome::HaltedBrokerInternalOthersOk) { EXPECT_EQ(c.first, 6); EXPECT_EQ(c.viaLeader, 0); continue; }
                    EXPECT_EQ(c.first == 3, e.via == cm::Via::Follower) << "a follower redirects first, a leader never";
                    EXPECT_EQ(c.afterRedirect != -1, e.via == cm::Via::Follower);
                    if (o == cm::Outcome::NotLeaderUntilBudgetThenOk) { EXPECT_EQ(c.final, 2); EXPECT_EQ(c.retried, 0); }
                    else if (o == cm::Outcome::OneWinsOneConflict) EXPECT_EQ((c.final == 0) + (c.other == 0), 1);
                    else EXPECT_EQ(c.final, 0);
                }
}

struct Cell { cm::Kind kind; cm::Entity entity; cm::Timing timing; };
std::vector<Cell> allCells() {
    std::vector<Cell> out;
    for (int k = 0; k < cm::kKinds; ++k)
        for (int v = 0; v < 2; ++v)
            for (int p = 0; p < 2; ++p)
                for (int t = 0; t < cm::kTimings; ++t)
                    out.push_back({static_cast<cm::Kind>(k), {static_cast<cm::Via>(v), static_cast<cm::Persist>(p)},
                                   static_cast<cm::Timing>(t)});
    return out;
}
std::string cellName(const testing::TestParamInfo<Cell>& i) {
    return std::string(cm::name(i.param.kind)) + "_" + (i.param.entity.via == cm::Via::Leader ? "Leader" : "Follower") +
           "_" + (i.param.entity.persist == cm::Persist::Disk ? "Disk" : "Transient") + "_" + std::string(cm::name(i.param.timing));
}

namespace {
// The matrix's template names are the rig's answer kinds (CatalogMatrixExpectations.h, M4).
rune::node::test::CatalogDdlRig::Answer::Kind kindOf(cm::Template t) {
    using K = rune::node::test::CatalogDdlRig::Answer::Kind;
    switch (t) {
    case cm::Template::Define:     return K::Define;
    case cm::Template::Alter:      return K::Alter;
    case cm::Template::CreateView: return K::CreateView;
    case cm::Template::Ddl:        return K::Ddl;
    case cm::Template::Error:      return K::Error;
    }
    return K::Error;
}
} // namespace

class CatalogInteractionMatrixRegression : public testing::TestWithParam<Cell> {};

TEST_P(CatalogInteractionMatrixRegression, Cell) {
    const Cell c = GetParam();
    const cm::Outcome want = cm::expect(c.kind, c.entity, c.timing);
    if (want == cm::Outcome::NotApplicable) GTEST_SKIP() << "R" << cm::naRule(c.kind, c.entity, c.timing);

    CatalogClusterHarness h(cellName({c, 0}), /*brokers=*/3);
    h.buildPrecondition(c.kind, c.entity.persist, c.timing);  // the "Precondition" column of the plan's ops table
    const auto run = h.runCell(c.kind, c.entity, c.timing);   // drives the timing, records every answer

    // 1. every answer code, all from cmatrix::expectedCodes (F15/F16 — no code is hard-coded here)
    const cm::ExpectedCodes ec = cm::expectedCodes(c.kind, c.entity, c.timing);
    EXPECT_EQ(run.first.code, ec.first);
    EXPECT_EQ(run.first.kind, kindOf(cm::expectedFirstTemplate(c.kind, c.entity, c.timing)))   // M4: the first answer's template
        << "the first answer's response template";
    if (ec.first == 3) EXPECT_EQ(run.first.message, h.leaderClientEndpoint());
    EXPECT_EQ(run.afterRedirectCode, ec.afterRedirect);
    EXPECT_EQ(run.finalCode, ec.final);
    EXPECT_EQ(run.otherFinalCode, ec.other);
    EXPECT_EQ(run.viaLeaderCode, ec.viaLeader);
    EXPECT_EQ(run.retriedCode, ec.retried);
    // 1b. what each outcome asserts besides codes
    switch (want) {
    case cm::Outcome::NotLeaderUntilBudgetThenOk: EXPECT_TRUE(run.produceOnExistingTopicSucceeded); break;
    case cm::Outcome::OneWinsOneConflict:         EXPECT_EQ(run.okCount, 1); break;
    case cm::Outcome::OkNewObjectId:              EXPECT_NE(run.objectIdAfter, run.objectIdBefore);
                                                  EXPECT_EQ(run.newVersionAfter, run.firstVersionOfNewObject);
                                                  EXPECT_TRUE(run.historyKeepsBothObjects) << "the history of the name keeps both objects distinct"; break;
    case cm::Outcome::OkAfterPlacement:           // The window itself is the harness's (applied everywhere, not placed, the probe
                                                  // answers NotLeader by construction); what production decides: the OK carries the
                                                  // gate topic id, so it leaves only after the placement, and the topic then serves.
                                                  EXPECT_EQ(run.produceInWindowCode, 2);
                                                  EXPECT_TRUE(run.appliedEverywhereBeforePlacement);
                                                  EXPECT_TRUE(run.okReleasedOnlyAfterServing);
                                                  if (c.kind == cm::Kind::CreateTopic) EXPECT_EQ(run.produceAfterPlacementCode, 0);
                                                  break;
    case cm::Outcome::OkDroppedBeforePlacement:   // R10: the topic is dropped between its create being applied and its placement committing
                                                  EXPECT_EQ(run.produceInWindowCode, 2) << "applied, not placed: retriable";
                                                  EXPECT_TRUE(run.createAnsweredOkBeforePlacement)
                                                      << "the create's held OK is released as a tombstone once the drop applied, not held";
                                                  EXPECT_LT(run.createAnswerMs, 1000) << "promptly (never the in-doubt cap)";
                                                  EXPECT_TRUE(run.neverPlaced) << "released placement finds no topic to place";
                                                  EXPECT_TRUE(run.unknownEverywhere) << "UnknownTopic through every live broker";
                                                  for (const auto& rid : run.extraOkRequestIds) EXPECT_EQ(h.countEventsWithRequestId(rid), 1u);
                                                  break;
    case cm::Outcome::HaltedBrokerInternalOthersOk:
        EXPECT_TRUE(h.applierHalted(3)); EXPECT_EQ(h.haltCounter(3), 1);
        EXPECT_TRUE(run.haltedBrokerStillServesAppliedTopics);
        EXPECT_TRUE(run.haltOffsetIsTheTamperedEvent) << "the halt names the offset of the event the victim refused"; break;
    default: break;
    }
    // 2. every DDL answered exactly once
    for (int b = 1; b <= 3; ++b) EXPECT_EQ(h.startedTotal(b), h.answeredTotal(b)) << "broker " << b;   // torn-down workers counted
    EXPECT_TRUE(run.everyRequestHadOneTerminalResult);
    // 3. identical catalogs on every running non-halted broker
    EXPECT_TRUE(h.awaitDigestsEqual(/*excludeHalted=*/true));
    // 4. no DDL lost or duplicated
    for (const rune::protocol::RequestId& rid : run.okRequestIds)      EXPECT_EQ(h.countEventsWithRequestId(rid), 1u);
    for (const rune::protocol::RequestId& rid : run.refusedRequestIds) EXPECT_EQ(h.countEventsWithRequestId(rid), 0u);
    // 5. history shows the op
    EXPECT_TRUE(h.historyShows(c.kind, run.objectIdAfter, run.statement, run.okRequestIds));
    // 6. serving iff applied + placed
    EXPECT_TRUE(h.servingMatchesAppliedAndPlaced());
    // 7. (I4) the persistence asked for is what every broker applied and registered ("Persist changes no outcome")
    EXPECT_TRUE(h.persistenceMatches(c.kind, c.entity.persist));
    // 8. (I4) a restarted broker holds its transient objects empty and its transient view's definition
    if (c.timing == cm::Timing::RestartMidApply && c.entity.persist == cm::Persist::Transient)
        EXPECT_TRUE(run.transientEmptyAfterRestart);
}

// ── I2: the catalog falls below its minIsr AFTER the append, BEFORE the commit ─────────────────────────────
// The leader appended the DDL; both followers die with the event only in the leader's log; the controller
// fences the ISR.  The in-flight DDL must be answered the retriable NotLeader at once (never a client Timeout),
// the leader appends nothing more, and after one broker returns the SAME requestId is answered by the ORIGINAL
// event - never a second one.
struct AfterAppendCell { cm::Kind kind; cm::Via via; };
std::vector<AfterAppendCell> afterAppendCells() {
    std::vector<AfterAppendCell> out;
    for (int k = 0; k < cm::kKinds; ++k)
        for (int v = 0; v < 2; ++v) out.push_back({static_cast<cm::Kind>(k), static_cast<cm::Via>(v)});
    return out;
}
std::string afterAppendName(const testing::TestParamInfo<AfterAppendCell>& i) {
    return std::string(cm::name(i.param.kind)) + "_" + (i.param.via == cm::Via::Leader ? "Leader" : "Follower");
}
class CatalogMatrixBelowMinIsrAfterAppendRegression : public testing::TestWithParam<AfterAppendCell> {};

TEST_P(CatalogMatrixBelowMinIsrAfterAppendRegression, InFlightDdlIsAnsweredNotLeaderAndTheRetryIsTheOriginalEventRegression) {
    const AfterAppendCell c = GetParam();
    CatalogClusterHarness h(afterAppendName({c, 0}), 3);
    h.buildPrecondition(c.kind, cm::Persist::Disk);
    const auto run = h.runBelowMinIsrAfterAppend(c.kind, c.via);
    EXPECT_NE(std::find(run.codes.begin(), run.codes.end(), 2), run.codes.end()) << "a NotLeader reached the client";
    EXPECT_EQ(std::count(run.codes.begin(), run.codes.end(), rune::node::test::kClientTimeout), 0) << "never a client Timeout";
    EXPECT_LT(run.firstNotLeaderAfterNs, 1'000'000'000) << "NotLeader at once after the fence, not after the DDL timeout";
    EXPECT_EQ(run.finalCode, 2) << "the retry budget ends with an explicit NotLeader";
    EXPECT_EQ(run.leaderStarted, run.leaderAnswered) << "the in-flight DDL was answered";
    EXPECT_EQ(run.leaderLogEndAfterBudget, run.leaderLogEndAtFence) << "nothing more was appended below minIsr";
    EXPECT_EQ(run.retriedCode, 0) << "the retry after one broker returned";
    EXPECT_TRUE(run.retryMatchesLog);
    EXPECT_EQ(run.eventsWithRid, 1u) << "exactly one event for the requestId";
    EXPECT_EQ(run.eventOffset, run.appendedAt) << "the OK is the ORIGINAL append";
    for (int b = 1; b <= 3; ++b) EXPECT_EQ(h.startedTotal(b), h.answeredTotal(b)) << "broker " << b;
}

INSTANTIATE_TEST_SUITE_P(AllKinds, CatalogMatrixBelowMinIsrAfterAppendRegression, testing::ValuesIn(afterAppendCells()), afterAppendName);

INSTANTIATE_TEST_SUITE_P(AllCells, CatalogInteractionMatrixRegression, testing::ValuesIn(allCells()), cellName);

// ── The harness's mirror of the client rules (Task 5) ────────────────────────
// The harness cannot link rune-client-cpp (the broker has no client dependency), so the
// rules a cell's expected codes depend on are mirrored in ONE place
// (CatalogClusterHarness.h, kDdl*), naming rune-client-cpp/src/Client.cpp.  This pins that
// mirror against the documented values and against the behaviour they produce.
namespace rn = rune::node::test;

TEST(CatalogInteractionMatrixMirrorRegression, ClientRuleConstantsAreTheDocumentedValues) {
    EXPECT_EQ(rn::kDdlInitialBackoffNs, 100'000'000);        // Client.cpp kDdlInitialBackoffNs: 100 ms
    EXPECT_EQ(rn::kDdlMaxBackoffNs,     1'000'000'000);      // Client.cpp kDdlMaxBackoffNs: 1 s
    EXPECT_EQ(rn::kDdlRetryBudgetNs,    10'000'000'000);     // ClientConfig::ddlRetryBudget: 10 s
    EXPECT_EQ(rn::kDdlDeadlineNs,       15'000'000'000);     // ClientConfig::ddlTimeoutMs: 15 s
    EXPECT_EQ(rn::kMaxDdlRedirects,     3);                  // Client::kMaxDdlRedirects
}

// NotLeader: retried with a 100 ms backoff that doubles to 1 s, the LAST resend lands at the
// budget's end, and a NotLeader at or past the budget end fails explicitly with NotLeader (2).
// Resends at +0.1 +0.3 +0.7 +1.5 s, then every second up to 9.5 s, then the budget end (10 s):
// 1 + 4 + 8 + 1 = 14 sends.
TEST(CatalogInteractionMatrixMirrorRegression, NotLeaderBackoffDoublesToTheCapAndFailsExplicitlyAtTheBudgetEnd) {
    CatalogClusterHarness h("mirror_budget", 3);
    h.buildPrecondition(cm::Kind::CreateTopic, cm::Persist::Disk);
    h.stopBroker(2);
    h.stopBroker(3);   // the leader is below its minIsr: every DDL answers NotLeader
    auto& op = h.newOpForTest(h.requestForTest(cm::Kind::CreateTopic, cm::Persist::Disk, true), h.leader());
    const int64_t start = h.now();
    h.runOpsForTest({&op});
    EXPECT_EQ(op.finalCode(), 2) << "an explicit NotLeader once the budget ran out";
    EXPECT_EQ(h.sendsOfForTest(op), 14u);
    EXPECT_EQ(op.codes, std::vector<int>(14, 2));
    // budgetEnd = first NotLeader + 10 s; the client gave up AT it (one pass of slack per hop).
    EXPECT_GE(h.now(), op.budgetEndNs);
    EXPECT_LE(h.now() - op.budgetEndNs, 2'000'000);
    EXPECT_GE(op.budgetEndNs - start, rn::kDdlRetryBudgetNs);
    EXPECT_LE(op.budgetEndNs - start, rn::kDdlRetryBudgetNs + 10'000'000) << "the budget starts at the first NotLeader";
}

// Redirect: followed up to 3 hops; the 4th ends the op with Redirect (3).
TEST(CatalogInteractionMatrixMirrorRegression, RedirectIsFollowedForThreeHopsAndTheFourthEndsTheOp) {
    CatalogClusterHarness h("mirror_redirect", 3);
    h.buildPrecondition(cm::Kind::CreateTopic, cm::Persist::Disk);
    // Every followed Redirect finds the leadership moved: the client chases it.
    auto& op = h.newOpForTest(h.requestForTest(cm::Kind::CreateTopic, cm::Persist::Disk, true), /*target=*/2);
    op.beforeFollowRedirect = [&] { h.moveCatalogLeadership(h.leader() == 1 ? 2 : 1); };
    h.runOpsForTest({&op});
    EXPECT_EQ(op.finalCode(), 3) << "redirected more than 3 times";
    EXPECT_EQ(op.codes, (std::vector<int>{3, 3, 3, 3}));
    EXPECT_EQ(op.redirects, 4);
}
