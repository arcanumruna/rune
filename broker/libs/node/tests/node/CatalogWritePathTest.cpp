// Catalog history §5.1/§8 (Task 9): every DDL is one event, validated against the
// applied catalog by dry-running the shared library's rules, answered after commit
// + local apply; rf bounds are explicit errors; retries dedupe on (requestId,
// statement fingerprint).
#include "support/CatalogDdlRig.h"

#include <array>

#include <rune/node/CatalogEffectFrames.h>
#include <rune/node/DdlAnswerGate.h>

#include <rune/catalog/CatalogQueries.h>
#include <rune/protocol/client/SchemaMessages.h>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using rune::node::test::CatalogDdlRig;
namespace rc = rune::catalog;

// ═══ The plan's tests (Task 9, Step 1) ═══════════════════════════════════════

TEST(CatalogWritePathRegression, DefineTopicIsOneCreateTopicEventWithPrincipalAndStatement) {
    CatalogDdlRig rig("wp_define", /*brokers=*/3);
    rig.becomeServingLeader();
    const auto a = rig.define({.name = "players", .partitions = 3, .typed = true, .rf = 3,
                               .statement = "topic players {...} partitions 3 replicas 3;",
                               .clientName = "cli", .requestId = {.hi = 0xC11E, .lo = 11}});
    EXPECT_EQ(a.code, 0); EXPECT_EQ(a.topicId, 0); EXPECT_EQ(a.version, 1);
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.size(), 1u);
    EXPECT_EQ(log[0].event.kind, rc::EventKind::CreateTopic);
    EXPECT_EQ(log[0].event.statement, "topic players {...} partitions 3 replicas 3;");
    EXPECT_EQ(log[0].event.principal.clientName, "cli");
    EXPECT_EQ(log[0].event.principal.requestId, (rune::protocol::RequestId{0xC11E, 11}));
    EXPECT_NE(log[0].event.principal.requestFingerprint, 0u) << "F14: the leader's statement fingerprint";
    EXPECT_GT(log[0].event.timestampMs, 0) << "stamped at append (F22)";
    EXPECT_EQ(log[0].event.principal.sessionId, rig.sessionId());
    EXPECT_FALSE(log[0].event.principal.sourceAddress.empty());
}

TEST(CatalogWritePathRegression, EveryDdlMapsToItsKind) {
    CatalogDdlRig rig("wp_kinds", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "u", .partitions = 1, .typed = false}).code, 0);           // CreateTopic
    ASSERT_EQ(rig.define({.name = "u", .partitions = 1, .typed = true}).code, 0);            // CreateSchema
    ASSERT_EQ(rig.alterAdd("u", "mana").code, 0);                                            // AlterSchema
    ASSERT_EQ(rig.alterSetCompat("u", rune::codec::CompatMode::Forward).code, 0);            // SetCompatibility
    ASSERT_EQ(rig.alterConfig("u", {{rc::ConfigKey::RetentionMs, 1000}}).code, 0);           // AlterTopicConfig
    ASSERT_EQ(rig.createView("v", "u").code, 0);                                             // CreateView
    ASSERT_EQ(rig.alterView("v", "u", /*extraColumn=*/true).code, 0);                        // AlterView
    ASSERT_EQ(rig.dropView("v").code, 0);                                                    // DropView
    ASSERT_EQ(rig.dropTopic("u").code, 0);                                                   // DropTopic
    std::vector<rc::EventKind> kinds;
    for (const auto& r : rig.recordedLog()) kinds.push_back(r.event.kind);
    EXPECT_EQ(kinds, (std::vector<rc::EventKind>{rc::EventKind::CreateTopic, rc::EventKind::CreateSchema,
        rc::EventKind::AlterSchema, rc::EventKind::SetCompatibility, rc::EventKind::AlterTopicConfig,
        rc::EventKind::CreateView, rc::EventKind::AlterView, rc::EventKind::DropView, rc::EventKind::DropTopic}));
}

TEST(CatalogWritePathRegression, ReplicationFactorBoundsAreExplicitErrorsRegression) {
    CatalogDdlRig rig("wp_rf", /*brokers=*/3, /*topicMinIsr=*/2);
    rig.becomeServingLeader();
    auto hi = rig.define({.name = "a", .partitions = 1, .rf = 4});
    EXPECT_EQ(hi.code, 39); EXPECT_EQ(hi.detail, "replicationFactor 4 > broker count 3");
    auto lo = rig.define({.name = "b", .partitions = 1, .rf = 1});
    EXPECT_EQ(lo.code, 39); EXPECT_EQ(lo.detail, "replicationFactor 1 < minIsr 2");
    EXPECT_EQ(rig.define({.name = "c", .partitions = 1, .rf = 0}).code, 0) << "0 = every broker";
    EXPECT_EQ(rig.define({.name = "d", .partitions = 1, .rf = 2}).code, 0);
    EXPECT_EQ(rig.recordedLog().size(), 2u) << "rejected DDL appends nothing";
}

TEST(CatalogWritePathRegression, DuplicateRequestIdAnswersTheOriginalResultRegression) {
    CatalogDdlRig rig("wp_dedupe", 3);
    rig.becomeServingLeader();
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 99};
    const auto first  = rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    const auto second = rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    EXPECT_EQ(second.code, 0) << "a retry of a committed DDL is its original success, not TopicExists";
    EXPECT_EQ(second.topicId, first.topicId);
    EXPECT_EQ(second.version, first.version);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1, .typed = true, .requestId = {.hi = 0xC11E, .lo = 100}}).code, 25);
}

TEST(CatalogWritePathRegression, RequestIdReusedForADifferentStatementIsInvalidRequestRegression) {
    // F14: two rune ql processes that drew the same id must never get each other's success.
    CatalogDdlRig rig("wp_reuse", 3);
    rig.becomeServingLeader();
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 7};
    ASSERT_EQ(rig.define({.name = "a", .partitions = 1, .requestId = rid}).code, 0);
    const auto other = rig.define({.name = "b", .partitions = 2, .requestId = rid});
    EXPECT_EQ(other.code, 39);
    EXPECT_EQ(other.detail, "requestId reused for a different statement");
    EXPECT_EQ(rig.recordedLog().size(), 1u) << "nothing appended for the reused id";
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
}

TEST(CatalogWritePathRegression, CommitTimeoutAnswersRetriableNotLeaderAndTheRetryIsExactRegression) {
    // F13: past kDdlTimeoutNs the appended event may still commit — the answer must be retriable,
    // and the retry (same requestId) must get the original result once it committed.
    CatalogDdlRig rig("wp_timeout", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});              // follower 2 silent: the event stays uncommitted
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 8};
    const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1, .requestId = rid});
    rig.advanceClockPast(rune::node::AdminWorker::ddlTimeoutNs());
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->code, 2) << "NotLeader (retriable), never Internal, on a healthy broker";
    rig.resumeFollower(2);                                 // the event commits after all
    const auto retry = rig.define({.name = "t", .partitions = 1, .requestId = rid});
    EXPECT_EQ(retry.code, 0) << "the retry is the original success, not TopicExists";
    EXPECT_EQ(rig.countEventsWithRequestId(rid), 1u);
}

// Task 20 fix round 1 (I2): the catalog falls below its minIsr while a DDL awaits its commit.  The event cannot
// commit until a broker returns, so the answer is the retriable NotLeader AT ONCE (within one pass, never after the
// DDL timeout - the client's 15 s deadline would pass first); the retry with the same requestId gets the original
// result once the event committed, never a second event.
TEST(CatalogWritePathRegression, CatalogBelowMinIsrWhileAwaitingCommitAnswersNotLeaderAtOnceRegression) {
    CatalogDdlRig rig("wp_below_isr", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});              // follower 2 silent: the event stays uncommitted
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 9};
    const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1, .requestId = rid});
    ASSERT_FALSE(rig.answerFor(corr).has_value()) << "AwaitingCommit";
    ASSERT_EQ(rig.recordedLog().size(), 1u) << "appended";
    rig.updateIsr({1});                                    // a committed AlterIsr: the ISR is the leader alone (< minIsr 2)
    rig.pump();                                            // ONE pass
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value()) << "answered within one pass, not after kDdlTimeoutNs";
    EXPECT_EQ(a->code, 2) << "NotLeader (retriable)";
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
    EXPECT_EQ(rig.recordedLog().size(), 1u) << "nothing more was appended";
    rig.updateIsr({1, 2});                                 // the follower is back in the ISR ...
    rig.resumeFollower(2);                                 // ... and fetches: the event commits
    const auto retry = rig.define({.name = "t", .partitions = 1, .requestId = rid});
    EXPECT_EQ(retry.code, 0) << "the retry is the original success, not TopicExists";
    EXPECT_EQ(rig.countEventsWithRequestId(rid), 1u);
}

// Task 23 fix round 2 (slice Recipe 3): every NotLeader answer of an in-flight DDL is counted BY CAUSE, so a cluster test can tell a
// stall-induced NotLeader (below minIsr, commit timeout, demotion) from a spurious one.  Each cause moves only its own counter.
TEST(AdminWorkerRegression, NotLeaderAnswersAreCountedByCauseRegression) {
    using Counts = std::array<uint64_t, 3>;
    auto countsOf = [](CatalogDdlRig& rig) {
        return Counts{rig.admin().notLeaderBelowMinIsr(), rig.admin().notLeaderCommitTimeout(), rig.admin().notLeaderDemoted()};
    };
    {   // below minIsr
        CatalogDdlRig rig("wp_cnt_isr", 3);
        rig.becomeServingLeader(/*isr=*/{1, 2});
        ASSERT_EQ(countsOf(rig), (Counts{0, 0, 0}));
        const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1, .requestId = {.hi = 0xC11E, .lo = 21}});
        ASSERT_FALSE(rig.answerFor(corr).has_value());
        rig.updateIsr({1});
        rig.pump();
        ASSERT_TRUE(rig.answerFor(corr).has_value());
        EXPECT_EQ(countsOf(rig), (Counts{1, 0, 0}));
    }
    {   // commit timeout
        CatalogDdlRig rig("wp_cnt_to", 3);
        rig.becomeServingLeader(/*isr=*/{1, 2});
        const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1, .requestId = {.hi = 0xC11E, .lo = 22}});
        rig.advanceClockPast(rune::node::AdminWorker::ddlTimeoutNs());
        ASSERT_TRUE(rig.answerFor(corr).has_value());
        EXPECT_EQ(countsOf(rig), (Counts{0, 1, 0}));
    }
    {   // demotion while the DDL awaits its commit
        CatalogDdlRig rig("wp_cnt_dem", 3);
        rig.becomeServingLeader(/*isr=*/{1, 2});
        const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1, .requestId = {.hi = 0xC11E, .lo = 23}});
        ASSERT_FALSE(rig.answerFor(corr).has_value());
        rig.becomeFollower(/*leaderId=*/2);
        ASSERT_TRUE(rig.answerFor(corr).has_value());
        EXPECT_EQ(countsOf(rig), (Counts{0, 0, 1}));
    }
}

// Task 23 fix round 3 (R2-I1, T8): the admin thread records its own largest inter-pass gap (the slice's stall signal, independent of
// every protocol decision); a reset clears the maximum but keeps the last pass time, so a gap spanning the reset still counts.
// RED: remove the update in AdminWorker::doWork -> 0 != 249 ms.
TEST(AdminWorkerRegression, MaxPassGapRecordsTheLargestSchedulingGapRegression) {
    CatalogDdlRig rig("wp_passgap", 3);
    rig.pump();
    rig.admin().resetMaxPassGapForTest();
    EXPECT_EQ(rig.admin().maxPassGapNs(), 0);
    // pump() itself advances the virtual clock 1 ms before its pass.
    rig.pump();                                   // t
    rig.pump();                                   // t + 1 ms
    rig.advanceClock(248'000'000); rig.pump();    // t + 250 ms
    rig.pump();                                   // t + 251 ms
    EXPECT_EQ(rig.admin().maxPassGapNs(), 249'000'000);
    rig.admin().resetMaxPassGapForTest();
    EXPECT_EQ(rig.admin().maxPassGapNs(), 0);
    rig.advanceClock(6'000'000); rig.pump();      // + 7 ms: the first pass after the reset measures from the last pass before it
    EXPECT_EQ(rig.admin().maxPassGapNs(), 7'000'000);
}

// The DDL timeout is below the client's default DDL deadline (ClientConfig::ddlTimeoutMs = 15 s), so the F13
// NotLeader reaches a default client instead of a Timeout.
TEST(CatalogWritePathRegression, DdlTimeoutIsBelowTheClientDefaultDeadlineRegression) {
    EXPECT_LT(rune::node::AdminWorker::ddlTimeoutNs(), 15'000'000'000LL);
}

// Task 9 fix round 1 (Minor 3): the applier HALTS while a DDL is AwaitingCommit
// (its event commits, the local apply refuses it): the in-flight DDL is answered
// Internal — never OK, never left unanswered — and the next DDL is refused too.
TEST(CatalogWritePathRegression, ApplierHaltWhileAwaitingCommitAnswersInternalRegression) {
    CatalogDdlRig rig("wp_halt_inflight", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});              // follower 2 silent: the event waits for commit
    const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1});
    ASSERT_FALSE(rig.answerFor(corr).has_value()) << "AwaitingCommit";
    ASSERT_EQ(rig.recordedLog().size(), 1u) << "appended";
    rig.admin().applierForTest().tamperNextFingerprintForTest();   // the apply of the committed event fails
    rig.resumeFollower(2);
    for (int i = 0; i < 16 && !rig.answerFor(corr).has_value(); ++i) rig.pump();
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value()) << "answered, never left in flight";
    EXPECT_EQ(a->code, 6) << "Internal: the event will never be applied here";
    EXPECT_TRUE(rig.admin().applier().halted());
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
    EXPECT_EQ(rig.define({.name = "u", .partitions = 1}).code, 6) << "a halted applier accepts no change";
    EXPECT_EQ(rig.recordedLog().size(), 1u);
}

// Task 9 fix round 1 (Minor 3): a same-requestId retry that arrives while the
// original is still in flight waits in the queue and is answered the ORIGINAL
// result once it committed — one event, two identical OKs, never TopicExists.
TEST(CatalogWritePathRegression, SameRequestIdRetryWhileTheOriginalIsInFlightIsItsResultRegression) {
    CatalogDdlRig rig("wp_retry_inflight", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});              // follower 2 silent: the original stays in flight
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 77};
    const int64_t first = rig.submitDefineNoWait({.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    const int64_t retry = rig.submitDefineNoWait({.name = "t", .partitions = 1, .typed = true, .requestId = rid});
    EXPECT_FALSE(rig.answerFor(first).has_value());
    EXPECT_FALSE(rig.answerFor(retry).has_value()) << "queued behind the original, not answered early";
    rig.resumeFollower(2);
    for (int i = 0; i < 64 && !(rig.answerFor(first) && rig.answerFor(retry)); ++i) rig.pump();
    const auto a = rig.answerFor(first);
    const auto b = rig.answerFor(retry);
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a->code, 0);
    EXPECT_EQ(b->code, 0) << "the original success, not TopicExists";
    EXPECT_EQ(b->topicId, a->topicId);
    EXPECT_EQ(b->version, a->version);
    EXPECT_EQ(rig.countEventsWithRequestId(rid), 1u);
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
}

TEST(CatalogWritePathRegression, AlterOfAnUntypedTopicIsUnknownTopicAsToday) {
    // F3: today's startAlter_ answers UnknownTopic (7) for a topic with no schema; DdlExpectations pins it.
    CatalogDdlRig rig("wp_untyped_alter", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "u", .partitions = 1, .typed = false}).code, 0);
    EXPECT_EQ(rig.alterAdd("u", "mana").code, 7);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
}

TEST(CatalogWritePathRegression, DropRulesAreExplicit) {
    CatalogDdlRig rig("wp_drop", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "s", .partitions = 1, .typed = true}).code, 0);
    ASSERT_EQ(rig.createView("v", "s").code, 0);
    auto inUse = rig.dropTopic("s");
    EXPECT_EQ(inUse.code, 39); EXPECT_EQ(inUse.detail, "topic 's' is the source of view 'v'");
    auto isView = rig.dropTopic("v");
    EXPECT_EQ(isView.code, 39); EXPECT_EQ(isView.detail, "'v' is a view; use DROP VIEW");
    EXPECT_EQ(rig.dropTopic("nope").code, 7);
    EXPECT_EQ(rig.dropView("nope").code, 7);
}

TEST(CatalogWritePathRegression, BelowMinIsrIsNotLeaderAndAppendsNothing) {
    CatalogDdlRig rig("wp_minisr", 3);
    rig.becomeServingLeader(/*isr=*/{1});                 // ISR below the catalog minIsr (2)
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1}).code, 2);
    EXPECT_EQ(rig.recordedLog().size(), 0u);
}

TEST(CatalogWritePathRegression, DemotionMidDdlAnswersNotLeaderNotInternal) {
    CatalogDdlRig rig("wp_demote", 3);
    rig.becomeServingLeader(/*isr=*/{1, 2});              // follower 2 never fetches: the event stays uncommitted
    const int64_t corr = rig.submitDefineNoWait({.name = "t", .partitions = 1});
    rig.becomeFollower(/*leaderId=*/2);
    const auto a = rig.answerFor(corr);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->code, 2);
    EXPECT_EQ(rig.admin().startedCount(), rig.admin().answeredCount());
}

TEST(CatalogWritePathRegression, AnswerComesAfterLocalApplyNotJustCommit) {
    CatalogDdlRig rig("wp_order", 3);
    rig.becomeServingLeader();
    std::optional<int64_t> appliedAtAnswer;
    rig.onAnswer([&] { appliedAtAnswer = rig.admin().applier().state().appliedOffset(); });
    ASSERT_EQ(rig.define({.name = "t", .partitions = 1}).code, 0);
    ASSERT_TRUE(appliedAtAnswer.has_value());
    EXPECT_EQ(*appliedAtAnswer, 0);
}

// ═══ Binding items (progress.md, Task 9) ═════════════════════════════════════

// Binding 1: the leader validates by dry-running the library rules — a rule the
// broker never checked itself (a key field that is not a live field) is refused
// with the library's answer and nothing is ever appended.
TEST(CatalogWritePathRegression, DryRunRefusesWhatTheLibraryRulesRefuseRegression) {
    CatalogDdlRig rig("wp_dryrun", 3);
    rig.becomeServingLeader();
    const auto badKey = rig.define({.name = "k", .partitions = 1, .typed = true, .keyField = "nope"});
    EXPECT_EQ(badKey.code, 23) << "a schema the frozen rules refuse is InvalidSchema";
    rune::protocol::TopicConfig cfg;
    cfg.retentionMs = -2;
    const auto badCfg = rig.define({.name = "c", .partitions = 1, .config = cfg});
    EXPECT_EQ(badCfg.code, 39);
    EXPECT_EQ(badCfg.detail, "retention.ms -2 < -1") << "topicConfigError: the ONE config validator";
    ASSERT_EQ(rig.define({.name = "t", .partitions = 1}).code, 0);
    const auto dup = rig.alterConfig("t", {{rc::ConfigKey::RetentionMs, 1}, {rc::ConfigKey::RetentionMs, 2}});
    EXPECT_EQ(dup.code, 39);
    EXPECT_NE(dup.detail.find("set twice"), std::string::npos) << dup.detail;
    const auto empty = rig.alterConfig("t", {});
    EXPECT_EQ(empty.code, 39);
    EXPECT_NE(empty.detail.find("no changes"), std::string::npos) << empty.detail;
    EXPECT_EQ(rig.recordedLog().size(), 1u) << "every refused DDL appended nothing";
    EXPECT_FALSE(rig.admin().applier().halted()) << "nothing the rules refuse ever reached the log";
}

// Binding 2: ONE topic-name rule — the library's (rune/catalog/TopicName.h).
TEST(CatalogWritePathRegression, TopicNameRuleIsTheLibrarysRegression) {
    CatalogDdlRig rig("wp_names", 3);
    rig.becomeServingLeader();
    EXPECT_EQ(rig.define({.name = "__catalog", .partitions = 1}).code, 40);
    EXPECT_EQ(rig.define({.name = "a/b", .partitions = 1}).code, 40);
    EXPECT_EQ(rig.define({.name = std::string(250, 'x'), .partitions = 1}).code, 40);
    ASSERT_EQ(rig.define({.name = "Players", .partitions = 1}).code, 0);
    const auto fold = rig.define({.name = "players", .partitions = 1});
    EXPECT_EQ(fold.code, 25) << "a name that case-folds onto a live one is TopicExists";
    EXPECT_EQ(fold.topicId, 0);
    EXPECT_EQ(rig.recordedLog().size(), 1u);
}

// Binding 3: the effect-size gate — a view whose plan frame would not fit is never
// written, and an AlterSchema that would push a DEPENDENT view's frame past the
// limit is refused too (else every broker would halt at its next boot).
TEST(CatalogWritePathRegression, EffectTooLargeForADependentViewIsRefusedBeforeAppendRegression) {
    CatalogDdlRig rig("wp_effects", 3, 1, /*effectMaxFrame=*/1024);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "s", .partitions = 1, .typed = true}).code, 0);
    ASSERT_EQ(rig.createView("v", "s").code, 0);
    const std::size_t before = rig.recordedLog().size();
    // Grow the source's descriptor past the frame the view's plan update carries.
    std::vector<rune::codec::AlterOp> ops;
    for (int i = 0; i < 8; ++i) {
        rune::codec::AlterOp op{};
        op.type = rune::codec::AlterOpType::Add;
        op.name = "field_" + std::to_string(i) + std::string(100, 'x');
        op.primType = rune::codec::PrimType::Int64;
        ops.push_back(op);
    }
    const auto a = rig.alter("s", ops);
    EXPECT_EQ(a.code, 39);
    EXPECT_NE(a.detail.find("dependent view 'v'"), std::string::npos) << a.detail;
    EXPECT_EQ(rig.recordedLog().size(), before) << "refused before the append";
    EXPECT_FALSE(rig.admin().applier().halted());
    // The same ALTER on a topic no view reads is fine (its own frames fit).
    ASSERT_EQ(rig.define({.name = "lone", .partitions = 1, .typed = true}).code, 0);
    EXPECT_EQ(rig.alter("lone", ops).code, 0);
}

TEST(CatalogWritePathRegression, ViewPlanTooLargeForTheFrameIsRefusedBeforeAppendRegression) {
    CatalogDdlRig rig("wp_effects_view", 3, 1, /*effectMaxFrame=*/120);   // the topic frames fit, the plan does not
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "s", .partitions = 1, .typed = true}).code, 0);
    const auto v = rig.createView("v", "s");
    EXPECT_EQ(v.code, 39);
    EXPECT_NE(v.detail.find("ViewPlanUpdate"), std::string::npos) << v.detail;
    EXPECT_EQ(rig.recordedLog().size(), 1u);
}

// Binding 4: the wire limits — an oversize field is refused, naming it, before the
// event is encoded (an encoder refusal is never reached by client input).
TEST(CatalogWritePathRegression, OversizeFieldIsRefusedBeforeEncodingRegression) {
    CatalogDdlRig rig("wp_wire", 3);
    rig.becomeServingLeader();
    rune::node::DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = 900; m.name = "t"; m.partitionCount = 1;
    m.meta.sessionId = rig.sessionId();
    m.meta.requestId = rig.freshRequestId();
    m.meta.sourceAddress = std::string(rc::kMaxStr16Bytes + 1, 'a');
    const auto a = rig.submitRaw(rune::node::DdlRequest{std::move(m)});
    EXPECT_EQ(a.code, 39);
    EXPECT_NE(a.detail.find("the source address is 65536 bytes"), std::string::npos) << a.detail;
    EXPECT_EQ(rig.recordedLog().size(), 0u);
}

// Binding 5, F37: a pre-v8 client (no DDL extension, no requestId) cannot follow a
// Redirect — on a follower of a known leader it gets InvalidRequest in its own DDL
// reply (never a wait-out); with no known leader (an election) it is the retriable
// NotLeader like every client; on the leader it is served (F28: answered from the
// applied event).
TEST(CatalogWritePathRegression, PreV8ClientIsInvalidRequestOnAFollowerAndServedOnTheLeaderRegression) {
    CatalogDdlRig rig("wp_prev8", 3);
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1, .preV8 = true}).code, 2)
        << "leaderless: nowhere to route — retriable for every client";
    rig.becomeFollower(/*leaderId=*/2);
    const auto onFollower = rig.define({.name = "t", .partitions = 1, .preV8 = true});
    EXPECT_EQ(onFollower.code, 39);
    EXPECT_EQ(onFollower.kind, CatalogDdlRig::Answer::Kind::Define) << "the normal DDL reply";
    EXPECT_EQ(rig.admin().preV8Refused(), 1u);
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1}).code, 2) << "a v8 client gets the retriable NotLeader";
    rig.becomeServingLeader();
    const auto served = rig.define({.name = "t", .partitions = 1, .typed = true, .preV8 = true});
    EXPECT_EQ(served.code, 0);
    EXPECT_EQ(served.topicId, 0);
    EXPECT_EQ(served.version, 1);
    const auto log = rig.recordedLog();
    ASSERT_EQ(log.size(), 1u);
    EXPECT_TRUE(log[0].event.principal.requestId.none());
}

// Binding 6: gateTopicId names the topic the answer is gated on — AlterView's fresh
// sink; -1 for a drop / config change.  The DdlAnswerGate gates exactly those.
TEST(CatalogWritePathRegression, GateTopicIdNamesAlterViewsFreshSinkRegression) {
    CatalogDdlRig rig("wp_gate", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "s", .partitions = 1, .typed = true}).code, 0);   // topic id 0
    const auto v = rig.createView("v", "s");
    ASSERT_EQ(v.code, 0);
    EXPECT_EQ(v.topicId, 1);
    EXPECT_NE(v.fingerprint, 0u) << "CreateView answers the output fingerprint";
    const auto alt = rig.alterView("v", "s", /*extraColumn=*/true);
    ASSERT_EQ(alt.code, 0);
    EXPECT_EQ(alt.gateTopicId, 2) << "the fresh sink (ruling 4)";
    EXPECT_EQ(alt.version, 2u) << "the view's object version";
    const auto cfg = rig.alterConfig("s", {{rc::ConfigKey::RetentionMs, 5}});
    ASSERT_EQ(cfg.code, 0);
    EXPECT_EQ(cfg.gateTopicId, -1);
    const auto drop = rig.dropView("v");
    ASSERT_EQ(drop.code, 0);
    EXPECT_EQ(drop.gateTopicId, -1);
}

TEST(DdlAnswerGateRegression, DdlResponseOkIsGatedOnItsGateTopicIdOnlyRegression) {
    namespace pc = rune::protocol::client;
    auto frame = [](uint16_t code, int32_t gate) {
        pc::DdlResponse r;
        r.sessionId = 7; r.correlationId = 9; r.errorCode = code; r.objectId = 3; r.newVersion = 2;
        r.gateTopicId = gate;
        std::vector<uint8_t> b(r.encodedSize());
        EXPECT_EQ(r.encode(b.data()), b.size());
        return b;
    };
    const auto ok   = frame(0, 5);
    const auto none = frame(0, -1);
    const auto err  = frame(39, 5);
    EXPECT_EQ(rune::node::DdlAnswerGate::gatedTopicOf(ok.data(), ok.size()), 5);
    EXPECT_EQ(rune::node::DdlAnswerGate::gatedTopicOf(none.data(), none.size()), -1);
    EXPECT_EQ(rune::node::DdlAnswerGate::gatedTopicOf(err.data(), err.size()), -1);
    uint8_t out[rune::node::DdlAnswerGate::kInDoubtMaxBytes];
    const std::size_t n = rune::node::DdlAnswerGate::encodeInDoubt(ok.data(), ok.size(), out);
    ASSERT_GT(n, 0u);
    const auto d = pc::DdlResponse::decode(out, n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
    EXPECT_EQ(d->correlationId, 9);
}

// The answer is the applied event's: an AlterSchema's new schema version, an
// AlterTopic CAS against the schema version, a stale AlterView / config version.
TEST(CatalogWritePathRegression, CasConflictsAreSchemaConflictWithTheCurrentVersion) {
    CatalogDdlRig rig("wp_cas", 3);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "s", .partitions = 1, .typed = true}).code, 0);
    const auto a1 = rig.alterAdd("s", "mana", {}, /*expectedVersion=*/1);
    ASSERT_EQ(a1.code, 0);
    EXPECT_EQ(a1.version, 2u);
    const auto stale = rig.alterAdd("s", "mp", {}, /*expectedVersion=*/1);
    EXPECT_EQ(stale.code, 31);
    EXPECT_EQ(stale.version, 2u) << "the current version, for the client's re-read";
    EXPECT_EQ(rig.alterConfig("s", {{rc::ConfigKey::RetentionMs, 9}}, /*expectedVersion=*/1).code, 31)
        << "the object version is 2 after the ALTER";
    ASSERT_EQ(rig.createView("v", "s").code, 0);
    EXPECT_EQ(rig.alterView("v", "s", true, /*expectedVersion=*/7).code, 31);
    EXPECT_EQ(rig.alterConfig("v", {{rc::ConfigKey::RetentionMs, 9}}).code, 38) << "a view's config is not alterable";
}

// Restart: a fresh worker over the same catalog dir replays its prefix (the one boot
// replay) — the schema survives through __catalog, and the next id continues.
TEST(CatalogWritePathRegression, ASchemaSurvivesRestartThroughTheCatalogRegression) {
    CatalogDdlRig rig("wp_restart", 1);
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "s", .partitions = 2, .typed = true}).code, 0);
    ASSERT_EQ(rig.alterAdd("s", "mana").code, 0);
    const uint64_t digest = rig.admin().applier().state().digest();
    rig.restart();
    EXPECT_EQ(rig.admin().applier().phaseName(), "Live");
    EXPECT_EQ(rig.admin().applier().state().digest(), digest);
    ASSERT_NE(rig.schemas().byName("s"), nullptr) << "the projection comes back from __catalog";
    EXPECT_EQ(rig.schemas().byName("s")->version, 2);
    EXPECT_EQ(rig.schemas().byName("s")->partitionCount, 2);
    rig.becomeServingLeader();
    const auto t = rig.define({.name = "t", .partitions = 1});
    EXPECT_EQ(t.code, 0);
    EXPECT_EQ(t.topicId, 1) << "catalog ids continue after a restart";
}

// Binding 4: the EncodeError mapping — the leader's own bugs are Internal, every
// input-derived refusal InvalidRequest with a detail naming it.  Every enumerator.
TEST(CatalogWritePathRegression, EncodeErrorMapsLeaderBugsToInternalAndInputToInvalidRequestRegression) {
    using E = rc::EncodeError;
    for (E e : {E::KindOpsMismatch, E::UnknownKind, E::UnknownFormatVer}) {
        SCOPED_TRACE(std::string(rc::encodeErrorName(e)));
        EXPECT_EQ(rune::node::encodeErrorAnswer(e).code, 6);
    }
    for (E e : {E::FieldTooLong, E::TooManyElements, E::ReservedKind, E::BadEnum}) {
        SCOPED_TRACE(std::string(rc::encodeErrorName(e)));
        const auto r = rune::node::encodeErrorAnswer(e);
        EXPECT_EQ(r.code, 39);
        EXPECT_NE(r.detail.find(std::string(rc::encodeErrorName(e))), std::string::npos) << r.detail;
    }
}

// F14: a retry after a leader change (a new epoch, and a restart — the state is
// rebuilt from the log) gets the ORIGINAL result, once; another statement under the
// same id is still refused.
TEST(CatalogWritePathRegression, RetryAfterALeaderChangeGetsTheOriginalResultRegression) {
    CatalogDdlRig rig("wp_retry_epoch", 3);
    rig.becomeServingLeader();
    const rune::protocol::RequestId rid{.hi = 0xC11E, .lo = 42};
    const auto first = rig.define({.name = "t", .partitions = 2, .typed = true, .requestId = rid});
    ASSERT_EQ(first.code, 0);
    rig.becomeFollower(2);
    EXPECT_EQ(rig.define({.name = "t", .partitions = 2, .typed = true, .requestId = rid}).code, 2)
        << "a follower answers the retriable NotLeader";
    rig.becomeServingLeader();   // re-elected at a new epoch
    const auto again = rig.define({.name = "t", .partitions = 2, .typed = true, .requestId = rid});
    EXPECT_EQ(again.code, 0);
    EXPECT_EQ(again.topicId, first.topicId);
    EXPECT_EQ(again.version, first.version);
    rig.restart();
    rig.becomeServingLeader();
    const auto afterRestart = rig.define({.name = "t", .partitions = 2, .typed = true, .requestId = rid});
    EXPECT_EQ(afterRestart.code, 0) << "the request record is part of the replayed state";
    EXPECT_EQ(afterRestart.topicId, first.topicId);
    EXPECT_EQ(rig.define({.name = "u", .partitions = 1, .requestId = rid}).code, 39);
    EXPECT_EQ(rig.countEventsWithRequestId(rid), 1u);
}

// ===========================================================================
// Task 11 fix round 1 (I2): never commit a topic its controller cannot place.
// ===========================================================================

// RED: rf 3 with one catalog-halted broker committed, then stranded unplaceable
// (the client saw Internal "in doubt" after 5 s, and the name was taken).
TEST(CatalogWritePathRegression, ReplicationFactorAbovePlaceableBrokersIsInvalidRequestRegression) {
    CatalogDdlRig rig("wp_placeable", 3);
    rig.becomeServingLeader();
    EXPECT_EQ(rig.admin().placeableBrokers(), 3) << "every broker until the first view";
    rune::node::PlacementViewMsg v;
    v.placeable = 2; v.haltedCount = 1; v.halted[0] = 3;
    rig.admin().onPlacementView(v);
    const auto refused = rig.define({.name = "t", .partitions = 1, .rf = 3});
    EXPECT_EQ(refused.code, 39);
    EXPECT_EQ(refused.detail, "replicationFactor 3 > placeable brokers 2 (broker 3: catalog apply halted)");
    EXPECT_TRUE(rig.recordedLog().empty()) << "nothing appended";
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1, .rf = 2}).code, 0) << "rf within the placeable brokers";
    EXPECT_EQ(rig.define({.name = "u", .partitions = 1}).code, 0) << "rf 0 = every placeable broker";
    v.placeable = 3; v.haltedCount = 0;
    rig.admin().onPlacementView(v);                        // broker 3 restarted healthy
    EXPECT_EQ(rig.define({.name = "w", .partitions = 1, .rf = 3}).code, 0);
}

TEST(CatalogWritePathRegression, PlacementViewFrameRoundTripsAndRefusesMalformedFrames) {
    rune::node::PlacementViewMsg v;
    v.placeable = 4; v.haltedCount = 2; v.halted[0] = 5; v.halted[1] = 9;
    std::vector<uint8_t> b(rune::node::placementViewWireLen(v));
    rune::node::writePlacementView(b.data(), v);
    rune::node::PlacementViewMsg d;
    ASSERT_TRUE(rune::node::decodePlacementView(b.data(), b.size(), d));
    EXPECT_EQ(d, v);
    EXPECT_FALSE(rune::node::decodePlacementView(b.data(), b.size() - 1, d)) << "short";
    b.push_back(0);
    EXPECT_FALSE(rune::node::decodePlacementView(b.data(), b.size(), d)) << "trailing byte";
    std::vector<uint8_t> big(6, 0);
    big[4] = 0xFF; big[5] = 0xFF;
    EXPECT_FALSE(rune::node::decodePlacementView(big.data(), big.size(), d)) << "count above the cap";
}

// The post-commit race (a broker halts between the commit and the placement):
// the gate's in-doubt answer is EXPLICIT — a DdlResponse carries the reason in
// its detail; a v8 DefineTopic gets an ErrorResponse{Internal, reason}; a pre-v8
// one its own type (no detail field).  RED: a bare Internal with no detail.
TEST(CatalogWritePathRegression, InDoubtAnswerNamesWhyTheCommittedTopicDoesNotServeRegression) {
    namespace pc = rune::protocol::client;
    using Gate = rune::node::DdlAnswerGate;
    const std::string why = "the DDL committed but topic id=5 does not serve after 5000 ms — UNPLACEABLE: rf";
    uint8_t out[Gate::kInDoubtMaxBytes];
    pc::DdlResponse ok;
    ok.sessionId = 7; ok.correlationId = 9; ok.gateTopicId = 5;
    std::vector<uint8_t> f(ok.encodedSize());
    ok.encode(f.data());
    std::size_t n = Gate::encodeInDoubt(f.data(), f.size(), out, why, true);
    const auto d = pc::DdlResponse::decode(out, n);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
    EXPECT_EQ(d->detail, why);
    pc::DefineTopicResponse def;
    def.sessionId = 7; def.correlationId = 10; def.topicId = 5;
    std::vector<uint8_t> g(pc::MessageHeader::ENCODED_SIZE + pc::DefineTopicResponse::BODY_FIXED);
    def.encode(g.data());
    n = Gate::encodeInDoubt(g.data(), g.size(), out, why, /*asErrorResponse=*/true);
    const auto e = pc::ErrorResponse::decode(out, n);
    ASSERT_TRUE(e.has_value()) << "a v8 session gets the reason";
    EXPECT_EQ(e->code, static_cast<int16_t>(pc::ErrorCode::Internal));
    EXPECT_EQ(e->correlationId, 10);
    EXPECT_EQ(e->message, why);
    n = Gate::encodeInDoubt(g.data(), g.size(), out, why, /*asErrorResponse=*/false);
    const auto own = pc::DefineTopicResponse::decode(out, n);
    ASSERT_TRUE(own.has_value()) << "a pre-v8 session gets its own type";
    EXPECT_EQ(own->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal));
    // release() asks for the explanation and sends it.
    Gate gate(1 << 16, /*maxHoldNs=*/10);
    auto never = [](int32_t) { return false; };
    ASSERT_EQ(gate.offer(7, f.data(), f.size(), 0, never), Gate::Offer::Held);
    std::vector<uint8_t> sent;
    EXPECT_EQ(gate.release(100, never, [&](int64_t, const uint8_t* p, std::size_t m) { sent.assign(p, p + m); },
                           [](int64_t, int32_t) {},
                           [&](int64_t, int32_t) { return Gate::Explanation{why, true}; }), 1);
    const auto r = pc::DdlResponse::decode(sent.data(), sent.size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->detail, why);
}

// Catalog history Task 12 (fix-on-sight): the leader appended events up to half
// a fetch (512 KiB) — larger than one Records frame of a catalog subscriber
// (kCatalogReadMaxBatchBytes, ~256 KiB), so a written event could never be READ
// by any client.  The leader's bound now includes the read bound: an event no
// subscriber could read is refused (InvalidRequest, named), never written.
TEST(CatalogWritePathRegression, EventLargerThanOneCatalogReadFrameIsRefusedRegression) {
    CatalogDdlRig rig("wp_readable", /*brokers=*/3);
    rig.becomeServingLeader();
    const auto big = rig.define({.name = "huge", .statement = std::string(rune::node::kCatalogMaxEventBytes, 's')});
    EXPECT_EQ(big.code, 39) << big.detail;
    EXPECT_NE(big.detail.find("catalog record limit"), std::string::npos) << big.detail;
    EXPECT_TRUE(rig.recordedLog().empty()) << "nothing written";
    const auto fits = rig.define({.name = "fits", .statement = std::string(rune::node::kCatalogMaxEventBytes / 2, 's')});
    EXPECT_EQ(fits.code, 0) << fits.detail;
    ASSERT_EQ(rig.recordedLog().size(), 1u);
}
