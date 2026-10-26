// DdlHarnessRegressionTest.cpp — DDL conformance plan: smoke tests of DdlHarness
// itself (Task 4) and the name / case-fold / log-escape regressions (B1-B3, B6),
// ported to the __catalog write path by catalog history Task 10.  The leadership
// regressions (B4, KL2, B8, B9, in-flight state) are in DdlLeadershipRegressionTest.cpp,
// the dedupe ones (KL1) in DdlDedupeRegressionTest.cpp (fix round 1, review M8).
// Every suite name contains "Regression".
//
// Former cells that tested the deleted control-thread create round-trip (the
// createTopicForAdmin reply ring, orphan topics / sinks and their adoption, the
// __orphan.<id> placeholders, the topic-id dir markers, the per-catalog commit
// replicas) are either re-pinned on their catalog-path counterpart (named in each
// test's comment) or ruled out in the Task 10 report.
#include <gtest/gtest.h>
#include <rune/node/NamespacePaths.h>
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


TEST(DdlHarnessRegression, DefineViewAlterRoundTripAndRestartKeepsTheCatalog) {
    DdlHarness h;
    const int64_t d = h.submit(Op::DefineDisk, "t");
    h.settle();
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    h.settle();
    const int64_t a = h.submit(Op::AlterAdd, "t");
    h.settle();
    ASSERT_EQ(h.response(d)->err, kOk);
    ASSERT_EQ(h.response(v)->err, kOk);
    ASSERT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(a)->version, 2u);
    const Catalog before = h.catalog();
    ASSERT_NE(findEntry(before, "v"), nullptr);
    EXPECT_EQ(findEntry(before, "v")->kind, EntryKind::View);
    h.restart();
    EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Was HeldControlReplyKeepsTheNextOpQueued: the held stage is now the HW.
TEST(DdlHarnessRegression, HeldHwKeepsTheNextOpQueued) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t x = h.submit(Op::DefineDisk, "x");
    const int64_t y = h.submit(Op::DefineDisk, "y");
    ASSERT_TRUE(h.tickUntilAppended());
    const uint64_t events = h.eventsAppended();
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_EQ(h.eventsAppended(), events) << "y must not start while x is AwaitingCommit";
    EXPECT_FALSE(h.response(x).has_value());
    EXPECT_FALSE(h.response(y).has_value());
    h.setControl(ControlMode::Auto);
    h.settle();
    EXPECT_EQ(h.response(x)->err, kOk);
    EXPECT_EQ(h.response(y)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlHarnessRegression, HeldCommitKeepsTheOpInFlightUntilReleased) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_FALSE(h.response(a).has_value()) << "an op is answered only after its event commits";
    EXPECT_TRUE(h.worker().hasInFlight());
    h.settle();   // releases commits
    EXPECT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(a)->version, 2u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Was OrphanThenLaterTopicRestartsWithAPlaceholder.  A create whose event never
// committed leaves no topic, dir or id: the next topic takes the id it would have
// had, and a restart shows no trace of it (no __orphan placeholder exists any more).
TEST(DdlHarnessRegression, UncommittedCreateThenLaterTopicLeavesNoTraceAcrossARestart) {
    DdlHarness h;
    const int32_t nextId = h.state().nextTopicId();
    ASSERT_EQ(refusedAtAppend(h, Op::ViewPersisted, "orph"), kNotLeader);
    EXPECT_EQ(h.idOf("orph"), -1);
    EXPECT_EQ(h.state().nextTopicId(), nextId) << "the refused create used no id";
    const int64_t l = h.submit(Op::DefineDisk, "later");
    h.settle();
    ASSERT_EQ(h.response(l)->err, kOk);
    EXPECT_EQ(h.idOf("later"), nextId);
    const Catalog before = h.catalog();
    h.restart();
    EXPECT_EQ(h.catalog(), before);
    EXPECT_EQ(h.idOf("orph"), -1) << "the name is free after the restart";
    EXPECT_EQ(h.idOf(std::string("__orphan.") + std::to_string(nextId)), -1) << "no placeholder exists";
    const int64_t again = h.submit(Op::ViewPersisted, "orph");
    h.settle();
    EXPECT_EQ(h.response(again)->err, kOk) << "the name was never taken";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// The no-hang queue cap (kMaxPendingDdl) answers an over-cap submit Internal at
// once, never started: the harness accounts it outside started/answered and FIFO.
TEST(DdlHarnessRegression, QueueCapRefusalCoexistsWithTheSerializationInvariant) {
    DdlHarness h;
    std::vector<int64_t> queued;
    for (std::size_t i = 0; i < AdminWorker::kMaxPendingDdl; ++i)
        queued.push_back(h.submit(Op::AlterAdd, "src"));
    const int64_t refused = h.submit(Op::DefineDisk, "over");
    ASSERT_TRUE(h.response(refused).has_value()) << "an over-cap submit is answered at once";
    EXPECT_EQ(h.response(refused)->err, kInternal);
    EXPECT_EQ(h.worker().pendingDdlRefused(), 1u);
    h.settle(/*maxTicks=*/4000);
    for (const int64_t c : queued) ASSERT_EQ(h.response(c)->err, kOk) << "corr " << c;
    EXPECT_EQ(h.idOf("over"), -1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── negative tests: the serialization invariant checker CAN fail ────────────
namespace {
bool anyViolationContains(const DdlHarness& h, const std::string& needle) {
    for (const std::string& v : h.violations())
        if (v.find(needle) != std::string::npos) return true;
    return false;
}
} // namespace

TEST(DdlHarnessRegression, InvariantFlagsAnAnswerWithNoOpInFlight) {
    DdlHarness h;
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    ASSERT_FALSE(h.worker().hasInFlight());
    const int64_t d = h.submit(Op::DefineDisk, "t");
    h.settle();
    ASSERT_EQ(h.response(d)->err, kOk);
    h.injectAnswerForCheckerSelfTest(d + 1000, kOk);
    ASSERT_FALSE(h.violations().empty()) << "a spurious answer must be flagged";
    EXPECT_TRUE(anyViolationContains(h, "with 0 ops started-but-unanswered (want 1)")) << h.violationText();
    EXPECT_TRUE(anyViolationContains(h, "injected answer: -1 DDL ops started-but-unanswered, in flight 0"))
        << h.violationText();
}

TEST(DdlHarnessRegression, InvariantFlagsAnOutOfOrderAnswer) {
    DdlHarness h;
    const int64_t a = h.submit(Op::DefineDisk, "a");
    const int64_t b = h.submit(Op::DefineDisk, "b");
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    ASSERT_EQ(h.response(b)->err, kOk);
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    h.injectAnswerForCheckerSelfTest(a, kOk);
    ASSERT_FALSE(h.violations().empty()) << "a misordered answer must be flagged";
    EXPECT_TRUE(anyViolationContains(h, "answer for corr " + std::to_string(a) + " out of FIFO order (previous " +
                                         std::to_string(b) + ")")) << h.violationText();
}

TEST(DdlHarnessRegression, InvariantFlagsAnAnswerWhileAnotherOpIsInFlight) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t x = h.submit(Op::DefineDisk, "x");
    ASSERT_TRUE(h.tickUntilAppended());
    ASSERT_TRUE(h.worker().hasInFlight());
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    // x is AwaitingCommit; an answer that is not x's leaves x started-but-unanswered
    // while the worker still reports it in flight: a second, phantom op.
    h.injectAnswerForCheckerSelfTest(x + 1000, kOk);
    h.injectAnswerForCheckerSelfTest(x + 1001, kOk);
    ASSERT_FALSE(h.violations().empty());
    EXPECT_TRUE(anyViolationContains(h, "injected answer: 0 DDL ops started-but-unanswered, in flight 1"))
        << "the post-step check catches what the per-answer check cannot: " << h.violationText();
    EXPECT_TRUE(anyViolationContains(h, "injected answer: -1 DDL ops started-but-unanswered, in flight 1"))
        << h.violationText();
    EXPECT_TRUE(anyViolationContains(h, "with 0 ops started-but-unanswered (want 1)")) << h.violationText();
    h.setControl(ControlMode::Auto);
    h.settle();
}

// ── B1 / B2 / B6 ──────────────────────────────────────────────────────────────
// Every rune::catalog::TopicNameError kind, sampled several ways each, on every path:
// DefineTopic (disk + transient), CreateView (persisted + transient), and the shared
// library's apply (the defensive layer every broker runs — was the control thread's
// createTopicForAdmin).  The config-parse path is in NodeConfigTest.cpp (Task 5a).
namespace {
struct BadName {
    std::string   name;
    cat::TopicNameError kind;
    const char*   label;
};
std::vector<BadName> badNames() {
    return {
        {"", cat::TopicNameError::Empty, "empty"},
        {std::string(250, 'x'), cat::TopicNameError::TooLong, "len250"},
        {std::string(1000, 'y'), cat::TopicNameError::TooLong, "len1000"},
        {".", cat::TopicNameError::DotSegment, "dot"},
        {"..", cat::TopicNameError::DotSegment, "dotdot"},
        {"../escape", cat::TopicNameError::BadChar, "traversal"},
        {"a/b", cat::TopicNameError::BadChar, "slash"},
        {"/abs", cat::TopicNameError::BadChar, "absolute"},
        {"a\\b", cat::TopicNameError::BadChar, "backslash"},
        {"has space", cat::TopicNameError::BadChar, "space"},
        {"tab\tname", cat::TopicNameError::BadChar, "tab"},
        {"new\nline", cat::TopicNameError::BadChar, "newline"},
        {std::string("nul\0byte", 8), cat::TopicNameError::BadChar, "nul"},
        {"caf\xc3\xa9", cat::TopicNameError::BadChar, "nonascii"},
        {"a:b", cat::TopicNameError::BadChar, "colon"},
        {"__topic_schemas", cat::TopicNameError::Reserved, "topic_schemas"},
        {"__views", cat::TopicNameError::Reserved, "views"},
        {"__consumer_offsets", cat::TopicNameError::Reserved, "consumer_offsets"},
        {"__transaction_state", cat::TopicNameError::Reserved, "transaction_state"},
        {"__cluster_metadata", cat::TopicNameError::Reserved, "cluster_metadata"},
        {"__view_snapshots", cat::TopicNameError::Reserved, "view_snapshots"},
        {"__orphan.1", cat::TopicNameError::Reserved, "orphan"},
        {"__x", cat::TopicNameError::Reserved, "x"},
        {"__", cat::TopicNameError::Reserved, "bare"},
    };
}

/// A CreateTopic event for `name` built exactly as the leader would on `s`.
cat::CatalogEvent createTopicEvent(const cat::CatalogState& s, const std::string& name, int32_t partitions = 1) {
    cat::CatalogEvent e;
    e.formatVer   = cat::kCurrentFormatVer;
    e.objectName  = name;
    e.objectId    = s.nextObjectId();
    e.prevVersion = 0;
    e.newVersion  = 1;
    cat::CreateTopicOps op;
    op.topicId    = s.nextTopicId();
    op.partitions = partitions;
    e.ops  = op;
    e.kind = cat::kindOf(e.ops);
    return e;
}
} // namespace

class DdlNameRegression : public ::testing::TestWithParam<BadName> {};

TEST_P(DdlNameRegression, TableCoversTheKindItClaims) {
    EXPECT_EQ(cat::checkTopicName(GetParam().name), GetParam().kind);
}

TEST_P(DdlNameRegression, AdminPathRejectsEveryOpWithInvalidTopicNameAndCreatesNothing) {
    const BadName& b = GetParam();
    DdlHarness h;
    const int32_t topics = h.topicCount();
    const int64_t logEnd = h.catalogLogEnd();
    const uint64_t events = h.eventsAppended();
    int64_t c[4];
    int i = 0;
    for (const Op op : {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient})
        c[i++] = h.submit(op, b.name);
    h.settle();
    for (const int64_t corr : c) {
        ASSERT_TRUE(h.response(corr).has_value());
        EXPECT_EQ(h.response(corr)->err, kInvalidTopicName);
        EXPECT_EQ(h.response(corr)->id, -1);
    }
    EXPECT_EQ(h.topicCount(), topics);
    EXPECT_EQ(h.eventsAppended(), events) << "never reaches the catalog";
    EXPECT_EQ(h.catalogLogEnd(), logEnd);
    EXPECT_FALSE(fs::exists(h.root() / "escape")) << "nothing written outside the data dir";
    EXPECT_FALSE(fs::exists(rune::node::topicsRoot(h.root(), 0))) << "the admin path writes no topic dir at all";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST_P(DdlNameRegression, InvalidNameIsCheckedBeforeEveryLaterDefineCheck) {
    // A bad name AND a zero partition count: the name wins (40, not 23).
    const BadName& b = GetParam();
    DdlHarness h;
    DefineTopicMsg m;
    m.partitionCount = 0; m.name = b.name; m.keyField = "id"; m.blob = charBlob();
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kInvalidTopicName);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Was ControlThreadRefusesTheNameDefensively (createTopicForAdmin is deleted): the
// defensive layer is now the shared library's frozen apply rules — a CreateTopic
// event naming a bad name is refused by every broker's apply (and the leader's
// dry run), so it can never be written or applied.
TEST_P(DdlNameRegression, CatalogApplyRefusesTheNameDefensively) {
    const BadName& b = GetParam();
    cat::CatalogState s;
    const cat::CatalogEvent e = createTopicEvent(s, b.name);
    const auto preview = cat::previewFingerprint(s, e);
    ASSERT_FALSE(preview.has_value());
    EXPECT_EQ(preview.error().code, cat::ApplyError::Code::Precondition);
    const auto applied = cat::apply(s, e, 0);
    ASSERT_FALSE(applied.has_value());
    EXPECT_EQ(s.appliedOffset(), -1) << "the state is unchanged";
    EXPECT_TRUE(s.topics().empty());
}

INSTANTIATE_TEST_SUITE_P(EveryKind, DdlNameRegression, ::testing::ValuesIn(badNames()),
                         [](const ::testing::TestParamInfo<BadName>& i) { return std::string(i.param.label); });

TEST(DdlFixRegression, B1_EveryTopicNameErrorKindIsCoveredByTheTable) {
    bool seen[6] = {};
    for (const BadName& b : badNames()) seen[static_cast<int>(b.kind)] = true;
    EXPECT_FALSE(seen[static_cast<int>(cat::TopicNameError::None)]);
    for (const cat::TopicNameError k : {cat::TopicNameError::Empty, cat::TopicNameError::TooLong,
                                        cat::TopicNameError::DotSegment, cat::TopicNameError::BadChar,
                                        cat::TopicNameError::Reserved})
        EXPECT_TRUE(seen[static_cast<int>(k)]) << static_cast<int>(k);
}

TEST(DdlFixRegression, B1_ValidBoundaryNamesStillCreate) {
    DdlHarness h;
    const std::string maxLen(249, 'm');
    int n = 0;
    for (const std::string name : {std::string("orders.v2"), std::string("_private"), std::string("a-b_c"),
                                   std::string("A1"), std::string("_"), std::string("x__y"), maxLen}) {
        SCOPED_TRACE(name.substr(0, 20));
        const int64_t d = h.submit(n++ % 2 ? Op::DefineTransient : Op::DefineDisk, name);
        h.settle();
        EXPECT_EQ(h.response(d)->err, kOk);
    }
    const int64_t v = h.submit(Op::ViewPersisted, "view.ok-1");
    h.settle();
    EXPECT_EQ(h.response(v)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B1_RejectedNamesDoNotPoisonTheQueueBehindThem) {
    DdlHarness h;
    const int64_t bad = h.submit(Op::DefineDisk, "../escape");
    const int64_t good = h.submit(Op::DefineDisk, "fine");
    const int64_t bad2 = h.submit(Op::ViewPersisted, "__views");
    h.settle();
    EXPECT_EQ(h.response(bad)->err, kInvalidTopicName);
    EXPECT_EQ(h.response(good)->err, kOk);
    EXPECT_EQ(h.response(bad2)->err, kInvalidTopicName);
    EXPECT_NE(h.idOf("fine"), -1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// The control-thread half is now the library's apply (partitions < 1 refused).
TEST(DdlFixRegression, B6_PartitionCountBelowOneIsInvalidSchemaOnAdminAndCatalogApply) {
    DdlHarness h;
    const int32_t topicsBefore = h.topicCount();
    const uint64_t eventsBefore = h.eventsAppended();
    for (const int32_t parts : {0, -1, -1000, INT32_MIN}) {
        SCOPED_TRACE(parts);
        for (const uint8_t persistence : {uint8_t{0}, uint8_t{1}}) {
            DefineTopicMsg m;
            m.partitionCount = parts; m.persistence = persistence;
            m.name = "zero_parts"; m.keyField = "id"; m.blob = charBlob();
            const int64_t c = h.submitRawDefine(std::move(m));
            h.settle();
            EXPECT_EQ(h.response(c)->err, kInvalidSchema);
        }
    }
    EXPECT_EQ(h.topicCount(), topicsBefore);
    EXPECT_EQ(h.eventsAppended(), eventsBefore);
    EXPECT_EQ(h.idOf("zero_parts"), -1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // One partition is the floor and still works.
    DefineTopicMsg ok;
    ok.partitionCount = 1; ok.name = "one_part"; ok.keyField = "id"; ok.blob = charBlob();
    const int64_t c = h.submitRawDefine(std::move(ok));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kOk);

    // The shared apply: the same refusal, nothing applied.
    cat::CatalogState s;
    for (const int32_t parts : {0, -1, INT32_MIN}) {
        const auto r = cat::previewFingerprint(s, createTopicEvent(s, "ok", parts));
        ASSERT_FALSE(r.has_value()) << parts;
        EXPECT_EQ(r.error().code, cat::ApplyError::Code::Precondition);
    }
    EXPECT_TRUE(cat::previewFingerprint(s, createTopicEvent(s, "ok", 1)).has_value());
}

TEST(DdlFixRegression, B1_RestartAfterRejectedNamesKeepsTheCatalogUnchanged) {
    DdlHarness h;
    (void)h.submit(Op::DefineDisk, "../escape");
    (void)h.submit(Op::ViewTransient, "__orphan.9");
    h.settle();
    const Catalog before = h.catalog();
    h.restart();
    EXPECT_EQ(h.catalog(), before);
    EXPECT_EQ(h.idOf("__orphan.9"), -1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── B3 ────────────────────────────────────────────────────────────────────────
// Names are case-sensitive identifiers, but no two may differ only in ASCII case
// (they would share dataDir/topics/<name>/ on a case-insensitive filesystem).  The
// admin thread decides from the applied catalog (the library's fold rule): it
// answers TopicExists (25) with the colliding topic's id and appends nothing.
namespace {
struct FoldPair { const char* existing; const char* variant; const char* label; };
const FoldPair kFoldPairs[] = {
    {"Orders", "orders", "UpperThenLower"},
    {"orders", "Orders", "LowerThenUpper"},
    {"orders", "ORDERS", "LowerThenAllCaps"},
    {"Ab.c-D_1", "aB.C-d_1", "MixedWithPunctuation"},
};
const Op kCreateOps[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient};

const char* opLabel(Op op) { return opName(op); }
bool isViewOp(Op op) { return op == Op::ViewPersisted || op == Op::ViewTransient; }

struct FoldCase { Op first; Op second; FoldPair names; };
void PrintTo(const FoldCase& c, std::ostream* os) {
    *os << opLabel(c.first) << " '" << c.names.existing << "' then " << opLabel(c.second) << " '"
        << c.names.variant << "'";
}
std::vector<FoldCase> foldCases() {
    std::vector<FoldCase> v;
    for (const Op a : kCreateOps)
        for (const Op b : kCreateOps)
            for (const FoldPair& p : kFoldPairs) v.push_back({a, b, p});
    return v;
}
std::string foldCaseName(const ::testing::TestParamInfo<FoldCase>& i) {
    return std::string(opLabel(i.param.first)) + "_then_" + opLabel(i.param.second) + "_" + i.param.names.label;
}
} // namespace

class DdlCaseFoldRegression : public ::testing::TestWithParam<FoldCase> {};

TEST_P(DdlCaseFoldRegression, CaseVariantIsTopicExistsAndCreatesNothing) {
    const FoldCase& c = GetParam();
    DdlHarness h;
    const int64_t a = h.submit(c.first, c.names.existing);
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    const int32_t existingId = h.idOf(c.names.existing);
    ASSERT_GE(existingId, 0);
    const int32_t topics = h.topicCount();
    const int64_t logEnd = h.catalogLogEnd();
    const size_t viewCount = h.views().all().size();

    const int64_t b = h.submit(c.second, c.names.variant);
    h.settle();
    ASSERT_TRUE(h.response(b).has_value());
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.response(b)->id, existingId) << "answers the colliding topic's id";
    EXPECT_EQ(h.topicCount(), topics) << "no second topic sharing the directory";
    EXPECT_EQ(h.idOf(c.names.variant), -1);
    EXPECT_EQ(h.views().byName(c.names.variant), nullptr);
    EXPECT_EQ(h.views().all().size(), viewCount);
    EXPECT_EQ(h.catalogLogEnd(), logEnd) << "no event for the variant";
    if (isViewOp(c.first)) EXPECT_NE(h.views().byName(c.names.existing), nullptr) << "the existing view is intact";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // The refusal leaves nothing behind that a restart could resurrect.
    const Catalog before = h.catalog();
    h.restart();
    EXPECT_EQ(h.catalog(), before);
    EXPECT_EQ(h.idOf(c.names.variant), -1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST_P(DdlCaseFoldRegression, CaseVariantQueuedBehindTheOriginalIsTopicExists) {
    // Both submitted before either runs: serialization decides — the first creates,
    // the variant (started only after the first committed) is refused.
    const FoldCase& c = GetParam();
    DdlHarness h;
    const int32_t topics = h.topicCount();
    const int64_t a = h.submit(c.first, c.names.existing);
    const int64_t b = h.submit(c.second, c.names.variant);
    h.settle();
    EXPECT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.response(b)->id, h.idOf(c.names.existing));
    EXPECT_EQ(h.topicCount(), topics + 1);
    EXPECT_EQ(h.idOf(c.names.variant), -1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(EveryKindPair, DdlCaseFoldRegression, ::testing::ValuesIn(foldCases()), foldCaseName);

// Exact-same names keep their pre-B3 outcomes (the fold rule never fires on them).
namespace {
struct ExactCase { Op first; Op second; uint16_t want; const char* label; };
void PrintTo(const ExactCase& c, std::ostream* os) { *os << c.label << " want " << c.want; }
std::vector<ExactCase> exactCases() {
    return {
        {Op::DefineDisk,      Op::DefineDisk,      kTopicExists, "DefineDisk_DefineDisk"},
        {Op::DefineDisk,      Op::DefineTransient, kTopicExists, "DefineDisk_DefineTransient"},
        {Op::DefineTransient, Op::DefineDisk,      kTopicExists, "DefineTransient_DefineDisk"},
        {Op::DefineDisk,      Op::ViewPersisted,   kTopicExists, "DefineDisk_ViewPersisted"},
        {Op::DefineTransient, Op::ViewTransient,   kTopicExists, "DefineTransient_ViewTransient"},
        {Op::ViewPersisted,   Op::DefineDisk,      kTopicExists, "ViewPersisted_DefineDisk"},
        {Op::ViewTransient,   Op::DefineTransient, kTopicExists, "ViewTransient_DefineTransient"},
        {Op::ViewPersisted,   Op::ViewPersisted,   kViewExists,  "ViewPersisted_ViewPersisted"},
        {Op::ViewPersisted,   Op::ViewTransient,   kViewExists,  "ViewPersisted_ViewTransient"},
        {Op::ViewTransient,   Op::ViewPersisted,   kViewExists,  "ViewTransient_ViewPersisted"},
    };
}
} // namespace

class DdlExactNameRegression : public ::testing::TestWithParam<ExactCase> {};

TEST_P(DdlExactNameRegression, ExactSameNameKeepsItsExistingOutcome) {
    const ExactCase& c = GetParam();
    DdlHarness h;
    const int64_t a = h.submit(c.first, "Orders");
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    const int32_t topics = h.topicCount();
    const int64_t b = h.submit(c.second, "Orders");
    h.settle();
    EXPECT_EQ(h.response(b)->err, c.want);
    EXPECT_EQ(h.response(b)->id, h.idOf("Orders"));
    EXPECT_EQ(h.topicCount(), topics);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(EveryKindPair, DdlExactNameRegression, ::testing::ValuesIn(exactCases()),
                         [](const ::testing::TestParamInfo<ExactCase>& i) { return std::string(i.param.label); });

// Was DdlStaticCaseFoldRegression (a schema-less static topic): the
// occupant is now an UNTYPED topic created by DDL.  The exact name adopts it
// (DefineTopic disk) / is TopicExists (anything else); any case variant is
// TopicExists and never gets its own topic.
namespace {
struct UntypedCase { const char* untypedName; const char* otherName; Op op; uint16_t want; std::string label; };
void PrintTo(const UntypedCase& c, std::ostream* os) {
    *os << "untyped '" << c.untypedName << "' then " << opLabel(c.op) << " '" << c.otherName << "'";
}
std::vector<UntypedCase> untypedCases() {
    std::vector<UntypedCase> v;
    const struct { const char* s; const char* r; const char* l; } names[] = {
        {"Stat", "stat", "UpperUntyped_LowerOther"},
        {"stat", "Stat", "LowerUntyped_UpperOther"},
        {"events", "EVENTS", "LowerUntyped_CapsOther"},
        {"Player.Pos", "player.pos", "DottedUntyped"},
    };
    for (const auto& n : names)
        for (const Op op : kCreateOps)
            v.push_back({n.s, n.r, op, kTopicExists, std::string(n.l) + "_" + opLabel(op)});
    return v;
}
} // namespace

class DdlUntypedCaseFoldRegression : public ::testing::TestWithParam<UntypedCase> {};

TEST_P(DdlUntypedCaseFoldRegression, CaseVariantOfAnUntypedTopicIsTopicExists) {
    const UntypedCase& c = GetParam();
    DdlHarness h;
    const int64_t u = submitUntyped(h, c.untypedName);
    h.settle();
    ASSERT_EQ(h.response(u)->err, kOk);
    const int32_t untypedId = h.idOf(c.untypedName);
    ASSERT_GE(untypedId, 0);
    const int32_t topics = h.topicCount();
    const int64_t b = h.submit(c.op, c.otherName);
    h.settle();
    EXPECT_EQ(h.response(b)->err, c.want);
    EXPECT_EQ(h.response(b)->id, untypedId);
    EXPECT_EQ(h.topicCount(), topics);
    EXPECT_EQ(h.idOf(c.otherName), -1);
    EXPECT_EQ(h.views().byName(c.otherName), nullptr);
    EXPECT_EQ(h.schemas().byName(c.otherName), nullptr) << "no schema adopted under the variant";
    EXPECT_EQ(h.schemas().byId(untypedId), nullptr) << "the untyped topic stays untyped";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // The exact name keeps its pre-B3 outcome: a disk DefineTopic adopts the untyped
    // topic; a CreateView never adopts it; an explicit transient define over a disk
    // topic is TopicExists (final review I1).
    const int64_t exact = h.submit(c.op, c.untypedName);
    h.settle();
    EXPECT_EQ(h.response(exact)->err, c.op == Op::DefineDisk ? kOk : kTopicExists);
    EXPECT_EQ(h.response(exact)->id, untypedId);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(EveryKind, DdlUntypedCaseFoldRegression, ::testing::ValuesIn(untypedCases()),
                         [](const ::testing::TestParamInfo<UntypedCase>& i) { return i.param.label; });

TEST(DdlFixRegression, B3_CaseVariantOfATypedTopicIsTopicExists) {
    DdlHarness h;
    const int64_t a = h.submit(Op::DefineDisk, "Orders");
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    const int32_t topicsBefore = h.topicCount();
    const int64_t b = h.submit(Op::DefineDisk, "orders");
    const int64_t v = h.submit(Op::ViewPersisted, "ORDERS");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.response(b)->id, h.idOf("Orders")) << "answers the colliding topic's id";
    EXPECT_EQ(h.response(v)->err, kTopicExists);
    EXPECT_EQ(h.topicCount(), topicsBefore) << "no second topic sharing Orders' directory";
    EXPECT_EQ(h.idOf("orders"), -1);
    EXPECT_EQ(h.views().byName("ORDERS"), nullptr);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B3_CaseVariantOfAnUntypedTopicIsTopicExists) {
    DdlHarness h;
    const int64_t u = submitUntyped(h, "Stat");
    h.settle();
    ASSERT_EQ(h.response(u)->err, kOk);
    const int64_t b = h.submit(Op::DefineDisk, "stat");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.idOf("stat"), -1);
    const int64_t exact = h.submit(Op::DefineDisk, "Stat");
    h.settle();
    EXPECT_EQ(h.response(exact)->err, kOk) << "the exact name still adopts the untyped topic";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B3_CaseVariantsOfTheSourceTopicAreRefused) {
    DdlHarness h;
    const int32_t srcId = h.idOf("src");
    ASSERT_GE(srcId, 0);
    const int32_t topics = h.topicCount();
    int64_t c[4];
    int i = 0;
    for (const Op op : kCreateOps) c[i++] = h.submit(op, i % 2 ? "SRC" : "Src");
    h.settle();
    for (const int64_t corr : c) {
        EXPECT_EQ(h.response(corr)->err, kTopicExists);
        EXPECT_EQ(h.response(corr)->id, srcId);
    }
    EXPECT_EQ(h.topicCount(), topics);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B3_DistinctNamesThatOnlyShareAPrefixStillCreate) {
    DdlHarness h;
    for (const char* name : {"Orders", "Orders2", "orders_", "order", "Orders.v2"}) {
        SCOPED_TRACE(name);
        const int64_t d = h.submit(Op::DefineDisk, name);
        h.settle();
        EXPECT_EQ(h.response(d)->err, kOk);
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B3_ARefusedVariantDoesNotPoisonTheQueueBehindIt) {
    DdlHarness h;
    const int64_t a = h.submit(Op::DefineDisk, "Orders");
    const int64_t b = h.submit(Op::ViewPersisted, "orders");
    const int64_t c = h.submit(Op::DefineDisk, "fine");
    const int64_t d = h.submit(Op::ViewPersisted, "fineView");
    h.settle();
    EXPECT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.response(c)->err, kOk);
    EXPECT_EQ(h.response(d)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Was B3_OrphanPlaceholderNamesCanNeverBeAFoldTarget: no placeholder exists any
// more, but every "__"-name (a placeholder's included) is still refused 40 before
// the fold rule, and the name of a create that never committed is free in any case.
TEST(DdlFixRegression, B3_ReservedNamesAreRefusedBeforeTheFoldRuleAndAnUncommittedNameIsFree) {
    DdlHarness h;
    const int32_t wouldBe = h.state().nextTopicId();
    ASSERT_EQ(refusedAtAppend(h, Op::ViewPersisted, "orph"), kNotLeader);
    std::string upper = "__ORPHAN." + std::to_string(wouldBe);
    ASSERT_TRUE(cat::topicNamesFoldEqual(upper, "__orphan." + std::to_string(wouldBe)));
    const int32_t topics = h.topicCount();
    int64_t c[4];
    int i = 0;
    for (const Op op : kCreateOps) c[i++] = h.submit(op, upper);
    h.settle();
    for (const int64_t corr : c) EXPECT_EQ(h.response(corr)->err, kInvalidTopicName);
    EXPECT_EQ(h.topicCount(), topics);
    const int64_t again = h.submit(Op::DefineDisk, "ORPH");
    h.settle();
    EXPECT_EQ(h.response(again)->err, kOk) << "the uncommitted create's name is creatable in any case";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── B3: the TopicRegistry's folded lookup (control thread, unchanged) ──────────
TEST(DdlFixRegression, B3_TopicRegistryFoldedLookupNeverMatchesTheExactName) {
    const fs::path dir = fs::temp_directory_path() / ("rune_ddl_fold_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    {
        TopicSpec s; s.name = "Orders"; s.partitionCount = 1;
        rune::gateway::TopicRegistry reg({s}, dir.string());
        EXPECT_EQ(reg.topicIdForFoldedName("orders"), std::optional<int32_t>(0));
        EXPECT_EQ(reg.topicIdForFoldedName("ORDERS"), std::optional<int32_t>(0));
        EXPECT_EQ(reg.topicIdForFoldedName("Orders"), std::nullopt) << "exact name is not a conflict";
        EXPECT_EQ(reg.topicIdForFoldedName("other"), std::nullopt);
        EXPECT_EQ(reg.topicIdForFoldedName("orders2"), std::nullopt) << "fold equality needs equal length";
        EXPECT_EQ(reg.topicIdForFoldedName(""), std::nullopt);
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

// Was B3_ControlThreadRefusesEveryCaseVariantAndReportsTheExistingTopic: the
// catalog's fold rule (CatalogState::liveNameFoldingOnto) names the existing live
// name; the leader answers 25 with ITS id and the library's apply refuses a
// fold-colliding create on every broker.
TEST(DdlFixRegression, B3_CatalogRefusesEveryCaseVariantAndReportsTheExistingTopic) {
    struct FoldCase2 { const char* existing; const char* variant; uint8_t existingMode; uint8_t requestMode; };
    const FoldCase2 cases[] = {
        {"Orders", "orders", 0, 0}, {"orders", "Orders", 0, 1}, {"Orders", "ORDERS", 1, 0}, {"Ab.c-D_1", "aB.C-d_1", 1, 1},
    };
    for (const FoldCase2& c : cases) {
        SCOPED_TRACE(std::string(c.existing) + " vs " + c.variant);
        DdlHarness h;
        const int64_t a = h.submit(c.existingMode ? Op::DefineTransient : Op::DefineDisk, c.existing);
        h.settle();
        ASSERT_EQ(h.response(a)->err, kOk);
        const std::string* folds = h.state().liveNameFoldingOnto(rune::catalog::kDefaultNsId, c.variant);
        ASSERT_NE(folds, nullptr);
        EXPECT_EQ(*folds, c.existing);
        EXPECT_EQ(h.state().liveNameFoldingOnto(rune::catalog::kDefaultNsId, c.existing), nullptr) << "the exact name is not a conflict";
        const int64_t b = h.submit(c.requestMode ? Op::DefineTransient : Op::DefineDisk, c.variant);
        h.settle();
        EXPECT_EQ(h.response(b)->err, kTopicExists);
        EXPECT_EQ(h.response(b)->id, h.idOf(c.existing)) << "the colliding topic's id";
        EXPECT_EQ(h.topicCount(), 3) << "nothing created (src, src2, the existing one)";
        const Catalog cat = h.catalog();
        const CatalogEntry* e = findEntry(cat, c.existing);
        ASSERT_NE(e, nullptr);
        EXPECT_EQ(e->persistence, c.existingMode ? PersistenceMode::Transient : PersistenceMode::Disk)
            << "the existing topic keeps its mode";
        // The library: a fold-colliding create is refused by every broker's apply.
        const auto r = cat::previewFingerprint(h.state(), createTopicEvent(h.state(), c.variant));
        ASSERT_FALSE(r.has_value());
        EXPECT_NE(r.error().detail.find("case-folds onto"), std::string::npos) << r.error().detail;
        EXPECT_TRUE(h.violations().empty()) << h.violationText();
    }
}

// Was B3_FailedCreateAfterAFoldCheckIsInternalNotTopicExists: a refused APPEND is
// never TopicExists, and the fold answer never depends on the append — below
// minIsr the variant is still 25 (decided first), the free name the retriable 2.
TEST(DdlFixRegression, B3_FoldAnswerPrecedesTheAppendAndARefusedAppendIsNotLeader) {
    DdlHarness h;
    const int64_t a = h.submit(Op::DefineDisk, "Orders");
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    h.shrinkIsr();
    for (const Op op : kCreateOps) {
        SCOPED_TRACE(opLabel(op));
        const int64_t v = h.submit(op, "orders");
        const int64_t f = h.submit(op, std::string("free_") + opLabel(op));
        h.settle();
        EXPECT_EQ(h.response(v)->err, kTopicExists);
        EXPECT_EQ(h.response(f)->err, kNotLeader);
        EXPECT_EQ(h.idOf("orders"), -1);
    }
    h.restoreIsr();
    h.settle();
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── fix-on-sight: a REJECTED name is reported escaped (no log-line injection) ────
namespace {
struct EscapeCase { std::string raw; std::string logged; const char* label; };
std::vector<EscapeCase> escapeCases() {
    return {
        {"orders", "orders", "validNameUnchanged"},
        {"a-b_c.D9", "a-b_c.D9", "validPunctuationUnchanged"},
        {"has space", "has space", "spaceIsPrintable"},
        {"new\nline", "new\\x0aline", "newline"},
        {"cr\rlf", "cr\\x0dlf", "carriageReturn"},
        {"tab\tname", "tab\\x09name", "tab"},
        {std::string("nul\0byte", 8), "nul\\x00byte", "nul"},
        {"\x1b[31mred", "\\x1b[31mred", "ansiEscape"},
        {"del\x7f", "del\\x7f", "del"},
        {"caf\xc3\xa9", "caf\\xc3\\xa9", "nonAscii"},
        {"a\\b", "a\\\\b", "backslashDoubled"},
        {"a\\x0ab", "a\\\\x0ab", "literalEscapeTextIsNotAmbiguous"},
        {"", "", "empty"},
    };
}
void PrintTo(const EscapeCase& c, std::ostream* os) { *os << c.label; }
bool printableAscii(const std::string& s) {
    return std::all_of(s.begin(), s.end(), [](char ch) {
        const auto u = static_cast<unsigned char>(ch);
        return u >= 0x20 && u < 0x7f;
    });
}
} // namespace

class DdlLogEscapeRegression : public ::testing::TestWithParam<EscapeCase> {};

TEST_P(DdlLogEscapeRegression, EscapedNameIsPrintableAsciiOnly) {
    const EscapeCase& c = GetParam();
    const std::string got = cat::escapeNameForLog(c.raw);
    EXPECT_EQ(got, c.logged);
    for (const char ch : got) {
        const auto u = static_cast<unsigned char>(ch);
        EXPECT_TRUE(u >= 0x20 && u < 0x7f) << "byte " << static_cast<int>(u);
    }
}

// Was ControlThreadLogsTheRejectedNameOnOneEscapedLine: the client-visible detail
// the admin thread answers for a rejected name (DropTopic of an unknown topic) is
// one escaped, printable line.
TEST_P(DdlLogEscapeRegression, AdminDetailCarriesTheRejectedNameOnOneEscapedLine) {
    const EscapeCase& c = GetParam();
    if (cat::isValidTopicName(c.raw)) GTEST_SKIP() << "only rejected names are checked here";
    DdlHarness h;
    DropTopicMsg m;
    m.name = c.raw;
    const int64_t corr = h.submitRawDropTopic(std::move(m));
    h.settle();
    ASSERT_TRUE(h.response(corr).has_value());
    EXPECT_EQ(h.response(corr)->err, kUnknownTopic);
    const std::string detail = h.response(corr)->detail;
    EXPECT_EQ(detail, "no topic named '" + c.logged + "'");
    EXPECT_TRUE(printableAscii(detail)) << detail;
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(EveryByteClass, DdlLogEscapeRegression, ::testing::ValuesIn(escapeCases()),
                         [](const ::testing::TestParamInfo<EscapeCase>& i) { return std::string(i.param.label); });

TEST(DdlFixRegression, LogEscape_ForgedLogLineStaysOnTheRejectionLine) {
    DdlHarness h;
    DropTopicMsg m;
    m.name = "x\n[2026-10-12 00:00:00.000] [info] admin: granted superuser";
    const int64_t corr = h.submitRawDropTopic(std::move(m));
    h.settle();
    ASSERT_TRUE(h.response(corr).has_value());
    const std::string detail = h.response(corr)->detail;
    EXPECT_EQ(std::count(detail.begin(), detail.end(), '\n'), 0) << detail;
    EXPECT_NE(detail.find("x\\x0a[2026-10-12"), std::string::npos) << detail;
}
