// DdlHarnessRegressionTest.cpp — DDL conformance plan: smoke tests of DdlHarness
// itself (Task 4) and the targeted regression for every fix the matrices exposed
// (B1-B9, KL2: Tasks 5-7), ported to the __catalog write path by catalog history
// Task 10.  Every suite name contains "Regression".
//
// Former cells that tested the deleted control-thread create round-trip (the
// createTopicForAdmin reply ring, orphan topics / sinks and their adoption, the
// __orphan.<id> placeholders, the topic-id dir markers, the per-catalog commit
// replicas) are either re-pinned on their catalog-path counterpart (named in each
// test's comment) or ruled out in the Task 10 report.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

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

namespace {
void createOk(DdlHarness& h, Op op, const std::string& name, const std::string& src = "src") {
    const int64_t c = h.submit(op, name, src);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kOk) << opName(op) << " '" << name << "'";
}
/// An UNTYPED topic (an empty descriptor) created by DDL — the former static
/// schema-less RUNE_TOPICS topic.
int64_t submitUntyped(DdlHarness& h, const std::string& name, uint8_t persistence = 0) {
    DefineTopicMsg m;
    m.name = name; m.partitionCount = 1; m.persistence = persistence;
    return h.submitRawDefine(std::move(m));
}
/// `op` on `name` while the ISR is below the catalog's minIsr: its append is
/// refused (NotLeader, nothing appended) — the catalog counterpart of a create
/// that never committed.  Returns its answer code.
uint16_t refusedAtAppend(DdlHarness& h, Op op, const std::string& name) {
    h.shrinkIsr();
    const int64_t c = h.submit(op, name);
    h.settle();
    h.restoreIsr();
    h.settle();
    return h.response(c) ? h.response(c)->err : uint16_t{0xFFFF};
}
} // namespace

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
    EXPECT_FALSE(fs::exists(h.root() / "topics")) << "the admin path writes no topic dir at all";
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

// Was DdlStaticCaseFoldRegression (a schema-less static RUNE_TOPICS topic): the
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
        const std::string* folds = h.state().liveNameFoldingOnto(c.variant);
        ASSERT_NE(folds, nullptr);
        EXPECT_EQ(*folds, c.existing);
        EXPECT_EQ(h.state().liveNameFoldingOnto(c.existing), nullptr) << "the exact name is not a conflict";
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
    AdminWorker w(reg, nullptr, 1, {1}, std::move(d), nullptr, &views, /*catalogReplica=*/nullptr);
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

namespace {
bool tickUntilAnswered(DdlHarness& h, int64_t corr, bool releaseCommits) {
    for (int i = 0; i < 50 && !h.response(corr).has_value(); ++i) {
        if (releaseCommits) h.releaseCommits();
        h.tick();
    }
    return h.response(corr).has_value();
}
void expectIdle(DdlHarness& h, const char* where) {
    EXPECT_EQ(h.worker().inFlightStateName(), "Idle") << where;
    EXPECT_FALSE(h.worker().hasInFlight()) << where;
}
DdlHarnessOptions heldCommits() {
    DdlHarnessOptions o;
    o.holdCommits = true;
    return o;
}
}  // namespace

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

// ── KL1 follow-up, on the catalog path: there is no orphan to adopt ───────────
// Was Optional_CreateViewRetryAdoptsItsOwnOrphanSink: a CREATE VIEW answered
// NotLeader by a demotion is retried with its requestId — answered the ORIGINAL
// result (the same sink), nothing appended twice (F14 dedupe replaces adoption).
TEST(DdlFixRegression, Optional_CreateViewRetryAfterNotLeaderGetsItsOriginalSink) {
    DdlHarness h(heldCommits());
    CreateViewMsg m;
    m.viewName = "v"; m.keyField = "id";
    m.planBlob = viewPlanBlob("v", h.idOf("src"), 0);
    m.meta = h.rig().meta("CREATE VIEW v");
    const CreateViewMsg retry = m;
    const int64_t a = h.submitRawView(std::move(m));
    ASSERT_TRUE(h.tickUntilAppended());
    h.demote();
    ASSERT_EQ(h.response(a)->err, kNotLeader);
    h.promote();
    h.settle();
    const int32_t sink = h.idOf("v");
    ASSERT_GE(sink, 0) << "the event committed under the re-promoted leader";
    const uint64_t events = h.eventsAppended();
    const int64_t b = h.submitRawView(retry);
    h.settle();
    EXPECT_EQ(h.response(b)->err, kOk) << "the retry gets the original result";
    EXPECT_EQ(h.response(b)->id, sink);
    EXPECT_EQ(h.eventsAppended(), events) << "deduped";
    EXPECT_NE(h.views().byName("v"), nullptr);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Was Optional_AViewNeverAdoptsAnOrphanDefineOrAStaticCompactTopic: a CREATE VIEW
// whose name is taken by ANY topic — untyped (the former static / orphan) or typed —
// is TopicExists; a view never takes over a topic.
TEST(DdlFixRegression, Optional_AViewNeverTakesOverAnExistingTopic) {
    DdlHarness h;
    const int64_t u = submitUntyped(h, "untyped_sink");
    h.settle();
    ASSERT_EQ(h.response(u)->err, kOk);
    for (const char* name : {"untyped_sink", "src"}) {
        SCOPED_TRACE(name);
        const int64_t v = h.submit(Op::ViewPersisted, name);
        h.settle();
        EXPECT_EQ(h.response(v)->err, kTopicExists);
        EXPECT_EQ(h.response(v)->id, h.idOf(name));
        EXPECT_EQ(h.views().byName(name), nullptr);
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// Was Optional_AnAdoptedSinkIsCommittedAndSurvivesRestart: the view whose CREATE
// was answered NotLeader (demotion) and then deduped survives a restart under its
// one sink id; a third CREATE (a NEW requestId) is ViewExists.
TEST(DdlFixRegression, Optional_ADedupedViewSurvivesRestartUnderItsOneSinkId) {
    DdlHarness h(heldCommits());
    const int64_t a = h.submit(Op::ViewPersisted, "v");
    ASSERT_TRUE(h.tickUntilAppended());
    h.demote();
    ASSERT_EQ(h.response(a)->err, kNotLeader);
    h.promote();
    h.settle();
    const int32_t sink = h.idOf("v");
    ASSERT_GE(sink, 0);
    const Catalog before = h.catalog();
    ASSERT_NE(findEntry(before, "v"), nullptr);
    EXPECT_EQ(findEntry(before, "v")->kind, EntryKind::View);
    h.restart();
    EXPECT_EQ(h.catalog(), before);
    EXPECT_EQ(h.idOf("v"), sink);
    ASSERT_NE(h.views().byName("v"), nullptr);
    EXPECT_EQ(h.views().byName("v")->viewTopicId, sink);
    const int64_t c = h.submit(Op::ViewPersisted, "v");
    h.settle();
    EXPECT_EQ(h.response(c)->err, kViewExists);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── Task 9 re-review follow-up: a deduped CREATE VIEW whose output fingerprint
//    cannot be recovered is answered Internal, never OK with a made-up fingerprint.
namespace {
/// Flip one byte inside the record batch holding `offset` in the catalog's segment
/// files: its CRC no longer verifies, so any later read of it fails.
bool corruptCatalogBatchAt(const std::string& dir, int64_t offset) {
    for (const auto& f : fs::directory_iterator(dir)) {
        if (f.path().extension() != ".log") continue;
        std::fstream io(f.path(), std::ios::in | std::ios::out | std::ios::binary);
        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(io)), std::istreambuf_iterator<char>());
        std::size_t pos = 0;
        while (pos < bytes.size()) {
            rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
            if (!r.ensureValid()) break;
            const auto tl = static_cast<std::size_t>(r.totalLength());
            bool holds = false;
            r.forEachRecord([&](std::string_view, std::string_view, int64_t off, int64_t) { holds = holds || off == offset; });
            if (holds) {
                const std::size_t at = pos + tl - 1;   // the last byte of its records
                io.clear();
                io.seekp(static_cast<std::streamoff>(at));
                const char flipped = static_cast<char>(bytes[at] ^ 0xFF);
                io.write(&flipped, 1);
                io.flush();
                return static_cast<bool>(io);
            }
            if (tl == 0) break;
            pos += tl;
        }
    }
    return false;
}
} // namespace

TEST(DdlFixRegression, DedupedCreateViewWhoseFingerprintIsUnrecoverableIsInternalRegression) {
    DdlHarness h;
    CreateViewMsg m;
    m.viewName = "v"; m.keyField = "id";
    m.planBlob = viewPlanBlob("v", h.idOf("src"), 0);
    m.meta = h.rig().meta("CREATE VIEW v");
    const CreateViewMsg retry = m;
    const int64_t a = h.submitRawView(std::move(m));
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    const int64_t createdAt = h.catalogLogEnd() - 1;
    // The view moves on (ALTER VIEW: version 2), so a retry's original output
    // fingerprint must be re-read from the CreateView event in the log ...
    const int64_t alt = h.submit(Op::AlterView, "v");
    h.settle();
    ASSERT_EQ(h.response(alt)->err, kOk);
    // ... and that event no longer reads back.
    ASSERT_TRUE(corruptCatalogBatchAt(h.catalogDir(), createdAt));
    const uint64_t events = h.eventsAppended();
    const int64_t b = h.submitRawView(retry);
    h.settle();
    ASSERT_TRUE(h.response(b).has_value()) << "answered, never dropped";
    EXPECT_EQ(h.response(b)->err, kInternal) << "never an OK carrying a made-up fingerprint";
    EXPECT_EQ(h.eventsAppended(), events) << "a dedupe appends nothing";
    EXPECT_FALSE(h.worker().applier().halted()) << "the applier already applied it: it keeps applying";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}
