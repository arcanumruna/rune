// DdlHarnessRegressionTest.cpp — DDL conformance plan: smoke tests of DdlHarness
// itself (Task 4) and the targeted regression for every fix the matrices exposed
// (B1-B4, KL2: Tasks 5-7).  Every suite name contains "Regression".
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <rune/codec/AlterOps.h>
#include <rune/node/TopicName.h>

#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;

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

TEST(DdlHarnessRegression, HeldControlReplyKeepsTheNextOpQueued) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t x = h.submit(Op::DefineDisk, "x");
    const int64_t y = h.submit(Op::DefineDisk, "y");
    ASSERT_TRUE(h.tickUntilCreatePending());
    const int reqs = h.createRequests();
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_EQ(h.createRequests(), reqs) << "y must not start while x is AwaitingId";
    EXPECT_FALSE(h.response(y).has_value());
    h.deliverCreate();
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
    EXPECT_FALSE(h.response(a).has_value()) << "an op is answered only after its record commits";
    EXPECT_TRUE(h.worker().hasInFlight());
    h.settle();   // releases commits
    EXPECT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.response(a)->version, 2u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlHarnessRegression, OrphanThenLaterTopicRestartsWithAPlaceholder) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t o = h.submit(Op::ViewPersisted, "orph");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.failCreate(/*afterCreate=*/true);
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_EQ(h.response(o)->err, kInternal);
    const int32_t orphanId = h.idOf("orph");
    ASSERT_GE(orphanId, 0) << "the control thread created the sink: an orphan";
    const int64_t l = h.submit(Op::DefineDisk, "later");
    h.settle();
    ASSERT_EQ(h.response(l)->err, kOk);
    const Catalog before = h.catalog();
    h.restart();   // B5: must not throw
    EXPECT_EQ(h.catalog(), h.expectedAfterRestart(before));
    EXPECT_EQ(h.idOf("orph"), -1) << "the orphan's name is free after the restart";
    EXPECT_EQ(h.idOf(orphanPlaceholderName(orphanId)), orphanId);
    EXPECT_EQ(h.topics().topic(orphanId)->partitionCount, 0) << "hidden, inert placeholder (D1)";
    EXPECT_FALSE(fs::exists(h.root() / "topics" / "orph")) << "the orphan sink's data dir is deleted (B10)";
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
    // A second answer for an already-answered op: nothing is started-but-unanswered.
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
    // An answer for an EARLIER submission after a later one was answered.
    h.injectAnswerForCheckerSelfTest(a, kOk);
    ASSERT_FALSE(h.violations().empty()) << "a misordered answer must be flagged";
    EXPECT_TRUE(anyViolationContains(h, "answer for corr " + std::to_string(a) + " out of FIFO order (previous " +
                                         std::to_string(b) + ")")) << h.violationText();
}

TEST(DdlHarnessRegression, InvariantFlagsAnAnswerWhileAnotherOpIsInFlight) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t x = h.submit(Op::DefineDisk, "x");
    ASSERT_TRUE(h.tickUntilCreatePending());
    ASSERT_TRUE(h.worker().hasInFlight());
    ASSERT_TRUE(h.violations().empty()) << h.violationText();
    // x is AwaitingId; an answer that is not x's leaves x started-but-unanswered
    // while the worker still reports it in flight: a second, phantom op.
    h.injectAnswerForCheckerSelfTest(x + 1000, kOk);
    h.injectAnswerForCheckerSelfTest(x + 1001, kOk);
    ASSERT_FALSE(h.violations().empty());
    EXPECT_TRUE(anyViolationContains(h, "injected answer: 0 DDL ops started-but-unanswered, in flight 1"))
        << "the post-step check catches what the per-answer check cannot: " << h.violationText();
    EXPECT_TRUE(anyViolationContains(h, "injected answer: -1 DDL ops started-but-unanswered, in flight 1"))
        << h.violationText();
    EXPECT_TRUE(anyViolationContains(h, "with 0 ops started-but-unanswered (want 1)")) << h.violationText();
    h.deliverCreate();   // let x finish so the harness tears down cleanly
    h.setControl(ControlMode::Auto);
    h.settle();
}

// ── B1 / B2 / B6 ──────────────────────────────────────────────────────────────
// Every TopicNameError kind, sampled several ways each, on every path: DefineTopic
// (disk + transient), CreateView (persisted + transient), createTopicForAdmin (the
// control thread).  The config-parse path is in NodeConfigTest.cpp (Task 5a).
namespace {
struct BadName {
    std::string   name;
    TopicNameError kind;
    const char*   label;
};
std::vector<BadName> badNames() {
    return {
        {"", TopicNameError::Empty, "empty"},
        {std::string(250, 'x'), TopicNameError::TooLong, "len250"},
        {std::string(1000, 'y'), TopicNameError::TooLong, "len1000"},
        {".", TopicNameError::DotSegment, "dot"},
        {"..", TopicNameError::DotSegment, "dotdot"},
        {"../escape", TopicNameError::BadChar, "traversal"},
        {"a/b", TopicNameError::BadChar, "slash"},
        {"/abs", TopicNameError::BadChar, "absolute"},
        {"a\\b", TopicNameError::BadChar, "backslash"},
        {"has space", TopicNameError::BadChar, "space"},
        {"tab\tname", TopicNameError::BadChar, "tab"},
        {"new\nline", TopicNameError::BadChar, "newline"},
        {std::string("nul\0byte", 8), TopicNameError::BadChar, "nul"},
        {"caf\xc3\xa9", TopicNameError::BadChar, "nonascii"},
        {"a:b", TopicNameError::BadChar, "colon"},
        {"__topic_schemas", TopicNameError::Reserved, "topic_schemas"},
        {"__views", TopicNameError::Reserved, "views"},
        {"__consumer_offsets", TopicNameError::Reserved, "consumer_offsets"},
        {"__transaction_state", TopicNameError::Reserved, "transaction_state"},
        {"__cluster_metadata", TopicNameError::Reserved, "cluster_metadata"},
        {"__view_snapshots", TopicNameError::Reserved, "view_snapshots"},
        {"__orphan.1", TopicNameError::Reserved, "orphan"},
        {"__x", TopicNameError::Reserved, "x"},
        {"__", TopicNameError::Reserved, "bare"},
    };
}
} // namespace

class DdlNameRegression : public ::testing::TestWithParam<BadName> {};

TEST_P(DdlNameRegression, TableCoversTheKindItClaims) {
    EXPECT_EQ(checkTopicName(GetParam().name), GetParam().kind);
}

TEST_P(DdlNameRegression, AdminPathRejectsEveryOpWithInvalidTopicNameAndCreatesNothing) {
    const BadName& b = GetParam();
    DdlHarness h;
    const int32_t topics = h.topicCount();
    const int64_t schemaEnd = h.schemaLogEnd(), viewsEnd = h.viewsLogEnd();
    const int reqs = h.createRequests();
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
    EXPECT_EQ(h.createRequests(), reqs) << "never reaches the control thread";
    EXPECT_EQ(h.schemaLogEnd(), schemaEnd);
    EXPECT_EQ(h.viewsLogEnd(), viewsEnd);
    EXPECT_FALSE(fs::exists(h.root() / "escape")) << "nothing written outside dataDir/topics";
    EXPECT_FALSE(fs::exists(h.root() / "topics" / "a")) << "no partial path";
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

TEST_P(DdlNameRegression, ControlThreadRefusesTheNameDefensively) {
    const BadName& b = GetParam();
    const fs::path dir = fs::temp_directory_path() / ("rune_ddl_ctl_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    {
        rune::gateway::TopicRegistry reg({}, dir.string());
        CreateTopicReqMsg req; req.ticket = 1; req.partitionCount = 1; req.name = b.name;
        for (const uint8_t persistence : {uint8_t{0}, uint8_t{1}}) {
            req.persistence = persistence;
            const CreateTopicReplyMsg r = createTopicForAdmin(reg, req);
            EXPECT_EQ(r.topicId, -1);
            EXPECT_FALSE(r.existed);
            EXPECT_EQ(r.ticket, 1);
        }
        EXPECT_EQ(reg.topicCount(), 0);
    }
    EXPECT_FALSE(fs::exists(dir / "escape"));
    std::error_code ec; fs::remove_all(dir, ec);
}

INSTANTIATE_TEST_SUITE_P(EveryKind, DdlNameRegression, ::testing::ValuesIn(badNames()),
                         [](const ::testing::TestParamInfo<BadName>& i) { return std::string(i.param.label); });

TEST(DdlFixRegression, B1_EveryTopicNameErrorKindIsCoveredByTheTable) {
    bool seen[6] = {};
    for (const BadName& b : badNames()) seen[static_cast<int>(b.kind)] = true;
    EXPECT_FALSE(seen[static_cast<int>(TopicNameError::None)]);
    for (const TopicNameError k : {TopicNameError::Empty, TopicNameError::TooLong, TopicNameError::DotSegment,
                                   TopicNameError::BadChar, TopicNameError::Reserved})
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

TEST(DdlFixRegression, B6_PartitionCountBelowOneIsInvalidSchemaOnAdminAndControl) {
    DdlHarness h;
    const int32_t topicsBefore = h.topicCount();
    const int reqsBefore = h.createRequests();
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
    EXPECT_EQ(h.createRequests(), reqsBefore);
    EXPECT_EQ(h.idOf("zero_parts"), -1);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // One partition is the floor and still works.
    DefineTopicMsg ok;
    ok.partitionCount = 1; ok.name = "one_part"; ok.keyField = "id"; ok.blob = charBlob();
    const int64_t c = h.submitRawDefine(std::move(ok));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kOk);

    // Control thread: same refusal, nothing created.
    const fs::path dir = fs::temp_directory_path() / ("rune_ddl_ctl0_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    {
        rune::gateway::TopicRegistry reg({}, dir.string());
        for (const int32_t parts : {0, -1, INT32_MIN}) {
            CreateTopicReqMsg zero; zero.ticket = 2; zero.partitionCount = parts; zero.name = "ok";
            EXPECT_EQ(createTopicForAdmin(reg, zero).topicId, -1) << parts;
        }
        EXPECT_EQ(reg.topicCount(), 0);
    }
    std::error_code ec; fs::remove_all(dir, ec);
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
// control thread (owner of TopicRegistry) decides; the admin thread answers
// TopicExists (25) with the colliding topic's id and nothing is created.
//
// Fold matrix: {DefineDisk, DefineTransient, ViewPersisted, ViewTransient} for the
// existing name × the same four for the case variant (topic×topic, topic×view,
// view×topic, view×view) × several spellings in both directions.
namespace {
struct FoldPair { const char* existing; const char* variant; const char* label; };
const FoldPair kFoldPairs[] = {
    {"Orders", "orders", "UpperThenLower"},
    {"orders", "Orders", "LowerThenUpper"},
    {"orders", "ORDERS", "LowerThenAllCaps"},
    {"Ab.c-D_1", "aB.C-d_1", "MixedWithPunctuation"},
};
const Op kCreateOps[] = {Op::DefineDisk, Op::DefineTransient, Op::ViewPersisted, Op::ViewTransient};

const char* opLabel(Op op) {
    switch (op) {
        case Op::DefineDisk:      return "DefineDisk";
        case Op::DefineTransient: return "DefineTransient";
        case Op::ViewPersisted:   return "ViewPersisted";
        case Op::ViewTransient:   return "ViewTransient";
        default:                  return "Other";
    }
}
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
    const int64_t schemaEnd = h.schemaLogEnd(), viewsEnd = h.viewsLogEnd();
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
    EXPECT_EQ(h.schemaLogEnd(), schemaEnd) << "no schema record for the variant";
    EXPECT_EQ(h.viewsLogEnd(), viewsEnd) << "no view record for the variant";
    if (isViewOp(c.first)) EXPECT_NE(h.views().byName(c.names.existing), nullptr) << "the existing view is intact";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // The refusal leaves nothing behind that a restart could resurrect.
    const Catalog before = h.catalog();
    h.restart();
    EXPECT_EQ(h.catalog(), h.expectedAfterRestart(before));
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

// Runtime DDL against a schema-less static (RUNE_TOPICS) topic: the exact name
// adopts it (DefineTopic) / is TopicExists (CreateView); any case variant is
// TopicExists and never gets its own topic.
namespace {
struct StaticCase { const char* staticName; const char* runtimeName; Op op; uint16_t want; std::string label; };
void PrintTo(const StaticCase& c, std::ostream* os) {
    *os << "static '" << c.staticName << "' then " << opLabel(c.op) << " '" << c.runtimeName << "'";
}
std::vector<StaticCase> staticCases() {
    std::vector<StaticCase> v;
    const struct { const char* s; const char* r; const char* l; } names[] = {
        {"Stat", "stat", "UpperStatic_LowerRuntime"},
        {"stat", "Stat", "LowerStatic_UpperRuntime"},
        {"events", "EVENTS", "LowerStatic_CapsRuntime"},
        {"Player.Pos", "player.pos", "DottedStatic"},
    };
    for (const auto& n : names) {
        for (const Op op : kCreateOps)
            v.push_back({n.s, n.r, op, kTopicExists, std::string(n.l) + "_" + opLabel(op)});
    }
    return v;
}
} // namespace

class DdlStaticCaseFoldRegression : public ::testing::TestWithParam<StaticCase> {};

TEST_P(DdlStaticCaseFoldRegression, CaseVariantOfAStaticTopicIsTopicExists) {
    const StaticCase& c = GetParam();
    DdlHarnessOptions o;
    TopicSpec s; s.name = c.staticName; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);
    const int32_t staticId = h.idOf(c.staticName);
    ASSERT_GE(staticId, 0);
    const int32_t topics = h.topicCount();
    const int64_t b = h.submit(c.op, c.runtimeName);
    h.settle();
    EXPECT_EQ(h.response(b)->err, c.want);
    EXPECT_EQ(h.response(b)->id, staticId);
    EXPECT_EQ(h.topicCount(), topics);
    EXPECT_EQ(h.idOf(c.runtimeName), -1);
    EXPECT_EQ(h.views().byName(c.runtimeName), nullptr);
    EXPECT_EQ(h.schemas().byName(c.runtimeName), nullptr) << "no schema adopted under the variant";
    EXPECT_EQ(h.schemas().byId(staticId), nullptr) << "the static topic stays schema-less";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    // The exact name keeps its pre-B3 outcome.
    const int64_t exact = h.submit(c.op, c.staticName);
    h.settle();
    // Pre-B3 outcomes: a disk DefineTopic adopts the schema-less static topic; a
    // CreateView never adopts it; an explicit transient define over a disk topic is
    // TopicExists (final review I1).
    EXPECT_EQ(h.response(exact)->err, c.op == Op::DefineDisk ? kOk : kTopicExists);
    EXPECT_EQ(h.response(exact)->id, staticId);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(EveryKind, DdlStaticCaseFoldRegression, ::testing::ValuesIn(staticCases()),
                         [](const ::testing::TestParamInfo<StaticCase>& i) { return i.param.label; });

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

TEST(DdlFixRegression, B3_CaseVariantOfASchemaLessStaticTopicIsTopicExists) {
    DdlHarnessOptions o;
    TopicSpec s; s.name = "Stat"; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);
    const int64_t b = h.submit(Op::DefineDisk, "stat");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kTopicExists);
    EXPECT_EQ(h.idOf("stat"), -1);
    const int64_t exact = h.submit(Op::DefineDisk, "Stat");
    h.settle();
    EXPECT_EQ(h.response(exact)->err, kOk) << "the exact name still adopts the static topic";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B3_CaseVariantsOfTheSourceTopicAreRefused) {
    // The harness's typed source "src": SRC / Src never become new topics or views.
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

TEST(DdlFixRegression, B3_OrphanPlaceholderNamesCanNeverBeAFoldTarget) {
    // __orphan.<id> placeholders are hidden, and every name folding onto one starts
    // with "__" — refused InvalidTopicName (40) before the fold rule is reached.
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t o = h.submit(Op::ViewPersisted, "orph");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.failCreate(/*afterCreate=*/true);
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_EQ(h.response(o)->err, kInternal);
    const int32_t orphanId = h.idOf("orph");
    const int64_t l = h.submit(Op::DefineDisk, "later");   // a later id keeps the orphan's slot
    h.settle();
    ASSERT_EQ(h.response(l)->err, kOk);
    h.restart();
    const std::string placeholder = orphanPlaceholderName(orphanId);
    ASSERT_EQ(h.idOf(placeholder), orphanId);
    std::string upper = placeholder;
    for (char& ch : upper) ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    ASSERT_NE(upper, placeholder);
    ASSERT_TRUE(topicNamesFoldEqual(upper, placeholder));
    const int32_t topics = h.topicCount();
    int64_t c[4];
    int i = 0;
    for (const Op op : kCreateOps) c[i++] = h.submit(op, upper);
    h.settle();
    for (const int64_t corr : c) EXPECT_EQ(h.response(corr)->err, kInvalidTopicName);
    EXPECT_EQ(h.topicCount(), topics);
    // And the orphan's freed user name is creatable again in any case.
    const int64_t again = h.submit(Op::DefineDisk, "ORPH");
    h.settle();
    EXPECT_EQ(h.response(again)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── B3: the control-thread half (createTopicForAdmin) and the reply ring ──────
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

namespace {
struct ControlFoldCase { const char* existing; const char* variant; uint8_t existingMode; uint8_t requestMode; int32_t parts; };
} // namespace

TEST(DdlFixRegression, B3_ControlThreadRefusesEveryCaseVariantAndReportsTheExistingTopic) {
    const ControlFoldCase cases[] = {
        {"Orders", "orders", 0, 0, 3}, {"orders", "Orders", 0, 1, 2}, {"Orders", "ORDERS", 1, 0, 1},
        {"Ab.c-D_1", "aB.C-d_1", 1, 1, 4},
    };
    for (const ControlFoldCase& c : cases) {
        SCOPED_TRACE(std::string(c.existing) + " vs " + c.variant);
        const fs::path dir = fs::temp_directory_path() / ("rune_ddl_ctlfold_" + std::to_string(::getpid()));
        fs::create_directories(dir);
        {
            rune::gateway::TopicRegistry reg({}, dir.string());
            CreateTopicReqMsg first; first.ticket = 1; first.name = c.existing; first.partitionCount = c.parts;
            first.persistence = c.existingMode;
            const CreateTopicReplyMsg r1 = createTopicForAdmin(reg, first);
            ASSERT_GE(r1.topicId, 0);
            EXPECT_FALSE(r1.caseConflict);

            CreateTopicReqMsg variant; variant.ticket = 2; variant.name = c.variant; variant.partitionCount = 7;
            variant.persistence = c.requestMode;
            const CreateTopicReplyMsg r2 = createTopicForAdmin(reg, variant);
            EXPECT_EQ(r2.ticket, 2);
            EXPECT_EQ(r2.topicId, r1.topicId) << "the colliding topic's id";
            EXPECT_TRUE(r2.existed);
            EXPECT_TRUE(r2.caseConflict);
            EXPECT_EQ(r2.partitionCount, c.parts) << "the existing topic's count, never the request's";
            EXPECT_EQ(r2.persistence, c.existingMode) << "the existing topic's mode";
            EXPECT_EQ(reg.topicCount(), 1) << "nothing created";
            EXPECT_EQ(reg.topicIdForName(c.variant), std::nullopt);

            // The exact name is the pre-B3 idempotent path: existed, no conflict.
            const CreateTopicReplyMsg r3 = createTopicForAdmin(reg, first);
            EXPECT_EQ(r3.topicId, r1.topicId);
            EXPECT_TRUE(r3.existed);
            EXPECT_FALSE(r3.caseConflict);
        }
        std::error_code ec; fs::remove_all(dir, ec);
    }
}

TEST(DdlFixRegression, B3_CaseConflictCrossesTheReplyRing) {
    OneToOneByteRing ring{kCreateTopicRingBytes};
    CreateTopicReplyMsg m;
    m.ticket = 5; m.topicId = 3; m.partitionCount = 2; m.existed = true; m.persistence = 0; m.caseConflict = true;
    ASSERT_TRUE(encodeCreateTopicReply(ring, m));
    CreateTopicReplyMsg plain = m;
    plain.caseConflict = false;
    ASSERT_TRUE(encodeCreateTopicReply(ring, plain));
    int frames = 0;
    ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                   aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        const std::size_t n = static_cast<std::size_t>(len);
        ASSERT_EQ(n, 19u) << "the B3 trailer is byte 18";
        const CreateTopicReplyMsg got = decodeCreateTopicReply(p, n);
        EXPECT_EQ(got.caseConflict, frames == 0);
        EXPECT_EQ(got.ticket, 5);
        EXPECT_EQ(got.topicId, 3);
        EXPECT_EQ(got.partitionCount, 2);
        EXPECT_TRUE(got.existed);
        EXPECT_EQ(got.persistence, 0);
        const CreateTopicReplyMsg old = decodeCreateTopicReply(p, n - 1);
        EXPECT_FALSE(old.caseConflict) << "a pre-B3 frame decodes as no conflict";
        EXPECT_EQ(old.persistence, 0) << "and keeps every earlier field";
        ++frames;
    }, 4);
    EXPECT_EQ(frames, 2);
}

TEST(DdlFixRegression, B3_FailedCreateAfterAFoldCheckIsInternalNotTopicExists) {
    // The harness's failCreate overwrites the reply with a failure: a stale
    // caseConflict flag must not survive it (else the admin thread would answer 25).
    DdlHarness h;
    const int64_t a = h.submit(Op::DefineDisk, "Orders");
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    for (const Op op : kCreateOps) {
        SCOPED_TRACE(opLabel(op));
        h.setControl(ControlMode::Hold);
        const int64_t b = h.submit(op, "orders");
        ASSERT_TRUE(h.tickUntilCreatePending());
        h.failCreate(/*afterCreate=*/true);
        h.setControl(ControlMode::Auto);
        h.settle();
        EXPECT_EQ(h.response(b)->err, kInternal);
        EXPECT_EQ(h.idOf("orders"), -1);
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── fix-on-sight: a REJECTED name is logged escaped (no log-line injection) ────
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

struct LogCapture {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink =
        std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(64);
    std::shared_ptr<spdlog::logger> prev = spdlog::default_logger();
    LogCapture() {
        auto lg = std::make_shared<spdlog::logger>("capture", sink);
        lg->set_level(spdlog::level::trace);
        lg->set_pattern("%v");
        spdlog::set_default_logger(lg);
    }
    ~LogCapture() { spdlog::set_default_logger(prev); }
    LogCapture(const LogCapture&) = delete;
    LogCapture& operator=(const LogCapture&) = delete;
};
} // namespace

class DdlLogEscapeRegression : public ::testing::TestWithParam<EscapeCase> {};

TEST_P(DdlLogEscapeRegression, EscapedNameIsPrintableAsciiOnly) {
    const EscapeCase& c = GetParam();
    const std::string got = escapeNameForLog(c.raw);
    EXPECT_EQ(got, c.logged);
    for (const char ch : got) {
        const auto u = static_cast<unsigned char>(ch);
        EXPECT_TRUE(u >= 0x20 && u < 0x7f) << "byte " << static_cast<int>(u);
    }
}

TEST_P(DdlLogEscapeRegression, ControlThreadLogsTheRejectedNameOnOneEscapedLine) {
    const EscapeCase& c = GetParam();
    if (isValidTopicName(c.raw)) GTEST_SKIP() << "only rejected names are logged here";
    const fs::path dir = fs::temp_directory_path() / ("rune_ddl_logesc_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    {
        rune::gateway::TopicRegistry reg({}, dir.string());
        CreateTopicReqMsg req; req.ticket = 1; req.partitionCount = 1; req.name = c.raw;
        std::vector<std::string> lines;
        {
            LogCapture cap;
            EXPECT_EQ(createTopicForAdmin(reg, req).topicId, -1);
            lines = cap.sink->last_formatted();
        }
        ASSERT_EQ(lines.size(), 1u);
        const std::string& line = lines[0];
        EXPECT_NE(line.find("name='" + c.logged + "'"), std::string::npos) << line;
        const std::string body = line.substr(0, line.find_last_not_of("\r\n") + 1);
        for (const char ch : body) {
            const auto u = static_cast<unsigned char>(ch);
            EXPECT_TRUE(u >= 0x20 && u < 0x7f) << "raw byte " << static_cast<int>(u) << " in: " << body;
        }
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

INSTANTIATE_TEST_SUITE_P(EveryByteClass, DdlLogEscapeRegression, ::testing::ValuesIn(escapeCases()),
                         [](const ::testing::TestParamInfo<EscapeCase>& i) { return std::string(i.param.label); });

TEST(DdlFixRegression, LogEscape_ForgedLogLineStaysOnTheRejectionLine) {
    const fs::path dir = fs::temp_directory_path() / ("rune_ddl_logforge_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    {
        rune::gateway::TopicRegistry reg({}, dir.string());
        CreateTopicReqMsg req; req.ticket = 1; req.partitionCount = 1;
        req.name = "x\n[2026-10-12 00:00:00.000] [info] admin: granted superuser";
        LogCapture cap;
        (void)createTopicForAdmin(reg, req);
        const std::vector<std::string> lines = cap.sink->last_formatted();
        ASSERT_EQ(lines.size(), 1u);
        EXPECT_EQ(std::count(lines[0].begin(), lines[0].end(), '\n'), 1) << "only the line terminator: " << lines[0];
        EXPECT_NE(lines[0].find("x\\x0a[2026-10-12"), std::string::npos) << lines[0];
    }
    std::error_code ec; fs::remove_all(dir, ec);
}

// ── B4 / KL2 / re-promotion ──────────────────────────────────────────────────
TEST(DdlFixRegression, B4_DemotionAnswersAnInFlightCreateImmediately) {
    DdlHarness h;
    h.setControl(ControlMode::Hold);
    const int64_t a = h.submit(Op::DefineDisk, "n");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.demote();
    ASSERT_TRUE(h.response(a).has_value()) << "answered on demotion, not after the 30 s timeout";
    EXPECT_EQ(h.response(a)->err, kInternal);
    EXPECT_FALSE(h.worker().hasInFlight());
    h.deliverCreate();   // the control thread's reply arrives late: a stale ticket
    EXPECT_EQ(h.schemas().byName("n"), nullptr) << "never applied";
    h.promote();
    h.setControl(ControlMode::Auto);
    const int64_t b = h.submit(Op::DefineDisk, "n");
    h.settle();
    EXPECT_EQ(h.response(b)->err, kOk) << "the retry adopts the orphan (SchemaLessDisk row)";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B4_DemotionAnswersAnInFlightCommitAndNeverAppliesIt) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    h.demote();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kInternal);
    for (int i = 0; i < 10; ++i) h.tick();
    EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "a follower never commits an op off its own HW";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST(DdlFixRegression, B4_RepromotionKeepsTheCatalogExact) {
    DdlHarness h;
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    h.settle();
    ASSERT_EQ(h.response(v)->err, kOk);
    const Catalog before = h.catalog();
    const std::size_t views = h.views().all().size();
    h.demote();
    h.promote();
    EXPECT_EQ(h.catalog(), before) << "promotion replays onto un-cleared registries: no duplicates, no loss";
    EXPECT_EQ(h.views().all().size(), views);
    const int64_t d = h.submit(Op::DefineDisk, "after");
    h.settle();
    EXPECT_EQ(h.response(d)->err, kOk);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// KNOWN LIMITATION KL2: an op answered Internal after its record was appended is in
// doubt.  The live registry never applies it; its committed record is replayed on
// restart.  Pinned so any change to this behaviour is deliberate.
TEST(DdlFixRegression, KL2_TimeoutInAwaitingCommitIsInDoubtAndReplayedOnRestart) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int64_t a = h.submit(Op::AlterAdd, "src");
    ASSERT_TRUE(h.tickUntilAppended());
    h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
    h.tick();
    ASSERT_TRUE(h.response(a).has_value());
    EXPECT_EQ(h.response(a)->err, kInternal);
    h.releaseCommits();
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "KL2: the live registry never applies it";
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->version, 2u) << "KL2: its committed record is replayed on restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── B4 matrix: demotion at EVERY async state of an in-flight DDL op ───────────
// Define / CreateView AwaitingId (the id / sink request is with the control
// thread), Define / CreateView / Alter AwaitingCommit (the record is appended,
// the HW does not cover it yet).  For each: an immediate Internal; nothing
// applied live (not on the late reply, not after the HW later covers the
// record, not on re-promotion); a demoted worker starts nothing (every op it
// dequeues is refused Internal at the leader guard: no create request, no
// append); after re-promotion the queue runs in FIFO order.  After a restart an
// AwaitingId op is still absent; an AwaitingCommit op is in doubt (KL2): its
// appended record is replayed.
enum class B4State : uint8_t {
    DefineAwaitingId, ViewAwaitingId, DefineAwaitingCommit, ViewAwaitingCommit, AlterAwaitingCommit,
};

[[nodiscard]] static const char* b4StateName(B4State s) noexcept {
    switch (s) {
        case B4State::DefineAwaitingId:     return "DefineAwaitingId";
        case B4State::ViewAwaitingId:       return "ViewAwaitingId";
        case B4State::DefineAwaitingCommit: return "DefineAwaitingCommit";
        case B4State::ViewAwaitingCommit:   return "ViewAwaitingCommit";
        case B4State::AlterAwaitingCommit:  return "AlterAwaitingCommit";
    }
    return "?";
}
static void PrintTo(B4State s, std::ostream* os) { *os << b4StateName(s); }
[[nodiscard]] static bool b4AwaitingCommit(B4State s) noexcept {
    return s == B4State::DefineAwaitingCommit || s == B4State::ViewAwaitingCommit ||
           s == B4State::AlterAwaitingCommit;
}
[[nodiscard]] static bool b4AwaitingId(B4State s) noexcept { return !b4AwaitingCommit(s); }

class DdlDemotionRegression : public ::testing::TestWithParam<B4State> {
protected:
    static DdlHarnessOptions options(B4State s) {
        DdlHarnessOptions o;
        o.holdCommits = b4AwaitingCommit(s);
        return o;
    }
    /// Submit the op and drive it into state `s`; returns its corr.
    static int64_t driveInto(DdlHarness& h, B4State s) {
        int64_t c = -1;
        switch (s) {
            case B4State::DefineAwaitingId:
                h.setControl(ControlMode::Hold);
                c = h.submit(Op::DefineDisk, "n");
                EXPECT_TRUE(h.tickUntilCreatePending());
                break;
            case B4State::ViewAwaitingId:
                h.setControl(ControlMode::Hold);
                c = h.submit(Op::ViewPersisted, "v");
                EXPECT_TRUE(h.tickUntilCreatePending());
                break;
            case B4State::DefineAwaitingCommit:
                c = h.submit(Op::DefineDisk, "n");
                EXPECT_TRUE(h.tickUntilAppended());
                break;
            case B4State::ViewAwaitingCommit:
                c = h.submit(Op::ViewPersisted, "v");
                EXPECT_TRUE(h.tickUntilAppended());
                break;
            case B4State::AlterAwaitingCommit:
                c = h.submit(Op::AlterAdd, "src");
                EXPECT_TRUE(h.tickUntilAppended());
                break;
        }
        EXPECT_TRUE(h.worker().hasInFlight());
        EXPECT_FALSE(h.response(c).has_value());
        return c;
    }
    /// Did the in-flight op's effect reach the admin catalogs?
    static bool applied(DdlHarness& h, B4State s) {
        switch (s) {
            case B4State::DefineAwaitingId:
            case B4State::DefineAwaitingCommit:
                return h.schemas().byName("n") != nullptr;
            case B4State::ViewAwaitingId:
            case B4State::ViewAwaitingCommit:
                return h.views().byName("v") != nullptr || h.schemas().byName("v") != nullptr;
            case B4State::AlterAwaitingCommit:
                return h.schemas().byName("src")->version != 1u;
        }
        return true;
    }
};

TEST_P(DdlDemotionRegression, DemotionAnswersInternalAtOnceStartsNothingAndAppliesNothing) {
    const B4State s = GetParam();
    DdlHarness h(options(s));
    const int pushes0 = h.viewPlanPushes(), validators0 = h.validatorPushes();
    const int64_t x = driveInto(h, s);
    const int64_t y = h.submit(Op::AlterAdd, "src2");       // queued behind x
    const int64_t w = h.submit(Op::DefineDisk, "queued");   // queued behind y

    h.demote();
    ASSERT_TRUE(h.response(x).has_value()) << "answered inside onRole(follower), not on a tick or the timeout";
    EXPECT_EQ(h.response(x)->err, kInternal);
    EXPECT_FALSE(h.worker().hasInFlight());
    EXPECT_FALSE(h.response(y).has_value()) << "demotion answers only the in-flight op";

    // A demoted worker starts nothing: every queued op is refused at the leader
    // guard (FIFO, checked by the harness), no create request, no append.
    const int reqs = h.createRequests();
    const int64_t schemaEnd = h.schemaLogEnd(), viewsEnd = h.viewsLogEnd();
    for (int i = 0; i < 10; ++i) h.tick();
    ASSERT_TRUE(h.response(y).has_value());
    ASSERT_TRUE(h.response(w).has_value());
    EXPECT_EQ(h.response(y)->err, kInternal);
    EXPECT_EQ(h.response(w)->err, kInternal);
    EXPECT_EQ(h.createRequests(), reqs) << "no create request while demoted";
    EXPECT_EQ(h.schemaLogEnd(), schemaEnd) << "no __topic_schemas append while demoted";
    EXPECT_EQ(h.viewsLogEnd(), viewsEnd) << "no __views append while demoted";
    EXPECT_EQ(h.schemas().byName("src2")->version, 1u);
    EXPECT_EQ(h.schemas().byName("queued"), nullptr);

    if (b4AwaitingId(s)) h.deliverCreate();   // the late reply: a stale ticket (an orphan)
    for (int i = 0; i < 5; ++i) h.tick();
    EXPECT_FALSE(applied(h, s)) << "never applied off a follower";

    h.promote();
    h.setControl(ControlMode::Auto);
    h.settle();   // releases held commits: the HW now covers x's record
    EXPECT_FALSE(applied(h, s)) << "re-promotion never applies an answered op";
    EXPECT_EQ(h.viewPlanPushes(), pushes0);
    EXPECT_EQ(h.validatorPushes(), validators0);

    const int64_t z = h.submit(Op::DefineDisk, "after");
    h.settle();
    ASSERT_TRUE(h.response(z).has_value());
    EXPECT_EQ(h.response(z)->err, kOk) << "the re-promoted leader serves DDL";

    h.restart();
    if (b4AwaitingId(s))
        EXPECT_FALSE(applied(h, s)) << "nothing was appended: still absent after a restart";
    else
        EXPECT_TRUE(applied(h, s)) << "KL2: an op demoted in AwaitingCommit is in doubt; its record replays";
    EXPECT_NE(h.schemas().byName("after"), nullptr);
    EXPECT_EQ(h.schemas().byName("queued"), nullptr);
    EXPECT_EQ(h.schemas().byName("src2")->version, 1u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST_P(DdlDemotionRegression, RepromotionRunsTheQueuedOpsInFifoOrder) {
    const B4State s = GetParam();
    DdlHarness h(options(s));
    const int64_t x = driveInto(h, s);
    const int64_t y = h.submit(Op::DefineDisk, "q1");
    const int64_t w = h.submit(Op::AlterAdd, "src2");
    const int64_t u = h.submit(Op::ViewTransient, "q3");

    h.demote();
    ASSERT_TRUE(h.response(x).has_value());
    EXPECT_EQ(h.response(x)->err, kInternal);
    h.promote();   // no tick while demoted: the queue survives into the new term
    if (b4AwaitingId(s)) h.deliverCreate();   // x's stale reply
    h.setControl(ControlMode::Auto);
    h.settle();

    ASSERT_TRUE(h.response(y).has_value());
    ASSERT_TRUE(h.response(w).has_value());
    ASSERT_TRUE(h.response(u).has_value());
    EXPECT_EQ(h.response(y)->err, kOk);
    EXPECT_EQ(h.response(w)->err, kOk);
    EXPECT_EQ(h.response(w)->version, 2u);
    EXPECT_EQ(h.response(u)->err, kOk);
    std::vector<int64_t> order;
    for (const Response& r : h.responses())
        if (r.corr == y || r.corr == w || r.corr == u) order.push_back(r.corr);
    EXPECT_EQ(order, (std::vector<int64_t>{y, w, u})) << "FIFO after re-promotion";
    EXPECT_FALSE(applied(h, s));
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(EveryAsyncState, DdlDemotionRegression,
                         ::testing::Values(B4State::DefineAwaitingId, B4State::ViewAwaitingId,
                                           B4State::DefineAwaitingCommit, B4State::ViewAwaitingCommit,
                                           B4State::AlterAwaitingCommit),
                         [](const ::testing::TestParamInfo<B4State>& i) { return std::string(b4StateName(i.param)); });

// ── B8 / user decision D3a: renaming the key field moves TopicSchema.keyField ────
TEST(DdlFixRegression, B8_RenamingTheKeyFieldMovesKeyFieldAndSurvivesRestart) {
    DdlHarness h;
    ASSERT_EQ(h.schemas().byName("src")->keyField, "id");
    const int64_t a = h.submit(Op::AlterRenameKey, "src");   // RENAME id -> pk
    h.settle();
    ASSERT_EQ(h.response(a)->err, kOk);
    EXPECT_EQ(h.schemas().byName("src")->keyField, "pk") << "live";
    EXPECT_EQ(h.schemas().byName("src")->version, 2u);
    h.restart();
    EXPECT_EQ(h.schemas().byName("src")->keyField, "pk") << "replayed from the durable record";
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
    EXPECT_EQ(h.schemas().byName("src")->keyField, "b") << "replayed from the durable record";
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
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kInvalidSchema) << "applyOps is atomic";
    EXPECT_EQ(h.schemas().byName("src")->keyField, "id");
    EXPECT_EQ(h.schemas().byName("src")->version, 1u);
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
            const int reqs = h.createRequests();
            const int64_t viewsEnd = h.viewsLogEnd();
            const int64_t c = h.submit(outer, "outer", inner);
            h.settle();
            EXPECT_EQ(h.response(c)->err, kViewSourceIsView);
            EXPECT_EQ(h.createRequests(), reqs) << "no sink requested";
            EXPECT_EQ(h.viewsLogEnd(), viewsEnd) << "no __views record";
            EXPECT_EQ(h.catalog(), before);
        }
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
    namespace rv = rune::view;
    rv::ViewPlan p;
    p.viewName = "outer";
    p.key = "id";
    p.persistence = 0;
    p.sources.push_back(rv::SourceSpec{h.idOf("src"), "id"});
    p.sources.push_back(rv::SourceSpec{h.idOf("inner"), "id"});
    p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    CreateViewMsg m;
    m.viewName = "outer"; m.keyField = "id"; m.persistence = 0;
    m.planBlob = rv::serializeAuthored(p);
    const Catalog before = h.catalog();
    const int reqs = h.createRequests();
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    EXPECT_EQ(h.response(c)->err, kViewSourceIsView);
    EXPECT_EQ(h.createRequests(), reqs);
    EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── B4 fix round 1: AwaitingCommit with its commit replica missing answers the
//    op Internal (never a silent drop: the client would hang), logs, counts, and
//    the next queued op starts.  Unreachable in production (an op reaches
//    AwaitingCommit only after appending to that replica); forced via the
//    test-only swapReplicasForTest hook.
TEST(DdlFixRegression, B4_MissingSchemaReplicaInAwaitingCommitAnswersInternal) {
    for (const Op op : {Op::AlterAdd, Op::DefineDisk}) {
        SCOPED_TRACE(opName(op));
        DdlHarnessOptions o;
        o.holdCommits = true;
        DdlHarness h(o);
        const int64_t a = h.submit(op, op == Op::AlterAdd ? "src" : "n");
        ASSERT_TRUE(h.tickUntilAppended());
        ASSERT_TRUE(h.worker().hasInFlight());
        const int64_t next = h.submit(Op::ViewPersisted, "v");   // uses only the __views replica
        const auto saved = h.worker().swapReplicasForTest({});
        (void)h.worker().swapReplicasForTest({nullptr, saved.views});
        h.tick();
        ASSERT_TRUE(h.response(a).has_value()) << "answered, never silently dropped";
        EXPECT_EQ(h.response(a)->err, kInternal);
        EXPECT_EQ(h.worker().commitReplicaMissing(), 1u);
        EXPECT_EQ(h.worker().startedCount(), 4u) << "the next queued op started (2 setup defines + a + next)";
        (void)h.worker().swapReplicasForTest(saved);
        h.settle();
        ASSERT_TRUE(h.response(next).has_value());
        EXPECT_EQ(h.response(next)->err, kOk);
        EXPECT_EQ(h.schemas().byName("n"), nullptr);
        EXPECT_EQ(h.schemas().byName("src")->version, 1u) << "never applied live";
        EXPECT_TRUE(h.violations().empty()) << h.violationText();
    }
}

TEST(DdlFixRegression, B4_MissingViewsReplicaInAwaitingCommitAnswersInternal) {
    DdlHarnessOptions o;
    o.holdCommits = true;
    DdlHarness h(o);
    const int pushes0 = h.viewPlanPushes();
    const int64_t v = h.submit(Op::ViewPersisted, "v");
    ASSERT_TRUE(h.tickUntilAppended());
    ASSERT_TRUE(h.worker().hasInFlight());
    const int64_t next = h.submit(Op::AlterAdd, "src");   // uses only the schema replica
    const auto saved = h.worker().swapReplicasForTest({});
    (void)h.worker().swapReplicasForTest({saved.schema, nullptr});
    h.tick();
    ASSERT_TRUE(h.response(v).has_value()) << "answered, never silently dropped";
    EXPECT_EQ(h.response(v)->err, kInternal);
    EXPECT_EQ(h.worker().commitReplicaMissing(), 1u);
    EXPECT_TRUE(h.worker().hasInFlight()) << "the next queued op (the ALTER) started on the same tick";
    (void)h.worker().swapReplicasForTest(saved);
    h.settle();
    ASSERT_TRUE(h.response(next).has_value());
    EXPECT_EQ(h.response(next)->err, kOk);
    EXPECT_EQ(h.response(next)->version, 2u);
    EXPECT_EQ(h.views().byName("v"), nullptr) << "never applied live";
    EXPECT_EQ(h.viewPlanPushes(), pushes0);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── Task 10 fix round 1: harness restart keeps the replica set; restart-aware installs ──

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
    EXPECT_EQ(h.catalog(), before) << "promotion replayed both catalogs up to the learned HW";

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

    // A leadership flap after the restart keeps the catalog (rebuild from the HW).
    h.demote();
    h.promote();
    h.settle();
    EXPECT_EQ(h.schemas().byName("t")->version, 2u);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

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
    EXPECT_EQ(h.recoveryViewInstalls(), 4) << "every promotion edge re-recovers the views";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}
