// DdlHarnessRegressionTest.cpp — DDL conformance plan: smoke tests of DdlHarness
// itself (Task 4) and the targeted regression for every fix the matrices exposed
// (B1-B4, KL2: Tasks 5-7).  Every suite name contains "Regression".
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <rune/node/TopicName.h>

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
