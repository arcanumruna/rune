// DdlOccupantMatrixRegressionTest.cpp — DDL conformance plan Task 8.
//
// One op against a name in every occupancy state (41 rows x 12 ops = 492 cells), driven
// through DdlHarness.  Expectations come ONLY from DdlExpectations.h.  Every cell
// asserts the exact code, the requestCreateTopic count, no half-creation, the
// final catalog delta, the serialization invariant, and the catalog after a restart.
#include <gtest/gtest.h>
#include "support/DdlHarness.h"

#include <algorithm>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

using namespace rune::node;
using namespace rune::node::test;

namespace {

enum class Setup : uint8_t {
    None, TypedDisk, TypedTransient, ViewPersisted, ViewTransient, StaticDisk, StaticTransient,
    RecoveredTyped, RecoveredView, OrphanDefineDisk, OrphanDefineTransient, OrphanSink,
    OrphanSinkAfterRestart,
    // Final review I5(b): KL2 — the op appended its record, then timed out in
    // AwaitingCommit (answered Internal, never applied live); the record committed.
    // Live it is an orphan; after the restart (every cell restarts) it is committed.
    InDoubtDefine, InDoubtView,
};
[[nodiscard]] constexpr bool isInDoubt(Setup s) noexcept { return s == Setup::InDoubtDefine || s == Setup::InDoubtView; }

struct Row {
    const char* label;      // test-name fragment ([A-Za-z0-9_] only)
    Setup       setup;
    std::string occupant;   // the name the setup creates
    std::string target;     // the name every op uses
    Occupant    expect;     // the expectation-table row
};
void PrintTo(const Row& r, std::ostream* os) { *os << r.label; }

std::vector<Row> rows() {
    std::vector<Row> r = {
        {"Free",                       Setup::None,                   "",      "fresh", Occupant::Free},
        {"TypedDisk",                  Setup::TypedDisk,              "occ",   "occ",   Occupant::TypedDisk},
        {"TypedTransient",             Setup::TypedTransient,         "occ",   "occ",   Occupant::TypedTransient},
        {"ViewPersisted",              Setup::ViewPersisted,          "occ",   "occ",   Occupant::ViewPersisted},
        {"ViewTransient",              Setup::ViewTransient,          "occ",   "occ",   Occupant::ViewTransient},
        {"StaticSchemaLessDisk",       Setup::StaticDisk,             "occ",   "occ",   Occupant::SchemaLessDisk},
        {"StaticSchemaLessTransient",  Setup::StaticTransient,        "occ",   "occ",   Occupant::SchemaLessTransient},
        {"RecoveredRuntimeTopic",      Setup::RecoveredTyped,         "occ",   "occ",   Occupant::TypedDisk},
        {"RecoveredView",              Setup::RecoveredView,          "occ",   "occ",   Occupant::ViewPersisted},
        {"OrphanDefineDisk",           Setup::OrphanDefineDisk,       "occ",   "occ",   Occupant::SchemaLessDisk},
        {"OrphanDefineTransient",      Setup::OrphanDefineTransient,  "occ",   "occ",   Occupant::SchemaLessTransient},
        {"OrphanSink_KnownLimitation", Setup::OrphanSink,             "occ",   "occ",   Occupant::OrphanSink},
        {"OrphanSinkAfterRestart",     Setup::OrphanSinkAfterRestart, "occ",   "occ",   Occupant::Free},
        {"CaseVariantOfTyped",         Setup::TypedDisk,              "Casey", "casey", Occupant::CaseVariant},
        {"CaseVariantOfView",          Setup::ViewPersisted,          "Viewy", "viewy", Occupant::CaseVariant},
        {"CaseVariantOfStatic",        Setup::StaticDisk,             "Stat",  "stat",  Occupant::CaseVariant},
        {"CaseVariantOfOrphan",        Setup::OrphanDefineDisk,       "Orph",  "orph",  Occupant::CaseVariant},
        {"InDoubtDefine",              Setup::InDoubtDefine,          "occ",   "occ",   Occupant::SchemaLessDisk},
        {"InDoubtView",                Setup::InDoubtView,            "occ",   "occ",   Occupant::OrphanSink},
        {"CaseVariantOfInDoubtView",   Setup::InDoubtView,            "Indo",  "indo",  Occupant::CaseVariant},
    };
    const std::vector<std::pair<const char*, std::string>> bad = {
        {"Reserved_topic_schemas",      "__topic_schemas"},
        {"Reserved_views",              "__views"},
        {"Reserved_consumer_offsets",   "__consumer_offsets"},
        {"Reserved_transaction_state",  "__transaction_state"},
        {"Reserved_cluster_metadata",   "__cluster_metadata"},
        {"Reserved_view_snapshots",     "__view_snapshots"},
        {"Reserved_orphan_placeholder", "__orphan.7"},
        {"Invalid_Empty",               ""},
        {"Invalid_Slash",               "a/b"},
        {"Invalid_DotDotSlash",         "../escape"},
        {"Invalid_Absolute",            "/abs"},
        {"Invalid_Dot",                 "."},
        {"Invalid_DotDot",              ".."},
        {"Invalid_TooLong250",          std::string(250, 'x')},
        {"Invalid_Space",               "has space"},
        {"Invalid_Tab",                 "tab\tname"},
        {"Invalid_NonAscii",            "caf\xC3\xA9"},
        {"Invalid_EmbeddedNul",         std::string("nul\0byte", 8)},
    };
    for (const auto& [label, name] : bad) r.push_back({label, Setup::None, "", name, Occupant::BadName});
    r.push_back({"Valid_MaxLen249",         Setup::None, "", std::string(249, 'x'), Occupant::Free});
    r.push_back({"Valid_OneChar",           Setup::None, "", "a",                   Occupant::Free});
    r.push_back({"Valid_LeadingDotAndDash", Setup::None, "", ".hidden_ok-1",        Occupant::Free});
    return r;
}

DdlHarnessOptions optionsFor(const Row& row) {
    DdlHarnessOptions o;
    o.holdCommits = isInDoubt(row.setup);   // the HW waits for follower 2: an op can sit in AwaitingCommit
    if (row.setup == Setup::StaticDisk || row.setup == Setup::StaticTransient) {
        TopicSpec s;
        s.name = row.occupant;
        s.partitionCount = 1;
        s.persistence = row.setup == Setup::StaticTransient ? PersistenceMode::Transient : PersistenceMode::Disk;
        o.statics.push_back(s);
    }
    return o;
}

void create(DdlHarness& h, Op op, const std::string& name) {
    const int64_t c = h.submit(op, name);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kOk) << "setup " << opName(op) << " '" << name << "'";
}

/// KL2: `op` appends its record, times out in AwaitingCommit (answered Internal,
/// never applied live), then the record commits.
void makeInDoubt(DdlHarness& h, Op op, const std::string& name) {
    const int64_t c = h.submit(op, name);
    ASSERT_TRUE(h.tickUntilAppended());
    h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
    h.tick();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kInternal);
    h.releaseCommits();
    h.tick();
    ASSERT_FALSE(h.commitHeld()) << "the in-doubt record committed";
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

void setupOccupant(DdlHarness& h, const Row& row) {
    switch (row.setup) {
        case Setup::None:
        case Setup::StaticDisk:
        case Setup::StaticTransient:        return;
        case Setup::TypedDisk:              create(h, Op::DefineDisk, row.occupant); return;
        case Setup::TypedTransient:         create(h, Op::DefineTransient, row.occupant); return;
        case Setup::ViewPersisted:          create(h, Op::ViewPersisted, row.occupant); return;
        case Setup::ViewTransient:          create(h, Op::ViewTransient, row.occupant); return;
        case Setup::RecoveredTyped:         create(h, Op::DefineDisk, row.occupant); h.restart(); return;
        case Setup::RecoveredView:          create(h, Op::ViewPersisted, row.occupant); h.restart(); return;
        case Setup::OrphanDefineDisk:       makeOrphan(h, Op::DefineDisk, row.occupant); return;
        case Setup::OrphanDefineTransient:  makeOrphan(h, Op::DefineTransient, row.occupant); return;
        case Setup::OrphanSink:             makeOrphan(h, Op::ViewPersisted, row.occupant); return;
        case Setup::InDoubtDefine:          makeInDoubt(h, Op::DefineDisk, row.occupant); return;
        case Setup::InDoubtView:            makeInDoubt(h, Op::ViewPersisted, row.occupant); return;
        case Setup::OrphanSinkAfterRestart:
            makeOrphan(h, Op::ViewPersisted, row.occupant);
            if (::testing::Test::HasFatalFailure()) return;
            create(h, Op::DefineDisk, "later");   // a later recorded id: the orphan is a gap (B5)
            h.restart();
            return;
    }
}

void expectSuccessDelta(const Catalog& before, const Catalog& after, Op op, const std::string& name,
                        const Response& r) {
    const CatalogEntry* b = findEntry(before, name);
    const CatalogEntry* a = findEntry(after, name);
    ASSERT_NE(a, nullptr) << "the op's entry exists";
    EXPECT_EQ(a->id, r.id) << "the answer carries the entry's id";
    if (isCreate(op)) {
        EXPECT_EQ(after.size(), before.size() + (b ? 0u : 1u)) << "exactly one new topic, or the adopted one";
        if (b) EXPECT_EQ(a->id, b->id) << "adoption keeps the existing topic's id";
        EXPECT_EQ(a->kind, isView(op) ? EntryKind::View : EntryKind::Typed);
        EXPECT_EQ(a->version, 1u);
        EXPECT_EQ(a->keyField, "id") << "the requested key field";
        const PersistenceMode want =
            isView(op)  ? (op == Op::ViewTransient ? PersistenceMode::Transient : PersistenceMode::Disk)
            : b         ? b->persistence   // an adopted topic keeps ITS log backend
            : (op == Op::DefineTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
        EXPECT_EQ(a->persistence, want);
    } else {
        ASSERT_NE(b, nullptr);
        EXPECT_EQ(after.size(), before.size());
        EXPECT_EQ(a->id, b->id);
        EXPECT_EQ(a->version, b->version + 1) << "one contiguous version";
        EXPECT_EQ(a->persistence, b->persistence) << "ALTER carries persistence forward";
        EXPECT_EQ(r.version, a->version);
        EXPECT_EQ(a->keyField, op == Op::AlterRenameKey ? std::string("pk") : b->keyField)
            << "keyField follows a key-field RENAME (B8), and nothing else";
    }
    for (const CatalogEntry& e : before) {
        if (e.name == name) continue;
        const CatalogEntry* x = findEntry(after, e.name);
        ASSERT_NE(x, nullptr) << e.name;
        EXPECT_EQ(*x, e) << "every other entry is untouched";
    }
}

class DdlOccupantMatrixRegression : public ::testing::TestWithParam<std::tuple<Row, Op>> {};

TEST_P(DdlOccupantMatrixRegression, OneOpOnAnOccupiedName) {
    const Row& row = std::get<0>(GetParam());
    const Op op = std::get<1>(GetParam());
    DdlHarness h(optionsFor(row));
    setupOccupant(h, row);
    if (HasFatalFailure()) return;
    ASSERT_TRUE(h.violations().empty()) << h.violationText();

    const Catalog before = h.catalog();
    const int reqs = h.createRequests();
    const int64_t schemaEnd = h.schemaLogEnd();
    const int64_t viewsEnd = h.viewsLogEnd();
    const int plans = h.viewPlanPushes();

    const int64_t corr = h.submit(op, row.target);
    h.settle();

    const Cell want = expectedCell(row.expect, op);
    const std::optional<Response> got = h.response(corr);
    ASSERT_TRUE(got.has_value()) << "every op is answered";
    EXPECT_EQ(got->err, want.code) << opName(op) << " on " << occupantName(row.expect);
    EXPECT_EQ(h.createRequests() - reqs, want.createRequests) << "requestCreateTopic calls";

    const Catalog after = h.catalog();
    if (want.code != kOk || op == Op::GetSchema) {
        EXPECT_EQ(after, before) << "no half-creation: a rejected op / a read leaves the catalog untouched";
        EXPECT_EQ(h.schemaLogEnd(), schemaEnd) << "no stray TopicSchema record";
        EXPECT_EQ(h.viewsLogEnd(), viewsEnd) << "no stray __views record";
        EXPECT_EQ(h.viewPlanPushes(), plans) << "no worker install";
    } else {
        expectSuccessDelta(before, after, op, row.target, *got);
        EXPECT_EQ(h.schemaLogEnd() - schemaEnd, isDefine(op) || isAlter(op) ? 1 : 0);
        EXPECT_EQ(h.viewsLogEnd() - viewsEnd, isView(op) ? 1 : 0);
        EXPECT_EQ(h.viewPlanPushes() - plans, isView(op) ? 1 : 0);
    }
    if (op == Op::GetSchema && want.code == kOk) {
        const CatalogEntry* e = findEntry(before, row.target);
        ASSERT_NE(e, nullptr);
        EXPECT_EQ(got->id, e->id);
        EXPECT_EQ(got->version, e->version);
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    Catalog wantAfter = h.expectedAfterRestart(after);
    if (isInDoubt(row.setup)) {
        // The in-doubt record committed: after a restart the topic is the in-doubt
        // op's committed kind — unless this op adopted it (then it is this op's).
        // The reference is the same setup restarted with no op in between.
        DdlHarness ref(optionsFor(row));
        setupOccupant(ref, row);
        if (HasFatalFailure()) return;
        ref.restart();
        const Catalog refCat = ref.catalog();
        const CatalogEntry* committed = findEntry(refCat, row.occupant);
        ASSERT_NE(committed, nullptr);
        const CatalogEntry* live = findEntry(after, row.occupant);
        const CatalogEntry kept = (live != nullptr && live->kind != EntryKind::SchemaLess) ? *live : *committed;
        std::erase_if(wantAfter, [&](const CatalogEntry& e) { return e.id == committed->id; });
        wantAfter.push_back(kept);
        std::sort(wantAfter.begin(), wantAfter.end(), [](const CatalogEntry& a, const CatalogEntry& b) { return a.id < b.id; });
    }
    h.restart();
    EXPECT_EQ(h.catalog(), wantAfter) << "state after a restart";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlOccupantMatrixRegression,
    ::testing::Combine(::testing::ValuesIn(rows()), ::testing::ValuesIn(kAllOps)),
    [](const ::testing::TestParamInfo<std::tuple<Row, Op>>& i) {
        return std::string(std::get<0>(i.param).label) + "_" + opName(std::get<1>(i.param));
    });

// ── Malformed / hand-built admin requests: every one is ANSWERED with an error ─────
class DdlMalformedRequestRegression : public ::testing::Test {
protected:
    void expectAnsweredUntouched(int64_t corr, uint16_t want) {
        ASSERT_TRUE(h.response(corr).has_value()) << "a malformed request is answered, never dropped";
        EXPECT_EQ(h.response(corr)->err, want);
        EXPECT_EQ(h.catalog(), before);
        EXPECT_EQ(h.schemaLogEnd(), schemaEnd);
        EXPECT_EQ(h.viewsLogEnd(), viewsEnd);
        EXPECT_TRUE(h.violations().empty()) << h.violationText();
    }
    DdlHarness h;
    Catalog before = h.catalog();
    int64_t schemaEnd = h.schemaLogEnd();
    int64_t viewsEnd = h.viewsLogEnd();
};

TEST_F(DdlMalformedRequestRegression, DefineWithUnknownPersistenceByteIsInvalidSchema_R1) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id"; m.blob = charBlob(); m.persistence = 2;
    const int reqs = h.createRequests();
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
    EXPECT_EQ(h.createRequests(), reqs);
}

TEST_F(DdlMalformedRequestRegression, DefineWithGarbageDescriptorIsInvalidSchema) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id"; m.blob = {0, 1, 2};
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, DefineWithEmptyDescriptorIsInvalidSchema) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id";   // what the ring decode leaves for a bad byte
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, CreateViewWithUnknownPersistenceByteIsInvalidSchema_R1) {
    CreateViewMsg m; m.viewName = "v"; m.keyField = "id"; m.persistence = 2;
    m.planBlob = viewPlanBlob("v", h.schemas().byName("src")->topicId, 0);
    const int reqs = h.createRequests();
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
    EXPECT_EQ(h.createRequests(), reqs);
}

TEST_F(DdlMalformedRequestRegression, CreateViewMarkedMalformedIsInvalidSchema) {
    CreateViewMsg m; m.malformed = true; m.viewName = "v";
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, CreateViewWithGarbagePlanIsFieldUnresolved) {
    CreateViewMsg m; m.viewName = "v"; m.keyField = "id"; m.planBlob = {9, 9, 9};
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kViewFieldUnresolved);
}

TEST_F(DdlMalformedRequestRegression, CreateViewWithEmptyPlanIsFieldUnresolved) {
    CreateViewMsg m; m.viewName = "v"; m.keyField = "id";
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kViewFieldUnresolved);
}

TEST_F(DdlMalformedRequestRegression, AlterWithGarbageOpsIsInvalidSchema) {
    AlterTopicMsg m; m.topicName = "src"; m.opsBlob = {0xff, 0xff, 0x01};
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, AlterWithEmptyOpsIsInvalidSchema) {
    AlterTopicMsg m; m.topicName = "src";
    const int64_t c = h.submitRawAlter(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

TEST_F(DdlMalformedRequestRegression, CreateReplyWithUnknownPersistenceIsInternal_R1) {
    h.setControl(ControlMode::Hold);
    const int64_t c = h.submit(Op::DefineDisk, "m");
    ASSERT_TRUE(h.tickUntilCreatePending());
    h.deliverCreateWithPersistence(2);   // a reply whose persistence byte is not a mode
    h.setControl(ControlMode::Auto);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    EXPECT_EQ(h.response(c)->err, kInternal);
    EXPECT_EQ(h.schemas().byName("m"), nullptr);
    EXPECT_EQ(h.schemaLogEnd(), schemaEnd) << "nothing recorded";
}

// ── View source cells: source kind x view persistence ─────────────────────────────
struct SourceCase { const char* label; const char* source; Op view; uint16_t want; };
void PrintTo(const SourceCase& c, std::ostream* os) { *os << c.label; }

const SourceCase kSourceCases[] = {
    {"DiskSource_PersistedView",          "src",    Op::ViewPersisted, kOk},
    {"DiskSource_TransientView",          "src",    Op::ViewTransient, kOk},
    {"TransientSource_PersistedView",     "tsrc",   Op::ViewPersisted, kViewSourceTransient},
    {"TransientSource_TransientView",     "tsrc",   Op::ViewTransient, kOk},
    {"SchemaLessSource_PersistedView",    "ssrc",   Op::ViewPersisted, kViewSourceNotFound},
    {"SchemaLessSource_TransientView",    "ssrc",   Op::ViewTransient, kViewSourceNotFound},
    {"MissingSource_PersistedView",       "nosuch", Op::ViewPersisted, kViewSourceNotFound},
    {"MissingSource_TransientView",       "nosuch", Op::ViewTransient, kViewSourceNotFound},
    // B9 / user decision D3b: a view over another view's output (until Phase 3).
    {"PersistedViewSource_PersistedView", "vsrc",   Op::ViewPersisted, kViewSourceIsView},
    {"PersistedViewSource_TransientView", "vsrc",   Op::ViewTransient, kViewSourceIsView},
    {"TransientViewSource_PersistedView", "tvsrc",  Op::ViewPersisted, kViewSourceIsView},
    {"TransientViewSource_TransientView", "tvsrc",  Op::ViewTransient, kViewSourceIsView},
};

class DdlViewSourceRegression : public ::testing::TestWithParam<SourceCase> {};

TEST_P(DdlViewSourceRegression, ViewOverSource) {
    const SourceCase& c = GetParam();
    DdlHarnessOptions o;
    TopicSpec s; s.name = "ssrc"; s.partitionCount = 1;
    o.statics.push_back(s);
    DdlHarness h(o);
    create(h, Op::DefineTransient, "tsrc");
    create(h, Op::ViewPersisted, "vsrc");    // over "src"
    create(h, Op::ViewTransient, "tvsrc");   // over "src"
    if (HasFatalFailure()) return;
    const Catalog before = h.catalog();
    const int reqs = h.createRequests();
    const int64_t corr = h.submit(c.view, "vw", c.source);
    h.settle();
    ASSERT_TRUE(h.response(corr).has_value());
    EXPECT_EQ(h.response(corr)->err, c.want);
    EXPECT_EQ(h.createRequests() - reqs, c.want == kOk ? 1 : 0) << "a rejected view requests no sink";
    if (c.want != kOk) EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    if (c.want == kViewSourceIsView) {   // the rule holds after a restart (views recovered first)
        h.restart();
        const int64_t again = h.submit(c.view, "vw", c.source);
        h.settle();
        ASSERT_TRUE(h.response(again).has_value());
        EXPECT_EQ(h.response(again)->err, kViewSourceIsView) << "after restart";
    }
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlViewSourceRegression, ::testing::ValuesIn(kSourceCases),
    [](const ::testing::TestParamInfo<SourceCase>& i) { return std::string(i.param.label); });

} // namespace
