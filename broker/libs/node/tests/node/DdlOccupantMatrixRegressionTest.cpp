// DdlOccupantMatrixRegressionTest.cpp — DDL conformance plan Task 8, ported to the
// catalog write path by catalog history Task 10.
//
// One op against a name in every occupancy state (41 rows x 16 ops = 656 cells), driven
// through DdlHarness.  Expectations come ONLY from DdlExpectations.h.  Every cell
// asserts the exact code, the __catalog events appended, no half-creation, the
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

// Former rows, mapped (catalog history Task 10):
//   StaticSchemaLess*  -> SchemaLess* : untyped topics created by DDL (no RUNE_TOPICS).
//   OrphanDefine* / OrphanSink* / CaseVariantOfOrphan -> Uncommitted* : the create's
//     append is refused (the ISR is below the catalog's minIsr) — answered NotLeader,
//     nothing appended: the name is Free (no orphan, no placeholder, no id used).
//   InDoubt*           : the op appended, its commit timed out (answered NotLeader, F13);
//     the event then commits and is applied LIVE — the name is the op's committed kind.
enum class Setup : uint8_t {
    None, TypedDisk, TypedTransient, ViewPersisted, ViewTransient, SchemaLessDisk, SchemaLessTransient,
    RecoveredTyped, RecoveredView, UncommittedDefineDisk, UncommittedDefineTransient, UncommittedView,
    UncommittedViewThenRestart, InDoubtDefine, InDoubtView,
};

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
        {"Free",                        Setup::None,                       "",      "fresh", Occupant::Free},
        {"TypedDisk",                   Setup::TypedDisk,                  "occ",   "occ",   Occupant::TypedDisk},
        {"TypedTransient",              Setup::TypedTransient,             "occ",   "occ",   Occupant::TypedTransient},
        {"ViewPersisted",               Setup::ViewPersisted,              "occ",   "occ",   Occupant::ViewPersisted},
        {"ViewTransient",               Setup::ViewTransient,              "occ",   "occ",   Occupant::ViewTransient},
        {"UntypedSchemaLessDisk",       Setup::SchemaLessDisk,             "occ",   "occ",   Occupant::SchemaLessDisk},
        {"UntypedSchemaLessTransient",  Setup::SchemaLessTransient,        "occ",   "occ",   Occupant::SchemaLessTransient},
        {"RecoveredRuntimeTopic",       Setup::RecoveredTyped,             "occ",   "occ",   Occupant::TypedDisk},
        {"RecoveredView",               Setup::RecoveredView,              "occ",   "occ",   Occupant::ViewPersisted},
        {"UncommittedDefineDisk",       Setup::UncommittedDefineDisk,      "occ",   "occ",   Occupant::Free},
        {"UncommittedDefineTransient",  Setup::UncommittedDefineTransient, "occ",   "occ",   Occupant::Free},
        {"UncommittedView",             Setup::UncommittedView,            "occ",   "occ",   Occupant::Free},
        {"UncommittedViewAfterRestart", Setup::UncommittedViewThenRestart, "occ",   "occ",   Occupant::Free},
        {"CaseVariantOfTyped",          Setup::TypedDisk,                  "Casey", "casey", Occupant::CaseVariant},
        {"CaseVariantOfView",           Setup::ViewPersisted,              "Viewy", "viewy", Occupant::CaseVariant},
        {"CaseVariantOfUntyped",        Setup::SchemaLessDisk,             "Stat",  "stat",  Occupant::CaseVariant},
        {"CaseVariantOfUncommitted",    Setup::UncommittedDefineDisk,      "Orph",  "orph",  Occupant::Free},
        {"InDoubtDefine",               Setup::InDoubtDefine,              "occ",   "occ",   Occupant::TypedDisk},
        {"InDoubtView",                 Setup::InDoubtView,                "occ",   "occ",   Occupant::ViewPersisted},
        {"CaseVariantOfInDoubtView",    Setup::InDoubtView,                "Indo",  "indo",  Occupant::CaseVariant},
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
    // The HW is held for the in-doubt setups (an op sits in AwaitingCommit).
    o.holdCommits = row.setup == Setup::InDoubtDefine || row.setup == Setup::InDoubtView;
    return o;
}

void create(DdlHarness& h, Op op, const std::string& name) {
    const int64_t c = h.submit(op, name);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kOk) << "setup " << opName(op) << " '" << name << "'";
}

/// An UNTYPED topic (an empty descriptor), created by DDL.
void createUntyped(DdlHarness& h, const std::string& name, uint8_t persistence) {
    DefineTopicMsg m;
    m.name = name; m.partitionCount = 1; m.persistence = persistence;
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kOk) << "setup untyped '" << name << "'";
    ASSERT_EQ(h.schemas().byName(name), nullptr) << "an untyped topic has no schema";
}

/// `op`'s append is refused (the ISR is below minIsr): NotLeader, nothing appended.
void makeUncommitted(DdlHarness& h, Op op, const std::string& name) {
    const int64_t end = h.catalogLogEnd();
    h.shrinkIsr();
    const int64_t c = h.submit(op, name);
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kNotLeader);
    h.restoreIsr();
    h.settle();
    ASSERT_EQ(h.catalogLogEnd(), end) << "nothing appended";
    ASSERT_EQ(h.idOf(name), -1) << "an uncommitted create leaves nothing behind";
}

/// F13: `op` appends its event, its commit times out (answered the retriable
/// NotLeader), then the event commits and is applied live.
void makeInDoubt(DdlHarness& h, Op op, const std::string& name) {
    const int64_t c = h.submit(op, name);
    ASSERT_TRUE(h.tickUntilAppended());
    h.advance(AdminWorker::ddlTimeoutNs() + 1'000'000);
    h.tick();
    ASSERT_TRUE(h.response(c).has_value());
    ASSERT_EQ(h.response(c)->err, kNotLeader);
    EXPECT_EQ(h.idOf(name), -1) << "not applied before it commits";
    h.settle();   // releases the held commit: the event commits and applies
    ASSERT_FALSE(h.commitHeld()) << "the in-doubt event committed";
    ASSERT_GE(h.idOf(name), 0) << "the committed event is applied live";
}

void setupOccupant(DdlHarness& h, const Row& row) {
    switch (row.setup) {
        case Setup::None:                        return;
        case Setup::TypedDisk:                   create(h, Op::DefineDisk, row.occupant); return;
        case Setup::TypedTransient:              create(h, Op::DefineTransient, row.occupant); return;
        case Setup::ViewPersisted:               create(h, Op::ViewPersisted, row.occupant); return;
        case Setup::ViewTransient:               create(h, Op::ViewTransient, row.occupant); return;
        case Setup::SchemaLessDisk:              createUntyped(h, row.occupant, 0); return;
        case Setup::SchemaLessTransient:         createUntyped(h, row.occupant, 1); return;
        case Setup::RecoveredTyped:              create(h, Op::DefineDisk, row.occupant); h.restart(); return;
        case Setup::RecoveredView:               create(h, Op::ViewPersisted, row.occupant); h.restart(); return;
        case Setup::UncommittedDefineDisk:       makeUncommitted(h, Op::DefineDisk, row.occupant); return;
        case Setup::UncommittedDefineTransient:  makeUncommitted(h, Op::DefineTransient, row.occupant); return;
        case Setup::UncommittedView:             makeUncommitted(h, Op::ViewPersisted, row.occupant); return;
        case Setup::InDoubtDefine:               makeInDoubt(h, Op::DefineDisk, row.occupant); return;
        case Setup::InDoubtView:                 makeInDoubt(h, Op::ViewPersisted, row.occupant); return;
        case Setup::UncommittedViewThenRestart:
            makeUncommitted(h, Op::ViewPersisted, row.occupant);
            if (::testing::Test::HasFatalFailure()) return;
            create(h, Op::DefineDisk, "later");   // a later topic: the refused create used no id
            h.restart();
            return;
    }
}

void expectSuccessDelta(DdlHarness& h, const Catalog& before, const Catalog& after, Op op, const std::string& name,
                        const Response& r) {
    const CatalogEntry* b = findEntry(before, name);
    const CatalogEntry* a = findEntry(after, name);
    if (isDrop(op)) {
        ASSERT_NE(b, nullptr);
        EXPECT_EQ(a, nullptr) << "a drop frees the name";
        // Review M2: the projections forget it too (catalog() is derived from the
        // applied state, so a stale registry entry would pass unnoticed).
        EXPECT_EQ(h.schemas().byName(name), nullptr) << "no stale SchemaRegistry entry";
        EXPECT_EQ(h.views().byName(name), nullptr) << "no stale ViewRegistry entry";
        const int64_t g = h.submit(Op::GetSchema, name);
        ASSERT_TRUE(h.response(g).has_value());
        EXPECT_EQ(h.response(g)->err, kUnknownTopic) << "GetSchema on the dropped name";
        EXPECT_EQ(after.size(), before.size() - 1);
        EXPECT_EQ(r.version, b->objectVersion + 1) << "the drop event is the object's next version";
    } else {
        ASSERT_NE(a, nullptr) << "the op's entry exists";
    }
    if (isCreate(op)) {
        EXPECT_EQ(a->id, r.id) << "the answer carries the entry's id";
        EXPECT_EQ(after.size(), before.size() + (b ? 0u : 1u)) << "exactly one new topic, or the adopted one";
        if (b) EXPECT_EQ(a->id, b->id) << "adoption keeps the existing topic's id";
        EXPECT_EQ(a->kind, isView(op) ? EntryKind::View : EntryKind::Typed);
        EXPECT_EQ(a->version, 1u);
        EXPECT_EQ(a->objectVersion, b ? b->objectVersion + 1 : 1u) << "a new object is v1; adoption is its next event";
        EXPECT_EQ(a->keyField, "id") << "the requested key field";
        const PersistenceMode want =
            isView(op)  ? (op == Op::ViewTransient ? PersistenceMode::Transient : PersistenceMode::Disk)
            : b         ? b->persistence   // an adopted topic keeps ITS log backend
            : (op == Op::DefineTransient ? PersistenceMode::Transient : PersistenceMode::Disk);
        EXPECT_EQ(a->persistence, want);
    } else if (op == Op::AlterView) {
        ASSERT_NE(b, nullptr);
        EXPECT_EQ(after.size(), before.size());
        EXPECT_NE(a->id, b->id) << "ALTER VIEW: a fresh sink (ruling 4)";
        EXPECT_EQ(a->id, r.id) << "the answer is gated on the fresh sink";
        EXPECT_EQ(a->kind, EntryKind::View);
        EXPECT_EQ(a->objectVersion, b->objectVersion + 1);
        EXPECT_EQ(r.version, a->objectVersion);
        EXPECT_EQ(a->persistence, b->persistence) << "the view keeps its persistence";
        EXPECT_NE(a->fingerprint, b->fingerprint) << "the plan changed";
    } else if (op == Op::AlterTopicConfig) {
        ASSERT_NE(b, nullptr);
        EXPECT_EQ(after.size(), before.size());
        EXPECT_EQ(a->id, b->id);
        EXPECT_EQ(a->kind, b->kind);
        EXPECT_EQ(a->version, b->version) << "a config change is not a schema change";
        EXPECT_EQ(a->objectVersion, b->objectVersion + 1);
        EXPECT_EQ(r.version, a->objectVersion);
        EXPECT_NE(a->fingerprint, b->fingerprint) << "the config is part of the definition";
    } else if (!isDrop(op)) {
        ASSERT_NE(b, nullptr);
        EXPECT_EQ(a->id, r.id) << "the answer carries the entry's id";
        EXPECT_EQ(after.size(), before.size());
        EXPECT_EQ(a->id, b->id);
        EXPECT_EQ(a->version, b->version + 1) << "one contiguous version";
        EXPECT_EQ(a->objectVersion, b->objectVersion + 1);
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
    const uint64_t events = h.eventsAppended();
    const int64_t logEnd = h.catalogLogEnd();
    const int plans = h.viewPlanPushes();

    const int64_t corr = h.submit(op, row.target);
    h.settle();

    const Cell want = expectedCell(row.expect, op);
    const std::optional<Response> got = h.response(corr);
    ASSERT_TRUE(got.has_value()) << "every op is answered";
    EXPECT_EQ(got->err, want.code) << opName(op) << " on " << occupantName(row.expect);
    EXPECT_EQ(h.eventsAppended() - events, want.events) << "__catalog events appended";

    const Catalog after = h.catalog();
    if (want.code != kOk || op == Op::GetSchema) {
        EXPECT_EQ(after, before) << "no half-creation: a rejected op / a read leaves the catalog untouched";
        EXPECT_EQ(h.catalogLogEnd(), logEnd) << "no stray __catalog event";
        EXPECT_EQ(h.viewPlanPushes(), plans) << "no worker install";
    } else {
        expectSuccessDelta(h, before, after, op, row.target, *got);
        EXPECT_EQ(h.catalogLogEnd() - logEnd, 1) << "exactly one event";
        EXPECT_EQ(h.viewPlanPushes() - plans, isView(op) || op == Op::AlterView ? 1 : 0);
    }
    if (op == Op::GetSchema && want.code == kOk) {
        const CatalogEntry* e = findEntry(before, row.target);
        ASSERT_NE(e, nullptr);
        EXPECT_EQ(got->id, e->id);
        EXPECT_EQ(got->version, e->version);
    }
    EXPECT_TRUE(h.violations().empty()) << h.violationText();

    h.restart();
    EXPECT_EQ(h.catalog(), after) << "state after a restart: the catalog is exactly what was applied";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

INSTANTIATE_TEST_SUITE_P(Ddl, DdlOccupantMatrixRegression,
    ::testing::Combine(::testing::ValuesIn(rows()), ::testing::ValuesIn(kAllOps)),
    [](const ::testing::TestParamInfo<std::tuple<Row, Op>>& i) {
        return std::string(std::get<0>(i.param).label) + "_" + opName(std::get<1>(i.param));
    });

// The table's DropTopic column assumes no live view reads the topic; a reader
// refuses it explicitly (Task 9: 39 with the view's name), for every topic kind.
TEST(DdlDropTopicReaderRegression, DropTopicOfAViewSourceIsInvalidRequestNamingTheView) {
    DdlHarness h;
    create(h, Op::DefineTransient, "tsrc");
    create(h, Op::ViewPersisted, "vp");                        // reads src
    if (HasFatalFailure()) return;
    const int64_t tv = h.submitView(Op::ViewTransient, "vt", {"tsrc"});
    h.settle();
    ASSERT_EQ(h.response(tv)->err, kOk);
    for (const auto& [topic, view] : {std::pair{"src", "vp"}, std::pair{"tsrc", "vt"}}) {
        SCOPED_TRACE(topic);
        const Catalog before = h.catalog();
        const int64_t c = h.submit(Op::DropTopic, topic);
        h.settle();
        ASSERT_TRUE(h.response(c).has_value());
        EXPECT_EQ(h.response(c)->err, kInvalidRequest);
        EXPECT_EQ(h.response(c)->detail, std::string("topic '") + topic + "' is the source of view '" + view + "'");
        EXPECT_EQ(h.catalog(), before);
    }
    const int64_t dv = h.submit(Op::DropView, "vp");
    h.settle();
    ASSERT_EQ(h.response(dv)->err, kOk);
    const int64_t ds = h.submit(Op::DropTopic, "src");
    h.settle();
    EXPECT_EQ(h.response(ds)->err, kOk) << "once its reader is dropped, the topic drops";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

// ── Malformed / hand-built admin requests: every one is ANSWERED with an error ─────
class DdlMalformedRequestRegression : public ::testing::Test {
protected:
    void expectAnsweredUntouched(int64_t corr, uint16_t want) {
        ASSERT_TRUE(h.response(corr).has_value()) << "a malformed request is answered, never dropped";
        EXPECT_EQ(h.response(corr)->err, want);
        EXPECT_EQ(h.catalog(), before);
        EXPECT_EQ(h.catalogLogEnd(), logEnd) << "nothing appended";
        EXPECT_TRUE(h.violations().empty()) << h.violationText();
    }
    DdlHarness h;
    Catalog before = h.catalog();
    int64_t logEnd = h.catalogLogEnd();
};

TEST_F(DdlMalformedRequestRegression, DefineWithUnknownPersistenceByteIsInvalidSchema_R1) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id"; m.blob = charBlob(); m.persistence = 2;
    const uint64_t events = h.eventsAppended();
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
    EXPECT_EQ(h.eventsAppended(), events);
}

TEST_F(DdlMalformedRequestRegression, DefineWithGarbageDescriptorIsInvalidSchema) {
    DefineTopicMsg m; m.name = "m"; m.keyField = "id"; m.blob = {0, 1, 2};
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
}

// Catalog history (ruling 3): an EMPTY descriptor is an untyped topic, no longer a
// malformed define — a ring frame the decode could not read is its own
// alternative (MalformedDdl, below), so an empty blob never stands for one.
TEST_F(DdlMalformedRequestRegression, DefineWithEmptyDescriptorIsAnUntypedTopic) {
    DefineTopicMsg m; m.name = "m"; m.partitionCount = 1;
    const int64_t c = h.submitRawDefine(std::move(m));
    h.settle();
    ASSERT_TRUE(h.response(c).has_value());
    EXPECT_EQ(h.response(c)->err, kOk);
    const Catalog cat = h.catalog();
    const CatalogEntry* e = findEntry(cat, "m");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->kind, EntryKind::SchemaLess);
    EXPECT_EQ(h.catalogLogEnd(), logEnd + 1);
    const int64_t bad = h.submitRawMalformed(kTidDefineTopic, "the frame is shorter than its fields");
    h.settle();
    ASSERT_TRUE(h.response(bad).has_value());
    EXPECT_EQ(h.response(bad)->err, kInvalidSchema) << "a malformed define frame";
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
}

TEST_F(DdlMalformedRequestRegression, CreateViewWithUnknownPersistenceByteIsInvalidSchema_R1) {
    CreateViewMsg m; m.viewName = "v"; m.keyField = "id"; m.persistence = 2;
    m.planBlob = viewPlanBlob("v", h.schemas().byName("src")->topicId, 0);
    const uint64_t events = h.eventsAppended();
    const int64_t c = h.submitRawView(std::move(m));
    h.settle();
    expectAnsweredUntouched(c, kInvalidSchema);
    EXPECT_EQ(h.eventsAppended(), events);
}

TEST_F(DdlMalformedRequestRegression, CreateViewMarkedMalformedIsInvalidSchema) {
    const int64_t c = h.submitRawMalformed(kTidCreateView, "an unknown persistence byte");
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

// Was CreateReplyWithUnknownPersistenceIsInternal_R1 (the deleted control-thread
// reply).  The catalog-path counterpart of a malformed hand-off: a v8-only frame
// (templates 74-77) the ring decode could not read is InvalidRequest WITH its reason.
TEST_F(DdlMalformedRequestRegression, MalformedCatalogOpFrameIsInvalidRequestWithItsReason_R1) {
    for (const uint16_t tid : {kTidDropTopic, kTidAlterTopicConfig, kTidDropView, kTidAlterView}) {
        SCOPED_TRACE(tid);
        const int64_t c = h.submitRawMalformed(tid, "trailing bytes after the last field");
        h.settle();
        expectAnsweredUntouched(c, kInvalidRequest);
        EXPECT_EQ(h.response(c)->detail, "malformed request: trailing bytes after the last field");
    }
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
    DdlHarness h;
    createUntyped(h, "ssrc", 0);                 // was a static RUNE_TOPICS topic
    create(h, Op::DefineTransient, "tsrc");
    create(h, Op::ViewPersisted, "vsrc");    // over "src"
    create(h, Op::ViewTransient, "tvsrc");   // over "src"
    if (HasFatalFailure()) return;
    const Catalog before = h.catalog();
    const uint64_t events = h.eventsAppended();
    const int64_t corr = h.submit(c.view, "vw", c.source);
    h.settle();
    ASSERT_TRUE(h.response(corr).has_value());
    EXPECT_EQ(h.response(corr)->err, c.want);
    EXPECT_EQ(h.eventsAppended() - events, c.want == kOk ? 1u : 0u) << "a rejected view appends nothing";
    if (c.want != kOk) EXPECT_EQ(h.catalog(), before);
    EXPECT_TRUE(h.violations().empty()) << h.violationText();
    if (c.want == kViewSourceIsView) {   // the rule holds after a restart (the boot replay rebuilds the views)
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
