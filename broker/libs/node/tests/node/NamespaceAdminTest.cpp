// NamespaceAdminTest.cpp — namespaces plan Task 7: namespace DDL (81 Create / SetLimits) and ListNamespaces (82)
// through the real AdminWorker over a real __catalog replica (CatalogDdlRig; the leader-change leg on CatalogTrio).
//
// Covers: the full-record Create (absent fields from the boot defaults, resolved at REQUEST time), the duplicate /
// invalid-name / maxNamespaces refusals (52 for a v14 client, 39 below), the idempotent full-record SetLimits,
// the paged list (admin sees all, anyone else only its own), topic DDL landing in a namespace (event nsId + the
// ns-<id> directory), the cross-namespace view refusal, the back-to-back dedupe and a leader change mid-create.

#include "support/CatalogDdlRig.h"
#include "support/CatalogTrio.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/catalog/NamespaceRegistry.h>
#include <rune/node/NamespacePaths.h>
#include <rune/protocol/NamespaceLimits.h>
#include <rune/protocol/client/NamespaceMessages.h>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <set>
#include <string>
#include <vector>

using namespace std::chrono_literals;
using rune::node::test::CatalogDdlRig;
using rune::node::test::CatalogTrio;
namespace cat = rune::catalog;
namespace rp  = rune::protocol;

namespace {

constexpr uint16_t kCodeOk            = 0;
constexpr uint16_t kCodeUnknown       = 7;
constexpr uint16_t kCodeExists        = 25;
constexpr uint16_t kCodeViewSource    = 33;
constexpr uint16_t kCodeInvalid       = 39;
constexpr uint16_t kCodeNsLimit       = 52;

std::array<uint8_t, 4> maskOf(std::initializer_list<std::size_t> fields) {
    std::array<uint8_t, 4> m{};
    for (std::size_t i : fields) m[i >> 3] = static_cast<uint8_t>(m[i >> 3] | (1u << (i & 7)));
    return m;
}
constexpr std::size_t kFieldMaxTopics = 0;
constexpr std::size_t kFieldMaxViews  = 3;

rp::NamespaceLimits defaults(uint64_t topics) {
    rp::NamespaceLimits l;
    l.maxTopics          = topics;
    l.maxViews           = 11;
    l.maxDiskBytes       = 1u << 30;
    l.produceBytesPerSec = 4096;
    l.featureBits        = rp::kNsFeatureViews | rp::kNsFeatureCompaction;
    return l;
}

std::vector<cat::RecordedEvent> eventsOfKind(const std::vector<cat::RecordedEvent>& log, cat::EventKind k) {
    std::vector<cat::RecordedEvent> out;
    for (const auto& r : log) if (r.event.kind == k) out.push_back(r);
    return out;
}

/// A rig serving DDL with these boot defaults / namespace cap (they apply at open: restart, then serve).
void serveWith(CatalogDdlRig& rig, const rp::NamespaceLimits& d, uint32_t maxNamespaces = 1024) {
    rig.setNamespaceDefaults(d);
    rig.setMaxNamespaces(maxNamespaces);
    rig.restart();
    rig.becomeServingLeader();
}

} // namespace

// Create with mask {maxTopics} = 5: the event carries the FULL record — maxTopics 5, every other field from the
// boot defaults — and the first namespace id is 1.  RED (the event built from m.limits alone): maxViews etc. 0.
TEST(NamespaceAdminRegression, CreateRegression) {
    CatalogDdlRig rig("ns_create");
    serveWith(rig, defaults(100));
    rp::NamespaceLimits given;
    given.maxTopics = 5;
    given.maxViews  = 999;   // NOT in the mask: ignored
    const auto a = rig.createNamespace("a", maskOf({kFieldMaxTopics}), given);
    ASSERT_EQ(a.code, kCodeOk) << a.detail;
    EXPECT_EQ(a.objectId, 1u);
    EXPECT_EQ(a.version, 1u);
    const auto evs = eventsOfKind(rig.recordedLog(), cat::EventKind::NamespaceCreated);
    ASSERT_EQ(evs.size(), 1u);
    const cat::CatalogEvent& e = evs[0].event;
    EXPECT_EQ(e.nsId, 1u);
    EXPECT_EQ(e.objectId, 1u);
    EXPECT_EQ(e.objectName, "a");
    const auto& ops = std::get<cat::NamespaceCreatedOps>(e.ops);
    rp::NamespaceLimits expect = defaults(100);
    expect.maxTopics = 5;
    EXPECT_EQ(ops.name, "a");
    EXPECT_EQ(ops.limits, expect);
    const cat::NamespaceRecord* rec = rig.admin().applier().state().namespaces().byName("a");
    ASSERT_NE(rec, nullptr);
    EXPECT_EQ(rec->nsId, 1u);
    EXPECT_EQ(rec->limits, expect);
}

TEST(NamespaceAdminRegression, CreateDuplicateRegression) {
    CatalogDdlRig rig("ns_dup");
    rig.becomeServingLeader();
    ASSERT_EQ(rig.createNamespace("a").code, kCodeOk);
    const auto again = rig.createNamespace("a");   // a NEW request id: not a dedupe hit
    EXPECT_EQ(again.code, kCodeExists) << again.detail;
    EXPECT_EQ(eventsOfKind(rig.recordedLog(), cat::EventKind::NamespaceCreated).size(), 1u);
}

// Any name validNamespaceName refuses is InvalidRequest (39) and appends nothing; the valid rows are created.
TEST(NamespaceAdminRegression, CreateInvalidNameRegression) {
    CatalogDdlRig rig("ns_name");
    rig.becomeServingLeader();
    struct Row { std::string name; bool ok; };
    const std::vector<Row> rows = {
        {"", false},                     {"default", false},          {"Upper", false},
        {"-lead", false},                {"has space", false},        {"a/b", false},
        {"dot.ted", false},              {"under_score", false},      {std::string(64, 'x'), false},
        {"caf\xc3\xa9", false},          {std::string("nu\0l", 4), false}, {"tab\t", false},
        {"a", true},                     {"0", true},                 {"a-b", true},
        {"trail-", true},                {"x9-9", true},              {std::string(63, 'y'), true},
        {"defaultx", true},              {"guild-42", true},
    };
    ASSERT_EQ(rows.size(), 20u);
    std::size_t created = 0;
    for (const auto& r : rows) {
        const auto a = rig.createNamespace(r.name);
        if (r.ok) {
            EXPECT_EQ(a.code, kCodeOk) << "'" << r.name << "': " << a.detail;
            ++created;
        } else {
            EXPECT_EQ(a.code, kCodeInvalid) << "'" << r.name << "'";
        }
    }
    EXPECT_EQ(eventsOfKind(rig.recordedLog(), cat::EventKind::NamespaceCreated).size(), created);
}

// RUNE_MAX_NAMESPACES 2: `default` + `a` exist → creating `b` is 52 for a v14 client, 39 for a v13 one.
// RED (the liveCount check skipped): `b` is created.
TEST(NamespaceAdminRegression, MaxNamespacesRegression) {
    CatalogDdlRig rig("ns_max");
    serveWith(rig, {}, /*maxNamespaces=*/2);
    ASSERT_EQ(rig.createNamespace("a").code, kCodeOk);
    const auto v14 = rig.createNamespace("b", {}, {}, {}, /*clientVersion=*/14);
    EXPECT_EQ(v14.code, kCodeNsLimit) << v14.detail;
    const auto v13 = rig.createNamespace("b", {}, {}, {}, /*clientVersion=*/13);
    EXPECT_EQ(v13.code, kCodeInvalid) << v13.detail;
    EXPECT_EQ(rig.admin().applier().state().namespaces().byName("b"), nullptr);
    EXPECT_EQ(eventsOfKind(rig.recordedLog(), cat::EventKind::NamespaceCreated).size(), 1u);
}

// SetLimits carries the FULL record.  The same request (same rid) twice → one event, the same state; a different rid
// → a second event whose fingerprint is identical (the fingerprint is of the state, not the version).  A missing
// namespace is UnknownTopic (7).
TEST(NamespaceAdminRegression, SetLimitsFullRecordIdempotentRegression) {
    CatalogDdlRig rig("ns_setlim");
    serveWith(rig, defaults(100));
    ASSERT_EQ(rig.createNamespace("a").code, kCodeOk);
    rp::NamespaceLimits given;
    given.maxViews = 3;
    const auto rid = rig.freshRequestId();
    const auto first = rig.setNamespaceLimits("a", maskOf({kFieldMaxViews}), given, rid);
    ASSERT_EQ(first.code, kCodeOk) << first.detail;
    EXPECT_EQ(first.version, 2u);
    const auto stateAfterFirst = *rig.admin().applier().state().namespaces().byName("a");
    const auto dup = rig.setNamespaceLimits("a", maskOf({kFieldMaxViews}), given, rid);
    EXPECT_EQ(dup.code, kCodeOk);
    EXPECT_EQ(dup.version, 2u) << "the original answer";
    EXPECT_EQ(rig.countEventsWithRequestId(rid), 1u);
    const auto stateAfterDup = *rig.admin().applier().state().namespaces().byName("a");
    EXPECT_EQ(stateAfterDup.version, stateAfterFirst.version);
    EXPECT_EQ(stateAfterDup.limits, stateAfterFirst.limits);

    rp::NamespaceLimits expect = defaults(100);
    expect.maxViews = 3;
    EXPECT_EQ(stateAfterFirst.limits, expect) << "the rest from the namespace's current record";

    const auto other = rig.setNamespaceLimits("a", maskOf({kFieldMaxViews}), given);   // a new rid
    ASSERT_EQ(other.code, kCodeOk);
    EXPECT_EQ(other.version, 3u);
    const auto sets = eventsOfKind(rig.recordedLog(), cat::EventKind::NamespaceLimitsSet);
    ASSERT_EQ(sets.size(), 2u);
    EXPECT_EQ(sets[0].event.fingerprint, sets[1].event.fingerprint);
    EXPECT_NE(sets[0].event.fingerprint, 0u);
    EXPECT_EQ(std::get<cat::NamespaceLimitsSetOps>(sets[0].event.ops).limits, expect);
    EXPECT_EQ(std::get<cat::NamespaceLimitsSetOps>(sets[1].event.ops).limits, expect);

    EXPECT_EQ(rig.setNamespaceLimits("nope", maskOf({kFieldMaxViews}), given).code, kCodeUnknown);
}

// Defaults are read at REQUEST time from the broker's (boot) config: a reboot with new defaults between two creates
// gives the second the new defaults while the first keeps what its event carried.
// RED (the default resolution skipped): both events carry the raw (zero) limits.
TEST(NamespaceAdminRegression, DefaultsAtRequestTimeRegression) {
    CatalogDdlRig rig("ns_defaults");
    serveWith(rig, defaults(100));
    ASSERT_EQ(rig.createNamespace("a").code, kCodeOk);
    serveWith(rig, defaults(200));
    ASSERT_EQ(rig.createNamespace("b").code, kCodeOk);
    const auto evs = eventsOfKind(rig.recordedLog(), cat::EventKind::NamespaceCreated);
    ASSERT_EQ(evs.size(), 2u);
    EXPECT_EQ(std::get<cat::NamespaceCreatedOps>(evs[0].event.ops).limits, defaults(100));
    EXPECT_EQ(std::get<cat::NamespaceCreatedOps>(evs[1].event.ops).limits, defaults(200));
    const auto& nss = rig.admin().applier().state().namespaces();
    EXPECT_EQ(nss.byName("a")->limits, defaults(100)) << "a replay never consults the new config";
    EXPECT_EQ(nss.byName("b")->limits, defaults(200));
    EXPECT_EQ(nss.byName("b")->nsId, 2u);
}

// 300 namespaces + default, pages of 100: nextAfter chains to 0; an oversized max is clamped to 100.  A non-admin
// caller sees only its own namespace.
TEST(NamespaceAdminRegression, ListPagingRegression) {
    CatalogDdlRig rig("ns_list");
    serveWith(rig, defaults(100));
    for (int i = 1; i <= 300; ++i) {
        const auto a = rig.createNamespace("n" + std::to_string(i));
        ASSERT_EQ(a.code, kCodeOk) << i << ": " << a.detail;
        ASSERT_EQ(a.objectId, static_cast<uint64_t>(i));
    }
    std::vector<uint32_t> seen;
    uint32_t after = 0;
    int pages = 0;
    for (; pages < 10; ++pages) {
        const auto page = rig.listNamespaces(after, pages == 0 ? 0 : 500);
        ASSERT_TRUE(page.has_value());
        EXPECT_LE(page->namespaces.size(), 100u);
        for (const auto& n : page->namespaces) seen.push_back(n.nsId);
        if (page->nextAfter == 0) { ++pages; break; }
        EXPECT_EQ(page->nextAfter, page->namespaces.back().nsId);
        after = page->nextAfter;
    }
    EXPECT_EQ(pages, 4);
    ASSERT_EQ(seen.size(), 301u);
    for (uint32_t i = 0; i < 301; ++i) EXPECT_EQ(seen[i], i);

    const auto first = rig.listNamespaces(0, 0);
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->namespaces.front().name, "default");
    EXPECT_EQ(first->namespaces.front().limits, defaults(100)) << "the untouched default shows the boot defaults";
    EXPECT_EQ(first->namespaces[5].name, "n5");

    const auto own = rig.listNamespaces(0, 0, /*callerNsId=*/42, /*clusterAdmin=*/false);
    ASSERT_TRUE(own.has_value());
    ASSERT_EQ(own->namespaces.size(), 1u);
    EXPECT_EQ(own->namespaces[0].nsId, 42u);
    EXPECT_EQ(own->namespaces[0].name, "n42");
    EXPECT_EQ(own->nextAfter, 0u);
    const auto past = rig.listNamespaces(42, 0, 42, false);
    ASSERT_TRUE(past.has_value());
    EXPECT_TRUE(past->namespaces.empty());
    EXPECT_EQ(past->nextAfter, 0u);
}

// A topic defined in namespace 1: its event carries nsId 1, it resolves only in ns 1, and its data lives under
// dataDir/ns-1/topics.
TEST(NamespaceAdminRegression, TopicDdlLandsInNsRegression) {
    CatalogDdlRig rig("ns_topic");
    rig.becomeServingLeader();
    ASSERT_EQ(rig.createNamespace("a").objectId, 1u);
    const auto d = rig.define({.name = "t", .partitions = 1, .typed = true, .nsId = 1});
    ASSERT_EQ(d.code, kCodeOk) << d.detail;
    const auto evs = eventsOfKind(rig.recordedLog(), cat::EventKind::CreateTopic);
    ASSERT_EQ(evs.size(), 1u);
    EXPECT_EQ(evs[0].event.nsId, 1u);
    const auto& st = rig.admin().applier().state();
    const cat::TopicObject* t = st.topicByName(1, "t");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->nsId, 1u);
    EXPECT_EQ(st.topicByName(cat::kDefaultNsId, "t"), nullptr) << "names are per namespace";
    EXPECT_EQ(rune::node::topicsRoot(rig.root(), t->nsId), rig.root() / "ns-1" / "topics");
    // The same name in ns 0 is a different topic, not a duplicate.
    EXPECT_EQ(rig.define({.name = "t", .partitions = 1, .typed = true}).code, kCodeOk);
}

// A view in ns 1 over a topic of ns 0 is refused (ViewSourceNotFound) — the admin worker has no principal gate, so
// this holds for every caller, the cluster admin included.  The same view in ns 0 is accepted.
TEST(NamespaceAdminRegression, CrossNsViewRefusedRegression) {
    CatalogDdlRig rig("ns_xview");
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "src", .partitions = 1, .typed = true}).code, kCodeOk);
    ASSERT_EQ(rig.createNamespace("a").code, kCodeOk);
    auto make = [&](uint32_t nsId) {
        rune::node::CreateViewMsg m;
        m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.viewName = "v"; m.keyField = "id";
        m.planBlob = rig.plan("v", "src", false);
        m.meta = rig.meta();
        m.nsId = nsId;
        return m;
    };
    const auto cross = rig.submitRaw(rune::node::DdlRequest{make(1)});
    EXPECT_EQ(cross.code, kCodeViewSource) << cross.detail;
    EXPECT_TRUE(eventsOfKind(rig.recordedLog(), cat::EventKind::CreateView).empty());
    const auto same = rig.submitRaw(rune::node::DdlRequest{make(0)});
    EXPECT_EQ(same.code, kCodeOk) << same.detail;
}

// Batch 2 review: a source topic in another namespace answers EXACTLY like a missing one (spec §1: a foreign object is
// hidden).  A plan names its sources by topic id, which a hostile client picks freely, and AlterView answers with a
// DdlResponse that carries the detail - so a detail naming the foreign topic enumerates another tenant's topic names.
// RED: the "source topic 'secret' is in another namespace" detail.
TEST(NamespaceAdminRegression, CrossNsViewSourceAnswersLikeMissingRegression) {
    namespace rv = rune::view;
    CatalogDdlRig rig("ns_xview_hidden");
    rig.becomeServingLeader();
    ASSERT_EQ(rig.define({.name = "secret", .partitions = 1, .typed = true}).code, kCodeOk);
    ASSERT_EQ(rig.createNamespace("a").code, kCodeOk);
    ASSERT_EQ(rig.define({.name = "own", .partitions = 1, .typed = true, .nsId = 1}).code, kCodeOk);
    const auto& st = rig.admin().applier().state();
    const int32_t foreignId = st.topicByName(cat::kDefaultNsId, "secret")->topicId;
    const int32_t ownId     = st.topicByName(1, "own")->topicId;
    auto planOver = [](int32_t topicId) {
        rv::ViewPlan p;
        p.viewName = "v";
        p.key      = "id";
        p.sources.push_back(rv::SourceSpec{topicId, "id"});
        p.output.push_back(rv::AuthoredField{"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
        p.output.push_back(rv::AuthoredField{"hp", rune::codec::PrimType::Int64, 0, "hp", rv::MergeRule::Replace});
        return rv::serializeAuthored(p);
    };
    rune::node::CreateViewMsg c;
    c.session = rig.sessionId(); c.corr = rig.nextCorr(); c.viewName = "v"; c.keyField = "id";
    c.planBlob = planOver(ownId); c.meta = rig.meta(); c.nsId = 1;
    ASSERT_EQ(rig.submitRaw(rune::node::DdlRequest{c}).code, kCodeOk);
    auto alter = [&](int32_t topicId) {
        rune::node::AlterViewMsg m;
        m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = "v"; m.keyField = "id";
        m.planBlob = planOver(topicId); m.meta = rig.meta(); m.nsId = 1;
        return rig.submitRaw(rune::node::DdlRequest{m});
    };
    const auto foreign = alter(foreignId);
    const auto missing = alter(999'999);
    EXPECT_EQ(foreign.code, kCodeViewSource) << foreign.detail;
    EXPECT_EQ(foreign.code, missing.code);
    EXPECT_EQ(foreign.detail, missing.detail) << "the refusal reveals the foreign topic";
    EXPECT_EQ(foreign.detail.find("secret"), std::string::npos) << foreign.detail;
    EXPECT_TRUE(eventsOfKind(rig.recordedLog(), cat::EventKind::AlterView).empty());
}

// The same Create (one rid) twice back to back, before the first is answered: both answered OK with nsId 1, one
// event.
TEST(NamespaceAdminRegression, BackToBackDedupeRegression) {
    CatalogDdlRig rig("ns_b2b");
    rig.becomeServingLeader();
    const auto rid = rig.freshRequestId();
    const int64_t c1 = rig.enqueue(rune::node::DdlRequest{rig.namespaceMsg(1, "a", {}, {}, rid)});
    const int64_t c2 = rig.enqueue(rune::node::DdlRequest{rig.namespaceMsg(1, "a", {}, {}, rid)});
    for (int i = 0; i < CatalogDdlRig::kCap && !(rig.answerFor(c1) && rig.answerFor(c2)); ++i) rig.pump();
    ASSERT_TRUE(rig.answerFor(c1).has_value());
    ASSERT_TRUE(rig.answerFor(c2).has_value());
    EXPECT_EQ(rig.answerFor(c1)->code, kCodeOk);
    EXPECT_EQ(rig.answerFor(c2)->code, kCodeOk) << rig.answerFor(c2)->detail;
    EXPECT_EQ(rig.answerFor(c1)->objectId, 1u);
    EXPECT_EQ(rig.answerFor(c2)->objectId, 1u);
    EXPECT_EQ(rig.countEventsWithRequestId(rid), 1u);
    EXPECT_EQ(eventsOfKind(rig.recordedLog(), cat::EventKind::NamespaceCreated).size(), 1u);
    EXPECT_EQ(rig.createNamespace("b").objectId, 2u);
}

// The leader appends a Create, the event reaches 2 and 3, then the leader dies before answering.  The client's
// same-rid retry on the new leader answers the original (nsId 1) and appends nothing; the next create is nsId 2.
TEST(NamespaceAdminRegression, LeaderChangeMidCreateRegression) {
    CatalogTrio t("ns_ldr");
    const rp::RequestId rid{.hi = 0x5A5A, .lo = 7};
    t.holdHighWatermark(1);
    const int64_t corr = t.submitCreateNamespaceNoWait(1, "a", rid);
    ASSERT_FALSE(t.answerFor(1, corr).has_value()) << "awaiting its commit";
    t.replicateTo({2, 3});
    t.kill(1);
    t.moveCatalogLeadership(2);
    t.updateIsr({2, 3});
    t.releaseHighWatermark();
    ASSERT_TRUE(t.awaitRole(2, "LeaderServing", 10s));
    const auto retry = t.awaitAnswer(2, t.submitCreateNamespaceNoWait(2, "a", rid));
    EXPECT_EQ(retry.code, kCodeOk) << retry.detail;
    EXPECT_EQ(retry.objectId, 1u) << "the original namespace id";
    EXPECT_EQ(eventsOfKind(t.recordedLog(2), cat::EventKind::NamespaceCreated).size(), 1u);
    EXPECT_EQ(t.countEventsWithRequestId(rid, 2), 1u);
    const cat::NamespaceRecord* rec = t.admin(2).applier().state().namespaces().byName("a");
    ASSERT_NE(rec, nullptr);
    EXPECT_EQ(rec->nsId, 1u);
    const auto next = t.awaitAnswer(2, t.submitCreateNamespaceNoWait(2, "b", {}));
    EXPECT_EQ(next.code, kCodeOk) << next.detail;
    EXPECT_EQ(next.objectId, 2u);
    EXPECT_EQ(eventsOfKind(t.recordedLog(2), cat::EventKind::NamespaceCreated).size(), 2u);
}
