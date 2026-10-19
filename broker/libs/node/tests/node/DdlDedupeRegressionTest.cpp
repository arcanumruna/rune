// DdlDedupeRegressionTest.cpp — DDL conformance plan: the KL1 follow-ups on the catalog path (a retried DDL
// is answered its original result) and the deduped-CreateView fingerprint regression
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
