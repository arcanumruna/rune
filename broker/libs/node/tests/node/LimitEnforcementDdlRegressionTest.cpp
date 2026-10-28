// LimitEnforcementDdlRegressionTest.cpp - namespaces Phase 2 Task 9 (D23): the count, durability and feature limits
// enforced by the REAL AdminWorker over a real __catalog replica (CatalogDdlRig), from the LimitEnforcementTable rows
// of the Ddl area; SetLimits below usage (NamespaceLimitsLowerRegression); the check order and the no-event guarantee.
//
// Interaction matrix (limit x {below, at, above} x timing):
//   | limit (row)                 | operation                    | timing                    | expected                                  | test
//   |-----------------------------|------------------------------|---------------------------|-------------------------------------------|-----
//   | every Ddl row               | the row's operation          | sequential                | Below, At ok; Above 52, no event written  | LimitEnforcementTableRegression.*Regression
//   | max-topics lowered below use| SetLimits, CreateTopic, Drop | sequential                | kept, new refused, drop then create ok    | NamespaceLimitsLowerRegression.ExistingKeptNewRefusedRegression
//   | SetLimits + CreateTopic     | both submitted, then replied | back-to-back              | the outcome follows the __catalog order   | NamespaceLimitsLowerRegression.RaceWithCreateRegression
//   | a duplicate name at the cap | DefineTopic of an existing   | sequential                | TopicExists, not the limit                | LimitEnforcementTableRegression.ExistingKeepsItsOwnAnswerRegression
//   | another namespace           | DefineTopic                  | sequential                | unaffected by a full namespace            | LimitEnforcementTableRegression.OtherNamespaceUnaffectedRegression
//   | limit 0                     | any                          | sequential                | unlimited                                 | LimitEnforcementTableRegression.ZeroIsUnlimitedRegression
#include "support/CatalogDdlRig.h"
#include "support/LimitEnforcementTable.h"

#include <rune/catalog/CatalogEvent.h>
#include <rune/protocol/NamespaceLimits.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>

using rune::node::test::CatalogDdlRig;
using namespace rune::node::test;
namespace cat = rune::catalog;
namespace rp  = rune::protocol;

namespace {

constexpr uint16_t kOk = 0, kExists = 25, kLimit = 52;

std::array<uint8_t, 4> maskOf(std::size_t field) {
    std::array<uint8_t, 4> m{};
    m[field >> 3] = static_cast<uint8_t>(1u << (field & 7));
    return m;
}
/// A limits record with ONE field set (index in wire order).
rp::NamespaceLimits limitsWith(std::size_t field, uint64_t v) {
    uint8_t b[rp::kNamespaceLimitsBytes] = {};
    if (field < 28) rp::detail::putLimitU64(b + 8 * field, v);
    else            rp::detail::putLimitU32(b + 28 * 8, static_cast<uint32_t>(v));
    rp::NamespaceLimits l;
    (void)rp::decodeLimits(b, sizeof(b), l);
    return l;
}

std::size_t eventCount(CatalogDdlRig& rig) { return rig.recordedLog().size(); }

/// One namespace `n` with one limit set; its nsId.
uint32_t makeNs(CatalogDdlRig& rig, const std::string& name, std::size_t field, uint64_t v) {
    const auto a = rig.createNamespace(name, maskOf(field), limitsWith(field, v));
    if (a.code != kOk) throw std::runtime_error("createNamespace: " + a.detail);
    return static_cast<uint32_t>(a.objectId);
}

CatalogDdlRig::DefineArgs def(const std::string& name, uint32_t ns, int32_t parts = 1, bool typed = false, int16_t rf = 0) {
    CatalogDdlRig::DefineArgs a;
    a.name = name; a.nsId = ns; a.partitions = parts; a.typed = typed; a.rf = rf;
    return a;
}

/// The three cells, in order, with the observable guarantees.  `fn(cell)` returns the Answer of the cell's operation.
struct Cells {
    CatalogDdlRig& rig;
    int run = 0;
    void ok(const CatalogDdlRig::Answer& a, const char* what) {
        EXPECT_EQ(a.code, kOk) << what << ": " << a.detail;
        ++run;
    }
    void refused(const std::function<CatalogDdlRig::Answer()>& op, const char* what) {
        const std::size_t before = eventCount(rig);
        const auto a = op();
        EXPECT_EQ(a.code, kLimit) << what << ": " << a.detail;
        EXPECT_EQ(eventCount(rig), before) << what << ": a refused DDL wrote an event";
        // Define/Alter answer with a bare ErrorResponse (no detail text); the DdlResponse family carries the reason.
        if (!a.detail.empty()) EXPECT_NE(a.detail.find("namespace limit exceeded"), std::string::npos) << a.detail;
        ++run;
    }
};

}  // namespace

TEST(LimitEnforcementTableRegression, RowsMatchTheFieldIndexRegression) {
    // The table's indices are the wire order of NamespaceLimits (28 u64 then featureBits).
    EXPECT_EQ(rowsOfArea(LimitArea::Ddl) + rowsOfArea(LimitArea::Security) + rowsOfArea(LimitArea::Group) + rowsOfArea(LimitArea::Gateway), 13u) << "the Task 9 rows";
    EXPECT_EQ(rowOf("max-topics")->index, 0u);
    EXPECT_EQ(rowOf("max-users")->index, 7u);
    EXPECT_EQ(rowOf("feature-bits")->index, 28u);
    EXPECT_EQ(rowOf("lowest-min-insync")->index, 27u);
    rp::NamespaceLimits l = limitsWith(rowOf("min-replication-factor")->index, 5);
    EXPECT_EQ(l.minReplicationFactor, 5u);
    l = limitsWith(rowOf("max-replication-factor")->index, 6);
    EXPECT_EQ(l.maxReplicationFactor, 6u);
    l = limitsWith(rowOf("lowest-acks")->index, 7);
    EXPECT_EQ(l.lowestAcks, 7u);
    l = limitsWith(rowOf("feature-bits")->index, 9);
    EXPECT_EQ(l.featureBits, 9u);
    EXPECT_EQ(13u * kCellsPerRow, 39u) << "Task 9 rows x {below, at, above}";
}

TEST(LimitEnforcementTableRegression, CountsRegression) {
    // ---- max-topics: 1st (below), 3rd (at), 4th (above) ----
    {
        const LimitRow& row = *rowOf("max-topics");
        CatalogDdlRig rig("lim_topics"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        Cells c{rig};
        c.ok(rig.define(def("t1", ns)), "below: 1st topic");
        c.ok(rig.define(def("t2", ns)), "2nd topic");
        c.ok(rig.define(def("t3", ns)), "at: 3rd topic (usage == limit)");
        c.refused([&] { return rig.define(def("t4", ns)); }, "above: 4th topic");
        EXPECT_EQ(c.run, 4);
    }
    // ---- max-partitions-per-topic: L-1, L, L+1 ----
    {
        const LimitRow& row = *rowOf("max-partitions-per-topic");
        CatalogDdlRig rig("lim_ppt"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        Cells c{rig};
        c.ok(rig.define(def("p3", ns, 3)), "below: L-1 partitions");
        c.ok(rig.define(def("p4", ns, 4)), "at: L partitions");
        c.refused([&] { return rig.define(def("p5", ns, 5)); }, "above: L+1 partitions");
        EXPECT_EQ(c.run, 3);
    }
    // ---- max-partitions-total: 2 + 2 (below) -> 6 (at) -> 7 (above) ----
    {
        const LimitRow& row = *rowOf("max-partitions-total");
        CatalogDdlRig rig("lim_ptot"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        Cells c{rig};
        c.ok(rig.define(def("a", ns, 2)), "below: 2 of 6");
        c.ok(rig.define(def("b", ns, 2)), "4 of 6");
        c.ok(rig.define(def("c", ns, 2)), "at: 6 of 6");
        c.refused([&] { return rig.define(def("d", ns, 1)); }, "above: 7 of 6");
        EXPECT_EQ(c.run, 4);
    }
    // ---- max-views ----
    {
        const LimitRow& row = *rowOf("max-views");
        CatalogDdlRig rig("lim_views"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        ASSERT_EQ(rig.define(def("src", ns, 1, true)).code, kOk);
        Cells c{rig};
        c.ok(rig.createView("v1", "src", false, 0, ns), "below: 1st view");
        c.ok(rig.createView("v2", "src", false, 0, ns), "at: 2nd view");
        c.refused([&] { return rig.createView("v3", "src", false, 0, ns); }, "above: 3rd view");
        EXPECT_EQ(c.run, 3);
    }
    // ---- max-schemas: a typed topic is a schema; untyped ones are not counted ----
    {
        const LimitRow& row = *rowOf("max-schemas");
        CatalogDdlRig rig("lim_schemas"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        ASSERT_EQ(rig.define(def("untyped1", ns)).code, kOk);   // not a schema
        ASSERT_EQ(rig.define(def("untyped2", ns)).code, kOk);
        Cells c{rig};
        c.ok(rig.define(def("s1", ns, 1, true)), "below: 1st schema");
        c.ok(rig.define(def("s2", ns, 1, true)), "at: 2nd schema");
        c.refused([&] { return rig.define(def("s3", ns, 1, true)); }, "above: 3rd schema");
        // Adopting a schema onto an untyped topic is a schema too.
        c.refused([&] { return rig.define(def("untyped1", ns, 1, true)); }, "above: adoption is a schema");
        EXPECT_EQ(c.run, 4);
    }
    // ---- max-versions-per-schema: v1 at create; add -> v2 (below), v3 (at), v4 (above) ----
    {
        const LimitRow& row = *rowOf("max-versions-per-schema");
        CatalogDdlRig rig("lim_versions"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        ASSERT_EQ(rig.define(def("x", ns, 1, true)).code, kOk);
        Cells c{rig};
        c.ok(rig.alterAdd("x", "f1", {}, 0, ns), "below: version 2 of 3");
        c.ok(rig.alterAdd("x", "f2", {}, 0, ns), "at: version 3 of 3");
        c.refused([&] { return rig.alterAdd("x", "f3", {}, 0, ns); }, "above: version 4 of 3");
        EXPECT_EQ(c.run, 3);
    }
}

TEST(LimitEnforcementTableRegression, DurabilityRegression) {
    {   // min-replication-factor 2 (the rig has 3 brokers)
        const LimitRow& row = *rowOf("min-replication-factor");
        CatalogDdlRig rig("lim_minrf"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        Cells c{rig};
        c.ok(rig.define(def("r3", ns, 1, false, 3)), "below: rf 3 > min");
        c.ok(rig.define(def("r2", ns, 1, false, 2)), "at: rf 2 == min");
        c.ok(rig.define(def("r0", ns, 1, false, 0)), "rf 0 = every broker (3) >= min");
        c.refused([&] { return rig.define(def("r1", ns, 1, false, 1)); }, "above: rf 1 < min");
        EXPECT_EQ(c.run, 4);
    }
    {   // max-replication-factor 2
        const LimitRow& row = *rowOf("max-replication-factor");
        CatalogDdlRig rig("lim_maxrf"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        Cells c{rig};
        c.ok(rig.define(def("r1", ns, 1, false, 1)), "below: rf 1 < max");
        c.ok(rig.define(def("r2", ns, 1, false, 2)), "at: rf 2 == max");
        c.refused([&] { return rig.define(def("r3", ns, 1, false, 3)); }, "above: rf 3 > max");
        c.refused([&] { return rig.define(def("r0", ns, 1, false, 0)); }, "above: rf 0 = every broker (3) > max");
        EXPECT_EQ(c.run, 4);
    }
    // lowest-min-insync 2: the cluster min ISR of the rig (a topic carries no min.insync of its own)
    for (const auto& [isr, okExpected] : {std::pair{3, true}, std::pair{2, true}, std::pair{1, false}}) {
        const LimitRow& row = *rowOf("lowest-min-insync");
        CatalogDdlRig rig("lim_minisr", 3, isr); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        Cells c{rig};
        if (okExpected) c.ok(rig.define(def("t", ns, 1, false, 3)), "min ISR >= limit");
        else            c.refused([&] { return rig.define(def("t", ns, 1, false, 3)); }, "min ISR below the limit");
        EXPECT_EQ(c.run, 1) << "min ISR " << isr;
    }
}

TEST(LimitEnforcementTableRegression, FeaturesRegression) {
    const LimitRow& row = *rowOf("feature-bits");
    {   // Compaction: 0 = unrestricted (below), exactly Compaction (at), Views only (above)
        CatalogDdlRig rig("lim_feat_compact"); rig.becomeServingLeader();
        auto compacted = [&](const std::string& n, uint32_t ns) {
            auto a = def(n, ns, 1, false, 1);
            a.config.cleanupPolicy = 1;
            return a;
        };
        const uint32_t unrestricted = makeNs(rig, "u", row.index, 0);
        const uint32_t atBits       = makeNs(rig, "c", row.index, rp::kNsFeatureCompaction);
        const uint32_t aboveBits    = makeNs(rig, "v", row.index, rp::kNsFeatureViews);
        Cells c{rig};
        c.ok(rig.define(compacted("t", unrestricted)), "below: featureBits 0 = unrestricted");
        c.ok(rig.define(compacted("t", atBits)), "at: Compaction allowed");
        c.refused([&] { return rig.define(compacted("t", aboveBits)); }, "above: Compaction not allowed");
        // A delete-policy topic needs no feature.
        c.ok(rig.define(def("plain", aboveBits, 1, false, 1)), "a non-compacted topic needs no feature");
        EXPECT_EQ(c.run, 4);
    }
    {   // Views
        CatalogDdlRig rig("lim_feat_views"); rig.becomeServingLeader();
        const uint32_t allowed = makeNs(rig, "v", row.index, rp::kNsFeatureViews);
        const uint32_t denied  = makeNs(rig, "c", row.index, rp::kNsFeatureCompaction);
        ASSERT_EQ(rig.define(def("src", allowed, 1, true)).code, kOk);
        ASSERT_EQ(rig.define(def("src", denied, 1, true)).code, kOk);
        Cells c{rig};
        c.ok(rig.createView("v1", "src", false, 0, allowed), "at: Views allowed");
        c.refused([&] { return rig.createView("v1", "src", false, 0, denied); }, "above: Views not allowed");
        EXPECT_EQ(c.run, 2);
    }
}

TEST(LimitEnforcementTableRegression, ExistingKeepsItsOwnAnswerRegression) {
    CatalogDdlRig rig("lim_order"); rig.becomeServingLeader();
    const uint32_t ns = makeNs(rig, "a", 0, 1);
    ASSERT_EQ(rig.define(def("t", ns)).code, kOk);
    // At the cap, a create of the SAME name is TopicExists (the specific answer), a new name is the limit.
    EXPECT_EQ(rig.define(def("t", ns)).code, kExists);
    EXPECT_EQ(rig.define(def("u", ns)).code, kLimit);
}

TEST(LimitEnforcementTableRegression, OtherNamespaceUnaffectedRegression) {
    CatalogDdlRig rig("lim_other"); rig.becomeServingLeader();
    const uint32_t a = makeNs(rig, "a", 0, 1);
    const uint32_t b = makeNs(rig, "b", 0, 1);
    ASSERT_EQ(rig.define(def("t", a)).code, kOk);
    EXPECT_EQ(rig.define(def("u", a)).code, kLimit);
    EXPECT_EQ(rig.define(def("t", b)).code, kOk) << "b's count is its own";
    EXPECT_EQ(rig.define(def("t", 0)).code, kOk) << "default is unlimited";
}

TEST(LimitEnforcementTableRegression, ZeroIsUnlimitedRegression) {
    CatalogDdlRig rig("lim_zero"); rig.becomeServingLeader();
    const auto a = rig.createNamespace("a");   // every limit the (zero) default
    ASSERT_EQ(a.code, kOk);
    const uint32_t ns = static_cast<uint32_t>(a.objectId);
    for (int i = 0; i < 12; ++i) EXPECT_EQ(rig.define(def("t" + std::to_string(i), ns, 3, true, 1)).code, kOk) << i;
}

// ---- SetLimits below usage: accepted, nothing deleted, new creates refused -----------------------------------------
TEST(NamespaceLimitsLowerRegression, ExistingKeptNewRefusedRegression) {
    CatalogDdlRig rig("lim_lower"); rig.becomeServingLeader();
    const auto a = rig.createNamespace("a");
    ASSERT_EQ(a.code, kOk);
    const uint32_t ns = static_cast<uint32_t>(a.objectId);
    for (int i = 0; i < 10; ++i) ASSERT_EQ(rig.define(def("t" + std::to_string(i), ns)).code, kOk);
    const std::size_t topicsBefore = rig.admin().applier().state().topics().size();
    const auto set = rig.setNamespaceLimits("a", maskOf(0), limitsWith(0, 5));   // below the 10 in use
    ASSERT_EQ(set.code, kOk) << set.detail;
    EXPECT_EQ(rig.admin().applier().state().topics().size(), topicsBefore) << "lowering deleted nothing";
    for (int i = 0; i < 10; ++i)
        EXPECT_NE(rig.admin().applier().state().topicByName(ns, "t" + std::to_string(i)), nullptr) << i;
    EXPECT_EQ(rig.define(def("new", ns)).code, kLimit) << "a new create is refused while over the limit";
    for (int i = 0; i < 5; ++i) ASSERT_EQ(rig.dropTopic("t" + std::to_string(i), ns).code, kOk);
    EXPECT_EQ(rig.define(def("new", ns)).code, kLimit) << "5 left == the limit: still refused (at)";
    ASSERT_EQ(rig.dropTopic("t5", ns).code, kOk);
    EXPECT_EQ(rig.define(def("new", ns)).code, kOk) << "4 left < 5: the create succeeds";
}

// ---- SetLimits and CreateTopic back to back before either reply -----------------------------------------------------
TEST(NamespaceLimitsLowerRegression, RaceWithCreateRegression) {
    for (const bool limitFirst : {true, false}) {
        CatalogDdlRig rig(limitFirst ? "lim_race_a" : "lim_race_b"); rig.becomeServingLeader();
        const auto a = rig.createNamespace("a");
        ASSERT_EQ(a.code, kOk);
        const uint32_t ns = static_cast<uint32_t>(a.objectId);
        ASSERT_EQ(rig.define(def("t0", ns)).code, kOk);   // usage 1
        // maxTopics := 1 (usage already 1) and a CreateTopic, in both orders, submitted before either is answered.
        rune::node::DdlRequest setReq{rig.namespaceMsg(2, "a", maskOf(0), limitsWith(0, 1))};
        CatalogDdlRig::DefineArgs d = def("t1", ns);
        const std::size_t before = eventCount(rig);
        int64_t cSet = 0, cDef = 0;
        if (limitFirst) { cSet = rig.enqueue(std::move(setReq)); cDef = rig.submitDefineNoWait(d); }
        else            { cDef = rig.submitDefineNoWait(d); cSet = rig.enqueue(std::move(setReq)); }
        for (int i = 0; i < CatalogDdlRig::kCap && !(rig.answerFor(cSet) && rig.answerFor(cDef)); ++i) rig.pump();
        ASSERT_TRUE(rig.answerFor(cSet) && rig.answerFor(cDef));
        EXPECT_EQ(rig.answerFor(cSet)->code, kOk);
        // The outcome matches the order on __catalog: the create, if it came before the limit, is in; after it, refused.
        const bool created = rig.admin().applier().state().topicByName(ns, "t1") != nullptr;
        EXPECT_EQ(created, rig.answerFor(cDef)->code == kOk);
        // Replay the committed log: the SetLimits and the create are in the same order as the verdict says.
        const auto log = rig.recordedLog();
        std::size_t setAt = SIZE_MAX, defAt = SIZE_MAX;
        for (std::size_t i = before; i < log.size(); ++i) {
            if (log[i].event.kind == cat::EventKind::NamespaceLimitsSet) setAt = i;
            if (log[i].event.kind == cat::EventKind::CreateTopic) defAt = i;
        }
        ASSERT_NE(setAt, SIZE_MAX);
        if (created) EXPECT_LT(defAt, setAt) << "the create won only if it committed before the new limit";
        else         EXPECT_EQ(defAt, SIZE_MAX);
    }
}

// ---- Task 13 (D27): retention above the namespace maximum is refused at DDL; SetLimits warns about the clamp ------------
TEST(LimitEnforcementTableRegression, RetentionRegression) {
    using rune::protocol::ConfigKey;
    for (const char* field : {"max-retention-ms", "max-retention-bytes"}) {
        const LimitRow& row = *rowOf(field);
        const bool ms = std::string(field) == "max-retention-ms";
        CatalogDdlRig rig(ms ? "lim_ret_ms" : "lim_ret_bytes"); rig.becomeServingLeader();
        const uint32_t ns = makeNs(rig, "a", row.index, row.limit);
        Cells c{rig};
        auto withRetention = [&](const std::string& name, int64_t v) {
            auto a = def(name, ns);
            (ms ? a.config.retentionMs : a.config.retentionBytes) = v;
            return a;
        };
        const auto L = static_cast<int64_t>(row.limit);
        c.ok(rig.define(withRetention("below", L - 1)), "below: retention == L-1");
        c.ok(rig.define(withRetention("at", L)), "at: retention == L");
        c.refused([&] { return rig.define(withRetention("above", L + 1)); }, "above: retention == L+1");
        c.ok(rig.define(def("unset", ns)), "an unset (-1, unbounded) retention is accepted: the cleaner clamps it");
        const ConfigKey key = ms ? ConfigKey::RetentionMs : ConfigKey::RetentionBytes;
        c.ok(rig.alterConfig("below", {{key, L}}, 0, ns), "alter to == L");
        c.refused([&] { return rig.alterConfig("below", {{key, L + 1}}, 0, ns); }, "alter to L+1");
        EXPECT_EQ(c.run, 6);
        // Another namespace without the limit is unaffected.
        const auto b = rig.createNamespace("b");
        ASSERT_EQ(b.code, kOk);
        auto big = def("big", static_cast<uint32_t>(b.objectId));
        (ms ? big.config.retentionMs : big.config.retentionBytes) = L * 1000;
        EXPECT_EQ(rig.define(big).code, kOk);
    }
}

TEST(LimitEnforcementTableRegression, SetLimitsWarnsWhenRetentionIsClampedRegression) {
    CatalogDdlRig rig("lim_ret_warn"); rig.becomeServingLeader();
    const auto a = rig.createNamespace("a");
    ASSERT_EQ(a.code, kOk);
    const uint32_t ns = static_cast<uint32_t>(a.objectId);
    for (int i = 0; i < 3; ++i) ASSERT_EQ(rig.define(def("t" + std::to_string(i), ns)).code, kOk);   // unbounded retention
    const auto set = rig.setNamespaceLimits("a", maskOf(9), limitsWith(9, 60000));
    ASSERT_EQ(set.code, kOk) << set.detail;
    EXPECT_EQ(set.detail, "retention clamped on 3 topics");
    // Raising it above every topic's retention (none left unbounded to clamp is impossible: -1 stays unbounded) or
    // clearing it leaves no warning.
    const auto clear = rig.setNamespaceLimits("a", maskOf(9), limitsWith(9, 0));
    ASSERT_EQ(clear.code, kOk);
    EXPECT_TRUE(clear.detail.empty()) << clear.detail;
    // An unrelated limit change carries no warning.
    const auto other = rig.setNamespaceLimits("a", maskOf(0), limitsWith(0, 100));
    ASSERT_EQ(other.code, kOk);
    EXPECT_TRUE(other.detail.empty());
}
