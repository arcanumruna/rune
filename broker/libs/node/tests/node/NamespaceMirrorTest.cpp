// NamespaceMirrorTest.cpp - namespaces Task 5: the broker's name mirrors are keyed by (nsId, name).
//
// A CatalogApplier driven over a formatVer-2 log with the SAME topic and view names in namespace 0 and namespace 1:
// the control-thread ObjectIndex (fed by the applier's effects), the SchemaRegistry and the ViewRegistry (the
// applier's projections) must each resolve both, and a restart that rebuilds from the checkpoint (the coalesced boot
// sink) must give the same answers.
#include "support/CatalogRig.h"

#include <rune/catalog/testing/GoldenScript.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <gtest/gtest.h>

using namespace rune::node;
using namespace rune::node::test;
namespace cat = rune::catalog;
namespace cd  = rune::codec;

namespace {

cat::CreateTopicOps typedTopic(const char* name) {
    cd::DescriptorBuilder d(name);
    d.add("id", cd::PrimType::Int64, 1, /*isKey=*/true).add("hp", cd::PrimType::Int32);
    cat::CreateTopicOps o;
    o.partitions = 1;
    o.replicationFactor = 1;
    o.schema = cat::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = cd::serialize(d.build()),
                              .nextTag = 2};
    return o;
}

cat::ViewDef viewOver(const char* source) {
    cd::DescriptorBuilder out("v");
    out.add("id", cd::PrimType::Int64, 1, true);
    cat::ViewDef v;
    v.sinkPartitions = 1;
    v.keyField = "id";
    v.sourceNames = {source};
    v.normalizedPlanBlob = {0x05, 0x00, 0x00, 0x00, 0x00};
    v.compiledPlanBlob = {0xC5, 0x00, 0x00, 0x00, 0x00};
    v.outputDescriptorBlob = cd::serialize(out.build());
    return v;
}

/// 0 NamespaceCreated alpha(1)  1 CreateTopic t (ns 0)  2 CreateTopic t (ns 1)  3 CreateView v (ns 0)
/// 4 CreateView v (ns 1)  5 DropView v (ns 1)  6 DropTopic t (ns 1)   (the view reads t: it goes first).
std::vector<cat::CatalogEvent> script() {
    rune::catalog::testing::golden_detail::ScriptBuilder b(cat::kFormatV2);
    rune::protocol::NamespaceLimits l;
    l.maxTopics = 10;
    b.nsCreate("alpha", l, "CREATE NAMESPACE alpha");
    b.inNs(0);
    b.createTopic("t", typedTopic("t"), "CREATE TOPIC t");
    b.inNs(1);
    b.createTopic("t", typedTopic("t"), "CREATE TOPIC t");
    b.inNs(0);
    b.createView("v", viewOver("t"), "CREATE VIEW v");
    b.inNs(1);
    b.createView("v", viewOver("t"), "CREATE VIEW v");
    b.dropView("v", "DROP VIEW v");
    b.dropTopic("t", "DROP TOPIC t");
    return std::move(b).take();
}

constexpr std::size_t kBeforeDrops = 5;

struct Ids {
    int32_t topic[2]{-1, -1};
    int32_t sink[2]{-1, -1};
};

/// The ids the catalog state assigned - the ground truth every mirror must reproduce.
Ids truth(const cat::CatalogState& s) {
    Ids ids;
    for (uint32_t ns = 0; ns < 2; ++ns) {
        if (const auto* t = s.topicByName(ns, "t")) ids.topic[ns] = t->topicId;
        if (const auto* v = s.viewByName(ns, "v")) ids.sink[ns] = v->def.viewTopicId;
    }
    return ids;
}

void expectMirrors(CatalogRig& rig, const Ids& want) {
    for (uint32_t ns = 0; ns < 2; ++ns) {
        SCOPED_TRACE("ns " + std::to_string(ns));
        ASSERT_GE(want.topic[ns], 0);
        ASSERT_GE(want.sink[ns], 0);
        // The control-thread ObjectIndex.
        ASSERT_TRUE(rig.rec.objects.byName(ns, "t").has_value());
        EXPECT_EQ(*rig.rec.objects.byName(ns, "t"), want.topic[ns]);
        ASSERT_TRUE(rig.rec.objects.byName(ns, "v").has_value());
        EXPECT_EQ(*rig.rec.objects.byName(ns, "v"), want.sink[ns]);
        EXPECT_EQ(rig.rec.objects.at(want.topic[ns])->nsId, ns);
        // The SchemaRegistry: the topic's schema and the view's typed output.
        const RegistryEntry* st = rig.schemas.byName(ns, "t");
        ASSERT_NE(st, nullptr);
        EXPECT_EQ(st->topicId, want.topic[ns]);
        EXPECT_EQ(st->nsId, ns);
        const RegistryEntry* sv = rig.schemas.byName(ns, "v");
        ASSERT_NE(sv, nullptr);
        EXPECT_EQ(sv->topicId, want.sink[ns]);
        // The ViewRegistry.
        const ViewRecord* vr = rig.views.byName(ns, "v");
        ASSERT_NE(vr, nullptr);
        EXPECT_EQ(vr->viewTopicId, want.sink[ns]);
        EXPECT_EQ(vr->nsId, ns);
    }
    EXPECT_NE(want.topic[0], want.topic[1]) << "ids stay global: one id per (nsId, name)";
    // A namespace that has no such name resolves nothing.
    EXPECT_FALSE(rig.rec.objects.byName(2, "t").has_value());
    EXPECT_EQ(rig.schemas.byName(2, "t"), nullptr);
    EXPECT_EQ(rig.views.byName(2, "v"), nullptr);
}

} // namespace

TEST(NamespaceMirrorRegression, EffectsCarryNsIdRegression) {
    CatalogRig rig("ns_mirror");
    const auto events = script();
    for (std::size_t i = 0; i < kBeforeDrops; ++i) rig.appendEventCommitted(events[i]);
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), static_cast<int>(kBeforeDrops));
    ASSERT_FALSE(rig.applier.halted()) << (rig.rec.halts.empty() ? "" : rig.rec.halts.back().detail);

    const Ids want = truth(rig.applier.state());
    expectMirrors(rig, want);

    // The live namespace effect carried alpha's record.
    ASSERT_EQ(rig.rec.nsUpserts.size(), 1u);
    EXPECT_EQ(rig.rec.nsUpserts[0].nsId, 1u);
    EXPECT_EQ(rig.rec.nsUpserts[0].name, "alpha");
    EXPECT_EQ(rig.rec.nsUpserts[0].limits.maxTopics, 10u);

    // Restart: the coalesced boot sink rebuilds the same mirrors from the checkpoint.
    rig.restart();
    ASSERT_GE(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 0);
    ASSERT_FALSE(rig.applier.halted());
    expectMirrors(rig, want);
    ASSERT_EQ(rig.rec.nsUpserts.size(), 2u) << "boot posts every namespace once: default and alpha, before any object";
    EXPECT_EQ(rig.rec.nsUpserts[0].nsId, 0u);
    EXPECT_EQ(rig.rec.nsUpserts[1].nsId, 1u);
}

// A drop in one namespace leaves the same name in the other namespace untouched, in every mirror.
TEST(NamespaceMirrorRegression, DropInOneNamespaceKeepsTheOtherRegression) {
    CatalogRig rig("ns_mirror_drop");
    const auto events = script();
    for (const auto& e : events) rig.appendEventCommitted(e);
    ASSERT_EQ(rig.applier.applyCommitted(rig.replica()), static_cast<int>(events.size()));
    ASSERT_FALSE(rig.applier.halted()) << (rig.rec.halts.empty() ? "" : rig.rec.halts.back().detail);

    auto check = [&](CatalogRig& r) {
        EXPECT_TRUE(r.rec.objects.byName(0, "t").has_value());
        EXPECT_TRUE(r.rec.objects.byName(0, "v").has_value());
        EXPECT_FALSE(r.rec.objects.byName(1, "t").has_value());
        EXPECT_FALSE(r.rec.objects.byName(1, "v").has_value());
        EXPECT_NE(r.schemas.byName(0, "t"), nullptr);
        EXPECT_NE(r.views.byName(0, "v"), nullptr);
        EXPECT_EQ(r.schemas.byName(1, "t"), nullptr);
        EXPECT_EQ(r.schemas.byName(1, "v"), nullptr);
        EXPECT_EQ(r.views.byName(1, "v"), nullptr);
    };
    check(rig);
    rig.restart();
    ASSERT_GE(rig.applier.replayLocal(rig.replica(), rig.rec.bootEffects()), 0);
    check(rig);
}
