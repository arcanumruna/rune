// Catalog history (spec §5.3, §6): the frozen formatVer-1 apply rules + fingerprints.
#include <rune/catalog/CatalogState.h>
#include <rune/catalog/CatalogFingerprint.h>
#include <rune/codec/AlterOps.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <gtest/gtest.h>
#include <limits>
#include <optional>

namespace rc = rune::catalog;
namespace cd = rune::codec;

namespace {
std::vector<uint8_t> blob2() {   // {id int64 key, hp int32}
    cd::DescriptorBuilder b("players");
    b.add("id", cd::PrimType::Int64, /*sinceVersion=*/1, /*isKey=*/true);
    b.add("hp", cd::PrimType::Int32);
    return cd::serialize(b.build());
}

/// The leader's way of building an event: fill everything, then stamp the
/// fingerprint the library previews — exactly what AdminWorker does (Task 9).
rc::CatalogEvent stamped(const rc::CatalogState& s, rc::Ops ops, std::string name, uint64_t objectId,
                         uint32_t prev, uint32_t next) {
    rc::CatalogEvent e;
    e.kind = rc::kindOf(ops); e.ops = std::move(ops); e.objectName = std::move(name);
    e.objectId = objectId; e.prevVersion = prev; e.newVersion = next;
    auto fp = rc::previewFingerprint(s, e);
    EXPECT_TRUE(fp.has_value()) << (fp ? "" : fp.error().detail);
    e.fingerprint = fp.value_or(0);
    return e;
}
rc::CreateTopicOps createOps(int32_t topicId, bool typed) {
    rc::CreateTopicOps o; o.topicId = topicId; o.partitions = 2; o.replicationFactor = 3;
    if (typed) o.schema = rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = blob2(), .nextTag = 2};
    return o;
}
} // namespace

TEST(CatalogApplyV1Regression, CreateTopicAssignsDenseIdsAndVersionOne) {
    rc::CatalogState s;
    auto e = stamped(s, createOps(0, true), "players", 1, 0, 1);
    auto a = rc::apply(s, e, 0);
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    const rc::TopicObject* t = s.topicByName("players");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->topicId, 0); EXPECT_EQ(t->version, 1u); EXPECT_EQ(t->schema->schemaVersion, 1);
    EXPECT_EQ(s.nextObjectId(), 2u); EXPECT_EQ(s.nextTopicId(), 1); EXPECT_EQ(s.appliedOffset(), 0);
    EXPECT_EQ(a->createdTopicId, 0);
}

TEST(CatalogApplyV1Regression, FingerprintMismatchIsAnErrorAndLeavesStateUnchanged) {
    rc::CatalogState s;
    auto e = stamped(s, createOps(0, false), "t", 1, 0, 1);
    e.fingerprint ^= 1;
    const uint64_t before = s.digest();
    auto a = rc::apply(s, e, 5);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::FingerprintMismatch);
    EXPECT_EQ(a.error().offset, 5);
    EXPECT_EQ(a.error().expected, e.fingerprint);
    EXPECT_NE(a.error().actual, e.fingerprint);
    EXPECT_EQ(s.digest(), before);
    EXPECT_EQ(s.topicByName("t"), nullptr);
}

TEST(CatalogApplyV1Regression, EveryPreconditionIsExplicit) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, false), "t", 1, 0, 1), 0));
    struct Case { const char* what; rc::CatalogEvent e; };
    std::vector<Case> cases;
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(1, false); e.objectName = "t";
      e.objectId = 2; e.newVersion = 1; cases.push_back({"name taken", e}); }   // unstamped (F10): no preview of an invalid event
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(7, false); e.objectName = "u";
      e.objectId = 2; e.newVersion = 1; cases.push_back({"topicId != nextTopicId", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::CreateTopic; e.ops = createOps(1, false); e.objectName = "u";
      e.objectId = 9; e.newVersion = 1; cases.push_back({"objectId != nextObjectId", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::DropTopic; e.ops = rc::DropTopicOps{0}; e.objectName = "t";
      e.objectId = 1; e.prevVersion = 3; e.newVersion = 4; cases.push_back({"version chain broken", e}); }
    { rc::CatalogEvent e; e.kind = rc::EventKind::AlterSchema;
      e.ops = rc::AlterSchemaOps{{rc::ResolvedAlterOp{.type = cd::AlterOpType::Drop, .tag = 0}}}; e.objectName = "t";
      e.objectId = 1; e.prevVersion = 1; e.newVersion = 2; cases.push_back({"alter of an untyped topic", e}); }
    for (auto& c : cases) {
        const uint64_t before = s.digest();
        auto a = rc::apply(s, c.e, 1);
        ASSERT_FALSE(a.has_value()) << c.what;
        EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition) << c.what;
        EXPECT_EQ(s.digest(), before) << c.what;
    }
}

TEST(CatalogApplyV1Regression, SchemaLifecycleOnOneObject) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, false), "t", 1, 0, 1), 0));            // untyped
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::CreateSchemaOps{{.keyField = "id", .validate = false,
        .descriptorBlob = blob2(), .nextTag = 2}}, "t", 1, 1, 2), 1));                     // CreateSchema
    cd::AlterOp add{}; add.type = cd::AlterOpType::Add; add.name = "mana"; add.primType = cd::PrimType::Int32;
    auto resolved = rc::resolveAlterOps(*s.topicByName("t")->schema, std::vector<cd::AlterOp>{add});
    ASSERT_TRUE(resolved.has_value()) << resolved.error();
    ASSERT_EQ(resolved->size(), 1u);
    EXPECT_EQ((*resolved)[0].tag, 2) << "an ADD is stored with the durable tag it is assigned (F20)";
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::AlterSchemaOps{*resolved}, "t", 1, 2, 3), 2));
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::SetCompatibilityOps{cd::CompatMode::Forward}, "t", 1, 3, 4), 3));
    const rc::TopicObject* t = s.topicByName("t");
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->objectId, 1u); EXPECT_EQ(t->version, 4u);
    EXPECT_EQ(t->schema->schemaVersion, 3);   // create 1, alter 2, set-compat 3
    EXPECT_EQ(t->schema->compat, cd::CompatMode::Forward);
    EXPECT_EQ(t->schema->nextTag, 3);
}

TEST(CatalogApplyV1Regression, SetCompatibilityBumpsTheEvolvingDescriptorVersionLikeTodayRegression) {
    // F9: today's startAlter_ runs applyOps for SET COMPATIBILITY, which stamps the evolving
    // descriptor with the new schema version (Evolve.cpp) — projections and GetTopicSchema see it.
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::SetCompatibilityOps{cd::CompatMode::Forward}, "t", 1, 1, 2), 1));
    const rc::TopicObject* t = s.topicByName("t");
    EXPECT_EQ(t->schema->schemaVersion, 2);
    EXPECT_EQ(cd::deserialize(t->schema->evolvingBlob).version, 2) << "the evolving blob went through applyOps";
}

TEST(CatalogApplyV1Regression, ResolvedOpsNameFieldsByDurableTagAndRoundTrip) {
    // F20: a RENAME / DROP is stored by tag; unresolving on the same definition gives the names back.
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    cd::AlterOp ren{}; ren.type = cd::AlterOpType::Rename; ren.name = "hp"; ren.newName = "health";
    cd::AlterOp drop{}; drop.type = cd::AlterOpType::Drop; drop.name = "health";   // sees the rename before it
    const auto& schema = *s.topicByName("t")->schema;
    auto r = rc::resolveAlterOps(schema, std::vector<cd::AlterOp>{ren, drop});
    ASSERT_TRUE(r.has_value()) << r.error();
    EXPECT_EQ((*r)[0].tag, 1); EXPECT_EQ((*r)[0].name, "health");
    EXPECT_EQ((*r)[1].tag, 1); EXPECT_TRUE((*r)[1].name.empty());
    auto back = rc::unresolveAlterOps(schema, std::span<const rc::ResolvedAlterOp>(*r));
    ASSERT_TRUE(back.has_value()) << back.error();
    EXPECT_EQ((*back)[0].name, "hp"); EXPECT_EQ((*back)[0].newName, "health");
    EXPECT_FALSE(rc::resolveAlterOps(schema, std::vector<cd::AlterOp>{drop}).has_value()) << "'health' is not live yet";
}

TEST(CatalogApplyV1Regression, AlterSchemaWithAStaleAddTagIsAPrecondition) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    rc::CatalogEvent e; e.kind = rc::EventKind::AlterSchema; e.objectName = "t"; e.objectId = 1;
    e.prevVersion = 1; e.newVersion = 2;
    e.ops = rc::AlterSchemaOps{{rc::ResolvedAlterOp{.type = cd::AlterOpType::Add, .tag = 7, .name = "mana",
                                                    .primType = cd::PrimType::Int32}}};   // nextTag is 2
    const uint64_t before = s.digest();
    auto a = rc::apply(s, e, 1);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_EQ(s.digest(), before);
}

TEST(CatalogApplyV1Regression, DropAndReuseGetsANewObjectIdAndTopicId) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, false), "t", 1, 0, 1), 0));
    auto d = rc::apply(s, stamped(s, rc::DropTopicOps{0}, "t", 1, 1, 2), 1);
    ASSERT_TRUE(d.has_value());
    EXPECT_EQ(d->droppedTopicId, 0);
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(1, false), "t", 2, 0, 1), 2));
    EXPECT_EQ(s.topicByName("t")->objectId, 2u);
    EXPECT_EQ(s.topicByName("t")->topicId, 1);
    ASSERT_EQ(s.tombstones().size(), 1u);
    EXPECT_EQ(s.tombstones()[0].objectId, 1u);
}

TEST(CatalogApplyV1Regression, DropTopicReadByAViewIsRefused) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "src", 1, 0, 1), 0));
    rc::ViewDef v; v.viewTopicId = 1; v.sinkPartitions = 2; v.keyField = "id"; v.sourceNames = {"src"};   // src has 2 (#143)
    v.outputDescriptorBlob = blob2(); v.normalizedPlanBlob = {1};
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::CreateViewOps{v}, "v", 2, 0, 1), 1));
    rc::CatalogEvent e; e.kind = rc::EventKind::DropTopic; e.ops = rc::DropTopicOps{0};
    e.objectName = "src"; e.objectId = 1; e.prevVersion = 1; e.newVersion = 2;
    auto a = rc::apply(s, e, 2);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
}

TEST(CatalogApplyV1Regression, AlterViewTakesAFreshSinkAndReportsBoth) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "src", 1, 0, 1), 0));
    rc::ViewDef v; v.viewTopicId = 1; v.sinkPartitions = 2; v.keyField = "id"; v.sourceNames = {"src"};   // src has 2 (#143)
    v.outputDescriptorBlob = blob2(); v.normalizedPlanBlob = {1};
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::CreateViewOps{v}, "v", 2, 0, 1), 1));
    rc::ViewDef v2 = v; v2.viewTopicId = 2; v2.normalizedPlanBlob = {2};
    auto a = rc::apply(s, stamped(s, rc::AlterViewOps{v2}, "v", 2, 1, 2), 2);
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    EXPECT_EQ(a->createdTopicId, 2); EXPECT_EQ(a->droppedTopicId, 1);
    EXPECT_EQ(s.viewByName("v")->def.viewTopicId, 2);
    EXPECT_EQ(s.viewByViewTopicId(1), nullptr);
}

TEST(CatalogApplyV1Regression, FingerprintsIgnoreIdsSoClustersCompare) {
    rc::CatalogState a, b;
    ASSERT_TRUE(rc::apply(a, stamped(a, createOps(0, true), "x", 1, 0, 1), 0));
    // Cluster b already holds another topic: same definition, different ids.
    ASSERT_TRUE(rc::apply(b, stamped(b, createOps(0, false), "other", 1, 0, 1), 0));
    ASSERT_TRUE(rc::apply(b, stamped(b, createOps(1, true), "x", 2, 0, 1), 1));
    EXPECT_EQ(rc::fingerprintOf(*a.topicByName("x")), rc::fingerprintOf(*b.topicByName("x")));
}

TEST(CatalogApplyV1Regression, RequestIdsAreRecordedAndADuplicateIsRefused) {
    rc::CatalogState s;
    const rc::RequestId rid{.hi = 0xC11E, .lo = 77};
    auto e = stamped(s, createOps(0, true), "x", 1, 0, 1);
    e.principal.requestId = rid;
    e.principal.requestFingerprint = 0xF1;
    ASSERT_TRUE(rc::apply(s, e, 0));
    auto r = s.request(rid);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->offset, 0); EXPECT_EQ(r->objectId, 1u); EXPECT_EQ(r->topicId, 0); EXPECT_EQ(r->schemaVersion, 1);
    EXPECT_EQ(r->requestFingerprint, 0xF1u) << "F14: the dedupe key carries the statement fingerprint";
    EXPECT_FALSE(s.request(rc::RequestId{.hi = 0xC11F, .lo = 77}).has_value()) << "all 128 bits are the key";
    auto again = stamped(s, createOps(1, false), "y", 2, 0, 1);
    again.principal.requestId = rid;
    auto a = rc::apply(s, again, 1);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_FALSE(s.request(rc::RequestId{}).has_value()) << "requestId {0,0} (pre-v8 clients) is never recorded";
}

TEST(CatalogApplyV1Regression, FingerprintCoversEveryDefinitionField) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "x", 1, 0, 1), 0));
    rc::TopicObject t = *s.topicByName("x");
    const uint64_t base = rc::fingerprintOf(t);
    auto differs = [&](auto mutate) { rc::TopicObject m = t; mutate(m); return rc::fingerprintOf(m) != base; };
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.name = "y"; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.version = 2; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.partitions = 3; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.replicationFactor = 1; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.persistence = rc::Persistence::Transient; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.config.retentionMs = 5; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.config.partitionKey = "k"; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.schema->compat = cd::CompatMode::Forward; }));
    EXPECT_TRUE(differs([](rc::TopicObject& m) { m.schema->evolvingBlob.push_back(0); }));
    EXPECT_FALSE(differs([](rc::TopicObject& m) { m.objectId = 99; m.topicId = 42; }));
}

// ── every rule refuses each invalid precondition explicitly ──────────────────────────
namespace {
/// src (typed, obj 1, topic 0), u (untyped, obj 2, topic 1), v (view over src, obj 3, sink 2).
/// #143: a sink mirrors its FIRST source's partition count (createOps' topics have 2).
rc::ViewDef viewOver(std::vector<std::string> sources, int32_t sink, int32_t sinkPartitions = 2) {
    rc::ViewDef v; v.viewTopicId = sink; v.sinkPartitions = sinkPartitions; v.keyField = "id"; v.sourceNames = std::move(sources);
    v.outputDescriptorBlob = blob2(); v.normalizedPlanBlob = {1};
    return v;
}
rc::CatalogState fixture() {
    rc::CatalogState s;
    EXPECT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "src", 1, 0, 1), 0));
    EXPECT_TRUE(rc::apply(s, stamped(s, createOps(1, false), "u", 2, 0, 1), 1));
    EXPECT_TRUE(rc::apply(s, stamped(s, rc::CreateViewOps{viewOver({"src"}, 2)}, "v", 3, 0, 1), 2));
    return s;
}
rc::CatalogEvent raw(rc::Ops ops, std::string name, uint64_t objectId, uint32_t prev, uint32_t next) {
    rc::CatalogEvent e;
    e.kind = rc::kindOf(ops); e.ops = std::move(ops); e.objectName = std::move(name);
    e.objectId = objectId; e.prevVersion = prev; e.newVersion = next;
    return e;
}
rc::SchemaDef schemaDef() { return rc::SchemaDef{.keyField = "id", .validate = true, .descriptorBlob = blob2(), .nextTag = 2}; }
rc::ResolvedAlterOp addOp(uint16_t tag, std::string name) {
    return rc::ResolvedAlterOp{.type = cd::AlterOpType::Add, .tag = tag, .name = std::move(name), .primType = cd::PrimType::Int32};
}
rc::ResolvedAlterOp dropOp(uint16_t tag) { return rc::ResolvedAlterOp{.type = cd::AlterOpType::Drop, .tag = tag}; }

struct RefusalCase {
    const char*          what;
    rc::CatalogEvent     e;
    rc::ApplyError::Code code;
    const char*          detail;   // a substring the error names
};
std::vector<RefusalCase> refusalCases() {
    using C = rc::ApplyError::Code;
    std::vector<RefusalCase> c;
    auto ct = [](int32_t tid) { return createOps(tid, false); };
    // CreateTopic
    c.push_back({"create: empty name", raw(ct(3), "", 4, 0, 1), C::Precondition, "is invalid (empty)"});
    c.push_back({"create: name is a topic", raw(ct(3), "u", 4, 0, 1), C::Precondition, "'u' is taken"});
    c.push_back({"create: name is a view", raw(ct(3), "v", 4, 0, 1), C::Precondition, "'v' is taken"});
    c.push_back({"create: objectId", raw(ct(3), "n", 5, 0, 1), C::Precondition, "objectId 5, next is 4"});
    c.push_back({"create: topicId", raw(ct(2), "n", 4, 0, 1), C::Precondition, "topicId 2, next is 3"});
    c.push_back({"create: prevVersion", raw(ct(3), "n", 4, 1, 1), C::Precondition, "a new object is 0 -> 1"});
    c.push_back({"create: newVersion", raw(ct(3), "n", 4, 0, 2), C::Precondition, "a new object is 0 -> 1"});
    { auto o = ct(3); o.partitions = 0;
      c.push_back({"create: partitions", raw(o, "n", 4, 0, 1), C::Precondition, "partitions 0 < 1"}); }
    { auto o = ct(3); o.replicationFactor = -1;
      c.push_back({"create: rf", raw(o, "n", 4, 0, 1), C::Precondition, "replicationFactor -1 < 0"}); }
    { auto o = ct(3); o.schema = schemaDef(); o.schema->descriptorBlob.clear();
      c.push_back({"create: empty descriptor", raw(o, "n", 4, 0, 1), C::Precondition, "empty descriptor"}); }
    { auto o = ct(3); o.schema = schemaDef(); o.schema->descriptorBlob = {1, 2, 3};
      c.push_back({"create: garbage descriptor", raw(o, "n", 4, 0, 1), C::BadOps, "does not deserialize"}); }
    { auto o = ct(3); o.schema = schemaDef(); o.schema->nextTag = 1;
      c.push_back({"create: nextTag behind a tag", raw(o, "n", 4, 0, 1), C::Precondition, "nextTag 1"}); }
    { auto o = ct(3); o.schema = schemaDef(); o.schema->keyField = "nope";
      c.push_back({"create: key field absent", raw(o, "n", 4, 0, 1), C::Precondition, "key field 'nope'"}); }
    { auto o = ct(3); o.persistence = static_cast<rc::Persistence>(9);
      c.push_back({"create: bad enum", raw(o, "n", 4, 0, 1), C::BadOps, "BadEnum"}); }
    { auto e = raw(ct(3), "n", 4, 0, 1); e.kind = rc::EventKind::DropTopic;
      c.push_back({"kind disagrees with ops", e, C::BadOps, "KindOpsMismatch"}); }
    // DropTopic
    c.push_back({"drop: unknown objectId", raw(rc::DropTopicOps{1}, "u", 9, 1, 2), C::Precondition, "no live topic with objectId 9"});
    c.push_back({"drop: objectId of a view", raw(rc::DropTopicOps{2}, "v", 3, 1, 2), C::Precondition, "no live topic"});
    c.push_back({"drop: name disagrees", raw(rc::DropTopicOps{1}, "x", 2, 1, 2), C::Precondition, "is topic 'u', not 'x'"});
    c.push_back({"drop: topicId disagrees", raw(rc::DropTopicOps{0}, "u", 2, 1, 2), C::Precondition, "topicId 0"});
    c.push_back({"drop: prevVersion", raw(rc::DropTopicOps{1}, "u", 2, 0, 1), C::Precondition, "version chain 0 -> 1, object at 1"});
    c.push_back({"drop: newVersion", raw(rc::DropTopicOps{1}, "u", 2, 1, 3), C::Precondition, "version chain 1 -> 3"});
    c.push_back({"drop: read by a view", raw(rc::DropTopicOps{0}, "src", 1, 1, 2), C::Precondition, "read by view 'v'"});
    // AlterTopicConfig
    auto cfg = [](std::vector<rc::ConfigChange> ch) { return rc::AlterTopicConfigOps{std::move(ch)}; };
    using K = rc::ConfigKey;
    c.push_back({"config: no changes", raw(cfg({}), "u", 2, 1, 2), C::Precondition, "no changes"});
    c.push_back({"config: key twice", raw(cfg({{K::RetentionMs, 1}, {K::RetentionMs, 2}}), "u", 2, 1, 2), C::Precondition, "set twice"});
    c.push_back({"config: retention.ms", raw(cfg({{K::RetentionMs, -2}}), "u", 2, 1, 2), C::Precondition, "< -1"});
    c.push_back({"config: retention.bytes", raw(cfg({{K::RetentionBytes, -2}}), "u", 2, 1, 2), C::Precondition, "< -1"});
    c.push_back({"config: delete.retention.ms", raw(cfg({{K::DeleteRetentionMs, -1}}), "u", 2, 1, 2), C::Precondition,
                 "delete.retention.ms -1 < 0"});
    c.push_back({"config: max lag", raw(cfg({{K::MaxCompactionLagMs, -2}}), "u", 2, 1, 2), C::Precondition, "< -1"});
    c.push_back({"config: min lag", raw(cfg({{K::MinCompactionLagMs, -1}}), "u", 2, 1, 2), C::Precondition, "< 0"});
    c.push_back({"config: ratio < 0", raw(cfg({{K::MinCleanableDirtyRatioPpm, -1}}), "u", 2, 1, 2), C::Precondition, "outside"});
    c.push_back({"config: ratio > 1e6", raw(cfg({{K::MinCleanableDirtyRatioPpm, 1'000'001}}), "u", 2, 1, 2), C::Precondition, "outside"});
    c.push_back({"config: ratio wraps into range", raw(cfg({{K::MinCleanableDirtyRatioPpm, (int64_t{1} << 32) + 5}}), "u", 2, 1, 2),
                 C::Precondition, "does not fit 32 bits"});
    c.push_back({"config: bad key", raw(cfg({{static_cast<K>(0), 1}}), "u", 2, 1, 2), C::BadOps, "BadEnum"});
    c.push_back({"config: chain", raw(cfg({{K::RetentionMs, 1}}), "u", 2, 2, 3), C::Precondition, "version chain"});
    c.push_back({"config: a view", raw(cfg({{K::RetentionMs, 1}}), "v", 3, 1, 2), C::Precondition, "no live topic"});
    // CreateTopic config: THE one TopicConfig validator (I1) — every range class, on create.
    auto cc = [&](auto mutate) { auto o = ct(3); mutate(o.config); return raw(o, "n", 4, 0, 1); };
    using TC = rc::TopicConfig;
    c.push_back({"create config: retention.ms", cc([](TC& k) { k.retentionMs = -2; }), C::Precondition, "retention.ms -2 < -1"});
    c.push_back({"create config: retention.bytes", cc([](TC& k) { k.retentionBytes = -2; }), C::Precondition, "retention.bytes -2 < -1"});
    c.push_back({"create config: delete.retention.ms", cc([](TC& k) { k.deleteRetentionMs = -1; }), C::Precondition,
                 "delete.retention.ms -1 < 0"});
    c.push_back({"create config: ratio < 0", cc([](TC& k) { k.minCleanableDirtyRatioPpm = -1; }), C::Precondition,
                 "min.cleanable.dirty.ratio -1 outside [0, 1000000] ppm"});
    c.push_back({"create config: ratio > 1e6", cc([](TC& k) { k.minCleanableDirtyRatioPpm = 1'000'001; }), C::Precondition,
                 "min.cleanable.dirty.ratio 1000001 outside"});
    c.push_back({"create config: segment 0", cc([](TC& k) { k.segmentRollBytes = 0; }), C::Precondition, "segment.bytes 0 < 1"});
    c.push_back({"create config: segment too big", cc([](TC& k) { k.segmentRollBytes = rc::kMaxSegmentRollBytes + 1; }),
                 C::Precondition, "segment.bytes 2080374784 > 2080374783"});
    c.push_back({"create config: min lag", cc([](TC& k) { k.minCompactionLagMs = -1; }), C::Precondition, "min.compaction.lag.ms -1 < 0"});
    c.push_back({"create config: max lag", cc([](TC& k) { k.maxCompactionLagMs = -2; }), C::Precondition, "max.compaction.lag.ms -2 < -1"});
    c.push_back({"create config: retention check 0", cc([](TC& k) { k.retentionCheckMs = 0; }), C::Precondition,
                 "retention.check.ms 0 < 1"});
    c.push_back({"create config: file delete delay 0", cc([](TC& k) { k.fileDeleteDelayMs = 0; }), C::Precondition,
                 "file.delete.delay.ms 0 < 1"});
    c.push_back({"create config: cleanup policy", cc([](TC& k) { k.cleanupPolicy = 3; }), C::BadOps, "BadEnum"});
    c.push_back({"create config: durability", cc([](TC& k) { k.durability = 2; }), C::BadOps, "BadEnum"});
    c.push_back({"create config: tier", cc([](TC& k) { k.durabilityTier = 3; }), C::BadOps, "BadEnum"});
    // CreateSchema
    c.push_back({"create schema: typed topic", raw(rc::CreateSchemaOps{schemaDef()}, "src", 1, 1, 2), C::Precondition, "already has a schema"});
    c.push_back({"create schema: chain", raw(rc::CreateSchemaOps{schemaDef()}, "u", 2, 1, 3), C::Precondition, "version chain"});
    { auto d = schemaDef(); d.keyField = "nope";
      c.push_back({"create schema: key field", raw(rc::CreateSchemaOps{d}, "u", 2, 1, 2), C::Precondition, "key field 'nope'"}); }
    { auto d = schemaDef(); d.descriptorBlob.clear();
      c.push_back({"create schema: empty", raw(rc::CreateSchemaOps{d}, "u", 2, 1, 2), C::Precondition, "empty descriptor"}); }
    // AlterSchema
    auto alt = [](std::vector<rc::ResolvedAlterOp> o) { return rc::AlterSchemaOps{std::move(o)}; };
    c.push_back({"alter: untyped", raw(alt({dropOp(0)}), "u", 2, 1, 2), C::Precondition, "is untyped"});
    c.push_back({"alter: no ops", raw(alt({}), "src", 1, 1, 2), C::Precondition, "no operations"});
    c.push_back({"alter: only set-compat", raw(alt({rc::ResolvedAlterOp{.type = cd::AlterOpType::SetCompatibility}}), "src", 1, 1, 2),
                 C::Precondition, "SetCompatibility event"});
    c.push_back({"alter: chain", raw(alt({addOp(2, "mana")}), "src", 1, 3, 5), C::Precondition, "version chain 3 -> 5, object at 1"});
    c.push_back({"alter: stale add tag", raw(alt({addOp(7, "mana")}), "src", 1, 1, 2), C::Precondition, "not the next tag 2"});
    c.push_back({"alter: dead tag", raw(alt({dropOp(9)}), "src", 1, 1, 2), C::Precondition, "tag 9: not a live field"});
    { auto d = dropOp(1); d.name = "hp";
      c.push_back({"alter: non-canonical drop", raw(alt({d}), "src", 1, 1, 2), C::Precondition, "does not use"}); }
    { auto a = addOp(2, "s"); a.primType = cd::PrimType::FixedStr;
      c.push_back({"alter: FixedStr without width", raw(alt({a}), "src", 1, 1, 2), C::Precondition, "needs a width"}); }
    { auto a = addOp(2, "s"); a.fixedLen = 4;
      c.push_back({"alter: width on a non-FixedStr", raw(alt({a}), "src", 1, 1, 2), C::Precondition, "only a FixedStr"}); }
    c.push_back({"alter: add collides", raw(alt({addOp(2, "hp")}), "src", 1, 1, 2), C::Precondition, "collides"});
    c.push_back({"alter: drop the key", raw(alt({dropOp(0)}), "src", 1, 1, 2), C::BadOps, "key field"});
    c.push_back({"alter: drop under Forward",
                 raw(alt({rc::ResolvedAlterOp{.type = cd::AlterOpType::SetCompatibility, .mode = cd::CompatMode::Forward}, dropOp(1)}),
                     "src", 1, 1, 2), C::BadOps, "Forward"});
    // AlterSchema: non-canonical resolved ops, RENAME collisions (M2), escaped names (M3)
    auto ren = [](uint16_t tag, std::string to) {
        return rc::ResolvedAlterOp{.type = cd::AlterOpType::Rename, .tag = tag, .name = std::move(to)};
    };
    { auto r = ren(1, "health"); r.primType = cd::PrimType::Int64;
      c.push_back({"alter: rename carries a type", raw(alt({r}), "src", 1, 1, 2), C::Precondition, "RENAME tag 1: carries fields"}); }
    { auto r = ren(1, "health"); r.fixedLen = 4;
      c.push_back({"alter: rename carries a width", raw(alt({r}), "src", 1, 1, 2), C::Precondition, "RENAME tag 1: carries fields"}); }
    { auto r = ren(1, "health"); r.mode = cd::CompatMode::Forward;
      c.push_back({"alter: rename carries a mode", raw(alt({r}), "src", 1, 1, 2), C::Precondition, "RENAME tag 1: carries fields"}); }
    { auto a = addOp(2, "mana"); a.mode = cd::CompatMode::Forward;
      c.push_back({"alter: add carries a mode", raw(alt({a}), "src", 1, 1, 2), C::Precondition, "carries a compatibility mode"}); }
    c.push_back({"alter: set-compat carries a tag",
                 raw(alt({addOp(2, "mana"), rc::ResolvedAlterOp{.type = cd::AlterOpType::SetCompatibility, .tag = 1}}), "src", 1, 1, 2),
                 C::Precondition, "SET COMPATIBILITY: carries fields"});
    c.push_back({"alter: set-compat carries a name",
                 raw(alt({addOp(2, "mana"), rc::ResolvedAlterOp{.type = cd::AlterOpType::SetCompatibility, .name = "x"}}), "src", 1, 1, 2),
                 C::Precondition, "SET COMPATIBILITY: carries fields"});
    c.push_back({"alter: drop carries a type",
                 raw(alt({rc::ResolvedAlterOp{.type = cd::AlterOpType::Drop, .tag = 1, .primType = cd::PrimType::Bool}}), "src", 1, 1, 2),
                 C::Precondition, "DROP tag 1: carries fields"});
    c.push_back({"alter: rename onto a live name", raw(alt({ren(1, "id")}), "src", 1, 1, 2), C::Precondition,
                 "RENAME tag 1: 'id' collides"});
    c.push_back({"alter: rename to empty", raw(alt({ren(1, "")}), "src", 1, 1, 2), C::Precondition, "new name is empty"});
    c.push_back({"alter: rename of a dead tag", raw(alt({ren(8, "z")}), "src", 1, 1, 2), C::Precondition, "RENAME tag 8: not a live"});
    c.push_back({"alter: add with an empty name", raw(alt({addOp(2, "")}), "src", 1, 1, 2), C::Precondition, "field name is empty"});
    c.push_back({"alter: a raw control byte is escaped", raw(alt({addOp(7, "x\ny\x1b")}), "src", 1, 1, 2), C::Precondition,
                 "ADD 'x\\x0ay\\x1b': tag 7"});
    // SetCompatibility
    c.push_back({"set compat: untyped", raw(rc::SetCompatibilityOps{cd::CompatMode::Forward}, "u", 2, 1, 2), C::Precondition, "is untyped"});
    c.push_back({"set compat: chain", raw(rc::SetCompatibilityOps{cd::CompatMode::Forward}, "src", 1, 0, 1), C::Precondition, "version chain"});
    // CreateView
    auto cv = [](rc::ViewDef v) { return rc::CreateViewOps{std::move(v)}; };
    c.push_back({"view: name taken", raw(cv(viewOver({"src"}, 3)), "u", 4, 0, 1), C::Precondition, "'u' is taken"});
    c.push_back({"view: objectId", raw(cv(viewOver({"src"}, 3)), "w", 7, 0, 1), C::Precondition, "objectId 7, next is 4"});
    c.push_back({"view: sink id", raw(cv(viewOver({"src"}, 9)), "w", 4, 0, 1), C::Precondition, "viewTopicId 9, next is 3"});
    c.push_back({"view: versions", raw(cv(viewOver({"src"}, 3)), "w", 4, 0, 2), C::Precondition, "0 -> 1"});
    c.push_back({"view: no sources", raw(cv(viewOver({}, 3)), "w", 4, 0, 1), C::Precondition, "at least one source"});
    c.push_back({"view: dead source", raw(cv(viewOver({"gone"}, 3)), "w", 4, 0, 1), C::Precondition, "source 'gone' is not a live topic"});
    c.push_back({"view: source is a view", raw(cv(viewOver({"v"}, 3)), "w", 4, 0, 1), C::Precondition, "source 'v'"});
    { auto v = viewOver({"src"}, 3); v.sinkPartitions = 0;
      c.push_back({"view: sink partitions", raw(cv(v), "w", 4, 0, 1), C::Precondition, "sinkPartitions 0"}); }
    { auto v = viewOver({"src"}, 3); v.normalizedPlanBlob.clear();
      c.push_back({"view: empty plan", raw(cv(v), "w", 4, 0, 1), C::Precondition, "plan is empty"}); }
    { auto v = viewOver({"src"}, 3); v.outputDescriptorBlob = {9};
      c.push_back({"view: garbage output", raw(cv(v), "w", 4, 0, 1), C::BadOps, "output descriptor does not deserialize"}); }
    { auto v = viewOver({"src"}, 3); v.keyField = "nope";
      c.push_back({"view: key field", raw(cv(v), "w", 4, 0, 1), C::Precondition, "key field 'nope'"}); }
    // AlterView
    auto av = [](rc::ViewDef v) { return rc::AlterViewOps{std::move(v)}; };
    c.push_back({"alter view: unknown", raw(av(viewOver({"src"}, 3)), "v", 1, 1, 2), C::Precondition, "no live view with objectId 1"});
    c.push_back({"alter view: name", raw(av(viewOver({"src"}, 3)), "w", 3, 1, 2), C::Precondition, "is view 'v', not 'w'"});
    c.push_back({"alter view: chain", raw(av(viewOver({"src"}, 3)), "v", 3, 2, 3), C::Precondition, "version chain"});
    c.push_back({"alter view: same sink", raw(av(viewOver({"src"}, 2)), "v", 3, 1, 2), C::Precondition, "not a fresh sink"});
    c.push_back({"alter view: dead source", raw(av(viewOver({"gone"}, 3)), "v", 3, 1, 2), C::Precondition, "not a live topic"});
    c.push_back({"alter view: source is a view", raw(av(viewOver({"v"}, 3)), "v", 3, 1, 2), C::Precondition,
                 "source 'v' is not a live topic"});
    { auto v = viewOver({"src"}, 3); v.keyField = "nope";
      c.push_back({"alter view: key field", raw(av(v), "v", 3, 1, 2), C::Precondition, "key field 'nope' is not a field of the output"}); }
    { auto v = viewOver({"src"}, 3); v.sinkPartitions = 0;
      c.push_back({"alter view: sink partitions", raw(av(v), "v", 3, 1, 2), C::Precondition, "sinkPartitions 0"}); }
    c.push_back({"alter view: a topic's objectId", raw(av(viewOver({"src"}, 3)), "src", 1, 1, 2), C::Precondition, "no live view"});
    // DropView
    c.push_back({"drop view: unknown", raw(rc::DropViewOps{2}, "v", 2, 1, 2), C::Precondition, "no live view"});
    c.push_back({"drop view: sink disagrees", raw(rc::DropViewOps{0}, "v", 3, 1, 2), C::Precondition, "sinks to 2"});
    c.push_back({"drop view: chain", raw(rc::DropViewOps{2}, "v", 3, 1, 1), C::Precondition, "version chain"});
    return c;
}
} // namespace

TEST(CatalogApplyV1Regression, EveryRuleRefusesEachInvalidPreconditionAndPreviewAgrees) {
    for (auto& c : refusalCases()) {
        rc::CatalogState s = fixture();
        const uint64_t before = s.digest();
        auto p = rc::previewFingerprint(s, c.e);
        if (p.has_value()) { ADD_FAILURE() << c.what << ": accepted by previewFingerprint"; continue; }
        EXPECT_EQ(p.error().code, c.code) << c.what << ": " << p.error().detail;
        auto a = rc::apply(s, c.e, 3);
        if (a.has_value()) { ADD_FAILURE() << c.what << ": accepted by apply"; continue; }
        EXPECT_EQ(a.error().code, c.code) << c.what << ": " << a.error().detail;
        EXPECT_EQ(a.error().offset, 3) << c.what;
        EXPECT_NE(a.error().detail.find(c.detail), std::string::npos) << c.what << ": " << a.error().detail;
        for (const char ch : a.error().detail)
            EXPECT_TRUE(static_cast<unsigned char>(ch) >= 0x20 && static_cast<unsigned char>(ch) < 0x7f)
                << c.what << ": a detail never carries a raw control or non-ASCII byte";
        EXPECT_EQ(s.digest(), before) << c.what;
        EXPECT_EQ(s.appliedOffset(), 2) << c.what;
    }
}

TEST(CatalogApplyV1Regression, OffsetsMustAdvanceAndUnknownFormatsHaveNoRules) {
    rc::CatalogState s = fixture();
    auto e = stamped(s, createOps(3, false), "n", 4, 0, 1);
    auto stale = rc::apply(s, e, 2);
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error().code, rc::ApplyError::Code::Precondition);
    auto v2 = e; v2.formatVer = 2;
    auto u = rc::apply(s, v2, 3);
    ASSERT_FALSE(u.has_value());
    EXPECT_EQ(u.error().code, rc::ApplyError::Code::UnsupportedFormat);
    EXPECT_EQ(rc::previewFingerprint(s, v2).error().code, rc::ApplyError::Code::UnsupportedFormat);
    EXPECT_EQ(rc::applyErrorName(rc::ApplyError::Code::FingerprintMismatch), "FingerprintMismatch");
    ASSERT_TRUE(rc::apply(s, e, 10)) << "offsets need not be contiguous, only increasing";
    EXPECT_EQ(s.appliedOffset(), 10);
}

// ── effects ───────────────────────────────────────────────────────────────────
TEST(CatalogApplyV1Regression, AlterTopicConfigSetsOnlyTheNamedKeys) {
    rc::CatalogState s = fixture();
    const rc::TopicConfig before = s.topicByName("u")->config;
    using K = rc::ConfigKey;
    rc::AlterTopicConfigOps o{{{K::RetentionMs, 1000}, {K::RetentionBytes, -1}, {K::DeleteRetentionMs, 5},
                               {K::MinCleanableDirtyRatioPpm, 1'000'000}, {K::MinCompactionLagMs, 0}, {K::MaxCompactionLagMs, 9}}};
    auto a = rc::apply(s, stamped(s, o, "u", 2, 1, 2), 3);
    ASSERT_TRUE(a.has_value()) << a.error().detail;
    const rc::TopicObject* t = s.topicByName("u");
    EXPECT_EQ(t->version, 2u);
    rc::TopicConfig want = before;
    want.retentionMs = 1000; want.retentionBytes = -1; want.deleteRetentionMs = 5;
    want.minCleanableDirtyRatioPpm = 1'000'000; want.minCompactionLagMs = 0; want.maxCompactionLagMs = 9;
    EXPECT_EQ(t->config, want);
    EXPECT_EQ(a->createdTopicId, -1); EXPECT_EQ(a->droppedTopicId, -1);
}

TEST(CatalogApplyV1Regression, RenameOfTheKeyFieldMovesKeyFieldAndAChainEndsAtTheLast) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    cd::AlterOp r1{}; r1.type = cd::AlterOpType::Rename; r1.name = "id"; r1.newName = "a";
    cd::AlterOp r2{}; r2.type = cd::AlterOpType::Rename; r2.name = "a"; r2.newName = "b";
    auto ops = rc::resolveAlterOps(*s.topicByName("t")->schema, std::vector<cd::AlterOp>{r1, r2});
    ASSERT_TRUE(ops.has_value()) << ops.error();
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::AlterSchemaOps{*ops}, "t", 1, 1, 2), 1));
    EXPECT_EQ(s.topicByName("t")->schema->keyField, "b");
    const cd::Descriptor d = cd::deserialize(s.topicByName("t")->schema->evolvingBlob);
    ASSERT_NE(d.find("b"), nullptr);
    EXPECT_EQ(d.find("b")->id, 0);
}

TEST(CatalogApplyV1Regression, ResolveTracksAddsAndRenamesWithinOneList) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    const auto& schema = *s.topicByName("t")->schema;
    cd::AlterOp a1{}; a1.type = cd::AlterOpType::Add; a1.name = "mana"; a1.primType = cd::PrimType::Int32;
    cd::AlterOp a2{}; a2.type = cd::AlterOpType::Add; a2.name = "tag"; a2.primType = cd::PrimType::FixedStr; a2.fixedLen = 8;
    cd::AlterOp rn{}; rn.type = cd::AlterOpType::Rename; rn.name = "mana"; rn.newName = "mp";
    cd::AlterOp sc{}; sc.type = cd::AlterOpType::SetCompatibility; sc.mode = cd::CompatMode::Forward;
    auto r = rc::resolveAlterOps(schema, std::vector<cd::AlterOp>{a1, a2, rn, sc});
    ASSERT_TRUE(r.has_value()) << r.error();
    EXPECT_EQ((*r)[0].tag, 2); EXPECT_EQ((*r)[1].tag, 3); EXPECT_EQ((*r)[1].fixedLen, 8);
    EXPECT_EQ((*r)[2].tag, 2) << "a RENAME of a field ADDed earlier in the list names its new tag";
    EXPECT_EQ((*r)[3].mode, cd::CompatMode::Forward); EXPECT_EQ((*r)[3].tag, 0);
    auto back = rc::unresolveAlterOps(schema, std::span<const rc::ResolvedAlterOp>(*r));
    ASSERT_TRUE(back.has_value()) << back.error();
    EXPECT_EQ((*back)[2].name, "mana"); EXPECT_EQ((*back)[2].newName, "mp");
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::AlterSchemaOps{*r}, "t", 1, 1, 2), 1));
    const rc::SchemaState& after = *s.topicByName("t")->schema;
    EXPECT_EQ(after.nextTag, 4); EXPECT_EQ(after.compat, cd::CompatMode::Forward); EXPECT_EQ(after.schemaVersion, 2);
    // Refusals name the field.
    cd::AlterOp ghost{}; ghost.type = cd::AlterOpType::Rename; ghost.name = "ghost"; ghost.newName = "x";
    auto g = rc::resolveAlterOps(after, std::vector<cd::AlterOp>{ghost});
    ASSERT_FALSE(g.has_value()); EXPECT_NE(g.error().find("'ghost'"), std::string::npos);
    cd::AlterOp dup{}; dup.type = cd::AlterOpType::Add; dup.name = "hp"; dup.primType = cd::PrimType::Int32;
    EXPECT_FALSE(rc::resolveAlterOps(after, std::vector<cd::AlterOp>{dup}).has_value());
}

TEST(CatalogApplyV1Regression, DropViewRemovesItFreesTheNameAndUnblocksTheSource) {
    rc::CatalogState s = fixture();
    auto d = rc::apply(s, stamped(s, rc::DropViewOps{2}, "v", 3, 1, 2), 3);
    ASSERT_TRUE(d.has_value()) << d.error().detail;
    EXPECT_EQ(d->droppedTopicId, 2); EXPECT_EQ(d->createdTopicId, -1);
    EXPECT_EQ(s.viewByName("v"), nullptr); EXPECT_EQ(s.viewByViewTopicId(2), nullptr);
    ASSERT_EQ(s.tombstones().size(), 1u);
    EXPECT_TRUE(s.tombstones()[0].wasView); EXPECT_EQ(s.tombstones()[0].finalVersion, 2u);
    EXPECT_EQ(d->fingerprint, rc::fingerprintOf(s.tombstones()[0]));
    EXPECT_EQ(s.nextTopicId(), 3) << "a drop never returns an id";
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::DropTopicOps{0}, "src", 1, 1, 2), 4)) << "no view reads src any more";
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(3, false), "v", 4, 0, 1), 5)) << "a dropped view's name is free";
    EXPECT_EQ(s.topics().size(), 2u); EXPECT_TRUE(s.views().empty());
}

TEST(CatalogApplyV1Regression, RequestRecordsNameTheAnswerOfEveryKind) {
    rc::CatalogState s = fixture();
    auto withId = [](rc::CatalogEvent e, uint64_t lo) { e.principal.requestId = rc::RequestId{.hi = 1, .lo = lo}; return e; };
    ASSERT_TRUE(rc::apply(s, withId(stamped(s, rc::SetCompatibilityOps{cd::CompatMode::Forward}, "src", 1, 1, 2), 1), 3));
    ASSERT_TRUE(rc::apply(s, withId(stamped(s, rc::AlterViewOps{viewOver({"src"}, 3)}, "v", 3, 1, 2), 2), 4));
    ASSERT_TRUE(rc::apply(s, withId(stamped(s, rc::DropTopicOps{1}, "u", 2, 1, 2), 3), 5));
    auto r1 = s.request({1, 1}); ASSERT_TRUE(r1);
    EXPECT_EQ(r1->kind, rc::EventKind::SetCompatibility); EXPECT_EQ(r1->topicId, 0); EXPECT_EQ(r1->schemaVersion, 2);
    EXPECT_EQ(r1->newVersion, 2u); EXPECT_EQ(r1->offset, 3);
    auto r2 = s.request({1, 2}); ASSERT_TRUE(r2);
    EXPECT_EQ(r2->topicId, 3) << "AlterView answers the fresh sink"; EXPECT_EQ(r2->schemaVersion, 0);
    auto r3 = s.request({1, 3}); ASSERT_TRUE(r3);
    EXPECT_EQ(r3->topicId, 1) << "a drop answers the retired topic id"; EXPECT_EQ(r3->objectId, 2u);
}

// ── fingerprints and digest ───────────────────────────────────────────────────────
TEST(CatalogApplyV1Regression, ViewFingerprintCoversTheDefinitionNeverIds) {
    rc::CatalogState s = fixture();
    const rc::ViewObject v = *s.viewByName("v");
    const uint64_t base = rc::fingerprintOf(v);
    auto differs = [&](auto mutate) { rc::ViewObject m = v; mutate(m); return rc::fingerprintOf(m) != base; };
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.name = "w"; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.version = 2; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.def.sinkPartitions = 4; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.def.persistence = rc::Persistence::Transient; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.def.keyField = "hp"; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.def.sourceNames = {"u"}; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.def.sourceNames = {"src", "src"}; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) { m.def.normalizedPlanBlob = {2}; }));
    EXPECT_TRUE(differs([](rc::ViewObject& m) {
        cd::DescriptorBuilder b("players"); b.add("id", cd::PrimType::Int64, 1, true);
        m.def.outputDescriptorBlob = cd::serialize(b.build()); }));
    EXPECT_FALSE(differs([](rc::ViewObject& m) { m.objectId = 99; m.def.viewTopicId = 42; m.def.compiledPlanBlob = {7, 7}; }))
        << "ruling 11: ids and the compiled plan are never fingerprinted";
    // Length prefixes: moving a byte across a source-name boundary changes the hash.
    rc::ViewObject a = v, b = v;
    a.def.sourceNames = {"ab", "c"}; b.def.sourceNames = {"a", "bc"};
    EXPECT_NE(rc::fingerprintOf(a), rc::fingerprintOf(b));
}

TEST(CatalogApplyV1Regression, TombstoneFingerprintCoversKindNameAndFinalVersionNeverIds) {
    const rc::Tombstone t{.objectId = 1, .name = "x", .finalVersion = 3, .wasView = false};
    const uint64_t base = rc::fingerprintOf(t);
    rc::Tombstone m = t; m.objectId = 77;  EXPECT_EQ(rc::fingerprintOf(m), base);
    m = t; m.name = "y";                   EXPECT_NE(rc::fingerprintOf(m), base);
    m = t; m.finalVersion = 4;             EXPECT_NE(rc::fingerprintOf(m), base);
    m = t; m.wasView = true;               EXPECT_NE(rc::fingerprintOf(m), base);
}

TEST(CatalogApplyV1Regression, DigestIsAFunctionOfTheAppliedPrefix) {
    rc::CatalogState a = fixture(), b = fixture();
    EXPECT_EQ(a.digest(), b.digest());
    ASSERT_TRUE(rc::apply(a, stamped(a, rc::DropViewOps{2}, "v", 3, 1, 2), 3));
    EXPECT_NE(a.digest(), b.digest());
    ASSERT_TRUE(rc::apply(b, stamped(b, rc::DropViewOps{2}, "v", 3, 1, 2), 3));
    EXPECT_EQ(a.digest(), b.digest());
    rc::CatalogState c = fixture();
    ASSERT_TRUE(rc::apply(c, stamped(c, rc::DropViewOps{2}, "v", 3, 1, 2), 4));
    EXPECT_NE(a.digest(), c.digest()) << "the applied offset is part of the digest";
}

// ── fix round 1 (M2): exhaustion and tombstoned objects ─────────────────────────────
TEST(CatalogApplyV1Regression, FieldTagExhaustionIsRefusedNeverWrapped) {
    rc::CatalogState s;
    auto o = createOps(0, true);
    o.schema->nextTag = 0xFFFF;   // legal: past every tag; but no tag is left to assign
    ASSERT_TRUE(rc::apply(s, stamped(s, o, "t", 1, 0, 1), 0));
    const rc::SchemaState& schema = *s.topicByName("t")->schema;
    cd::AlterOp add{}; add.type = cd::AlterOpType::Add; add.name = "mana"; add.primType = cd::PrimType::Int32;
    auto r = rc::resolveAlterOps(schema, std::vector<cd::AlterOp>{add});
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().find("field tags are exhausted"), std::string::npos) << r.error();
    const uint64_t before = s.digest();
    auto a = rc::apply(s, raw(rc::AlterSchemaOps{{addOp(0xFFFF, "mana")}}, "t", 1, 1, 2), 1);
    ASSERT_FALSE(a.has_value());
    EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
    EXPECT_NE(a.error().detail.find("field tags are exhausted"), std::string::npos) << a.error().detail;
    EXPECT_EQ(s.digest(), before);
    // One below the limit still works and leaves nextTag at the limit.
    rc::SchemaState almost = schema; almost.nextTag = 0xFFFE;
    auto ok = rc::resolveAlterOps(almost, std::vector<cd::AlterOp>{add});
    ASSERT_TRUE(ok.has_value()) << ok.error();
    EXPECT_EQ((*ok)[0].tag, 0xFFFE);
    EXPECT_FALSE(rc::resolveAlterOps(almost, std::vector<cd::AlterOp>{add, add}).has_value()) << "the second ADD has no tag";
}

TEST(CatalogApplyV1Regression, SchemaVersionExhaustionIsRefused) {
    // 0xFFFF is the untilVersion sentinel: the last assignable schema version is 0xFFFE.
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    int64_t off = 1;
    uint32_t ver = 1;
    for (uint32_t sv = 1; sv < 0xFFFE; ++sv, ++ver, ++off) {
        auto mode = (sv % 2) ? cd::CompatMode::Forward : cd::CompatMode::Backward;
        auto e = raw(rc::SetCompatibilityOps{mode}, "t", 1, ver, ver + 1);
        auto fp = rc::previewFingerprint(s, e);
        ASSERT_TRUE(fp.has_value()) << sv << ": " << fp.error().detail;
        e.fingerprint = *fp;
        ASSERT_TRUE(rc::apply(s, e, off)) << sv;
    }
    ASSERT_EQ(s.topicByName("t")->schema->schemaVersion, 0xFFFE);
    for (rc::Ops ops : {rc::Ops{rc::SetCompatibilityOps{cd::CompatMode::Forward}},
                        rc::Ops{rc::AlterSchemaOps{{addOp(2, "mana")}}}}) {
        auto a = rc::apply(s, raw(ops, "t", 1, ver, ver + 1), off);
        ASSERT_FALSE(a.has_value());
        EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition);
        EXPECT_NE(a.error().detail.find("schema version 65534 is exhausted"), std::string::npos) << a.error().detail;
    }
}

TEST(CatalogApplyV1Regression, ATombstonedObjectIdIsNeverLiveAgain) {
    rc::CatalogState s = fixture();
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::DropViewOps{2}, "v", 3, 1, 2), 3));
    ASSERT_TRUE(rc::apply(s, stamped(s, rc::DropTopicOps{1}, "u", 2, 1, 2), 4));
    struct Case { const char* what; rc::CatalogEvent e; const char* detail; };
    const Case cases[] = {
        {"drop a dropped topic", raw(rc::DropTopicOps{1}, "u", 2, 2, 3), "no live topic with objectId 2"},
        {"drop a dropped view", raw(rc::DropViewOps{2}, "v", 3, 2, 3), "no live view with objectId 3"},
        {"alter a dropped topic", raw(rc::SetCompatibilityOps{cd::CompatMode::Forward}, "u", 2, 2, 3), "no live topic"},
        {"alter a dropped view", raw(rc::AlterViewOps{viewOver({"src"}, 3)}, "v", 3, 2, 3), "no live view"},
        {"config of a dropped topic", raw(rc::AlterTopicConfigOps{{{rc::ConfigKey::RetentionMs, 1}}}, "u", 2, 2, 3), "no live topic"},
        {"re-create under the old objectId", raw(createOps(3, false), "u", 2, 0, 1), "objectId 2, next is 4"},
    };
    for (const auto& c : cases) {
        const uint64_t before = s.digest();
        auto a = rc::apply(s, c.e, 5);
        ASSERT_FALSE(a.has_value()) << c.what;
        EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition) << c.what;
        EXPECT_NE(a.error().detail.find(c.detail), std::string::npos) << c.what << ": " << a.error().detail;
        EXPECT_EQ(s.digest(), before) << c.what;
    }
}

TEST(CatalogApplyV1Regression, ResolveAndUnresolveEscapeNamesInTheirErrors) {
    rc::CatalogState s;
    ASSERT_TRUE(rc::apply(s, stamped(s, createOps(0, true), "t", 1, 0, 1), 0));
    const rc::SchemaState& schema = *s.topicByName("t")->schema;
    cd::AlterOp ghost{}; ghost.type = cd::AlterOpType::Drop; ghost.name = "gh\x1b[2Jost\n";
    auto r = rc::resolveAlterOps(schema, std::vector<cd::AlterOp>{ghost});
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().find("'gh\\x1b[2Jost\\x0a'"), std::string::npos) << r.error();
    EXPECT_EQ(r.error().find('\x1b'), std::string::npos);
    EXPECT_EQ(r.error().find('\n'), std::string::npos);
    auto u = rc::unresolveAlterOps(schema, std::vector<rc::ResolvedAlterOp>{addOp(9, "a\tb")});
    ASSERT_FALSE(u.has_value());
    EXPECT_NE(u.error().find("'a\\x09b'"), std::string::npos) << u.error();
}

TEST(CatalogApplyV1Regression, TopicConfigErrorIsTheOneValidator) {
    EXPECT_FALSE(rc::topicConfigError(rc::TopicConfig{}).has_value()) << "today's defaults are valid";
    rc::TopicConfig edge;
    edge.retentionMs = -1; edge.retentionBytes = -1; edge.maxCompactionLagMs = -1;
    edge.deleteRetentionMs = 0; edge.minCompactionLagMs = 0; edge.minCleanableDirtyRatioPpm = 0;
    edge.segmentRollBytes = rc::kMaxSegmentRollBytes; edge.retentionCheckMs = 1; edge.fileDeleteDelayMs = 1;
    edge.cleanupPolicy = 2; edge.durability = 1; edge.durabilityTier = 2;
    EXPECT_FALSE(rc::topicConfigError(edge).has_value()) << *rc::topicConfigError(edge);
    edge.minCleanableDirtyRatioPpm = 1'000'000; edge.segmentRollBytes = 1;
    EXPECT_FALSE(rc::topicConfigError(edge).has_value());
    // The enum ranges (apply refuses them earlier as BadOps; front ends rely on this function).
    auto refused = [](auto mutate, const char* want) {
        rc::TopicConfig c; mutate(c);
        auto e = rc::topicConfigError(c);
        return e.has_value() && e->find(want) != std::string::npos;
    };
    EXPECT_TRUE(refused([](rc::TopicConfig& c) { c.cleanupPolicy = 3; }, "cleanup.policy 3 > 2"));
    EXPECT_TRUE(refused([](rc::TopicConfig& c) { c.durability = 2; }, "durability 2 > 1"));
    EXPECT_TRUE(refused([](rc::TopicConfig& c) { c.durabilityTier = 3; }, "durability.tier 3 > 2"));
    // AlterTopicConfig validates the RESULTING config with the same function.
    rc::CatalogState s = fixture();
    auto a = rc::apply(s, raw(rc::AlterTopicConfigOps{{{rc::ConfigKey::MinCompactionLagMs, -5}}}, "u", 2, 1, 2), 3);
    ASSERT_FALSE(a.has_value());
    EXPECT_NE(a.error().detail.find("config min.compaction.lag.ms -5 < 0"), std::string::npos) << a.error().detail;
}

TEST(CatalogApplyV1Regression, ConfigEnumBoundsAreOneDefinitionForEventValidationAndTopicConfigErrorRegression) {
    // Task 2 re-review N1: cleanup.policy / durability / durability.tier bounds were spelled
    // twice (the event validation and topicConfigError).  Now one definition: for every byte
    // value, encode (the event validation) and topicConfigError accept exactly the same set.
    enum class F { CleanupPolicy, Durability, DurabilityTier };
    for (F f : {F::CleanupPolicy, F::Durability, F::DurabilityTier}) {
        int accepted = 0;
        for (int v = 0; v <= 255; ++v) {
            rc::TopicConfig c;
            switch (f) {
            case F::CleanupPolicy:  c.cleanupPolicy = static_cast<uint8_t>(v); break;
            case F::Durability:     c.durability = static_cast<uint8_t>(v); break;
            case F::DurabilityTier: c.durabilityTier = static_cast<uint8_t>(v); break;
            }
            rc::CatalogEvent e;
            rc::CreateTopicOps o; o.topicId = 0; o.config = c;
            e.kind = rc::EventKind::CreateTopic; e.ops = o; e.objectName = "t"; e.objectId = 1; e.newVersion = 1;
            const bool encodes = rc::encode(e).has_value();
            const bool configOk = !rc::topicConfigError(c).has_value();
            EXPECT_EQ(encodes, configOk) << "field " << static_cast<int>(f) << " value " << v;
            accepted += configOk ? 1 : 0;
        }
        EXPECT_EQ(accepted, f == F::Durability ? 2 : 3) << "field " << static_cast<int>(f);
    }
}

// ── #143: a view sink mirrors its (first) source's partition count, checked at apply ─────────────────
// AdminWorker writes def.sinkPartitions = the first source's partitions and partition counts are immutable, so a
// mismatch comes only from a crafted or foreign catalog.  The verified apply refuses it (CreateView and AlterView;
// sink < source and sink > source; with two sources only the FIRST counts).  Each refused event carries the
// fingerprint of the object it would create, so without the rule it would apply cleanly.
namespace {
struct SinkCase {
    const char* what;
    bool        alter;                     // AlterView of v (else CreateView "x")
    std::vector<std::string> sources;
    int32_t     sinkPartitions;
    bool        refused;
};
}  // namespace

TEST(CatalogApplyRegression, AViewSinkWhosePartitionCountDiffersFromItsSourceIsRefusedRegression) {
    const std::vector<SinkCase> cases = {
        {"create: sink < source",                   false, {"src"},       1, true},
        {"create: sink > source",                   false, {"src"},       3, true},
        {"create: sink == source",                  false, {"src"},       2, false},
        {"create: two sources, sink = the 2nd's",   false, {"src", "w4"}, 4, true},
        {"create: two sources, sink = the 1st's",   false, {"src", "w4"}, 2, false},
        {"create: first source has 4",              false, {"w4", "src"}, 2, true},
        {"create: first source has 4, sink 4",      false, {"w4", "src"}, 4, false},
        {"alter: sink < source",                    true,  {"src"},       1, true},
        {"alter: sink > source",                    true,  {"src"},       3, true},
        {"alter: sink == source",                   true,  {"src"},       2, false},
        {"alter: two sources, sink = the 2nd's",    true,  {"src", "w4"}, 4, true},
        {"alter: re-source onto w4 keeping 2",      true,  {"w4"},        2, true},
        {"alter: re-source onto w4 with 4",         true,  {"w4"},        4, false},
    };
    for (const SinkCase& c : cases) {
        rc::CatalogState s = fixture();                                   // src, u (2 partitions), v over src (sink 2)
        rc::CreateTopicOps w4 = createOps(3, false);
        w4.partitions = 4;
        ASSERT_TRUE(rc::apply(s, stamped(s, w4, "w4", 4, 0, 1), 3)) << c.what;
        const uint64_t before = s.digest();
        const rc::ViewDef def = viewOver(c.sources, /*sink=*/4, c.sinkPartitions);   // nextTopicId is 4
        rc::CatalogEvent e;
        rc::ViewObject result;
        if (c.alter) {
            e = raw(rc::AlterViewOps{def}, "v", 3, 1, 2);
            result = rc::ViewObject{.objectId = 3, .name = "v", .version = 2, .def = def};
        } else {
            e = raw(rc::CreateViewOps{def}, "x", 5, 0, 1);
            result = rc::ViewObject{.objectId = 5, .name = "x", .version = 1, .def = def};
        }
        e.fingerprint = rc::fingerprintOf(result);
        const std::string first = c.sources.front();
        const int32_t sourceParts = s.topicByName(first)->partitions;
        auto a = rc::apply(s, e, 4);
        if (!c.refused) {
            EXPECT_TRUE(a.has_value()) << c.what << ": " << (a ? "" : a.error().detail);
            continue;
        }
        if (a.has_value()) { ADD_FAILURE() << c.what << ": a sink of " << c.sinkPartitions << " over '" << first
                                           << "' (" << sourceParts << ") was applied"; continue; }
        EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition) << c.what << ": " << a.error().detail;
        EXPECT_EQ(a.error().offset, 4) << c.what;
        const std::string& d = a.error().detail;
        EXPECT_NE(d.find(c.alter ? "'v'" : "'x'"), std::string::npos) << c.what << ": names the view: " << d;
        EXPECT_NE(d.find("sink partitions " + std::to_string(c.sinkPartitions)), std::string::npos) << c.what << ": " << d;
        EXPECT_NE(d.find("source '" + first + "' has " + std::to_string(sourceParts)), std::string::npos) << c.what << ": " << d;
        EXPECT_EQ(s.digest(), before) << c.what << ": a refused event changes nothing";
        auto p = rc::previewFingerprint(s, e);
        EXPECT_FALSE(p.has_value()) << c.what << ": previewFingerprint agrees with apply";
    }
}

// ── Adversarial review of PR #597 (#143): the sink-partition rule at its bounds and over random DDL histories ─────
namespace {
/// A CreateView of `name` over `sources` with `sinkPartitions`, fingerprinted as the object it would create (so only
/// the apply rules can refuse it).
rc::CatalogEvent craftedCreateView(const rc::CatalogState& s, std::string name, std::vector<std::string> sources,
                                   int32_t sinkPartitions) {
    rc::ViewDef def = viewOver(std::move(sources), s.nextTopicId(), sinkPartitions);
    rc::CatalogEvent e = raw(rc::CreateViewOps{def}, name, s.nextObjectId(), 0, 1);
    e.fingerprint = rc::fingerprintOf(rc::ViewObject{.objectId = e.objectId, .name = name, .version = 1, .def = def});
    return e;
}
rc::CatalogEvent craftedAlterView(const rc::CatalogState& s, const rc::ViewObject& v, std::vector<std::string> sources,
                                  int32_t sinkPartitions) {
    rc::ViewDef def = viewOver(std::move(sources), s.nextTopicId(), sinkPartitions);
    rc::CatalogEvent e = raw(rc::AlterViewOps{def}, v.name, v.objectId, v.version, v.version + 1);
    e.fingerprint = rc::fingerprintOf(rc::ViewObject{.objectId = v.objectId, .name = v.name, .version = v.version + 1, .def = def});
    return e;
}
void expectEverySinkMirrorsItsFirstSource(const rc::CatalogState& s, const std::string& when) {
    for (const rc::ViewObject* v : s.views()) {
        const rc::TopicObject* first = s.topicByName(v->def.sourceNames.front());
        ASSERT_NE(first, nullptr) << when << ": view " << v->name << " reads a dead topic";
        EXPECT_EQ(v->def.sinkPartitions, first->partitions) << when << ": view " << v->name;
    }
}
}  // namespace

// Input bounds: sink partition counts at i32 min, -1, 0, P-1, P+1 and i32 max over sources of 1, 7 and i32 max
// partitions, at CreateView and AlterView.  Only sink == P applies; every other one is a Precondition that changes
// nothing, and the leader's previewFingerprint refuses it too.
TEST(CatalogApplyAdversarial, SinkPartitionBoundsAdversarialRegression) {
    constexpr int32_t kMax = std::numeric_limits<int32_t>::max();
    constexpr int32_t kMin = std::numeric_limits<int32_t>::min();
    for (const int32_t P : {1, 7, kMax}) {
        std::vector<int32_t> sinks = {kMin, -1, 0, 1, P - 1, P, kMax};
        if (P < kMax) sinks.push_back(P + 1);
        for (const bool alter : {false, true}) {
            for (const int32_t sink : sinks) {
                const std::string what = std::string(alter ? "alter" : "create") + " source " + std::to_string(P) +
                                         " sink " + std::to_string(sink);
                rc::CatalogState s;
                rc::CreateTopicOps t = createOps(0, true);
                t.partitions = P;
                ASSERT_TRUE(rc::apply(s, stamped(s, t, "s", 1, 0, 1), 0)) << what;
                if (alter) ASSERT_TRUE(rc::apply(s, craftedCreateView(s, "v", {"s"}, P), 1)) << what;
                const rc::CatalogEvent e =
                    alter ? craftedAlterView(s, *s.viewByName("v"), {"s"}, sink) : craftedCreateView(s, "v", {"s"}, sink);
                const uint64_t before = s.digest();
                const bool expectOk = sink == P;
                EXPECT_EQ(rc::previewFingerprint(s, e).has_value(), expectOk) << what;
                auto a = rc::apply(s, e, 2);
                ASSERT_EQ(a.has_value(), expectOk) << what << (a ? "" : ": " + a.error().detail);
                if (expectOk) { EXPECT_EQ(s.viewByName("v")->def.sinkPartitions, P) << what; continue; }
                EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition) << what;
                if (sink >= 1)   // a sink < 1 is refused by the older rule first
                    EXPECT_NE(a.error().detail.find("sink partitions " + std::to_string(sink)), std::string::npos)
                        << what << ": " << a.error().detail;
                EXPECT_EQ(s.digest(), before) << what << ": a refused event changes nothing";
            }
        }
    }
}

// Data integrity, model-checked: seeded random DDL histories (create topics of 1..4 partitions, create / alter /
// drop views over 1-2 live sources with a correct or a random sink count, drop unread topics, and reuse dropped
// topic names with a different partition count).  Every view event applies iff its sink equals its first source's
// partitions; after every step every live view mirrors its first source; and replaying the applied events into a
// fresh state gives the same digest (the state is a pure function of the log).
TEST(CatalogApplyAdversarial, RandomDdlHistoriesKeepEverySinkMirroringItsFirstSourceAdversarialRegression) {
    const std::string topicNames[] = {"t0", "t1", "t2", "t3"};
    const std::string viewNames[]  = {"v0", "v1", "v2"};
    int refusedMismatches = 0, appliedViews = 0;
    for (uint64_t seed = 1; seed <= 200; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        uint64_t st = seed * 0x9E3779B97F4A7C15ULL;
        auto rnd = [&](uint32_t n) { st = st * 6364136223846793005ULL + 1442695040888963407ULL; return uint32_t(st >> 33) % n; };
        rc::CatalogState s;
        std::vector<std::pair<rc::CatalogEvent, int64_t>> log;
        int64_t off = 0;
        auto liveTopicNames = [&] { std::vector<std::string> v; for (auto* t : s.topics()) v.push_back(t->name); return v; };
        auto randomSources = [&] {
            auto live = liveTopicNames();
            std::vector<std::string> src{live[rnd(uint32_t(live.size()))]};
            if (live.size() > 1 && rnd(2) == 0) {
                const std::string& second = live[rnd(uint32_t(live.size()))];
                if (second != src[0]) src.push_back(second);
            }
            return src;
        };
        for (int step = 0; step < 60; ++step) {
            const std::string when = "step " + std::to_string(step);
            const uint32_t kind = rnd(5);
            std::optional<rc::CatalogEvent> e;
            std::optional<bool> expectOk;
            if (kind == 0) {                                                   // CreateTopic (reuses dropped names)
                const std::string& n = topicNames[rnd(4)];
                if (s.nameTaken(n)) continue;
                rc::CreateTopicOps o = createOps(s.nextTopicId(), true);
                o.partitions = int32_t(1 + rnd(4));
                e = stamped(s, o, n, s.nextObjectId(), 0, 1);
                expectOk = true;
            } else if (kind == 1 || kind == 2) {                               // CreateView / AlterView
                if (s.topics().empty()) continue;
                const auto src = randomSources();
                const int32_t firstParts = s.topicByName(src[0])->partitions;
                const int32_t sink = rnd(2) == 0 ? firstParts : int32_t(1 + rnd(4));
                if (kind == 1) {
                    const std::string& n = viewNames[rnd(3)];
                    if (s.nameTaken(n)) continue;
                    e = craftedCreateView(s, n, src, sink);
                } else {
                    const auto views = s.views();
                    if (views.empty()) continue;
                    e = craftedAlterView(s, *views[rnd(uint32_t(views.size()))], src, sink);
                }
                expectOk = sink == firstParts;
            } else if (kind == 3) {                                            // DropView
                const auto views = s.views();
                if (views.empty()) continue;
                const rc::ViewObject& v = *views[rnd(uint32_t(views.size()))];
                e = stamped(s, rc::DropViewOps{v.def.viewTopicId}, v.name, v.objectId, v.version, v.version + 1);
                expectOk = true;
            } else {                                                           // DropTopic, when no view reads it
                const auto topics = s.topics();
                if (topics.empty()) continue;
                const rc::TopicObject& t = *topics[rnd(uint32_t(topics.size()))];
                if (s.viewReading(t.name) != nullptr) continue;
                e = stamped(s, rc::DropTopicOps{t.topicId}, t.name, t.objectId, t.version, t.version + 1);
                expectOk = true;
            }
            const uint64_t before = s.digest();
            auto a = rc::apply(s, *e, off);
            ASSERT_EQ(a.has_value(), *expectOk) << when << " kind " << kind << (a ? "" : ": " + a.error().detail);
            if (a) {
                log.emplace_back(*e, off++);
                appliedViews += (kind == 1 || kind == 2);
            } else {
                ++refusedMismatches;
                EXPECT_EQ(a.error().code, rc::ApplyError::Code::Precondition) << when;
                EXPECT_EQ(s.digest(), before) << when;
            }
            ASSERT_NO_FATAL_FAILURE(expectEverySinkMirrorsItsFirstSource(s, when));
        }
        rc::CatalogState replayed;
        for (const auto& [ev, o] : log) ASSERT_TRUE(rc::apply(replayed, ev, o)) << "replay of offset " << o;
        EXPECT_EQ(replayed.digest(), s.digest()) << "the state equals a recompute from its log";
    }
    EXPECT_GT(refusedMismatches, 300) << "not vacuous: many mismatched sinks were attempted";
    EXPECT_GT(appliedViews, 300) << "not vacuous: many view events applied";
}
