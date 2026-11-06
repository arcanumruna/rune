// CheckedWritesConstraintDdlTest.cpp — checked writes Part 09 Task 3: the AdminWorker accepts `unique` and `check` views (R08e),
// refuses them over a transient source (R10a) and enforces the constraint id and member limits (SD5, P9-9), against the REAL
// AdminWorker (CatalogDdlRig).
#include <rune/codec/Serialize.h>
#include <rune/node/ConstraintDdlRules.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/view/PlanCompiler.h>
#include <rune/view/Verdict.h>
#include <rune/view/ViewPlan.h>

#include "support/CatalogDdlRig.h"

#include <gtest/gtest.h>

#include <fstream>
#include <map>
#include <sstream>

using namespace rune::node;
using namespace rune::node::test;
namespace ql = rune::ql;
namespace rv = rune::view;
namespace cat = rune::catalog;
using rune::protocol::client::ErrorCode;
using rune::replication::MetadataFeature::kCheckedWrites;

namespace {

constexpr uint16_t kOk = 0;

uint16_t codeOf(ErrorCode c) { return static_cast<uint16_t>(c); }

ql::TopicDef topicOf(const std::string& text) {
    auto st = ql::parse(text);
    EXPECT_TRUE(st.has_value()) << text;
    return st ? std::get<ql::TopicDef>(*st) : ql::TopicDef{};
}

ql::CreateViewDef viewOf(const std::string& text) {
    auto st = ql::parse(text);
    EXPECT_TRUE(st.has_value()) << text << (st ? "" : ": " + st.error().message);
    return st ? std::get<ql::CreateViewDef>(*st) : ql::CreateViewDef{};
}

CatalogDdlRig::Answer defineTopic(CatalogDdlRig& rig, const ql::TopicDef& t, uint8_t persistence = 0, bool keyIndex = false) {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = t.name; m.keyField = t.keyField;
    m.blob = rune::codec::serialize(ql::toDescriptor(t));
    m.partitionCount = t.partitions;
    m.persistence = persistence;
    m.keyFeatures.keyIndex = keyIndex;
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

void serving(CatalogDdlRig& rig, int16_t level = kCheckedWrites) {
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(level);
}

rv::ViewPlan planOf(CatalogDdlRig& rig, const ql::TopicDef& topic, const std::string& text) {
    auto plan = ql::applyViewPlan(viewOf(text), ql::viewSourceOf(topic));
    EXPECT_TRUE(plan.has_value()) << text << (plan ? "" : ": " + plan.error().message);
    rv::ViewPlan p = plan ? *plan : rv::ViewPlan{};
    p.sources[0].topicId = rig.admin().applier().state().topicByName(0, topic.name)->topicId;
    return p;
}

std::string keyFieldOf(const rv::ViewPlan& p) { return p.keyComponents.empty() ? p.key : p.keyComponents[0].outName; }

CatalogDdlRig::Answer submitView(CatalogDdlRig& rig, const rv::ViewPlan& p) {
    return rig.createViewWithPlan(p.viewName, keyFieldOf(p), p.persistence, rv::serializeAuthored(p));
}

CatalogDdlRig::Answer submitAlter(CatalogDdlRig& rig, const rv::ViewPlan& p) {
    AlterViewMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = p.viewName; m.keyField = keyFieldOf(p);
    m.planBlob = rv::serializeAuthored(p);
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

rv::CompiledViewPlan compiledOfLast(CatalogDdlRig& rig, uint64_t* objectId = nullptr) {
    const cat::CatalogEvent e = rig.recordedLog().back().event;
    if (objectId != nullptr) *objectId = e.objectId;
    const cat::ViewDef& def = std::holds_alternative<cat::CreateViewOps>(e.ops) ? std::get<cat::CreateViewOps>(e.ops).view
                                                                                   : std::get<cat::AlterViewOps>(e.ops).view;
    auto p = rv::deserializeCompiledViewPlan(def.compiledPlanBlob);
    EXPECT_TRUE(p.has_value());
    return p ? std::move(*p) : rv::CompiledViewPlan{};
}

const char* kSource = "topic src { id uint64, userId uint64, name string(16), n int64 } partitions 1 key id;";

std::string checkView(int i) { return "create view v" + std::to_string(i) + " key id as select n from src constraint c1 check (n >= 0);"; }

// A key.index topic `src` and 31 check views over it: 31 + the key index + the next constrained view = 33 > 32.
void fillToTheLimit(CatalogDdlRig& rig, const ql::TopicDef& topic) {
    ASSERT_EQ(defineTopic(rig, topic, 0, true).code, kOk);
    for (int i = 0; i < 31; ++i) {
        const auto a = submitView(rig, planOf(rig, topic, checkView(i)));
        ASSERT_EQ(a.code, kOk) << i << ": " << a.detail;
    }
}

const char* kMembersMessage = "a topic feeds at most 32 verdict views (the key index counts as one)";

}  // namespace

TEST(CheckedWritesConstraintDdl, VerdictKindAndFingerprintOfAPlan) {
    rv::CompiledViewPlan plain;
    EXPECT_FALSE(rv::verdictKindOf(plain).has_value());
    rv::CompiledViewPlan checked;
    checked.constraints.push_back({1, "c", {}});
    EXPECT_EQ(rv::verdictKindOf(checked), rv::VerdictViewKind::Check);
    rv::CompiledViewPlan unique;
    unique.unique = true;
    EXPECT_EQ(rv::verdictKindOf(unique), rv::VerdictViewKind::Unique);
    unique.constraints.push_back({1, "c", {}});
    EXPECT_EQ(rv::verdictKindOf(unique), rv::VerdictViewKind::Unique);

    EXPECT_EQ(rv::verdictPlanFingerprint({}), 0xcbf29ce484222325ull);
    const std::vector<uint8_t> blob = {1, 2, 3, 250};
    uint64_t h = 0xcbf29ce484222325ull;
    for (const uint8_t b : blob) h = (h ^ b) * 0x100000001b3ull;
    EXPECT_EQ(rv::verdictPlanFingerprint(blob), h);
    std::vector<uint8_t> other = blob;
    other[2] ^= 1;
    EXPECT_NE(rv::verdictPlanFingerprint(other), h);
}

TEST(CheckedWritesConstraintDdl, ConstrainedViewRefusalBoundaries) {
    EXPECT_EQ(constrainedViewRefusal(65535, 1, 2, 100), std::nullopt);
    EXPECT_EQ(constrainedViewRefusal(65536, 1, 2, 100), "constraint ids exhausted");
    EXPECT_EQ(constrainedViewRefusal(7, 65535, 2, 100), std::nullopt);
    EXPECT_EQ(constrainedViewRefusal(7, 65536, 2, 100), "constraint ids exhausted");
    EXPECT_EQ(constrainedViewRefusal(7, 1, 32, 100), std::nullopt);
    EXPECT_EQ(constrainedViewRefusal(7, 1, 33, 100), kMembersMessage);
    EXPECT_EQ(constrainedViewRefusal(7, 1, 2, 65536), std::nullopt);
    EXPECT_EQ(constrainedViewRefusal(7, 1, 2, 65537), "a constrained view's plan must encode in at most 65536 bytes");
}

TEST(CheckedWritesConstraintDdl, RegressorsPathViewsAreCreated) {
    std::ifstream in(std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql");
    ASSERT_TRUE(in.good());
    std::stringstream ss;
    ss << in.rdbuf();
    const auto script = ql::parseScript(ss.str());
    ASSERT_TRUE(script.has_value());

    CatalogDdlRig rig("cwcdr");
    serving(rig);
    std::map<std::string, ql::TopicDef> topics;
    for (const ql::Statement& st : *script) {
        if (const auto* t = std::get_if<ql::TopicDef>(&st)) {
            topics[t->name] = *t;
            ASSERT_EQ(defineTopic(rig, *t).code, kOk) << t->name;
            continue;
        }
        const auto* cv = std::get_if<ql::CreateViewDef>(&st);
        if (cv == nullptr) continue;
        const auto plan = ql::applyViewPlan(*cv, ql::viewSourceOf(topics.at(cv->source)));
        ASSERT_TRUE(plan.has_value()) << cv->name;
        rv::ViewPlan p = *plan;
        p.sources[0].topicId = rig.admin().applier().state().topicByName(0, cv->source)->topicId;
        const auto a = submitView(rig, p);
        ASSERT_EQ(a.code, kOk) << cv->name << ": " << a.detail;
        uint64_t objectId = 0;
        const rv::CompiledViewPlan compiled = compiledOfLast(rig, &objectId);
        if (cv->name == "character_name") {
            EXPECT_TRUE(compiled.unique);
            EXPECT_EQ(compiled.viewObjectId, objectId);
        }
        if (cv->name == "account_stats") {
            ASSERT_EQ(compiled.constraints.size(), 1u);
            EXPECT_EQ(compiled.constraints[0].ordinal, 1);
            EXPECT_EQ(compiled.constraints[0].name, "max_characters");
        }
    }
}

TEST(CheckedWritesConstraintDdl, TransientSourceIsRefused) {
    CatalogDdlRig rig("cwcdt");
    serving(rig);
    const ql::TopicDef topic = topicOf(kSource);
    ASSERT_EQ(defineTopic(rig, topic, 1).code, kOk);
    const auto u = submitView(rig, planOf(rig, topic, "create unique view u key lower(name) as k as select id from src;"));
    EXPECT_EQ(u.code, codeOf(ErrorCode::InvalidRequest));
    EXPECT_EQ(u.detail, "constraints need a persisted topic");
    const auto c = submitView(rig, planOf(rig, topic, checkView(0)));
    EXPECT_EQ(c.code, codeOf(ErrorCode::InvalidRequest));
    EXPECT_EQ(c.detail, "constraints need a persisted topic");
    const auto p = submitView(rig, planOf(rig, topic, "create view plain key id as select n from src;"));
    EXPECT_EQ(p.code, kOk) << p.detail;
}

TEST(CheckedWritesConstraintDdl, TransientConstrainedViewOverAPersistedSourceIsAllowed) {
    CatalogDdlRig rig("cwcdp");
    serving(rig);
    const ql::TopicDef topic = topicOf(kSource);
    ASSERT_EQ(defineTopic(rig, topic).code, kOk);
    const auto a = submitView(rig, planOf(rig, topic, "create transient unique view u key lower(name) as k as select id from src;"));
    EXPECT_EQ(a.code, kOk) << a.detail;
}

TEST(CheckedWritesConstraintDdl, ThirtyThirdMemberIsRefused) {
    CatalogDdlRig rig("cwcd33");
    serving(rig);
    const ql::TopicDef topic = topicOf(kSource);
    fillToTheLimit(rig, topic);
    const auto over = submitView(rig, planOf(rig, topic, checkView(31)));
    EXPECT_EQ(over.code, codeOf(ErrorCode::ViewConstraintInvalid));
    EXPECT_EQ(over.detail, kMembersMessage);
    const auto plain = submitView(rig, planOf(rig, topic, "create view plain key id as select n from src;"));
    EXPECT_EQ(plain.code, kOk) << plain.detail;
}

TEST(CheckedWritesConstraintDdl, AlterKeepsTheObjectIdAndCountsItselfOnce) {
    CatalogDdlRig rig("cwcda");
    serving(rig);
    const ql::TopicDef topic = topicOf(kSource);
    ASSERT_EQ(defineTopic(rig, topic).code, kOk);
    const auto created = submitView(rig, planOf(rig, topic, "create view stats key userId as select count(*) as n from src group by userId constraint a check (n <= 5);"));
    ASSERT_EQ(created.code, kOk) << created.detail;
    uint64_t createdId = 0;
    (void)compiledOfLast(rig, &createdId);
    const auto altered = submitAlter(rig, planOf(rig, topic, "create view stats key userId as select count(*) as n from src group by userId constraint a check (n <= 5) constraint b check (n >= 0);"));
    ASSERT_EQ(altered.code, kOk) << altered.detail;
    uint64_t alteredId = 0;
    const rv::CompiledViewPlan plan = compiledOfLast(rig, &alteredId);
    EXPECT_EQ(alteredId, createdId);
    EXPECT_EQ(plan.viewObjectId, createdId);
    ASSERT_EQ(plan.constraints.size(), 2u);
    EXPECT_EQ(plan.constraints[0].ordinal, 1);
    EXPECT_EQ(plan.constraints[1].ordinal, 2);

    CatalogDdlRig full("cwcda2");
    serving(full);
    fillToTheLimit(full, topic);
    const auto again = submitAlter(full, planOf(full, topic, checkView(5)));
    EXPECT_EQ(again.code, kOk) << again.detail;
}

TEST(CheckedWritesConstraintDdl, CheckOnAWideColumnIsViewConstraintInvalid) {
    CatalogDdlRig rig("cwcdw");
    serving(rig);
    const ql::TopicDef topic = topicOf("topic user { userId uint64, provider uint16, subject string(64) } partitions 1 key userId;");
    ASSERT_EQ(defineTopic(rig, topic).code, kOk);
    rv::ViewPlan plan = planOf(rig, topic, "create view ident key userId as select subject from user;");   // QL refuses the wide column itself
    rv::PredicateNode subject{rv::PredicateOp::Column, 0, 0, "subject"};
    rv::PredicateNode a1{rv::PredicateOp::Const, 0, 0, "", rune::codec::PrimType::FixedStr, {'a'}};
    plan.constraints = {{"c1", {subject, a1, {rv::PredicateOp::Eq, 0, 1, ""}}}};
    const auto a = submitView(rig, plan);
    EXPECT_EQ(a.code, codeOf(ErrorCode::ViewConstraintInvalid));
    EXPECT_EQ(a.detail, "view 'ident': constraint 'c1': check compares fixed-width columns of at most 16 bytes; 'subject' is string(64)");
}

TEST(CheckedWritesConstraintDdl, BelowTheLevelIsInvalidRequest) {
    CatalogDdlRig rig("cwcdl");
    serving(rig, kCheckedWrites - 1);
    const ql::TopicDef topic = topicOf(kSource);
    ASSERT_EQ(defineTopic(rig, topic).code, kOk);
    const auto a = submitView(rig, planOf(rig, topic, "create unique view u key lower(name) as k as select id from src;"));
    EXPECT_EQ(a.code, codeOf(ErrorCode::InvalidRequest));
    EXPECT_EQ(a.detail, "plan v2 needs the checked-writes feature level");
}
