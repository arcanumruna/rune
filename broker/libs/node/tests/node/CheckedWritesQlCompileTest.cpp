// CheckedWritesQlCompileTest.cpp — checked writes Part 08 Task 5 (QM2): plans authored by rune::ql::applyViewPlan meet the broker's
// compiler (rune::view::compileView) and the real AdminWorker (CatalogDdlRig).
#include <rune/codec/Serialize.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <rune/replication/ReplicationMessages.h>
#include <rune/view/PlanCompiler.h>
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
using rune::protocol::client::ErrorCode;
using rune::replication::MetadataFeature::kCheckedWrites;

namespace {

constexpr int32_t kViewTopic = 0x40000007;

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

CatalogDdlRig::Answer defineTopic(CatalogDdlRig& rig, const ql::TopicDef& t) {
    DefineTopicMsg m;
    m.session = rig.sessionId(); m.corr = rig.nextCorr(); m.name = t.name; m.keyField = t.keyField;
    m.blob = rune::codec::serialize(ql::toDescriptor(t));
    m.partitionCount = t.partitions;
    m.meta.requestId = rig.freshRequestId();
    return rig.submitRaw(DdlRequest{std::move(m)});
}

void serving(CatalogDdlRig& rig) {
    rig.becomeServingLeader();
    rig.admin().setClusterFeatureLevel(kCheckedWrites);
}

// The statement as the client submits it: the plan with the resolved topic id and the first key column as keyField.
CatalogDdlRig::Answer submitView(CatalogDdlRig& rig, rv::ViewPlan plan, const std::string& topic) {
    plan.sources[0].topicId = rig.admin().applier().state().topicByName(0, topic)->topicId;
    const std::string keyField = plan.keyComponents.empty() ? plan.key : plan.keyComponents[0].outName;   // Client::viewKeyField
    return rig.createViewWithPlan(plan.viewName, keyField, plan.persistence, rv::serializeAuthored(plan));
}

std::expected<rv::CompileResult, rv::CompileError> compileOver(const rv::ViewPlan& p, const rune::codec::Descriptor& d) {
    const rune::codec::Descriptor* srcs[] = {&d};
    return rv::compileView(p, srcs, kViewTopic);
}

const char* kSource = "{ id uint64, userId uint64, name string(16), tag string(80), gender uint16, n int64 }";

const char* kKeys[] = {"key id", "key userId", "key (userId, id)", "key lower(name) as nameKey", "key (lower(name) as nameKey, id)",
                       "key (lower(tag) as tagKey, id)", "key (lower(tag) as tagKey, id, gender)"};
const char* kGroups[] = {"id", "userId", "userId, id", "lower(name)", "lower(name), id", "lower(tag), id", "lower(tag), id, gender"};

enum class Kind : uint8_t { Projection, Aggregate, Unique };
enum class Outcome : uint8_t { Legacy, Keyed, KeyedOwnerMapped, Refused };

struct MatrixCell {
    int key;       // 0..6 = K1..K7
    Kind kind;
    int source;    // 1..3 = S1..S3
    uint16_t admin;                      // the AdminWorker's answer (0 for a unique cell: it runs the compile only)
    Outcome compile;
    uint16_t width;                      // keyWidth for a Keyed outcome
};

// K x kind x S1/S2/S3 admin codes (the QM2 tables) and the compile outcome that the S1 answer is built on.
std::vector<MatrixCell> cells() {
    const uint16_t kRefused = codeOf(ErrorCode::ViewKeyExpressionInvalid), kPartitions = codeOf(ErrorCode::ViewNotCoPartitioned),
                   kKeyless = codeOf(ErrorCode::ViewKeyInvalid);
    struct Row { Outcome outcome; uint16_t width; bool multiPartitionOk; };
    const Row projection[] = {{Outcome::Legacy, 8, true}, {Outcome::Refused, 0, false}, {Outcome::KeyedOwnerMapped, 16, false},
                              {Outcome::Refused, 0, false}, {Outcome::KeyedOwnerMapped, 32, false}, {Outcome::KeyedOwnerMapped, 128, false},
                              {Outcome::Refused, 0, false}};
    const Row aggregate[] = {{Outcome::Legacy, 8, false}, {Outcome::Legacy, 8, false}, {Outcome::Keyed, 16, false}, {Outcome::Keyed, 24, false},
                             {Outcome::Keyed, 32, false}, {Outcome::Keyed, 128, false}, {Outcome::Refused, 0, false}};
    std::vector<MatrixCell> out;
    for (int k = 0; k < 7; ++k) {
        for (const Kind kind : {Kind::Projection, Kind::Aggregate}) {
            const Row& r = kind == Kind::Projection ? projection[k] : aggregate[k];
            const uint16_t second = r.outcome == Outcome::Refused ? kRefused : r.multiPartitionOk ? uint16_t{0} : kPartitions;
            out.push_back({k, kind, 1, r.outcome == Outcome::Refused ? kRefused : uint16_t{0}, r.outcome, r.width});
            out.push_back({k, kind, 2, second, r.outcome, r.width});
            out.push_back({k, kind, 3, kKeyless, r.outcome, r.width});
        }
    }
    const Row unique[] = {{Outcome::Legacy, 8, false}, {Outcome::KeyedOwnerMapped, 8, false}, {Outcome::KeyedOwnerMapped, 16, false},
                          {Outcome::KeyedOwnerMapped, 24, false}, {Outcome::KeyedOwnerMapped, 32, false}, {Outcome::KeyedOwnerMapped, 128, false},
                          {Outcome::Refused, 0, false}};
    for (int k = 0; k < 7; ++k) out.push_back({k, Kind::Unique, 1, 0, unique[k].outcome, unique[k].width});
    return out;
}

std::string viewText(const MatrixCell& c) {
    const std::string head = std::string("create ") + (c.kind == Kind::Unique ? "unique " : "") + "view v " + kKeys[c.key] + " as select ";
    if (c.kind == Kind::Aggregate) return head + "count(*) as c from src group by " + kGroups[c.key] + ";";
    return head + "n from src;";
}

class Cell : public ::testing::TestWithParam<int> {};

}  // namespace

TEST_P(Cell, QlPlanMeetsTheBroker) {
    const MatrixCell c = cells()[static_cast<std::size_t>(GetParam())];
    const int partitions = c.source == 2 ? 3 : 1;
    const std::string topicText = std::string("topic src ") + kSource + " partitions " + std::to_string(partitions) +
                                  (c.source == 3 ? ";" : " key id;");
    const ql::TopicDef topic = topicOf(topicText);
    const auto plan = ql::applyViewPlan(viewOf(viewText(c)), ql::viewSourceOf(topic));
    ASSERT_TRUE(plan.has_value()) << viewText(c) << ": " << plan.error().message;

    if (c.source != 3) {   // a keyless source never reaches the compile (the broker refuses it first)
        const auto desc = ql::toDescriptor(topic);
        const auto compiled = compileOver(*plan, desc);
        if (c.compile == Outcome::Refused) {
            ASSERT_FALSE(compiled.has_value()) << viewText(c);
            EXPECT_EQ(compiled.error().kind, rv::CompileErrorKind::KeyExpressionInvalid) << compiled.error().message;
        } else {
            ASSERT_TRUE(compiled.has_value()) << viewText(c) << ": " << compiled.error().message;
            EXPECT_EQ(rv::keyModeOf(compiled->plan), c.compile == Outcome::Legacy ? rv::ViewKeyMode::LegacyU64 : rv::ViewKeyMode::Keyed) << viewText(c);
            EXPECT_EQ(rv::ownerMappedOf(compiled->plan), c.compile == Outcome::KeyedOwnerMapped) << viewText(c);
            EXPECT_EQ(compiled->plan.keyWidth, c.width) << viewText(c);
        }
    }
    if (c.kind == Kind::Unique) return;   // the admin refuses every unique view before Part 09 (CheckedWritesQlCompile.UniqueViewIsRefusedBeforePart09)

    CatalogDdlRig rig("cwqm");
    serving(rig);
    ASSERT_EQ(defineTopic(rig, topic).code, 0);
    const auto a = submitView(rig, *plan, "src");
    EXPECT_EQ(a.code, c.admin) << viewText(c) << " S" << c.source << ": " << a.detail;
    if (c.source == 3) EXPECT_EQ(a.detail, "view source needs a key");
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesQlCompileMatrix, Cell, ::testing::Range(0, 49));

TEST(CheckedWritesQlCompileMatrixCount, CellCountIsPinned) { EXPECT_EQ(cells().size(), 49u); }

TEST(CheckedWritesQlCompile, UniqueViewIsRefusedBeforePart09) {
    const ql::TopicDef topic = topicOf(std::string("topic src ") + kSource + " partitions 1 key id;");
    const auto plan = ql::applyViewPlan(viewOf("create unique view v key (lower(name) as nameKey, id) as select n from src;"), ql::viewSourceOf(topic));
    ASSERT_TRUE(plan.has_value());
    CatalogDdlRig rig("cwqu");
    serving(rig);
    ASSERT_EQ(defineTopic(rig, topic).code, 0);
    const auto a = submitView(rig, *plan, "src");
    EXPECT_EQ(a.code, codeOf(ErrorCode::ViewUniqueInvalid));
    EXPECT_EQ(a.detail, "unique views are not available yet");
}

TEST(CheckedWritesQlCompile, GroupByOrderIsTheCompilersRule) {
    const ql::TopicDef topic = topicOf("topic device { id uint64, userId uint64, deviceFp string(32), ts int64 } partitions 1 key id;");
    const auto plan = ql::applyViewPlan(viewOf("create view v key (userId, deviceFp) as select count(*) as c from device group by deviceFp, userId;"),
                                        ql::viewSourceOf(topic));
    ASSERT_TRUE(plan.has_value()) << plan.error().message;
    CatalogDdlRig rig("cwqg");
    serving(rig);
    ASSERT_EQ(defineTopic(rig, topic).code, 0);
    const auto a = submitView(rig, *plan, "device");
    EXPECT_EQ(a.code, codeOf(ErrorCode::ViewKeyInvalid));
    EXPECT_EQ(a.detail, "view 'v': group by (deviceFp, userId) must equal the key (userId, deviceFp)");
}

TEST(CheckedWritesQlCompileRegression, GroupColumnOtherThanTheKeyNamesTheCompilersDetailRegression) {
    const ql::TopicDef topic = topicOf("topic src { id uint64, userId uint64, gender uint16 } partitions 1 key id;");
    const auto plan = ql::applyViewPlan(viewOf("create view v key userId as select count(*) as n from src group by gender;"), ql::viewSourceOf(topic));
    ASSERT_TRUE(plan.has_value()) << plan.error().message;
    CatalogDdlRig rig("cwqk");
    serving(rig);
    ASSERT_EQ(defineTopic(rig, topic).code, 0);
    const auto a = submitView(rig, *plan, "src");
    EXPECT_EQ(a.code, codeOf(ErrorCode::ViewKeyInvalid));
    EXPECT_EQ(a.detail, "view 'v': group by (gender) must equal the key (userId)");
}

TEST(CheckedWritesQlCompile, RegressorsPathCompilesWithTheBrokerCompiler) {
    std::ifstream in(std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql");
    ASSERT_TRUE(in.good());
    std::stringstream ss;
    ss << in.rdbuf();
    const auto script = ql::parseScript(ss.str());
    ASSERT_TRUE(script.has_value());

    std::map<std::string, ql::TopicDef> topics;
    std::map<std::string, rv::CompileResult> compiled;
    for (const ql::Statement& st : *script) {
        if (const auto* t = std::get_if<ql::TopicDef>(&st)) topics[t->name] = *t;
        const auto* cv = std::get_if<ql::CreateViewDef>(&st);
        if (cv == nullptr) continue;
        const ql::TopicDef& topic = topics.at(cv->source);
        const auto plan = ql::applyViewPlan(*cv, ql::viewSourceOf(topic));
        ASSERT_TRUE(plan.has_value()) << cv->name << ": " << plan.error().message;
        auto r = compileOver(*plan, ql::toDescriptor(topic));
        ASSERT_TRUE(r.has_value()) << cv->name << ": " << r.error().message;
        compiled.emplace(cv->name, std::move(*r));
    }
    ASSERT_EQ(compiled.size(), 4u);

    const rv::CompiledViewPlan& identity = compiled.at("user_identity").plan;
    EXPECT_EQ(rv::keyModeOf(identity), rv::ViewKeyMode::Keyed);
    EXPECT_TRUE(rv::ownerMappedOf(identity));
    EXPECT_EQ(identity.keyWidth, 66);
    EXPECT_EQ(rv::prefixWidthOf(identity), 2);
    EXPECT_TRUE(identity.unique);

    const rv::CompiledViewPlan& history = compiled.at("device_history").plan;
    EXPECT_EQ(rv::keyModeOf(history), rv::ViewKeyMode::Keyed);
    EXPECT_FALSE(rv::ownerMappedOf(history));
    EXPECT_EQ(history.keyWidth, 72);
    EXPECT_EQ(rv::prefixWidthOf(history), 8);

    const rv::CompiledViewPlan& name = compiled.at("character_name").plan;
    EXPECT_EQ(rv::keyModeOf(name), rv::ViewKeyMode::Keyed);
    EXPECT_TRUE(rv::ownerMappedOf(name));
    EXPECT_EQ(name.keyWidth, 24);
    EXPECT_EQ(rv::prefixWidthOf(name), 0);
    EXPECT_EQ(name.unicodeVersion, 0x00100000u);

    const rv::CompiledViewPlan& stats = compiled.at("account_stats").plan;
    EXPECT_EQ(rv::keyModeOf(stats), rv::ViewKeyMode::LegacyU64);
    EXPECT_EQ(stats.keyWidth, 8);
    ASSERT_EQ(stats.constraints.size(), 1u);
    EXPECT_EQ(stats.constraints[0].ordinal, 1);
    const auto& columns = compiled.at("account_stats").outputDescriptor.fields;
    const auto characters = std::find_if(columns.begin(), columns.end(), [](const auto& f) { return f.name == "characters"; });
    ASSERT_NE(characters, columns.end());
    ASSERT_FALSE(stats.constraints[0].predicate.empty());
    EXPECT_EQ(stats.constraints[0].predicate[0].op, rv::PredicateOp::Column);
    EXPECT_EQ(stats.constraints[0].predicate[0].outSlot, 1);
    EXPECT_EQ(static_cast<std::size_t>(characters - columns.begin()), 1u);
}
