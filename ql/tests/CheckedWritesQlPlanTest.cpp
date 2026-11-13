// Checked Writes Part 08, Task 4: create view -> the authored ViewPlan v2, and the RegressorsPath golden.
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/codec/ViewPlanAuthored.h>
#include <gtest/gtest.h>

#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace ql = rune::ql;
namespace v = rune::view;
using rune::codec::PrimType;
using v::MergeRule;

namespace {

std::vector<ql::Statement> script(const std::string& src) {
    auto r = ql::parseScript(src);
    EXPECT_TRUE(r.has_value()) << src;
    return r ? std::move(*r) : std::vector<ql::Statement>{};
}

// Plans every create view of `src` against the topics defined earlier in the same script.
struct Planned { std::string viewName; std::expected<v::ViewPlan, ql::ParseError> plan; };
std::vector<Planned> planAll(const std::string& src) {
    std::map<std::string, ql::ViewSource> sources;
    std::vector<Planned> out;
    for (const auto& st : script(src)) {
        if (const auto* t = std::get_if<ql::TopicDef>(&st)) sources[t->name] = ql::viewSourceOf(*t);
        if (const auto* cv = std::get_if<ql::CreateViewDef>(&st)) {
            auto it = sources.find(cv->source);
            out.push_back({cv->name, it == sources.end() ? std::unexpected(ql::ParseError{0, 0, "no source"})
                                                         : ql::applyViewPlan(*cv, it->second)});
        }
    }
    return out;
}

const char* kCharacter =
    "topic character { characterId uint64, userId uint64, name string(16), gender uint16, hairSkin uint16, hairSheet uint16, "
    "headSkin uint16, createdAt int64, lastPlayed int64 } key characterId;";

std::expected<v::ViewPlan, ql::ParseError> planOne(const std::string& topic, const std::string& view) {
    auto all = planAll(topic + view);
    EXPECT_EQ(all.size(), 1u) << view;
    return all.empty() ? std::unexpected(ql::ParseError{0, 0, "none"}) : std::move(all[0].plan);
}

std::string errorOf(const std::string& topic, const std::string& view) {
    auto p = planOne(topic, view);
    EXPECT_FALSE(p.has_value()) << view;
    return p ? std::string() : p.error().message;
}

v::AuthoredField F(const char* name, PrimType t, const char* src, MergeRule r) { return {name, t, 0, src, r}; }

void expectOutput(const v::ViewPlan& p, const std::vector<v::AuthoredField>& want) {
    ASSERT_EQ(p.output.size(), want.size());
    for (std::size_t i = 0; i < want.size(); ++i) {
        EXPECT_EQ(p.output[i].name, want[i].name) << i;
        EXPECT_EQ(p.output[i].type, want[i].type) << i;
        EXPECT_EQ(p.output[i].sourceIdx, 0u) << i;
        EXPECT_EQ(p.output[i].srcField, want[i].srcField) << i;
        EXPECT_EQ(p.output[i].rule, want[i].rule) << i;
    }
}

void expectComponents(const std::vector<v::KeyComponent>& got, const std::vector<v::KeyComponent>& want) {
    ASSERT_EQ(got.size(), want.size());
    for (std::size_t i = 0; i < want.size(); ++i) EXPECT_EQ(got[i], want[i]) << i;
}

void expectRoundTrip(const v::ViewPlan& p) {
    const auto bytes = v::serializeAuthored(p);
    const auto back = v::deserializeAuthored(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(v::serializeAuthored(*back), bytes);
    EXPECT_EQ(back->key, p.key);
    EXPECT_EQ(back->unique, p.unique);
    expectComponents(back->keyComponents, p.keyComponents);
    EXPECT_EQ(back->constraints, p.constraints);
    EXPECT_EQ(back->unicodeVersion, p.unicodeVersion);
}

std::string readGolden(const char* name) {
    std::ifstream in(std::string(RUNE_QL_GOLDEN_DIR) + "/" + name);
    EXPECT_TRUE(in.good()) << name;
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

std::array<uint8_t, 16> bytes16(std::initializer_list<uint8_t> b) {
    std::array<uint8_t, 16> a{};
    std::size_t i = 0;
    for (auto x : b) a[i++] = x;
    return a;
}

} // namespace

TEST(CheckedWritesQlGolden, RegressorsPathParses) {
    const auto st = script(readGolden("regressorspath.rql"));
    ASSERT_EQ(st.size(), 7u);
    const std::vector<std::size_t> want = {0, 21, 0, 21, 0, 21, 21};
    for (std::size_t i = 0; i < st.size(); ++i) EXPECT_EQ(st[i].index(), want[i]) << i;
}

TEST(CheckedWritesQlGolden, RegressorsPathAuthorsPlanV2) {
    const auto all = planAll(readGolden("regressorspath.rql"));
    ASSERT_EQ(all.size(), 4u);
    for (const auto& pl : all) {
        ASSERT_TRUE(pl.plan.has_value()) << pl.viewName << ": " << pl.plan.error().message;
        expectRoundTrip(*pl.plan);
        EXPECT_EQ(v::serializeAuthored(*pl.plan)[0], 0x02) << pl.viewName;
    }
    {
        const auto& p = *all[0].plan;
        EXPECT_EQ(p.viewName, "user_identity");
        EXPECT_EQ(p.key, "");
        ASSERT_EQ(p.sources.size(), 1u);
        EXPECT_EQ(p.sources[0].topicId, -1);
        EXPECT_EQ(p.sources[0].keyField, "userId");
        expectOutput(p, {F("provider", PrimType::UInt16, "provider", MergeRule::Replace),
                         F("subject", PrimType::FixedStr, "subject", MergeRule::Replace),
                         F("userId", PrimType::UInt64, "userId", MergeRule::Replace)});
        expectComponents(p.keyComponents, {{"provider", 0, "provider"}, {"subject", 0, "subject"}});
        EXPECT_TRUE(p.unique);
        EXPECT_EQ(p.unicodeVersion, 0u);
        EXPECT_EQ(p.persistence, 0);
        EXPECT_EQ(v::keyDisplayName(p), "provider,subject");
    }
    {
        const auto& p = *all[1].plan;
        EXPECT_EQ(p.key, "");
        EXPECT_EQ(p.sources[0].keyField, "loginId");
        expectOutput(p, {F("userId", PrimType::UInt64, "userId", MergeRule::Replace),
                         F("deviceFp", PrimType::FixedStr, "deviceFp", MergeRule::Replace),
                         F("firstSeen", PrimType::Int64, "ts", MergeRule::Min),
                         F("lastSeen", PrimType::Int64, "ts", MergeRule::Max),
                         F("logins", PrimType::Int64, "", MergeRule::CountAll),
                         F("platform", PrimType::UInt16, "platform", MergeRule::Last)});
        const std::vector<v::KeyComponent> both = {{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}};
        expectComponents(p.keyComponents, both);
        EXPECT_FALSE(p.unique);
    }
    {
        const auto& p = *all[2].plan;
        EXPECT_EQ(p.key, "");
        EXPECT_EQ(p.sources[0].keyField, "characterId");
        expectOutput(p, {F("characterId", PrimType::UInt64, "characterId", MergeRule::Replace)});
        expectComponents(p.keyComponents, {{"name", 1, "nameKey"}});
        EXPECT_TRUE(p.unique);
        EXPECT_EQ(p.unicodeVersion, 0x00100000u);
        EXPECT_EQ(v::keyDisplayName(p), "nameKey");
    }
    {
        const auto& p = *all[3].plan;
        EXPECT_EQ(p.key, "userId");
        EXPECT_EQ(p.sources[0].keyField, "characterId");
        expectOutput(p, {F("userId", PrimType::UInt64, "userId", MergeRule::Replace),
                         F("characters", PrimType::Int64, "", MergeRule::CountAll)});
        EXPECT_TRUE(p.keyComponents.empty());
        ASSERT_EQ(p.constraints.size(), 1u);
        EXPECT_EQ(p.constraints[0].name, "max_characters");
        const std::vector<v::PredicateNode> want = {
            {v::PredicateOp::Column, 0, 0, "characters", PrimType{}, {}},
            {v::PredicateOp::Const, 0, 0, "", PrimType::Int64, bytes16({0x0C})},
            {v::PredicateOp::Le, 0, 1, "", PrimType{}, {}}};
        EXPECT_EQ(p.constraints[0].predicate, want);
        EXPECT_EQ(ql::predicateText(p.constraints[0].predicate), "characters <= 12");
    }
}

TEST(CheckedWritesQlPlan, SinglePlainSourceKeyIsTheV1Shape) {
    auto p = planOne(kCharacter, "create view character_by_id key characterId as select * from character;");
    ASSERT_TRUE(p.has_value()) << p.error().message;
    EXPECT_EQ(p->key, "characterId");
    EXPECT_TRUE(p->keyComponents.empty());
    ASSERT_EQ(p->sources.size(), 1u);
    EXPECT_EQ(p->sources[0].topicId, -1);
    EXPECT_EQ(p->sources[0].keyField, "characterId");
    expectOutput(*p, {F("characterId", PrimType::UInt64, "characterId", MergeRule::Replace),
                      F("userId", PrimType::UInt64, "userId", MergeRule::Replace),
                      F("name", PrimType::FixedStr, "name", MergeRule::Replace),
                      F("gender", PrimType::UInt16, "gender", MergeRule::Replace),
                      F("hairSkin", PrimType::UInt16, "hairSkin", MergeRule::Replace),
                      F("hairSheet", PrimType::UInt16, "hairSheet", MergeRule::Replace),
                      F("headSkin", PrimType::UInt16, "headSkin", MergeRule::Replace),
                      F("createdAt", PrimType::Int64, "createdAt", MergeRule::Replace),
                      F("lastPlayed", PrimType::Int64, "lastPlayed", MergeRule::Replace)});
    // The same plan authored field by field, as docs/api/client-cpp.md ("Materialized views") does.
    v::ViewPlan doc;
    doc.viewName = "character_by_id";
    doc.key = "characterId";
    doc.persistence = 0;
    doc.sources.push_back(v::SourceSpec{-1, "characterId"});
    const struct { const char* n; PrimType t; } cols[] = {
        {"characterId", PrimType::UInt64}, {"userId", PrimType::UInt64}, {"name", PrimType::FixedStr},
        {"gender", PrimType::UInt16}, {"hairSkin", PrimType::UInt16}, {"hairSheet", PrimType::UInt16},
        {"headSkin", PrimType::UInt16}, {"createdAt", PrimType::Int64}, {"lastPlayed", PrimType::Int64}};
    for (const auto& c : cols) doc.output.push_back(v::AuthoredField{c.n, c.t, 0, c.n, MergeRule::Replace});
    const auto bytes = v::serializeAuthored(*p);
    EXPECT_EQ(bytes[0], 0x01);
    EXPECT_EQ(bytes, v::serializeAuthored(doc));
}

TEST(CheckedWritesQlPlan, CompositeProjection) {
    auto p = planOne(kCharacter, "create view account_characters key (userId, characterId) as select name, gender, lastPlayed from character;");
    ASSERT_TRUE(p.has_value()) << p.error().message;
    expectOutput(*p, {F("userId", PrimType::UInt64, "userId", MergeRule::Replace),
                      F("characterId", PrimType::UInt64, "characterId", MergeRule::Replace),
                      F("name", PrimType::FixedStr, "name", MergeRule::Replace),
                      F("gender", PrimType::UInt16, "gender", MergeRule::Replace),
                      F("lastPlayed", PrimType::Int64, "lastPlayed", MergeRule::Replace)});
    EXPECT_EQ(p->sources[0].keyField, "characterId");
    EXPECT_EQ(p->key, "");
}

TEST(CheckedWritesQlPlan, AggregateTyping) {
    const std::string t = "topic t { id uint64, i int32, u uint16, f float64, s string(8) } key id;";
    auto p = planOne(t, "create view a key id as select sum(i) as si, sum(u) as su, avg(i) as ai, count(s) as cs, min(f) as mf, last(s) as ls from t;");
    ASSERT_TRUE(p.has_value()) << p.error().message;
    expectOutput(*p, {F("id", PrimType::UInt64, "id", MergeRule::Replace),
                      F("si", PrimType::Int64, "i", MergeRule::Sum),
                      F("su", PrimType::UInt64, "u", MergeRule::Sum),
                      F("ai", PrimType::Float64, "i", MergeRule::Avg),
                      F("cs", PrimType::Int64, "s", MergeRule::Count),
                      F("mf", PrimType::Float64, "f", MergeRule::Min),
                      F("ls", PrimType::FixedStr, "s", MergeRule::Last)});
    EXPECT_EQ(errorOf(t, "create view b key id as select sum(s) as x from t;"),
              "sum() cannot aggregate 's' of type string(8)");
}

TEST(CheckedWritesQlPlan, ShapeErrors) {
    const std::string t = "topic t { id uint64, a int32, s string(8) } key id;";
    EXPECT_EQ(errorOf(t, "create view v key id as select nope from t;"), "column 'nope' is not a field of 't'");
    EXPECT_EQ(errorOf(t, "create view v key nope as select a from t;"), "column 'nope' is not a field of 't'");
    {   // The parser never mixes '*' with an aggregate; the planner still refuses a hand-built AST that does.
        auto all = script(t + "create view v key id as select * from t;");
        auto* cv = std::get_if<ql::CreateViewDef>(&all.back());
        ASSERT_NE(cv, nullptr);
        cv->items.push_back({ql::ItemKind::CountAll, "", "n"});
        auto p = ql::applyViewPlan(*cv, ql::viewSourceOf(std::get<ql::TopicDef>(all[0])));
        ASSERT_FALSE(p.has_value());
        EXPECT_EQ(p.error().message, "'*' is not valid in an aggregate view");
    }
    EXPECT_EQ(errorOf("topic u;", "create view v key id as select a from u;"), "view source must be a typed topic");
}

TEST(CheckedWritesQlPlanRegression, AggregateIsGroupedByItsKeyRegression) {
    namespace v = rune::view;
    const std::string t = "topic d { id uint64, userId uint64, deviceFp string(64), ts int64 } key id;";
    // A plain key: v1, the key column rides as the source key field, no group by anywhere.
    auto s = planOne(t, "create view s key userId as select count(*) as n from d;");
    ASSERT_TRUE(s.has_value()) << s.error().message;
    EXPECT_EQ(s->key, "userId");
    EXPECT_EQ(s->sources[0].keyField, "userId");
    EXPECT_FALSE(v::needsV2(*s));
    // A composite key: v2, the source key is the topic key.
    auto p = planOne(t, "create view d2 key (userId, deviceFp) as select count(*) as c from d;");
    ASSERT_TRUE(p.has_value()) << p.error().message;
    EXPECT_TRUE(v::needsV2(*p));
    EXPECT_EQ(p->sources[0].keyField, "id");
    expectComponents(p->keyComponents, {{"userId", 0, "userId"}, {"deviceFp", 0, "deviceFp"}});
    // A lower() key is the group, too.
    auto q = planOne(t, "create view w key lower(deviceFp) as fp as select count(*) as n from d;");
    ASSERT_TRUE(q.has_value()) << q.error().message;
    expectComponents(q->keyComponents, {{"deviceFp", 1, "fp"}});
    EXPECT_EQ(q->unicodeVersion, 0x00100000u);
    expectOutput(*q, {F("n", PrimType::Int64, "", MergeRule::CountAll)});
    EXPECT_EQ(q->sources[0].keyField, "id");
    // `group by` is refused at parse time (a script that parses nothing).
    EXPECT_FALSE(ql::parseScript("topic t { id uint64 } key id; create view v key id as select count(*) as n from t group by id;").has_value());
}

TEST(CheckedWritesQlPlan, TransientAndReplace) {
    auto p = planOne(kCharacter, "create or replace transient view c key characterId as select name from character;");
    ASSERT_TRUE(p.has_value()) << p.error().message;
    EXPECT_EQ(p->persistence, 1);
}

TEST(CheckedWritesQlPlanRegression, CheckConstantEdgesRegression) {
    const std::string t = "topic c { id uint64, u uint64, s string(16), w string(64), b bool, f float64 } key id;";
    auto check = [&](const char* col, const char* pred) {
        return planOne(t, std::string("create view v key id as select count(*) as characters, ") + col + " from c constraint max_characters check (" + pred + ");");
    };
    auto neg = planOne(t, "create view v key id as select count(*) as characters from c constraint max_characters check (characters <= -1);");
    ASSERT_TRUE(neg.has_value()) << neg.error().message;
    const auto& k = neg->constraints[0].predicate[1];
    EXPECT_EQ(k.constType, PrimType::Int64);
    EXPECT_EQ(k.constBytes, bytes16({0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}));
    EXPECT_EQ(errorOf(t, "create view v key id as select count(*) as characters from c constraint max_characters check (characters <= 9223372036854775808);"),
              "constraint 'max_characters': 9223372036854775808 does not fit int64 column 'characters'");
    EXPECT_EQ(errorOf(t, "create view v key id as select max(u) as m from c constraint k check (m >= -1);"),
              "constraint 'k': -1 does not fit uint64 column 'm'");
    auto str = check("last(s) as ls", "ls = 'abc'");
    ASSERT_TRUE(str.has_value()) << str.error().message;
    EXPECT_EQ(str->constraints[0].predicate[1].constBytes, bytes16({0x61, 0x62, 0x63}));
    EXPECT_EQ(ql::predicateText(str->constraints[0].predicate), "ls = 'abc'");
    EXPECT_EQ(errorOf(t, "create view v key id as select last(w) as lw from c constraint k check (lw = 'abc');"),
              "constraint 'k': check compares fixed-width columns of at most 16 bytes; 'lw' is string(64)");
    EXPECT_EQ(errorOf(t, "create view v key id as select max(u) as m from c constraint k check (m = true);"),
              "constraint 'k': true does not fit uint64 column 'm'");
    EXPECT_EQ(errorOf(t, "create view v key id as select count(*) as n from c constraint k check (nope = 1);"),
              "constraint 'k': column 'nope' is not a column of the view");
    auto b = check("last(b) as lb", "lb = true");
    ASSERT_TRUE(b.has_value()) << b.error().message;
    EXPECT_EQ(b->constraints[0].predicate[1].constBytes, bytes16({1}));
    EXPECT_EQ(ql::predicateText(b->constraints[0].predicate), "lb = true");
}

TEST(CheckedWritesQlPlanRegression, NegativeZeroFloatConstantIsPositiveZeroRegression) {
    const std::string t = "topic c { id uint64, f float64 } key id;";
    auto p = planOne(t, "create view v key id as select last(f) as lf from c constraint k check (lf > -0);");
    ASSERT_TRUE(p.has_value()) << p.error().message;
    EXPECT_EQ(p->constraints[0].predicate[1].constBytes, bytes16({}));
    EXPECT_EQ(ql::predicateText(p->constraints[0].predicate), "lf > 0");
}

TEST(CheckedWritesQlPlan, PredicateTextShapes) {
    const std::string t = "topic c { id uint64, u uint64 } key id;";
    auto p = planOne(t, "create view v key id as select count(*) as n, max(u) as m from c "
                        "constraint k check (not (n < 3) and (m = 1 or m <> 2) or n >= 7);");
    ASSERT_TRUE(p.has_value()) << p.error().message;
    EXPECT_EQ(ql::predicateText(p->constraints[0].predicate), "(not (n < 3) and (m = 1 or m <> 2)) or n >= 7");
}

TEST(CheckedWritesQlPlanRegression, V2AggregateNamesTheSourceKeyRegression) {
    const std::string t = "topic d { id uint64, userId uint64, ts int64 } key id;";
    auto v1 = planOne(t, "create view a key userId as select count(*) as n from d;");
    ASSERT_TRUE(v1.has_value()) << v1.error().message;
    EXPECT_EQ(v1->sources[0].keyField, "userId");
    auto v2 = planOne(t, "create view b key userId as select count(*) as n from d constraint c check (n <= 1);");
    ASSERT_TRUE(v2.has_value()) << v2.error().message;
    EXPECT_EQ(v2->sources[0].keyField, "id");
}

TEST(CheckedWritesQlPlanRegression, PredicateTextOfASelfReferencingPlanIsInvalidRegression) {
    namespace v = rune::view;
    const std::vector<v::PredicateNode> selfRef = {
        {v::PredicateOp::Column, 0, 0, "n", rune::codec::PrimType{}, {}},
        {v::PredicateOp::And, 1, 0, "", rune::codec::PrimType{}, {}}};
    EXPECT_EQ(ql::predicateText(selfRef), "<invalid predicate>");
    const std::vector<v::PredicateNode> forward = {
        {v::PredicateOp::Not, 5, 0, "", rune::codec::PrimType{}, {}}};
    EXPECT_EQ(ql::predicateText(forward), "<invalid predicate>");
}

TEST(CheckedWritesQlPlanRegression, PredicateTextOfALongAndChainRendersRegression) {
    namespace v = rune::view;
    constexpr uint16_t kTerms = 30000;
    std::vector<v::PredicateNode> nodes = {{v::PredicateOp::Column, 0, 0, "n", rune::codec::PrimType{}, {}}};
    uint16_t chain = 0;
    std::string want(kTerms - 2, '(');
    want += 'n';
    for (uint16_t t = 1; t < kTerms; ++t) {
        nodes.push_back({v::PredicateOp::Column, 0, 0, "n", rune::codec::PrimType{}, {}});
        nodes.push_back({v::PredicateOp::And, chain, static_cast<uint16_t>(nodes.size() - 1), "", rune::codec::PrimType{}, {}});
        chain = static_cast<uint16_t>(nodes.size() - 1);
        want += " and n";
        if (t != kTerms - 1) want += ')';
    }
    EXPECT_EQ(ql::predicateText(nodes), want);
}

// A lowered literal (select-list `lower('x')`; the grammar allows it nowhere on the check path, so the planner's
// "does not fit" error cannot carry one) must read back as the user wrote it.
TEST(CheckedWritesQlPlanRegression, LoweredLiteralIsPrintedAsWrittenRegression) {
    ql::Literal i;
    i.kind = ql::Literal::Kind::Integer;
    i.magnitude = 7;
    EXPECT_EQ(ql::literalText(i), "7");
    i.negative = true;
    EXPECT_EQ(ql::literalText(i), "-7");
    i.magnitude = 0;
    EXPECT_EQ(ql::literalText(i), "0");
    ql::Literal b;
    b.kind = ql::Literal::Kind::Bool;
    b.boolean = true;
    EXPECT_EQ(ql::literalText(b), "true");
    ql::Literal s;
    s.kind = ql::Literal::Kind::String;
    s.text = "ABC";
    EXPECT_EQ(ql::literalText(s), "'ABC'");
    s.lowered = true;
    EXPECT_EQ(ql::literalText(s), "lower('ABC')");
}
