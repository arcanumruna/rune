// tests/CheckedWritesQlCliTest.cpp
//
// Checked writes Part 08, Task 9 - the CLI half of QL, OFFLINE: `create view` plans in --dry-run, the `select` planner
// (QM3 and the lower() cell of spec section 15), the row and stats formatting, and the new `show` lines.
#include "QlEngine.h"

#include "rune/cli/Cli.h"
#include "rune/cli/CatalogCommands.h"

#include <rune/catalog/CatalogFingerprint.h>
#include <rune/catalog/CatalogState.h>
#include <rune/client/view/ViewKeyBuilder.h>
#include <rune/client/view/ViewRowCodec.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/UnicodeLower.h>
#include <rune/codec/ViewKeyEncoding.h>
#include <rune/ql/Backends.h>
#include <rune/ql/Parser.h>

#include <gtest/gtest.h>

#include <cstring>
#include <expected>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

namespace fs  = std::filesystem;
namespace ql  = rune::ql;
namespace eng = rune::cli::engine;
namespace cat = rune::catalog;
using rune::codec::PrimType;

namespace {

std::string readFile(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    std::stringstream s;
    s << in.rdbuf();
    return s.str();
}

std::string writeTemp(const std::string& name, const std::string& content) {
    const fs::path p = fs::temp_directory_path() / ("rune_cw_ql_cli_" + std::to_string(::getpid()) + "_" + name);
    std::ofstream(p, std::ios::binary) << content;
    return p.string();
}

int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    std::stringstream o, e;
    const int rc = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return rc;
}

template <class T>
T parseAs(const std::string& text) {
    auto r = ql::parse(text);
    EXPECT_TRUE(r.has_value()) << text << (r ? "" : " -> " + r.error().message);
    const T* t = r ? std::get_if<T>(&*r) : nullptr;
    EXPECT_NE(t, nullptr) << text;
    return t ? *t : T{};
}

const char* kCharacter = "topic character { characterId uint64, userId uint64, name string(16), gender uint16 } partitions 1 key characterId;";

ql::TopicDef topicOf(const std::string& text) { return parseAs<ql::TopicDef>(text); }

rune::view::ViewPlan planOf(const std::string& viewText, const ql::TopicDef& topic) {
    auto plan = ql::applyViewPlan(parseAs<ql::CreateViewDef>(viewText), ql::viewSourceOf(topic));
    EXPECT_TRUE(plan.has_value()) << viewText << (plan ? "" : " -> " + plan.error().message);
    return plan ? *plan : rune::view::ViewPlan{};
}

// The view's output descriptor: the plan's output fields, FixedStr widths from the source.
rune::codec::Descriptor outputOf(const rune::view::ViewPlan& plan, const rune::codec::Descriptor& source) {
    rune::codec::DescriptorBuilder b("out");
    for (const auto& f : plan.output) {
        const auto* src = source.find(f.srcField);
        if (f.type == PrimType::FixedStr) b.addFixedStr(f.name, src ? src->fixedLen : 1);
        else b.add(f.name, f.type);
    }
    return b.build();
}

std::vector<uint8_t> be(uint64_t v, std::size_t n) {
    std::vector<uint8_t> out(n);
    for (std::size_t i = n; i-- > 0; v >>= 8) out[i] = static_cast<uint8_t>(v);
    return out;
}

std::vector<uint8_t> padded(std::string_view s, std::size_t n) {
    std::vector<uint8_t> out(n, 0);
    std::memcpy(out.data(), s.data(), s.size());
    return out;
}

std::vector<uint8_t> cat2(std::vector<uint8_t> a, const std::vector<uint8_t>& b) {
    a.insert(a.end(), b.begin(), b.end());
    return a;
}

std::expected<eng::SelectRead, std::string> planFor(const std::string& viewText, const std::string& selectText,
                                                    const char* topicText = kCharacter, int32_t partitions = 1) {
    const ql::TopicDef topic = topicOf(topicText);
    const auto src           = ql::viewSourceOf(topic);
    const auto plan          = planOf(viewText, topic);
    return eng::planSelect(parseAs<ql::SelectDef>(selectText), plan, src.descriptor, outputOf(plan, src.descriptor), partitions, {});
}

}  // namespace

// ---- create view in --dry-run -------------------------------------------------------------------------------------

TEST(CheckedWritesQlCli, RegressorsPathDryRunGolden) {
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "--dry-run", "-f", std::string(RUNE_QL_GOLDEN_DIR) + "/regressorspath.rql"}, &out, &err), 0) << err;
    EXPECT_EQ(out, readFile(fs::path(RUNE_CLI_GOLDEN_DIR) / "regressorspath.dryrun.txt"));
}

TEST(CheckedWritesQlCli, BadViewRefusesTheScriptBeforeAnythingIsSent) {
    const std::string script = writeTemp("bad.rql", "topic s { id uint64, n int64 } key id; create view v key id as select nope from s;");
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "-f", script, "--broker", "127.0.0.1:1"}, &out, &err), 1);
    EXPECT_EQ(err, "statement 2: column 'nope' is not a field of 's'\n");
    EXPECT_TRUE(out.empty());
}

TEST(CheckedWritesQlCli, UnresolvedSourcePrintsRunTime) {
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "--dry-run", "-e", "create view v key id as select n from elsewhere;"}, &out, &err), 0) << err;
    EXPECT_EQ(out, "create view v source elsewhere key id\n  (source resolved when the statement runs)\n");
}

TEST(CheckedWritesQlCli, DefinePlanNamesNonDefaultKeyFeatures) {
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "--dry-run", "-e", "topic t { id uint64 autoincrement } key id with (key.index = on, autoincrement.reserve = 100, autoincrement.max = 1000);"}, &out, &err), 0) << err;
    EXPECT_NE(out.find("key id autoincrement key.index autoincrement.reserve 100 autoincrement.max 1000\n"), std::string::npos) << out;
}

// ---- select planning (QM3) ----------------------------------------------------------------------------------------

namespace {

struct SelectCell {
    const char* view;
    const char* where;
    const char* error;                // "" = a read
    bool scan;
    std::vector<uint8_t> bytes;       // the key or prefix
};

void PrintTo(const SelectCell& c, std::ostream* os) { *os << c.where; }   // the same text on every run: ctest names its cases after it

const char* kV1 = "create view v1 key characterId as select userId from character;";
const char* kV2 = "create view v2 key (userId, characterId) as select name from character;";
const char* kV3 = "create view v3 key (lower(name) as nameKey, characterId) as select userId from character;";
const char* kV4 = "create view v4 key lower(name) as nameKey as select userId from character;";

const char* kShape2 = "where must name every key column (userId, characterId) for a point read, or the leading (userId) for a scan";
const char* kShape3 = "where must name every key column (nameKey, characterId) for a point read, or the leading (nameKey) for a scan";

std::vector<SelectCell> selectCells() {
    const auto foo = padded("foo", 24);
    return {
        {kV2, "userId = 7 and characterId = 9", "", false, cat2(be(7, 8), be(9, 8))},
        {kV2, "userId = 7", "", true, be(7, 8)},
        {kV2, "characterId = 9", kShape2, false, {}},
        {kV2, "gender = 1", kShape2, false, {}},
        {kV2, "userId = 7 and characterId = 9 and gender = 1", kShape2, false, {}},
        {kV2, "characterId = 9 and characterId = 10", "duplicate where column 'characterId'", false, {}},
        {kV3, "nameKey = 'Foo' and characterId = 9", "", false, cat2(foo, be(9, 8))},
        {kV3, "nameKey = 'Foo'", "", true, foo},
        {kV3, "characterId = 9", kShape3, false, {}},
        {kV3, "gender = 1", kShape3, false, {}},
        {kV3, "nameKey = 'Foo' and characterId = 9 and gender = 1", kShape3, false, {}},
        {kV3, "characterId = 9 and characterId = 10", "duplicate where column 'characterId'", false, {}},
        {kV1, "characterId = 9", "", false, be(9, 8)},
        {kV1, "userId = 7", "where must name the key column (characterId)", false, {}},
        {kV1, "characterId = 9 and userId = 7", "where must name the key column (characterId)", false, {}},
        {kV1, "characterId = 9 and characterId = 10", "duplicate where column 'characterId'", false, {}},
        {kV4, "nameKey = 'Foo'", "", false, foo},
        {kV4, "gender = 1", "where must name the key column (nameKey)", false, {}},
        {kV4, "nameKey = 'Foo' and gender = 1", "where must name the key column (nameKey)", false, {}},
        {kV4, "nameKey = 'a' and nameKey = 'b'", "duplicate where column 'nameKey'", false, {}},
    };
}

}  // namespace

class Cell : public testing::TestWithParam<SelectCell> {};

TEST_P(Cell, PlansTheRead) {
    const SelectCell& c = GetParam();
    const std::string text = std::string("select userId from v where ") + c.where + ";";
    if (const auto parsed = ql::parse(text); !parsed) {   // the parser owns the duplicate-column refusal
        EXPECT_EQ(parsed.error().message, c.error);
        return;
    }
    const auto r = planFor(c.view, text);
    if (*c.error != '\0') {
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), c.error);
        return;
    }
    ASSERT_TRUE(r.has_value()) << r.error();
    EXPECT_EQ(r->kind, c.scan ? eng::SelectRead::Kind::Scan : eng::SelectRead::Kind::Point);
    EXPECT_EQ(r->keyOrPrefix, c.bytes);
    EXPECT_EQ(r->partition, -1);
}

INSTANTIATE_TEST_SUITE_P(CheckedWritesQlSelectMatrix, Cell, testing::ValuesIn(selectCells()),
                         [](const testing::TestParamInfo<SelectCell>& info) { return std::to_string(info.index); });

TEST(CheckedWritesQlSelectMatrixCount, CellCountIsPinned) { EXPECT_EQ(selectCells().size(), 20u); }

TEST(CheckedWritesQlSelect, LowerLiteralEqualsTheBrokerKey) {
    const char* nameView = "create unique view character_name key lower(name) as nameKey as select characterId from character;";
    const auto foo       = padded("foo", 24);

    const auto point = planFor(nameView, "select characterId from character_name where nameKey = lower('Foo');");
    ASSERT_TRUE(point.has_value()) << point.error();
    EXPECT_EQ(point->kind, eng::SelectRead::Kind::Point);
    EXPECT_EQ(point->keyOrPrefix, foo);

    std::vector<uint8_t> broker(24);
    rune::codec::FieldValue fv;
    fv.type    = PrimType::FixedStr;
    fv.present = true;
    fv.bytes   = "Foo";
    ASSERT_TRUE(rune::codec::encodeKeyComponent(PrimType::FixedStr, rune::codec::kKeyFunctionLower, fv, broker));
    EXPECT_EQ(point->keyOrPrefix, broker);

    const ql::TopicDef topic = topicOf(kCharacter);
    const auto src           = ql::viewSourceOf(topic);
    auto builder = rune::client::view::ViewKeyBuilder::fromPlan(planOf(nameView, topic), src.descriptor);
    ASSERT_TRUE(builder.has_value()) << builder.error();
    const auto built = builder->setString("name", "Foo").key();
    ASSERT_TRUE(built.has_value()) << built.error();
    EXPECT_EQ(point->keyOrPrefix, *built);

    const auto upper = planFor(nameView, "select characterId from character_name where nameKey = 'FOO';");
    ASSERT_TRUE(upper.has_value()) << upper.error();
    EXPECT_EQ(upper->keyOrPrefix, foo);

    const auto scan = planFor(kV3, "select userId from v3 where nameKey = lower('Foo');");
    ASSERT_TRUE(scan.has_value()) << scan.error();
    EXPECT_EQ(scan->kind, eng::SelectRead::Kind::Scan);
    EXPECT_EQ(scan->keyOrPrefix, foo);

    const auto plain = planFor("create view tags key tag as select n from t;", "select n from tags where tag = lower('ABC');",
                               "topic t { tag string(16), n int64 } key tag;");
    ASSERT_TRUE(plain.has_value()) << plain.error();
    EXPECT_EQ(plain->keyOrPrefix, padded("abc", 16));
}

TEST(CheckedWritesQlSelect, ShorterPrefixNeedsAnOrderedIndex) {
    const char* identity = "topic ident { provider uint16, subject string(64), userId uint64 } key userId;";
    const char* view     = "create view v key (provider, subject, userId) as select userId from ident;";
    const auto one = planFor(view, "select userId from v where provider = 1;", identity);
    ASSERT_FALSE(one.has_value());
    EXPECT_EQ(one.error(), "where names only the leading (provider); a read by a shorter prefix needs an ordered index");
    const auto two = planFor(view, "select userId from v where provider = 1 and subject = 'x';", identity);
    ASSERT_TRUE(two.has_value()) << two.error();
    EXPECT_EQ(two->kind, eng::SelectRead::Kind::Scan);
}

TEST(CheckedWritesQlSelectRegression, LiteralEdgesAreRefusedNeverTruncatedRegression) {
    const char* topic = "topic e { provider uint16, big uint64, signedId int64, name string(16), tag string(16), flag bool } key big;";
    auto uintView = [&](const char* col) { return std::string("create view v key ") + col + " as select flag from e;"; };
    auto attempt  = [&](const char* col, const std::string& lit) {
        return planFor(uintView(col), std::string("select flag from v where ") + col + " = " + lit + ";", topic);
    };

    for (const char* lit : {"-1", "70000"}) {
        const auto r = attempt("provider", lit);
        ASSERT_FALSE(r.has_value()) << lit;
        EXPECT_EQ(r.error(), std::string(lit) + " does not fit uint16 column 'provider'");
    }
    EXPECT_TRUE(attempt("provider", "65535").has_value());

    const auto maxU = attempt("big", "18446744073709551615");
    ASSERT_TRUE(maxU.has_value()) << maxU.error();
    EXPECT_EQ(maxU->keyOrPrefix, be(~uint64_t{0}, 8));
    ASSERT_FALSE(attempt("big", "-1").has_value());

    const auto minI = attempt("signedId", "-9223372036854775808");
    ASSERT_TRUE(minI.has_value()) << minI.error();
    EXPECT_EQ(minI->keyOrPrefix, be(0, 8));
    ASSERT_FALSE(attempt("signedId", "9223372036854775808").has_value());

    const auto longName = attempt("name", "'12345678901234567'");
    ASSERT_FALSE(longName.has_value());
    EXPECT_EQ(longName.error(), "'12345678901234567' is 17 bytes; column 'name' is string(16)");

    const auto lowered = attempt("tag", "lower('\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA')");
    ASSERT_FALSE(lowered.has_value());
    EXPECT_EQ(lowered.error(), "lower('\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA\xC8\xBA') is 24 bytes; column 'tag' is string(16)");

    const auto flagOnInt = attempt("big", "true");
    ASSERT_FALSE(flagOnInt.has_value());
    EXPECT_EQ(flagOnInt.error(), "true does not fit uint64 column 'big'");
}

TEST(CheckedWritesQlSelect, LimitIsForScansAndColumnsComeFromTheOutput) {
    const auto point = planFor(kV1, "select userId from v1 where characterId = 9 limit 5;");
    ASSERT_FALSE(point.has_value());
    EXPECT_EQ(point.error(), "limit is only valid on a scan");
    const auto scan = planFor(kV2, "select name, userId from v2 where userId = 7 limit 5;");
    ASSERT_TRUE(scan.has_value()) << scan.error();
    EXPECT_EQ(scan->limit, 5u);
    EXPECT_EQ(scan->slots, (std::vector<uint16_t>{2, 0}));   // output: userId, characterId, name
    const auto bad = planFor(kV2, "select nope from v2 where userId = 7;");
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), "column 'nope' is not a column of view 'v2'");
}

// ---- row and stats formatting -------------------------------------------------------------------------------------

TEST(CheckedWritesQlCli, FormatRows) {
    rune::codec::DescriptorBuilder b("out");
    b.add("userId", PrimType::UInt64).addFixedStr("name", 16).add("score", PrimType::Float64).add("alive", PrimType::Bool);
    const auto output = b.build();
    const std::vector<PrimType> types{PrimType::UInt64, PrimType::FixedStr, PrimType::Float64, PrimType::Bool};
    const std::vector<uint16_t> widths{8, 16, 8, 1};
    const auto layout = rune::client::view::buildRowLayout(types, widths);

    auto row = [&](uint64_t id, std::string_view name, double score, bool nullScore, bool alive) {
        std::vector<uint8_t> r(layout.rowLen, 0);
        if (nullScore) r[0] |= 1u << 2;
        std::memcpy(r.data() + layout.slotOffset[0], &id, 8);
        std::memcpy(r.data() + layout.slotOffset[1], name.data(), name.size());
        std::memcpy(r.data() + layout.slotOffset[2], &score, 8);
        r[layout.slotOffset[3]] = alive ? 1 : 0;
        return r;
    };
    const std::vector<std::vector<uint8_t>> rows{row(42, "Foo", 1.5, false, true), row(43, "Bar", 0, true, false)};
    EXPECT_EQ(eng::formatRows(output, std::vector<uint16_t>{0, 1, 2, 3}, rows),
              "userId  name  score  alive\n42      Foo   1.5    true\n43      Bar   NULL   false\n(2 rows)\n");
    EXPECT_EQ(eng::formatRows(output, std::vector<uint16_t>{1}, rows), "name\nFoo\nBar\n(2 rows)\n");
}

TEST(CheckedWritesQlCli, StatsJson) {
    using rune::protocol::client::StatId;
    rune::cli::PartitionStats p0, p1;
    p0.partition = 0;
    p1.partition = 1;
    for (uint16_t id = 1; id <= 7; ++id) {
        p0.result.stats.emplace_back(id, id);
        p1.result.stats.emplace_back(id, 10 * id);
    }
    p1.result.constraints.push_back({(5u << 16) | 1u, 3});
    p1.constraintLabels = {"account_stats.max_characters"};
    const std::vector<rune::cli::PartitionStats> parts{p0, p1};

    const std::string stats0 = R"({"rejectionsKeyExists": 1, "rejectionsKeyNotFound": 2, "rejectionsConstraint": 3, "verdictLag": 4, )"
                               R"("verdictWatermark": 5, "verdictMarkersWritten": 6, "rejectedNotCompacted": 7})";
    const std::string stats1 = R"({"rejectionsKeyExists": 10, "rejectionsKeyNotFound": 20, "rejectionsConstraint": 30, "verdictLag": 40, )"
                               R"("verdictWatermark": 50, "verdictMarkersWritten": 60, "rejectedNotCompacted": 70})";
    EXPECT_EQ(rune::cli::formatStatsJson("topic", "character", parts),
              R"({"topic": "character", "partitions": [{"partition": 0, "stats": )" + stats0 + R"(, "constraints": []}, )" +
              R"({"partition": 1, "stats": )" + stats1 +
              R"(, "constraints": [{"constraintId": 327681, "label": "account_stats.max_characters", "rejections": 3}]}]})");
    EXPECT_EQ(rune::cli::formatStatsJson("view", "v", std::vector<rune::cli::PartitionStats>{p0}).substr(0, 19), R"({"view": "v", "part)");

    const std::string text = rune::cli::formatStatsText(parts);
    EXPECT_NE(text.find("  partition 0\n    rejectionsKeyExists 1\n    rejectionsKeyNotFound 2\n"), std::string::npos) << text;
    EXPECT_NE(text.find("    constraint account_stats.max_characters 3\n"), std::string::npos) << text;
    EXPECT_EQ(rune::cli::formatStatsText(std::vector<rune::cli::PartitionStats>{rune::cli::PartitionStats{2, {}, {}}}),
              "  partition 2\n    (no counters)\n");
}

// ---- show topic / show view ---------------------------------------------------------------------------------------

TEST(CheckedWritesQlCli, ShowTopicFormatsFeatures) {
    cat::TopicObject t;
    t.name = "t";
    t.schema.emplace();
    t.schema->keyFeatures = {true, true, rune::protocol::Partitioner::Modulo, (uint64_t{1} << 53) - 1, 4096};
    EXPECT_NE(rune::cli::formatTopicDefinition(t).find("  key.index on\n  autoincrement on (reserve 4096, max 9007199254740991)\n  partitioner modulo\n"),
              std::string::npos);

    cat::TopicObject plain;
    plain.name = "p";
    plain.schema.emplace();
    const std::string text = rune::cli::formatTopicDefinition(plain);
    EXPECT_NE(text.find("  key.index off\n  autoincrement off\n  partitioner fnv1a\n"), std::string::npos) << text;

    cat::TopicObject untyped;
    untyped.name = "u";
    const std::string raw = rune::cli::formatTopicDefinition(untyped);
    for (const char* s : {"key.index", "autoincrement", "partitioner"}) EXPECT_EQ(raw.find(s), std::string::npos) << s;
}

namespace {

cat::ViewObject viewObjectOf(const std::string& viewText) {
    const ql::TopicDef topic = topicOf(kCharacter);
    const auto src           = ql::viewSourceOf(topic);
    const auto plan          = planOf(viewText, topic);
    cat::ViewObject v;
    v.name                       = plan.viewName;
    v.def.sourceNames            = {"character"};
    v.def.keyField               = plan.key;
    v.def.normalizedPlanBlob     = rune::view::serializeAuthored(plan);
    v.def.outputDescriptorBlob   = rune::codec::serialize(outputOf(plan, src.descriptor));
    return v;
}

}  // namespace

TEST(CheckedWritesQlCli, ShowViewFormatsConstraints) {
    const std::string stats = rune::cli::formatViewDefinition(viewObjectOf(
        "create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= 12);"));
    const std::string tail = "  keyspec userId\n  unique no\n  group by (userId)\n  constraint max_characters check (characters <= 12)\n";
    ASSERT_GE(stats.size(), tail.size());
    EXPECT_EQ(stats.substr(stats.size() - tail.size()), tail) << stats;

    const std::string v1 = rune::cli::formatViewDefinition(viewObjectOf(
        "create view by_user key userId as select count(*) as characters from character group by userId;"));
    EXPECT_NE(v1.find("  keyspec userId\n  unique no\n  group by (userId)\n"), std::string::npos) << v1;

    const std::string name = rune::cli::formatViewDefinition(viewObjectOf(kV4));
    EXPECT_NE(name.find("  keyspec lower(name) as nameKey\n  unique no\n  unicode 16.0.0\n"), std::string::npos) << name;
    const std::string wide = rune::cli::formatViewDefinition(viewObjectOf(
        "create unique view u key (userId, name) as select characterId from character;"));
    EXPECT_NE(wide.find("  keyspec (userId, name)\n  unique yes\n"), std::string::npos) << wide;
}

// ---- fix round 1 --------------------------------------------------------------------------------------------------

TEST(CheckedWritesQlCliRegression, StatsFormattersTolerateAMissingConstraintLabelRegression) {
    rune::cli::PartitionStats p;
    p.result.constraints.push_back({(5u << 16) | 1u, 3});
    const std::vector<rune::cli::PartitionStats> parts{p};
    EXPECT_NE(rune::cli::formatStatsText(parts).find("    constraint constraint#327681 3\n"), std::string::npos);
    EXPECT_NE(rune::cli::formatStatsJson("topic", "t", parts).find(R"("label": "constraint#327681")"), std::string::npos);
}

TEST(CheckedWritesQlCliRegression, UnknownStatIdsKeepDistinctJsonKeysRegression) {
    rune::cli::PartitionStats p;
    p.result.stats = {{98, 1}, {99, 2}};
    const std::vector<rune::cli::PartitionStats> parts{p};
    EXPECT_NE(rune::cli::formatStatsJson("topic", "t", parts).find(R"({"stat98": 1, "stat99": 2})"), std::string::npos);
    EXPECT_NE(rune::cli::formatStatsText(parts).find("    stat98 1\n    stat99 2\n"), std::string::npos);
}

TEST(CheckedWritesQlCliRegression, AlterTopicDropsTheTopicFromTheScriptScopeRegression) {
    std::string out, err;
    const std::string script = writeTemp("alter.rql", "topic t { id uint64, a int64 } key id; alter topic t add column b int64; create view v key id as select b from t;");
    ASSERT_EQ(runCli({"ql", "--dry-run", "-f", script}, &out, &err), 0) << err;
    EXPECT_NE(out.find("create view v source t key id\n  (source resolved when the statement runs)\n"), std::string::npos) << out;
}

TEST(CheckedWritesQlCli, ShowViewPrintsTheKeyColumnForAV1ShapedAggregate) {
    rune::view::ViewPlan plan;
    plan.viewName = "by_user";
    plan.key      = "userId";
    plan.sources.push_back({-1, "userId"});
    plan.output.push_back({"userId", PrimType::UInt64, 0, "userId", rune::view::MergeRule::Replace});
    plan.output.push_back({"characters", PrimType::Int64, 0, "", rune::view::MergeRule::CountAll});
    ASSERT_TRUE(plan.groupBy.empty());
    cat::ViewObject v;
    v.name                     = "by_user";
    v.def.normalizedPlanBlob   = rune::view::serializeAuthored(plan);
    v.def.outputDescriptorBlob = rune::codec::serialize(rune::codec::DescriptorBuilder("out").add("userId", PrimType::UInt64).add("characters", PrimType::Int64).build());
    const std::string text = rune::cli::formatViewDefinition(v);
    EXPECT_NE(text.find("  keyspec userId\n  unique no\n  group by (userId)\n"), std::string::npos) << text;
}

TEST(CheckedWritesQlCliRegression, ConstraintLabelsComeFromTheOwningViewsPlanRegression) {
    cat::ViewObject v = viewObjectOf(
        "create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= 12);");
    v.objectId = 5;
    EXPECT_EQ(eng::constraintLabel(v, 1), "account_stats.max_characters");
    EXPECT_EQ(eng::constraintLabel(v, 0), "account_stats.unique");
    EXPECT_EQ(eng::constraintLabel(v, 7), "account_stats.#7");   // an ordinal the current plan lacks never throws
    EXPECT_EQ(eng::constraintLabel(cat::CatalogState{}, (5u << 16) | 1u), "constraint#327681");   // no view owns it
}

TEST(CheckedWritesQlCli, UnauthorizedSourceNotesTheConstraintRows) {
    rune::cli::PartitionStats p;
    p.partition = 0;
    p.result.stats.emplace_back(1, 4);
    p.constraintsNote = "not authorized on source 'character'";
    const std::string text = rune::cli::formatStatsText(std::vector<rune::cli::PartitionStats>{p});
    EXPECT_NE(text.find("    constraints (not authorized on source 'character')\n"), std::string::npos) << text;
    EXPECT_EQ(text.find("(no counters)"), std::string::npos) << text;
    EXPECT_EQ(rune::cli::formatStatsJson("view", "v", std::vector<rune::cli::PartitionStats>{p}),
              "{\"view\": \"v\", \"partitions\": [{\"partition\": 0, \"stats\": {\"rejectionsKeyExists\": 4}, \"constraints\": [], "
              "\"constraintsNote\": \"not authorized on source 'character'\"}]}");
}

TEST(CheckedWritesQlCli, ShowStatsTextNotesAnOldBrokerAndFailsOnOtherRefusals) {
    using Parts = std::expected<std::vector<rune::cli::PartitionStats>, std::string>;
    const auto old = eng::showStatsText(Parts{std::unexpected(std::string(eng::kStatsBrokerTooOld))});
    ASSERT_TRUE(old.has_value());
    EXPECT_EQ(*old, "  (counters need broker v10)\n");
    const auto refused = eng::showStatsText(Parts{std::unexpected(std::string("stats of partition 0: not authorized"))});
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), "stats of partition 0: not authorized");
    const auto ok = eng::showStatsText(Parts{std::vector<rune::cli::PartitionStats>{rune::cli::PartitionStats{2, {}, {}, {}}}});
    ASSERT_TRUE(ok.has_value());
    EXPECT_EQ(*ok, "  partition 2\n    (no counters)\n");
}

TEST(CheckedWritesQlCli, ConstraintLabelOfAStatsResponseNamesTheOwningView) {
    cat::CatalogState state;
    const auto stamped = [&](cat::Ops ops, const std::string& name, uint64_t objectId) {
        cat::CatalogEvent e{.formatVer = cat::kFormatV1};
        e.kind = cat::kindOf(ops); e.ops = std::move(ops); e.objectName = name; e.objectId = objectId; e.prevVersion = 0; e.newVersion = 1;
        const auto fp = cat::previewFingerprint(state, e);
        EXPECT_TRUE(fp.has_value()) << (fp ? "" : fp.error().detail);
        e.fingerprint = fp.value_or(0);
        return e;
    };
    const ql::TopicDef topic = topicOf(kCharacter);
    cat::CreateTopicOps t;
    t.topicId = 0; t.partitions = 1; t.replicationFactor = 1;
    t.schema = cat::SchemaDef{.keyField = "characterId", .validate = true, .descriptorBlob = rune::codec::serialize(ql::viewSourceOf(topic).descriptor), .nextTag = 5};
    ASSERT_TRUE(cat::apply(state, stamped(t, "character", 1), 0));
    for (int32_t i = 1; i <= 3; ++i) {   // objectIds are dense: the view is the fifth object
        cat::CreateTopicOps filler;
        filler.topicId = i; filler.partitions = 1; filler.replicationFactor = 1;
        ASSERT_TRUE(cat::apply(state, stamped(filler, "filler" + std::to_string(i), static_cast<uint64_t>(i) + 1), i));
    }
    cat::ViewDef def = viewObjectOf(
        "create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= 12);").def;
    def.viewTopicId = 4; def.sinkPartitions = 1;
    ASSERT_TRUE(cat::apply(state, stamped(cat::CreateViewOps{def}, "account_stats", 5), 4));
    EXPECT_EQ(eng::constraintLabel(state, (5u << 16) | 1u), "account_stats.max_characters");
    EXPECT_EQ(eng::constraintLabel(state, (6u << 16) | 1u), "constraint#393217");
}

TEST(CheckedWritesQlCli, StatsJsonEscapesControlCharacters) {
    EXPECT_NE(rune::cli::formatStatsJson("topic", std::string("a\n\t\x01\"b"), {}).find(R"("a\u000a\u0009\u0001\"b")"), std::string::npos);
}

TEST(CheckedWritesQlCli, LimitZeroIsRefusedBeforeAnythingRuns) {
    std::string out, err;
    const std::string script = writeTemp("limit0.rql", "select * from v where k = 1 limit 0;");
    EXPECT_EQ(runCli({"ql", "--dry-run", "-f", script}, &out, &err), 1);
    EXPECT_NE(err.find("limit must be >= 1"), std::string::npos) << err;
}
