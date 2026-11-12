#include <gtest/gtest.h>
#include "rune/ql/Parser.h"
#include <string>
#include <variant>
#include <vector>
using namespace rune::ql; using rune::codec::PrimType;

TEST(Parser, ParsesCharacters) {
    auto r = parse("topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;");
    ASSERT_TRUE(r.has_value());
    const TopicDef* tp = std::get_if<TopicDef>(&*r);
    ASSERT_NE(tp, nullptr);
    const TopicDef& t = *tp;
    EXPECT_EQ(t.name, "characters");
    EXPECT_EQ(t.partitions, 16u);
    EXPECT_EQ(t.keyField, "userId");
    ASSERT_EQ(t.fields.size(), 4u);
    EXPECT_EQ(t.fields[0].type, PrimType::Int64);
    EXPECT_EQ(t.fields[2].type, PrimType::String);   // variable
    EXPECT_EQ(t.fields[3].type, PrimType::FixedStr);  // string(8)
    EXPECT_EQ(t.fields[3].fixedLen, 8u);
}
TEST(Parser, LineCommentsAndTrailingComma) {
    auto r = parse("-- a topic\ntopic t { a int32, } partitions 1 key a;");
    ASSERT_TRUE(r.has_value());
    auto* t = std::get_if<TopicDef>(&*r);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->fields.size(), 1u);
}
TEST(Parser, RejectsUnknownType) {
    auto r = parse("topic t { a int128 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("type"), std::string::npos);
}
TEST(Parser, RejectsDuplicateField) {
    auto r = parse("topic t { a int32, a int64 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
}
TEST(Parser, RejectsKeyNotAField) {
    auto r = parse("topic t { a int32 } partitions 1 key b;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("key"), std::string::npos);
}
TEST(Parser, RejectsZeroPartitionsAndZeroWidth) {
    EXPECT_FALSE(parse("topic t { a int32 } partitions 0 key a;").has_value());
    EXPECT_FALSE(parse("topic t { a string(0) } partitions 1 key a;").has_value());
}
TEST(Parser, ErrorHasLocation) {
    auto r = parse("topic t { a int32 }\npartitions x key a;");   // 'x' not an int
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 2u);
}

// ── Regression suite ─────────────────────────────────────────────────────────
// Guards against silent uint16_t truncation of string(N) width (review Important #1).
TEST(ParserRegression, RejectsStringWidthAbove65535) {
    // 65536 truncates to 0 under naive static_cast<uint16_t> — must be rejected.
    auto r0 = parse("topic t { a string(65536) } partitions 1 key a;");
    ASSERT_FALSE(r0.has_value());
    EXPECT_NE(r0.error().message.find("string"), std::string::npos);
    // 65537 truncates to 1 — must be rejected.
    auto r1 = parse("topic t { a string(65537) } partitions 1 key a;");
    ASSERT_FALSE(r1.has_value());
    // 65535 is the maximum valid width.
    auto rMax = parse("topic t { a string(65535) } partitions 1 key a;");
    ASSERT_TRUE(rMax.has_value());
    auto* tMax = std::get_if<TopicDef>(&*rMax);
    ASSERT_NE(tMax, nullptr);
    EXPECT_EQ(tMax->fields[0].type, rune::codec::PrimType::FixedStr);
    EXPECT_EQ(tMax->fields[0].fixedLen, 65535u);
}

// Guards against uint32_t truncation of partitions (review Important #1, applied to partitions).
TEST(ParserRegression, RejectsPartitionsAboveUint32Max) {
    auto r = parse("topic t { a int32 } partitions 4294967296 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("partitions"), std::string::npos);
}

// Guards against the "one token too far" location bug for partitions < 1 (review Minor #2).
TEST(ParserRegression, ZeroPartitionsErrorPointsAtZero) {
    // 'partitions 0' on line 2 — the '0' token is at line 2, col 12.
    auto r = parse("topic t { a int32 }\npartitions 0 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 2u);
    EXPECT_EQ(r.error().col, 12u);  // points at '0', not the following 'key'
}

// ── ALTER TOPIC ───────────────────────────────────────────────────────────────

TEST(Parser, AlterTopicAddDropRename) {
    auto r = rune::ql::parse("alter topic users add column score int32, drop column age, rename column name to fullName;");
    ASSERT_TRUE(r.has_value());
    auto* a = std::get_if<rune::ql::AlterTopicDef>(&*r);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->name, "users");
    ASSERT_EQ(a->ops.size(), 3u);
    EXPECT_EQ(a->ops[0].kind, rune::ql::AlterOpKind::Add);
    EXPECT_EQ(a->ops[0].field.name, "score");
    EXPECT_EQ(a->ops[0].field.type, PrimType::Int32);
    EXPECT_EQ(a->ops[1].kind, rune::ql::AlterOpKind::Drop);
    EXPECT_EQ(a->ops[1].name, "age");
    EXPECT_EQ(a->ops[2].kind, rune::ql::AlterOpKind::Rename);
    EXPECT_EQ(a->ops[2].name, "name");
    EXPECT_EQ(a->ops[2].newName, "fullName");
}

TEST(Parser, AlterSetCompatibility) {
    auto r = rune::ql::parse("alter topic users set compatibility forward;");
    ASSERT_TRUE(r.has_value());
    auto* a = std::get_if<rune::ql::AlterTopicDef>(&*r);
    ASSERT_NE(a, nullptr);
    ASSERT_EQ(a->ops.size(), 1u);
    EXPECT_EQ(a->ops[0].kind, rune::ql::AlterOpKind::SetCompatibility);
    EXPECT_EQ(a->ops[0].mode, rune::ql::Compatibility::Forward);
}

TEST(Parser, AlterSetCompatibilityBackward) {
    auto r = rune::ql::parse("alter topic users set compatibility backward;");
    ASSERT_TRUE(r.has_value());
    auto* a = std::get_if<rune::ql::AlterTopicDef>(&*r);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->ops[0].mode, rune::ql::Compatibility::Backward);
}

TEST(Parser, ExistingTopicStatementStillParses) {
    auto r = rune::ql::parse("topic users { id int64 } partitions 4 key id;");
    ASSERT_TRUE(r.has_value());
    EXPECT_NE(std::get_if<rune::ql::TopicDef>(&*r), nullptr);
}

TEST(Parser, RejectsUnknownStatementKeyword) {
    auto r = rune::ql::parse("truncate topic users;");
    ASSERT_FALSE(r.has_value());
}

// ── ALTER TOPIC located-error regression ─────────────────────────────────────
TEST(ParserRegression, AlterMissingColumnKeywordIsLocatedError) {
    auto r = rune::ql::parse("alter topic users add score int32;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("column"), std::string::npos);
}

TEST(ParserRegression, AlterUnknownOpIsLocatedError) {
    auto r = rune::ql::parse("alter topic users frobnicate column age;");
    ASSERT_FALSE(r.has_value());
    EXPECT_GT(r.error().line, 0u);
}

// ── Phase 2b E: transient topics ─────────────────────────────────────────────
TEST(Parser, TransientTopicSetsPersistence) {
    auto r = rune::ql::parse(
        "transient topic presence { userId int64, online bool } partitions 4 key userId;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto* t = std::get_if<rune::ql::TopicDef>(&*r);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->name, "presence");
    EXPECT_EQ(t->partitions, 4u);
    EXPECT_EQ(t->keyField, "userId");
    EXPECT_EQ(t->persistence, rune::ql::Persistence::Transient);
}

TEST(Parser, PlainTopicDefaultsToDisk) {
    auto r = rune::ql::parse("topic users { id int64 } partitions 4 key id;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(std::get<rune::ql::TopicDef>(*r).persistence, rune::ql::Persistence::Disk);
    EXPECT_EQ(static_cast<uint8_t>(rune::ql::Persistence::Disk), 0u);
    EXPECT_EQ(static_cast<uint8_t>(rune::ql::Persistence::Transient), 1u);
}

TEST(ParserRegression, TransientWithoutTopicIsLocatedError) {
    auto r = rune::ql::parse("transient presence { a int32 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("'topic' after 'transient'"), std::string::npos)
        << r.error().message;
    EXPECT_EQ(r.error().line, 1u);
    EXPECT_EQ(r.error().col, 11u) << "error points at the token after 'transient'";
}

// 'transient' is a statement keyword only in statement-leading position: a topic
// or a field literally named "transient" must keep parsing.
TEST(ParserRegression, TopicNamedTransientStillParses) {
    auto r = rune::ql::parse("topic transient { transient int32 } partitions 1 key transient;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& t = std::get<rune::ql::TopicDef>(*r);
    EXPECT_EQ(t.name, "transient");
    EXPECT_EQ(t.fields[0].name, "transient");
    EXPECT_EQ(t.persistence, rune::ql::Persistence::Disk);
}

// Spec E: alter topic cannot change persistence — both spellings a user would
// try are rejected with an explicit message (not a generic syntax error).
TEST(ParserRegression, AlterCannotChangePersistence) {
    auto a = rune::ql::parse("alter topic presence set persistence transient;");
    ASSERT_FALSE(a.has_value());
    EXPECT_NE(a.error().message.find("cannot change persistence"), std::string::npos)
        << a.error().message;
    EXPECT_EQ(a.error().col, 26u) << "points at 'persistence'";

    auto b = rune::ql::parse("alter transient topic presence add column x int32;");
    ASSERT_FALSE(b.has_value());
    EXPECT_NE(b.error().message.find("cannot change persistence"), std::string::npos)
        << b.error().message;
    EXPECT_EQ(b.error().col, 7u) << "points at 'transient'";
}

TEST(ParserRegression, UnknownStatementMentionsTransient) {
    auto r = rune::ql::parse("truncate topic users;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("'transient'"), std::string::npos) << r.error().message;
}

// Checked writes Part 02: `partitions` is optional and defaults to 1.
static const TopicDef& topicOf(const std::expected<Statement, ParseError>& r) {
    return std::get<TopicDef>(*r);
}

TEST(CheckedWritesQl, PartitionsDefaultToOne) {
    auto r = parse("topic events;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(topicOf(r).partitions, 1u);
    EXPECT_TRUE(topicOf(r).fields.empty());

    r = parse("topic p { id uint64 } key id;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(topicOf(r).partitions, 1u);

    r = parse("topic p { id uint64 } partitions 4 key id;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(topicOf(r).partitions, 4u);

    r = parse("topic p partitions 0;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("partitions must be >= 1"), std::string::npos);

    r = parse("transient topic t;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(topicOf(r).partitions, 1u);
    EXPECT_EQ(topicOf(r).persistence, Persistence::Transient);
}

TEST(CheckedWritesQl, ReplicasAndWithWorkWithoutPartitions) {
    auto r = parse("topic e replicas 3 with (tier = leader);");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(topicOf(r).partitions, 1u);
    EXPECT_EQ(topicOf(r).replicas, 3);
    EXPECT_EQ(topicOf(r).with.size(), 1u);
}

TEST(CheckedWritesQl, DocExamplesParse) {
    for (const char* stmt : {"topic events;",
                             "topic logs partitions 3 replicas 3;",
                             "transient topic presence;",
                             "topic player { userId uint64, name string(32), score int64 } partitions 4 key userId;",
                             "topic orders partitions 4 replicas 3 with (tier = leader, durability = all-in-sync);",
                             "topic audit partitions 1 with (cleanup.policy = compact, segment.bytes = 1048576);"}) {
        const auto r = parse(stmt);
        EXPECT_TRUE(r.has_value()) << stmt << ": " << (r ? "" : r.error().message);
    }
}

// ---- #1008: `create [transient] [lease] topic` ----
namespace {
TopicDef topicFrom(const std::string& text) {
    auto r = parse(text);
    EXPECT_TRUE(r.has_value()) << text << (r ? "" : " -> " + r.error().message);
    const TopicDef* t = r ? std::get_if<TopicDef>(&*r) : nullptr;
    EXPECT_NE(t, nullptr) << text;
    return t ? *t : TopicDef{};
}
std::string parseError(const std::string& text) {
    auto r = parse(text);
    return r ? std::string("<accepted>") : r.error().message;
}
}  // namespace

TEST(ParserRegression, CreateTopicFormsEqualBareFormsRegression) {
    struct Pair { std::string bare, create; };
    const std::vector<Pair> pairs = {
        {"topic e;", "create topic e;"},
        {"topic l partitions 3 replicas 3;", "create topic l partitions 3 replicas 3;"},
        {"topic p { userId uint64, name string(32), score int64 } partitions 4 key userId;",
         "create topic p { userId uint64, name string(32), score int64 } partitions 4 key userId;"},
        {"topic o partitions 4 replicas 3 with (tier = leader, durability = all-in-sync);",
         "create topic o partitions 4 replicas 3 with (tier = leader, durability = all-in-sync);"},
        {"topic c { id uint64 autoincrement } partitions 2 key id with (cleanup.policy = compact);",
         "create topic c { id uint64 autoincrement } partitions 2 key id with (cleanup.policy = compact);"},
        {"transient topic t { id uint64 } key id;", "create transient topic t { id uint64 } key id;"},
        {"lease topic s { path string(128), uri string(128) } partitions 1 replicas 3 key path with (lease.ttl = 15000);",
         "create lease topic s { path string(128), uri string(128) } partitions 1 replicas 3 key path with (lease.ttl = 15000);"},
        {"transient lease topic on { userId uint64, zone int32 } partitions 4 key userId with (lease.ttl = 10000, lease.max_ttl = 60000);",
         "create transient lease topic on { userId uint64, zone int32 } partitions 4 key userId with (lease.ttl = 10000, lease.max_ttl = 60000);"},
    };
    for (const auto& p : pairs) {
        const TopicDef a = topicFrom(p.bare), b = topicFrom(p.create);
        EXPECT_EQ(a.name, b.name) << p.create;
        ASSERT_EQ(a.fields.size(), b.fields.size()) << p.create;
        for (std::size_t i = 0; i < a.fields.size(); ++i) {
            EXPECT_EQ(a.fields[i].name, b.fields[i].name) << p.create;
            EXPECT_EQ(a.fields[i].type, b.fields[i].type) << p.create;
            EXPECT_EQ(a.fields[i].fixedLen, b.fields[i].fixedLen) << p.create;
            EXPECT_EQ(a.fields[i].autoIncrement, b.fields[i].autoIncrement) << p.create;
        }
        EXPECT_EQ(a.partitions, b.partitions) << p.create;
        EXPECT_EQ(a.keyField, b.keyField) << p.create;
        EXPECT_EQ(a.persistence, b.persistence) << p.create;
        EXPECT_EQ(a.replicas, b.replicas) << p.create;
        ASSERT_EQ(a.with.size(), b.with.size()) << p.create;
        for (std::size_t i = 0; i < a.with.size(); ++i) {
            EXPECT_EQ(a.with[i].key, b.with[i].key) << p.create;
            EXPECT_EQ(a.with[i].value, b.with[i].value) << p.create;
            EXPECT_EQ(a.with[i].text, b.with[i].text) << p.create;
        }
        EXPECT_EQ(a.lease, b.lease) << p.create;
    }
    EXPECT_EQ(topicFrom("create transient topic t { id uint64 } key id;").persistence, Persistence::Transient);
    EXPECT_FALSE(topicFrom("create transient topic t { id uint64 } key id;").lease);
    const TopicDef tl = topicFrom("create transient lease topic t { id uint64 } key id;");
    EXPECT_TRUE(tl.lease);
    EXPECT_EQ(tl.persistence, Persistence::Transient);
    EXPECT_TRUE(topicFrom("create lease topic t { id uint64 } key id;").lease);
    EXPECT_EQ(topicFrom("create topic t;").persistence, Persistence::Disk);
    EXPECT_EQ(topicFrom("CREATE TOPIC x;").name, "x");
    const TopicDef named = topicFrom("create topic lease { id int32 } key id;");
    EXPECT_EQ(named.name, "lease");
    EXPECT_FALSE(named.lease);
}

TEST(ParserRegression, CreateTransientStillMeansViewRegression) {
    for (const char* text : {"create transient view v key id as select id from t;",
                             "create transient unique view v key id as select id from t;"}) {
        auto r = parse(text);
        ASSERT_TRUE(r.has_value()) << text << (r ? "" : " -> " + r.error().message);
        const auto* v = std::get_if<CreateViewDef>(&*r);
        ASSERT_NE(v, nullptr) << text;
        EXPECT_TRUE(v->transient) << text;
    }
}

TEST(ParserRegression, CreateTopicErrorsRegression) {
    EXPECT_EQ(parseError("create lease view x;"), "expected 'topic' after 'create lease', got 'view'");
    EXPECT_EQ(parseError("create transient lease view x;"), "expected 'topic' after 'create transient lease', got 'view'");
    EXPECT_EQ(parseError("create or replace topic t;"),
              "expected 'view' after 'create [or replace] [transient] [unique]', got 'topic'");
    EXPECT_EQ(parseError("create frob v;"),
              "expected 'topic', 'lease', 'transient', 'view', 'user' or 'role' after 'create', got 'frob'");
}
