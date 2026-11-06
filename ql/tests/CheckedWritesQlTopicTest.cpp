// Checked Writes Part 08, Task 1: keyless typed topics, the `autoincrement` modifier,
// key.index / autoincrement.* settings and the comparison tokens (QM1).
#include <rune/ql/Parser.h>
#include <rune/ql/Backends.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace ql = rune::ql;
using rune::codec::PrimType;
using rune::protocol::Partitioner;
using rune::protocol::TopicKeyFeatures;

namespace {

struct TopicCell {
    const char*      sql;
    const char*      error;          // nullptr = parses
    uint32_t         partitions;
    const char*      keyField;
    std::vector<bool> autoInc;       // per field, declaration order
    TopicKeyFeatures keyFeatures;
    uint8_t          cleanupPolicy;
};

TopicKeyFeatures kf(bool index, bool autoInc, uint64_t max = rune::protocol::kAutoIncrementMaxDefault,
                    uint32_t reserve = rune::protocol::kAutoIncrementReserveDefault) {
    return {index, autoInc, Partitioner::Fnv1a, max, reserve};
}

const std::vector<TopicCell> kTopicCells{
    /*1*/  {"topic e;", nullptr, 1, "", {}, kf(false, false), 0},
    /*2*/  {"topic t { id uint64 };", nullptr, 1, "", {false}, kf(false, false), 0},
    /*3*/  {"topic t { id uint64, v int64 } partitions 2;", nullptr, 2, "", {false, false}, kf(false, false), 0},
    /*4*/  {"topic t { id uint64 autoincrement, v int64 } key id with (cleanup.policy = compact);", nullptr, 1, "id",
            {true, false}, kf(false, true), 1},
    /*5*/  {"topic t { id uint64 autoincrement } key id with (cleanup.policy = compact, autoincrement.reserve = 1024, "
            "autoincrement.max = 1000000);", nullptr, 1, "id", {true}, kf(false, true, 1000000, 1024), 1},
    /*6*/  {"topic t { id uint64, v int64 } key id with (key.index = on);", nullptr, 1, "id", {false, false}, kf(true, false), 0},
    /*7*/  {"topic t { id uint64, v int64 } key id with (key.index = off);", nullptr, 1, "id", {false, false}, kf(false, false), 0},
    /*8*/  {"topic t { id uint64 autoincrement, v int64 } key v;", "autoincrement is allowed only on the key field; 'id' is not the key"},
    /*9*/  {"topic t { id uint64 autoincrement };", "autoincrement needs a key"},
    /*10*/ {"topic t { a uint64 autoincrement, b uint64 autoincrement } key a;", "autoincrement is allowed only on the key field; 'b' is not the key"},
    /*11*/ {"topic e with (key.index = on);", nullptr, 1, "", {}, kf(true, false), 0},
    /*12*/ {"topic t { id uint64 } key id with (key.index = yes);", "invalid value 'yes' for key.index"},
    /*13*/ {"topic t { id uint64 } key id with (key.index = on, key.index = off);", "duplicate setting 'key.index'"},
    /*14*/ {"alter topic t set key.index = on;", "unknown or immutable topic setting 'key.index'"},
    /*15*/ {"topic t { id uint64 autoincrement autoincrement } key id;", "duplicate modifier 'autoincrement'"},
    /*16*/ {"topic t { id uint64 } key id;", nullptr, 1, "id", {false}, kf(false, false), 0},
    /*17*/ {"transient topic t { id uint64 autoincrement } key id with (cleanup.policy = compact);", nullptr, 1, "id", {true}, kf(false, true), 1},
    /*18*/ {"topic t { name string(16) autoincrement } key name;", nullptr, 1, "name", {true}, kf(false, true), 0},
};

class CheckedWritesQlTopicGrammar : public ::testing::TestWithParam<size_t> {};
using Cell = CheckedWritesQlTopicGrammar;

} // namespace

TEST_P(Cell, ParsesToThePlan) {
    const auto& c = kTopicCells[GetParam()];
    auto r = ql::parse(c.sql);
    if (c.error) {
        ASSERT_FALSE(r.has_value()) << c.sql;
        EXPECT_EQ(r.error().message, c.error);
        return;
    }
    ASSERT_TRUE(r.has_value()) << c.sql << ": " << r.error().message;
    const auto plan = ql::applyPlan(std::get<ql::TopicDef>(*r));
    const auto& td = std::get<ql::TopicDef>(*r);
    EXPECT_EQ(plan.partitions, c.partitions);
    EXPECT_EQ(plan.keyField, c.keyField);
    ASSERT_EQ(td.fields.size(), c.autoInc.size());
    for (size_t i = 0; i < c.autoInc.size(); ++i) EXPECT_EQ(td.fields[i].autoIncrement, c.autoInc[i]) << "field " << i;
    EXPECT_EQ(plan.keyFeatures, c.keyFeatures);
    EXPECT_EQ(plan.config.cleanupPolicy, c.cleanupPolicy);
}

INSTANTIATE_TEST_SUITE_P(Qm1, Cell, ::testing::Range(size_t{0}, kTopicCells.size()));

TEST(CheckedWritesQlTopicGrammarCount, CellCountIsPinned) { EXPECT_EQ(kTopicCells.size(), 18u); }

TEST(CheckedWritesQlTopic, NewTokensLex) {
    EXPECT_EQ(ql::classifyInput("a <= 3"), ql::InputState::Incomplete);
    EXPECT_TRUE(ql::parse("topic t { a int32 } key a with (retention.ms = -1);").has_value());
    auto r = ql::parse("topic t { a int32 } key a <");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().message, "expected ';', got '<'");
    for (const char* op : {"<>", ">=", "<", ">"}) {
        r = ql::parse(std::string("topic t { a int32 } key a ") + op);
        ASSERT_FALSE(r.has_value()) << op;
        EXPECT_EQ(r.error().message, std::string("expected ';', got '") + op + "'") << op;
    }
}

TEST(CheckedWritesQlTopic, KeyIndexValueIsCaseInsensitive) {
    for (const char* v : {"ON", "Off"}) {
        auto r = ql::parse(std::string("topic t { id uint64 } key id with (key.index = ") + v + ");");
        EXPECT_TRUE(r.has_value()) << v << (r ? "" : r.error().message);
    }
}

TEST(CheckedWritesQlTopic, AutoincrementSettingRanges) {
    auto r = ql::parse("topic t { id uint64 } key id with (autoincrement.reserve = 4294967296);");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().message, "autoincrement.reserve must be between 0 and 4294967295");
    r = ql::parse("topic t { id uint64 } key id with (autoincrement.max = -1);");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().message, "autoincrement.max must be >= 0");
}

TEST(CheckedWritesQlTopicRegression, KeyStillRefusedOnUntypedRegression) {
    auto r = ql::parse("topic e key id;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().message, "'key' needs a field list: an untyped topic (no '{…}') has no key");
}

TEST(CheckedWritesQlTopic, UserTopicOfTheTargetBlock) {
    auto r = ql::parse("topic user { userId uint64 autoincrement, provider uint16, subject string(64), createdAt int64 } "
                       "partitions 1 replicas 3 key userId with (cleanup.policy = compact);");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& td = std::get<ql::TopicDef>(*r);
    const auto plan = ql::applyPlan(td);
    EXPECT_EQ(plan.partitions, 1u);
    EXPECT_EQ(plan.replicas, 3);
    EXPECT_EQ(plan.keyField, "userId");
    ASSERT_EQ(td.fields.size(), 4u);
    EXPECT_EQ(td.fields[0].name, "userId");
    EXPECT_EQ(td.fields[0].type, PrimType::UInt64);
    EXPECT_EQ(td.fields[0].fixedLen, 0);
    EXPECT_TRUE(td.fields[0].autoIncrement);
    EXPECT_EQ(td.fields[2].name, "subject");
    EXPECT_EQ(td.fields[2].type, PrimType::FixedStr);
    EXPECT_EQ(td.fields[2].fixedLen, 64);
    EXPECT_FALSE(td.fields[2].autoIncrement);
    EXPECT_EQ(plan.config.cleanupPolicy, 1);
    EXPECT_EQ(plan.config.retentionMs, -1);
    EXPECT_TRUE(plan.keyFeatures.autoIncrement);
    EXPECT_FALSE(plan.keyFeatures.keyIndex);
    EXPECT_EQ(plan.keyFeatures.partitioner, Partitioner::Fnv1a);
}

TEST(CheckedWritesQlTopicRegression, AutoincrementRefusedOnAlterAddColumnRegression) {
    auto r = ql::parse("alter topic t add column n uint64 autoincrement;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().message, "autoincrement can only be set when the topic is created");
}
