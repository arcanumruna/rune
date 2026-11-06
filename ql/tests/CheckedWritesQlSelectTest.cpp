// Checked Writes Part 08, Task 3: select ... where ... [limit n], show topic, show view.
#include <rune/ql/Parser.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace ql = rune::ql;
using ql::ItemKind;
using ql::KeyFunction;

namespace {

ql::SelectDef select(const std::string& sql) {
    auto r = ql::parse(sql);
    EXPECT_TRUE(r.has_value()) << sql << (r ? "" : " -> " + r.error().message);
    if (!r) return {};
    const auto* s = std::get_if<ql::SelectDef>(&*r);
    EXPECT_NE(s, nullptr) << sql;
    return s ? *s : ql::SelectDef{};
}

std::string errorOf(const std::string& sql) {
    auto r = ql::parse(sql);
    EXPECT_FALSE(r.has_value()) << sql;
    return r ? std::string() : r.error().message;
}

}  // namespace

TEST(CheckedWritesQlSelect, SpecReadsParse) {
    auto a = select("select * from account_characters where userId = 42;");
    EXPECT_TRUE(a.columns.empty());
    EXPECT_EQ(a.view, "account_characters");
    ASSERT_EQ(a.where.size(), 1u);
    EXPECT_EQ(a.where[0].column, "userId");
    EXPECT_EQ(a.where[0].value.kind, ql::Literal::Kind::Integer);
    EXPECT_FALSE(a.where[0].value.negative);
    EXPECT_EQ(a.where[0].value.magnitude, 42u);
    EXPECT_FALSE(a.where[0].value.lowered);
    EXPECT_FALSE(a.limit.has_value());

    auto b = select("select characterId from character_name where nameKey = lower('Foo');");
    EXPECT_EQ(b.columns, std::vector<std::string>{"characterId"});
    EXPECT_EQ(b.view, "character_name");
    ASSERT_EQ(b.where.size(), 1u);
    EXPECT_EQ(b.where[0].column, "nameKey");
    EXPECT_EQ(b.where[0].value.kind, ql::Literal::Kind::String);
    EXPECT_EQ(b.where[0].value.text, "Foo");
    EXPECT_TRUE(b.where[0].value.lowered);

    auto c = select("select a, b from v where x = -3 and y = 'q' limit 10;");
    EXPECT_EQ(c.columns, (std::vector<std::string>{"a", "b"}));
    ASSERT_EQ(c.where.size(), 2u);
    EXPECT_EQ(c.where[0].column, "x");
    EXPECT_TRUE(c.where[0].value.negative);
    EXPECT_EQ(c.where[0].value.magnitude, 3u);
    EXPECT_EQ(c.where[1].column, "y");
    EXPECT_EQ(c.where[1].value.text, "q");
    EXPECT_FALSE(c.where[1].value.lowered);
    ASSERT_TRUE(c.limit.has_value());
    EXPECT_EQ(*c.limit, 10u);

    EXPECT_EQ(*select("select * from v where x = true limit 4294967295;").limit, 4294967295u);
}

TEST(CheckedWritesQlSelect, ShowForms) {
    auto t = ql::parse("show topic user;");
    ASSERT_TRUE(t.has_value());
    ASSERT_NE(std::get_if<ql::ShowTopicDef>(&*t), nullptr);
    EXPECT_EQ(std::get<ql::ShowTopicDef>(*t).name, "user");

    auto v = ql::parse("show view device_history;");
    ASSERT_TRUE(v.has_value());
    ASSERT_NE(std::get_if<ql::ShowViewDef>(&*v), nullptr);
    EXPECT_EQ(std::get<ql::ShowViewDef>(*v).name, "device_history");

    auto a = ql::parse("show topic user as of version 2;");
    ASSERT_TRUE(a.has_value());
    EXPECT_NE(std::get_if<ql::ShowAsOfDef>(&*a), nullptr);

    EXPECT_NE(errorOf("show schema user;").find("expected 'as', got ';'"), std::string::npos);
}

TEST(CheckedWritesQlSelect, NewStatementsAreNotSecurityStatements) {
    for (const char* sql : {"select * from v where k = 1;", "show topic t;", "show view v;"}) {
        auto r = ql::parse(sql);
        ASSERT_TRUE(r.has_value()) << sql;
        EXPECT_FALSE(ql::isSecurityStatement(*r)) << sql;
    }
}

TEST(CheckedWritesQlSelect, Errors) {
    EXPECT_NE(errorOf("select * from v where k = 1 limit 0;").find("limit must be >= 1"), std::string::npos);
    EXPECT_NE(errorOf("select * from v where k = 1 limit x;").find("expected integer for limit, got"), std::string::npos);
    EXPECT_FALSE(errorOf("select from v where k = 1;").empty());
    EXPECT_FALSE(errorOf("select * from v where k = ;").empty());
    EXPECT_NE(errorOf("select * from v where k = 1 limit 4294967296;").find("limit must be <= 4294967295"), std::string::npos);
    EXPECT_NE(errorOf("select * from v where k = 1 and k = 2;").find("duplicate where column 'k'"), std::string::npos);
    EXPECT_NE(errorOf("select * from v where k = lower(name);").find("lower() takes a string literal"), std::string::npos);
    EXPECT_NE(errorOf("select * from v;").find("a select needs 'where'"), std::string::npos);
    EXPECT_NE(errorOf("select * from v limit 3;").find("a select needs 'where'"), std::string::npos);
}

TEST(CheckedWritesQlRegression, KeywordsAreNamesWhereTheGrammarAllowsRegression) {
    auto r = ql::parse("create view v key key as select count as min, last(platform) as platform, count(*) as group from src group by key;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& v = std::get<ql::CreateViewDef>(*r);
    ASSERT_EQ(v.key.size(), 1u);
    EXPECT_EQ(v.key[0].column, "key");
    EXPECT_EQ(v.key[0].fn, KeyFunction::None);
    ASSERT_EQ(v.items.size(), 3u);
    EXPECT_EQ(v.items[0].kind, ItemKind::Column);
    EXPECT_EQ(v.items[0].column, "count");
    EXPECT_EQ(v.items[0].alias, "min");
    EXPECT_EQ(v.items[1].kind, ItemKind::Last);
    EXPECT_EQ(v.items[1].column, "platform");
    EXPECT_EQ(v.items[1].alias, "platform");
    EXPECT_EQ(v.items[2].kind, ItemKind::CountAll);
    EXPECT_EQ(v.items[2].alias, "group");
    ASSERT_EQ(v.groupBy.size(), 1u);
    EXPECT_EQ(v.groupBy[0].column, "key");

    auto s = select("select from, check from unique where key = 1;");
    EXPECT_EQ(s.columns, (std::vector<std::string>{"from", "check"}));
    EXPECT_EQ(s.view, "unique");
    ASSERT_EQ(s.where.size(), 1u);
    EXPECT_EQ(s.where[0].column, "key");
}
