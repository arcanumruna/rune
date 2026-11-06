// Checked Writes Part 08, Task 2: create [or replace] [transient] [unique] view.
#include <rune/ql/Parser.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace ql = rune::ql;
using ql::ItemKind;
using ql::KeyFunction;
using ql::PredOp;

namespace {

ql::CreateViewDef view(const std::string& sql) {
    auto r = ql::parse(sql);
    EXPECT_TRUE(r.has_value()) << sql << (r ? "" : " -> " + r.error().message);
    if (!r) return {};
    const auto* v = std::get_if<ql::CreateViewDef>(&*r);
    EXPECT_NE(v, nullptr) << sql;
    return v ? *v : ql::CreateViewDef{};
}

std::string errorOf(const std::string& sql) {
    auto r = ql::parse(sql);
    EXPECT_FALSE(r.has_value()) << sql;
    return r ? std::string() : r.error().message;
}

void expectKey(const std::vector<ql::KeyItem>& got, const std::vector<ql::KeyItem>& want) {
    ASSERT_EQ(got.size(), want.size());
    for (std::size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].column, want[i].column) << i;
        EXPECT_EQ(got[i].fn, want[i].fn) << i;
        EXPECT_EQ(got[i].outName, want[i].outName) << i;
    }
}

void expectItems(const std::vector<ql::SelectItem>& got, const std::vector<ql::SelectItem>& want) {
    ASSERT_EQ(got.size(), want.size());
    for (std::size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i].kind, want[i].kind) << i;
        EXPECT_EQ(got[i].column, want[i].column) << i;
        EXPECT_EQ(got[i].alias, want[i].alias) << i;
    }
}

void expectArgs(const ql::PredNode& n, uint16_t a, uint16_t b) {
    EXPECT_EQ(n.a, a);
    EXPECT_EQ(n.b, b);
}

}  // namespace

TEST(CheckedWritesQlView, TargetBlockViewsParse) {
    auto ui = view("create unique view user_identity key (provider, subject) as select userId from user;");
    EXPECT_EQ(ui.name, "user_identity");
    EXPECT_TRUE(ui.unique);
    EXPECT_FALSE(ui.transient);
    EXPECT_FALSE(ui.orReplace);
    expectKey(ui.key, {{"provider", KeyFunction::None, "provider"}, {"subject", KeyFunction::None, "subject"}});
    expectItems(ui.items, {{ItemKind::Column, "userId", ""}});
    EXPECT_EQ(ui.source, "user");
    EXPECT_TRUE(ui.groupBy.empty());
    EXPECT_TRUE(ui.constraints.empty());

    auto dh = view("create view device_history key (userId, deviceFp) as select min(ts) as firstSeen, max(ts) as lastSeen, "
                   "count(*) as logins, last(platform) as platform from device_login group by userId, deviceFp;");
    EXPECT_FALSE(dh.unique);
    expectKey(dh.key, {{"userId", KeyFunction::None, "userId"}, {"deviceFp", KeyFunction::None, "deviceFp"}});
    expectItems(dh.items, {{ItemKind::Min, "ts", "firstSeen"}, {ItemKind::Max, "ts", "lastSeen"},
                           {ItemKind::CountAll, "", "logins"}, {ItemKind::Last, "platform", "platform"}});
    EXPECT_EQ(dh.source, "device_login");
    expectKey(dh.groupBy, {{"userId", KeyFunction::None, "userId"}, {"deviceFp", KeyFunction::None, "deviceFp"}});

    auto cn = view("create unique view character_name key lower(name) as nameKey as select characterId from character;");
    EXPECT_TRUE(cn.unique);
    expectKey(cn.key, {{"name", KeyFunction::Lower, "nameKey"}});
    expectItems(cn.items, {{ItemKind::Column, "characterId", ""}});

    auto as = view("create view account_stats key userId as select count(*) as characters from character group by userId "
                   "constraint max_characters check (characters <= 12);");
    expectKey(as.key, {{"userId", KeyFunction::None, "userId"}});
    expectItems(as.items, {{ItemKind::CountAll, "", "characters"}});
    expectKey(as.groupBy, {{"userId", KeyFunction::None, "userId"}});
    ASSERT_EQ(as.constraints.size(), 1u);
    EXPECT_EQ(as.constraints[0].name, "max_characters");
    const auto& p = as.constraints[0].predicate;
    ASSERT_EQ(p.size(), 3u);
    EXPECT_EQ(p[0].op, PredOp::Column);
    EXPECT_EQ(p[0].column, "characters");
    EXPECT_EQ(p[1].op, PredOp::Const);
    EXPECT_EQ(p[1].value.kind, ql::Literal::Kind::Integer);
    EXPECT_EQ(p[1].value.magnitude, 12u);
    EXPECT_FALSE(p[1].value.negative);
    EXPECT_EQ(p[2].op, PredOp::Le);
    EXPECT_EQ(p[2].a, 0);
    EXPECT_EQ(p[2].b, 1);
}

TEST(CheckedWritesQlView, KeySpecForms) {
    const std::string tail = " as select x from s;";
    expectKey(view("create view v key c" + tail).key, {{"c", KeyFunction::None, "c"}});
    expectKey(view("create view v key (c)" + tail).key, {{"c", KeyFunction::None, "c"}});
    expectKey(view("create view v key (a, b, c)" + tail).key,
              {{"a", KeyFunction::None, "a"}, {"b", KeyFunction::None, "b"}, {"c", KeyFunction::None, "c"}});
    expectKey(view("create view v key lower(n) as k" + tail).key, {{"n", KeyFunction::Lower, "k"}});
    expectKey(view("create view v key (lower(n) as k, id)" + tail).key,
              {{"n", KeyFunction::Lower, "k"}, {"id", KeyFunction::None, "id"}});
    EXPECT_EQ(errorOf("create view v key lower(5) as k" + tail), "lower() takes a column name");
}

TEST(CheckedWritesQlView, Modifiers) {
    auto v = view("create or replace transient unique view v key id as select x from s;");
    EXPECT_TRUE(v.orReplace);
    EXPECT_TRUE(v.transient);
    EXPECT_TRUE(v.unique);
    EXPECT_EQ(errorOf("create unique transient view v key id as select x from s;"),
              "expected 'view' after 'create [or replace] [transient] [unique]', got 'transient'");
    EXPECT_EQ(errorOf("create frob v;"), "expected 'user', 'role' or 'view' after 'create', got 'frob'");
}

TEST(CheckedWritesQlView, PredicateTree) {
    auto v = view("create view v key id as select x from s constraint c check (a >= 1 and not (b <> 'x' or c < -5));");
    const auto& p = v.constraints.at(0).predicate;
    ASSERT_EQ(p.size(), 12u);
    const PredOp ops[] = {PredOp::Column, PredOp::Const, PredOp::Ge, PredOp::Column, PredOp::Const, PredOp::Ne,
                          PredOp::Column, PredOp::Const, PredOp::Lt, PredOp::Or, PredOp::Not, PredOp::And};
    for (std::size_t i = 0; i < p.size(); ++i) EXPECT_EQ(p[i].op, ops[i]) << i;
    EXPECT_EQ(p[0].column, "a");
    EXPECT_EQ(p[1].value.magnitude, 1u);
    EXPECT_EQ(p[3].column, "b");
    EXPECT_EQ(p[4].value.kind, ql::Literal::Kind::String);
    EXPECT_EQ(p[4].value.text, "x");
    EXPECT_EQ(p[6].column, "c");
    EXPECT_TRUE(p[7].value.negative);
    EXPECT_EQ(p[7].value.magnitude, 5u);
    expectArgs(p[2], 0, 1);
    expectArgs(p[5], 3, 4);
    expectArgs(p[8], 6, 7);
    expectArgs(p[9], 5, 8);
    EXPECT_EQ(p[10].a, 9);
    expectArgs(p[11], 2, 10);

    const std::string head = "create view v key id as select x from s constraint c check (";
    EXPECT_EQ(errorOf(head + "1 < a);"), "a check compares a column with a constant: <column> <op> <constant>");
    auto b = view(head + "a = true);");
    const auto& pb = b.constraints.at(0).predicate;
    ASSERT_EQ(pb.size(), 3u);
    EXPECT_EQ(pb[1].value.kind, ql::Literal::Kind::Bool);
    EXPECT_TRUE(pb[1].value.boolean);

    auto two = view("create view v key id as select x from s constraint first check (a = 1) constraint second check (b > 2);");
    ASSERT_EQ(two.constraints.size(), 2u);
    EXPECT_EQ(two.constraints[0].name, "first");
    EXPECT_EQ(two.constraints[1].name, "second");
}

TEST(CheckedWritesQlView, ItemErrors) {
    const std::string h = "create view v key id as select ";
    EXPECT_EQ(errorOf(h + "*, a from s;"), "'*' cannot be combined with other select items");
    EXPECT_EQ(errorOf(h + "sum(*) as t from s group by id;"), "'*' is only valid in count(*)");
    EXPECT_EQ(errorOf(h + "count(*) from s group by id;"), "count(*) needs 'as <name>'");
    EXPECT_EQ(errorOf(h + "min(ts) from s group by id;"), "min(…) needs 'as <name>'");
    auto star = view(h + "* from s;");
    expectItems(star.items, {{ItemKind::Star, "", ""}});
}

TEST(CheckedWritesQlView, KeywordsStayUsableAsNames) {
    auto v = view("create view v key key as select count, min as m, last from from group by group;");
    expectItems(v.items, {{ItemKind::Column, "count", ""}, {ItemKind::Column, "min", "m"}, {ItemKind::Column, "last", ""}});
    EXPECT_EQ(v.source, "from");
}

TEST(CheckedWritesQlRegression, NewStatementsAreNotSecurityStatementsRegression) {
    auto st = ql::parse("create view v key id as select x from s;").value();
    EXPECT_FALSE(ql::isSecurityStatement(st));
    EXPECT_TRUE(ql::redactedText(st).empty());
    for (const char* s : {"create user u with password 'abcdefghijkl';", "alter user u disable;", "drop user u;", "create role r;",
                          "drop role r;", "grant read on topic t to user u;", "revoke read on topic t from user u;",
                          "grant role r to user u;", "revoke role r from user u;", "show users;", "show roles;",
                          "show grants for user u;", "show security history;"})
        EXPECT_TRUE(ql::isSecurityStatement(ql::parse(s).value())) << s;
    EXPECT_FALSE(ql::isSecurityStatement(ql::parse("drop view v;").value()));
}
