#include <gtest/gtest.h>
#include "rune/ql/Ast.h"

TEST(Ast, Constructs) {
    rune::ql::TopicDef t{"characters", {{"id", rune::codec::PrimType::Int64, 0}}, 16, "id"};
    EXPECT_EQ(t.name, "characters");
    EXPECT_EQ(t.partitions, 16u);
    EXPECT_EQ(t.fields.size(), 1u);
}

TEST(Ast, FixedStrFieldDef) {
    rune::ql::FieldDef f{"username", rune::codec::PrimType::FixedStr, 32};
    EXPECT_EQ(f.name, "username");
    EXPECT_EQ(f.type, rune::codec::PrimType::FixedStr);
    EXPECT_EQ(f.fixedLen, 32u);
}

TEST(Ast, ParseErrorDefaults) {
    rune::ql::ParseError e{0, 0, "unexpected token"};
    EXPECT_EQ(e.line, 0u);
    EXPECT_EQ(e.col, 0u);
    EXPECT_EQ(e.message, "unexpected token");
}

TEST(Ast, AlterOpConstructs) {
    rune::ql::AlterOp op{rune::ql::AlterOpKind::Rename, {}, "oldName", "newName", rune::ql::Compatibility::Forward};
    EXPECT_EQ(op.kind, rune::ql::AlterOpKind::Rename);
    EXPECT_EQ(op.name, "oldName");
    EXPECT_EQ(op.newName, "newName");
    EXPECT_EQ(op.mode, rune::ql::Compatibility::Forward);
}

TEST(Ast, AlterTopicDefConstructs) {
    rune::ql::AlterTopicDef ad{"users", {{rune::ql::AlterOpKind::Drop, {}, "age", "", rune::ql::Compatibility::Backward}}};
    EXPECT_EQ(ad.name, "users");
    ASSERT_EQ(ad.ops.size(), 1u);
    EXPECT_EQ(ad.ops[0].kind, rune::ql::AlterOpKind::Drop);
}
