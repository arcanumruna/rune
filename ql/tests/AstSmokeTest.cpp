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
