// tests/ScriptTest.cpp — Phase 2b Part D: console input classification
// (classifyInput) and pins of the console-relevant parseScript behaviour.
#include <gtest/gtest.h>

#include <variant>

#include "rune/ql/Parser.h"

using namespace rune::ql;

// ── classifyInput ────────────────────────────────────────────────────────────

TEST(ClassifyInput, EmptyIncompleteComplete) {
    EXPECT_EQ(classifyInput(""), InputState::Empty);
    EXPECT_EQ(classifyInput("  \t\r\n"), InputState::Empty);
    EXPECT_EQ(classifyInput("-- just a comment\n"), InputState::Empty);
    EXPECT_EQ(classifyInput("topic t {\n"), InputState::Incomplete);
    EXPECT_EQ(classifyInput("exit"), InputState::Incomplete);
    EXPECT_EQ(classifyInput("topic t { a int32 } partitions 1 key a;"), InputState::Complete);
    EXPECT_EQ(classifyInput("topic t { a int32 } partitions 1 key a; -- done\n"), InputState::Complete);
    EXPECT_EQ(classifyInput("a; b"), InputState::Incomplete);
    EXPECT_EQ(classifyInput(";"), InputState::Complete);
}

TEST(ClassifyInputRegression, SemicolonInsideCommentIsIncomplete) {
    EXPECT_EQ(classifyInput("topic t { a int32 } -- partitions next;\n"), InputState::Incomplete);
}

TEST(ClassifyInputRegression, SemicolonInsideStringIsIncomplete) {
    // `with (copartition.group = 'a;b')` — the ';' is inside a string token.
    EXPECT_EQ(classifyInput("topic t partitions 1 with (copartition.group = 'a;b'"), InputState::Incomplete);
    // An unterminated string never completes the input, whatever follows on the line.
    EXPECT_EQ(classifyInput("topic t partitions 1 with (copartition.group = 'a;"), InputState::Incomplete);
}

// ── parseScript pins the console relies on ───────────────────────────────────

// Statement 2 starts on line 4; its bad type is on line 5, col 5 — absolute, never statement-relative.
TEST(ParseScriptRegression, ErrorPositionIsAbsoluteAcrossLines) {
    auto r = parseScript(
        "-- header\n"
        "topic a { x int32 } partitions 1 key x;\n"
        "\n"
        "topic b {\n"
        "  x int128\n"
        "} partitions 1 key x;\n");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().statementIndex, 2u);
    EXPECT_EQ(r.error().error.line, 5u);
    EXPECT_EQ(r.error().error.col, 5u);
}

TEST(ParseScriptRegression, ErrorPositionIsAbsoluteOnSameLine) {
    auto r = parseScript("topic a { x int32 } partitions 1 key x; topic b { x int128 } partitions 1 key x;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().statementIndex, 2u);
    EXPECT_EQ(r.error().error.line, 1u);
    EXPECT_EQ(r.error().error.col, 53u);
}

TEST(ParseScriptRegression, UnterminatedFinalStatementIsErrorNotDropped) {
    auto r = parseScript("topic a { x int32 } partitions 1 key x;\ntopic b { x int32 } partitions 1 key x");
    ASSERT_FALSE(r.has_value()) << "two statements, the second unterminated: never a 1-statement success";
    EXPECT_EQ(r.error().statementIndex, 2u);
    EXPECT_EQ(r.error().error.line, 2u);
}

TEST(ParseScriptRegression, SemicolonInCommentDoesNotSplit) {
    auto r = parseScript("topic a { x int32 -- not the end;\n } partitions 1 key x;\n");
    ASSERT_TRUE(r.has_value()) << r.error().error.message;
    EXPECT_EQ(r->size(), 1u);
}

TEST(ParseScriptRegression, CrlfLineEndings) {
    auto r = parseScript("topic a { x int32 }\r\npartitions 1 key x;\r\nalter topic a add column y int32;\r\n");
    ASSERT_TRUE(r.has_value()) << r.error().error.message;
    EXPECT_EQ(r->size(), 2u);
}

TEST(ParseScriptRegression, TransientTopicInAScript) {
    auto r = parseScript("transient topic ticks { id int64 } partitions 2 key id;\ntopic trades { id int64 } partitions 2 key id;\n");
    ASSERT_TRUE(r.has_value()) << r.error().error.message;
    ASSERT_EQ(r->size(), 2u);
    EXPECT_EQ(std::get<TopicDef>((*r)[0]).persistence, Persistence::Transient);
    EXPECT_EQ(std::get<TopicDef>((*r)[1]).persistence, Persistence::Disk);
}
