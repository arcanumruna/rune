// TopicNameTest.cpp — DDL conformance: the one rule for a DDL-created topic/view
// name (TopicName.h).  A name becomes the directory dataDir/topics/<name>/, so it
// must be ONE safe path segment; "__" names are internal.
#include <gtest/gtest.h>
#include <rune/node/TopicName.h>

#include <cctype>
#include <string>
#include <vector>

using rune::node::checkTopicName;
using rune::node::isValidTopicName;
using rune::node::topicNamesFoldEqual;
using rune::node::TopicNameError;

static_assert(isValidTopicName("orders"));
static_assert(!isValidTopicName(""));
static_assert(!isValidTopicName("__views"));
static_assert(topicNamesFoldEqual("Orders", "oRDERS"));

TEST(TopicNameRegression, AcceptsSafeSegments) {
    for (const char* n : {"orders", "Orders.v2", "a", "_x", "-", ".hidden", "a-b_c.D9"})
        EXPECT_EQ(checkTopicName(n), TopicNameError::None) << n;
    EXPECT_EQ(checkTopicName(std::string(249, 'x')), TopicNameError::None) << "249 bytes is the max";
}

TEST(TopicNameRegression, RejectsEmptyTooLongAndDotSegments) {
    EXPECT_EQ(checkTopicName(""), TopicNameError::Empty);
    EXPECT_EQ(checkTopicName(std::string(250, 'x')), TopicNameError::TooLong);
    EXPECT_EQ(checkTopicName("."), TopicNameError::DotSegment);
    EXPECT_EQ(checkTopicName(".."), TopicNameError::DotSegment);
}

TEST(TopicNameRegression, RejectsPathControlAndNonAsciiBytes) {
    for (const std::string n : {std::string("a/b"), std::string("../x"), std::string("/abs"),
                                std::string("a\\b"), std::string("has space"), std::string("tab\tx"),
                                std::string("caf\xC3\xA9"), std::string("nul\0b", 5), std::string("colon:x")})
        EXPECT_EQ(checkTopicName(n), TopicNameError::BadChar) << n;
}

TEST(TopicNameRegression, ReservesTheDoubleUnderscorePrefix) {
    for (const char* n : {"__topic_schemas", "__views", "__consumer_offsets", "__transaction_state",
                          "__cluster_metadata", "__view_snapshots", "__orphan.3", "__"})
        EXPECT_EQ(checkTopicName(n), TopicNameError::Reserved) << n;
    EXPECT_EQ(checkTopicName("_single"), TopicNameError::None);
}

TEST(TopicNameRegression, FoldEqualityIsAsciiCaseOnly) {
    EXPECT_TRUE(topicNamesFoldEqual("Orders", "orders"));
    EXPECT_FALSE(topicNamesFoldEqual("orders", "orders "));
    EXPECT_FALSE(topicNamesFoldEqual("orders", "order"));
    EXPECT_FALSE(topicNamesFoldEqual("caf\xC3\xA9", "CAF\xC3\x89")) << "non-ASCII bytes are never folded";
}

TEST(TopicNameRegression, EveryErrorHasAHumanReadableText) {
    using rune::node::topicNameErrorText;
    EXPECT_EQ(topicNameErrorText(TopicNameError::Empty), "empty");
    EXPECT_EQ(topicNameErrorText(TopicNameError::TooLong), "longer than 249 bytes");
    EXPECT_EQ(topicNameErrorText(TopicNameError::DotSegment), "'.' or '..'");
    EXPECT_EQ(topicNameErrorText(TopicNameError::BadChar), "a byte outside [A-Za-z0-9._-]");
    EXPECT_EQ(topicNameErrorText(TopicNameError::Reserved), "the '__' prefix is reserved for internal topics");
    EXPECT_EQ(topicNameErrorText(TopicNameError::None), "ok");
}

// ---- Fix round 1: every byte value, in every position -----------------------

namespace {
bool byteIsAllowed(int b) {
    return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') ||
           b == '.' || b == '_' || b == '-';
}

enum class Pos { Alone, Start, Middle, End };

std::string nameWithByte(int b, Pos pos) {
    const char c = static_cast<char>(b);
    switch (pos) {
        case Pos::Alone:  return std::string(1, c);
        case Pos::Start:  return std::string(1, c) + "abc";
        case Pos::Middle: return std::string("ab") + c + "cd";
        case Pos::End:    return std::string("abc") + c;
    }
    return {};
}

struct ByteCase { int byte; Pos pos; };
std::vector<ByteCase> allByteCases() {
    std::vector<ByteCase> v;
    for (int b = 0; b < 256; ++b)
        for (Pos p : {Pos::Alone, Pos::Start, Pos::Middle, Pos::End}) v.push_back({b, p});
    return v;
}
} // namespace

class TopicNameByteRegression : public ::testing::TestWithParam<ByteCase> {};

TEST_P(TopicNameByteRegression, ByteIsAcceptedExactlyWhenInTheAllowedSet) {
    const auto [b, pos] = GetParam();
    const std::string n = nameWithByte(b, pos);
    const auto got = checkTopicName(n);
    if (!byteIsAllowed(b)) {
        EXPECT_EQ(got, TopicNameError::BadChar) << "byte " << b;
    } else if (pos == Pos::Alone && b == '.') {
        EXPECT_EQ(got, TopicNameError::DotSegment) << "a lone '.' is a dot segment";
    } else {
        EXPECT_EQ(got, TopicNameError::None) << "byte " << b;
    }
}

std::string byteCaseName(const ::testing::TestParamInfo<ByteCase>& i) {
    static const char* const kPos[] = {"Alone", "Start", "Middle", "End"};
    return "b" + std::to_string(i.param.byte) + "_" + kPos[static_cast<int>(i.param.pos)];
}

INSTANTIATE_TEST_SUITE_P(AllBytes, TopicNameByteRegression, ::testing::ValuesIn(allByteCases()), byteCaseName);

TEST(TopicNameRegression, RuleExceptionsStillApplyToAllowedBytes) {
    EXPECT_EQ(checkTopicName("."), TopicNameError::DotSegment);
    EXPECT_EQ(checkTopicName(".."), TopicNameError::DotSegment);
    EXPECT_EQ(checkTopicName("..."), TopicNameError::None);
    EXPECT_EQ(checkTopicName("__"), TopicNameError::Reserved);
    EXPECT_EQ(checkTopicName("__x"), TopicNameError::Reserved);
    EXPECT_EQ(checkTopicName("_"), TopicNameError::None);
    EXPECT_EQ(checkTopicName("a__"), TopicNameError::None);
}

TEST(TopicNameRegression, FoldDoesNotCrossTheAsciiLetterBoundaries) {
    EXPECT_FALSE(topicNamesFoldEqual("@", "`"));   // '@' 0x40 is just below 'A', '`' 0x60 just below 'a'
    EXPECT_FALSE(topicNamesFoldEqual("[", "{"));   // '[' 0x5B just above 'Z', '{' 0x7B just above 'z'
    EXPECT_FALSE(topicNamesFoldEqual("@", "a"));
    EXPECT_FALSE(topicNamesFoldEqual("[", "z"));
    EXPECT_FALSE(topicNamesFoldEqual("`", "A"));
    EXPECT_FALSE(topicNamesFoldEqual("{", "Z"));
}

TEST(TopicNameRegression, FoldSweepsAllLettersPairwiseAndNothingElse) {
    for (int i = 0; i < 26; ++i) {
        const std::string up(1, static_cast<char>('A' + i)), lo(1, static_cast<char>('a' + i));
        EXPECT_TRUE(topicNamesFoldEqual(up, lo)) << up;
        EXPECT_TRUE(topicNamesFoldEqual(lo, up)) << up;
        EXPECT_TRUE(topicNamesFoldEqual(up, up));
        for (int j = 0; j < 26; ++j) {
            if (j == i) continue;
            EXPECT_FALSE(topicNamesFoldEqual(up, std::string(1, static_cast<char>('a' + j))));
        }
    }
    for (int a = 0; a < 256; ++a) {          // every non-letter byte folds only with itself
        if (std::isalpha(a) && a < 128) continue;
        for (int b = 0; b < 256; ++b)
            EXPECT_EQ(topicNamesFoldEqual(std::string(1, static_cast<char>(a)), std::string(1, static_cast<char>(b))), a == b);
    }
}
