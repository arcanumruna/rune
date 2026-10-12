// TopicNameTest.cpp — DDL conformance: the one rule for a DDL-created topic/view
// name (TopicName.h).  A name becomes the directory dataDir/topics/<name>/, so it
// must be ONE safe path segment; "__" names are internal.
#include <gtest/gtest.h>
#include <rune/node/TopicName.h>

#include <string>

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
