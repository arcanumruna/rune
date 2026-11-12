// SecurityStatementTextTest.cpp - the leader's rendering of a security statement (#527: escaping inside a literal).
#include <rune/security/SecurityStatementText.h>

#include <gtest/gtest.h>

#include <string>

namespace {
// GRANT READ ON GROUP <exact pattern> TO ROLE r;
std::string grantGroup(const std::string& pattern) {
    return rune::security::securityStatementText(6, "r", 0, 0, 0, 0, 1, 0, 3, 0, pattern, "");
}
}

TEST(SecurityStatementTextRegression, ControlBytesBackslashAndHighBytesAreEscapedRegression) {
    struct Case { std::string pattern; std::string literal; };
    const Case cases[] = {
        {"a\nb", "'a\\x0ab'"},
        {"a\x1b[31m", "'a\\x1b[31m'"},
        {std::string("a\0b", 3), "'a\\x00b'"},
        {"a\\b", "'a\\\\b'"},
        {"it's", "'it''s'"},
        {"caf\xC3\xA9", "'caf\\xc3\\xa9'"},
        {"a\x7f", "'a\\x7f'"},
        {"orders.", "'orders.'"},
    };
    for (const Case& c : cases) {
        EXPECT_EQ(grantGroup(c.pattern), "GRANT READ ON GROUP " + c.literal + " TO ROLE r;") << c.literal;
        const std::string prefix = rune::security::securityStatementText(6, "r", 0, 0, 0, 0, 1, 0, 3, 1, c.pattern, "");
        EXPECT_EQ(prefix, "GRANT READ ON GROUP PREFIX " + c.literal + " TO ROLE r;") << c.literal;
    }
}

TEST(SecurityStatementTextRegression, ANameWithAControlOrHighByteIsAQuotedLiteralRegression) {
    const std::string role = rune::security::securityStatementText(4, "a\nb", 0, 0, 0, 0, 0, 0, 0, 0, "", "");
    EXPECT_EQ(role, "CREATE ROLE 'a\\x0ab';");
    const std::string hi = rune::security::securityStatementText(4, "caf\xC3\xA9", 0, 0, 0, 0, 0, 0, 0, 0, "", "");
    EXPECT_EQ(hi, "CREATE ROLE 'caf\\xc3\\xa9';");
}
