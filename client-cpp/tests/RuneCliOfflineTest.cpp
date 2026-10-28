// tests/RuneCliOfflineTest.cpp — rune CLI offline tests (no Aeron).  Rune QL execution is
// `rune console` / `rune ql` (Phase 2b D); the former `schema apply|alter --dry-run`
// assertions live in tests/RuneConsoleOfflineTest.cpp.
#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "rune/cli/Cli.h"

TEST(RuneCliOfflineRegression, SchemaApplyAndAlterAreRemovedWithConsolePointer) {
    for (const std::string sub : {"apply", "alter"}) {
        std::ostringstream out, err;
        EXPECT_EQ(rune::cli::run({"schema", sub, "whatever.rql", "--dry-run"}, out, err), 2) << sub;
        EXPECT_NE(err.str().find("rune schema " + sub + " was removed"), std::string::npos) << err.str();
        EXPECT_NE(err.str().find("rune console -f"), std::string::npos) << err.str();
        EXPECT_TRUE(out.str().empty()) << "a removed subcommand runs nothing; got " << out.str();
    }
}

TEST(RuneCliOffline, UsageListsConsoleAndReadOnlySchema) {
    std::ostringstream out, err;
    EXPECT_EQ(rune::cli::run({}, out, err), 1);
    const std::string u = err.str();
    for (const char* s : {"rune console", "rune schema get", "rune schema list", "rune schema generate", "rune ql"})
        EXPECT_NE(u.find(s), std::string::npos) << s << "\n" << u;
    EXPECT_EQ(u.find("schema apply"), std::string::npos) << u;
    EXPECT_EQ(u.find("schema alter"), std::string::npos) << u;
}

TEST(RuneQlCliRegression, UsageListsValidateAndExpectedVersionRegression) {
    // #304: `rune ql`'s usage names --validate and --expected-version (the flags it parses), on a bad flag and in the
    // top-level usage.
    std::ostringstream out, err;
    ASSERT_EQ(rune::cli::run({"ql", "--no-such-flag"}, out, err), 2);
    const std::string own = err.str();
    std::ostringstream out2, err2;
    ASSERT_EQ(rune::cli::run({}, out2, err2), 1);
    const std::string all = err2.str();
    const auto from = all.find("  rune ql");
    const auto to = all.find("  rune catalog export");
    ASSERT_NE(from, std::string::npos) << all;
    ASSERT_NE(to, std::string::npos) << all;
    const std::string top = all.substr(from, to - from);
    for (const char* f : {"-e", "-f", "--dry-run", "--verbose", "--request-id", "--validate", "--expected-version", "--broker"}) {
        EXPECT_NE(own.find(f), std::string::npos) << f << " missing from `rune ql`'s own usage:\n" << own;
        EXPECT_NE(top.find(f), std::string::npos) << f << " missing from the top-level `rune ql` usage:\n" << top;
    }
}
