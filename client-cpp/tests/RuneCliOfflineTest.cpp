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

TEST(RuneQlCliRegression, UsageListsTheParsedFlagsRegression) {
    // #304: `rune ql`'s usage names --expected-version (the flags it parses), on a bad flag and in the
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
    for (const char* f : {"-e", "-f", "--dry-run", "--verbose", "--request-id", "--expected-version", "--broker"}) {
        EXPECT_NE(own.find(f), std::string::npos) << f << " missing from `rune ql`'s own usage:\n" << own;
        EXPECT_NE(top.find(f), std::string::npos) << f << " missing from the top-level `rune ql` usage:\n" << top;
    }
}

// rune::cli::run must read `ql -f -` from the injected stream, not the process's std::cin (which is empty or the
// terminal under ctest).  Mutation: put std::cin back in cmdQl -> the script is empty and the output has no statement.
TEST(RuneCliOfflineRegression, QlScriptFromDashReadsTheInjectedStreamRegression) {
    std::istringstream in("topic injected_t partitions 1;\n");
    std::ostringstream out, err;
    const int rc = rune::cli::run({"ql", "-f", "-", "--dry-run"}, in, out, err);
    ASSERT_EQ(rc, 0) << err.str();
    EXPECT_NE(out.str().find("topic injected_t"), std::string::npos) << out.str();
}

// Checked writes Part 02: typed topics always validate, so the flag is gone.
TEST(CheckedWritesCli, ValidateFlagIsGoneRegression) {
    std::ostringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "--validate", "-e", "topic e;"}, out, err), 2);
    EXPECT_NE(err.str().find("--validate"), std::string::npos) << err.str();
    std::ostringstream out2, err2;
    EXPECT_EQ(rune::cli::run({"console", "--validate"}, out2, err2), 2);
    EXPECT_NE(err2.str().find("--validate"), std::string::npos) << err2.str();
}

TEST(CheckedWritesCli, PlanMarksRawTopics) {
    std::ostringstream out, err;
    ASSERT_EQ(rune::cli::run({"ql", "--dry-run", "-e", "topic e;"}, out, err), 0) << err.str();
    EXPECT_EQ(out.str(), "topic e partitions 1 replicas 0 persistence disk raw\n");
    std::ostringstream out2, err2;
    ASSERT_EQ(rune::cli::run({"ql", "--dry-run", "-e", "topic p { id int64 } key id;"}, out2, err2), 0) << err2.str();
    EXPECT_EQ(out2.str().find(" raw"), std::string::npos) << out2.str();
    EXPECT_EQ(out2.str().find("validate"), std::string::npos) << out2.str();
    EXPECT_NE(out2.str().find("topic p partitions 1 replicas 0 persistence disk key id\n"), std::string::npos) << out2.str();
}
