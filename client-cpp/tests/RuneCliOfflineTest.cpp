// tests/RuneCliOfflineTest.cpp
//
// Task 8 — rune CLI offline unit tests.
//
// Tests the `schema apply --dry-run` path via rune::cli::run().
// Links rune-ql + rune_codec only — no Aeron dependency.
//
#include <gtest/gtest.h>
#include <sstream>
#include <filesystem>
#include <fstream>
#include "rune/cli/Cli.h"   // exposes rune::cli::run
namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Regression test suite
// ---------------------------------------------------------------------------

TEST(RuneCliOffline, ApplyDryRunPrintsDescriptor) {
    fs::path f = fs::temp_directory_path()/"c.rql";
    { std::ofstream o(f); o << "topic characters { id int64, userId int64, name string } partitions 16 key userId;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.str().find("schema characters"), std::string::npos);   // toText output
}

TEST(RuneCliOffline, ApplyReportsParseErrorWithLocation) {
    fs::path f = fs::temp_directory_path()/"bad.rql";
    { std::ofstream o(f); o << "topic t { a int128 } partitions 1 key a;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
    EXPECT_NE(rc, 0);
    EXPECT_NE(err.str().find("type"), std::string::npos);
}

// ---------------------------------------------------------------------------
// Regression suite (engineering rule 6: named *Regression* so it filters
// together via --gtest_filter="*Regression*").
// ---------------------------------------------------------------------------

// Regression: apply --dry-run of a valid schema returns 0 and emits the
// toText descriptor (schema <name>) to out — never contacts a broker.
TEST(RuneCliOfflineRegression, ApplyDryRunPrintsDescriptor) {
    fs::path f = fs::temp_directory_path()/"c_regression.rql";
    { std::ofstream o(f); o << "topic characters { id int64, userId int64, name string } partitions 16 key userId;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
    EXPECT_EQ(rc, 0);
    EXPECT_NE(out.str().find("schema characters"), std::string::npos);
}

// Regression: parse errors must be reported in the exact <file>:<line>:<col>: <msg>
// format (spec) — not just contain the word "type".
TEST(RuneCliOfflineRegression, ApplyReportsParseErrorWithLocationPrefix) {
    fs::path f = fs::temp_directory_path()/"bad_regression.rql";
    { std::ofstream o(f); o << "topic t { a int128 } partitions 1 key a;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
    EXPECT_NE(rc, 0);
    const std::string e = err.str();
    // File path must appear as the prefix.
    EXPECT_NE(e.find(f.string()), std::string::npos)
        << "error output must include the source file path; got: " << e;
    // Followed by :<line>: — the offending field is on line 1.
    EXPECT_NE(e.find(f.string() + ":1:"), std::string::npos)
        << "error output must carry file:line: location prefix; got: " << e;
    // And a message describing the unknown type.
    EXPECT_NE(e.find("type"), std::string::npos);
}
