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
