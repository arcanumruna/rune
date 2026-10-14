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
// Task 20: `rune schema alter` + `apply` variant dispatch (ql Task 16/17
// widened parse() to std::variant<TopicDef, AlterTopicDef>).
// ---------------------------------------------------------------------------

// `rune schema alter <alter.rql> --dry-run` prints the parsed ops (no broker).
TEST(RuneCliOffline, AlterDryRunPrintsParsedOps) {
    fs::path f = fs::temp_directory_path()/"alter_characters.rql";
    { std::ofstream o(f);
      o << "alter topic characters add column nickname string, "
           "set compatibility backward;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","alter", f.string(), "--dry-run"}, out, err);
    EXPECT_EQ(rc, 0) << "err=" << err.str();
    const std::string o = out.str();
    EXPECT_NE(o.find("alter topic characters"), std::string::npos) << "got: " << o;
    EXPECT_NE(o.find("ADD"), std::string::npos) << "got: " << o;
    EXPECT_NE(o.find("nickname"), std::string::npos) << "got: " << o;
    EXPECT_NE(o.find("SET_COMPATIBILITY"), std::string::npos) << "got: " << o;
}

// A 'topic' file through `apply --dry-run` still prints a DefineTopic plan
// (the TopicDef alternative of the variant), unchanged behavior.
TEST(RuneCliOffline, ApplyDryRunStillDefinesTopicFile) {
    fs::path f = fs::temp_directory_path()/"topic_characters.rql";
    { std::ofstream o(f);
      o << "topic characters { id int64, userId int64 } partitions 4 key userId;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
    EXPECT_EQ(rc, 0) << "err=" << err.str();
    EXPECT_NE(out.str().find("schema characters"), std::string::npos)
        << "got: " << out.str();
}

// An 'alter topic' file through `apply --dry-run` routes to the alter path
// (prints the parsed ops), not the DefineTopic descriptor path.
TEST(RuneCliOffline, ApplyDryRunRoutesAlterFileToAlterPath) {
    fs::path f = fs::temp_directory_path()/"apply_alter.rql";
    { std::ofstream o(f); o << "alter topic characters drop column nickname;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
    EXPECT_EQ(rc, 0) << "err=" << err.str();
    const std::string o = out.str();
    EXPECT_NE(o.find("alter topic characters"), std::string::npos) << "got: " << o;
    EXPECT_NE(o.find("DROP"), std::string::npos) << "got: " << o;
    EXPECT_NE(o.find("nickname"), std::string::npos) << "got: " << o;
    // Must NOT look like the DefineTopic descriptor dump.
    EXPECT_EQ(o.find("schema characters"), std::string::npos) << "got: " << o;
}

// A malformed --expected-version must be a clean CLI error (non-zero exit,
// clear message on err), never an uncaught std::invalid_argument/terminate —
// and this must hold even under --dry-run (no broker contact needed to fail).
TEST(RuneCliOffline, AlterRejectsNonNumericExpectedVersion) {
    fs::path f = fs::temp_directory_path()/"alter_bad_expected_version.rql";
    { std::ofstream o(f); o << "alter topic characters drop column nickname;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run(
        {"schema","alter", f.string(), "--dry-run", "--expected-version", "foo"}, out, err);
    EXPECT_NE(rc, 0);
    EXPECT_NE(err.str().find("--expected-version"), std::string::npos) << "got: " << err.str();
}

// An out-of-range --expected-version (> 65535, the uint16_t ceiling) must
// also be a clean error, not a silent truncation or a crash.
TEST(RuneCliOffline, AlterRejectsOutOfRangeExpectedVersion) {
    fs::path f = fs::temp_directory_path()/"alter_oor_expected_version.rql";
    { std::ofstream o(f); o << "alter topic characters drop column nickname;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run(
        {"schema","alter", f.string(), "--dry-run", "--expected-version", "99999999"}, out, err);
    EXPECT_NE(rc, 0);
    EXPECT_NE(err.str().find("--expected-version"), std::string::npos) << "got: " << err.str();
}

// A negative --expected-version must also be a clean error.
TEST(RuneCliOffline, AlterRejectsNegativeExpectedVersion) {
    fs::path f = fs::temp_directory_path()/"alter_neg_expected_version.rql";
    { std::ofstream o(f); o << "alter topic characters drop column nickname;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run(
        {"schema","alter", f.string(), "--dry-run", "--expected-version", "-1"}, out, err);
    EXPECT_NE(rc, 0);
    EXPECT_NE(err.str().find("--expected-version"), std::string::npos) << "got: " << err.str();
}

// `rune schema alter` on a 'topic' (not 'alter topic') statement is an error.
TEST(RuneCliOffline, AlterRejectsTopicStatement) {
    fs::path f = fs::temp_directory_path()/"alter_wrong_kind.rql";
    { std::ofstream o(f);
      o << "topic characters { id int64 } partitions 1 key id;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","alter", f.string(), "--dry-run"}, out, err);
    EXPECT_NE(rc, 0);
    EXPECT_NE(err.str().find("alter topic"), std::string::npos) << "got: " << err.str();
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

// Regression: `apply`'s dispatch on parse()'s std::variant<TopicDef,
// AlterTopicDef> result must route EACH alternative to its own plan/path —
// a 'topic' file keeps producing a DefineTopic descriptor dump, an
// 'alter topic' file produces the alter-ops dump, and neither path is ever
// taken for the other kind of statement. This pins the Task 20 fix for the
// CLI build break (parse() used to return a bare TopicDef).
TEST(RuneCliOfflineRegression, ApplyDispatchesOnParseVariant) {
    fs::path topicFile = fs::temp_directory_path()/"regression_apply_topic.rql";
    { std::ofstream o(topicFile);
      o << "topic characters { id int64, userId int64 } partitions 4 key userId;"; }
    fs::path alterFile = fs::temp_directory_path()/"regression_apply_alter.rql";
    { std::ofstream o(alterFile);
      o << "alter topic characters add column nickname string;"; }

    std::ostringstream topicOut, topicErr;
    int topicRc = rune::cli::run({"schema","apply", topicFile.string(), "--dry-run"},
                                  topicOut, topicErr);
    EXPECT_EQ(topicRc, 0) << "err=" << topicErr.str();
    EXPECT_NE(topicOut.str().find("schema characters"), std::string::npos)
        << "a 'topic' file must still produce a DefineTopic descriptor dump; got: "
        << topicOut.str();
    EXPECT_EQ(topicOut.str().find("alter topic"), std::string::npos)
        << "a 'topic' file must never be routed through the alter path; got: "
        << topicOut.str();

    std::ostringstream alterOut, alterErr;
    int alterRc = rune::cli::run({"schema","apply", alterFile.string(), "--dry-run"},
                                  alterOut, alterErr);
    EXPECT_EQ(alterRc, 0) << "err=" << alterErr.str();
    EXPECT_NE(alterOut.str().find("alter topic characters"), std::string::npos)
        << "an 'alter topic' file must be routed through the alter path; got: "
        << alterOut.str();
    EXPECT_EQ(alterOut.str().find("schema characters"), std::string::npos)
        << "an 'alter topic' file must never produce a DefineTopic descriptor dump; got: "
        << alterOut.str();
}

// Regression: a malformed/out-of-range --expected-version must never reach
// std::stoi uncaught (std::invalid_argument/out_of_range -> std::terminate).
// Pinned for BOTH `alter` (where the validation must run before --dry-run
// short-circuits, so an offline dry-run never crashes) and `apply` routed to
// an alter file (same flag, live-only code path but still guarded).
TEST(RuneCliOfflineRegression, ExpectedVersionMalformedIsCleanErrorNotCrash) {
    fs::path f = fs::temp_directory_path()/"regression_expected_version.rql";
    { std::ofstream o(f); o << "alter topic characters add column nickname string;"; }

    for (const std::string& bad : {std::string("foo"), std::string("99999999"), std::string("-1")}) {
        std::ostringstream out, err;
        int rc = rune::cli::run(
            {"schema","alter", f.string(), "--dry-run", "--expected-version", bad}, out, err);
        EXPECT_NE(rc, 0) << "bad value '" << bad << "' must be a non-zero exit";
        EXPECT_NE(err.str().find("--expected-version"), std::string::npos)
            << "bad value '" << bad << "' must print a clear error; got: " << err.str();
    }
}

// Regression: `rune schema alter` on an .rql file whose statement is 'topic'
// (not 'alter topic') must fail clearly rather than misinterpreting it.
TEST(RuneCliOfflineRegression, AlterRejectsTopicStatement) {
    fs::path f = fs::temp_directory_path()/"regression_alter_wrong_kind.rql";
    { std::ofstream o(f); o << "topic characters { id int64 } partitions 1 key id;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","alter", f.string(), "--dry-run"}, out, err);
    EXPECT_NE(rc, 0);
    EXPECT_NE(err.str().find("alter topic"), std::string::npos) << "got: " << err.str();
}

// Regression: `rune schema alter --dry-run` prints every op kind (ADD/DROP/
// RENAME/SET_COMPATIBILITY) so a reviewer can see the full plan before a
// live CAS-gated alter is issued.
TEST(RuneCliOfflineRegression, AlterDryRunPrintsAllOpKinds) {
    fs::path f = fs::temp_directory_path()/"regression_alter_all_ops.rql";
    { std::ofstream o(f);
      o << "alter topic characters "
           "add column nickname string, "
           "drop column legacy, "
           "rename column userId to playerId, "
           "set compatibility forward;"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","alter", f.string(), "--dry-run"}, out, err);
    EXPECT_EQ(rc, 0) << "err=" << err.str();
    const std::string o = out.str();
    EXPECT_NE(o.find("ADD"), std::string::npos)              << "got: " << o;
    EXPECT_NE(o.find("DROP"), std::string::npos)             << "got: " << o;
    EXPECT_NE(o.find("RENAME"), std::string::npos)           << "got: " << o;
    EXPECT_NE(o.find("userId -> playerId"), std::string::npos) << "got: " << o;
    EXPECT_NE(o.find("SET_COMPATIBILITY"), std::string::npos) << "got: " << o;
    EXPECT_NE(o.find("FORWARD"), std::string::npos)          << "got: " << o;
}

// F8: `schema apply` takes exactly one statement; a second one is an explicit parse
// error pointing at `rune ql -f`, never "apply the first and drop the rest".
TEST(RuneCliOfflineRegression, SchemaApplyRefusesAMultiStatementFileRegression) {
    fs::path f = fs::temp_directory_path()/"regression_apply_multi.rql";
    { std::ofstream o(f);
      o << "topic a { id int64 } partitions 1 key id;\ntopic b { id int64 } partitions 1 key id;\n"; }
    std::ostringstream out, err;
    int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
    EXPECT_NE(rc, 0);
    EXPECT_NE(err.str().find("rune ql -f"), std::string::npos) << "got: " << err.str();
    EXPECT_TRUE(out.str().empty()) << "nothing may be applied/printed; got: " << out.str();
}

// The new rune-ql statements are not `schema apply` statements (until Task 15's `rune ql`),
// and a topic feature apply would drop (untyped / replicas / with) is refused, not ignored.
TEST(RuneCliOfflineRegression, SchemaApplyRefusesStatementsItWouldDrop) {
    for (const char* src : {"drop topic a;", "show history;", "diff catalog offset 0 to 1;",
                            "alter topic a set retention.ms = 5;", "topic a partitions 1;",
                            "topic a { id int64 } partitions 1 replicas 3 key id;",
                            "topic a { id int64 } partitions 1 key id with (retention.ms = 5);"}) {
        fs::path f = fs::temp_directory_path()/"regression_apply_unsupported.rql";
        { std::ofstream o(f); o << src; }
        std::ostringstream out, err;
        int rc = rune::cli::run({"schema","apply", f.string(), "--dry-run"}, out, err);
        EXPECT_NE(rc, 0) << src;
        EXPECT_NE(err.str().find("use 'rune ql'"), std::string::npos) << src << " got: " << err.str();
        EXPECT_TRUE(out.str().empty()) << src;
    }
}
