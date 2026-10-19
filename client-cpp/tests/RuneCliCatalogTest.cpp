// tests/RuneCliCatalogTest.cpp
//
// Catalog history Task 15 — `rune ql` and `rune catalog export | replay | diff`, OFFLINE:
// --dry-run, .rcat files, and the pure row / step-check functions.  Nothing here blocks on a
// broker (the real CLI against real brokers lives in the broker repo's
// libs/node/tests/client/RuneCliBrokerRegressionTest.cpp).
#include "rune/cli/Cli.h"
#include "rune/cli/CatalogCommands.h"

#include <rune/catalog/CatalogFile.h>
#include <rune/catalog/CatalogQueries.h>
#include <rune/catalog/testing/GoldenScript.h>
#include <rune/client/view/ViewPlanAuthor.h>

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unistd.h>

namespace fs = std::filesystem;
namespace cat = rune::catalog;

namespace {

fs::path tmpDir() {
    static const fs::path d = [] {
        fs::path p = fs::temp_directory_path() / ("rune_cli_catalog_test_" + std::to_string(::getpid()));
        fs::create_directories(p);
        return p;
    }();
    return d;
}

std::string writeTemp(const std::string& name, const std::string& content) {
    const fs::path p = tmpDir() / name;
    std::ofstream o(p, std::ios::binary);
    o << content;
    return p.string();
}

std::string writeRcatFile(const std::string& name, const std::vector<cat::CatalogEvent>& log) {
    const fs::path p = tmpDir() / name;
    std::ofstream o(p, std::ios::binary);
    cat::writeRcatHeader(o);
    int64_t off = 0;
    for (const auto& e : log) {
        const auto bytes = cat::encode(e);
        EXPECT_TRUE(bytes.has_value());
        cat::writeRcatRecord(o, off++, std::span<const uint8_t>(bytes->data(), bytes->size()));
    }
    return p.string();
}

int runCli(std::vector<std::string> args, std::string* out = nullptr, std::string* err = nullptr) {
    std::stringstream o, e;
    const int rc = rune::cli::run(std::move(args), o, e);
    if (out) *out = o.str();
    if (err) *err = e.str();
    return rc;
}

// An endpoint nothing listens on: any command that contacts it would block until its connect timeout.
constexpr const char* kUnreachable = "127.0.0.1:1";

} // namespace

// ── the brief's cases ───────────────────────────────────────────────────────

TEST(RuneCliCatalogRegression, QlDryRunParsesWithoutABroker) {
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions 2 replicas 3 with (tier = leader);", "--dry-run"}, out, err), 0);
    EXPECT_NE(out.str().find("replicas 3"), std::string::npos);
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions;", "--dry-run"}, out, err), 1);
}

TEST(RuneCliCatalogRegression, QlFileDryRunNamesTheFailingStatementIndexRegression) {
    // F8: -f is a script; a bad statement is named by its index, never skipped.
    const auto f = writeTemp("three.rql", "topic a partitions 1;\ntopic b partitions;\ntopic c partitions 1;\n");
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-f", f, "--dry-run"}, out, err), 1);
    EXPECT_NE(err.str().find("statement 2"), std::string::npos) << err.str();
    std::stringstream o2, e2;
    const auto g = writeTemp("ok.rql", "topic a partitions 1;\ntopic b partitions 2;\n");
    ASSERT_EQ(rune::cli::run({"ql", "-f", g, "--dry-run"}, o2, e2), 0) << e2.str();
    EXPECT_NE(o2.str().find("topic a"), std::string::npos);
    EXPECT_NE(o2.str().find("topic b"), std::string::npos) << "every statement is planned, not just the first";
}

TEST(RuneCliCatalogRegression, RequestIdFlagIsValidated) {
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "topic t partitions 1;", "--request-id", "xyz", "--dry-run"}, out, err), 2);
    EXPECT_NE(err.str().find("--request-id"), std::string::npos);
    const auto id = rune::cli::parseRequestId("0000000000000001000000000000002a");
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(*id, (rune::catalog::RequestId{1, 42}));
}

TEST(RuneCliCatalogRegression, HistoryRowPrintsReplayedFromRegression) {
    // F18: Task 24 reads "replayedFrom a@<offset>" from `show history`.
    auto log = rune::catalog::testing::goldenScriptV1();
    rune::catalog::RecordedEvent r{7, log[7]};
    EXPECT_NE(rune::cli::formatHistoryRow(r).find(" - "), std::string::npos) << "no replayedFrom: '-'";
    r.event.replayedFrom = rune::catalog::ReplayedFrom{.clusterId = "a", .offset = 7, .principal = "cli@x"};
    EXPECT_NE(rune::cli::formatHistoryRow(r).find("a@7"), std::string::npos);
}

TEST(RuneCliCatalogRegression, ReplayStepMismatchNamesTheSourceOffset) {
    auto log = rune::catalog::testing::goldenScriptV1();
    rune::catalog::RecordedEvent src{6, log[6]}, tgt{40, log[6]};
    tgt.event.replayedFrom = rune::catalog::ReplayedFrom{.clusterId = "src", .offset = 6};
    EXPECT_FALSE(rune::cli::verifyReplayStep(src, tgt).has_value());
    tgt.event.fingerprint ^= 1;
    const auto m = rune::cli::verifyReplayStep(src, tgt);
    ASSERT_TRUE(m.has_value());
    EXPECT_NE(m->find("replay aborted at source offset 6"), std::string::npos) << *m;
}

TEST(RuneCliCatalogRegression, DiffOfTwoFilesReportsDifferencesWithExit3) {
    const auto full = writeRcatFile("full.rcat", rune::catalog::testing::goldenScriptV1());
    auto shorter = rune::catalog::testing::goldenScriptV1();
    shorter.resize(12);
    const auto part = writeRcatFile("part.rcat", shorter);
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", full, "--to", part}, out, err), 3);
    EXPECT_NE(out.str().find("players"), std::string::npos);
    std::stringstream o2, e2;
    EXPECT_EQ(rune::cli::run({"catalog", "diff", "--from", full, "--to", full}, o2, e2), 0);
}

// ── edge case 1: exit codes ─────────────────────────────────────────────────

TEST(RuneCliCatalogRegression, ExitCodesAreZeroOneTwoAndDiffOnlyThree) {
    std::string out, err;
    // 0: success
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 1;", "--dry-run"}, &out, &err), 0) << err;
    // 1: an explicit error with a message on stderr
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions;", "--dry-run"}, &out, &err), 1);
    EXPECT_FALSE(err.empty());
    EXPECT_EQ(runCli({"ql", "-f", (tmpDir() / "no_such_file.rql").string(), "--dry-run"}, &out, &err), 1);
    EXPECT_NE(err.find("cannot open"), std::string::npos) << err;
    EXPECT_EQ(runCli({"catalog", "diff", "--from", (tmpDir() / "no_such.rcat").string(), "--to",
                      (tmpDir() / "no_such.rcat").string()}, &out, &err), 1);
    EXPECT_FALSE(err.empty());
    // 2: usage
    EXPECT_EQ(runCli({"ql"}, &out, &err), 2);
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 1;", "-f", "x"}, &out, &err), 2) << "-e and -f together";
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 1;", "--no-such-flag"}, &out, &err), 2);
    EXPECT_NE(err.find("--no-such-flag"), std::string::npos);
    EXPECT_EQ(runCli({"ql", "-e"}, &out, &err), 2) << "a flag without its value";
    EXPECT_EQ(runCli({"catalog"}, &out, &err), 2);
    EXPECT_EQ(runCli({"catalog", "frobnicate"}, &out, &err), 2);
    EXPECT_EQ(runCli({"catalog", "diff", "--from", "a.rcat"}, &out, &err), 2) << "--to missing";
    // 3: only catalog diff, only for differences
    const auto full = writeRcatFile("e1_full.rcat", cat::testing::goldenScriptV1());
    auto shorter = cat::testing::goldenScriptV1();
    shorter.resize(5);
    const auto part = writeRcatFile("e1_part.rcat", shorter);
    EXPECT_EQ(runCli({"catalog", "diff", "--from", full, "--to", part}, &out, &err), 3);
    EXPECT_EQ(runCli({"catalog", "diff", "--from", full, "--to", full}, &out, &err), 0);
}

// ── edge case 2: ql -f ──────────────────────────────────────────────────────

TEST(RuneCliCatalogRegression, QlFileEmptyScriptIsExitZeroAndContactsNoBroker) {
    const auto f = writeTemp("empty.rql", "");
    const auto onlyComments = writeTemp("comments.rql", "-- nothing here\n\n   \n-- at all\n");
    for (const auto& path : {f, onlyComments}) {
        const auto t0 = std::chrono::steady_clock::now();
        std::string out, err;
        EXPECT_EQ(runCli({"ql", "-f", path, "--broker", kUnreachable}, &out, &err), 0) << err;
        EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(3)) << "an empty script never connects";
        EXPECT_TRUE(out.empty()) << out;
        EXPECT_TRUE(err.empty()) << err;
    }
}

TEST(RuneCliCatalogRegression, QlFileReadsStdinForDash) {
    std::istringstream in("topic a partitions 1;\ntopic b partitions 2;\n");
    std::streambuf* old = std::cin.rdbuf(in.rdbuf());
    std::string out, err;
    const int rc = runCli({"ql", "-f", "-", "--dry-run"}, &out, &err);
    std::cin.rdbuf(old);
    ASSERT_EQ(rc, 0) << err;
    EXPECT_NE(out.find("topic a"), std::string::npos);
    EXPECT_NE(out.find("topic b"), std::string::npos);

    std::istringstream bad("topic a partitions 1;\nnope;\n");
    old = std::cin.rdbuf(bad.rdbuf());
    const int rc2 = runCli({"ql", "-f", "-", "--dry-run"}, &out, &err);
    std::cin.rdbuf(old);
    EXPECT_EQ(rc2, 1);
    EXPECT_NE(err.find("statement 2"), std::string::npos) << err;
}

TEST(RuneCliCatalogRegression, QlFileNothingIsIssuedWhenAnyStatementFailsToParse) {
    // Every statement is parsed and planned before the first is issued: a bad statement 3 means
    // statements 1 and 2 are not issued either (an unreachable broker proves it: no connect, no wait).
    const auto f = writeTemp("bad3.rql", "topic a partitions 1;\ntopic b partitions 1;\ntopic c partitions;\ntopic d partitions 1;\n");
    const auto t0 = std::chrono::steady_clock::now();
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "-f", f, "--broker", kUnreachable}, &out, &err), 1);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(3));
    EXPECT_NE(err.find("statement 3: "), std::string::npos) << err;
    EXPECT_EQ(err.find("statement 4"), std::string::npos) << err;
}

// ── edge case 3: -e with two statements ─────────────────────────────────────

TEST(RuneCliCatalogRegression, QlExpressionWithTwoStatementsIsAUsageErrorNeverRunsTheFirst) {
    std::string out, err;
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_EQ(runCli({"ql", "-e", "topic a partitions 1; topic b partitions 1;", "--broker", kUnreachable}, &out, &err), 2);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(3)) << "the first statement was not issued";
    EXPECT_NE(err.find("exactly one statement"), std::string::npos) << err;
    EXPECT_TRUE(out.empty()) << out;
    EXPECT_EQ(runCli({"ql", "-e", "", "--dry-run"}, &out, &err), 1) << "no statement at all is an explicit error";
    EXPECT_FALSE(err.empty());
}

// ── edge case 4: --request-id ───────────────────────────────────────────────

TEST(RuneCliCatalogRegression, RequestIdTakesExactly32HexDigitsEitherCase) {
    using rune::cli::parseRequestId;
    const auto lower = parseRequestId("0000000000abcdef000000000000002a");
    const auto upper = parseRequestId("0000000000ABCDEF000000000000002A");
    const auto mixed = parseRequestId("0000000000aBcDeF000000000000002A");
    ASSERT_TRUE(lower && upper && mixed);
    EXPECT_EQ(*lower, *upper);
    EXPECT_EQ(*lower, *mixed);
    EXPECT_EQ(*lower, (cat::RequestId{0xabcdefull, 0x2aull}));
    EXPECT_EQ(*parseRequestId("ffffffffffffffffffffffffffffffff"), (cat::RequestId{~0ull, ~0ull}));

    EXPECT_FALSE(parseRequestId(""));
    EXPECT_FALSE(parseRequestId("0000000000000001000000000000002")) << "31 digits";
    EXPECT_FALSE(parseRequestId("0000000000000001000000000000002a0")) << "33 digits";
    EXPECT_FALSE(parseRequestId("0000000000000001000000000000002g")) << "a non-hex letter";
    EXPECT_FALSE(parseRequestId("0x00000000000001000000000000002a0")) << "a 0x prefix";
    EXPECT_FALSE(parseRequestId("0000000000000001 00000000000002a")) << "a blank inside";
    EXPECT_FALSE(parseRequestId("-000000000000001000000000000002a")) << "a sign";

    std::string out, err;
    for (const char* bad : {"", "xyz", "0000000000000001000000000000002", "0000000000000001000000000000002a0",
                            "0000000000000001000000000000002g"}) {
        EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 1;", "--request-id", bad, "--dry-run"}, &out, &err), 2) << bad;
        EXPECT_NE(err.find("--request-id"), std::string::npos) << bad;
    }
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 1;", "--request-id", "0000000000ABCDEF000000000000002A", "--dry-run"},
                     &out, &err), 0) << err;
    EXPECT_NE(out.find("0000000000abcdef000000000000002a"), std::string::npos) << out;
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 1;", "--request-id", "00000000000000000000000000000000", "--dry-run"},
                     &out, &err), 2) << "{0,0} means 'none': it would silently drop the idempotency asked for";
}

TEST(RuneCliCatalogRegression, RequestIdOfStatementNIsLoPlusNMinusOneAndOverflowIsRefused) {
    const auto f = writeTemp("ids.rql", "topic a partitions 1;\ntopic b partitions 1;\ntopic c partitions 1;\n");
    std::string out, err;
    ASSERT_EQ(runCli({"ql", "-f", f, "--request-id", "000000000000000a0000000000000010", "--dry-run"}, &out, &err), 0) << err;
    const auto p10 = out.find("000000000000000a0000000000000010");
    const auto p11 = out.find("000000000000000a0000000000000011");
    const auto p12 = out.find("000000000000000a0000000000000012");
    ASSERT_NE(p10, std::string::npos) << out;
    ASSERT_NE(p11, std::string::npos) << out;
    ASSERT_NE(p12, std::string::npos) << out;
    EXPECT_LT(p10, p11);
    EXPECT_LT(p11, p12);

    // lo = 0xFFFF…FF: one statement fits, a second would wrap — refused, explicitly, nothing issued.
    const auto one = writeTemp("ids1.rql", "topic a partitions 1;\n");
    EXPECT_EQ(runCli({"ql", "-f", one, "--request-id", "0000000000000001ffffffffffffffff", "--dry-run"}, &out, &err), 0) << err;
    EXPECT_EQ(runCli({"ql", "-f", f, "--request-id", "0000000000000001ffffffffffffffff", "--dry-run"}, &out, &err), 1);
    EXPECT_NE(err.find("overflow"), std::string::npos) << err;
    EXPECT_NE(err.find("--request-id"), std::string::npos) << err;
    EXPECT_TRUE(out.empty()) << "no plan is printed for a refused script: " << out;
    // ... and exactly at the edge: 3 statements from FFFF…FD use …FD, …FE, …FF.
    EXPECT_EQ(runCli({"ql", "-f", f, "--request-id", "0000000000000001fffffffffffffffd", "--dry-run"}, &out, &err), 0) << err;
    EXPECT_NE(out.find("0000000000000001ffffffffffffffff"), std::string::npos) << out;
}

// ── edge case 5: versions above 65535 ───────────────────────────────────────

TEST(RuneCliCatalogRegression, VersionsAbove65535AreRefusedNotTruncated) {
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "-e", "show topic t as of version 65535;", "--dry-run"}, &out, &err), 0) << err;
    EXPECT_EQ(runCli({"ql", "-e", "show schema t as of version 65536;", "--dry-run"}, &out, &err), 1);
    EXPECT_NE(err.find("65536"), std::string::npos) << err;
    EXPECT_NE(err.find("65535"), std::string::npos) << err;
    EXPECT_EQ(runCli({"ql", "-e", "show view v as of version 4294967297;", "--dry-run"}, &out, &err), 1)
        << "2^32 + 1 must not wrap to version 1";
    EXPECT_EQ(runCli({"ql", "-e", "diff schema t version 1 to 65535;", "--dry-run"}, &out, &err), 0) << err;
    EXPECT_EQ(runCli({"ql", "-e", "diff schema t version 1 to 65536;", "--dry-run"}, &out, &err), 1);
    EXPECT_NE(err.find("65536"), std::string::npos) << err;
    EXPECT_EQ(runCli({"ql", "-e", "diff schema t version 70000 to 1;", "--dry-run"}, &out, &err), 1);
    // topic / view object versions are u32 (every AlterTopicConfig bumps them): only the u32 limit applies
    EXPECT_EQ(runCli({"ql", "-e", "show topic t as of version 70000;", "--dry-run"}, &out, &err), 0) << err;
    EXPECT_EQ(runCli({"ql", "-e", "show topic t as of version 4294967296;", "--dry-run"}, &out, &err), 1);
    EXPECT_NE(err.find("4294967296"), std::string::npos) << err;
    EXPECT_EQ(runCli({"ql", "-e", "show view v as of version 70000;", "--dry-run"}, &out, &err), 0) << err;
    // offsets are not versions: a catalog diff takes any offset
    EXPECT_EQ(runCli({"ql", "-e", "diff catalog offset 1 to 100000;", "--dry-run"}, &out, &err), 0) << err;
    // -f: named by index, nothing after
    const auto f = writeTemp("ver.rql", "show topic t as of version 1;\nshow schema t as of version 65536;\n");
    EXPECT_EQ(runCli({"ql", "-f", f, "--dry-run"}, &out, &err), 1);
    EXPECT_NE(err.find("statement 2: "), std::string::npos) << err;
}

// ── edge case 6: --dry-run never contacts a broker ──────────────────────────

TEST(RuneCliCatalogRegression, DryRunNeverContactsAnUnreachableBroker) {
    const auto t0 = std::chrono::steady_clock::now();
    std::string out, err;
    EXPECT_EQ(runCli({"ql", "-e", "topic t partitions 2 replicas 3;", "--dry-run", "--broker", "203.0.113.9:40123"}, &out, &err), 0)
        << err;
    EXPECT_EQ(runCli({"ql", "-e", "show history topic t;", "--dry-run", "--broker", kUnreachable}, &out, &err), 0) << err;
    EXPECT_EQ(runCli({"ql", "-e", "drop topic t;", "--dry-run", "--broker", kUnreachable}, &out, &err), 0) << err;
    EXPECT_NE(out.find("drop topic t"), std::string::npos) << "the plan is printed: " << out;
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(3));
}

// ── edge case 7: --verbose prints nothing extra by itself ───────────────────

TEST(RuneCliCatalogRegression, VerboseDryRunAddsNothingToStderr) {
    std::string o1, e1, o2, e2;
    ASSERT_EQ(runCli({"ql", "-e", "topic t partitions 2;", "--dry-run"}, &o1, &e1), 0);
    ASSERT_EQ(runCli({"ql", "-e", "topic t partitions 2;", "--dry-run", "--verbose"}, &o2, &e2), 0);
    EXPECT_EQ(o1, o2);
    EXPECT_TRUE(e1.empty());
    EXPECT_TRUE(e2.empty()) << "no redirect / retry happened: nothing is printed: " << e2;
}

// ── edge case 8: history rows (F18) ─────────────────────────────────────────

TEST(RuneCliCatalogRegression, HistoryRowColumnsAreStableAndEmptyFieldsAreDashes) {
    auto log = cat::testing::goldenScriptV1();
    cat::RecordedEvent r{4, log[4]};   // golden offset 4 carries replayedFrom
    ASSERT_TRUE(r.event.replayedFrom.has_value());
    const std::string with = rune::cli::formatHistoryRow(r);
    EXPECT_NE(with.find(r.event.replayedFrom->clusterId + "@" + std::to_string(r.event.replayedFrom->offset)), std::string::npos)
        << with;
    EXPECT_NE(with.find("AlterSchema"), std::string::npos) << with;
    EXPECT_NE(with.find("players("), std::string::npos) << with;
    EXPECT_NE(with.find("\xE2\x86\x92"), std::string::npos) << "version prev->new: " << with;
    EXPECT_EQ(with.find('\n'), std::string::npos);

    cat::RecordedEvent bare{5, log[5]};
    bare.event.replayedFrom.reset();
    bare.event.statement.clear();
    const std::string without = rune::cli::formatHistoryRow(bare);
    // columns are separated by two spaces; the replayedFrom and the statement columns are '-'
    EXPECT_NE(without.find("  -  -"), std::string::npos) << without;
    EXPECT_EQ(without.substr(without.size() - 3), "  -") << without;

    bare.event.statement = "alter topic players\nadd column x int64;";
    EXPECT_EQ(rune::cli::formatHistoryRow(bare).find('\n'), std::string::npos) << "one row is one line";
}

// ── edge case 9: export ─────────────────────────────────────────────────────

TEST(RuneCliCatalogRegression, ExportUsageErrors) {
    std::string out, err;
    EXPECT_EQ(runCli({"catalog", "export", "--up-to", "-1", "--broker", kUnreachable}, &out, &err), 2);
    EXPECT_NE(err.find("--up-to"), std::string::npos);
    EXPECT_EQ(runCli({"catalog", "export", "--up-to", "abc", "--broker", kUnreachable}, &out, &err), 2);
    EXPECT_EQ(runCli({"catalog", "export", "--up-to", "5x", "--broker", kUnreachable}, &out, &err), 2);
    EXPECT_EQ(runCli({"catalog", "export", "--up-to", "99999999999999999999", "--broker", kUnreachable}, &out, &err), 2);
    EXPECT_TRUE(out.empty()) << "nothing is written for a refused command";
    EXPECT_EQ(runCli({"catalog", "export"}, &out, &err), 2) << "--broker is required";
}

// ── edge case 10: replay (offline parts) ────────────────────────────────────

TEST(RuneCliCatalogRegression, ReplayRequestIdIsDeterministicFnv1aOfTheClusterIdAndTheOffset) {
    using rune::cli::replayRequestId;
    EXPECT_EQ(replayRequestId("", 0), (cat::RequestId{0xcbf29ce484222325ull, 0}));
    EXPECT_EQ(replayRequestId("a", 7), (cat::RequestId{0xaf63dc4c8601ec8cull, 7}));
    EXPECT_EQ(replayRequestId("src", 3), replayRequestId("src", 3));
    EXPECT_NE(replayRequestId("src", 3), replayRequestId("src", 4));
    EXPECT_NE(replayRequestId("src", 3), replayRequestId("dst", 3));
    EXPECT_FALSE(replayRequestId("src", 0).none());
}

TEST(RuneCliCatalogRegression, ReplayStepCheckRefusesEveryDeviation) {
    auto log = cat::testing::goldenScriptV1();
    cat::RecordedEvent src{6, log[6]};
    cat::RecordedEvent ok{40, log[6]};
    ok.event.replayedFrom = cat::ReplayedFrom{.clusterId = "src", .offset = 6};
    EXPECT_FALSE(rune::cli::verifyReplayStep(src, ok));

    auto none = ok;
    none.event.replayedFrom.reset();
    auto m = rune::cli::verifyReplayStep(src, none);
    ASSERT_TRUE(m);
    EXPECT_NE(m->find("replay aborted at source offset 6"), std::string::npos) << *m;
    EXPECT_NE(m->find("replayedFrom"), std::string::npos) << *m;

    auto wrongOffset = ok;
    wrongOffset.event.replayedFrom->offset = 5;
    m = rune::cli::verifyReplayStep(src, wrongOffset);
    ASSERT_TRUE(m);
    EXPECT_NE(m->find("replay aborted at source offset 6"), std::string::npos) << *m;

    auto other = ok;
    other.event.replayedFrom->clusterId = "other";
    EXPECT_FALSE(rune::cli::verifyReplayStep(src, ok, "src"));
    m = rune::cli::verifyReplayStep(src, other, "src");
    ASSERT_TRUE(m);
    EXPECT_NE(m->find("replays cluster 'other'"), std::string::npos) << *m;

    auto wrongKind = ok;
    wrongKind.event.kind = cat::EventKind::DropTopic;
    m = rune::cli::verifyReplayStep(src, wrongKind);
    ASSERT_TRUE(m);

    auto wrongFp = ok;
    wrongFp.event.fingerprint ^= 0x8000000000000000ull;
    m = rune::cli::verifyReplayStep(src, wrongFp);
    ASSERT_TRUE(m);
    EXPECT_NE(m->find("target fingerprint 0x"), std::string::npos) << *m;
    EXPECT_NE(m->find("!= source 0x"), std::string::npos) << *m;
    EXPECT_NE(m->find("SetCompatibility 'players'"), std::string::npos) << *m;
}

TEST(RuneCliCatalogRegression, ReplayOfATamperedSourceIssuesNothingAndNamesTheOffset) {
    // The source is verified BEFORE anything is issued: --into is unreachable, so reaching it would
    // cost a connect timeout; the verification error comes back at once, exit 1, naming the offset.
    auto log = cat::testing::goldenScriptV1();
    std::get<cat::CreateTopicOps>(log[1].ops).partitions += 1;   // the stored fingerprint no longer matches
    const auto bad = writeRcatFile("tampered.rcat", log);
    const auto t0 = std::chrono::steady_clock::now();
    std::string out, err;
    EXPECT_EQ(runCli({"catalog", "replay", "--from", bad, "--into", kUnreachable}, &out, &err), 1);
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(3));
    EXPECT_NE(err.find("offset 1"), std::string::npos) << err;
    EXPECT_NE(err.find("nothing was issued"), std::string::npos) << err;
    EXPECT_TRUE(out.empty());

    // A flipped bit in the file (CRC): the record is named by its offset, nothing issued either.
    const auto good = writeRcatFile("good.rcat", cat::testing::goldenScriptV1());
    {
        std::fstream f(good, std::ios::in | std::ios::out | std::ios::binary);
        f.seekg(0, std::ios::end);
        const auto size = f.tellg();
        f.seekp(size / 2);
        char c = 0;
        f.seekg(size / 2);
        f.get(c);
        f.seekp(size / 2);
        f.put(static_cast<char>(c ^ 0x40));
    }
    EXPECT_EQ(runCli({"catalog", "replay", "--from", good, "--into", kUnreachable}, &out, &err), 1);
    EXPECT_NE(err.find("offset"), std::string::npos) << err;
    EXPECT_EQ(runCli({"catalog", "diff", "--from", good, "--to", good}, &out, &err), 1) << "diff verifies its sources too";
    EXPECT_NE(err.find("offset"), std::string::npos) << err;
}

TEST(RuneCliCatalogRegression, ReplayUsageErrors) {
    std::string out, err;
    EXPECT_EQ(runCli({"catalog", "replay", "--from", "a.rcat"}, &out, &err), 2) << "--into missing";
    EXPECT_EQ(runCli({"catalog", "replay", "--into", kUnreachable}, &out, &err), 2) << "--from missing";
    EXPECT_EQ(runCli({"catalog", "replay", "--from", "a.rcat", "--into", kUnreachable, "--up-to", "-3"}, &out, &err), 2);
    EXPECT_EQ(runCli({"catalog", "replay", "--from", "a.rcat", "--into", kUnreachable, "--bogus", "1"}, &out, &err), 2);
    EXPECT_EQ(runCli({"catalog", "replay", "--from", "a.rcat", "--into", kUnreachable, "--cluster-id", ""}, &out, &err), 2);
    EXPECT_NE(err.find("--cluster-id"), std::string::npos) << err;
}

// ── edge case 11: diff ──────────────────────────────────────────────────────

TEST(RuneCliCatalogRegression, DiffOfIdenticalCatalogsPrintsNothingAndOfDifferentOnesPrintsEveryDifference) {
    const auto a = writeRcatFile("d_a.rcat", cat::testing::goldenScriptV1());
    const auto a2 = writeRcatFile("d_a2.rcat", cat::testing::goldenScriptV1());
    std::string out, err;
    EXPECT_EQ(runCli({"catalog", "diff", "--from", a, "--to", a2}, &out, &err), 0) << err;
    EXPECT_TRUE(out.empty()) << out;
    EXPECT_TRUE(err.empty()) << err;

    auto shorter = cat::testing::goldenScriptV1();
    shorter.resize(10);
    const auto b = writeRcatFile("d_b.rcat", shorter);
    ASSERT_EQ(runCli({"catalog", "diff", "--from", a, "--to", b}, &out, &err), 3) << err;
    // every difference, computed independently by the library
    cat::CatalogReplayer ra, rb;
    int64_t off = 0;
    for (const auto& e : cat::testing::goldenScriptV1()) ASSERT_TRUE(ra.feedDecoded(off++, e));
    off = 0;
    for (const auto& e : shorter) ASSERT_TRUE(rb.feedDecoded(off++, e));
    const auto want = cat::diff(ra.state(), rb.state());
    ASSERT_FALSE(want.empty());
    std::size_t lines = 0;
    for (char c : out) lines += (c == '\n');
    EXPECT_EQ(lines, want.size()) << out;
    for (const auto& d : want) EXPECT_NE(out.find(rune::cli::formatDiffEntry(d)), std::string::npos) << out;
    // the direction matters: b -> a swaps added and removed
    std::string rev;
    ASSERT_EQ(runCli({"catalog", "diff", "--from", b, "--to", a}, &rev, &err), 3);
    EXPECT_NE(rev, out);
}

TEST(RuneCliCatalogRegression, FormatDiffEntryAndDefinitionsRenderTheGoldenState) {
    cat::CatalogReplayer r;
    int64_t off = 0;
    for (const auto& e : cat::testing::goldenScriptV1()) ASSERT_TRUE(r.feedDecoded(off++, e));
    const auto* players = r.state().topicByName("players");
    ASSERT_NE(players, nullptr);
    const std::string topic = rune::cli::formatTopicDefinition(*players);
    EXPECT_NE(topic.find("topic players"), std::string::npos) << topic;
    EXPECT_NE(topic.find("partitions"), std::string::npos) << topic;
    EXPECT_NE(topic.find("config.retentionMs"), std::string::npos) << topic;
    EXPECT_NE(rune::cli::formatSchemaDefinition(*players).find("schema players"), std::string::npos);
    const auto* view = r.state().viewByName("ev_view");
    ASSERT_NE(view, nullptr);
    const std::string v = rune::cli::formatViewDefinition(*view);
    EXPECT_NE(v.find("view ev_view"), std::string::npos) << v;
    EXPECT_NE(v.find("sources"), std::string::npos) << v;

    cat::DiffEntry added{cat::DiffEntry::Change::Added, cat::ObjectType::Topic, "x", 0, 1, {}};
    cat::DiffEntry removed{cat::DiffEntry::Change::Removed, cat::ObjectType::View, "y", 1, 0, {}};
    cat::DiffEntry changed{cat::DiffEntry::Change::Changed, cat::ObjectType::Topic, "z", 1, 2, {"version", "config.retentionMs"}};
    EXPECT_EQ(rune::cli::formatDiffEntry(added), "+ topic x");
    EXPECT_EQ(rune::cli::formatDiffEntry(removed), "- view y");
    EXPECT_EQ(rune::cli::formatDiffEntry(changed), "~ topic z: version, config.retentionMs");
}

// ── edge case 12: every error is explicit ───────────────────────────────────

TEST(RuneCliCatalogRegression, EveryOfflineFailurePrintsAMessageAndExitsNonZero) {
    const std::string garbage = writeTemp("garbage.rcat", "this is not a catalog file");
    const std::string truncated = [] {
        std::ostringstream o;
        cat::writeRcatHeader(o);
        const auto bytes = cat::encode(cat::testing::goldenScriptV1()[0]);
        cat::writeRcatRecord(o, 0, std::span<const uint8_t>(bytes->data(), bytes->size()));
        const std::string s = o.str();
        return writeTemp("truncated.rcat", s.substr(0, s.size() - 5));
    }();
    for (const auto& args : std::vector<std::vector<std::string>>{
             {"catalog", "diff", "--from", garbage, "--to", garbage},
             {"catalog", "diff", "--from", truncated, "--to", truncated},
             {"catalog", "diff", "--from", (tmpDir() / "missing.rcat").string(), "--to", garbage},
             {"catalog", "replay", "--from", garbage, "--into", kUnreachable},
             {"catalog", "replay", "--from", (tmpDir() / "missing.rcat").string(), "--into", kUnreachable},
             {"ql", "-f", (tmpDir() / "missing.rql").string(), "--dry-run"},
             {"ql", "-e", "alter topic t set retention.ms = x;", "--dry-run"},
             {"ql", "-e", "drop;", "--dry-run"},
         }) {
        std::string out, err;
        const int rc = runCli(args, &out, &err);
        EXPECT_EQ(rc, 1) << args[0] << " " << args[1] << " -> " << err;
        EXPECT_FALSE(err.empty()) << args[0] << " " << args[1] << " printed no error";
        EXPECT_TRUE(out.empty()) << out;
    }
}

TEST(RuneCliCatalogRegression, RcatFileWrittenByTheCliTestHelpersRoundTripsThroughReadRcat) {
    const auto path = writeRcatFile("rt.rcat", cat::testing::goldenScriptV1());
    std::ifstream f(path, std::ios::binary);
    const auto recs = cat::readRcat(f);
    ASSERT_TRUE(recs.has_value());
    EXPECT_EQ(recs->size(), cat::testing::goldenScriptV1().size());
}

// ── the view-plan helpers replay relies on ──────────────────────────────────

TEST(RuneCliCatalogRegression, AuthoredViewPlanRoundTripsAndRemapsItsSources) {
    namespace rv = rune::client::view;
    rv::ViewPlan p;
    p.viewName = "v";
    p.key = "id";
    p.sources.push_back({0, "id"});
    p.sources.push_back({1, "pid"});
    p.output.push_back({"id", rune::codec::PrimType::Int64, 0, "id", rv::MergeRule::Replace});
    p.output.push_back({"n", rune::codec::PrimType::Int64, 1, "pid", rv::MergeRule::Sum});
    p.persistence = 1;
    const auto blob = rv::serializeAuthored(p);
    auto back = rv::deserializeAuthored(blob);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(rv::serializeAuthored(*back), blob) << "byte-identical round trip";

    const std::vector<int32_t> ids = {7, 9};
    ASSERT_TRUE(rv::remapSources(*back, ids));
    EXPECT_EQ(back->sources[0].topicId, 7);
    EXPECT_EQ(back->sources[1].topicId, 9);
    const std::vector<int32_t> wrong = {1};
    EXPECT_FALSE(rv::remapSources(*back, wrong)) << "a source-count mismatch is refused, the plan untouched";
    EXPECT_EQ(back->sources[0].topicId, 7);

    // strict: truncation at EVERY length, trailing bytes, a bad version, a bad rule
    for (std::size_t n = 0; n < blob.size(); ++n)
        EXPECT_FALSE(rv::deserializeAuthored(std::span<const uint8_t>(blob.data(), n))) << "truncated to " << n;
    auto trailing = blob;
    trailing.push_back(0);
    EXPECT_FALSE(rv::deserializeAuthored(trailing));
    const std::size_t ruleAt = blob.size() - 18;   // rule | persistence(1) chunkBytes(4) capacity(4) maxRows(8)
    const std::size_t typeAt = blob.size() - 28;   // type | sourceIdx(2) srcField "pid"(4+3) rule(1) + 17
    ASSERT_EQ(blob[ruleAt], static_cast<uint8_t>(rv::MergeRule::Sum)) << "layout anchor";
    ASSERT_EQ(blob[typeAt], static_cast<uint8_t>(rune::codec::PrimType::Int64)) << "layout anchor";
    auto badRule = blob; badRule[ruleAt] = 0xFF; EXPECT_FALSE(rv::deserializeAuthored(badRule));
    auto badType = blob; badType[typeAt] = 0xFF; EXPECT_FALSE(rv::deserializeAuthored(badType));
    auto badVersion = blob;
    badVersion[0] = 9;
    EXPECT_FALSE(rv::deserializeAuthored(badVersion));
    EXPECT_FALSE(rv::deserializeAuthored(std::span<const uint8_t>{}));
}

// ── fix round 1, R1: the bounded read-your-writes wait ──────────────────────

TEST(RuneCliCatalogRegression, AwaitRequestVisibleRetriesUntilVisibleAndIsBoundedRegression) {
    using rune::cli::awaitRequestVisible;
    std::string why;
    {   // (a) visible on the third look: three refreshes, two pauses
        int refreshes = 0, pauses = 0, lookups = 0;
        int64_t t = 0;
        const auto r = awaitRequestVisible(
            [&]() -> std::optional<std::string> { ++refreshes; return std::nullopt; },
            [&]() -> std::optional<cat::RequestRecord> {
                if (++lookups <= 2) return std::nullopt;
                cat::RequestRecord rec;
                rec.offset = 5;
                return rec;
            },
            [&] { return t += 1; }, [&] { ++pauses; }, 1000, why);
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(r->offset, 5);
        EXPECT_EQ(refreshes, 3);
        EXPECT_EQ(pauses, 2);
    }
    {   // (b) never visible: ends at the deadline, after at least one attempt
        int refreshes = 0;
        int64_t t = 0;
        const auto r = awaitRequestVisible(
            [&]() -> std::optional<std::string> { ++refreshes; EXPECT_LT(refreshes, 100) << "the deadline must end the loop";
                                                   return refreshes >= 100 ? std::optional<std::string>("runaway") : std::nullopt; },
            []() -> std::optional<cat::RequestRecord> { return std::nullopt; }, [&] { return t += 10; }, [] {}, 25, why);
        EXPECT_FALSE(r.has_value());
        EXPECT_EQ(why, "not visible before the deadline");
        EXPECT_EQ(refreshes, 3);
    }
    {   // (c) a failing refresh ends it at once, with its own message, before any lookup
        int lookups = 0;
        const auto r = awaitRequestVisible(
            []() -> std::optional<std::string> { return std::string("boom"); },
            [&]() -> std::optional<cat::RequestRecord> { ++lookups; return std::nullopt; }, [] { return int64_t{0}; }, [] {}, 1000, why);
        EXPECT_FALSE(r.has_value());
        EXPECT_EQ(why, "boom");
        EXPECT_EQ(lookups, 0);
    }
}

TEST(RuneCliCatalogRegression, QlExpectedVersionIsValidatedAndShownInTheDryRunPlanRegression) {
    std::stringstream out, err;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "alter topic t add column c int32;", "--expected-version", "x", "--dry-run"}, out, err), 2);
    EXPECT_NE(err.str().find("--expected-version"), std::string::npos) << err.str();
    std::stringstream o2, e2;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "alter topic t add column c int32;", "--expected-version", "70000", "--dry-run"}, o2, e2), 2);
    std::stringstream o3, e3;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "alter topic t add column c int32;", "--expected-version", "5", "--dry-run"}, o3, e3), 0)
        << e3.str();
    EXPECT_NE(o3.str().find("expected version 5"), std::string::npos) << o3.str();
}

TEST(RuneCliCatalogRegression, QlValidateIsShownInTheDryRunPlanRegression) {
    std::stringstream out, err;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic t { id int64 } partitions 1 key id;", "--validate", "--dry-run"}, out, err), 0)
        << err.str();
    EXPECT_NE(out.str().find("validate"), std::string::npos) << out.str();
}
