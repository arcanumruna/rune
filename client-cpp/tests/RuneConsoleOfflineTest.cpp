// tests/RuneConsoleOfflineTest.cpp — Phase 2b Part D: `rune console` offline (--dry-run, no broker).
// Drives rune::cli::run(args, in, out, err) with an injected istream: no TTY needed.
// Also carries the migrated `rune schema apply|alter --dry-run` assertions (Task 5 table).
#include <gtest/gtest.h>

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "rune/cli/Cli.h"
#include "rune/cli/Console.h"

namespace fs = std::filesystem;

namespace {
struct ConsoleRun { int rc = -1; std::string out, err; };

ConsoleRun console(std::vector<std::string> args, const std::string& stdinText = {}) {
    args.insert(args.begin(), "console");
    std::istringstream in(stdinText);
    std::ostringstream out, err;
    const int rc = rune::cli::run(std::move(args), in, out, err);
    return {rc, out.str(), err.str()};
}

fs::path writeScript(const std::string& name, const std::string& body) {
    const fs::path dir = fs::temp_directory_path() / ("rune_console_" + std::to_string(::getpid()));
    fs::create_directories(dir);
    const fs::path p = dir / name;
    std::ofstream o(p, std::ios::binary);
    o << body;
    return p;
}

std::size_t count(const std::string& hay, const std::string& needle) {
    std::size_t n = 0;
    for (auto pos = hay.find(needle); pos != std::string::npos; pos = hay.find(needle, pos + needle.size())) ++n;
    return n;
}
} // namespace

// ── Spec §D gates ────────────────────────────────────────────────────────────

TEST(ConsoleOffline, ScriptRunsEveryStatementInOrder) {
    const fs::path f = writeScript("order.rql",
        "-- two topics and an alter\n"
        "topic player { id int64, name string } partitions 8 key id;\n"
        "alter topic player add column level int32;\n"
        "topic guild { id int64 } partitions 1 key id;\n");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 0) << r.err;
    const auto p = r.out.find("topic player partitions 8");
    const auto a = r.out.find("alter topic player");
    const auto g = r.out.find("topic guild partitions 1");
    ASSERT_NE(p, std::string::npos) << r.out;
    ASSERT_NE(a, std::string::npos) << r.out;
    ASSERT_NE(g, std::string::npos) << r.out;
    EXPECT_LT(p, a);
    EXPECT_LT(a, g);
    EXPECT_EQ(r.out.find("rune> "), std::string::npos) << "a script prints no prompts";
}

TEST(ConsoleOffline, InteractiveMultiLineStatementUsesContinuationPrompt) {
    const auto r = console({"--dry-run"},
        "topic characters {\n"
        "  id int64,\n"
        "  userId int64\n"
        "} partitions 4 key userId;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_TRUE(r.err.empty()) << r.err;
    EXPECT_EQ(count(r.out, "   ...> "), 3u) << r.out;
    EXPECT_EQ(count(r.out, "rune> "), 2u) << "first line + the prompt after execution; got " << r.out;
    EXPECT_EQ(count(r.out, "topic characters partitions 4"), 1u) << r.out;
}

TEST(ConsoleOffline, SeveralStatementsOnOneLine) {
    const auto r = console({"--dry-run"}, "topic a { x int32 } partitions 1 key x; topic b { x int32 } partitions 1 key x;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    const auto a = r.out.find("topic a partitions 1");
    const auto b = r.out.find("topic b partitions 1");
    ASSERT_NE(a, std::string::npos) << r.out;
    ASSERT_NE(b, std::string::npos) << r.out;
    EXPECT_LT(a, b);
}

TEST(ConsoleOffline, InteractiveParseErrorKeepsSession) {
    const auto r = console({"--dry-run"},
        "topic t { a int128 } partitions 1 key a;\n"
        "topic ok { a int32 } partitions 1 key a;\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_NE(r.err.find("1:13: "), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("int128"), std::string::npos) << r.err;
    EXPECT_NE(r.out.find("topic ok partitions 1"), std::string::npos) << "the session continues; got " << r.out;
}

TEST(ConsoleOffline, ExitQuitAndEofLeave) {
    for (const std::string leave : {"exit\n", "quit\n", "exit;\n", "  quit;  \n"}) {
        const auto r = console({"--dry-run"}, leave + "topic after { a int32 } partitions 1 key a;\n");
        EXPECT_EQ(r.rc, 0) << leave;
        EXPECT_EQ(r.out.find("topic after"), std::string::npos) << "'" << leave << "' ends the session; got " << r.out;
    }
    const auto eof = console({"--dry-run"}, "topic last { a int32 } partitions 1 key a;");
    EXPECT_EQ(eof.rc, 0);
    EXPECT_NE(eof.out.find("topic last partitions 1"), std::string::npos) << eof.out;
}

TEST(ConsoleOffline, DryRunMarksTransientTopic) {
    const auto r = console({"--dry-run"}, "transient topic ticks { id int64 } partitions 2 key id;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("persistence transient"), std::string::npos) << r.out;
}

TEST(ConsoleOffline, CatalogReadsAndDropsArePlannedToo) {
    // DD1: the console runs every statement rune ql runs.
    const auto r = console({"--dry-run"}, "show history;\ndrop topic old;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("show history"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("drop topic old"), std::string::npos) << r.out;
}

// ── Review Focus pins ────────────────────────────────────────────────────────

TEST(ConsoleOfflineRegression, SemicolonInsideCommentDoesNotEndStatement) {
    const auto r = console({"--dry-run"}, "topic t { a int32 } -- partitions come next;\npartitions 2 key a;\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_TRUE(r.err.empty()) << r.err;
    EXPECT_EQ(count(r.out, "   ...> "), 1u) << r.out;
    EXPECT_EQ(count(r.out, "topic t partitions 2"), 1u) << r.out;
}

TEST(ConsoleOfflineRegression, UnterminatedStatementIsReportedNotExecuted) {
    const fs::path f = writeScript("unterminated.rql", "topic a { x int32 } partitions 1 key x;\ntopic b { x int32 } partitions 1 key x\n");
    const auto s = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(s.rc, 1);
    EXPECT_NE(s.err.find(f.string() + ":statement 2: "), std::string::npos) << s.err;
    EXPECT_EQ(s.out.find("topic a partitions"), std::string::npos) << "nothing runs when the script does not parse; got " << s.out;

    const auto i = console({"--dry-run"}, "topic b { x int32 } partitions 1 key x\n");
    EXPECT_EQ(i.rc, 0);
    EXPECT_NE(i.err.find("missing ';'"), std::string::npos) << i.err;
    EXPECT_EQ(i.out.find("topic b partitions"), std::string::npos) << i.out;
}

TEST(ConsoleOfflineRegression, ScriptStopsAtFirstParseErrorAndIssuesNothing) {
    const fs::path f = writeScript("parse_error.rql",
        "topic a { x int32 } partitions 1 key x;\n\ntopic b { x int128 } partitions 1 key x;\ntopic c { x int32 } partitions 1 key x;\n");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 1);
    EXPECT_NE(r.err.find(f.string() + ":statement 2: 3:13: "), std::string::npos) << r.err;
    EXPECT_TRUE(r.out.empty()) << r.out;
}

TEST(ConsoleOfflineRegression, CrlfInputParsesAndExits) {
    const auto r = console({"--dry-run"}, "topic t { a int32 }\r\npartitions 1 key a;\r\nexit\r\ntopic after { a int32 } partitions 1 key a;\r\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_TRUE(r.err.empty()) << r.err;
    EXPECT_NE(r.out.find("topic t partitions 1"), std::string::npos) << r.out;
    EXPECT_EQ(r.out.find("topic after"), std::string::npos) << r.out;
}

TEST(ConsoleOfflineRegression, MissingValueAndUnknownArgumentAreUsageErrors) {
    const auto missing = console({"-f"});
    EXPECT_EQ(missing.rc, 2);
    EXPECT_NE(missing.err.find("-f needs a value"), std::string::npos) << missing.err;
    const auto unknown = console({"--file", "x.rql", "--dry-run"});
    EXPECT_EQ(unknown.rc, 2);
    EXPECT_NE(unknown.err.find("unknown option '--file'"), std::string::npos) << unknown.err;
    const auto stray = console({"script.rql"});
    EXPECT_EQ(stray.rc, 2);
    EXPECT_NE(stray.err.find("unexpected argument 'script.rql'"), std::string::npos) << stray.err;
    EXPECT_TRUE(stray.out.empty()) << "no prompt: the session never starts; got " << stray.out;
    const auto rid = console({"--request-id", "0000000000000001000000000000002a", "--dry-run"});
    EXPECT_EQ(rid.rc, 2) << "--request-id needs -f";
}

// ── Migrated from `rune schema apply|alter --dry-run` (RuneCliOfflineTest, Task 5 table) ──

TEST(ConsoleOfflineRegression, DryRunScriptPrintsDescriptor) {
    const fs::path f = writeScript("c.rql", "topic characters { id int64, userId int64, name string } partitions 16 key userId;");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("topic characters partitions 16"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("userId"), std::string::npos) << "the descriptor text is printed; got " << r.out;
}

TEST(ConsoleOfflineRegression, ScriptParseErrorHasFileLineColPrefix) {
    const fs::path f = writeScript("bad.rql", "topic t { a int128 } partitions 1 key a;");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 1);
    EXPECT_NE(r.err.find(f.string() + ":statement 1: 1:13: "), std::string::npos) << r.err;
}

TEST(ConsoleOfflineRegression, DryRunDispatchesEachStatementKind) {
    const auto t = console({"-f", writeScript("t.rql", "topic characters { id int64, userId int64 } partitions 4 key userId;").string(), "--dry-run"});
    EXPECT_EQ(t.rc, 0) << t.err;
    EXPECT_NE(t.out.find("topic characters partitions 4"), std::string::npos) << t.out;
    EXPECT_EQ(t.out.find("alter topic"), std::string::npos) << t.out;
    const auto a = console({"-f", writeScript("a.rql", "alter topic characters drop column nickname;").string(), "--dry-run"});
    EXPECT_EQ(a.rc, 0) << a.err;
    EXPECT_NE(a.out.find("alter topic characters"), std::string::npos) << a.out;
    EXPECT_NE(a.out.find("DROP nickname"), std::string::npos) << a.out;
}

TEST(ConsoleOfflineRegression, DryRunPrintsAllAlterOpKinds) {
    const fs::path f = writeScript("all_ops.rql",
        "alter topic characters add column nickname string, drop column legacy, "
        "rename column userId to playerId, set compatibility forward;");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ADD nickname"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("DROP legacy"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("RENAME userId -> playerId"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("SET_COMPATIBILITY forward"), std::string::npos) << r.out;
}

TEST(ConsoleOfflineRegression, ExpectedVersionMalformedIsCleanErrorNotCrash) {
    const fs::path f = writeScript("ev.rql", "alter topic characters add column nickname string;");
    for (const std::string bad : {"foo", "99999999", "-1"}) {
        const auto r = console({"-f", f.string(), "--dry-run", "--expected-version", bad});
        EXPECT_EQ(r.rc, 2) << bad;
        EXPECT_NE(r.err.find("--expected-version"), std::string::npos) << bad << ": " << r.err;
    }
}

// ── PollingFdLineSource (the TTY reader; a pipe stands in for the terminal) ────────────────────────────────────────

namespace {
void countIdle(void* ctx) noexcept { ++*static_cast<int*>(ctx); }
}

TEST(ConsolePollingSourceRegression, SplitsLinesStripsCrAndKeepsAnUnterminatedLastLine) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    int idle = 0;
    rune::cli::console::PollingFdLineSource src(fds[0], &countIdle, &idle);
    const std::string in = "one\r\ntwo\nlast";
    ASSERT_EQ(::write(fds[1], in.data(), in.size()), static_cast<ssize_t>(in.size()));
    ::close(fds[1]);
    std::string line;
    ASSERT_TRUE(src.next(line)); EXPECT_EQ(line, "one");
    ASSERT_TRUE(src.next(line)); EXPECT_EQ(line, "two");
    ASSERT_TRUE(src.next(line)); EXPECT_EQ(line, "last");
    EXPECT_FALSE(src.next(line));
    ::close(fds[0]);
}

TEST(ConsolePollingSourceRegression, PumpsTheClientWhileTheTerminalIsIdle) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    int idle = 0;
    rune::cli::console::PollingFdLineSource src(fds[0], &countIdle, &idle);
    std::thread writer([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        ASSERT_EQ(::write(fds[1], "go\n", 3), 3);
    });
    std::string line;
    ASSERT_TRUE(src.next(line));
    writer.join();
    EXPECT_EQ(line, "go");
    EXPECT_GE(idle, 4) << "a 50 ms poll between pumps over ~400 ms of silence";
    EXPECT_EQ(static_cast<uint64_t>(idle), src.idlePumps);
    ::close(fds[0]); ::close(fds[1]);
}

TEST(ConsolePollingSourceRegression, AnOverlongLineIsReportedAndDiscardedNotRunTogetherWithTheNextOne) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    int idle = 0;
    std::ostringstream err;
    rune::cli::console::PollingFdLineSource src(fds[0], &countIdle, &idle);
    src.err = &err;
    std::thread writer([&] {   // 100 KiB without a newline, then the line's end and a normal line
        const std::string chunk(4096, 'x');
        for (int i = 0; i < 25; ++i) {
            const char* p = chunk.data();
            std::size_t left = chunk.size();
            while (left > 0) { const ssize_t n = ::write(fds[1], p, left); if (n <= 0) return; p += n; left -= static_cast<std::size_t>(n); }
        }
        const std::string tail = "yyy\nnext;\n";
        (void)!::write(fds[1], tail.data(), tail.size());
        ::close(fds[1]);
    });
    std::string line;
    ASSERT_TRUE(src.next(line));
    EXPECT_TRUE(line.empty());
    EXPECT_NE(err.str().find("input line too long"), std::string::npos) << err.str();
    ASSERT_TRUE(src.next(line));
    EXPECT_EQ(line, "next;") << "the rest of the long line is dropped, the next line is intact";
    EXPECT_FALSE(src.next(line));
    writer.join();
    ::close(fds[0]);
}
