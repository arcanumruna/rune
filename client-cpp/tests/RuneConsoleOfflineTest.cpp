// tests/RuneConsoleOfflineTest.cpp — Phase 2b Part D: `rune console` offline (--dry-run, no broker).
// Drives rune::cli::run(args, in, out, err) with an injected istream: no TTY needed.
// Also carries the migrated `rune schema apply|alter --dry-run` assertions (Task 5 table).
#include <gtest/gtest.h>

#include <fcntl.h>
#include <pty.h>
#include <poll.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <termios.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "rune/cli/Cli.h"
#include "rune/cli/Console.h"
#include "rune/cli/SecurityCommands.h"
#include <rune/client/detail/SecureZero.h>
#include "rune/ql/Parser.h"

#include <rune/client/Completion.h>
#include <rune/protocol/client/Protocol.h>
#include "QlEngine.h"   // src/cli: splitStatements, the engine's internal seams
#include "support/AllocCounter.h"
#include "support/InvokerGateway.h"   // C20 review (#690): a fake gateway for the live console

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
    // #313: the descriptor name line, and every field's NAME and TYPE (not only that the text mentions a name).
    std::istringstream lines(r.out);
    std::string line;
    bool sawNameLine = false;
    std::vector<std::pair<std::string, std::string>> fields;   // (name, type) in print order
    while (std::getline(lines, line)) {
        std::istringstream tok(line);
        std::string a, b;
        tok >> a >> b;
        if (a == "schema") {
            EXPECT_EQ(b, "characters") << "the descriptor name line names the topic: " << line;
            EXPECT_NE(line.find(" version=1 {"), std::string::npos) << line;
            sawNameLine = true;
        } else if (line.rfind("  ", 0) == 0 && !a.empty() && !b.empty()) {
            fields.emplace_back(a, b);
        }
    }
    EXPECT_TRUE(sawNameLine) << "no `schema characters ...` line: " << r.out;
    const std::vector<std::pair<std::string, std::string>> want{{"id", "int64"}, {"userId", "int64"}, {"name", "string"}};
    EXPECT_EQ(fields, want) << "field names and types, in order: " << r.out;
}

TEST(ConsoleOfflineRegression, ScriptParseErrorHasFileLineColPrefix) {
    const fs::path f = writeScript("bad.rql", "topic t { a int128 } partitions 1 key a;");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 1);
    EXPECT_NE(r.err.find(f.string() + ":statement 1: 1:13: "), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("type"), std::string::npos) << "the message names the unknown type: " << r.err;
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
    EXPECT_EQ(a.out.find("topic characters partitions"), std::string::npos) << "an alter never prints a DefineTopic descriptor: " << a.out;
    const auto c = console({"-f", writeScript("c2.rql", "alter topic characters add column nickname string, set compatibility backward;").string(), "--dry-run"});
    EXPECT_EQ(c.rc, 0) << c.err;
    EXPECT_NE(c.out.find("ADD nickname"), std::string::npos) << c.out;
    EXPECT_NE(c.out.find("SET_COMPATIBILITY backward"), std::string::npos) << c.out;
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

namespace {
struct IdleWriter { int count = 0; int fd = -1; };   // the 4th idle pump writes the line: no sleeping writer thread
void idleThenWrite(void* ctx) noexcept {
    auto* w = static_cast<IdleWriter*>(ctx);
    if (++w->count == 4) { [[maybe_unused]] const ssize_t n = ::write(w->fd, "go\n", 3); }
}
}

TEST(ConsolePollingSourceRegression, PumpsTheClientWhileTheTerminalIsIdle) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    IdleWriter w{0, fds[1]};
    rune::cli::console::PollingFdLineSource src(fds[0], &idleThenWrite, &w);
    std::string line;
    ASSERT_TRUE(src.next(line));
    EXPECT_EQ(line, "go");
    EXPECT_EQ(w.count, 4) << "the source pumps on every idle poll until input arrives, then returns the line";
    EXPECT_EQ(static_cast<uint64_t>(w.count), src.idlePumps);
    ::close(fds[0]); ::close(fds[1]);
}

namespace {
int g_eagainReads = 0;
ssize_t eagainThenRead(int fd, void* b, std::size_t n) {   // #672: first read reports EAGAIN, then the real one
    if (g_eagainReads++ == 0) { errno = EAGAIN; return -1; }
    return ::read(fd, b, n);
}
}

TEST(ConsolePollingSourceRegression, EagainOnANonBlockingFdIsNotEndOfInputRegression) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    int idle = 0;
    rune::cli::console::PollingFdLineSource src(fds[0], &countIdle, &idle);
    g_eagainReads = 0;
    src.setReadFnForTest(&eagainThenRead);
    ASSERT_EQ(::write(fds[1], "line\n", 5), 5);   // poll reports POLLIN, the first read then says EAGAIN
    std::string line;
    ASSERT_TRUE(src.next(line)) << "EAGAIN is a spurious wakeup, not end of input";
    EXPECT_EQ(line, "line");
    EXPECT_GE(src.idlePumps, 1u);
    EXPECT_GE(g_eagainReads, 2);
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
    EXPECT_TRUE(src.lastLineDiscarded) << "Issue #307: the placeholder is flagged";
    EXPECT_NE(err.str().find("input line too long"), std::string::npos) << err.str();
    ASSERT_TRUE(src.next(line));
    EXPECT_EQ(line, "next;") << "the rest of the long line is dropped, the next line is intact";
    EXPECT_FALSE(src.lastLineDiscarded);
    EXPECT_FALSE(src.next(line));
    writer.join();
    ::close(fds[0]);
}

// ── #314: paths the console tests never exercised ────────────────────────────────────────────────────────────────

namespace {
/// RUNE_BROKER / RUNE_USER are cleared for a test (a developer shell or CI may set them) and restored after.
struct EnvCleared {
    std::vector<std::pair<std::string, std::string>> saved;
    explicit EnvCleared(std::initializer_list<const char*> names) {
        for (const char* n : names) {
            if (const char* v = std::getenv(n); v != nullptr) saved.emplace_back(n, v);
            ::unsetenv(n);
        }
    }
    ~EnvCleared() { for (const auto& [k, v] : saved) ::setenv(k.c_str(), v.c_str(), 1); }
};
}

TEST(RuneConsoleOfflineRegression, ScriptFromStdinRegression) {
    // `-f -` reads the script from the injected stdin; nothing else is read, and no prompt is printed.
    const auto r = console({"-f", "-", "--dry-run"},
                           "topic stdinTopic { id int64, label string } partitions 3 key id;\n"
                           "drop topic gone;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_TRUE(r.err.empty()) << r.err;
    EXPECT_NE(r.out.find("topic stdinTopic partitions 3"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("schema stdinTopic "), std::string::npos) << "the descriptor name line: " << r.out;
    EXPECT_NE(r.out.find("drop topic gone"), std::string::npos) << r.out;
    EXPECT_EQ(r.out.find("rune> "), std::string::npos) << "a script prints no prompts: " << r.out;
    const auto empty = console({"-f", "-", "--dry-run"}, "");
    EXPECT_EQ(empty.rc, 0) << "an empty stdin script is an empty plan";
    EXPECT_TRUE(empty.out.empty()) << empty.out;
    const auto bad = console({"-f", "-", "--dry-run"}, "topic t { a int128 } partitions 1 key a;\n");
    EXPECT_EQ(bad.rc, 1);
    EXPECT_NE(bad.err.find("-:statement 1: 1:13: "), std::string::npos) << "stdin is named '-': " << bad.err;
}

TEST(RuneConsoleOfflineRegression, ValidateIsPlannedOnADryRunAndNeedsABrokerOtherwiseRegression) {
    const fs::path f = writeScript("validate.rql", "topic v { id int64 } partitions 1 key id;\nalter topic v add column a int32;\n");
    const auto plain = console({"-f", f.string(), "--dry-run"});
    ASSERT_EQ(plain.rc, 0) << plain.err;
    EXPECT_NE(plain.out.find("topic v partitions 1 replicas 0 persistence disk key id\n"), std::string::npos)
        << "without --validate the define line ends at the key: " << plain.out;
    const auto v = console({"-f", f.string(), "--dry-run", "--validate"});
    ASSERT_EQ(v.rc, 0) << v.err;
    EXPECT_NE(v.out.find("topic v partitions 1 replicas 0 persistence disk key id validate\n"), std::string::npos)
        << "--validate marks the define in the plan: " << v.out;
    EXPECT_EQ(count(v.out, " validate"), 1u) << "only the define carries it, not the alter: " << v.out;

    // No --dry-run and no broker: a usage error (2), nothing runs.
    EnvCleared env{"RUNE_BROKER"};
    const auto live = console({"-f", f.string(), "--validate"});
#ifdef RUNE_CLI_LIVE_ENABLED
    EXPECT_EQ(live.rc, 2) << live.err;
    EXPECT_NE(live.err.find("--broker host:port is required (or --dry-run)"), std::string::npos) << live.err;
#else
    EXPECT_EQ(live.rc, 1) << live.err;
    EXPECT_NE(live.err.find("live mode requires RUNE_CLI_LIVE_ENABLED"), std::string::npos) << live.err;
#endif
    EXPECT_TRUE(live.out.empty()) << live.out;
}

TEST(RuneConsoleOfflineRegression, ExpectedVersionIsCarriedIntoTheDryRunPlanRegression) {
    const fs::path f = writeScript("ev5.rql",
        "topic characters { id int64 } partitions 1 key id;\n"
        "alter topic characters add column nickname string;\n"
        "alter topic characters set compatibility forward;\n");
    const auto r = console({"-f", f.string(), "--dry-run", "--expected-version", "5"});
    ASSERT_EQ(r.rc, 0) << r.err;
    EXPECT_EQ(count(r.out, "  expected version 5\n"), 2u) << "both alters carry the gate: " << r.out;
    const auto topicLine = r.out.find("topic characters partitions 1");
    const auto firstGate = r.out.find("expected version 5");
    ASSERT_NE(topicLine, std::string::npos) << r.out;
    EXPECT_LT(topicLine, r.out.find("alter topic characters")) << "the define comes first and has no gate: " << r.out;
    EXPECT_LT(r.out.find("alter topic characters"), firstGate);
    EXPECT_EQ(r.out.substr(0, r.out.find("alter topic characters")).find("expected version"), std::string::npos)
        << "a `topic` define carries no expected version: " << r.out;
    const auto none = console({"-f", f.string(), "--dry-run"});
    ASSERT_EQ(none.rc, 0) << none.err;
    EXPECT_EQ(none.out.find("expected version"), std::string::npos) << "default 0 = no check, nothing printed: " << none.out;
    const auto top = console({"-f", f.string(), "--dry-run", "--expected-version", "65535"});
    EXPECT_EQ(top.rc, 0) << top.err;
    EXPECT_NE(top.out.find("expected version 65535"), std::string::npos) << "the u16 maximum is accepted: " << top.out;
    // Interactive input carries it too.
    const auto i = console({"--dry-run", "--expected-version", "7"}, "alter topic characters add column x int32;\n");
    EXPECT_NE(i.out.find("expected version 7"), std::string::npos) << i.out;
}

TEST(RuneConsoleOfflineRegression, RequestIdIsPrintedPerStatementOnADryRunRegression) {
    const fs::path f = writeScript("rid.rql",
        "topic a { x int32 } partitions 1 key x;\ntopic b { x int32 } partitions 1 key x;\ndrop topic c;\n");
    const auto r = console({"-f", f.string(), "--dry-run", "--request-id", "0000000000000001000000000000002a"});
    ASSERT_EQ(r.rc, 0) << r.err;
    const auto a = r.out.find("request-id 0000000000000001000000000000002a\n");
    const auto b = r.out.find("request-id 0000000000000001000000000000002b\n");
    const auto c = r.out.find("request-id 0000000000000001000000000000002c\n");
    ASSERT_NE(a, std::string::npos) << "statement 1 uses the id itself: " << r.out;
    ASSERT_NE(b, std::string::npos) << "statement 2 uses lo + 1: " << r.out;
    ASSERT_NE(c, std::string::npos) << "statement 3 uses lo + 2: " << r.out;
    EXPECT_LT(r.out.find("topic a partitions"), a);
    EXPECT_LT(a, r.out.find("topic b partitions"));
    EXPECT_LT(r.out.find("topic b partitions"), b);
    EXPECT_LT(b, r.out.find("drop topic c"));
    EXPECT_LT(r.out.find("drop topic c"), c);
    EXPECT_EQ(count(r.out, "request-id "), 3u) << r.out;
    const auto none = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(none.out.find("request-id"), std::string::npos) << "no id without the flag: " << none.out;
    // Refusals: not hex, wrong length, all zero (and "needs -f", pinned by MissingValueAndUnknownArgument...).
    for (const std::string bad : {"zz", "2a", "00000000000000000000000000000000"}) {
        const auto e = console({"-f", f.string(), "--dry-run", "--request-id", bad});
        EXPECT_EQ(e.rc, 2) << bad;
        EXPECT_NE(e.err.find("--request-id must be 32 hex digits, not all zero"), std::string::npos) << bad << ": " << e.err;
        EXPECT_TRUE(e.out.empty()) << bad << ": " << e.out;
    }
}

TEST(RuneConsoleOfflineRegression, SplitterAndParserAgreeOnTrickyInputsRegression) {
    // prepareAll's count-mismatch branch ("internal error: statement splitter found N statements, the parser M") is a
    // defence: the splitter and the parser must agree on the statement count for every input the parser ACCEPTS.  No
    // black-box input reaches the mismatch, so pin the agreement instead, on the inputs where a hand-written splitter
    // would drift: ';' and '--' inside strings, ';' in comments, quotes in comments, CRLF, several per line, no tail.
    const std::pair<std::string, std::size_t> inputs[] = {
        {"create user u with password 'a;b';\ndrop topic z;\n", 2},
        {"create user u with password 'a -- b; c';\ndrop topic z;\n", 2},
        {"drop topic a; -- tail ; comment\n", 1},
        {"drop topic a; -- it's\ndrop topic b;\n", 2},
        {"-- it's;\ndrop topic b; -- 'unterminated\ndrop topic c;\n", 2},
        {"drop topic a;\r\ndrop topic b;\r\n", 2},
        {"show history topic a; show history topic b;\n", 2},
        {"drop topic a\n;\n", 1},
        {"-- only a comment; with a semicolon\n", 0},
        {"\n\n", 0},
        {"", 0},
    };
    for (const auto& [src, want] : inputs) {
        const auto parsed = rune::ql::parseScript(src);
        ASSERT_TRUE(parsed.has_value()) << src;
        const auto texts = rune::cli::engine::splitStatements(src);
        EXPECT_EQ(parsed->size(), want) << src;
        EXPECT_EQ(texts.size(), parsed->size()) << "splitter vs parser: " << src;
        for (const auto& t : texts) {   // each split text is itself exactly one statement
            const auto one = rune::ql::parseScript(t);
            ASSERT_TRUE(one.has_value()) << t;
            EXPECT_EQ(one->size(), 1u) << t;
        }
        const auto r = console({"-f", "-", "--dry-run"}, src);
        EXPECT_EQ(r.rc, 0) << src << ": " << r.err;
        EXPECT_EQ(r.err.find("internal error"), std::string::npos) << src << ": " << r.err;
    }
    // The error path next to it: a script the parser refuses never reaches the splitter and runs nothing (rc 1).
    const auto bad = console({"-f", "-", "--dry-run"}, "drop topic a;\ndrop topic b;;\n");
    EXPECT_EQ(bad.rc, 1);
    EXPECT_NE(bad.err.find("-:statement 3: 2:14: "), std::string::npos) << "the stray ; is statement 3: " << bad.err;
    EXPECT_EQ(bad.err.find("internal error"), std::string::npos) << bad.err;
    EXPECT_TRUE(bad.out.empty()) << "nothing runs: " << bad.out;
}

TEST(RuneConsoleOfflineRegression, UsageNamesEveryAcceptedConsoleFlagRegression) {
    // The top-level usage (`rune` with no arguments) lists every console flag the parser accepts (#314); the
    // console's own usage (a bad flag) agrees.
    static const char* const kFlags[] = {"--broker", "--cli-egress", "-f", "--dry-run", "--validate", "--expected-version",
                                         "--request-id", "--verbose", "--user", "--password-file", "--tls-ca",
                                         "--tls-host", "--insecure-plaintext"};
    std::ostringstream out, err;
    ASSERT_EQ(rune::cli::run({}, out, err), 1);
    const std::string all = err.str();
    const auto from = all.find("  rune console");
    const auto to = all.find("  rune schema get");
    ASSERT_NE(from, std::string::npos) << all;
    ASSERT_NE(to, std::string::npos) << all;
    const std::string top = all.substr(from, to - from);
    const auto own = console({"--no-such-flag"});
    ASSERT_EQ(own.rc, 2);
    for (const char* f : kFlags) {
        EXPECT_NE(top.find(f), std::string::npos) << f << " missing from the top-level console usage:\n" << top;
        EXPECT_NE(own.err.find(f), std::string::npos) << f << " missing from the console's own usage:\n" << own.err;
    }
    // And each listed flag really is accepted by the parser (never "unknown option").
    for (const char* f : {"--dry-run", "--validate", "--verbose", "--insecure-plaintext"}) {
        const auto r = console({"--dry-run", f}, "");
        EXPECT_EQ(r.err.find("unknown option"), std::string::npos) << f << ": " << r.err;
    }
    for (const char* f : {"--broker", "--cli-egress", "--user", "--password-file", "--tls-ca", "--tls-host"}) {
        const auto r = console({"--dry-run", f, "x"}, "");
        EXPECT_EQ(r.err.find("unknown option"), std::string::npos) << f << ": " << r.err;
    }
}

#ifdef RUNE_CLI_LIVE_ENABLED
// ── #315: an empty --cli-data-dir is a private scratch directory ──────────────────────────────────────────────────

TEST(QlEngineRegression, EmptyCliDataDirIsScratchRegression) {
    namespace support = rune::cli::support;
    fs::path seen;
    {
        support::ScopedDir scratch;
        const std::string dir = support::resolveDataDir("", scratch);   // what `--cli-data-dir ""` and an absent flag give
        ASSERT_FALSE(dir.empty()) << "an empty value must never reach ClientConfig::dataDir as ''";
        EXPECT_EQ(scratch.path().string(), dir) << "the scope owns the directory it returned";
        EXPECT_EQ(fs::path(dir).parent_path(), fs::temp_directory_path()) << dir;
        EXPECT_FALSE(fs::exists(dir)) << "fresh: nothing exists there yet";
        seen = dir;
        fs::create_directories(seen / "sub");   // what the embedded driver would leave behind
        { std::ofstream(seen / "sub" / "cnc.dat") << "x"; }
        support::ScopedDir other;
        EXPECT_NE(support::resolveDataDir("", other), dir) << "every client gets its own scratch directory";
        // A non-empty value is the caller's directory, used as is, and owned by nobody.
        support::ScopedDir none;
        EXPECT_EQ(support::resolveDataDir("/some/dir", none), "/some/dir");
        EXPECT_TRUE(none.path().empty());
    }
    EXPECT_FALSE(fs::exists(seen)) << "removed, with its contents, when the scope ends";
}
#endif

#ifdef RUNE_CLI_LIVE_ENABLED
// ── C20 adversarial review (#690): `-f` exits with the session-end code, not the failed statement's ──────────────────
namespace {
/// A fake gateway on its own thread, on the console client's embedded driver: it answers the first DropTopic with
/// NotAuthorized (45) on its correlation id and, in the same pump, revokes the session (ErrorResponse{SessionRevoked,
/// corr 0}).  Nothing is shared with the console thread but the file system and the stop pipe.
struct RevokingGateway {
    std::string dataDir;
    int32_t     stream;
    int         stop[2] = {-1, -1};
    int         drops = 0;      // gateway thread only; read after join
    bool        started = false;
    std::thread t;
    RevokingGateway(std::string dir, int32_t s) : dataDir(std::move(dir)), stream(s) {
        EXPECT_EQ(::pipe(stop), 0);
        t = std::thread([this] { run(); });
    }
    bool stopRequested() const {
        pollfd p{stop[0], POLLIN, 0};
        return ::poll(&p, 1, 0) > 0;
    }
    void run() {
        const fs::path cnc = fs::path(dataDir) / "aeron" / "cnc.dat";
        for (long i = 0; i < 100'000'000 && !fs::exists(cnc); ++i) {   // the console's client starts the driver
            if (stopRequested()) return;
            ::sched_yield();
        }
        if (!fs::exists(cnc)) return;
        test_support::InvokerGateway gw(dataDir + "/aeron", "aeron:ipc", stream, "aeron:ipc", stream + 1);
        started = true;
        namespace pc = rune::protocol::client;
        gw.templateHandlers[pc::DropTopic::TEMPLATE_ID] = [&](int from, const uint8_t* f, std::size_t len) {
            const auto m = pc::DropTopic::decode(f, len);
            if (!m) return;
            ++drops;
            gw.sendError(from, static_cast<int16_t>(pc::ErrorCode::NotAuthorized), "no drop", m->sessionId, m->correlationId);
            gw.revokeSession(m->sessionId, from);
        };
        for (long i = 0; i < 2'000'000'000 && !stopRequested(); ++i) {
            if (gw.pump() == 0) ::sched_yield();
        }
    }
    ~RevokingGateway() {
        (void)!::write(stop[1], "x", 1);
        if (t.joinable()) t.join();
        ::close(stop[0]); ::close(stop[1]);
    }
};
} // namespace

// #690: statement 1 fails NotAuthorized (45) and the session is revoked (46) in the same pump.  runBatch's own status
// is the statement's (45); the script must exit with the session-end code (46), like the interactive console - and
// statement 2 never runs.  Mutation: the script branch returns runBatch's status -> rc 45.
TEST(RuneConsoleOfflineRegression, ScriptExitsWithTheSessionEndCodeNotTheFailedStatementsAdversarialRegression) {
    const fs::path dir = fs::temp_directory_path() / ("c20r_690_" + std::to_string(::getpid()));
    fs::remove_all(dir);
    const fs::path script = writeScript("c20r_690.rql", "drop topic first;\ndrop topic second;\n");
    std::istringstream in;
    std::ostringstream out, err;
    int rc = -1;
    int drops = -1;
    bool started = false;
    {
        RevokingGateway gw(dir.string(), 6900);
        rc = rune::cli::run({"console", "-f", script.string(), "--broker", "aeron:ipc", "--cli-egress", "aeron:ipc",
                             "--cli-stream-in", "6900", "--cli-stream-out", "6901", "--cli-data-dir", dir.string()},
                            in, out, err);
        (void)!::write(gw.stop[1], "x", 1);
        if (gw.t.joinable()) gw.t.join();
        drops = gw.drops;
        started = gw.started;
    }
    fs::remove_all(dir);
    fs::remove(script);
    ASSERT_TRUE(started) << "vacuity guard: the gateway came up. err: " << err.str();
    EXPECT_EQ(drops, 1) << "statement 2 never reached the broker: " << err.str();
    EXPECT_NE(err.str().find("NotAuthorized (45)"), std::string::npos) << "vacuity guard: statement 1 failed 45: " << err.str();
    EXPECT_EQ(count(err.str(), "SessionRevoked (46): the session ended"), 1u) << err.str();
    EXPECT_EQ(rc, 46) << "the script exits with the session-end code, not the statement's 45: " << err.str();
}
#endif

// ── Adversarial review of PR #669 (#311): every process stdin now goes through PollingFdLineSource ─────────────────
// A pipe, a regular file and /dev/null; a line completed across idle pumps; a signal storm while blocked.  No sleeps:
// input arrives from the idle callback itself, and every drive has a hard cap that ends the input (EOF) instead of
// hanging.

namespace {
/// Idle callback: on pump N writes the next chunk; after the last chunk closes the writer (EOF).  Past `cap` pumps it
/// closes the writer anyway (the test then fails on its assertions, never hangs).
struct ChunkFeeder {
    int fd = -1;
    std::vector<std::pair<int, std::string>> chunks;   // (pump number, bytes)
    std::size_t next = 0;
    int pumps = 0;
    int cap = 1000;
    bool capped = false;
    void closeWriter() { if (fd >= 0) { ::close(fd); fd = -1; } }
};
void feedOnIdle(void* ctx) noexcept {
    auto* f = static_cast<ChunkFeeder*>(ctx);
    ++f->pumps;
    while (f->next < f->chunks.size() && f->chunks[f->next].first == f->pumps) {
        const std::string& s = f->chunks[f->next++].second;
        [[maybe_unused]] const ssize_t n = ::write(f->fd, s.data(), s.size());
    }
    if (f->next == f->chunks.size() || f->pumps >= f->cap) {
        f->capped = f->next != f->chunks.size();
        f->closeWriter();
    }
}
} // namespace

TEST(ConsolePollingSourceRegression, PartialLineCompletedAcrossIdlePumpsAdversarialRegression) {
    // A pipe that delivers a statement in fragments separated by gaps (`slow-producer | rune console`): the partial
    // line is held across the idle pumps and returned whole; a CRLF split between two writes, a final line with no
    // newline, then EOF.  The source keeps pumping while it holds a partial line.
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    ChunkFeeder f;
    f.fd = fds[1];
    f.chunks = {{2, "topic a parti"}, {4, "tions 1;\r"}, {5, "\nsecond"}, {8, " line\nthird"}};
    rune::cli::console::PollingFdLineSource src(fds[0], &feedOnIdle, &f);
    std::string line;
    ASSERT_TRUE(src.next(line));
    EXPECT_EQ(line, "topic a partitions 1;") << "a line split across idle gaps (and a split CRLF) is joined";
    EXPECT_GE(f.pumps, 5);
    ASSERT_TRUE(src.next(line));
    EXPECT_EQ(line, "second line");
    ASSERT_TRUE(src.next(line));
    EXPECT_EQ(line, "third") << "a last line without a newline is still returned at EOF";
    EXPECT_FALSE(src.next(line));
    EXPECT_FALSE(src.next(line)) << "EOF is sticky";
    EXPECT_FALSE(f.capped) << "every chunk was delivered by the idle pump";
    EXPECT_EQ(src.idlePumps, static_cast<uint64_t>(f.pumps));
    ::close(fds[0]);
}

TEST(ConsolePollingSourceRegression, RegularFileAndDevNullStdinEndCleanlyAdversarialRegression) {
    // `rune console < script.rql` and `rune console < /dev/null`: poll(2) reports a regular file always readable, so
    // the source must read it to EOF without an idle pump, and end (false) - never spin on a ready-but-empty fd.
    const fs::path p = writeScript("stdin_file.rql", "a\r\nb\n\nc");
    const int fd = ::open(p.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    int idle = 0;
    rune::cli::console::PollingFdLineSource src(fd, &countIdle, &idle);
    std::string line;
    std::vector<std::string> got;
    for (int i = 0; i < 10 && src.next(line); ++i) got.push_back(line);
    EXPECT_EQ(got, (std::vector<std::string>{"a", "b", "", "c"}));
    EXPECT_FALSE(src.next(line));
    EXPECT_EQ(src.idlePumps, 0u);
    ::close(fd);

    const int nul = ::open("/dev/null", O_RDONLY);
    ASSERT_GE(nul, 0);
    rune::cli::console::PollingFdLineSource empty(nul, &countIdle, &idle);
    EXPECT_FALSE(empty.next(line));
    EXPECT_FALSE(empty.next(line));
    EXPECT_EQ(empty.idlePumps, 0u);
    ::close(nul);

    // A descriptor that is not open (`rune console <&-`): poll reports POLLNVAL and read fails: the end of input.
    int dead[2];
    ASSERT_EQ(::pipe(dead), 0);
    ::close(dead[0]);
    ::close(dead[1]);
    rune::cli::console::PollingFdLineSource closed(dead[0], &countIdle, &idle);
    EXPECT_FALSE(closed.next(line));
    EXPECT_EQ(idle, 0) << "no pump loop on a closed descriptor";
}

namespace {
// The signal storm of the next test.  The handler is async-signal-safe: it counts, and after kWatchdog signals it
// writes the input itself and closes the pipe (so the test fails on its assertions instead of hanging).
volatile sig_atomic_t gStormSignals = 0;
volatile sig_atomic_t gStormWatchdogFd = -1;
constexpr sig_atomic_t kWatchdog = 3000;
void stormHandler(int) {
    gStormSignals = gStormSignals + 1;
    if (gStormSignals == kWatchdog && gStormWatchdogFd >= 0) {
        const int fd = gStormWatchdogFd;
        gStormWatchdogFd = -1;
        [[maybe_unused]] const ssize_t n = ::write(fd, "watchdog\n", 9);
        ::close(fd);
    }
}
} // namespace

namespace {
// Storm idle callback: the 3rd pump writes the line (the watchdog is disarmed first: one writer, one close).
int gStormPumps = 0;
void stormIdle(void*) noexcept {
    if (++gStormPumps != 3) return;
    sigset_t alrm, saved;   // the handler is held off while the pump takes the descriptor
    sigemptyset(&alrm);
    sigaddset(&alrm, SIGALRM);
    ::pthread_sigmask(SIG_BLOCK, &alrm, &saved);
    if (gStormWatchdogFd >= 0) {
        const int fd = gStormWatchdogFd;
        gStormWatchdogFd = -1;
        [[maybe_unused]] const ssize_t n = ::write(fd, "pumped\n", 7);
        ::close(fd);
    }
    ::pthread_sigmask(SIG_SETMASK, &saved, nullptr);
}
} // namespace

TEST(ConsolePollingSourceRegression, SignalStormStillPumpsTheClientAdversarialRegression) {
    // A process that receives signals more often than the 50 ms poll timeout (a profiler's SIGPROF, a terminal being
    // resized, a timer) has every poll(2) interrupted with EINTR.  The idle pump must still run - it is what keeps the
    // broker session alive - so the input that the 3rd pump writes arrives.  Before the fix an EINTR re-polled without
    // pumping, and the pump starved for as long as the signals lasted (here: until the watchdog wrote the input).
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    gStormSignals = 0;
    gStormPumps = 0;
    gStormWatchdogFd = fds[1];   // both the 3rd pump and the watchdog write here; whichever is first disarms the other

    struct sigaction sa{}, old{};
    sa.sa_handler = &stormHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;   // no SA_RESTART: poll(2) is interrupted
    ASSERT_EQ(::sigaction(SIGALRM, &sa, &old), 0);
    itimerval tick{{0, 1000}, {0, 1000}};   // every 1 ms, far below the 50 ms poll timeout
    ASSERT_EQ(::setitimer(ITIMER_REAL, &tick, nullptr), 0);

    rune::cli::console::PollingFdLineSource src(fds[0], &stormIdle, nullptr);
    std::string line;
    const bool got = src.next(line);

    itimerval off{};
    ::setitimer(ITIMER_REAL, &off, nullptr);
    ::sigaction(SIGALRM, &old, nullptr);
    const int signals = gStormSignals;
    if (gStormWatchdogFd >= 0) { ::close(gStormWatchdogFd); gStormWatchdogFd = -1; }   // neither fired: close it here

    ASSERT_TRUE(got);
    EXPECT_EQ(line, "pumped") << "the idle pump starved under the signal storm: the watchdog wrote the input after "
                              << signals << " signals (" << gStormPumps << " pumps)";
    EXPECT_GE(gStormPumps, 3);
    EXPECT_EQ(src.idlePumps, static_cast<uint64_t>(gStormPumps));
    ::close(fds[0]);
}

// ── Credential hygiene (#305, #306) ──────────────────────────────────────────────────────────────────────────────────
TEST(ConsolePollingSourceRegression, PasswordLiteralIsWipedFromTheReadBufferRegression) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    const std::string text = "create user u with password 'S3cretS3cret';\nshow users;\n";
    ASSERT_EQ(::write(fds[1], text.data(), text.size()), static_cast<ssize_t>(text.size()));
    rune::cli::console::PollingFdLineSource src(fds[0], nullptr, nullptr);
    std::string line;
    ASSERT_TRUE(src.next(line));
    EXPECT_NE(line.find("S3cret"), std::string::npos);
    ASSERT_TRUE(src.next(line));
    EXPECT_EQ(line, "show users;");
    EXPECT_EQ(src.heldBytesForTest().find("S3cret"), std::string_view::npos) << "a consumed line lingers in the buffer";
    ::close(fds[0]);
    ::close(fds[1]);
}

namespace {
bool readPw(const std::string& input, std::string& pw) {
    int fds[2];
    if (::pipe(fds) != 0) return false;
    EXPECT_EQ(::write(fds[1], input.data(), input.size()), static_cast<ssize_t>(input.size()));
    ::close(fds[1]);
    const bool ok = rune::cli::console::readPasswordLine(fds[0], pw);
    ::close(fds[0]);
    return ok;
}
} // namespace

TEST(ConsolePromptRegression, PasswordIsReadIntoAFixedBufferAndOverlongIsRefusedRegression) {
    std::string pw;
    ASSERT_TRUE(readPw("pw with spaces \r\n", pw));
    EXPECT_EQ(pw, "pw with spaces ");
    EXPECT_FALSE(readPw(std::string(1025, 'a') + "\n", pw));
    EXPECT_TRUE(pw.empty());
    ASSERT_TRUE(readPw(std::string(1024, 'a') + "\n", pw));
    EXPECT_EQ(pw.size(), 1024u);
}

namespace {
/// C20 review: the scheduler state of a thread ('R' running, 'S' sleeping in a blocking call, 'Z' ...); 0 if unknown.
char threadState(pid_t pid, pid_t tid) {
    const std::string path = "/proc/" + std::to_string(pid) + "/task/" + std::to_string(tid) + "/stat";
    std::ifstream f(path);
    std::string s;
    std::getline(f, s);
    const auto rp = s.rfind(')');
    return (rp == std::string::npos || rp + 2 >= s.size()) ? '\0' : s[rp + 2];
}
/// Writes @p rest to @p fd once thread (pid, tid) blocks (sleeps) - i.e. once it waits for more input - or once
/// `doneFd` is readable / the process is gone.  Capped: never hangs.  Async-safe enough for a forked child.
void writeWhenBlocked(pid_t pid, pid_t tid, int fd, const std::string& rest, int doneFd) {
    for (long i = 0; i < 50'000'000; ++i) {
        if (doneFd >= 0) {
            pollfd p{doneFd, POLLIN, 0};
            if (::poll(&p, 1, 0) > 0) break;   // the reader returned without waiting
        }
        const char st = threadState(pid, tid);
        if (st == 'S') { (void)!::write(fd, rest.data(), rest.size()); break; }
        if (st == '\0' || st == 'Z') break;
        ::sched_yield();
    }
}

int g_scriptStep = 0;
/// #672 adversarial: a scripted read(2) - partial lines, EAGAIN and EWOULDBLOCK at every position, EINTR, an
/// over-long run, then EOF.  An entry {nullptr, e} fails with errno e; {s, 0} returns s; past the end: EOF.
struct ScriptedRead { const char* data; int err; };
std::vector<ScriptedRead> g_script;
std::string g_bigChunk;
ssize_t scriptedRead(int, void* b, std::size_t n) {
    if (g_scriptStep >= static_cast<int>(g_script.size())) return 0;
    const ScriptedRead s = g_script[static_cast<std::size_t>(g_scriptStep++)];
    if (s.data == nullptr) { errno = s.err; return -1; }
    const std::size_t len = std::min(n, std::strlen(s.data));
    std::memcpy(b, s.data, len);
    return static_cast<ssize_t>(len);
}
} // namespace

// C20 adversarial review (#672, input bounds / truncation at every offset): EAGAIN / EWOULDBLOCK / EINTR between the
// bytes of one line, while an over-long line is being discarded, and before a last line without '\n' never end the
// input or split a line; each would-block pumps the client.
TEST(ConsolePollingSourceRegression, WouldBlockAtEveryPositionNeverEndsOrSplitsALineAdversarialRegression) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    ASSERT_EQ(::write(fds[1], "x", 1), 1);   // never read (the scripted read stands in): poll always says POLLIN
    g_bigChunk.assign(rune::cli::console::kMaxInputLineBytes + 10, 'z');
    g_script = {{"ab", 0}, {nullptr, EAGAIN}, {"c", 0}, {nullptr, EWOULDBLOCK}, {nullptr, EINTR}, {"d\nsecond", 0},
                {nullptr, EAGAIN}, {nullptr, EAGAIN}, {"\n", 0},
                {g_bigChunk.c_str(), 0}, {nullptr, EAGAIN}, {"zzz", 0}, {nullptr, EWOULDBLOCK}, {"zz\nafter", 0},
                {nullptr, EAGAIN}, {" long\ntail-no-newline", 0}, {nullptr, EAGAIN}};
    g_scriptStep = 0;
    int idle = 0;
    std::ostringstream err;
    rune::cli::console::PollingFdLineSource src(fds[0], &countIdle, &idle);
    src.err = &err;
    src.setReadFnForTest(&scriptedRead);
    std::vector<std::string> lines;
    std::vector<bool> discarded;
    std::string line;
    for (int i = 0; i < 20 && src.next(line); ++i) { lines.push_back(line); discarded.push_back(src.lastLineDiscarded); }
    ASSERT_EQ(lines.size(), 5u) << "abcd, second, the dropped long line, `after long`, the last line";
    EXPECT_EQ(lines[0], "abcd");
    EXPECT_EQ(lines[1], "second");
    EXPECT_TRUE(discarded[2]) << "the over-long line is the flagged placeholder";
    EXPECT_EQ(lines[3], "after long") << "the discard ends at the long line's newline, across would-blocks";
    EXPECT_EQ(lines[4], "tail-no-newline") << "a would-block before EOF does not lose the last line";
    EXPECT_EQ(src.idlePumps, 8u) << "every EAGAIN / EWOULDBLOCK pumps the client (EINTR only retries)";
    EXPECT_EQ(idle, 8);
    EXPECT_EQ(count(err.str(), "input line too long"), 1u) << err.str();
    ::close(fds[0]); ::close(fds[1]);
}

// C20 adversarial review (#672 bug class - the terminal password prompt): readPasswordLine on an O_NONBLOCK fd (a
// terminal another process left non-blocking) takes EAGAIN for the end of the line: a password typed in two bursts
// is accepted TRUNCATED to its first burst and the login fails with the wrong password.  The rest of the line arrives
// once the reader waits; the whole password must come back.
TEST(ConsolePromptRegression, PasswordOnANonBlockingFdIsNotCutAtEagainAdversarialRegression) {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    ASSERT_EQ(::fcntl(fds[0], F_SETFL, ::fcntl(fds[0], F_GETFL) | O_NONBLOCK), 0);
    ASSERT_EQ(::write(fds[1], "secr", 4), 4);   // the first burst; the rest only once the reader waits
    const pid_t self = ::getpid();
    const pid_t tid = static_cast<pid_t>(::syscall(SYS_gettid));
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        writeWhenBlocked(self, tid, fds[1], "et-pw\n", -1);
        ::_exit(0);
    }
    std::string pw;
    const bool ok = rune::cli::console::readPasswordLine(fds[0], pw);
    int status = 0;
    ::waitpid(child, &status, 0);
    EXPECT_TRUE(ok);
    EXPECT_EQ(pw, "secret-pw") << "the password was cut at the first EAGAIN";
    rune::client::detail::wipeString(pw);
    ::close(fds[0]); ::close(fds[1]);
}

// C20 adversarial review (#672 bug class - `-f -` scripts): a script on a non-blocking stdin pipe is read up to the
// first EAGAIN and the prefix RUNS as if it were the whole script (rc 0): `rune console -f -` and `rune ql -f -`
// share readScript.  The whole script must run.
TEST(RuneConsoleOfflineRegression, StdinScriptOnANonBlockingPipeIsReadWholeAdversarialRegression) {
    int in[2], res[2];
    ASSERT_EQ(::pipe(in), 0);
    ASSERT_EQ(::pipe(res), 0);
    ASSERT_EQ(::write(in[1], "topic first partitions 1;\n", 26), 26);
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        ::dup2(in[0], STDIN_FILENO);
        ::close(in[1]);
        (void)::fcntl(STDIN_FILENO, F_SETFL, ::fcntl(STDIN_FILENO, F_GETFL) | O_NONBLOCK);
        std::ostringstream out, err;
        const int rc = rune::cli::run({"console", "-f", "-", "--dry-run"}, std::cin, out, err);
        const std::string r = std::to_string(rc) + "\n" + out.str() + err.str();
        (void)!::write(res[1], r.data(), r.size());
        ::_exit(0);
    }
    ::close(res[1]);
    writeWhenBlocked(child, child, in[1], "topic second partitions 1;\n", res[0]);
    ::close(in[1]);   // EOF: the script ends
    std::string got;
    char b[4096];
    for (ssize_t n; (n = ::read(res[0], b, sizeof b)) > 0;) got.append(b, static_cast<std::size_t>(n));
    int status = 0;
    ::waitpid(child, &status, 0);
    ::close(in[0]); ::close(res[0]);
    EXPECT_EQ(got.rfind("0\n", 0), 0u) << got;
    EXPECT_NE(got.find("first"), std::string::npos) << got;
    EXPECT_NE(got.find("second"), std::string::npos) << "the script was cut at the first EAGAIN: " << got;
}

// ---- Whole-branch review of PR #704: readScript ('-' through std::cin, 25275f44) attacked through the public CLI ----
namespace {
constexpr const char kStdinNeedle[] = "stdin-needle-review-704-qz";
/// Runs `rune <args>` in a forked child on the real std::cin.  `setupChild` (child side) puts fd 0 in place; `feed`
/// (parent side, gets the child's pid) writes the input.  The child reports "<rc>\n<out><err>" and, on glibc, whether
/// stdio's stdin buffer still holds kStdinNeedle ("STDIO-HIT").
template <class Setup, class Feed>
std::string runCliInChild(std::vector<std::string> args, Setup setupChild, Feed feed) {
    int res[2];
    if (::pipe(res) != 0) return "pipe failed";
    const pid_t child = ::fork();
    if (child < 0) return "fork failed";
    if (child == 0) {
        ::close(res[0]);
        setupChild();
        std::ostringstream out, err;
        const int rc = rune::cli::run(std::move(args), out, err);   // the process entry: std::cin, as main() does
        std::string r = std::to_string(rc) + "\n" + out.str() + err.str();
#ifdef __GLIBC__
        if (stdin->_IO_buf_base != nullptr) {
            const std::string_view buf(stdin->_IO_buf_base, static_cast<std::size_t>(stdin->_IO_buf_end - stdin->_IO_buf_base));
            if (buf.find(kStdinNeedle + 1) != std::string_view::npos) r += "\nSTDIO-HIT\n";
        }
#endif
        (void)!::write(res[1], r.data(), r.size());
        ::_exit(0);
    }
    ::close(res[1]);
    feed(child);
    std::string got;
    char b[4096];
    for (ssize_t n; (n = ::read(res[0], b, sizeof b)) > 0;) got.append(b, static_cast<std::size_t>(n));
    int status = 0;
    ::waitpid(child, &status, 0);
    ::close(res[0]);
    return got;
}
/// Waits (capped) until thread (pid, tid) sleeps; false if it never does or is gone.
bool awaitSleeping(pid_t pid) {
    for (long i = 0; i < 50'000'000; ++i) {
        const char st = threadState(pid, pid);
        if (st == 'S') return true;
        if (st == '\0' || st == 'Z') return false;
        ::sched_yield();
    }
    return false;
}
int g_sigPipeWr = -1;
void notifyOnSignal(int) { const char c = 's'; (void)!::write(g_sigPipeWr, &c, 1); }
} // namespace

// Input bounds: a '-' script of 0, 1 and chunk-boundary sizes (4095/4096/4097/8192: the read loop's n == sizeof chunk
// edge) on a regular-file stdin is read whole - the tail statement sits in the last byte(s), so a cut loses it.
TEST(RuneConsoleOfflineRegression, StdinScriptAtEveryChunkBoundaryIsReadWholeAdversarialRegression) {
    const std::string tail = "topic tail_topic partitions 1;";
    for (const std::size_t size : {std::size_t{0}, std::size_t{1}, std::size_t{4095}, std::size_t{4096}, std::size_t{4097},
                                   std::size_t{8192}, std::size_t{8193}}) {
        std::string script;
        if (size == 1) script = " ";
        else if (size > 0) {
            ASSERT_GT(size, tail.size() + 4);
            script = "--" + std::string(size - tail.size() - 3, 'p') + "\n" + tail;
            ASSERT_EQ(script.size(), size);
        }
        const fs::path file = writeScript("boundary_" + std::to_string(size) + ".rql", script);
        for (const char* cmd : {"console", "ql"}) {
            const std::string got = runCliInChild(
                {cmd, "-f", "-", "--dry-run"},
                [&] { const int fd = ::open(file.c_str(), O_RDONLY); ::dup2(fd, STDIN_FILENO); ::close(fd); },
                [](pid_t) {});
            EXPECT_EQ(got.rfind("0\n", 0), 0u) << cmd << " size " << size << ": " << got;
            EXPECT_EQ(got.find("tail_topic") != std::string::npos, size > 1) << cmd << " size " << size << ": " << got;
        }
        fs::remove(file);
    }
}

// Resources / input bounds: a script of ~200 KB on an O_NONBLOCK stdin pipe, fed in seeded random bursts by a writer
// that keeps the pipe running dry (EAGAIN between bursts, the pipe's 64 KiB capacity exceeded): every statement runs,
// once, in order - nothing is lost at an EAGAIN or a chunk edge.
TEST(RuneConsoleOfflineRegression, HugeStdinScriptInRandomBurstsOnANonBlockingPipeIsReadWholeAdversarialRegression) {
    constexpr int kStatements = 8000;
    std::string script;
    for (int i = 0; i < kStatements; ++i) script += "topic burst_" + std::to_string(i) + " partitions 1;\n";
    const unsigned seed = 704u;
    // A reader that gives up early must fail the test, not kill it with SIGPIPE.
    struct IgnoreSigpipe { void (*old)(int) = ::signal(SIGPIPE, SIG_IGN); ~IgnoreSigpipe() { ::signal(SIGPIPE, old); } } ignoreSigpipe;
    for (const char* cmd : {"console", "ql"}) {
        int in[2];
        ASSERT_EQ(::pipe(in), 0);
        const std::string got = runCliInChild(
            {cmd, "-f", "-", "--dry-run"},
            [&] {
                ::dup2(in[0], STDIN_FILENO);
                ::close(in[0]); ::close(in[1]);
                (void)::fcntl(STDIN_FILENO, F_SETFL, ::fcntl(STDIN_FILENO, F_GETFL) | O_NONBLOCK);
            },
            [&](pid_t) {
                ::close(in[0]);
                unsigned s = seed;
                std::size_t off = 0;
                while (off < script.size()) {
                    s = s * 1103515245u + 12345u;
                    const std::size_t len = std::min<std::size_t>(script.size() - off, 1 + (s >> 8) % 9000);
                    const ssize_t w = ::write(in[1], script.data() + off, len);
                    if (w <= 0) break;
                    off += static_cast<std::size_t>(w);
                    if ((s >> 4) % 3 == 0) ::sched_yield();
                }
                ::close(in[1]);
            });
        EXPECT_EQ(got.rfind("0\n", 0), 0u) << cmd << " seed " << seed << ": " << got.substr(0, 400);
        std::size_t seen = 0, pos = 0, last = 0;
        bool ordered = true;
        for (int i = 0; i < kStatements; ++i) {
            const std::string name = "burst_" + std::to_string(i) + " ";
            pos = got.find(name, last);
            if (pos == std::string::npos) { ordered = false; break; }
            last = pos + name.size();
            ++seen;
        }
        EXPECT_TRUE(ordered) << cmd << " seed " << seed << ": statement " << seen << " missing or out of order";
        EXPECT_EQ(count(got, "burst_"), static_cast<std::size_t>(kStatements)) << cmd << " seed " << seed;
    }
}

// EINTR: a signal (no SA_RESTART) interrupting the blocked stdin read of a '-' script is not end of input: the
// statements written after it run too.
TEST(RuneConsoleOfflineRegression, SignalInterruptingTheStdinScriptReadIsNotEndOfInputAdversarialRegression) {
    for (const char* cmd : {"console", "ql"}) {
        int in[2], sig[2];
        ASSERT_EQ(::pipe(in), 0);
        ASSERT_EQ(::pipe(sig), 0);
        ASSERT_EQ(::write(in[1], "topic before_signal partitions 1;\n", 34), 34);
        const std::string got = runCliInChild(
            {cmd, "-f", "-", "--dry-run"},
            [&] {
                ::dup2(in[0], STDIN_FILENO);
                ::close(in[0]); ::close(in[1]); ::close(sig[0]);
                g_sigPipeWr = sig[1];
                struct sigaction sa {};
                sa.sa_handler = &notifyOnSignal;   // no SA_RESTART: the blocked read(2) fails with EINTR
                ::sigaction(SIGUSR1, &sa, nullptr);
            },
            [&](pid_t child) {
                ::close(in[0]); ::close(sig[1]);
                ASSERT_TRUE(awaitSleeping(child)) << "the reader never blocked";
                ::kill(child, SIGUSR1);
                char c = 0;
                ASSERT_EQ(::read(sig[0], &c, 1), 1) << "the handler never ran";   // the read was interrupted
                ASSERT_TRUE(awaitSleeping(child)) << "the reader did not go back to reading";
                ASSERT_EQ(::write(in[1], "topic after_signal partitions 1;\n", 33), 33);
                ::close(in[1]);
                ::close(sig[0]);
            });
        EXPECT_EQ(got.rfind("0\n", 0), 0u) << cmd << ": " << got;
        EXPECT_NE(got.find("before_signal"), std::string::npos) << cmd << ": " << got;
        EXPECT_NE(got.find("after_signal"), std::string::npos) << cmd << ": EINTR ended the script: " << got;
    }
}

// A stdin read ERROR (fd 0 write-only: EBADF; a directory: EISDIR) fails the command with nothing run - never an
// empty or prefix script that exits 0.
TEST(RuneConsoleOfflineRegression, StdinReadErrorFailsTheScriptAndRunsNothingAdversarialRegression) {
    for (const int mode : {0, 1}) {
        for (const char* cmd : {"console", "ql"}) {
            const std::string got = runCliInChild(
                {cmd, "-f", "-", "--dry-run"},
                [&] {
                    const int fd = mode == 0 ? ::open("/dev/null", O_WRONLY) : ::open("/", O_RDONLY | O_DIRECTORY);
                    ::dup2(fd, STDIN_FILENO);
                    ::close(fd);
                },
                [](pid_t) {});
            EXPECT_EQ(got.rfind("1\n", 0), 0u) << cmd << " mode " << mode << ": " << got;
            EXPECT_NE(got.find("cannot read the script from stdin"), std::string::npos) << cmd << " mode " << mode << ": " << got;
        }
    }
}

// Redirected std::cin (an embedder, the CLI tests): the redirected script is read even when the process stdin carries
// stale end-of-file AND error flags from earlier use - those flags speak for the real stdin, not for the stream read.
TEST(RuneConsoleOfflineRegression, RedirectedCinIgnoresStaleProcessStdinFlagsAdversarialRegression) {
    for (const char* cmd : {"console", "ql"}) {
        std::istringstream script("topic redirected_topic partitions 1;\n");
        const std::string got = runCliInChild(
            {cmd, "-f", "-", "--dry-run"},
            [&] {
                const int fd = ::open("/dev/null", O_RDONLY);
                ::dup2(fd, STDIN_FILENO);
                ::close(fd);
                (void)std::getchar();          // end of file: feof(stdin)
                (void)std::fputc('x', stdin);  // a read-only stream: ferror(stdin)
                std::cin.rdbuf(script.rdbuf());
            },
            [](pid_t) {});
        EXPECT_EQ(got.rfind("0\n", 0), 0u) << cmd << ": " << got;
        EXPECT_NE(got.find("redirected_topic"), std::string::npos) << cmd << ": " << got;
    }
}

// Secret hygiene (#305 / #680) of the '-' script read: a password literal arriving in a later pipe burst leaves no copy
// in stdio's stdin buffer (a heap block that lives, unwiped, for the rest of the process) - the script text is only
// ever held in the wiped chunk and the wiped string.
TEST(RuneConsoleOfflineRegression, StdinScriptPasswordLeavesNoCopyInTheStdioBufferAdversarialRegression) {
#ifndef __GLIBC__
    GTEST_SKIP() << "inspects glibc's FILE buffer";
#else
    const std::string secret = std::string("create user stdin_user with password '") + kStdinNeedle + "';\n";
    for (const char* cmd : {"console", "ql"}) {
        int in[2];
        ASSERT_EQ(::pipe(in), 0);
        ASSERT_EQ(::write(in[1], "topic first_burst partitions 1;\n", 32), 32);
        const std::string got = runCliInChild(
            {cmd, "-f", "-", "--dry-run"},
            [&] { ::dup2(in[0], STDIN_FILENO); ::close(in[0]); ::close(in[1]); },
            [&](pid_t child) {
                ::close(in[0]);
                ASSERT_TRUE(awaitSleeping(child));
                ASSERT_EQ(::write(in[1], secret.data(), secret.size()), static_cast<ssize_t>(secret.size()));
                ::close(in[1]);
            });
        EXPECT_EQ(got.rfind("0\n", 0), 0u) << cmd << ": " << got;
        EXPECT_NE(got.find("stdin_user"), std::string::npos) << cmd << ": " << got;
        EXPECT_EQ(got.find("STDIO-HIT"), std::string::npos)
            << cmd << ": the password literal stays in stdio's stdin buffer after the script was read";
    }
#endif
}

TEST(ConsoleOfflineRegression, WipeStringZeroesTheWholeCapacityRegression) {
    std::string s(100, 'z');
    s.resize(3);
    const char* p = s.data();
    ASSERT_GE(s.capacity(), 100u);
    rune::client::detail::wipeString(s);
    EXPECT_TRUE(s.empty());
    for (std::size_t i = 0; i < 100; ++i) ASSERT_EQ(p[i], 0) << "byte " << i;
}


// ---- C6 review: adversarial credential hygiene of the console (#305) ----------------------------------------------
// The freed-memory scan (AllocCounter): every heap block freed while it is armed is searched for the password literal
// (from its second byte: a cleared string keeps all but the first).  Test-owned copies (the input stream, the
// script text) outlive the window.
namespace {
constexpr const char kConsoleNeedle[] = "console-needle-c6-0001-xyz";
struct ConsoleFreedScan {
    ConsoleFreedScan() { alloc_counter::scanFreedForThisThread(kConsoleNeedle + 1, sizeof kConsoleNeedle - 2); }
    ~ConsoleFreedScan() { alloc_counter::scanFreedForThisThread(nullptr, 0); }
    long hits() const { return alloc_counter::freedHitsThisThread(); }
};
// Runs `rune console <args>` on `stdinText` with the scan armed only around the call itself.
long freedHitsOfConsole(std::vector<std::string> args, const std::string& stdinText, ConsoleRun& r) {
    args.insert(args.begin(), "console");
    std::istringstream in(stdinText);
    std::ostringstream out, err;
    long hits = -1;
    {
        ConsoleFreedScan scan;
        r.rc = rune::cli::run(std::move(args), in, out, err);
        hits = scan.hits();
    }
    r.out = out.str();
    r.err = err.str();
    return hits;
}
std::string manyStatements(int n) {   // enough text after the password for every buffer to grow several times
    std::string s;
    for (int i = 0; i < n; ++i) s += "drop topic padding_topic_" + std::to_string(i) + ";\n";
    return s;
}
} // namespace

TEST(RuneConsoleOfflineRegression, FreedScanIgnoresStaleBytesOfAReusedChunkRegression) {
    // The flake behind AScriptPasswordLiteralLeavesNoFreedCopy under --gtest_repeat: a chunk freed while the scan was off
    // (a test-owned, unwiped copy of the script) came back to the code under test, which wrote only part of it; the
    // stale needle in the untouched tail counted as a hit.  Only bytes written inside the window may count.
    constexpr std::size_t kSize = 200;
    char* stale = static_cast<char*>(::operator new(kSize));
    std::memset(stale, 'x', kSize);
    std::memcpy(stale + 100, kConsoleNeedle, sizeof kConsoleNeedle - 1);
    ::operator delete(stale);   // scan off: freed unwiped, like a test temporary
    long hits = -1;
    char* reused = nullptr;
    {
        ConsoleFreedScan scan;
        reused = static_cast<char*>(::operator new(kSize));   // the same size class: the allocator hands it back
        std::memset(reused, 'y', 16);                          // the code under test writes only a prefix
        const bool sameChunk = reused == stale;
        ::operator delete(reused);
        hits = scan.hits();
        if (!sameChunk) GTEST_SKIP() << "the allocator did not reuse the chunk: nothing to check";
    }
    EXPECT_EQ(hits, 0) << "stale bytes of a reused chunk counted as an unwiped secret";
    // The scan still sees what the window itself writes.
    {
        ConsoleFreedScan scan;
        char* b = static_cast<char*>(::operator new(kSize));
        std::memcpy(b + 50, kConsoleNeedle, sizeof kConsoleNeedle - 1);
        ::operator delete(b);
        EXPECT_EQ(scan.hits(), 1) << "vacuity guard: a needle written inside the window is a hit";
    }
}

TEST(RuneConsoleOfflineRegression, AScriptPasswordLiteralLeavesNoFreedCopyAdversarialRegression) {
    // `-f -` (the documented way to pass a password literal) and `-f <file>`: the script text, its growth copies,
    // the statement texts and the parsed / prepared statements.
    const std::string script = std::string("create user script_user with password '") + kConsoleNeedle + "';\n" +
                               "alter user script_user set password '" + kConsoleNeedle + "';\n" + manyStatements(2000);
    ConsoleRun r;
    EXPECT_EQ(freedHitsOfConsole({"-f", "-", "--dry-run"}, script, r), 0) << "stdin script: a freed block held it";
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_EQ(r.out.find(kConsoleNeedle + 1), std::string::npos) << "the plan never prints the password";
    const fs::path file = writeScript("c6_password_script.rql", script);
    EXPECT_EQ(freedHitsOfConsole({"-f", file.string(), "--dry-run"}, "", r), 0) << "file script: a freed block held it";
    EXPECT_EQ(r.rc, 0) << r.err;
    fs::remove(file);
}

TEST(RuneConsoleOfflineRegression, AnInteractivePasswordLiteralLeavesNoFreedCopyAdversarialRegression) {
    // The REPL: a long line (the line buffer grows while the password is in it) and a statement continued over many
    // lines (the statement text grows after the password line).
    std::string longLine = std::string("create user repl_user with password '") + kConsoleNeedle + "'; -- " +
                           std::string(5000, 'x') + "\n";
    std::string continued = std::string("alter user repl_user set password\n'") + kConsoleNeedle + "'\n";
    for (int i = 0; i < 400; ++i) continued += "-- a comment line inside the statement " + std::to_string(i) + "\n";
    continued += ";\n";
    ConsoleRun r;
    EXPECT_EQ(freedHitsOfConsole({"--dry-run"}, longLine + continued + manyStatements(50), r), 0)
        << "interactive: a freed block held the password literal";
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_EQ(r.out.find(kConsoleNeedle + 1), std::string::npos);
}

// ---- #681: every console wipe guard has its own test ----------------------------------------------------------------
// Each input puts the needle ONLY in the buffer its guard owns (a comment is dropped by the parser, so no parsed
// statement or password field holds it): delete that guard's wipe and exactly that buffer is freed with the needle.
TEST(ConsoleWipeRegression, ScriptWipeLeavesNoFreedCopyRegression) {
    // The script text: the needle sits in a trailing comment, so no statement text holds it.
    const fs::path file = writeScript("c20a_script_wipe.rql", std::string("show history;\n-- ") + kConsoleNeedle + "\n");
    ConsoleRun r;
    EXPECT_EQ(freedHitsOfConsole({"-f", file.string(), "--dry-run"}, "", r), 0) << "the script buffer was freed unwiped";
    EXPECT_EQ(r.rc, 0) << r.err;
    fs::remove(file);
}

TEST(ConsoleWipeRegression, BatchWipeLeavesNoFreedCopyRegression) {
    // The prepared statement's text: the needle is in a comment inside the statement (the parser drops it, the text
    // keeps it).  The statement splitter's copy is TextsWipe's: a mutation of either guard turns this red (the same
    // bytes); TextsWipe has its own test where the batch never holds them.
    const fs::path file = writeScript("c20a_batch_wipe.rql", std::string("show history -- ") + kConsoleNeedle + "\n;\n");
    ConsoleRun r;
    EXPECT_EQ(freedHitsOfConsole({"-f", file.string(), "--dry-run"}, "", r), 0) << "a batch text was freed unwiped";
    EXPECT_EQ(r.rc, 0) << r.err;
    fs::remove(file);
}

TEST(ConsoleWipeRegression, TextsWipeLeavesNoFreedCopyRegression) {
    // Statement 1 parses but cannot be prepared (a version out of range): the batch stays empty while the splitter's texts
    // (all statements) are freed - statement 2 carries the needle in a comment.
    const fs::path file = writeScript("c20a_texts_wipe.rql",
        std::string("show schema t as of version 99999999999;\nshow history -- ") + kConsoleNeedle + "\n;\n");
    ConsoleRun r;
    EXPECT_EQ(freedHitsOfConsole({"-f", file.string(), "--dry-run"}, "", r), 0) << "the splitter's texts were freed unwiped";
    EXPECT_NE(r.rc, 0) << "the statement cannot be prepared: " << r.err;
    EXPECT_NE(r.err.find("out of range"), std::string::npos) << "vacuity guard: it failed in prepare: " << r.err;
    fs::remove(file);
}

TEST(ConsoleWipeRegression, TextWipeLeavesNoFreedCopyRegression) {
    // The REPL's per-line statement text: a comment-only line is an Empty input - only `text` ever holds it.
    ConsoleRun r;
    EXPECT_EQ(freedHitsOfConsole({"--dry-run"}, std::string("-- ") + kConsoleNeedle + "\n", r), 0)
        << "the REPL text buffer was freed unwiped";
    EXPECT_EQ(r.rc, 0) << r.err;
}

TEST(ConsoleWipeRegression, ReplWipeLeavesNoFreedCopyRegression) {
    // Input ends inside a statement: the text sits in the ContinuingStatement when the REPL returns; only ReplWipe
    // (its second wipe) reaches it.  (Its `line` wipe is not isolable: the loop wipes `line` itself on every path.)
    ConsoleRun r;
    EXPECT_EQ(freedHitsOfConsole({"--dry-run"}, std::string("show history -- ") + kConsoleNeedle + "\n", r), 0)
        << "the unfinished statement was freed unwiped";
    EXPECT_NE(r.err.find("input ended inside a statement"), std::string::npos) << "vacuity guard: " << r.err;
}

TEST(ConsoleWipeRegression, CfgPasswordWipeLeavesNoFreedCopyRegression) {
    // LiveBroker::open copies the prompted password into its ClientConfig, then refuses the TLS options before any
    // Client exists: only CfgPasswordWipe reaches that copy.  (--insecure-plaintext never sends credentials.)
    namespace engine = rune::cli::engine;
    engine::LiveOptions o;
    o.endpoint          = "aeron:ipc";
    o.user              = "ws1";
    o.password          = kConsoleNeedle;   // test-owned: it outlives the window
    o.insecurePlaintext = true;
    engine::LiveBroker broker;
    std::ostringstream err;
    long hits = -1;
    bool opened = true;
    {
        ConsoleFreedScan scan;
        opened = broker.open(o, err);
        hits = scan.hits();
    }
    EXPECT_FALSE(opened);
    EXPECT_NE(broker.lastFailure().find("insecure-plaintext"), std::string::npos) << "vacuity guard: " << broker.lastFailure();
    EXPECT_EQ(hits, 0) << "the ClientConfig password copy was freed unwiped";
}

TEST(SecurityBootstrapRegression, KeyAndPasswordLeaveNoCopyInSpareCapacityRegression) {
    // #680: the key and the password are wiped over their whole capacity, not only their size.  Both files carry the
    // needle; bootstrap reads both, then fails at the broker (an endpoint that cannot be reached).
    const fs::path keyFile = writeScript("c20a_cluster_key", kConsoleNeedle);
    const fs::path pwFile  = writeScript("c20a_bootstrap_pw", std::string(kConsoleNeedle) + "\n");
    fs::permissions(keyFile, fs::perms::owner_read | fs::perms::owner_write);
    fs::permissions(pwFile, fs::perms::owner_read | fs::perms::owner_write);
    std::ostringstream out, err;
    int rc = -1;
    long hits = -1;
    {
        ConsoleFreedScan scan;
        rc = rune::cli::cmdSecurity({"bootstrap", "--admin", "root", "--cluster-key-file", keyFile.string(),
                                     "--password-file", pwFile.string(), "--broker", "no-such-host.invalid:1"}, out, err);
        hits = scan.hits();
    }
    EXPECT_NE(rc, 0) << "the broker cannot be reached: bootstrap fails after reading both files";
    EXPECT_EQ(hits, 0) << "a freed block held the key or the password: " << err.str();
    EXPECT_EQ(err.str().find(kConsoleNeedle + 1), std::string::npos);
    fs::remove(keyFile); fs::remove(pwFile);
}

namespace {
/// Points TMPDIR at a path that is not a directory for the scope (std::filesystem::temp_directory_path then throws);
/// the old value comes back on exit.
struct BadTmpdir {
    std::string old;
    bool had = false;
    explicit BadTmpdir(const std::string& notADir) {
        if (const char* e = std::getenv("TMPDIR"); e != nullptr) { old = e; had = true; }
        ::setenv("TMPDIR", notADir.c_str(), 1);
    }
    ~BadTmpdir() { if (had) ::setenv("TMPDIR", old.c_str(), 1); else ::unsetenv("TMPDIR"); }
};
} // namespace

// C20 adversarial review (#680, crash at a durable step): the private scratch dir comes from temp_directory_path(),
// which throws when TMPDIR is not a directory.  The throw leaves LiveBroker::open and cmdBootstrap with the key and the
// password still in their strings: they are freed unwiped and the exception reaches std::terminate (main has no catch),
// so a core dump holds both.  Bootstrap must fail cleanly (1) and leave no freed copy.
TEST(SecurityBootstrapRegression, BadTmpdirFailsCleanlyAndLeavesNoKeyOrPasswordAdversarialRegression) {
    const fs::path keyFile = writeScript("c20r_tmpdir_key", kConsoleNeedle);
    const fs::path pwFile  = writeScript("c20r_tmpdir_pw", std::string(kConsoleNeedle) + "\n");
    fs::permissions(keyFile, fs::perms::owner_read | fs::perms::owner_write);
    fs::permissions(pwFile, fs::perms::owner_read | fs::perms::owner_write);
    std::ostringstream out, err;
    int rc = -1;
    long hits = -1;
    bool threw = false;
    {
        BadTmpdir bad(keyFile.string());   // a regular file: not a directory
        ConsoleFreedScan scan;
        try {
            rc = rune::cli::cmdSecurity({"bootstrap", "--admin", "root", "--cluster-key-file", keyFile.string(),
                                         "--password-file", pwFile.string(), "--broker", "127.0.0.1:1"}, out, err);
        } catch (const std::exception&) {
            threw = true;
        }
        hits = scan.hits();
    }
    EXPECT_FALSE(threw) << "an exception left `rune security bootstrap` (std::terminate in the binary)";
    EXPECT_EQ(rc, 1) << err.str();
    EXPECT_EQ(hits, 0) << "a freed block held the cluster key or the password";
    fs::remove(keyFile); fs::remove(pwFile);
}

// C20 adversarial review (#680 sibling entry points): the same bad TMPDIR on `rune console --broker` and `rune ql
// --broker` must be a clean failure (1), not an exception out of rune::cli::run.
TEST(RuneConsoleOfflineRegression, BadTmpdirIsACleanFailureForConsoleAndQlAdversarialRegression) {
    const fs::path notADir = writeScript("c20r_tmpdir_file", "x");
    BadTmpdir bad(notADir.string());
    for (const std::vector<std::string>& args :
         {std::vector<std::string>{"console", "--broker", "127.0.0.1:1"},
          std::vector<std::string>{"ql", "--broker", "127.0.0.1:1", "-e", "show topics;"}}) {
        SCOPED_TRACE(args[0]);
        std::istringstream in("topic t partitions 1;\n");
        std::ostringstream out, err;
        int rc = -1;
        bool threw = false;
        try { rc = rune::cli::run(args, in, out, err); } catch (const std::exception&) { threw = true; }
        EXPECT_FALSE(threw) << "an exception left rune::cli::run (std::terminate in the binary)";
        EXPECT_NE(rc, 0) << err.str();
    }
}

// C20 adversarial review (#680, resources): an allocation failure at ANY step of `rune security bootstrap` after the
// key is read leaves no freed copy of the key or the password.  Sweeps every operator new of the run.
TEST(SecurityBootstrapRegression, AllocationFailureAtAnyStepLeavesNoKeyOrPasswordAdversarialRegression) {
    const fs::path keyFile = writeScript("c20r_oom_key", kConsoleNeedle);
    const fs::path pwFile  = writeScript("c20r_oom_pw", std::string(kConsoleNeedle) + "\n");
    fs::permissions(keyFile, fs::perms::owner_read | fs::perms::owner_write);
    fs::permissions(pwFile, fs::perms::owner_read | fs::perms::owner_write);
    const std::vector<std::string> args{"bootstrap", "--admin", "root", "--cluster-key-file", keyFile.string(),
                                        "--password-file", pwFile.string(), "--broker", "no-such-host.invalid:1"};
    int leakySteps = 0;
    long n = 1;
    for (; n <= 5000; ++n) {
        std::ostringstream out, err;
        long hits = -1;
        bool fired = false;
        {
            ConsoleFreedScan scan;
            alloc_counter::failNthAllocationThisThread(n);
            try { (void)rune::cli::cmdSecurity(args, out, err); } catch (const std::bad_alloc&) {}
            fired = alloc_counter::injectedFailureFiredThisThread();
            alloc_counter::failNthAllocationThisThread(0);
            hits = scan.hits();
        }
        if (hits != 0) { ++leakySteps; ADD_FAILURE() << "allocation " << n << " failed: " << hits << " freed block(s) held a secret"; }
        if (!fired) break;   // the run finished without reaching the n-th allocation: every step was swept
    }
    EXPECT_LT(n, 5000) << "vacuity guard: the sweep never reached the end of the run";
    EXPECT_GT(n, 3) << "vacuity guard: the run allocates";
    EXPECT_EQ(leakySteps, 0);
    fs::remove(keyFile); fs::remove(pwFile);
}

TEST(RuneConsoleOfflineRegression, AMovedFromPreparedSecurityKeepsNoShortPasswordInPlaceAdversarialRegression) {
    // A short (in-place) password survives a move in the moved-from string: PSecurity's wipe must reach it.
    namespace engine = rune::cli::engine;
    auto st = rune::ql::parse("create user u with password 'shortpw-7q';");
    ASSERT_TRUE(st.has_value());
    auto plan = rune::ql::applySecurityPlan(*st);
    ASSERT_TRUE(plan.has_value());
    auto holds = [](const engine::PSecurity& p) {
        const std::string_view raw(reinterpret_cast<const char*>(&p), sizeof p);
        return raw.find("hortpw-7q") != std::string_view::npos;
    };
    engine::PSecurity a{std::move(*plan)};
    EXPECT_EQ(plan->password.find("hortpw-7q"), std::string::npos);
    const std::string_view planRaw(reinterpret_cast<const char*>(&plan->password), sizeof plan->password);
    EXPECT_EQ(planRaw.find("hortpw-7q"), std::string_view::npos) << "the moved-from plan kept the password in place";
    engine::PSecurity b{std::move(a)};
    EXPECT_FALSE(holds(a)) << "move-constructed from";
    EXPECT_EQ(b.plan.password, "shortpw-7q");
    engine::PSecurity c;
    c = std::move(b);
    EXPECT_FALSE(holds(b)) << "move-assigned from";
    c.wipe();
    EXPECT_FALSE(holds(c)) << "wiped";
}

namespace {
struct FdBuf final : std::streambuf {
    int fd;
    explicit FdBuf(int f) : fd(f) {}
    int_type overflow(int_type c) override {
        if (c != traits_type::eof()) { const char ch = static_cast<char>(c); (void)!::write(fd, &ch, 1); }
        return c;
    }
    std::streamsize xsputn(const char* s, std::streamsize n) override {
        return ::write(fd, s, static_cast<std::size_t>(n));
    }
};
} // namespace

TEST(RuneConsolePtyRegression, SigintAtPasswordPromptRestoresEchoRegression) {
    int master = -1, slave = -1;
    ASSERT_EQ(::openpty(&master, &slave, nullptr, nullptr, nullptr), 0);
    termios t{};
    ASSERT_EQ(::tcgetattr(slave, &t), 0);
    ASSERT_NE(t.c_lflag & ECHO, 0u) << "a fresh pty echoes";
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        ::signal(SIGINT, SIG_DFL);   // a backgrounded test runner may have left it ignored
        FdBuf fb(slave);
        std::ostream errStream(&fb);
        std::string pw;
        (void)rune::cli::console::promptPasswordOnFd(slave, "u", pw, errStream);
        ::_exit(0);
    }
    // The prompt is written after echo is off and the handlers are in: it is the sync point.
    std::string seen;
    bool prompted = false;
    for (int i = 0; i < 64 && !prompted; ++i) {
        char buf[256];
        const ssize_t n = ::read(master, buf, sizeof buf);
        if (n <= 0) break;
        seen.append(buf, static_cast<std::size_t>(n));
        prompted = seen.find("password for u: ") != std::string::npos;
    }
    ASSERT_TRUE(prompted) << "saw: " << seen;
    ASSERT_EQ(::kill(child, SIGINT), 0);
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    EXPECT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGINT) << "the child dies of the SIGINT, status " << status;
    ASSERT_EQ(::tcgetattr(slave, &t), 0);
    EXPECT_NE(t.c_lflag & ECHO, 0u) << "echo stayed off after Ctrl-C at the password prompt";
    ::close(master);
    ::close(slave);
}

// ── C6 adversarial review (#306): every signal at the password prompt leaves the terminal as it found it ────────────
namespace {
int g_childSlave = -1;   // the forked child's pty, for its own test handler (a handler reaches only globals)
void childHandlerThatReturns(int) {   // '!' when the prompt handed it an echoing terminal first, '?' when not
    termios t{};
    const bool echo = ::tcgetattr(g_childSlave, &t) == 0 && (t.c_lflag & ECHO) != 0;
    (void)!::write(g_childSlave, echo ? "!" : "?", 1);
}

struct PromptChild {
    pid_t pid = -1;
    int master = -1, slave = -1;
    std::string seen;
    ~PromptChild() {
        if (pid > 0) { ::kill(pid, SIGKILL); ::waitpid(pid, nullptr, 0); }
        if (master >= 0) ::close(master);
        if (slave >= 0) ::close(slave);
    }
    /// Reads the master until `marker` shows (or 10 s pass with no progress): a wait on the child's output.
    bool readUntil(std::string_view marker) {
        for (int i = 0; i < 4096; ++i) {
            if (seen.find(marker) != std::string::npos) return true;
            pollfd p{master, POLLIN, 0};
            if (::poll(&p, 1, 10'000) <= 0) return false;
            char buf[256];
            const ssize_t n = ::read(master, buf, sizeof buf);
            if (n <= 0) return false;
            seen.append(buf, static_cast<std::size_t>(n));
        }
        return false;
    }
    [[nodiscard]] bool echoOn() const {
        termios t{};
        return ::tcgetattr(slave, &t) == 0 && (t.c_lflag & ECHO) != 0;
    }
    /// Waits until the slave's echo flag is `on` (10 s cap): the handler runs asynchronously to kill().
    bool awaitEcho(bool on) const {
        const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (echoOn() != on) {
            if (std::chrono::steady_clock::now() > until) return false;
            ::sched_yield();
        }
        return true;
    }
};

/// Forks a child that prompts on a fresh pty after `setup` and reports "DONE:<ok>:<password>" when the prompt
/// returns.  Returns once the prompt shows (echo is off and the handlers are in by then).
template <class Setup>
void startPromptChild(PromptChild& c, Setup setup) {
    ASSERT_EQ(::openpty(&c.master, &c.slave, nullptr, nullptr, nullptr), 0);
    ASSERT_TRUE(c.echoOn()) << "a fresh pty echoes";
    c.pid = ::fork();
    ASSERT_GE(c.pid, 0);
    if (c.pid == 0) {
        const rlimit noCore{0, 0};
        ::setrlimit(RLIMIT_CORE, &noCore);   // SIGQUIT dumps core by default
        for (const int sig : {SIGINT, SIGTERM, SIGHUP, SIGQUIT, SIGTSTP}) ::signal(sig, SIG_DFL);
        g_childSlave = c.slave;
        setup();
        FdBuf fb(c.slave);
        std::ostream errStream(&fb);
        std::string pw;
        const bool ok = rune::cli::console::promptPasswordOnFd(c.slave, "u", pw, errStream);
        errStream << "DONE:" << ok << ":" << pw << ":" << std::flush;
        ::_exit(0);
    }
    ASSERT_TRUE(c.readUntil("password for u: ")) << "saw: " << c.seen;
}
} // namespace

TEST(RuneConsolePtyRegression, EveryTerminatingSignalAtThePromptRestoresEchoAdversarialRegression) {
    for (const int sig : {SIGINT, SIGTERM, SIGHUP, SIGQUIT}) {
        SCOPED_TRACE(::strsignal(sig));
        PromptChild c;
        startPromptChild(c, [] {});
        if (HasFatalFailure()) return;
        ASSERT_FALSE(c.echoOn()) << "echo is off at the prompt";
        ASSERT_EQ(::kill(c.pid, sig), 0);
        int status = 0;
        ASSERT_EQ(::waitpid(c.pid, &status, 0), c.pid);
        c.pid = -1;
        EXPECT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == sig) << "the child dies of the signal, status " << status;
        EXPECT_TRUE(c.echoOn()) << "echo stayed off after the signal at the password prompt";
    }
}

TEST(RuneConsolePtyRegression, AnOldSignalHandlerThatReturnsKeepsTheTypedPasswordUnechoedAdversarialRegression) {
    // The application's own SIGINT handler runs and returns: the prompt goes on, so echo must be off again and the
    // prompt's handler back in before the password is typed.
    PromptChild c;
    startPromptChild(c, [] {
        struct sigaction sa{};
        sa.sa_handler = &childHandlerThatReturns;
        sigemptyset(&sa.sa_mask);
        ::sigaction(SIGINT, &sa, nullptr);
    });
    if (HasFatalFailure()) return;
    for (int round = 1; round <= 2; ++round) {   // twice: the prompt's handler must be back in after the first
        SCOPED_TRACE(round);
        ASSERT_EQ(::kill(c.pid, SIGINT), 0);
        ASSERT_TRUE(c.readUntil(std::string(static_cast<std::size_t>(round), '!')))
            << "the old handler ran after the prompt restored the terminal ('?': it was not): " << c.seen;
        ASSERT_TRUE(c.awaitEcho(false)) << "echo stayed on after the old handler returned, mid-prompt";
    }
    const std::string kTyped = "typed-pw-c6-0306";
    ASSERT_EQ(::write(c.master, (kTyped + "\n").data(), kTyped.size() + 1), static_cast<ssize_t>(kTyped.size() + 1));
    ASSERT_TRUE(c.readUntil("DONE:")) << c.seen;
    ASSERT_TRUE(c.readUntil(":" + kTyped + ":")) << "the prompt read the password: " << c.seen;
    const auto done = c.seen.find("DONE:");
    EXPECT_EQ(c.seen.substr(0, done).find(kTyped), std::string::npos) << "the terminal echoed the password: " << c.seen;
    EXPECT_NE(c.seen.find("DONE:1:"), std::string::npos);
    int status = 0;
    ASSERT_EQ(::waitpid(c.pid, &status, 0), c.pid);
    c.pid = -1;
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << status;
    EXPECT_TRUE(c.echoOn()) << "echo is back on after the prompt";
}

TEST(RuneConsolePtyRegression, AStopAndContinueAtThePromptEchoesWhileStoppedOnlyAdversarialRegression) {
    PromptChild c;
    startPromptChild(c, [] { ::setpgid(0, 0); });   // its own group: a stop is not discarded as an orphan's
    if (HasFatalFailure()) return;
    for (int round = 1; round <= 2; ++round) {
        SCOPED_TRACE(round);
        ASSERT_EQ(::kill(c.pid, SIGTSTP), 0);
        int status = 0;
        ASSERT_EQ(::waitpid(c.pid, &status, WUNTRACED), c.pid);
        ASSERT_TRUE(WIFSTOPPED(status)) << "the child stops, status " << status;
        EXPECT_TRUE(c.echoOn()) << "the shell gets an echoing terminal while the prompt is stopped";
        ASSERT_EQ(::kill(c.pid, SIGCONT), 0);
        ASSERT_TRUE(c.awaitEcho(false)) << "echo stayed on after the prompt resumed";
    }
    const std::string kTyped = "typed-pw-c6-tstp";
    ASSERT_EQ(::write(c.master, (kTyped + "\n").data(), kTyped.size() + 1), static_cast<ssize_t>(kTyped.size() + 1));
    ASSERT_TRUE(c.readUntil(":" + kTyped + ":")) << c.seen;
    EXPECT_EQ(c.seen.substr(0, c.seen.find("DONE:")).find(kTyped), std::string::npos) << c.seen;
    int status = 0;
    ASSERT_EQ(::waitpid(c.pid, &status, 0), c.pid);
    c.pid = -1;
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << status;
    EXPECT_TRUE(c.echoOn());
}

// ── C8: exit codes, script parsing order, request-id overflow, over-long lines, session end ────────────────────────

// Issue #23 (schema apply / alter are removed usage errors, rc 2 + "was removed"): already pinned by
// RuneCliOfflineRegression.SchemaApplyAndAlterAreRemovedWithConsolePointer in tests/RuneCliOfflineTest.cpp.

TEST(ConsoleOfflineRegression, LowBrokerCodesDoNotCollideWithLocalExitCodesRegression) {
    using rune::cli::console::consoleExitStatus;
    const std::pair<uint16_t, int> cases[] = {{1, 125}, {2, 125}, {3, 125}, {4, 4},     {30, 30}, {45, 45},
                                              {46, 46}, {124, 124}, {125, 125}, {300, 125}, {0, 125}};
    for (const auto& [code, want] : cases) EXPECT_EQ(consoleExitStatus(code), want) << "broker code " << code;
}

namespace {
/// Runs `console` with RUNE_BROKER removed from the environment (restored afterwards).
ConsoleRun consoleWithoutBrokerEnv(std::vector<std::string> args, const std::string& stdinText = {}) {
    const char* old = std::getenv("RUNE_BROKER");
    const std::string saved = old != nullptr ? old : "";
    ::unsetenv("RUNE_BROKER");
    auto r = console(std::move(args), stdinText);
    if (old != nullptr) ::setenv("RUNE_BROKER", saved.c_str(), 1);
    return r;
}
} // namespace

TEST(ConsoleOfflineRegression, ScriptIsParsedBeforeTheBrokerIsContactedRegression) {
    const fs::path bad = writeScript("c8_syntax.rql", "topic ;\n");
    const auto r = consoleWithoutBrokerEnv({"-f", bad.string()});
    EXPECT_EQ(r.rc, 1) << r.err;
    EXPECT_NE(r.err.find("c8_syntax.rql:statement 1: "), std::string::npos) << r.err;
    EXPECT_EQ(r.err.find("--broker"), std::string::npos) << "the syntax error is reported before any broker is needed: " << r.err;
    for (const std::string body : {"", "-- only a comment\n"}) {
        const fs::path empty = writeScript("c8_empty.rql", body);
        const auto e = consoleWithoutBrokerEnv({"-f", empty.string()});
        EXPECT_EQ(e.rc, 0) << "'" << body << "': " << e.err;
        EXPECT_TRUE(e.err.empty()) << e.err;
    }
}

TEST(ConsoleOfflineRegression, RequestIdLowBitsOverflowIsRefusedRegression) {
    const std::string stmts3 = "topic a { x int32 } partitions 1 key x;\ntopic b { x int32 } partitions 1 key x;\ndrop topic c;\n";
    const std::string stmts2 = "topic a { x int32 } partitions 1 key x;\ntopic b { x int32 } partitions 1 key x;\n";
    const std::string base   = "0000000000000001fffffffffffffffe";
    const auto over = console({"-f", writeScript("c8_rid3.rql", stmts3).string(), "--dry-run", "--request-id", base});
    EXPECT_EQ(over.rc, 1) << over.out;
    EXPECT_NE(over.err.find("would overflow"), std::string::npos) << over.err;
    EXPECT_TRUE(over.out.empty()) << "nothing is planned or run: " << over.out;
    const auto fits = console({"-f", writeScript("c8_rid2.rql", stmts2).string(), "--dry-run", "--request-id", base});
    EXPECT_EQ(fits.rc, 0) << fits.err;
    EXPECT_NE(fits.out.find("request-id 0000000000000001ffffffffffffffff\n"), std::string::npos) << fits.out;
    // The live path refuses before any broker contact too (no --broker needed to see the refusal).
    const auto live = consoleWithoutBrokerEnv({"-f", writeScript("c8_rid3l.rql", stmts3).string(), "--request-id", base});
    EXPECT_EQ(live.rc, 1) << live.err;
    EXPECT_NE(live.err.find("would overflow"), std::string::npos) << live.err;
}

namespace {
/// A scripted LineSource: each entry is a line and whether it is the "too long" placeholder.
struct ScriptedLines final : rune::cli::console::LineSource {
    struct L { std::string text; bool discarded; };
    std::vector<L> lines;
    std::size_t at = 0;
    bool next(std::string& line) override {
        if (at >= lines.size()) { lastLineDiscarded = false; return false; }
        line = lines[at].text;
        lastLineDiscarded = lines[at].discarded;
        ++at;
        return true;
    }
};
} // namespace

TEST(ConsoleOfflineRegression, OverlongLineAbandonsTheContinuedStatementRegression) {
    ScriptedLines src;
    src.lines = {{"topic a", false}, {"", true}, {"partitions 1;", false}, {"topic b { x int32 } partitions 1 key x;", false}};
    std::ostringstream out, err;
    EXPECT_EQ(rune::cli::console::runInteractiveDryRunForTest(src, out, err), 0);
    EXPECT_NE(err.str().find("the statement in progress was abandoned (an input line was too long)"), std::string::npos) << err.str();
    EXPECT_EQ(count(out.str(), "partitions"), 1u) << "exactly one statement is planned (topic b): " << out.str();
    EXPECT_NE(out.str().find("topic b partitions 1"), std::string::npos) << out.str();
    EXPECT_EQ(out.str().find("topic a"), std::string::npos) << "a and 'partitions 1' are never joined: " << out.str();
    // "partitions 1;" alone is a parse error, reported as one.
    EXPECT_NE(err.str().find("1:1: "), std::string::npos) << err.str();
}

TEST(ConsoleOfflineRegression, OverlongLineWhileAwaitingAStatementIsSkippedQuietlyRegression) {
    ScriptedLines src;
    src.lines = {{"", true}, {"topic b { x int32 } partitions 1 key x;", false}};
    std::ostringstream out, err;
    EXPECT_EQ(rune::cli::console::runInteractiveDryRunForTest(src, out, err), 0);
    EXPECT_EQ(err.str().find("abandoned"), std::string::npos) << err.str();
    EXPECT_EQ(count(out.str(), "partitions"), 1u) << out.str();
}

TEST(ConsoleOfflineRegression, AnyRejectedSessionEndsTheConsoleRegression) {
    using rune::cli::console::consoleSessionEndCode;
    struct C { uint16_t reject, stmt, want; };
    for (const C c : {C{0, 0, 0}, C{0, 30, 0}, C{0, 46, 46}, C{44, 0, 44}, C{48, 30, 48}, C{46, 46, 46}})
        EXPECT_EQ(consoleSessionEndCode(c.reject, c.stmt), c.want) << "reject " << c.reject << " statement " << c.stmt;
}

// ── C8 fix round ─────────────────────────────────────────────────────────────────────────────────────────────────────

TEST(ConsoleOfflineRegression, ARejectCodeAboveInt16MaxStillEndsTheSessionRegression) {
    // (b) the client keeps the reject code as an int16_t: a code >= 0x8000 (a newer broker) is negative there and must
    // still end the session with that code, not be ignored.
    using rune::cli::console::consoleRejectCode;
    using rune::cli::console::consoleSessionEndCode;
    EXPECT_EQ(consoleRejectCode(0), 0u);
    EXPECT_EQ(consoleRejectCode(44), 44u);
    EXPECT_EQ(consoleRejectCode(0x7FFF), 0x7FFFu);
    EXPECT_EQ(consoleRejectCode(static_cast<int16_t>(0x8000)), 0x8000u);
    EXPECT_EQ(consoleRejectCode(static_cast<int16_t>(0xFFFF)), 0xFFFFu);
    for (const uint16_t code : {uint16_t{0x8000}, uint16_t{0x8001}, uint16_t{0xFFFE}, uint16_t{0xFFFF}})
        EXPECT_EQ(consoleSessionEndCode(rune::cli::console::consoleRejectCode(static_cast<int16_t>(code)), 0), code) << code;
}

TEST(ConsoleOfflineRegression, ScriptModeReportsARejectedSessionLikeInteractiveRegression) {
    // (c) a session that ended exits with its end code - the statement that failed may carry another code (or none).
    using rune::cli::console::consoleExitStatus;
    using rune::cli::console::consoleScriptExitStatus;
    for (const uint16_t end : {uint16_t{44}, uint16_t{46}, uint16_t{48}, uint16_t{200}, uint16_t{0x8000}}) {
        for (const int batch : {1, 30, 125})
            EXPECT_EQ(consoleScriptExitStatus(batch, true, end), consoleExitStatus(end)) << "end " << end << " batch " << batch;
    }
    // no session end: the failing statement's status stands
    for (const int batch : {0, 1, 30, 125}) EXPECT_EQ(consoleScriptExitStatus(batch, false, 0), batch);
}

TEST(ConsoleOfflineRegression, ARefusedLoginAtStartExitsLikeARefusedReconnectRegression) {
    // (f) AuthenticationFailed (44) at start is the status a refused reconnect gives; any other open failure is 1.
    using rune::cli::console::consoleExitStatus;
    using rune::cli::console::consoleOpenFailureStatus;
    EXPECT_EQ(consoleOpenFailureStatus("connect(): broker error AuthenticationFailed (44)"), consoleExitStatus(44));
    EXPECT_EQ(consoleOpenFailureStatus("AuthenticationFailed"), 44);
    EXPECT_EQ(consoleOpenFailureStatus("connect(): timed out waiting"), 1);
    EXPECT_EQ(consoleOpenFailureStatus("NotAuthorized"), 1);
    EXPECT_EQ(consoleOpenFailureStatus(""), 1);
}

TEST(ConsoleOfflineRegression, StreamSourceDiscardsAnOverlongLineLikeTheFdSourceRegression) {
    // (e) the dry-run interactive stream source caps a line at 64 KiB exactly as the fd source does (#307): the long
    // line is reported and dropped, a statement in progress is abandoned, the next line still runs.
    using rune::cli::console::kInputLineTooLongMessage;
    using rune::cli::console::kMaxInputLineBytes;
    const std::string good = "topic b { x int32 } partitions 1 key x;\n";
    for (const std::size_t len : {kMaxInputLineBytes, kMaxInputLineBytes + 1, std::size_t{70 * 1024}}) {
        const auto r = console({"--dry-run"}, std::string(len, 'x') + "\n" + good);
        EXPECT_EQ(r.rc, 0) << len << ": " << r.err;
        EXPECT_NE(r.err.find(std::string(kInputLineTooLongMessage)), std::string::npos) << len << ": " << r.err.substr(0, 300);
        EXPECT_EQ(count(r.err, std::string(kInputLineTooLongMessage)), 1u) << len;
        EXPECT_EQ(count(r.out, "partitions"), 1u) << "the next line still ran: " << r.out;
    }
    // One byte under the cap is an ordinary (here: unparsable) line, not a discard.
    const auto under = console({"--dry-run"}, std::string(kMaxInputLineBytes - 2, 'x') + ";\n" + good);   // cap-1 bytes
    EXPECT_EQ(under.err.find(std::string(kInputLineTooLongMessage)), std::string::npos) << under.err.substr(0, 300);
    EXPECT_EQ(count(under.out, "partitions"), 1u) << under.out;
    // An over-long line without a final newline, and one that ends the input, are dropped too (no hang, no run).
    const auto tail = console({"--dry-run"}, good + std::string(kMaxInputLineBytes + 5, 'y'));
    EXPECT_EQ(count(tail.err, std::string(kInputLineTooLongMessage)), 1u) << tail.err.substr(0, 300);
    EXPECT_EQ(count(tail.out, "partitions"), 1u) << tail.out;
    // A statement in progress is abandoned by the long line, never joined with what follows.
    const auto joined = console({"--dry-run"}, "topic a\n" + std::string(70 * 1024, 'x') + "\npartitions 1;\n" + good);
    EXPECT_NE(joined.err.find("the statement in progress was abandoned (an input line was too long)"), std::string::npos)
        << joined.err.substr(0, 400);
    EXPECT_EQ(joined.out.find("topic a"), std::string::npos) << joined.out;
}

// ── Adversarial review of client batch C8 (#301 #302 #303 #307 #308) ────────────────────────────────────────────────

TEST(ConsoleOfflineRegression, ExitStatusNeverCollidesAtAnyCodeAdversarialRegression) {
    // #301: every 16-bit broker code maps to 4..125; never 0 (success), 1/2/3 (local codes) or a value a shell
    // truncates (256 would be 0 mod 256, 257 would be 1).
    using rune::cli::console::consoleExitStatus;
    const std::pair<uint16_t, int> edges[] = {{3, 125},   {4, 4},     {5, 5},     {123, 123}, {124, 124}, {125, 125},
                                              {126, 125}, {127, 125}, {128, 125}, {255, 125}, {256, 125}, {257, 125},
                                              {258, 125}, {259, 125}, {0x7FFF, 125}, {0x8000, 125}, {0xFFFF, 125}};
    for (const auto& [code, want] : edges) EXPECT_EQ(consoleExitStatus(code), want) << "broker code " << code;
    for (uint32_t c = 0; c <= 0xFFFF; ++c) {
        const int s = consoleExitStatus(static_cast<uint16_t>(c));
        ASSERT_TRUE(s >= 4 && s <= 125) << "code " << c << " -> " << s;
        ASSERT_TRUE(s == static_cast<int>(c) || s == 125) << "code " << c << " -> " << s << ": a code is never remapped to another code";
    }
}

TEST(ConsoleOfflineRegression, EverySessionRejectCodeEndsTheSessionAdversarialRegression) {
    // #308: reject codes 44..48 and unknown ones (a newer broker) each end the session with their own code, whatever the
    // failed statement's code was; without a reject only SessionRevoked (46) ends it.
    using rune::cli::console::consoleSessionEndCode;
    const uint16_t rejects[] = {44, 45, 46, 47, 48, 60, 0x7FFF, 0x8000, 0xFFFF};
    const uint16_t stmts[]   = {0, 1, 7, 30, 44, 45, 46, 47, 48, 0xFFFF};
    for (const uint16_t r : rejects)
        for (const uint16_t s : stmts) EXPECT_EQ(consoleSessionEndCode(r, s), r) << "reject " << r << " statement " << s;
    for (const uint16_t s : stmts)
        EXPECT_EQ(consoleSessionEndCode(0, s), s == 46 ? 46 : 0) << "no reject, statement " << s
            << ": a statement error other than SessionRevoked keeps the session (DD7)";
}

namespace {
std::string ridStatements(std::size_t n) {
    std::string s;
    for (std::size_t i = 0; i < n; ++i) s += "topic t" + std::to_string(i) + " { x int32 } partitions 1 key x;\n";
    return s;
}
} // namespace

TEST(ConsoleOfflineRegression, RequestIdAtMaxMaxMinusOneAndWrapAdversarialRegression) {
    // #303: statement N uses lo + N - 1.  lo = max fits one statement only; lo = max-1 fits two; the refusal names the
    // first statement whose id would wrap.
    const std::string hiMax = "0000000000000001ffffffffffffffff", hiMax1 = "0000000000000001fffffffffffffffe";
    struct C { std::string rid; std::size_t n; int rc; std::string needle; };
    const C cases[] = {
        {hiMax, 1, 0, "request-id 0000000000000001ffffffffffffffff\n"},
        {hiMax, 2, 1, "overflow at statement 2 of 2 "},
        {hiMax1, 2, 0, "request-id 0000000000000001ffffffffffffffff\n"},
        {hiMax1, 3, 1, "overflow at statement 3 of 3 "},
        {hiMax1, 9, 1, "overflow at statement 3 of 9 "},
        {"ffffffffffffffff0000000000000000", 9, 0, "request-id ffffffffffffffff0000000000000008\n"},   // hi never carries
    };
    for (const auto& c : cases) {
        const fs::path f = writeScript("c8adv_rid.rql", ridStatements(c.n));
        const auto r = console({"-f", f.string(), "--dry-run", "--request-id", c.rid});
        EXPECT_EQ(r.rc, c.rc) << c.rid << " x" << c.n << ": " << r.err;
        EXPECT_NE((c.rc == 0 ? r.out : r.err).find(c.needle), std::string::npos) << c.rid << " x" << c.n << ": " << r.out << r.err;
        if (c.rc != 0) EXPECT_TRUE(r.out.empty()) << "nothing is planned before the refusal: " << r.out;
        if (c.rc == 0) EXPECT_EQ(count(r.out, "request-id "), c.n) << r.out;
    }
}

TEST(ConsoleOfflineRegression, RequestIdRefusalOrderAndScriptFromStdinAdversarialRegression) {
    const std::string hiMax = "0000000000000001ffffffffffffffff";
    // `-f -` reads the script from stdin: still script mode, still refused.
    const auto fromStdin = consoleWithoutBrokerEnv({"-f", "-", "--request-id", hiMax}, ridStatements(2));
    EXPECT_EQ(fromStdin.rc, 1) << fromStdin.err;
    EXPECT_NE(fromStdin.err.find("would overflow at statement 2 of 2"), std::string::npos) << fromStdin.err;
    // Interactive mode cannot take --request-id at all (the claimed reason #303 needs no per-statement guard there).
    for (const bool dry : {true, false}) {
        std::vector<std::string> a{"--request-id", hiMax};
        if (dry) a.push_back("--dry-run");
        const auto i = consoleWithoutBrokerEnv(a, ridStatements(3));
        EXPECT_EQ(i.rc, 2) << i.err;
        EXPECT_NE(i.err.find("--request-id needs -f"), std::string::npos) << i.err;
        EXPECT_TRUE(i.out.empty()) << "no statement ran: " << i.out;
    }
    // A syntax error is reported (rc 1, statement position) before the overflow; an empty script never overflows.
    const fs::path bad = writeScript("c8adv_rid_bad.rql", ridStatements(2) + "topic ;\n");
    const auto b = console({"-f", bad.string(), "--dry-run", "--request-id", hiMax});
    EXPECT_EQ(b.rc, 1);
    EXPECT_NE(b.err.find("c8adv_rid_bad.rql:statement 3: "), std::string::npos) << b.err;
    EXPECT_EQ(b.err.find("would overflow"), std::string::npos) << b.err;
    const auto e = console({"-f", writeScript("c8adv_rid_empty.rql", "-- nothing\n").string(), "--dry-run", "--request-id", hiMax});
    EXPECT_EQ(e.rc, 0) << e.err;
    EXPECT_TRUE(e.err.empty()) << e.err;
}

namespace {
/// Runs the dry-run REPL over the REAL 64 KiB PollingFdLineSource reading a file (no writer thread needed).
struct PolledRepl { int rc = -1; std::string out, err; };
PolledRepl replOverFile(const std::string& name, const std::string& body) {
    const fs::path p = writeScript(name, body);
    const int fd = ::open(p.c_str(), O_RDONLY);
    if (fd < 0) return {};
    int idle = 0;
    std::ostringstream out, err;
    PolledRepl r;
    {
        rune::cli::console::PollingFdLineSource src(fd, &countIdle, &idle);
        src.err = &err;
        r.rc = rune::cli::console::runInteractiveDryRunForTest(src, out, err);
    }
    ::close(fd);
    r.out = out.str();
    r.err = err.str();
    return r;
}
const std::string kLong(70 * 1024, 'z');
} // namespace

TEST(ConsoleOfflineRegression, DiscardInEveryReplStateAdversarialRegression) {
    // #307 through the real reader: the over-long line arrives in every parser state the REPL can be in.
    struct C { const char* name; std::string body; std::size_t abandoned; std::size_t planned; std::string mustNotPlan; };
    const C cases[] = {
        // the issue's repro: the halves around the dropped line are never joined into "topic t { a int32 } ..."
        {"repro", "topic t {\n" + kLong + "\na int32 } partitions 1 key a;\n", 1, 0, "topic t "},
        // inside an open string literal and an open block comment
        // (a REPL input is buffered until it ends in ';', so the complete `topic s` before the open string goes with it; the
        // leftover "');" is an unterminated string again, so it swallows the next line into one parse error - the
        // abandoned text is never planned)
        {"string", "topic s { a string } partitions 1 key a; insert into s values ('abc\n" + kLong + "\n');\ntopic ok { x int32 } partitions 1 key x;\n", 1, 0, "topic s "},
        {"comment", "/* open\n" + kLong + "\n*/ topic c { x int32 } partitions 1 key x;\ntopic ok { x int32 } partitions 1 key x;\n", 1, 1, "topic c "},
        // two dropped lines in a row: one abandonment, not two
        {"twice", "topic d {\n" + kLong + "\n" + kLong + "\nx int32 } partitions 1 key x;\ntopic ok { x int32 } partitions 1 key x;\n", 1, 1, "topic d "},
        // a complete statement and the dropped line on ONE physical line: the whole line goes, nothing runs from it
        {"sameline", "topic e { x int32 } partitions 1 key x; -- " + kLong + "\ntopic ok { x int32 } partitions 1 key x;\n", 0, 1, "topic e "},
        // dropped line while awaiting, then EOF without a newline: no "input ended inside a statement"
        {"eof", "topic ok { x int32 } partitions 1 key x;\n" + kLong, 0, 1, ""},
        // dropped line mid-statement, then exit: exit is honoured as a fresh command, not a continuation
        {"exit", "topic f {\n" + kLong + "\nexit\ntopic g { x int32 } partitions 1 key x;\n", 1, 0, "topic g "},
    };
    for (const auto& c : cases) {
        const auto r = replOverFile(std::string("c8adv_discard_") + c.name + ".rql", c.body);
        EXPECT_EQ(r.rc, 0) << c.name << ": " << r.err;
        EXPECT_EQ(count(r.err, "input line too long"), c.body.size() > 2 * kLong.size() ? 2u : 1u) << c.name << ": " << r.err;
        EXPECT_EQ(count(r.err, "was abandoned"), c.abandoned) << c.name << ": " << r.err;
        EXPECT_EQ(count(r.err, "input ended inside a statement"), 0u) << c.name << ": " << r.err;
        EXPECT_EQ(count(r.out, " partitions 1"), c.planned) << c.name << ": " << r.out << "\nerr: " << r.err.substr(0, 600);
        if (!c.mustNotPlan.empty()) EXPECT_EQ(r.out.find(c.mustNotPlan), std::string::npos) << c.name << ": " << r.out;
        EXPECT_EQ(r.out.find("zzzz"), std::string::npos) << c.name << ": no byte of the dropped line is planned";
    }
}

TEST(ConsoleOfflineRegression, LineLengthBoundaryIsExactAdversarialRegression) {
    // 65535 bytes + '\n' fills the 64 KiB buffer exactly and is a line; one byte more is dropped.  Neither leaks into
    // the following line.
    const auto fits = replOverFile("c8adv_len_fit.rql", "-- " + std::string(65535 - 3, 'q') + "\ntopic ok { x int32 } partitions 1 key x;\n");
    EXPECT_EQ(fits.err.find("too long"), std::string::npos) << fits.err;
    EXPECT_EQ(count(fits.out, " partitions 1"), 1u) << fits.out;
    const auto over = replOverFile("c8adv_len_over.rql", "-- " + std::string(65536 - 3, 'q') + "\ntopic ok { x int32 } partitions 1 key x;\n");
    EXPECT_EQ(count(over.err, "input line too long"), 1u) << over.err;
    EXPECT_EQ(count(over.out, " partitions 1"), 1u) << "the next line is read whole: " << over.out;
    EXPECT_EQ(over.err.find("1:1: "), std::string::npos) << "the tail of the dropped line is not parsed: " << over.err;
}

// Issue #225: the CLI prints the auth names from the client's one source (authErrorName); the rest stay on its table.
TEST(CliErrorNamesRegression, CliAndClientNamesAgreeFor44To48Regression) {
    for (uint16_t code = 44; code <= 48; ++code) {
        SCOPED_TRACE(code);
        ASSERT_NE(rune::client::authErrorName(code), nullptr);
        EXPECT_EQ(rune::cli::engine::errorCodeName(code), std::string_view(rune::client::authErrorName(code)));
    }
    EXPECT_EQ(rune::cli::engine::errorCodeName(43), "PartitionDataReset");
    EXPECT_EQ(rune::cli::engine::errorCodeName(49), "RecordEgressUnavailable");
}

// C7 adversarial review (#225) input bounds: the printed failure line carries the client's auth name for 44..48;
// the table's ends and codes past it still resolve (0, the last code, last+1, u16 max).
TEST(CliErrorNamesRegression, BrokerFailureLinesAndTableBoundsAdversarialRegression) {
    for (uint16_t code = 44; code <= 48; ++code) {
        SCOPED_TRACE(code);
        EXPECT_EQ(rune::cli::engine::brokerFailure(code, "d"),
                  std::string(rune::client::authErrorName(code)) + " (" + std::to_string(code) + ") d");
    }
    namespace pc = rune::protocol::client;
    constexpr auto kLast = static_cast<uint16_t>(pc::ErrorCode::ViewPaused);
    EXPECT_EQ(rune::cli::engine::errorCodeName(0), "OK");
    EXPECT_EQ(rune::cli::engine::errorCodeName(kLast), "ViewPaused");
    EXPECT_EQ(rune::cli::engine::errorCodeName(kLast + 1), "UnknownErrorCode");
    EXPECT_EQ(rune::cli::engine::errorCodeName(0xFFFF), "UnknownErrorCode");
    EXPECT_EQ(rune::cli::engine::brokerFailure(0xFFFF, ""), "UnknownErrorCode (65535)");
}

// ── #702: the live test seams honour -f ───────────────────────────────────────
// runLiveOnFdForTest / runLiveOnStdinForTest used to pass an EMPTY batch to the live session, so a `-f` script was
// silently never read, prepared or run (rc 0).  They now read and prepare the script exactly as `rune console -f`
// does, before any broker is needed (the endpoint below is never contacted).

// A missing script fails with the readScript error and rc 1.  RED before the fix: no script error (the seam went on
// to connect and ran nothing).
TEST(ConsoleSeamRegression, LiveSeamWithAMissingScriptFailsRegression) {
    const fs::path missing = fs::temp_directory_path() / ("rune_seam_missing_" + std::to_string(::getpid()) + ".rql");
    fs::remove(missing);
    for (const bool viaFd : {true, false}) {
        std::ostringstream out, err;
        uint64_t pumps = 0;
        const std::vector<std::string> args{"console", "--broker", "127.0.0.1:1", "-f", missing.string()};
        const int rc = viaFd ? rune::cli::console::runLiveOnFdForTest(args, STDIN_FILENO, out, err, &pumps)
                             : rune::cli::console::runLiveOnStdinForTest(args, nullptr, out, err, &pumps);
        EXPECT_EQ(rc, 1) << (viaFd ? "fd" : "stdin") << " seam: " << err.str();
        EXPECT_NE(err.str().find("cannot open file: " + missing.string()), std::string::npos) << err.str();
    }
}

// A script that does not parse fails with the prepare error and rc 1, before any broker is needed.  RED before the
// fix: rc 0 (an empty batch ran).
TEST(ConsoleSeamRegression, LiveSeamWithAnUnparseableScriptFailsRegression) {
    const fs::path f = writeScript("seam_bad.rql", "THIS IS NOT RUNE QL;\n");
    for (const bool viaFd : {true, false}) {
        std::ostringstream out, err;
        uint64_t pumps = 0;
        const std::vector<std::string> args{"console", "--broker", "127.0.0.1:1", "-f", f.string()};
        const int rc = viaFd ? rune::cli::console::runLiveOnFdForTest(args, STDIN_FILENO, out, err, &pumps)
                             : rune::cli::console::runLiveOnStdinForTest(args, nullptr, out, err, &pumps);
        EXPECT_EQ(rc, 1) << (viaFd ? "fd" : "stdin") << " seam: " << err.str();
        EXPECT_NE(err.str().find(":statement 1:"), std::string::npos) << err.str();
    }
}

// The fd seam has no stream for `-f -`: a usage error (2), never a silent empty run.
TEST(ConsoleSeamRegression, FdSeamRefusesAScriptFromStdinRegression) {
    std::ostringstream out, err;
    uint64_t pumps = 0;
    const int rc = rune::cli::console::runLiveOnFdForTest({"console", "--broker", "127.0.0.1:1", "-f", "-"},
                                                          STDIN_FILENO, out, err, &pumps);
    EXPECT_EQ(rc, 2) << err.str();
    EXPECT_NE(err.str().find("-f -"), std::string::npos) << err.str();
}
