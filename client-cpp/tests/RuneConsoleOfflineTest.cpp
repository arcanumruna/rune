// tests/RuneConsoleOfflineTest.cpp — Phase 2b Part D: `rune console` offline (--dry-run, no broker).
// Drives rune::cli::run(args, in, out, err) with an injected istream: no TTY needed.
// Also carries the migrated `rune schema apply|alter --dry-run` assertions (Task 5 table).
#include <gtest/gtest.h>

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "rune/cli/Cli.h"
#include "rune/cli/Console.h"
#include "rune/ql/Parser.h"

#include "QlEngine.h"   // src/cli: splitStatements, the engine's internal seams

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
