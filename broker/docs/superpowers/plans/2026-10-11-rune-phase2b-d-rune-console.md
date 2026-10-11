# Rune Phase 2b — Part D: `rune console` (Rune QL); `rune schema` read-only — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rune QL scripts of one or more `;`-terminated statements are parsed by a new `rune::ql::parseScript`. A new `rune console` command runs them (`topic …;`, `transient topic …;`, `alter topic …;`) either interactively (`rune> ` prompt) or from a file (`-f`), using one broker session. `rune schema` becomes read-only (`list|get|generate`), and the old `schema apply|alter` assertions move to console tests.

**Architecture:** rune-ql gets `parseScript`, which splits the source at top-level `;` tokens using the existing lexer (so a `;` inside a `--` comment never splits). It hands each statement's exact source slice to the unchanged single-statement `parse()` and then maps error positions back to absolute `line:col`. It also gets `classifyInput`, which tells the console whether its buffered input is empty, incomplete or complete. In rune-client-cpp, the shared CLI helpers move out of `Cli.cpp` into `src/cli/CliCommon.{h,cpp}`. A new `src/cli/Console.{h,cpp}` holds the console: a two-state REPL (`AwaitingStatement` / `ContinuingStatement{text}`) and a script runner. Both are templated on an executor: `DryRunExecutor` prints descriptors and alter plans with no broker, and `LiveExecutor` drives one connected `rune::client::Client`. All input comes through an injected `std::istream`, so every path is tested without a TTY.

**Tech Stack:** C++23, GoogleTest, Google Benchmark, CMake (rune-ql: conan `build/build/Release`, Unix Makefiles; rune-client-cpp: conan `build`, Unix Makefiles, real Aeron), Aeron loopback `CliTestGateway` for live tests, docker e2e scripts for the real broker.

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` §D (binding). Cross-plan: §E (transient topics) — consumed, see *Prerequisites*.

**Builds on:** Phase 2a (`feature/materialized-views`: broker 6965024, protocol d0578c4, client 0e8505c) **and Part E** (`docs/superpowers/plans/2026-10-11-rune-phase2b-e-*.md`, build order C → E → D).

## Execution context

- Branch `feat/phase2b-d-rune-console` in each touched repo, cut from that repo's integration branch **after Part E has merged into it**. If E has not merged yet, cut from E's branch tip (`feat/phase2b-e-…`) and rebase once E lands:
  - **rune-ql**: `/Users/vradu/Projects/se-worktrees/rune-ql`. Integration branch `feat/schema-evolution-durable-tags` (current line for ql/codec).
  - **rune-client-cpp**: `/Users/vradu/Projects/se-worktrees/rune-client-cpp`. Integration branch `feature/materialized-views` (the worktree's `feat/view-engine-phase2a` == it).
  - **rune-broker**: `/Users/vradu/Projects/rune-broker-matviews`. Integration branch `feature/materialized-views`. Task 6 only: one docker e2e script, no C++.
  ```bash
  git -C /Users/vradu/Projects/se-worktrees/rune-ql       switch -c feat/phase2b-d-rune-console
  git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp switch -c feat/phase2b-d-rune-console
  git -C /Users/vradu/Projects/rune-broker-matviews        switch -c feat/phase2b-d-rune-console   # Task 6 only
  ```
- Canonical sibling layout. The client's CMake compiles `../rune-ql/src/*.cpp` and `../rune-codec`, so `se-worktrees/rune-ql` must be on `feat/phase2b-d-rune-console` while the client builds.
- **rune-ql build** (existing conan dir): `cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build/build/Release -j --target rune_ql_tests rune_ql_bench`.
  - Regression gate: `/Users/vradu/Projects/se-worktrees/rune-ql/build/build/Release/rune_ql_tests --gtest_filter='*Regression*'`.
  - Full suite: same binary with no filter.
- **rune-client-cpp build** (existing conan dir, real Aeron): `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j`.
  - Regression gate: `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R Regression --output-on-failure`.
  - CLI suites: `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|CliLive|Console' --output-on-failure`.
- **rune-broker** (Task 6): regression gate `ctest --test-dir /Users/vradu/Projects/rune-broker-matviews/build-fast -R Regression --output-on-failure` (build dir `build-fast`, Ninja).
- CLAUDE.md #7: in every touched repo, the full `*Regression*` suite runs and is green **before** each commit. Every fix or behavior pin gets a `*Regression*`-named test.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Prerequisites (Part E): verify before Task 1, adapt in exactly three places

Part D's console executes `transient topic …;`. That depends on Part E's outputs, which this plan uses by these exact names:

| Part E output | Used by | Where in this plan |
|---|---|---|
| rune-ql `Ast.h`: `enum class Persistence : uint8_t { Disk = 0, Transient = 1 };` and `TopicDef::persistence` (default `Disk`) | `isTransient()`, `toClientPersistence()` | Task 3 `Console.cpp`, Task 4 |
| rune-ql `parse("transient topic …;")` → `TopicDef{.persistence = Transient}` | `parseScript` delegates to `parse()` | Task 1 test `TransientTopicDelegatesToParse` |
| client `rune::client::Persistence { Disk, Transient }` and `DefineTopicResult Client::defineTopicAndWait(std::string_view name, uint32_t partitions, std::string_view keyField, bool validate, std::span<const uint8_t> descriptorBlob, Persistence persistence, std::chrono::nanoseconds timeout)` | `LiveExecutor::define` | Task 4 |
| protocol `rpc::DefineTopic::persistence` (appended byte, decoded) | test gateway capture | Task 4 `RuneCliLiveTest.cpp` |

- [ ] **Verify:**
```bash
grep -n "enum class Persistence" /Users/vradu/Projects/se-worktrees/rune-ql/include/rune/ql/Ast.h
grep -n "persistence" /Users/vradu/Projects/se-worktrees/rune-ql/include/rune/ql/Ast.h
grep -n "Persistence" /Users/vradu/Projects/se-worktrees/rune-client-cpp/include/rune/client/Client.h
grep -n "persistence" /Users/vradu/Projects/se-worktrees/rune-protocol/include/rune/protocol/client/SchemaMessages.h
```
Expected: each grep returns at least one hit. **If any Part E name differs, change only these three spots:** `toClientPersistence` and the `defineTopicAndWait` call in `Console.cpp` (Task 4), and the `opt->persistence` capture in `RuneCliLiveTest.cpp` (Task 4). Never edit Part E's code from this plan. If a grep finds nothing, E has not landed: stop and report it.

## Global Constraints

- HARD RULES (CLAUDE.md):
  - Shared-nothing; ring buffers are the only cross-thread passing.
  - State machines whose states hold exactly their own variables (the REPL is `std::variant<AwaitingStatement, ContinuingStatement>`).
  - Zero steady-state allocation on hot paths. The console is a control-plane CLI and touches no produce/consume/query hot path.
  - A regression test on every fix, with a suite name containing `Regression`; run the full `*Regression*` suite first; fix-on-sight.
- Spec §D, verbatim:
  - Command line: `rune console [--broker <aeron channel>] [--cli-egress <ch>] [-f script.rql]`. The broker also comes from `RUNE_BROKER` / `RUNE_EGRESS` (existing `buildClientConfig`).
  - Interactive: prompt `rune> `, continuation `   ...> ` until a `;` outside comments.
  - Each statement executes with **one connected client for the whole session**.
  - Output: `ok topic player v1 (8 partitions)` / `ok topic player v2`, or the error: `line:col: message` for parse errors, the broker error name for broker errors.
  - An error never ends the session. `exit` / `quit` / EOF leaves.
  - `-f script.rql`: same execution over a file; stops at the first error, exit code non-zero.
  - Statements in 2b: `topic …;`, `transient topic …;`, `alter topic …;`.
- `parseScript(std::string_view) → std::expected<std::vector<Statement>, ParseError>`: one or more statements, each `;`-terminated, `--` comments allowed, positions reported per statement. `parse()` stays for single statements.
- `rune schema` keeps `list`, `get`, `generate` and **removes `apply` and `alter`**. Their offline/live tests migrate to console tests with the same assertions (mapping table in Task 5). Known limitation carried forward: `schema list` shows only topics known to the session.
- Published names (arcanum docs v0.5.6) must match exactly: the `rune>` console with statements `topic …;`, `transient topic …;`, `alter topic …;`, and `rune schema list|get|generate`.
- Console exit codes:
  - `0`: success, or any interactive session that ended by `exit`/`quit`/EOF.
  - `1`: usage error, I/O error, connect failure, timeout or transport exception.
  - `2`: script parse error.
  - Otherwise, the broker's wire `errorCode` of the first failing statement in `-f` mode. This keeps the old CLI convention that `docker/schema-evolution-e2e.sh` asserts (30 = SchemaIncompatible).
- Wire enums are append-only. This part adds no wire field; Part E owns the persistence byte.

## Review Focus

These are the input classes most likely to bite a user. Each is pinned by a named test in its owning task:

1. **A `;` inside a `--` comment** (`topic t { a int32 } -- keys next;`) must not end the statement or run it half-typed. Pinned by `ClassifyInputRegression.SemicolonInsideCommentIsIncomplete` and `ParseScriptRegression.SemicolonInCommentDoesNotSplit` (Task 1), and `ConsoleOfflineRegression.SemicolonInsideCommentDoesNotEndStatement` (Task 3).
2. **The final statement has no `;`.** This happens with a file saved without one, or with EOF in the middle of an interactive statement. It must be reported with a position and must never be silently dropped or executed. Pinned by `ParseScriptRegression.UnterminatedFinalStatementIsErrorNotDropped` (Task 1) and `ConsoleOfflineRegression.UnterminatedStatementIsReportedNotExecuted` (Task 3).
3. **A parse error in statement N of a script** must point at the absolute `line:col` in the file, not at a position relative to the statement. Pinned by `ParseScriptRegression.ErrorPositionIsAbsoluteOnSameLine` / `…AcrossLines` (Task 1) and `ConsoleOffline.ScriptStopsAtFirstParseErrorWithNonZeroExit` (Task 3).
4. **CRLF line endings** (a Windows-edited `.rql`, or a terminal sending `\r\n`) must parse, and `exit\r` must still leave. Pinned by `ParseScriptRegression.CrlfLineEndings` (Task 1) and `ConsoleOfflineRegression.CrlfInputParsesAndExits` (Task 3).
5. **A mistyped or valueless flag** (`rune console -f`, `rune console --file x.rql`) must be a usage error. It must not silently fall through to an interactive session that blocks on stdin. Pinned by `ConsoleOfflineRegression.MissingValueAndUnknownArgumentAreUsageErrors` (Task 3).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-ql/include/rune/ql/Parser.h` | `Statement` alias, `parseScript`, `InputState`, `classifyInput` | 1 |
| `rune-ql/src/Parser.cpp` | `parseScript` / `classifyInput` on the existing `Lexer` (appended) | 1 |
| `rune-ql/tests/ScriptTest.cpp` (new) | script + classify tests | 1 |
| `rune-ql/CMakeLists.txt` | add `tests/ScriptTest.cpp` | 1 |
| `rune-ql/benchmark/ParseBench.cpp` | `BM_ParseScript3`, `BM_ClassifyInput` | 1 |
| `rune-client-cpp/src/cli/CliCommon.h`, `CliCommon.cpp` (new) | shared helpers moved from `Cli.cpp` (flags, expected-version, readSource(istream), alter-plan text, live config/connect) | 2 |
| `rune-client-cpp/src/cli/Cli.cpp` | uses CliCommon (2); `run(args,in,out,err)` + `console` dispatch (3); apply/alter removed, usage (5) | 2, 3, 5 |
| `rune-client-cpp/include/rune/cli/Cli.h` | `run(args, in, out, err)` overload | 3 |
| `rune-client-cpp/src/cli/Console.h`, `Console.cpp` (new) | console: options, REPL state machine, script runner, dry-run executor (3), live executor (4) | 3, 4 |
| `rune-client-cpp/tests/RuneConsoleOfflineTest.cpp` (new) | offline console tests (+ migrated dry-run assertions) | 3 |
| `rune-client-cpp/tests/RuneCliLiveTest.cpp` | gateway: connects/validate/persistence capture, define error sequence; `runCli` stdin; console live tests (4); old apply/alter tests deleted (5) | 4, 5 |
| `rune-client-cpp/tests/RuneCliOfflineTest.cpp` | rewritten: removed-subcommand regression + usage | 5 |
| `rune-client-cpp/CMakeLists.txt` | new sources/tests (2, 3); comments (5, 6) | 2, 3, 5, 6 |
| `rune-client-cpp/docker/rune-ql-e2e.sh`, `docker/Dockerfile.rune-ql-probe`, `docker/docker-compose.rune-ql.yml`, `tests/e2e/*.rql`, `tests/e2e/*.cpp` comments | `rune console -f` instead of `schema apply` | 6 |
| `rune-broker/docker/schema-evolution-e2e.sh` | `rune console -f` instead of `schema apply|alter` | 6 |

---

### Task 1: rune-ql `parseScript` + `classifyInput`

**Files:**
- Modify: `rune-ql/include/rune/ql/Parser.h` (whole file, 8 lines)
- Modify: `rune-ql/src/Parser.cpp` (append before the final `} // namespace rune::ql`, currently line 488)
- Create: `rune-ql/tests/ScriptTest.cpp`
- Modify: `rune-ql/CMakeLists.txt:30-33` (test sources)
- Modify: `rune-ql/benchmark/ParseBench.cpp` (append)

**Interfaces:**
- Consumes: the existing `rune::ql::Lexer`, `Token`, `TK` and `parse()` in `src/Parser.cpp`. These are in namespace `rune::ql` and visible inside the TU. Part E's `transient topic` grammar comes along through `parse()`.
- Produces (rune-ql public API):
  ```cpp
  namespace rune::ql {
  using Statement = std::variant<TopicDef, AlterTopicDef>;
  std::expected<Statement, ParseError> parse(std::string_view src) noexcept;            // unchanged type
  std::expected<std::vector<Statement>, ParseError> parseScript(std::string_view src);
  enum class InputState : uint8_t { Empty, Incomplete, Complete };
  [[nodiscard]] InputState classifyInput(std::string_view src) noexcept;
  }
  ```
  `parseScript` is all-or-nothing: on error it returns no statements. Error `line`/`col` are absolute positions in `src`.

- [ ] **Step 1: Write the failing tests.** Create `rune-ql/tests/ScriptTest.cpp`:

```cpp
// tests/ScriptTest.cpp — Phase 2b Part D: multi-statement Rune QL scripts
// (parseScript) and console input classification (classifyInput).
#include <gtest/gtest.h>

#include <type_traits>
#include <variant>

#include "rune/ql/Parser.h"

using namespace rune::ql;

static_assert(std::is_same_v<Statement, std::variant<TopicDef, AlterTopicDef>>,
              "Statement is exactly the variant parse() returns");

// ── parseScript ──────────────────────────────────────────────────────────────

TEST(ParseScript, ParsesStatementsInOrder) {
    auto r = parseScript(
        "-- schema for the game\n"
        "topic player { id int64, name string } partitions 8 key id;\n"
        "alter topic player add column level int32;\n"
        "topic guild { id int64 } partitions 1 key id; -- trailing comment\n");
    ASSERT_TRUE(r.has_value()) << r.error().line << ":" << r.error().col << ": " << r.error().message;
    ASSERT_EQ(r->size(), 3u);

    const auto* p = std::get_if<TopicDef>(&(*r)[0]);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->name, "player");
    EXPECT_EQ(p->partitions, 8u);
    EXPECT_EQ(p->keyField, "id");

    const auto* a = std::get_if<AlterTopicDef>(&(*r)[1]);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->name, "player");
    ASSERT_EQ(a->ops.size(), 1u);
    EXPECT_EQ(a->ops[0].kind, AlterOpKind::Add);
    EXPECT_EQ(a->ops[0].field.name, "level");

    const auto* g = std::get_if<TopicDef>(&(*r)[2]);
    ASSERT_NE(g, nullptr);
    EXPECT_EQ(g->name, "guild");
}

TEST(ParseScript, MultiLineStatement) {
    auto r = parseScript(
        "topic characters {\n"
        "    id     int64,\n"
        "    userId int64,\n"
        "    code   string(8)\n"
        "} partitions 16\n"
        "  key userId;\n");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    ASSERT_EQ(r->size(), 1u);
    const auto& t = std::get<TopicDef>((*r)[0]);
    EXPECT_EQ(t.fields.size(), 3u);
    EXPECT_EQ(t.partitions, 16u);
}

TEST(ParseScript, SingleStatementMatchesParse) {
    constexpr std::string_view kDdl =
        "topic characters { id int64, userId int64, name string } partitions 16 key userId;";
    auto one = parse(kDdl);
    auto many = parseScript(kDdl);
    ASSERT_TRUE(one.has_value());
    ASSERT_TRUE(many.has_value());
    ASSERT_EQ(many->size(), 1u);
    const auto& a = std::get<TopicDef>(*one);
    const auto& b = std::get<TopicDef>((*many)[0]);
    EXPECT_EQ(a.name, b.name);
    EXPECT_EQ(a.partitions, b.partitions);
    EXPECT_EQ(a.keyField, b.keyField);
    ASSERT_EQ(a.fields.size(), b.fields.size());
    for (std::size_t i = 0; i < a.fields.size(); ++i) {
        EXPECT_EQ(a.fields[i].name, b.fields[i].name);
        EXPECT_EQ(a.fields[i].type, b.fields[i].type);
    }
}

TEST(ParseScript, EmptyScriptIsError) {
    for (std::string_view src : {std::string_view(""), std::string_view("-- only a comment\n  \n")}) {
        auto r = parseScript(src);
        ASSERT_FALSE(r.has_value()) << "'" << src << "'";
        EXPECT_EQ(r.error().message, "script contains no statements");
    }
}

// ── Regression suite (CLAUDE.md #6) ──────────────────────────────────────────

// Statement 2 starts at column 41 of line 1; its unknown type sits at slice
// col 13 -> absolute 1:53, never the slice-relative 1:13.
TEST(ParseScriptRegression, ErrorPositionIsAbsoluteOnSameLine) {
    auto r = parseScript(
        "topic a { x int32 } partitions 1 key x; topic b { x int128 } partitions 1 key x;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 1u);
    EXPECT_EQ(r.error().col, 53u);
    EXPECT_NE(r.error().message.find("int128"), std::string::npos) << r.error().message;
}

// Statement 2 starts on line 4; the bad type is on its second line (line 5, col 5).
TEST(ParseScriptRegression, ErrorPositionIsAbsoluteAcrossLines) {
    auto r = parseScript(
        "-- header\n"
        "topic a { x int32 } partitions 1 key x;\n"
        "\n"
        "topic b {\n"
        "  x int128\n"
        "} partitions 1 key x;\n");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 5u);
    EXPECT_EQ(r.error().col, 5u);
}

// A final statement without ';' is an error at end of input, never silently
// dropped (which would "succeed" with one statement instead of two).
TEST(ParseScriptRegression, UnterminatedFinalStatementIsErrorNotDropped) {
    auto r = parseScript(
        "topic a { x int32 } partitions 1 key x;\n"
        "topic b { x int32 } partitions 1 key x");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 2u);
    EXPECT_EQ(r.error().col, 39u);
    EXPECT_EQ(r.error().message, "expected ;, got ''");
}

TEST(ParseScriptRegression, SemicolonInCommentDoesNotSplit) {
    auto r = parseScript(
        "topic a { x int32 -- not the end;\n"
        " } partitions 1 key x;\n");
    ASSERT_TRUE(r.has_value()) << r.error().line << ":" << r.error().col << ": " << r.error().message;
    EXPECT_EQ(r->size(), 1u);
}

TEST(ParseScriptRegression, TrailingGarbageIsError) {
    auto r = parseScript("topic a { x int32 } partitions 1 key x; garbage");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 1u);
    EXPECT_EQ(r.error().col, 41u);
    EXPECT_NE(r.error().message.find("garbage"), std::string::npos) << r.error().message;
}

TEST(ParseScriptRegression, CrlfLineEndings) {
    auto r = parseScript(
        "topic a { x int32 }\r\n"
        "partitions 1 key x;\r\n"
        "alter topic a add column y int32;\r\n");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    EXPECT_EQ(r->size(), 2u);
}

// parseScript must delegate each statement to parse(), so Part E's
// `transient topic` grammar works in scripts with no script-side change.
TEST(ParseScriptRegression, TransientTopicDelegatesToParse) {
    auto r = parseScript(
        "transient topic ticks { id int64 } partitions 2 key id;\n"
        "topic trades { id int64 } partitions 2 key id;\n");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    ASSERT_EQ(r->size(), 2u);
    EXPECT_EQ(std::get<TopicDef>((*r)[0]).persistence, Persistence::Transient);
    EXPECT_EQ(std::get<TopicDef>((*r)[1]).persistence, Persistence::Disk);
}

// ── classifyInput ────────────────────────────────────────────────────────────

TEST(ClassifyInput, EmptyIncompleteComplete) {
    EXPECT_EQ(classifyInput(""), InputState::Empty);
    EXPECT_EQ(classifyInput("  \t\r\n"), InputState::Empty);
    EXPECT_EQ(classifyInput("-- just a comment\n"), InputState::Empty);
    EXPECT_EQ(classifyInput("topic t {\n"), InputState::Incomplete);
    EXPECT_EQ(classifyInput("exit"), InputState::Incomplete);
    EXPECT_EQ(classifyInput("topic t { a int32 } partitions 1 key a;"), InputState::Complete);
    EXPECT_EQ(classifyInput("topic t { a int32 } partitions 1 key a; -- done\n"), InputState::Complete);
    EXPECT_EQ(classifyInput("a; b"), InputState::Incomplete);
    EXPECT_EQ(classifyInput(";"), InputState::Complete);
}

TEST(ClassifyInputRegression, SemicolonInsideCommentIsIncomplete) {
    EXPECT_EQ(classifyInput("topic t { a int32 } -- partitions next;\n"), InputState::Incomplete);
}
```

`rune-ql/CMakeLists.txt`: replace the test executable source list
```cmake
    add_executable(rune_ql_tests
        tests/AstSmokeTest.cpp
        tests/ParserTest.cpp
        tests/BackendsTest.cpp)
```
with
```cmake
    add_executable(rune_ql_tests
        tests/AstSmokeTest.cpp
        tests/ParserTest.cpp
        tests/BackendsTest.cpp
        tests/ScriptTest.cpp)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build/build/Release -j --target rune_ql_tests`
Expected: compile error `no type named 'Statement' in namespace 'rune::ql'` / `use of undeclared identifier 'parseScript'`.

- [ ] **Step 3: Implement.** Replace `rune-ql/include/rune/ql/Parser.h` with:

```cpp
#pragma once
#include <cstdint>
#include <expected>
#include <string_view>
#include <variant>
#include <vector>
#include "rune/ql/Ast.h"
namespace rune::ql {

/// One parsed Rune QL statement.
using Statement = std::variant<TopicDef, AlterTopicDef>;

/// Parse exactly ONE ';'-terminated statement (nothing but whitespace /
/// comments may follow it).
std::expected<Statement, ParseError> parse(std::string_view src) noexcept;

/// Parse a script of one or more ';'-terminated statements (`--` comments
/// anywhere). All-or-nothing: on error no statement is returned. ParseError
/// line/col are ABSOLUTE positions in `src`. A script with no statement is an
/// error ("script contains no statements").
std::expected<std::vector<Statement>, ParseError> parseScript(std::string_view src);

/// Console input state of the buffered text `src`:
///   Empty      — only whitespace / comments;
///   Incomplete — has tokens, and the last token is not ';';
///   Complete   — the last token outside comments is ';'.
enum class InputState : uint8_t { Empty, Incomplete, Complete };
[[nodiscard]] InputState classifyInput(std::string_view src) noexcept;

} // namespace rune::ql
```

`rune-ql/src/Parser.cpp`: insert immediately before the final line `} // namespace rune::ql`:

```cpp
// ── Scripts (Phase 2b Part D) ────────────────────────────────────────────────
//
// parseScript splits `src` at each top-level ';' token (the Lexer already
// skips `--` comments, so a ';' inside a comment never splits) and hands each
// statement's exact source slice to parse(). Every grammar change to parse()
// (e.g. Part E's `transient topic`) therefore applies to scripts unchanged.
// parse() reports positions relative to the slice; rebase() maps them back to
// absolute line:col in `src`.

namespace {

ParseError rebase(ParseError e, const Token& first) noexcept {
    if (e.line == 1) e.col += first.col - 1;   // slice line 1 starts at first.col
    e.line += first.line - 1;
    return e;
}

std::size_t offsetOf(std::string_view src, const Token& t) noexcept {
    return static_cast<std::size_t>(t.text.data() - src.data());
}

} // namespace

std::expected<std::vector<Statement>, ParseError> parseScript(std::string_view src) {
    Lexer lex{src};
    Token t = lex.next();
    if (t.kind == TK::Eof) {
        return std::unexpected(ParseError{t.line, t.col, "script contains no statements"});
    }
    std::vector<Statement> out;
    while (t.kind != TK::Eof) {
        const Token first = t;
        while (t.kind != TK::Semi && t.kind != TK::Eof) t = lex.next();
        const std::size_t begin = offsetOf(src, first);
        if (t.kind == TK::Eof) {
            // No terminating ';': let parse() locate the problem in the tail.
            auto r = parse(src.substr(begin));
            if (!r) return std::unexpected(rebase(std::move(r.error()), first));
            return std::unexpected(rebase(ParseError{1, 1, "statement is not terminated by ';'"}, first));
        }
        const std::size_t end = offsetOf(src, t) + 1;   // include the ';'
        auto r = parse(src.substr(begin, end - begin));
        if (!r) return std::unexpected(rebase(std::move(r.error()), first));
        out.push_back(std::move(*r));
        t = lex.next();
    }
    return out;
}

InputState classifyInput(std::string_view src) noexcept {
    Lexer lex{src};
    Token t = lex.next();
    if (t.kind == TK::Eof) return InputState::Empty;
    TK last = t.kind;
    for (t = lex.next(); t.kind != TK::Eof; t = lex.next()) last = t.kind;
    return last == TK::Semi ? InputState::Complete : InputState::Incomplete;
}
```

(`parse()`'s definition keeps its spelled-out return type `std::expected<std::variant<TopicDef, AlterTopicDef>, ParseError>`. It is the same type as `std::expected<Statement, ParseError>`, so the declaration matches.)

- [ ] **Step 4: Add the benchmarks.** Append to `rune-ql/benchmark/ParseBench.cpp`:

```cpp
// Phase 2b Part D: a three-statement console script (split + per-statement parse).
static constexpr std::string_view kScript3 =
    "-- three statements\n"
    "topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;\n"
    "alter topic characters add column level int32, set compatibility backward;\n"
    "topic guilds { id int64, name string } partitions 4 key id;\n";

static void BM_ParseScript3(benchmark::State& state) {
    for (auto _ : state) {
        auto result = rune::ql::parseScript(kScript3);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations() * 3);
}
BENCHMARK(BM_ParseScript3);

// The console calls classifyInput once per typed line; it must be allocation-free.
static void BM_ClassifyInput(benchmark::State& state) {
    for (auto _ : state) {
        auto s = rune::ql::classifyInput(kScript3);
        benchmark::DoNotOptimize(s);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ClassifyInput);
```

- [ ] **Step 5: Run the tests and bench**

Run:
```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build/build/Release -j --target rune_ql_tests rune_ql_bench
/Users/vradu/Projects/se-worktrees/rune-ql/build/build/Release/rune_ql_tests --gtest_filter='*Regression*'
/Users/vradu/Projects/se-worktrees/rune-ql/build/build/Release/rune_ql_tests
/Users/vradu/Projects/se-worktrees/rune-ql/build/build/Release/rune_ql_bench --benchmark_filter='BM_ParseCharacters|BM_ParseScript3|BM_ClassifyInput'
```
Expected: all tests PASS, including the pre-existing `ParserRegression.*`. Record the bench numbers in the commit body. `BM_ParseScript3` should be about 3× `BM_ParseCharacters` (no gate in the spec; this is a regression baseline). `BM_ClassifyInput` should be well under 1 µs.

- [ ] **Step 6: Commit (rune-ql)**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-ql add include/rune/ql/Parser.h src/Parser.cpp tests/ScriptTest.cpp CMakeLists.txt benchmark/ParseBench.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-ql commit -F - <<'EOF'
feat(ql): parseScript + classifyInput for multi-statement Rune QL scripts

parseScript splits at top-level ';' tokens (comments never split) and
delegates each slice to parse(), rebasing error positions to absolute
line:col. classifyInput drives the console's continuation prompt.
Bench: BM_ParseScript3 <n> ns, BM_ClassifyInput <n> ns (fill from Step 5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```
(Before committing, replace the two `<n>` with the measured numbers.)

---

### Task 2: Extract shared CLI helpers into `src/cli/CliCommon.{h,cpp}` (pure refactor)

**Files:**
- Create: `rune-client-cpp/src/cli/CliCommon.h`, `rune-client-cpp/src/cli/CliCommon.cpp`
- Modify: `rune-client-cpp/src/cli/Cli.cpp:32-105` (replace), `:127-252` (delete), `:306`, `:437` (readSource call sites)
- Modify: `rune-client-cpp/CMakeLists.txt:1372-1374` (rune_cli_lib sources)
- Test: existing `rune_cli_offline_tests`, `rune_cli_live_tests` (unchanged; must stay green)

**Interfaces:**
- Produces (`namespace rune::cli::detail`, internal header `src/cli/CliCommon.h`):
  ```cpp
  std::string flagValue(const std::vector<std::string>& args, std::string_view flag);
  bool hasFlag(const std::vector<std::string>& args, std::string_view flag);
  bool parseExpectedVersion(std::string_view flagStr, uint16_t& out, std::ostream& err);
  bool readSource(const std::string& path, std::istream& in, std::string& src, std::ostream& err); // "-" reads `in`
  void printAlterPlanText(const rune::ql::AlterTopicDef& ad, std::ostream& out);
  #ifdef RUNE_CLI_LIVE_ENABLED
  inline constexpr std::chrono::seconds kCliWait{10};
  bool isWaitTimeout(const std::exception& e);
  bool connectOrReport(rune::client::Client& client, std::ostream& err);
  rune::client::ClientConfig buildClientConfig(const std::vector<std::string>& args);
  #endif
  ```

- [ ] **Step 1: Baseline.** Run the client regression gate and CLI suites so the refactor has a green "before":

```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R Regression --output-on-failure
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|CliLive' --output-on-failure
```
Expected: all PASS. If anything fails, stop and fix it before refactoring, with a `*Regression*` test (CLAUDE.md #8).

- [ ] **Step 2: Create `src/cli/CliCommon.h`**

```cpp
// src/cli/CliCommon.h — helpers shared by the `rune` subcommands
// (`rune schema …`, `rune console`). Internal: not installed, not public API.
#pragma once

#include "rune/ql/Ast.h"

#include <cstdint>
#include <iosfwd>
#include <string>
#include <string_view>
#include <vector>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <chrono>
#include <exception>
#endif

namespace rune::cli::detail {

/// Value of `flag` in args (the next arg), or "" if absent / last.
std::string flagValue(const std::vector<std::string>& args, std::string_view flag);

/// True if `flag` appears in args.
bool hasFlag(const std::vector<std::string>& args, std::string_view flag);

/// Parse --expected-version defensively: "" means not given (out = 0 = no
/// CAS). Non-numeric, trailing garbage, negative or > 65535 is a clean error
/// written to err (returns false) — never an uncaught std::stoi exception.
bool parseExpectedVersion(std::string_view flagStr, uint16_t& out, std::ostream& err);

/// Read a whole source file into src; path "-" reads all of `in`.
bool readSource(const std::string& path, std::istream& in, std::string& src, std::ostream& err);

/// Human-readable (non-contract) dump of an 'alter topic' statement's ops.
void printAlterPlanText(const rune::ql::AlterTopicDef& ad, std::ostream& out);

#ifdef RUNE_CLI_LIVE_ENABLED
// F8c C3: the CLI waits with the *AndWait conveniences (no futures).
inline constexpr std::chrono::seconds kCliWait{10};

bool isWaitTimeout(const std::exception& e);

/// connectAndWait with the CLI's messages; closes the client on failure.
bool connectOrReport(rune::client::Client& client, std::ostream& err);

/// --broker / RUNE_BROKER, --cli-egress / RUNE_EGRESS, hidden test flags
/// --cli-stream-in/--cli-stream-out/--cli-data-dir.
rune::client::ClientConfig buildClientConfig(const std::vector<std::string>& args);
#endif

} // namespace rune::cli::detail
```

- [ ] **Step 3: Create `src/cli/CliCommon.cpp`.** The bodies are moved verbatim from `Cli.cpp`. The one behavior change: `readSource` reads `in` instead of `std::cin`.

```cpp
// src/cli/CliCommon.cpp — helpers shared by `rune schema …` and `rune console`.
// Bodies moved verbatim from src/cli/Cli.cpp (Phase 2b Part D, Task 2);
// readSource now reads an injected std::istream for "-" (testable without a TTY).

#include "CliCommon.h"

#include <charconv>
#include <fstream>
#include <istream>
#include <ostream>
#include <sstream>
#include <string_view>

#ifdef RUNE_CLI_LIVE_ENABLED
#include <cstdlib>
#endif

namespace rune::cli::detail {

std::string flagValue(const std::vector<std::string>& args, std::string_view flag) {
    for (std::size_t i = 0; i + 1 < args.size(); ++i) {
        if (args[i] == flag) return args[i + 1];
    }
    return {};
}

bool hasFlag(const std::vector<std::string>& args, std::string_view flag) {
    for (const auto& a : args) {
        if (a == flag) return true;
    }
    return false;
}

bool parseExpectedVersion(std::string_view flagStr, uint16_t& out, std::ostream& err) {
    if (flagStr.empty()) { out = 0; return true; }
    int value = 0;
    const auto res = std::from_chars(flagStr.data(), flagStr.data() + flagStr.size(), value);
    if (res.ec != std::errc{} || res.ptr != flagStr.data() + flagStr.size() ||
        value < 0 || value > 0xFFFF) {
        err << "error: --expected-version must be a non-negative integer (0..65535)\n";
        return false;
    }
    out = static_cast<uint16_t>(value);
    return true;
}

bool readSource(const std::string& path, std::istream& in, std::string& src, std::ostream& err) {
    std::ostringstream buf;
    if (path == "-") {
        buf << in.rdbuf();
    } else {
        std::ifstream f(path, std::ios::binary);
        if (!f) {
            err << "rune: cannot open file: " << path << "\n";
            return false;
        }
        buf << f.rdbuf();
    }
    src = buf.str();
    return true;
}

namespace {

std::string_view alterOpKindName(rune::ql::AlterOpKind k) {
    switch (k) {
        case rune::ql::AlterOpKind::Add:              return "ADD";
        case rune::ql::AlterOpKind::Drop:             return "DROP";
        case rune::ql::AlterOpKind::Rename:           return "RENAME";
        case rune::ql::AlterOpKind::SetCompatibility: return "SET_COMPATIBILITY";
    }
    return "UNKNOWN";
}

std::string_view primTypeNameForCli(rune::codec::PrimType t) {
    switch (t) {
        case rune::codec::PrimType::Int32:    return "int32";
        case rune::codec::PrimType::Int64:    return "int64";
        case rune::codec::PrimType::UInt16:   return "uint16";
        case rune::codec::PrimType::UInt32:   return "uint32";
        case rune::codec::PrimType::UInt64:   return "uint64";
        case rune::codec::PrimType::Float64:  return "float64";
        case rune::codec::PrimType::Bool:     return "bool";
        case rune::codec::PrimType::Enum:     return "enum";
        case rune::codec::PrimType::String:   return "string";
        case rune::codec::PrimType::Bytes:    return "bytes";
        case rune::codec::PrimType::FixedStr: return "string";
    }
    return "unknown";
}

} // namespace

void printAlterPlanText(const rune::ql::AlterTopicDef& ad, std::ostream& out) {
    out << "alter topic " << ad.name << "\n";
    for (const auto& op : ad.ops) {
        out << "  " << alterOpKindName(op.kind);
        switch (op.kind) {
            case rune::ql::AlterOpKind::Add:
                out << " " << op.field.name << " " << primTypeNameForCli(op.field.type);
                if (op.field.type == rune::codec::PrimType::FixedStr) {
                    out << "(" << op.field.fixedLen << ")";
                }
                break;
            case rune::ql::AlterOpKind::Drop:
                out << " " << op.name;
                break;
            case rune::ql::AlterOpKind::Rename:
                out << " " << op.name << " -> " << op.newName;
                break;
            case rune::ql::AlterOpKind::SetCompatibility:
                out << " " << (op.mode == rune::ql::Compatibility::Backward ? "BACKWARD" : "FORWARD");
                break;
        }
        out << "\n";
    }
}

#ifdef RUNE_CLI_LIVE_ENABLED

bool isWaitTimeout(const std::exception& e) {
    return std::string_view(e.what()).find("timed out") != std::string_view::npos;
}

bool connectOrReport(rune::client::Client& client, std::ostream& err) {
    try {
        client.connectAndWait(kCliWait);
        return true;
    } catch (const std::exception& e) {
        if (isWaitTimeout(e)) {
            err << "rune: connect timed out\n";
        } else {
            err << "rune error: " << e.what() << "\n";
        }
        client.close();
        return false;
    }
}

rune::client::ClientConfig buildClientConfig(const std::vector<std::string>& args) {
    rune::client::ClientConfig cfg;

    // Broker channel: --broker <ep> or RUNE_BROKER env.
    cfg.gatewayChannel = flagValue(args, "--broker");
    if (cfg.gatewayChannel.empty()) {
        if (const char* env = std::getenv("RUNE_BROKER"); env) cfg.gatewayChannel = env;
    }
    if (cfg.gatewayChannel.empty()) cfg.gatewayChannel = "aeron:ipc";

    // Egress channel: --cli-egress <ch> or RUNE_EGRESS env.  For a UDP gateway
    // this MUST be a client-reachable UDP endpoint so the broker can route
    // responses back to this client; the default aeron:ipc only works for a
    // loopback/same-host gateway.
    std::string egress = flagValue(args, "--cli-egress");
    if (egress.empty()) {
        if (const char* env = std::getenv("RUNE_EGRESS"); env) egress = env;
    }
    if (!egress.empty()) cfg.egressChannel = egress;

    // Stream IDs: hidden test overrides.
    const std::string sIn  = flagValue(args, "--cli-stream-in");
    const std::string sOut = flagValue(args, "--cli-stream-out");
    if (!sIn.empty())  cfg.gatewayStreamId = std::stoi(sIn);
    if (!sOut.empty()) cfg.egressStreamId  = std::stoi(sOut);

    // Aeron data directory: hidden test override.
    const std::string dataDir = flagValue(args, "--cli-data-dir");
    if (!dataDir.empty()) cfg.dataDir = dataDir;

    return cfg;
}

#endif // RUNE_CLI_LIVE_ENABLED

} // namespace rune::cli::detail
```

- [ ] **Step 4: Slim `src/cli/Cli.cpp`.**

(a) Replace lines 32-105 (from `// Live broker path requires Aeron.` through the closing `}` of `parseExpectedVersion`) with:

```cpp
// Live broker path requires Aeron.
#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <rune/protocol/client/Protocol.h>
#endif

#include "CliCommon.h"

#include <iostream>

namespace {

// Shared helpers (flags, expected-version, readSource, live config/connect,
// alter-plan text) live in src/cli/CliCommon.{h,cpp}.
using namespace rune::cli::detail;
```

(b) Delete lines 127-252: from the comment `// ── Live ClientConfig builder (shared by apply/get/list)` through the closing `}` of `printAlterPlanText`. Keep `printUsage` (lines 107-125) where it is.

(c) Two call sites (`cmdSchemaApply`, `cmdSchemaAlter`) change from `if (!readSource(path, src, err)) return 1;` to:

```cpp
    if (!readSource(path, std::cin, src, err)) return 1;
```

- [ ] **Step 5: CMake.** In `rune-client-cpp/CMakeLists.txt` replace

```cmake
add_library(rune_cli_lib STATIC
    src/cli/Cli.cpp
)
```
with
```cmake
add_library(rune_cli_lib STATIC
    src/cli/Cli.cpp
    src/cli/CliCommon.cpp
)
```

- [ ] **Step 6: Run regression gate + CLI suites**

```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R Regression --output-on-failure
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|CliLive' --output-on-failure
```
Expected: identical PASS set to Step 1. No test changed.

- [ ] **Step 7: Commit (rune-client-cpp)**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli/CliCommon.h src/cli/CliCommon.cpp src/cli/Cli.cpp CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -F - <<'EOF'
refactor(cli): move shared CLI helpers to src/cli/CliCommon

flagValue/hasFlag/parseExpectedVersion/readSource/printAlterPlanText and the
live config/connect helpers move out of Cli.cpp so `rune console` can share
them. readSource takes an injected std::istream for "-". No behavior change.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 3: `rune console`: REPL state machine, script runner, dry-run executor, offline tests

**Files:**
- Create: `rune-client-cpp/src/cli/Console.h`, `rune-client-cpp/src/cli/Console.cpp`
- Modify: `rune-client-cpp/include/rune/cli/Cli.h` (add overload)
- Modify: `rune-client-cpp/src/cli/Cli.cpp` (`printUsage`, `run` at the end of the file)
- Modify: `rune-client-cpp/CMakeLists.txt` (rune_cli_lib sources; rune_cli_offline_tests sources)
- Create: `rune-client-cpp/tests/RuneConsoleOfflineTest.cpp`

**Interfaces:**
- Consumes: `rune::ql::parseScript`, `rune::ql::classifyInput`, `rune::ql::InputState`, `rune::ql::Statement` (Task 1); `rune::ql::Persistence` / `TopicDef::persistence` (Part E); `flagValue`, `hasFlag`, `parseExpectedVersion`, `readSource`, `printAlterPlanText` (Task 2); `rune::ql::toDescriptor`, `rune::codec::toText` (existing).
- Produces:
  - Public `int rune::cli::run(std::vector<std::string> args, std::istream& in, std::ostream& out, std::ostream& err);`. The existing `run(args, out, err)` forwards `std::cin`.
  - Internal `int rune::cli::detail::cmdConsole(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err);` (args[0] == "console").
  - In `Console.cpp` (anonymous namespace), used by Task 4: `struct ConsoleOptions { std::string scriptPath; bool dryRun; bool validate; uint16_t expectedVersion; }`, `using ExecCode = int;`, `template <class Exec> int runScript(Exec&, std::string_view path, std::string_view src, std::ostream&, std::ostream&)`, `template <class Exec> int runInteractive(Exec&, std::istream&, std::ostream&, std::ostream&)`, `bool isTransient(const rune::ql::TopicDef&)`, `std::string_view topicNoun(const rune::ql::TopicDef&)`, and `int runLive(const ConsoleOptions&, const std::vector<std::string>& args, const std::string& script, std::istream&, std::ostream&, std::ostream&)` (Task 4 replaces its body).
  - Executor concept: `ExecCode execute(const rune::ql::Statement&, std::ostream& out, std::ostream& err)` returns 0 for ok, otherwise the exit code a script stops with.

- [ ] **Step 1: Write the failing tests.** Create `rune-client-cpp/tests/RuneConsoleOfflineTest.cpp`:

```cpp
// tests/RuneConsoleOfflineTest.cpp
//
// Phase 2b Part D — `rune console` offline tests (--dry-run: no broker).
// Drives rune::cli::run(args, in, out, err) with an injected std::istream, so
// the interactive console is tested without a TTY. Also carries the migrated
// `rune schema apply|alter --dry-run` assertions (see the plan's Task 5 table).
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "rune/cli/Cli.h"

namespace fs = std::filesystem;

namespace {

struct ConsoleRun {
    int         rc = -1;
    std::string out;
    std::string err;
};

// `rune console <args…>` with `stdinText` as the console's input.
ConsoleRun console(std::vector<std::string> args, const std::string& stdinText = {}) {
    args.insert(args.begin(), "console");
    std::istringstream in(stdinText);
    std::ostringstream out, err;
    const int rc = rune::cli::run(std::move(args), in, out, err);
    return ConsoleRun{rc, out.str(), err.str()};
}

fs::path writeScript(const std::string& name, const std::string& body) {
    const fs::path p = fs::temp_directory_path() / name;
    std::ofstream o(p, std::ios::binary);
    o << body;
    return p;
}

std::size_t count(const std::string& hay, const std::string& needle) {
    std::size_t n = 0;
    for (auto pos = hay.find(needle); pos != std::string::npos;
         pos = hay.find(needle, pos + needle.size())) {
        ++n;
    }
    return n;
}

} // namespace

// ── Spec §D gates ────────────────────────────────────────────────────────────

TEST(ConsoleOffline, ScriptRunsEveryStatementInOrder) {
    const fs::path f = writeScript("console_order.rql",
        "-- two topics and an alter\n"
        "topic player { id int64, name string } partitions 8 key id;\n"
        "alter topic player add column level int32;\n"
        "topic guild { id int64 } partitions 1 key id;\n");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 0) << r.err;
    const auto p = r.out.find("schema player");
    const auto a = r.out.find("alter topic player");
    const auto g = r.out.find("schema guild");
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
    EXPECT_EQ(count(r.out, "rune> "), 2u) << "first line + prompt after execution; got " << r.out;
    EXPECT_EQ(count(r.out, "schema characters"), 1u) << r.out;
}

TEST(ConsoleOffline, SeveralStatementsOnOneLine) {
    const auto r = console({"--dry-run"},
        "topic a { x int32 } partitions 1 key x; topic b { x int32 } partitions 1 key x;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    const auto a = r.out.find("schema a ");
    const auto b = r.out.find("schema b ");
    ASSERT_NE(a, std::string::npos) << r.out;
    ASSERT_NE(b, std::string::npos) << r.out;
    EXPECT_LT(a, b);
}

TEST(ConsoleOffline, InteractiveParseErrorKeepsSession) {
    const auto r = console({"--dry-run"},
        "topic t { a int128 } partitions 1 key a;\n"
        "topic ok { a int32 } partitions 1 key a;\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_EQ(r.err, "1:13: unknown type 'int128'\n");
    EXPECT_NE(r.out.find("schema ok"), std::string::npos) << "the session must continue; got " << r.out;
}

TEST(ConsoleOffline, ScriptStopsAtFirstParseErrorWithNonZeroExit) {
    const fs::path f = writeScript("console_parse_error.rql",
        "topic a { x int32 } partitions 1 key x;\n"
        "\n"
        "topic b { x int128 } partitions 1 key x;\n"
        "topic c { x int32 } partitions 1 key x;\n");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 2);
    EXPECT_EQ(r.err, f.string() + ":3:13: unknown type 'int128'\n");
    EXPECT_EQ(r.out.find("schema a "), std::string::npos)
        << "a script with a parse error executes nothing; got " << r.out;
}

TEST(ConsoleOffline, ExitQuitAndEofLeave) {
    for (const std::string leave : {"exit\n", "quit\n", "exit;\n", "  quit;  \n"}) {
        const auto r = console({"--dry-run"}, leave + "topic after { a int32 } partitions 1 key a;\n");
        EXPECT_EQ(r.rc, 0) << leave;
        EXPECT_EQ(r.out.find("schema after"), std::string::npos)
            << "'" << leave << "' must end the session; got " << r.out;
    }
    const auto eof = console({"--dry-run"}, "topic last { a int32 } partitions 1 key a;");
    EXPECT_EQ(eof.rc, 0);
    EXPECT_NE(eof.out.find("schema last"), std::string::npos) << eof.out;
}

TEST(ConsoleOffline, DryRunMarksTransientTopic) {
    const auto r = console({"--dry-run"},
        "transient topic ticks { id int64 } partitions 2 key id;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("-- transient\nschema ticks"), std::string::npos) << r.out;
}

// ── Review Focus pins ────────────────────────────────────────────────────────

TEST(ConsoleOfflineRegression, SemicolonInsideCommentDoesNotEndStatement) {
    const auto r = console({"--dry-run"},
        "topic t { a int32 } -- partitions come next;\n"
        "partitions 2 key a;\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_TRUE(r.err.empty()) << r.err;
    EXPECT_EQ(count(r.out, "   ...> "), 1u) << r.out;
    EXPECT_EQ(count(r.out, "schema t "), 1u) << r.out;
}

TEST(ConsoleOfflineRegression, UnterminatedStatementIsReportedNotExecuted) {
    const fs::path f = writeScript("console_unterminated.rql",
        "topic a { x int32 } partitions 1 key x;\n"
        "topic b { x int32 } partitions 1 key x\n");
    const auto s = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(s.rc, 2);
    EXPECT_EQ(s.err, f.string() + ":3:1: expected ;, got ''\n");
    EXPECT_EQ(s.out.find("schema a "), std::string::npos) << s.out;

    const auto i = console({"--dry-run"}, "topic b { x int32 } partitions 1 key x\n");
    EXPECT_EQ(i.rc, 0);
    EXPECT_NE(i.err.find("missing ';'"), std::string::npos) << i.err;
    EXPECT_EQ(i.out.find("schema b "), std::string::npos) << i.out;
}

TEST(ConsoleOfflineRegression, CrlfInputParsesAndExits) {
    const auto r = console({"--dry-run"},
        "topic t { a int32 }\r\n"
        "partitions 1 key a;\r\n"
        "exit\r\n"
        "topic after { a int32 } partitions 1 key a;\r\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_TRUE(r.err.empty()) << r.err;
    EXPECT_NE(r.out.find("schema t "), std::string::npos) << r.out;
    EXPECT_EQ(r.out.find("schema after"), std::string::npos) << r.out;

    const fs::path f = writeScript("console_crlf.rql",
        "topic a { x int32 } partitions 1 key x;\r\nalter topic a add column y int32;\r\n");
    const auto s = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(s.rc, 0) << s.err;
    EXPECT_NE(s.out.find("alter topic a"), std::string::npos) << s.out;
}

TEST(ConsoleOfflineRegression, MissingValueAndUnknownArgumentAreUsageErrors) {
    const auto missing = console({"-f"});
    EXPECT_EQ(missing.rc, 1);
    EXPECT_NE(missing.err.find("-f requires a value"), std::string::npos) << missing.err;

    const auto unknown = console({"--file", "x.rql", "--dry-run"});
    EXPECT_EQ(unknown.rc, 1);
    EXPECT_NE(unknown.err.find("unexpected argument '--file'"), std::string::npos) << unknown.err;

    const auto stray = console({"script.rql"});
    EXPECT_EQ(stray.rc, 1);
    EXPECT_NE(stray.err.find("unexpected argument 'script.rql'"), std::string::npos) << stray.err;
    EXPECT_TRUE(stray.out.empty()) << "no prompt: the session must not start; got " << stray.out;
}

// ── Migrated from `rune schema apply|alter --dry-run` (RuneCliOfflineTest) ────

TEST(ConsoleOfflineRegression, DryRunScriptPrintsDescriptor) {
    const fs::path f = writeScript("console_c_regression.rql",
        "topic characters { id int64, userId int64, name string } partitions 16 key userId;");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("schema characters"), std::string::npos) << r.out;
}

TEST(ConsoleOfflineRegression, ScriptParseErrorHasFileLineColPrefix) {
    const fs::path f = writeScript("console_bad_regression.rql",
        "topic t { a int128 } partitions 1 key a;");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_NE(r.rc, 0);
    EXPECT_EQ(r.err, f.string() + ":1:13: unknown type 'int128'\n");
}

TEST(ConsoleOfflineRegression, DryRunDispatchesEachStatementKind) {
    const fs::path topicFile = writeScript("console_regression_topic.rql",
        "topic characters { id int64, userId int64 } partitions 4 key userId;");
    const fs::path alterFile = writeScript("console_regression_alter.rql",
        "alter topic characters drop column nickname;");

    const auto t = console({"-f", topicFile.string(), "--dry-run"});
    EXPECT_EQ(t.rc, 0) << t.err;
    EXPECT_NE(t.out.find("schema characters"), std::string::npos) << t.out;
    EXPECT_EQ(t.out.find("alter topic"), std::string::npos) << t.out;

    const auto a = console({"-f", alterFile.string(), "--dry-run"});
    EXPECT_EQ(a.rc, 0) << a.err;
    EXPECT_NE(a.out.find("alter topic characters"), std::string::npos) << a.out;
    EXPECT_NE(a.out.find("DROP"), std::string::npos) << a.out;
    EXPECT_NE(a.out.find("nickname"), std::string::npos) << a.out;
    EXPECT_EQ(a.out.find("schema characters"), std::string::npos) << a.out;
}

TEST(ConsoleOfflineRegression, DryRunPrintsAllAlterOpKinds) {
    const fs::path f = writeScript("console_regression_all_ops.rql",
        "alter topic characters "
        "add column nickname string, "
        "drop column legacy, "
        "rename column userId to playerId, "
        "set compatibility forward;");
    const auto r = console({"-f", f.string(), "--dry-run"});
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ADD"), std::string::npos)               << r.out;
    EXPECT_NE(r.out.find("DROP"), std::string::npos)              << r.out;
    EXPECT_NE(r.out.find("RENAME"), std::string::npos)            << r.out;
    EXPECT_NE(r.out.find("userId -> playerId"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("SET_COMPATIBILITY"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("FORWARD"), std::string::npos)           << r.out;
}

TEST(ConsoleOfflineRegression, ExpectedVersionMalformedIsCleanErrorNotCrash) {
    const fs::path f = writeScript("console_regression_expected_version.rql",
        "alter topic characters add column nickname string;");
    for (const std::string bad : {"foo", "99999999", "-1"}) {
        const auto r = console({"-f", f.string(), "--dry-run", "--expected-version", bad});
        EXPECT_NE(r.rc, 0) << "bad value '" << bad << "' must be a non-zero exit";
        EXPECT_NE(r.err.find("--expected-version"), std::string::npos)
            << "bad value '" << bad << "' must print a clear error; got: " << r.err;
    }
}
```

CMake (`rune-client-cpp/CMakeLists.txt`), offline test target. Replace
```cmake
    add_executable(rune_cli_offline_tests
        tests/RuneCliOfflineTest.cpp
    )
```
with
```cmake
    add_executable(rune_cli_offline_tests
        tests/RuneCliOfflineTest.cpp
        tests/RuneConsoleOfflineTest.cpp
    )
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j --target rune_cli_offline_tests`
Expected: compile error `no matching function for call to 'run'` (no 4-argument overload yet).

- [ ] **Step 3: Public overload.** In `include/rune/cli/Cli.h` replace `#include <ostream>` with
```cpp
#include <istream>
#include <ostream>
```
and append, after the existing `run` declaration, inside `namespace rune::cli`:

```cpp
/// As above, reading interactive / `-f -` input from @p in instead of
/// std::cin (lets `rune console` be tested without a TTY).
/// e.g. {"console","-f","schema.rql"} or {"console"} with statements on @p in.
int run(std::vector<std::string> args, std::istream& in, std::ostream& out, std::ostream& err);
```

- [ ] **Step 4: Create `src/cli/Console.h`**

```cpp
// src/cli/Console.h — `rune console` (internal; reached via rune::cli::run).
#pragma once

#include <iosfwd>
#include <string>
#include <vector>

namespace rune::cli::detail {

/// `rune console …` — args[0] == "console". Reads interactive input (or
/// `-f -`) from @p in; statement results to @p out, errors to @p err.
int cmdConsole(const std::vector<std::string>& args,
               std::istream& in, std::ostream& out, std::ostream& err);

} // namespace rune::cli::detail
```

- [ ] **Step 5: Create `src/cli/Console.cpp`**

```cpp
// src/cli/Console.cpp
//
// Phase 2b Part D — `rune console`: Rune QL statements against one broker session.
//
//   rune console [--broker <aeron channel>] [--cli-egress <channel>]
//                [-f <script.rql|->] [--dry-run] [--validate] [--expected-version N]
//
// Interactive (no -f): prompt "rune> ", continuation "   ...> " until the
// buffered text ends with a ';' outside comments (rune::ql::classifyInput).
// Each completed input is parsed with rune::ql::parseScript and its
// statements run in order; a failing statement skips the rest of THAT input.
// An error never ends the session. exit / quit / EOF leave (exit code 0).
//
// Script (-f): the whole file is parsed first (a parse error executes
// nothing, exit 2); statements then run in order and the first failure stops
// the script: exit = the broker's wire errorCode for a broker rejection, 1 for
// a timeout / transport failure.
//
// --dry-run: no broker; each statement prints its descriptor / alter plan.
// --validate: every topic defined in the session validates records broker-side.
// --expected-version N: every `alter topic` in the session asserts the topic
//   is at version N (optimistic concurrency; 0 = no check).
//
// Control plane only — never on a produce/consume/query hot path. One Client
// (one connection) serves the whole session.

#include "Console.h"
#include "CliCommon.h"

#include "rune/ql/Ast.h"
#include "rune/ql/Backends.h"
#include "rune/ql/Parser.h"

#include <rune/codec/Serialize.h>

#include <array>
#include <cstdint>
#include <exception>
#include <istream>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::cli::detail {
namespace {

constexpr std::string_view kPrompt             = "rune> ";
constexpr std::string_view kContinuationPrompt = "   ...> ";

// ── Options ──────────────────────────────────────────────────────────────────

struct ConsoleOptions {
    std::string scriptPath;            // -f; empty = interactive
    bool        dryRun          = false;
    bool        validate        = false;
    uint16_t    expectedVersion = 0;   // 0 = no CAS
};

constexpr std::array<std::string_view, 7> kValueFlags = {
    "-f", "--broker", "--cli-egress", "--expected-version",
    "--cli-stream-in", "--cli-stream-out", "--cli-data-dir"};   // last three: hidden test flags
constexpr std::array<std::string_view, 2> kBoolFlags = {"--dry-run", "--validate"};

template <std::size_t N>
bool isOneOf(const std::array<std::string_view, N>& set, std::string_view s) {
    for (const auto v : set) {
        if (v == s) return true;
    }
    return false;
}

void printConsoleUsage(std::ostream& err) {
    err << "Usage: rune console [--broker <aeron channel>] [--cli-egress <channel>]\n"
           "                    [-f <script.rql|->] [--dry-run] [--validate]\n"
           "                    [--expected-version N]\n";
}

// Strict: every arg after "console" is a known flag; value flags have a value.
// A typo must never fall through to an interactive session blocked on stdin.
bool parseConsoleOptions(const std::vector<std::string>& args, ConsoleOptions& o,
                         std::ostream& err) {
    for (std::size_t i = 1; i < args.size(); ++i) {
        const std::string& a = args[i];
        if (isOneOf(kBoolFlags, a)) continue;
        if (isOneOf(kValueFlags, a)) {
            if (i + 1 >= args.size()) {
                err << "rune console: " << a << " requires a value\n";
                printConsoleUsage(err);
                return false;
            }
            ++i;
            continue;
        }
        err << "rune console: unexpected argument '" << a << "'\n";
        printConsoleUsage(err);
        return false;
    }
    o.scriptPath = flagValue(args, "-f");
    o.dryRun     = hasFlag(args, "--dry-run");
    o.validate   = hasFlag(args, "--validate");
    return parseExpectedVersion(flagValue(args, "--expected-version"), o.expectedVersion, err);
}

// ── Statement helpers ────────────────────────────────────────────────────────

/// 0 = ok; otherwise the exit code a script stops with.
using ExecCode = int;

bool isTransient(const rune::ql::TopicDef& td) {
    return td.persistence == rune::ql::Persistence::Transient;
}

std::string_view topicNoun(const rune::ql::TopicDef& td) {
    return isTransient(td) ? "transient topic " : "topic ";
}

void printParseError(std::string_view origin, const rune::ql::ParseError& pe, std::ostream& err) {
    if (!origin.empty()) err << origin << ":";
    err << pe.line << ":" << pe.col << ": " << pe.message << "\n";
}

// Runs one parsed batch in order; stops at the first failure. Any exception
// from an executor is reported as that statement's failure (exit 1), so an
// exception never ends an interactive session.
template <class Exec>
ExecCode executeAll(Exec& exec, const std::vector<rune::ql::Statement>& stmts,
                    std::ostream& out, std::ostream& err) {
    for (const auto& st : stmts) {
        ExecCode rc = 0;
        try {
            rc = exec.execute(st, out, err);
        } catch (const std::exception& e) {
            err << "error: " << e.what() << "\n";
            rc = 1;
        }
        if (rc != 0) return rc;
    }
    return 0;
}

// ── Script mode (-f) ─────────────────────────────────────────────────────────

template <class Exec>
int runScript(Exec& exec, std::string_view path, std::string_view src,
              std::ostream& out, std::ostream& err) {
    auto parsed = rune::ql::parseScript(src);
    if (!parsed) {
        printParseError(path, parsed.error(), err);
        return 2;
    }
    return executeAll(exec, *parsed, out, err);
}

// ── Interactive mode: REPL state machine (CLAUDE.md #3) ──────────────────────
//
//   AwaitingStatement ──line (Empty)──────> AwaitingStatement
//   AwaitingStatement ──exit/quit──────────> (leave, 0)
//   AwaitingStatement ──line (Incomplete)──> ContinuingStatement{text}
//   ContinuingStatement ──line (Incomplete)> ContinuingStatement{text+line}
//   any ──line (Complete)──> parse + execute ──> AwaitingStatement
//   any ──EOF──> (leave, 0; a dangling ContinuingStatement is reported, not run)

struct AwaitingStatement {};                       // prompt "rune> "
struct ContinuingStatement { std::string text; };  // prompt "   ...> "
using ReplState = std::variant<AwaitingStatement, ContinuingStatement>;

std::string_view trimmed(std::string_view s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string_view::npos) return {};
    const auto e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

bool isExitCommand(std::string_view line) {
    const auto t = trimmed(line);
    return t == "exit" || t == "quit" || t == "exit;" || t == "quit;";
}

template <class Exec>
int runInteractive(Exec& exec, std::istream& in, std::ostream& out, std::ostream& err) {
    ReplState state = AwaitingStatement{};
    std::string line;
    for (;;) {
        out << (std::holds_alternative<AwaitingStatement>(state) ? kPrompt : kContinuationPrompt)
            << std::flush;
        if (!std::getline(in, line)) {
            out << "\n";
            if (std::holds_alternative<ContinuingStatement>(state)) {
                err << "error: input ended inside a statement (missing ';'); it was not executed\n";
            }
            return 0;
        }

        std::string text;
        if (auto* c = std::get_if<ContinuingStatement>(&state)) {
            text = std::move(c->text);
        } else if (isExitCommand(line)) {
            return 0;
        }
        text += line;
        text += '\n';

        switch (rune::ql::classifyInput(text)) {
            case rune::ql::InputState::Empty:
                state = AwaitingStatement{};
                break;
            case rune::ql::InputState::Incomplete:
                state = ContinuingStatement{std::move(text)};
                break;
            case rune::ql::InputState::Complete: {
                state = AwaitingStatement{};
                auto parsed = rune::ql::parseScript(text);
                if (!parsed) {
                    printParseError({}, parsed.error(), err);
                } else {
                    (void)executeAll(exec, *parsed, out, err);
                }
                break;
            }
        }
    }
}

// ── Executors ────────────────────────────────────────────────────────────────

struct DryRunExecutor {
    ExecCode execute(const rune::ql::Statement& st, std::ostream& out, std::ostream&) {
        if (const auto* td = std::get_if<rune::ql::TopicDef>(&st)) {
            if (isTransient(*td)) out << "-- transient\n";
            out << rune::codec::toText(rune::ql::toDescriptor(*td)) << "\n";
            return 0;
        }
        printAlterPlanText(std::get<rune::ql::AlterTopicDef>(st), out);
        return 0;
    }
};

// Live execution (Task 4 replaces this body with the connected-client path).
int runLive(const ConsoleOptions&, const std::vector<std::string>&, const std::string&,
            std::istream&, std::ostream&, std::ostream& err) {
    err << "rune console: live execution is not available in this build "
           "(requires RUNE_CLI_LIVE_ENABLED and a real Aeron build). "
           "Use --dry-run to check statements offline.\n";
    return 1;
}

} // namespace

int cmdConsole(const std::vector<std::string>& args,
               std::istream& in, std::ostream& out, std::ostream& err) {
    ConsoleOptions o;
    if (!parseConsoleOptions(args, o, err)) return 1;

    const bool scripted = !o.scriptPath.empty();
    std::string script;
    if (scripted && !readSource(o.scriptPath, in, script, err)) return 1;

    if (o.dryRun) {
        DryRunExecutor exec;
        return scripted ? runScript(exec, o.scriptPath, script, out, err)
                        : runInteractive(exec, in, out, err);
    }
    return runLive(o, args, script, in, out, err);
}

} // namespace rune::cli::detail
```

- [ ] **Step 6: Wire `run` + usage in `src/cli/Cli.cpp`.**

(a) Add `#include "Console.h"` right after `#include "CliCommon.h"`.

(b) In `printUsage`, replace the first line `err << "Usage:\n"` with:

```cpp
    err << "Usage:\n"
        << "  rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->]\n"
        << "               [--dry-run] [--validate] [--expected-version N]\n"
        << "      Run Rune QL statements (topic …; transient topic …; alter topic …;)\n"
        << "      interactively (prompt `rune> `) or from a script (-f; stops at the\n"
        << "      first error). --dry-run prints each statement's plan (no broker).\n"
```
(Keep the existing `schema apply/alter` lines for now; Task 5 removes them.)

(c) Replace the whole `namespace rune::cli { … }` block at the end of the file with:

```cpp
namespace rune::cli {

int run(std::vector<std::string> args, std::ostream& out, std::ostream& err)
{
    return run(std::move(args), std::cin, out, err);
}

int run(std::vector<std::string> args, std::istream& in, std::ostream& out, std::ostream& err)
{
    if (args.empty()) {
        printUsage(err);
        return 1;
    }

    const std::string& cmd = args[0];

    if (cmd == "console") {
        return detail::cmdConsole(args, in, out, err);
    }
    if (cmd == "schema") {
        return cmdSchema(args, out, err);
    }

    err << "rune: unknown command '" << cmd << "'\n";
    printUsage(err);
    return 1;
}

} // namespace rune::cli
```

(d) CMake: replace
```cmake
add_library(rune_cli_lib STATIC
    src/cli/Cli.cpp
    src/cli/CliCommon.cpp
)
```
with
```cmake
add_library(rune_cli_lib STATIC
    src/cli/Cli.cpp
    src/cli/CliCommon.cpp
    src/cli/Console.cpp
)
```

- [ ] **Step 7: Run the tests**

```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R Regression --output-on-failure
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|CliLive|Console' --output-on-failure
```
Expected: every `ConsoleOffline*` test PASSES, along with all pre-existing tests.

- [ ] **Step 8: Commit (rune-client-cpp)**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli/Console.h src/cli/Console.cpp src/cli/Cli.cpp include/rune/cli/Cli.h tests/RuneConsoleOfflineTest.cpp CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -F - <<'EOF'
feat(cli): rune console — REPL state machine, -f scripts, --dry-run

rune::cli::run(args, in, out, err) injects the console's input stream so the
console is tested without a TTY. Interactive: rune> / ...> prompts driven by
rune::ql::classifyInput; errors never end the session; exit/quit/EOF leave.
Script: whole-file parseScript, stop at first error, non-zero exit.
Migrated `schema apply|alter --dry-run` assertions into ConsoleOffline*.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 4: Live console: one connected client per session, broker errors by name, transient topics

**Files:**
- Modify: `rune-client-cpp/src/cli/Console.cpp` (replace the `runLive` stub; add the live executor)
- Modify: `rune-client-cpp/tests/RuneCliLiveTest.cpp` (gateway fields; `runCli` stdin; new `runConsole`; new tests appended)

**Interfaces:**
- Consumes: `ConsoleOptions`, `ExecCode`, `runScript`, `runInteractive`, `isTransient`, `topicNoun` (Task 3); `kCliWait`, `connectOrReport`, `buildClientConfig` (Task 2); `rune::ql::applyPlan`, `rune::ql::applyAlterPlan(const AlterTopicDef&, uint16_t)`; `Client::alterTopicAndWait(std::string_view, uint16_t, std::span<const uint8_t>, std::chrono::nanoseconds)`; **Part E**: `rune::client::Persistence`, `Client::defineTopicAndWait(…, Persistence, timeout)`, `rpc::DefineTopic::persistence`.
- Produces: console output contract (tests and e2e depend on it):
  - define ok → stdout `ok topic <name> v<version> (<N> partitions)`. With N == 1 it reads `(1 partition)`. A transient topic reads `ok transient topic …`.
  - `TopicExists` → stdout `ok topic <name> already exists (topicId=<id>)`, exit 0.
  - alter ok → stdout `ok topic <name> v<newVersion>`.
  - broker error → stderr `error: topic <name>: <ErrorCodeName><hint>`, exit = errorCode. Hints are added for `SchemaIncompatible` (contains "incompatible") and `SchemaConflict` (contains "stale").
  - exception or timeout → stderr `error: <what()>`, exit 1.

- [ ] **Step 1: Extend the test gateway.** In `tests/RuneCliLiveTest.cpp`, struct `CliTestGateway`:

(a) After the `// Canned AlterTopic response (Task 20).` block (ends `uint16_t alterErrorCode   = 0;`), add:

```cpp
    // Console tests (Phase 2b D): per-call DefineTopic error codes, consumed in
    // order; once exhausted, defineErrorCode applies.
    std::vector<uint16_t> defineErrorSeq;
    std::size_t           defineErrorIdx = 0;
```

(b) After `std::atomic<int>     altersSeen{0};` add:

```cpp
    std::atomic<int>     connectsSeen{0};
    bool                 lastDefineValidate    = false;
    uint8_t              lastDefinePersistence = 0;   // Part E appended byte (0 disk, 1 transient)
```

(c) In `onFrame`, `case rcc::TemplateId::Connect:`, right after `if (!opt) return;` add:

```cpp
            connectsSeen.fetch_add(1, std::memory_order_relaxed);
```

(d) In `case rcc::TemplateId::DefineTopic:`, after `lastDefineKeyField   = std::string(opt->keyField);` add:

```cpp
            lastDefineValidate    = opt->validate;
            lastDefinePersistence = static_cast<uint8_t>(opt->persistence);
```
and replace `resp.errorCode     = defineErrorCode;` with:

```cpp
            resp.errorCode     = defineErrorIdx < defineErrorSeq.size()
                               ? defineErrorSeq[defineErrorIdx++] : defineErrorCode;
```

(e) Replace the `runCli` helper with this version. The new parameter `stdinText` is the console's stdin; non-console commands ignore it:

```cpp
CliRunResult runCli(CliTestGateway& gw,
                    const std::string& aeronDir,
                    const std::vector<std::string>& args,
                    std::chrono::milliseconds gatewayStartDelay = 300ms,
                    const std::string& stdinText = {})
{
    std::istringstream inBuf(stdinText);
    std::ostringstream outBuf, errBuf;
    int rcVal = -1;

    // Launch cli::run in a background thread.
    auto fut = std::async(std::launch::async, [&]() {
        rcVal = rune::cli::run(args, inBuf, outBuf, errBuf);
    });

    // Wait briefly for the embedded driver to initialise, then start the gateway.
    std::this_thread::sleep_for(gatewayStartDelay);
    gw.start();

    // Pump the gateway until the CLI thread finishes (or times out).
    const auto deadline = std::chrono::steady_clock::now() + kCliTimeout;
    while (fut.wait_for(50ms) != std::future_status::ready) {
        gw.poll(16);
        if (std::chrono::steady_clock::now() > deadline) break;
    }
    // Final drain.
    for (int i = 0; i < 20; ++i) { gw.poll(16); std::this_thread::sleep_for(5ms); }

    fut.wait();  // ensure the thread is done

    return CliRunResult{rcVal, outBuf.str(), errBuf.str()};
}

// `rune console <extra…>` with the hidden loopback flags, `stdinText` as input.
CliRunResult runConsole(CliTestGateway& gw,
                        const std::string& aeronDir,
                        const std::string& agentDir,
                        int32_t inStream, int32_t outStream,
                        std::vector<std::string> extra,
                        const std::string& stdinText = {})
{
    std::vector<std::string> args = {"console"};
    args.insert(args.end(), extra.begin(), extra.end());
    args.insert(args.end(), {"--cli-data-dir", agentDir,
                             "--cli-stream-in",  std::to_string(inStream),
                             "--cli-stream-out", std::to_string(outStream)});
    return runCli(gw, aeronDir, args, 300ms, stdinText);
}
```

- [ ] **Step 2: Write the failing live tests.** Append to `tests/RuneCliLiveTest.cpp` (stream IDs 1250-1279 are unused):

```cpp
// ===========================================================================
// Phase 2b Part D — `rune console` live tests (one session against the
// CliTestGateway). Tests marked "migrated" carry the assertions of the removed
// `rune schema apply|alter` live tests.
// ===========================================================================

// Migrated from CliLive.SchemaApplyLive.
TEST(ConsoleLive, ScriptDefinesTopic) {
    static constexpr int32_t kIn = 1250, kOut = 1251;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string ddl =
        "topic characters { id int64, userId int64, name string } partitions 16 key userId;";
    const std::string rqlPath = writeTmpRql(baseDir, ddl);

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.defineTopicId = 0x40000001;
    gw.defineVersion = 1;

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(gw.definesSeen.load(), 1);
    EXPECT_EQ(gw.lastDefineName,       "characters");
    EXPECT_EQ(gw.lastDefinePartitions, 16u);
    EXPECT_EQ(gw.lastDefineKeyField,   "userId");
    EXPECT_FALSE(gw.lastDefineValidate) << "--validate was not given";
    EXPECT_EQ(gw.lastDefinePersistence, 0u) << "a plain `topic` is a disk topic";

    ASSERT_FALSE(gw.lastDefineBlob.empty());
    const rune::codec::Descriptor d = rune::codec::deserialize(
        std::span<const uint8_t>(gw.lastDefineBlob.data(), gw.lastDefineBlob.size()));
    auto parsed = rune::ql::parse(ddl);
    ASSERT_TRUE(parsed.has_value());
    const rune::ql::DefineTopicPlan plan = rune::ql::applyPlan(std::get<rune::ql::TopicDef>(*parsed));
    const rune::codec::Descriptor expected = rune::codec::deserialize(
        std::span<const uint8_t>(plan.descriptorBlob.data(), plan.descriptorBlob.size()));
    EXPECT_EQ(rune::codec::fingerprint(d), rune::codec::fingerprint(expected));

    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic characters v1 (16 partitions)"), std::string::npos) << r.out;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Migrated from CliLive.SchemaApplyTopicExists.
TEST(ConsoleLive, ScriptTopicExistsIsOk) {
    static constexpr int32_t kIn = 1252, kOut = 1253;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "topic characters { id int64 } partitions 1 key id;");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.defineTopicId   = 0x40000009;
    gw.defineVersion   = 0;
    gw.defineErrorCode = static_cast<uint16_t>(rpc::ErrorCode::TopicExists);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(r.rc, 0) << "TopicExists is not an error; err=" << r.err;
    EXPECT_NE(r.out.find("ok topic characters already exists"), std::string::npos) << r.out;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Migrated from CliLive.AlterTopicLive.
TEST(ConsoleLive, ScriptAltersTopicWithExpectedVersion) {
    static constexpr int32_t kIn = 1254, kOut = 1255;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "alter topic characters add column nickname string, set compatibility backward;");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.alterTopicId    = 0x40000001;
    gw.alterNewVersion = 2;

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut,
                        {"-f", rqlPath, "--expected-version", "1"});

    EXPECT_EQ(gw.altersSeen.load(), 1);
    EXPECT_EQ(gw.lastAlterName, "characters");
    EXPECT_EQ(gw.lastAlterExpectedVersion, 1u);
    ASSERT_FALSE(gw.lastAlterOpsBlob.empty());
    auto decoded = rune::codec::decodeAlterOps(
        std::span<const uint8_t>(gw.lastAlterOpsBlob.data(), gw.lastAlterOpsBlob.size()));
    ASSERT_TRUE(decoded.has_value());
    ASSERT_EQ(decoded->size(), 2u);
    EXPECT_EQ((*decoded)[0].type, rune::codec::AlterOpType::Add);
    EXPECT_EQ((*decoded)[0].name, "nickname");
    EXPECT_EQ((*decoded)[1].type, rune::codec::AlterOpType::SetCompatibility);
    EXPECT_EQ((*decoded)[1].mode, rune::codec::CompatMode::Backward);

    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic characters v2"), std::string::npos) << r.out;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Migrated from CliLive.AlterTopicSchemaIncompatible.
TEST(ConsoleLive, ScriptAlterSchemaIncompatible) {
    static constexpr int32_t kIn = 1256, kOut = 1257;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir, "alter topic characters drop column id;");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.alterTopicId   = -1;
    gw.alterErrorCode = static_cast<uint16_t>(rpc::ErrorCode::SchemaIncompatible);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(r.rc, static_cast<int>(rpc::ErrorCode::SchemaIncompatible)) << r.err;
    EXPECT_NE(r.err.find("error: topic characters: SchemaIncompatible"), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("incompatible"), std::string::npos) << r.err;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Migrated from CliLive.AlterTopicSchemaConflict.
TEST(ConsoleLive, ScriptAlterSchemaConflict) {
    static constexpr int32_t kIn = 1258, kOut = 1259;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "alter topic characters add column nickname string;");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.alterTopicId   = -1;
    gw.alterErrorCode = static_cast<uint16_t>(rpc::ErrorCode::SchemaConflict);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut,
                        {"-f", rqlPath, "--expected-version", "5"});

    EXPECT_EQ(gw.lastAlterExpectedVersion, 5u);
    EXPECT_EQ(r.rc, static_cast<int>(rpc::ErrorCode::SchemaConflict)) << r.err;
    EXPECT_NE(r.err.find("SchemaConflict"), std::string::npos) << r.err;
    EXPECT_NE(r.err.find("stale"), std::string::npos) << r.err;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Migrated from CliLive.ApplyAlterFileLive (+ one-session gate).
TEST(ConsoleLive, ScriptDefinesThenAltersInOneSession) {
    static constexpr int32_t kIn = 1260, kOut = 1261;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "topic characters { id int64, userId int64 } partitions 4 key userId;\n"
        "alter topic characters add column nickname string;\n");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.defineVersion   = 1;
    gw.alterNewVersion = 3;

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(gw.definesSeen.load(), 1);
    EXPECT_EQ(gw.altersSeen.load(), 1);
    EXPECT_EQ(gw.connectsSeen.load(), 1) << "one client for the whole script";
    EXPECT_EQ(r.rc, 0) << r.err;
    const auto d = r.out.find("ok topic characters v1 (4 partitions)");
    const auto a = r.out.find("ok topic characters v3");
    ASSERT_NE(d, std::string::npos) << r.out;
    ASSERT_NE(a, std::string::npos) << r.out;
    EXPECT_LT(d, a);

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

TEST(ConsoleLive, ScriptStopsAtFirstBrokerError) {
    static constexpr int32_t kIn = 1262, kOut = 1263;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "topic a { id int64 } partitions 1 key id;\n"
        "alter topic a drop column id;\n"
        "topic c { id int64 } partitions 1 key id;\n");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.alterErrorCode = static_cast<uint16_t>(rpc::ErrorCode::SchemaIncompatible);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(gw.definesSeen.load(), 1) << "statement 3 must not run";
    EXPECT_EQ(gw.altersSeen.load(), 1);
    EXPECT_EQ(gw.lastDefineName, "a");
    EXPECT_EQ(r.rc, static_cast<int>(rpc::ErrorCode::SchemaIncompatible)) << r.err;
    EXPECT_NE(r.out.find("ok topic a v1"), std::string::npos) << r.out;
    EXPECT_EQ(r.out.find("topic c"), std::string::npos) << r.out;
    EXPECT_NE(r.err.find("error: topic a: SchemaIncompatible"), std::string::npos) << r.err;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

TEST(ConsoleLive, InteractiveBrokerErrorKeepsSession) {
    static constexpr int32_t kIn = 1264, kOut = 1265;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.defineErrorSeq = {static_cast<uint16_t>(rpc::ErrorCode::InvalidSchema)};

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {},
        "topic a { id int64 } partitions 1 key id;\n"
        "topic b { id int64 } partitions 1 key id;\n");

    EXPECT_EQ(gw.definesSeen.load(), 2) << "the session continues after a broker error";
    EXPECT_EQ(gw.connectsSeen.load(), 1);
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.err.find("error: topic a: InvalidSchema"), std::string::npos) << r.err;
    EXPECT_NE(r.out.find("ok topic b v1 (1 partition)"), std::string::npos) << r.out;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

TEST(ConsoleLive, InteractiveParseErrorKeepsSession) {
    static constexpr int32_t kIn = 1266, kOut = 1267;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";

    CliTestGateway gw(aeronDir, kIn, kOut);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {},
        "topic t { a int128 } partitions 1 key a;\n"
        "topic good { a int32 } partitions 1 key a;\n");

    EXPECT_EQ(gw.definesSeen.load(), 1);
    EXPECT_EQ(gw.lastDefineName, "good");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.err.find("1:13: unknown type 'int128'"), std::string::npos) << r.err;
    EXPECT_NE(r.out.find("ok topic good v1 (1 partition)"), std::string::npos) << r.out;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

TEST(ConsoleLive, InteractiveMultiLineThenExit) {
    static constexpr int32_t kIn = 1268, kOut = 1269;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";

    CliTestGateway gw(aeronDir, kIn, kOut);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {},
        "topic multi {\n"
        "  id int64\n"
        "} partitions 2 key id;\n"
        "exit\n"
        "topic never { id int64 } partitions 1 key id;\n");

    EXPECT_EQ(gw.definesSeen.load(), 1);
    EXPECT_EQ(gw.lastDefineName, "multi");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("   ...> "), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic multi v1 (2 partitions)"), std::string::npos) << r.out;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Part E dependency: `transient topic` reaches the wire as persistence = 1.
TEST(ConsoleLive, TransientTopicSendsPersistence) {
    static constexpr int32_t kIn = 1270, kOut = 1271;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "transient topic ticks { id int64 } partitions 2 key id;");

    CliTestGateway gw(aeronDir, kIn, kOut);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(gw.definesSeen.load(), 1);
    EXPECT_EQ(gw.lastDefinePersistence, 1u);
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok transient topic ticks v1 (2 partitions)"), std::string::npos) << r.out;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

TEST(ConsoleLive, ValidateFlagReachesDefineTopic) {
    static constexpr int32_t kIn = 1272, kOut = 1273;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir, "topic v { id int64 } partitions 1 key id;");

    CliTestGateway gw(aeronDir, kIn, kOut);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath, "--validate"});

    EXPECT_EQ(gw.definesSeen.load(), 1);
    EXPECT_TRUE(gw.lastDefineValidate);
    EXPECT_EQ(r.rc, 0) << r.err;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Migrated from CliLiveRegression.ApplyLive_TopicNameInOutput.
TEST(ConsoleLiveRegression, ScriptTopicNameInOutput) {
    static constexpr int32_t kIn = 1274, kOut = 1275;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "topic characters { id int64 } partitions 1 key id;");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.defineTopicId = 0x40000010;

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("characters"), std::string::npos) << r.out;
    EXPECT_EQ(gw.definesSeen.load(), 1);

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Migrated from CliLiveRegression.AlterLive_NewVersionInOutput.
TEST(ConsoleLiveRegression, AlterNewVersionInOutput) {
    static constexpr int32_t kIn = 1276, kOut = 1277;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath  = writeTmpRql(baseDir,
        "alter topic characters add column nickname string;");

    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.alterTopicId    = 0x40000006;
    gw.alterNewVersion = 7;

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {"-f", rqlPath});

    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic characters v7"), std::string::npos) << r.out;
    EXPECT_EQ(gw.altersSeen.load(), 1);

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}

// Spec §D: "one connected client for the whole session". A per-statement
// client would show connectsSeen == 3 here.
TEST(ConsoleLiveRegression, OneConnectionForWholeSession) {
    static constexpr int32_t kIn = 1278, kOut = 1279;
    const std::string baseDir  = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";

    CliTestGateway gw(aeronDir, kIn, kOut);

    auto r = runConsole(gw, aeronDir, baseDir, kIn, kOut, {},
        "topic s1 { id int64 } partitions 1 key id;\n"
        "topic s2 { id int64 } partitions 1 key id;\n"
        "topic s3 { id int64 } partitions 1 key id;\n");

    EXPECT_EQ(gw.definesSeen.load(), 3);
    EXPECT_EQ(gw.connectsSeen.load(), 1);
    EXPECT_EQ(r.rc, 0) << r.err;

    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}
```

- [ ] **Step 3: Run to verify they fail**

```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j --target rune_cli_live_tests
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'ConsoleLive' --output-on-failure
```
Expected: every `ConsoleLive*` test FAILS with rc 1 and err `rune console: live execution is not available in this build`. The pre-existing `CliLive*` tests still PASS: `runCli` now feeds an empty stdin, which non-console commands ignore.

- [ ] **Step 4: Implement the live path.** In `src/cli/Console.cpp`:

(a) After the `#include <vector>` line, add:

```cpp
#ifdef RUNE_CLI_LIVE_ENABLED
#include <rune/client/Client.h>
#include <rune/protocol/client/Protocol.h>
#include <span>
#endif
```

(b) Replace the whole stub, from `// Live execution (Task 4 replaces this body with the connected-client path).` through the closing `}` of that `runLive`, with:

```cpp
#ifdef RUNE_CLI_LIVE_ENABLED

namespace rpc = rune::protocol::client;

/// Wire ErrorCode name (spec §D: "the broker error name for broker errors").
/// No default: -Wswitch flags a newly appended code that is missing here.
std::string_view brokerErrorName(uint16_t code) {
    switch (static_cast<rpc::ErrorCode>(code)) {
        case rpc::ErrorCode::OK:                     return "OK";
        case rpc::ErrorCode::ProtocolVersion:        return "ProtocolVersion";
        case rpc::ErrorCode::NotLeader:              return "NotLeader";
        case rpc::ErrorCode::Redirect:               return "Redirect";
        case rpc::ErrorCode::SessionExpired:         return "SessionExpired";
        case rpc::ErrorCode::UnknownSession:         return "UnknownSession";
        case rpc::ErrorCode::Internal:               return "Internal";
        case rpc::ErrorCode::UnknownTopic:           return "UnknownTopic";
        case rpc::ErrorCode::UnknownPartition:       return "UnknownPartition";
        case rpc::ErrorCode::NotPartitionLeader:     return "NotPartitionLeader";
        case rpc::ErrorCode::MinIsrViolation:        return "MinIsrViolation";
        case rpc::ErrorCode::StaleGeneration:        return "StaleGeneration";
        case rpc::ErrorCode::UnknownMember:          return "UnknownMember";
        case rpc::ErrorCode::RebalanceInProgress:    return "RebalanceInProgress";
        case rpc::ErrorCode::NotCoordinator:         return "NotCoordinator";
        case rpc::ErrorCode::IllegalGeneration:      return "IllegalGeneration";
        case rpc::ErrorCode::GroupDead:              return "GroupDead";
        case rpc::ErrorCode::InvalidProducerEpoch:   return "InvalidProducerEpoch";
        case rpc::ErrorCode::ProducerFenced:         return "ProducerFenced";
        case rpc::ErrorCode::InvalidTxnState:        return "InvalidTxnState";
        case rpc::ErrorCode::OutOfOrderSequence:     return "OutOfOrderSequence";
        case rpc::ErrorCode::DuplicateSequence:      return "DuplicateSequence";
        case rpc::ErrorCode::MessageTooLarge:        return "MessageTooLarge";
        case rpc::ErrorCode::InvalidSchema:          return "InvalidSchema";
        case rpc::ErrorCode::SchemaValidationFailed: return "SchemaValidationFailed";
        case rpc::ErrorCode::TopicExists:            return "TopicExists";
        case rpc::ErrorCode::StorageError:           return "StorageError";
        case rpc::ErrorCode::ConcurrentTransactions: return "ConcurrentTransactions";
        case rpc::ErrorCode::UnknownProducerId:      return "UnknownProducerId";
        case rpc::ErrorCode::HandshakeFailed:        return "HandshakeFailed";
        case rpc::ErrorCode::SchemaIncompatible:     return "SchemaIncompatible";
        case rpc::ErrorCode::SchemaConflict:         return "SchemaConflict";
        case rpc::ErrorCode::ViewExists:             return "ViewExists";
        case rpc::ErrorCode::ViewSourceNotFound:     return "ViewSourceNotFound";
        case rpc::ErrorCode::ViewFieldUnresolved:    return "ViewFieldUnresolved";
        case rpc::ErrorCode::ViewKeyInvalid:         return "ViewKeyInvalid";
        case rpc::ErrorCode::ViewNotCoPartitioned:   return "ViewNotCoPartitioned";
    }
    return "UnknownErrorCode";
}

std::string_view brokerErrorHint(uint16_t code) {
    switch (static_cast<rpc::ErrorCode>(code)) {
        case rpc::ErrorCode::SchemaIncompatible:
            return " (the alter ops are incompatible with the existing schema/data)";
        case rpc::ErrorCode::SchemaConflict:
            return " (expected version is stale: the schema changed since it was read; "
                   "re-fetch with `rune schema get` and retry)";
        default:
            return "";
    }
}

ExecCode reportBrokerError(std::string_view topic, uint16_t code, std::ostream& err) {
    err << "error: topic " << topic << ": " << brokerErrorName(code) << brokerErrorHint(code) << "\n";
    return static_cast<ExecCode>(code);
}

// PART E DEPENDENCY — the only place that names E's client enum.
rune::client::Persistence toClientPersistence(rune::ql::Persistence p) {
    switch (p) {
        case rune::ql::Persistence::Disk:      return rune::client::Persistence::Disk;
        case rune::ql::Persistence::Transient: return rune::client::Persistence::Transient;
    }
    return rune::client::Persistence::Disk;
}

void printPartitions(std::ostream& out, uint32_t n) {
    out << n << (n == 1 ? " partition" : " partitions");
}

/// Executes statements on ONE connected client for the whole session.
struct LiveExecutor {
    rune::client::Client& client;
    bool                  validate;
    uint16_t              expectedVersion;

    ExecCode execute(const rune::ql::Statement& st, std::ostream& out, std::ostream& err) {
        if (const auto* td = std::get_if<rune::ql::TopicDef>(&st)) return define(*td, out, err);
        return alter(std::get<rune::ql::AlterTopicDef>(st), out, err);
    }

    ExecCode define(const rune::ql::TopicDef& td, std::ostream& out, std::ostream& err) {
        const rune::ql::DefineTopicPlan plan = rune::ql::applyPlan(td);
        const rune::client::DefineTopicResult r = client.defineTopicAndWait(
            plan.name, plan.partitions, plan.keyField, validate,
            std::span<const uint8_t>(plan.descriptorBlob.data(), plan.descriptorBlob.size()),
            toClientPersistence(td.persistence), kCliWait);
        if (r.errorCode == static_cast<uint16_t>(rpc::ErrorCode::TopicExists)) {
            out << "ok " << topicNoun(td) << plan.name
                << " already exists (topicId=" << r.topicId << ")\n";
            return 0;
        }
        if (r.errorCode != 0) return reportBrokerError(plan.name, r.errorCode, err);
        out << "ok " << topicNoun(td) << plan.name << " v" << r.version << " (";
        printPartitions(out, plan.partitions);
        out << ")\n";
        return 0;
    }

    ExecCode alter(const rune::ql::AlterTopicDef& ad, std::ostream& out, std::ostream& err) {
        const rune::ql::AlterTopicPlan plan = rune::ql::applyAlterPlan(ad, expectedVersion);
        const rune::client::AlterTopicResult r = client.alterTopicAndWait(
            plan.name, plan.expectedVersion,
            std::span<const uint8_t>(plan.opsBlob.data(), plan.opsBlob.size()), kCliWait);
        if (r.errorCode != 0) return reportBrokerError(plan.name, r.errorCode, err);
        out << "ok topic " << plan.name << " v" << r.newVersion << "\n";
        return 0;
    }
};

int runLive(const ConsoleOptions& o, const std::vector<std::string>& args, const std::string& script,
            std::istream& in, std::ostream& out, std::ostream& err) {
    rune::client::Client client(buildClientConfig(args));
    if (!connectOrReport(client, err)) return 1;   // closes the client on failure

    LiveExecutor exec{client, o.validate, o.expectedVersion};
    const int rc = o.scriptPath.empty() ? runInteractive(exec, in, out, err)
                                        : runScript(exec, o.scriptPath, script, out, err);
    client.close();
    return rc;
}

#else

int runLive(const ConsoleOptions&, const std::vector<std::string>&, const std::string&,
            std::istream&, std::ostream&, std::ostream& err) {
    err << "rune console: live execution is not available in this build "
           "(requires RUNE_CLI_LIVE_ENABLED and a real Aeron build). "
           "Use --dry-run to check statements offline.\n";
    return 1;
}

#endif // RUNE_CLI_LIVE_ENABLED
```

(A timeout surfaces as the `*AndWait` exception, whose message contains "timed out". `executeAll` reports it as `error: <what()>`, exit 1. That error ends neither the session nor the client.)

- [ ] **Step 5: Run the tests**

```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R Regression --output-on-failure
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|CliLive|Console' --output-on-failure
```
Expected: all PASS, including all 15 `ConsoleLive*` tests.

- [ ] **Step 6: Commit (rune-client-cpp)**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli/Console.cpp tests/RuneCliLiveTest.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -F - <<'EOF'
feat(cli): rune console live execution on one connected client

topic / transient topic / alter topic run on a single Client for the whole
session; ok lines per spec, broker errors printed by wire ErrorCode name;
-f stops at the first failure with the errorCode as exit status.
Migrated the schema apply/alter live assertions into ConsoleLive*.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 5: Remove `rune schema apply|alter`; finish test migration

**Files:**
- Modify: `rune-client-cpp/src/cli/Cli.cpp` (delete `reportAlterResult`, `cmdSchemaApply`, `cmdSchemaAlter`; dispatch; usage)
- Modify: `rune-client-cpp/tests/RuneCliOfflineTest.cpp` (rewrite)
- Modify: `rune-client-cpp/tests/RuneCliLiveTest.cpp` (delete migrated tests, header comment)
- Modify: `rune-client-cpp/CMakeLists.txt` (two comments)

**Interfaces:**
- Consumes: the console tests from Tasks 3/4, which already carry the migrated assertions.
- Produces: `rune schema apply|alter` → exit 1 with `rune schema <sub> was removed: …rune console -f…`.

**Migration map** (old test → console test that carries its assertions):

| Removed test | Carried by |
|---|---|
| `RuneCliOffline.ApplyDryRunPrintsDescriptor`, `RuneCliOfflineRegression.ApplyDryRunPrintsDescriptor`, `RuneCliOffline.ApplyDryRunStillDefinesTopicFile` | `ConsoleOfflineRegression.DryRunScriptPrintsDescriptor` |
| `RuneCliOffline.ApplyReportsParseErrorWithLocation`, `RuneCliOfflineRegression.ApplyReportsParseErrorWithLocationPrefix` | `ConsoleOfflineRegression.ScriptParseErrorHasFileLineColPrefix` |
| `RuneCliOffline.AlterDryRunPrintsParsedOps`, `RuneCliOffline.ApplyDryRunRoutesAlterFileToAlterPath`, `RuneCliOfflineRegression.ApplyDispatchesOnParseVariant` | `ConsoleOfflineRegression.DryRunDispatchesEachStatementKind` |
| `RuneCliOfflineRegression.AlterDryRunPrintsAllOpKinds` | `ConsoleOfflineRegression.DryRunPrintsAllAlterOpKinds` |
| `RuneCliOffline.AlterRejects{NonNumeric,OutOfRange,Negative}ExpectedVersion`, `RuneCliOfflineRegression.ExpectedVersionMalformedIsCleanErrorNotCrash` | `ConsoleOfflineRegression.ExpectedVersionMalformedIsCleanErrorNotCrash` |
| `RuneCliOffline.AlterRejectsTopicStatement`, `RuneCliOfflineRegression.AlterRejectsTopicStatement` (the console accepts both kinds by design; what the test guarded was `schema alter`) | `RuneCliOfflineRegression.SchemaApplyAndAlterAreRemovedWithConsolePointer` |
| `CliLive.SchemaApplyLive` | `ConsoleLive.ScriptDefinesTopic` |
| `CliLive.SchemaApplyTopicExists` | `ConsoleLive.ScriptTopicExistsIsOk` |
| `CliLiveRegression.ApplyLive_TopicNameInOutput` | `ConsoleLiveRegression.ScriptTopicNameInOutput` |
| `CliLive.AlterTopicLive` | `ConsoleLive.ScriptAltersTopicWithExpectedVersion` |
| `CliLive.AlterTopicSchemaIncompatible` | `ConsoleLive.ScriptAlterSchemaIncompatible` |
| `CliLive.AlterTopicSchemaConflict` | `ConsoleLive.ScriptAlterSchemaConflict` |
| `CliLive.ApplyAlterFileLive` | `ConsoleLive.ScriptDefinesThenAltersInOneSession` |
| `CliLiveRegression.AlterLive_NewVersionInOutput` | `ConsoleLiveRegression.AlterNewVersionInOutput` |

- [ ] **Step 1: Write the failing tests.** Replace the whole of `tests/RuneCliOfflineTest.cpp` with:

```cpp
// tests/RuneCliOfflineTest.cpp
//
// rune CLI offline tests (no Aeron). Rune QL execution moved to `rune console`
// in Phase 2b Part D; the former `schema apply|alter --dry-run` assertions live
// in tests/RuneConsoleOfflineTest.cpp (ConsoleOffline*).
#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "rune/cli/Cli.h"

// Regression (Phase 2b D): `schema apply` / `schema alter` are removed. They
// must fail cleanly, point at `rune console -f`, and execute nothing — never
// silently succeed (e.g. as a dry-run) or reach a broker.
TEST(RuneCliOfflineRegression, SchemaApplyAndAlterAreRemovedWithConsolePointer) {
    for (const std::string sub : {"apply", "alter"}) {
        std::ostringstream out, err;
        const int rc = rune::cli::run({"schema", sub, "whatever.rql", "--dry-run"}, out, err);
        EXPECT_EQ(rc, 1) << sub;
        EXPECT_NE(err.str().find("rune schema " + sub + " was removed"), std::string::npos) << err.str();
        EXPECT_NE(err.str().find("rune console -f"), std::string::npos) << err.str();
        EXPECT_TRUE(out.str().empty()) << "a removed subcommand must not run anything; got " << out.str();
    }
}

TEST(RuneCliOffline, UsageListsConsoleAndReadOnlySchema) {
    std::ostringstream out, err;
    EXPECT_EQ(rune::cli::run({}, out, err), 1);
    const std::string u = err.str();
    EXPECT_NE(u.find("rune console"), std::string::npos) << u;
    EXPECT_NE(u.find("rune schema get"), std::string::npos) << u;
    EXPECT_NE(u.find("rune schema list"), std::string::npos) << u;
    EXPECT_NE(u.find("rune schema generate"), std::string::npos) << u;
    EXPECT_EQ(u.find("schema apply"), std::string::npos) << u;
    EXPECT_EQ(u.find("schema alter"), std::string::npos) << u;
}
```

- [ ] **Step 2: Run to verify they fail**

```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j --target rune_cli_offline_tests
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCliOffline' --output-on-failure
```
Expected: both FAIL. `apply` exits 1 with `cannot open file`, without the "was removed" text. `alter` exits 1 for the same reason. The usage text still lists `schema apply`.

- [ ] **Step 3: Implement in `src/cli/Cli.cpp`.**

(a) Delete these blocks entirely:
  - From `#ifdef RUNE_CLI_LIVE_ENABLED` / `// Shared AlterTopicResult reporting for both \`apply\`` through the `#endif` closing `reportAlterResult`.
  - From `// ── schema apply ──` through the closing `}` of `cmdSchemaApply`.
  - From `// ── schema alter ──` through the closing `}` of `cmdSchemaAlter`.

(b) Replace the whole `printUsage` function with:

```cpp
void printUsage(std::ostream& err) {
    err << "Usage:\n"
        << "  rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->]\n"
        << "               [--dry-run] [--validate] [--expected-version N]\n"
        << "      Run Rune QL statements (topic …; transient topic …; alter topic …;)\n"
        << "      interactively (prompt `rune> `) or from a script (-f; stops at the\n"
        << "      first error). --dry-run prints each statement's plan (no broker).\n"
        << "  rune schema get <topic> --broker <ep>\n"
        << "      Fetch topic schema from broker and print descriptor.\n"
        << "  rune schema list [--verbose] --broker <ep>\n"
        << "      List topics known to this session.\n"
        << "  rune schema generate --from-file <blob> --out <dir>\n"
        << "      Generate C++ header from a serialized descriptor blob.\n"
        << "  rune schema generate <topic> --out <dir> [--broker <ep>]\n"
        << "      Fetch topic schema from broker and generate C++ header (live).\n";
}
```

(c) In `cmdSchema`, replace

```cpp
        err << "rune schema: subcommand required (apply|alter|get|list|generate)\n";
```
with
```cpp
        err << "rune schema: subcommand required (get|list|generate)\n";
```
and replace

```cpp
    if (sub == "apply") {
        return cmdSchemaApply(subArgs, out, err);
    }
    if (sub == "alter") {
        return cmdSchemaAlter(subArgs, out, err);
    }
```
with
```cpp
    if (sub == "apply" || sub == "alter") {
        // Phase 2b Part D: Rune QL statements run through `rune console`.
        err << "rune schema " << sub << " was removed: run the statements with "
               "`rune console -f <file.rql>` (or interactively with `rune console`)\n";
        return 1;
    }
```

(d) Unused now: delete `#include "rune/ql/Parser.h"`, `#include "rune/ql/Backends.h"`, `#include <charconv>` and `#include <variant>` from `Cli.cpp`. If any `#include` turns out to still be needed, the compiler reports it; restore only that one.

(e) `tests/RuneCliLiveTest.cpp`:
  - Delete these TESTs entirely: `CliLive.SchemaApplyLive`, `CliLive.SchemaApplyTopicExists`, `CliLiveRegression.ApplyLive_TopicNameInOutput`, `CliLive.AlterTopicLive`, `CliLive.AlterTopicSchemaIncompatible`, `CliLive.AlterTopicSchemaConflict`, `CliLive.ApplyAlterFileLive`, `CliLiveRegression.AlterLive_NewVersionInOutput`. Also delete the `// Task 20: \`rune schema alter\` + \`apply\` routing …` banner comment.
  - Replace the file's header comment (lines 1-37, from `// tests/RuneCliLiveTest.cpp` through `// Stream ranges start at 1200 …`) with:

```cpp
// tests/RuneCliLiveTest.cpp
//
// rune CLI live tests via the in-process CliTestGateway loopback (real Aeron):
//   - `rune schema get|list` (CliLive.*, CliLiveRegression.*)
//   - `rune console` (Phase 2b Part D: ConsoleLive.*, ConsoleLiveRegression.*),
//     which carries the assertions of the removed `rune schema apply|alter`
//     live tests (see docs plan 2026-10-11-rune-phase2b-d-rune-console.md).
//
// All waits are bounded (kCliTimeout). Stream IDs: 1200-1279 (ClientSchemaTest
// uses 1100-1119).
```

(f) `CMakeLists.txt` comments: replace `# Tests schema apply --dry-run (and generate --from-file) without Aeron.` with `# Tests rune console --dry-run and the removed-subcommand errors without Aeron.`. Replace `# ── Task 9: rune CLI live tests (schema apply/get/list via loopback Aeron) ─────` with `# ── rune CLI live tests (schema get/list + rune console via loopback Aeron) ────`.

- [ ] **Step 4: Run the tests**

```bash
cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R Regression --output-on-failure
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|CliLive|Console' --output-on-failure
grep -n "cmdSchemaApply\|cmdSchemaAlter\|reportAlterResult" /Users/vradu/Projects/se-worktrees/rune-client-cpp/src/cli/*.cpp
```
Expected: all PASS, and the final grep prints nothing.

- [ ] **Step 5: Commit (rune-client-cpp)**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli/Cli.cpp tests/RuneCliOfflineTest.cpp tests/RuneCliLiveTest.cpp CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -F - <<'EOF'
feat(cli)!: rune schema is read-only (list|get|generate)

schema apply/alter are removed and point at `rune console -f`; their
offline/live assertions now live in the ConsoleOffline*/ConsoleLive* tests
(migration map in the Phase 2b D plan).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

### Task 6: Real-path e2e: the e2e scripts drive `rune console -f`

**Files:**
- Modify: `rune-client-cpp/docker/rune-ql-e2e.sh`, `docker/Dockerfile.rune-ql-probe`, `docker/docker-compose.rune-ql.yml`, `CMakeLists.txt` (comments), `tests/e2e/characters.rql`, `tests/e2e/schema_evolution/v1.rql`, `tests/e2e/rune_ql_probe.cpp`, `tests/e2e/evo_probe_v{1,2,3,4}.cpp` (comments)
- Modify: `rune-broker-matviews/docker/schema-evolution-e2e.sh`

**Interfaces:**
- Consumes: the `rune console` exit-code contract (Task 4): `-f` returns the broker errorCode, and stderr carries `SchemaIncompatible` + "incompatible" and `SchemaConflict` + "stale". It also consumes `--validate` and `--expected-version` (Task 3).
- Produces: both real-broker e2e flows green on `rune console -f`.

- [ ] **Step 1: Confirm the scripts are broken.** With Task 5 landed, the scripts would fail at their first `rune schema apply`. Confirm every call site:

```bash
grep -rn "rune schema \(apply\|alter\)" /Users/vradu/Projects/se-worktrees/rune-client-cpp/docker /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e /Users/vradu/Projects/se-worktrees/rune-client-cpp/CMakeLists.txt /Users/vradu/Projects/rune-broker-matviews/docker
```
Expected hits (current code):
- `rune-ql-e2e.sh`: lines 12, 137, 138, 200.
- `Dockerfile.rune-ql-probe`: line 13.
- `docker-compose.rune-ql.yml`: line 55.
- `CMakeLists.txt`: lines 582, 619.
- `characters.rql`: line 9.
- `v1.rql`: line 5.
- `rune_ql_probe.cpp`: lines 9, 28, 172.
- `evo_probe_v1..v4.cpp`: lines 3-4.
- `schema-evolution-e2e.sh`: lines 14-28, 229, 243, 255, 268, 281, 285, 319, 323.

- [ ] **Step 2: Rewrite the invocations.** The flags carry over unchanged: `--broker`, `--validate`, `--expected-version`, `--cli-egress`, `--cli-data-dir`.

```bash
perl -pi -e 's/rune schema (apply|alter)\b/rune console -f/g' \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/docker/rune-ql-e2e.sh \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/docker/Dockerfile.rune-ql-probe \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/docker/docker-compose.rune-ql.yml \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/CMakeLists.txt \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/characters.rql \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/schema_evolution/v1.rql \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/rune_ql_probe.cpp \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/evo_probe_v1.cpp \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/evo_probe_v2.cpp \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/evo_probe_v3.cpp \
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/evo_probe_v4.cpp \
  /Users/vradu/Projects/rune-broker-matviews/docker/schema-evolution-e2e.sh
```

Then make three manual edits:

(a) `rune-client-cpp/docker/rune-ql-e2e.sh`. After the perl pass, line 137 reads `echo "[in] rune console -f (validate=on) ..." >&2`, which is fine. Line 200 now reads `log "  rune console -f --validate -> generate -> compile probe -> run"`. Replace it with:
```bash
log "  rune console -f characters.rql --validate -> generate -> compile probe -> run"
```

(b) `rune-broker-matviews/docker/schema-evolution-e2e.sh`. Replace the comment
```bash
# Tighten beyond "any nonzero exit": the CLI's cmdSchemaAlter returns the
# broker's wire errorCode as its process exit code (rune::cli reportAlterResult
# -> `return static_cast<int>(r.errorCode)`), and
```
with
```bash
# Tighten beyond "any nonzero exit": `rune console -f` stops at the first
# failing statement and returns the broker's wire errorCode as its process
# exit code (rune::cli LiveExecutor -> reportBrokerError), and
```

(c) Same file: the phase-7 race still works unchanged. Each racer is `rune console -f race_X.rql --expected-version 5`. The loser exits 31, and its stderr contains `SchemaConflict` and `stale`, which `grep -qi "stale\|conflict"` matches. No edit is needed; just verify this by reading lines 317-345 after the perl pass.

- [ ] **Step 3: Verify no stale reference remains**

```bash
grep -rn "rune schema \(apply\|alter\)" /Users/vradu/Projects/se-worktrees/rune-client-cpp/docker /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e /Users/vradu/Projects/se-worktrees/rune-client-cpp/CMakeLists.txt /Users/vradu/Projects/rune-broker-matviews/docker
bash -n /Users/vradu/Projects/se-worktrees/rune-client-cpp/docker/rune-ql-e2e.sh
bash -n /Users/vradu/Projects/rune-broker-matviews/docker/schema-evolution-e2e.sh
```
Expected: the grep prints nothing; both `bash -n` checks pass (no syntax errors).

- [ ] **Step 4: Run the real-broker e2e** (CLAUDE.md #5: the real path green). Run with the canonical sibling layout (`../rune-client-cpp`, `../rune-ql`, `../rune-protocol`, `../rune-codec` on their `feat/phase2b-d-rune-console` / integration branches):

```bash
cd /Users/vradu/Projects/rune-client-cpp && bash docker/rune-ql-e2e.sh
cd /Users/vradu/Projects/rune-broker-matviews && bash docker/schema-evolution-e2e.sh
```
Expected:
- `rune-ql-e2e.sh` ends with `TASK 10 (0c #11 GATE) CLI-DRIVEN E2E PASSED`.
- `schema-evolution-e2e.sh` passes all phases 1-8. In phase 5 the drop-under-forward is rejected with exit 30 plus an "incompatible" message. In phase 7 there is exactly one success and one SchemaConflict.

- [ ] **Step 5: Regression gates + commit (two repos)**

```bash
ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R Regression --output-on-failure
ctest --test-dir /Users/vradu/Projects/rune-broker-matviews/build-fast -R Regression --output-on-failure

git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add docker/rune-ql-e2e.sh docker/Dockerfile.rune-ql-probe docker/docker-compose.rune-ql.yml CMakeLists.txt tests/e2e/characters.rql tests/e2e/schema_evolution/v1.rql tests/e2e/rune_ql_probe.cpp tests/e2e/evo_probe_v1.cpp tests/e2e/evo_probe_v2.cpp tests/e2e/evo_probe_v3.cpp tests/e2e/evo_probe_v4.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -F - <<'EOF'
test(e2e): drive rune-ql e2e through `rune console -f`

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF

git -C /Users/vradu/Projects/rune-broker-matviews add docker/schema-evolution-e2e.sh
git -C /Users/vradu/Projects/rune-broker-matviews commit -F - <<'EOF'
test(e2e): schema-evolution e2e drives `rune console -f` (schema apply/alter removed)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
EOF
```

---

## Self-review

- **Spec coverage (§D):**
  - `parseScript` (one or more `;`-terminated statements, `--` comments, per-statement positions) → Task 1. `parse()` is unchanged.
  - `rune console` flags `--broker` / `--cli-egress` / `-f` and the env vars → Task 3 (`parseConsoleOptions`), Task 2 (`buildClientConfig`).
  - Prompt and continuation `   ...> ` → Task 3.
  - One client per session → Task 4 (`OneConnectionForWholeSession`).
  - ok line formats → Task 4.
  - `line:col: message` parse errors → Tasks 1/3.
  - Broker error name → Task 4 (`brokerErrorName`).
  - Errors never end the session → Tasks 3/4.
  - `exit`/`quit`/EOF → Task 3.
  - `-f` stops at the first error with a non-zero exit → Tasks 3/4.
  - Statements `topic` / `transient topic` / `alter topic` → Tasks 1/3/4, with Part E as a prerequisite.
  - `rune schema` keeps list/get/generate and removes apply/alter, with tests migrated → Task 5 (table).
  - Known limitation of `schema list` kept, with the usage text saying "known to this session".
  - Gates: multi-statement script, multi-line, parse error keeps the session, broker error keeps the session, `-f` stops at the first error with non-zero exit, exit/EOF, live tests against the CLI gateway → Tasks 3/4.
- **Placeholders:** none. The two `<n>` in the Task 1 commit body are measured values to be filled in, and the step says so explicitly.
- **Type consistency:**
  - `Statement`, `InputState`, `classifyInput`, `parseScript` (Task 1) match their use in Console.cpp (Tasks 3/4).
  - `readSource(path, in, src, err)` is defined in Task 2 and called in Task 2 (two sites) and Task 3.
  - `runLive` has the same signature in Task 3's stub and in Task 4's replacement and `#else` branch.
  - `runCli`'s new trailing parameter is defaulted, so existing calls compile unchanged.
- **Review Focus:** five lines, each pinned by named tests in Tasks 1 and 3.

## Spec questions

1. **`--dry-run` on `rune console`.** The spec says the offline tests of `schema apply|alter` migrate "to console tests (same assertions)". Those assertions are all `--dry-run` (no broker), so the console needs an offline mode. This plan adds `rune console --dry-run`, which prints each statement's descriptor or alter plan. Confirm that it is wanted as a user-facing flag.
2. **`--validate`.** The removed `schema apply --validate` sets DefineTopic's `validate` bit, and the e2e scripts depend on it. The spec does not say how the console sets it. This plan adds a session-wide `rune console --validate`.
3. **`--expected-version N`.** The optimistic-concurrency check for `alter topic` has no Rune QL syntax. The broker e2e phase 7 (concurrent alter → SchemaConflict) and the migrated regression `ExpectedVersionMalformedIsCleanErrorNotCrash` need it. This plan adds a session-wide `--expected-version N`: every `alter topic` in the session asserts version N, and the default 0 means no check. A Rune QL clause such as `alter topic t … if version 5;` would be a Phase 3 alternative.
4. **`TopicExists`.** The old CLI treated it as success ("already exists", exit 0). This plan keeps that (`ok topic X already exists (topicId=N)`), so re-running a script is idempotent. The alternative is to print it as a broker error (`TopicExists`) and stop the `-f` script.
5. **`-f` and parse errors.** This plan parses the whole script before executing anything, so a syntax error anywhere executes nothing (exit 2). The spec's "stops at the first error" is honored for broker errors: statements before the failing one have already been applied. Confirm that all-or-nothing parsing is intended.
6. **`-f` exit code.** This plan returns the broker's wire `errorCode` (e.g. 30), which keeps the convention that `schema-evolution-e2e.sh` asserts. The spec only says "non-zero".
7. **Several statements in one interactive input.** If statement 1 of `topic a …; topic b …;` fails, this plan skips the rest of that input (same rule as a script), and the session continues. The spec does not say.
8. **Ok line for transient topics.** This plan prints `ok transient topic ticks v1 (2 partitions)`, and `(1 partition)` (singular) when N == 1. The spec only gives the disk-topic example.
9. **Error stream.** Parse and broker errors go to stderr; `ok` lines and prompts go to stdout.
10. **Repo scope.** The spec lists D as ql + client, but removing `schema apply|alter` breaks `rune-broker/docker/schema-evolution-e2e.sh`. Task 6 therefore also edits that broker script (no broker C++).
11. **Part E names.** This plan assumes `rune::client::Persistence { Disk, Transient }`, a `defineTopicAndWait(…, Persistence, timeout)` overload, and `rpc::DefineTopic::persistence`. If Part E's plan uses other names, the three adaptation points are listed in *Prerequisites*.
