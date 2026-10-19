# Rune Phase 2b — Part D: `rune console` (Rune QL); `rune schema` read-only — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Updated 2026-10-19** — re-checked against the code after catalog history (`feat/catalog-history`, broker `c50b06cd`) and against the authentication plan (`2026-10-19-rune-authentication.md`). Changes:
> 1. **`parseScript` already exists** (catalog history Task 14, ruling F8): `rune::ql::parseScript` returns `std::expected<std::vector<Statement>, ScriptError>` (`ScriptError{statementIndex, error}`, positions already absolute, an empty script is an empty vector). The old Task 1 implementation is **dropped**; Task 1 now adds only `classifyInput` plus regression pins over the existing `parseScript` (CRLF, `;` in a comment, absolute positions).
> 2. **`rune ql -e|-f` already executes every statement** (catalog history Task 15) through `prepare()` / `QlRun` / `LiveBroker` in `src/cli/CatalogCommands.cpp`. The old Task 2 (extract `CliCommon` from `Cli.cpp`) and the old `LiveExecutor` are **replaced**: Task 2 now moves the `rune ql` engine into `src/cli/QlEngine.{h,cpp}` so the console and `rune ql` share one executor. `Statement` now has 8 alternatives (topic, alter, alter-config, drop topic/view, show history, show as-of, diff) and gains the security statements in the auth phase: the console runs whatever `rune ql` runs.
> 3. **DDL flows through `__catalog`**: every DDL goes to the catalog leader (the client follows `Redirect` / retries `NotLeader`), carries `DdlOptions{statement, requestId}`. The console passes each statement's own text; `--request-id` is accepted with `-f` exactly like `rune ql -f`.
> 4. **`RUNE_TOPICS` is gone**: topics exist only through DDL. Task 6 must not reintroduce it (`scripts/rune_topics_allowlist.txt` grep gate).
> 5. **Live tests move to real brokers** (catalog ruling F7): blocking `rune::cli::run(... --broker ...)` cases run in the broker repo's `rune_client_broker_tests` against in-process `OneBroker`s, not against the client repo's single-threaded `CliTestGateway`.
> 6. **Auth interactions** (auth runs before D): the console logs in with `--user` / `--password-file` (or `RUNE_USER` / `RUNE_PASSWORD_FILE`; never a password in argv / env); an interactive console with `--user` and no password file prompts with echo off; the dev-mode banner and error names 44–47 come from the shared engine; `SessionRevoked` ends the console (auth D26). No new request types → no new grants; each statement needs its request's grant from the auth plan's op table.
> 7. **Interactive idle**: the CLI client runs `RunnerMode::Invoker`; a console waiting at its prompt must keep pumping `Client::doWork()` or its session lapses. Task 4 adds a polling line source.
> 8. Repos / paths fixed: worktrees `/Users/vradu/Projects/se-worktrees/*` only (the old `rune-broker-matviews` path is gone); docker e2e runs on the k3s VM under `flock /root/vm.lock`, never on the Mac.
> 9. Exit codes aligned with the existing CLI (2 = usage, 1 = local / parse error, N = the broker error code of the failing statement).

**Goal:** A `rune console` command runs Rune QL statements interactively (`rune> ` prompt) or from a file (`-f`) over one broker session, sharing one executor with `rune ql`. `rune schema` becomes read-only (`list|get|generate`); the old `schema apply|alter` assertions move to console tests.

**Architecture:** rune-ql gains `classifyInput` (Empty / Incomplete / Complete on the existing lexer, so a `;` inside a `--` comment or a `'…'` string never ends a statement). In rune-client-cpp the `rune ql` engine (statement preparation, dry-run printing, the connected `LiveBroker`, the per-statement `QlRun` executor) moves out of `CatalogCommands.cpp` into `src/cli/QlEngine.{h,cpp}` unchanged, gaining an output style (`Ql` / `Console`) and the last broker error code. `src/cli/Console.{h,cpp}` holds the console: options, a two-state REPL (`AwaitingStatement` / `ContinuingStatement{text}`), the script runner, and a `LineSource` (an injected `std::istream` for tests and files; a polling stdin reader for a TTY that pumps the client while idle).

**Tech Stack:** C++23, GoogleTest, Google Benchmark, CMake (rune-ql `build`, rune-client-cpp `build`, broker `build-fast` Ninja), real in-process `BrokerNode`s (`OneBroker`) for live tests, docker e2e on the k3s VM.

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` §D (binding).

**Builds on:** catalog history (`feat/catalog-history`) and authentication (`docs/superpowers/plans/2026-10-19-rune-authentication.md`, merged before D). Part E (transient topics) is already in the code (`ql::Persistence`, `TopicDef::persistence`, `rc::Persistence`).

## Execution context

- Branch `feat/phase2b-d-rune-console` in rune-ql, rune-client-cpp and rune-broker, cut from each repo's integration tip **after the authentication phase merged** (`feat/authentication`, or wherever it was merged):
  ```bash
  for r in rune-ql rune-client-cpp rune-broker; do
    git -C /Users/vradu/Projects/se-worktrees/$r switch -c feat/phase2b-d-rune-console feat/authentication
  done
  ```
  Verify before Task 1 (each must hit): `grep -n "parseScript" /Users/vradu/Projects/se-worktrees/rune-ql/include/rune/ql/Parser.h`, `grep -n "struct QlRun" /Users/vradu/Projects/se-worktrees/rune-client-cpp/src/cli/CatalogCommands.cpp`, `grep -n "passwordFile" /Users/vradu/Projects/se-worktrees/rune-client-cpp/include/rune/client/Client.h`, `grep -n "authStatus" /Users/vradu/Projects/se-worktrees/rune-client-cpp/include/rune/client/Client.h`. If the auth greps miss, auth has not merged: stop and report.
- The client compiles `../rune-ql/src/*.cpp`, `../rune-codec` and `../rune-protocol/include`; the broker's `rune_client_broker_tests` compiles the client from `RUNE_CLIENT_ROOT` (default `../rune-client-cpp`). All three worktrees stay on `feat/phase2b-d-rune-console` while building.
- **Builds:** rune-ql `cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build -j14 --target rune_ql_tests rune_ql_bench`; client `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14`; broker `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_client_broker_tests`.
- **Test gate (CLAUDE.md #7):** each task runs the executables covering what it touches plus every new or changed test (its "Run" step names the filter). The FULL `*Regression*` suite runs at the checkpoints — after Task 2 (A), after Task 5 (B), after Task 6 (C, before the final review) — in every repo touched since the last checkpoint: `ctest --test-dir <build> -R Regression --output-on-failure -j14`, plus the client TSan build (`build_tsan`, `BUILD_TESTING=ON`) and broker ASan+UBSan / TSan (`build-asan`, `build-tsan`) per the sanitizer-checkpoint rule. A checkpoint failure is bisected over the commits since the last green checkpoint.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Prerequisites (already in the code — verify, do not re-implement)

| Needed | Where it is today |
|---|---|
| `rune::ql::parseScript(std::string_view) noexcept → std::expected<std::vector<Statement>, ScriptError>` (1-based `statementIndex`, absolute `line:col`, empty script → empty vector) | `rune-ql/include/rune/ql/Parser.h`, `src/Parser.cpp` (`Parser p{src}` over the whole source) |
| `rune::ql::Persistence { Disk, Transient }`, `TopicDef::persistence`, `transient topic …;` | `rune-ql/include/rune/ql/Ast.h` |
| `prepare(const ql::Statement&, std::string& why) → std::optional<Prepared>`, `printPlan`, `splitStatements`, `readScript`, `LiveBroker`, `QlRun`, `errorCodeName`, `brokerFailure`, `IdSeq` | `rune-client-cpp/src/cli/CatalogCommands.cpp` (anonymous namespaces) |
| `defineTopicAndWait(name, partitions, keyField, descriptor, validate, Persistence, TopicOptions, DdlOptions, timeout)`, `alterTopicAndWait(name, expectedVersion, ops, DdlOptions, timeout)` | `rune-client-cpp/include/rune/client/Client.h` |
| Credentials (`ClientConfig::{username, passwordFile}`), `Client::authStatus()`, error codes 44–47, the dev-mode banner, `--user` / `--password-file` on `rune ql` | auth plan Tasks 7–8 |

## Global Constraints

- HARD RULES (CLAUDE.md, all eight): shared-nothing; rings only across threads (the console is single-threaded: the CLI thread is the Invoker duty thread); per-state data (`ReplState = std::variant<AwaitingStatement, ContinuingStatement>`; the line source's pending-line buffer belongs to the source); zero steady-state allocation applies to hot paths — the console is control plane and touches none; a `*Regression*` test on every fix; fix-on-sight.
- Spec §D, verbatim: `rune console [--broker <aeron channel>] [--cli-egress <ch>] [-f script.rql]`; broker also from `RUNE_BROKER` / `RUNE_EGRESS`. Interactive: prompt `rune> `, continuation `   ...> ` until a `;` outside comments; one connected client for the whole session; prints `ok topic player v1 (8 partitions)` / `ok topic player v2` or the error (`line:col: message` for parse errors, the broker error name for broker errors); an error never ends the session; `exit` / `quit` / EOF leaves. `-f script.rql`: same execution over a file; stops at the first error, exit code non-zero.
- `rune schema` keeps `list`, `get`, `generate` and **removes `apply` and `alter`**; their tests migrate to console tests with the same assertions (Task 5 table). Known limitation kept: `schema list` shows only topics known to the session.
- Published names (arcanum docs): the `rune>` console; `rune schema list|get|generate`.
- **Exit codes** (`rune console`, aligned with `rune ql` / `rune catalog`): `0` success, or an interactive session left by `exit` / `quit` / EOF; `2` usage error; `1` parse error (`-f`), I/O error, connect failure, timeout, local refusal; otherwise the broker's wire `errorCode` of the first failing statement in `-f` mode (keeps `schema-evolution-e2e.sh`'s exit-30 assertion). An interactive session ended by `SessionRevoked` exits `46`.
- No wire change, no new error code, no new request type.

## Decisions (binding for executors)

- **DD1 — One engine.** The console executes through the same `prepare` / `QlRun` as `rune ql`; it runs every statement `rune ql` runs (spec's "statements in 2b" is the minimum). Only the success lines differ (`OutputStyle::Console`).
- **DD2 — Console success lines:** `ok topic <n> v<version> (<P> partitions)` (`(1 partition)` when P = 1; `ok transient topic …` for transient; an untyped topic prints no `v<version>`); `ok topic <n> v<newVersion>` for `alter topic` ops; `ok topic <n> config v<newVersion>`; `ok dropped topic <n>` / `ok dropped view <n>`; reads print exactly as `rune ql`. Any statement kind added later (the auth phase's security statements) prints `ok ` + the `rune ql` line.
- **DD3 — `--dry-run`** (needed to carry the offline `schema apply|alter --dry-run` assertions): prints `printPlan` per statement, no broker.
- **DD4 — `--validate`** (session-wide, replaces `schema apply --validate`): every `topic …;` of the session is defined with `validate = true`. `rune ql` gains the same flag (one engine).
- **DD5 — `--expected-version N`** (replaces `schema alter --expected-version`; needed by the schema-evolution race phase): with `-f`, every `alter topic … <ops>;` in the script asserts version N (0 = no check, the default); `rune ql` gains it too. Malformed values are usage errors (exit 2).
- **DD6 — `-f` parses everything first** (`parseScript` + `prepare` for all statements): a parse or prepare error issues nothing (exit 1, `<file>:statement N: line:col: message`). Broker errors stop at the failing statement; earlier statements stay applied.
- **DD7 — Interactive batches:** a completed input with several statements runs them in order; a failure skips the rest of that input, the session continues.
- **DD8 — `TopicExists` is an error** (as in `rune ql`); idempotent re-runs use `--request-id` (`-f` only; statement N uses `{hi, lo + N − 1}`).
- **DD9 — Idle pumping:** with a TTY on stdin and a live broker, the console reads lines with `poll(2)` (50 ms) and calls `Client::doWork()` between polls; tests inject an `std::istream` (no pumping needed: input is immediate).
- **DD10 — Login:** credentials as `rune ql` (auth Task 8). Interactive + `--user` + no `--password-file` + stdin is a TTY → prompt `password for <user>: ` with echo off (termios), the buffer wiped (`explicit_bzero`) after connect; otherwise a missing password is a usage error. `PasswordChangeRequired` on connect: the console still opens and prints `WARNING: password change required — run ALTER USER <u> SET PASSWORD '…';` (the only statement the broker accepts then).
- **DD11 — `SessionRevoked`** (46) ends the console with `error: SessionRevoked (46): the session ended` and exit 46 (no reconnect, auth D26). `NotAuthorized` (45) is an ordinary statement error.
- **DD12 — stdout / stderr:** `ok` lines, plans, read output and prompts on stdout; every error on stderr.

## Review Focus

Most likely to bite a user, each pinned by a named test:

1. **A `;` inside a `--` comment or a `'…'` string** must not end the statement → `ClassifyInputRegression.SemicolonInsideCommentIsIncomplete`, `ClassifyInputRegression.SemicolonInsideStringIsIncomplete` (Task 1), `ConsoleOfflineRegression.SemicolonInsideCommentDoesNotEndStatement` (Task 3).
2. **The final statement has no `;`** (file saved without it, EOF mid-statement) — reported, never silently dropped or executed → `ParseScriptRegression.UnterminatedFinalStatementIsErrorNotDropped` (Task 1), `ConsoleOfflineRegression.UnterminatedStatementIsReportedNotExecuted` (Task 3).
3. **A parse error in statement N** points at the absolute `line:col` in the file → `ParseScriptRegression.ErrorPositionIsAbsoluteAcrossLines` (Task 1), `ConsoleOfflineRegression.ScriptStopsAtFirstParseErrorAndIssuesNothing` (Task 3).
4. **CRLF input** parses and `exit\r` leaves → `ParseScriptRegression.CrlfLineEndings` (Task 1), `ConsoleOfflineRegression.CrlfInputParsesAndExits` (Task 3).
5. **A mistyped / valueless flag** is a usage error, never an interactive session blocking on stdin → `ConsoleOfflineRegression.MissingValueAndUnknownArgumentAreUsageErrors` (Task 3).
6. **An idle interactive session stays connected** (Invoker client pumped while waiting) → `RuneConsoleBrokerRegression.IdlePromptKeepsTheSessionAliveRegression` (Task 4).
7. **One connection for the whole session**, a broker error keeps it → `RuneConsoleBrokerRegression.OneSessionForEveryStatementRegression`, `…BrokerErrorKeepsTheSessionRegression` (Task 4).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-ql/include/rune/ql/Parser.h`, `src/Parser.cpp` | `InputState`, `classifyInput` (appended) | 1 |
| `rune-ql/tests/ScriptTest.cpp` (new), `rune-ql/CMakeLists.txt` | classify + parseScript pins | 1 |
| `rune-ql/benchmark/ParseBench.cpp` | `BM_ParseScript3`, `BM_ClassifyInput` | 1 |
| `rune-client-cpp/src/cli/QlEngine.h`, `QlEngine.cpp` (new) | the `rune ql` engine moved out of `CatalogCommands.cpp`: `Prepared`, `PrepareOptions`, `prepare`, `printPlan`, `splitStatements`, `readScript`, `LiveOptions`, `LiveBroker`, `QlRun`, `OutputStyle`, `errorCodeName`, `brokerFailure` | 2 |
| `rune-client-cpp/src/cli/CatalogCommands.cpp` | uses `QlEngine.h`; `--validate`, `--expected-version` on `rune ql` | 2 |
| `rune-client-cpp/include/rune/cli/Console.h` (new; declares only `cmdConsole`, `runLiveOnFdForTest`), `src/cli/Console.cpp` (new; options, `LineSource`s, REPL state machine, script runner, live session — all in its anonymous namespace) | the console | 3, 4 |
| `rune-client-cpp/include/rune/cli/Cli.h`, `src/cli/Cli.cpp` | `run(args, in, out, err)`; `console` dispatch; usage; `schema apply|alter` removed | 3, 5 |
| `rune-client-cpp/tests/RuneConsoleOfflineTest.cpp` (new) | offline console tests (+ migrated dry-run assertions) | 3 |
| `rune-client-cpp/tests/RuneCliOfflineTest.cpp`, `tests/RuneCliLiveTest.cpp` | removed-subcommand regression; apply/alter tests deleted | 5 |
| `rune-client-cpp/CMakeLists.txt` | new sources / tests | 2, 3, 5 |
| `rune-broker/libs/node/tests/client/RuneConsoleBrokerRegressionTest.cpp` (new), `libs/node/CMakeLists.txt` | live console against real `OneBroker`s | 4, 5 |
| `rune-client-cpp/docker/rune-ql-e2e.sh`, `docker/Dockerfile.rune-ql-probe`, `docker/docker-compose.rune-ql.yml`, `tests/e2e/*.rql`, `tests/e2e/*.cpp` (comments) | `rune console -f` instead of `schema apply` | 6 |
| `rune-broker/docker/schema-evolution-e2e.sh` | `rune console -f` instead of `schema apply|alter` | 6 |

---

### Task 1: rune-ql — `classifyInput`; regression pins over the existing `parseScript`

**Already done (catalog history Task 14):** `parseScript` itself — do not re-implement it. Its existing pins live in `tests/CatalogStatementsTest.cpp` (`QlCatalogStatementsRegression.ParseScript*`).

**Files:**
- Modify: `rune-ql/include/rune/ql/Parser.h` (append the declarations)
- Modify: `rune-ql/src/Parser.cpp` (append before the final `} // namespace rune::ql`)
- Create: `rune-ql/tests/ScriptTest.cpp`
- Modify: `rune-ql/CMakeLists.txt` (`rune_ql_tests` sources: add `tests/ScriptTest.cpp` after `tests/CatalogStatementsTest.cpp`)
- Modify: `rune-ql/benchmark/ParseBench.cpp` (append)

**Interfaces:**
- Consumes: the TU-local `Lexer`, `Token`, `TK` in `src/Parser.cpp` (`TK::Semi`, `TK::Eof`, `TK::String`, `TK::BadString`).
- Produces:
  ```cpp
  namespace rune::ql {
  enum class InputState : uint8_t { Empty, Incomplete, Complete };
  /// Console input state of the buffered text:
  ///   Empty      — only whitespace / comments;
  ///   Incomplete — has tokens and the last one is not ';' (or an unterminated '…' string);
  ///   Complete   — the last token outside comments and strings is ';'.
  [[nodiscard]] InputState classifyInput(std::string_view src) noexcept;
  }
  ```

- [ ] **Step 1: Write the failing tests.** Create `rune-ql/tests/ScriptTest.cpp`:

```cpp
// tests/ScriptTest.cpp — Phase 2b Part D: console input classification
// (classifyInput) and pins of the console-relevant parseScript behaviour.
#include <gtest/gtest.h>

#include <variant>

#include "rune/ql/Parser.h"

using namespace rune::ql;

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

TEST(ClassifyInputRegression, SemicolonInsideStringIsIncomplete) {
    // `with (copartition.group = 'a;b')` — the ';' is inside a string token.
    EXPECT_EQ(classifyInput("topic t partitions 1 with (copartition.group = 'a;b'"), InputState::Incomplete);
    // An unterminated string never completes the input, whatever follows on the line.
    EXPECT_EQ(classifyInput("topic t partitions 1 with (copartition.group = 'a;"), InputState::Incomplete);
}

// ── parseScript pins the console relies on ───────────────────────────────────

// Statement 2 starts on line 4; its bad type is on line 5, col 5 — absolute, never statement-relative.
TEST(ParseScriptRegression, ErrorPositionIsAbsoluteAcrossLines) {
    auto r = parseScript(
        "-- header\n"
        "topic a { x int32 } partitions 1 key x;\n"
        "\n"
        "topic b {\n"
        "  x int128\n"
        "} partitions 1 key x;\n");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().statementIndex, 2u);
    EXPECT_EQ(r.error().error.line, 5u);
    EXPECT_EQ(r.error().error.col, 5u);
}

TEST(ParseScriptRegression, ErrorPositionIsAbsoluteOnSameLine) {
    auto r = parseScript("topic a { x int32 } partitions 1 key x; topic b { x int128 } partitions 1 key x;");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().statementIndex, 2u);
    EXPECT_EQ(r.error().error.line, 1u);
    EXPECT_EQ(r.error().error.col, 53u);
}

TEST(ParseScriptRegression, UnterminatedFinalStatementIsErrorNotDropped) {
    auto r = parseScript("topic a { x int32 } partitions 1 key x;\ntopic b { x int32 } partitions 1 key x");
    ASSERT_FALSE(r.has_value()) << "two statements, the second unterminated: never a 1-statement success";
    EXPECT_EQ(r.error().statementIndex, 2u);
    EXPECT_EQ(r.error().error.line, 2u);
}

TEST(ParseScriptRegression, SemicolonInCommentDoesNotSplit) {
    auto r = parseScript("topic a { x int32 -- not the end;\n } partitions 1 key x;\n");
    ASSERT_TRUE(r.has_value()) << r.error().error.message;
    EXPECT_EQ(r->size(), 1u);
}

TEST(ParseScriptRegression, CrlfLineEndings) {
    auto r = parseScript("topic a { x int32 }\r\npartitions 1 key x;\r\nalter topic a add column y int32;\r\n");
    ASSERT_TRUE(r.has_value()) << r.error().error.message;
    EXPECT_EQ(r->size(), 2u);
}

TEST(ParseScriptRegression, TransientTopicInAScript) {
    auto r = parseScript("transient topic ticks { id int64 } partitions 2 key id;\ntopic trades { id int64 } partitions 2 key id;\n");
    ASSERT_TRUE(r.has_value()) << r.error().error.message;
    ASSERT_EQ(r->size(), 2u);
    EXPECT_EQ(std::get<TopicDef>((*r)[0]).persistence, Persistence::Transient);
    EXPECT_EQ(std::get<TopicDef>((*r)[1]).persistence, Persistence::Disk);
}
```

- [ ] **Step 2: Run to verify they fail** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-ql/build -j14 --target rune_ql_tests` → `use of undeclared identifier 'classifyInput'`. (The `ParseScriptRegression.*` pins compile and are expected to pass already once the file builds; they guard behaviour the console depends on.)

- [ ] **Step 3: Implement.** Append to `include/rune/ql/Parser.h`, inside `namespace rune::ql`, after `parseScript`:

```cpp
/// Console input state of buffered text (rune console's continuation prompt).
enum class InputState : uint8_t { Empty, Incomplete, Complete };
[[nodiscard]] InputState classifyInput(std::string_view src) noexcept;
```
(add `#include <cstdint>` to the includes). Append to `src/Parser.cpp` before the final `} // namespace rune::ql`:

```cpp
// Phase 2b Part D: the console's continuation rule, on the SAME lexer as parse(),
// so comments ('--') and strings ('…') are skipped exactly as the parser skips them.
InputState classifyInput(std::string_view src) noexcept {
    Lexer lex{src};
    Token t = lex.next();
    if (t.kind == TK::Eof) return InputState::Empty;
    TK last = t.kind;
    for (t = lex.next(); t.kind != TK::Eof; t = lex.next()) last = t.kind;
    return last == TK::Semi ? InputState::Complete : InputState::Incomplete;
}
```
(A `BadString` token runs to the end of its line, so `'a;` ends in `BadString` → Incomplete.)

- [ ] **Step 4: Benchmarks.** Append to `rune-ql/benchmark/ParseBench.cpp`:

```cpp
// Phase 2b Part D: a three-statement console script, and the per-line classification.
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

static void BM_ClassifyInput(benchmark::State& state) {
    for (auto _ : state) {
        auto s = rune::ql::classifyInput(kScript3);
        benchmark::DoNotOptimize(s);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ClassifyInput);
```

- [ ] **Step 5: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-ql/build -R 'ClassifyInput|ParseScript|QlCatalogStatements|Parser' --output-on-failure -j14` → PASS; `/Users/vradu/Projects/se-worktrees/rune-ql/build/rune_ql_bench --benchmark_filter='BM_ParseScript3|BM_ClassifyInput'` → record both numbers (reported, not gated; `BM_ClassifyInput` should be well under 1 µs).

- [ ] **Step 6: Commit (rune-ql)**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-ql add include/rune/ql/Parser.h src/Parser.cpp tests/ScriptTest.cpp CMakeLists.txt benchmark/ParseBench.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-ql commit -m "feat(ql): classifyInput for the console continuation prompt; parseScript console pins

Bench: BM_ParseScript3 <n> ns, BM_ClassifyInput <n> ns.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
(Replace the two `<n>` with the measured numbers.)

---

### Task 2: Move the `rune ql` engine into `src/cli/QlEngine.{h,cpp}`; `--validate` / `--expected-version` on `rune ql`

**Replaces** the old "extract `CliCommon` from `Cli.cpp`" task: the shared executor now lives in `CatalogCommands.cpp` (catalog history Task 15), not in `Cli.cpp`.

**Files:**
- Create: `rune-client-cpp/src/cli/QlEngine.h`, `src/cli/QlEngine.cpp`
- Modify: `rune-client-cpp/src/cli/CatalogCommands.cpp` (delete the moved definitions; `cmdQl` builds `LiveOptions` / `PrepareOptions`)
- Modify: `rune-client-cpp/CMakeLists.txt` (`rune_cli_lib` sources: add `src/cli/QlEngine.cpp`)
- Test: `rune-client-cpp/tests/RuneCliCatalogTest.cpp` (append), `rune-broker/libs/node/tests/client/RuneCliBrokerRegressionTest.cpp` (append one case)

**Interfaces:**
- Moves **unchanged** (same bodies, namespace `rune::cli::engine`, internal header — not installed): `Prepared` and its alternatives, `prepare`, `printPlan`, `splitStatements`, `readScript`, `IdSeq`, `requestIdHex`, `errorCodeName`, `brokerFailure`, `catalogReadFailure`, `awaitOwnWrite`, `LiveBroker`, `QlRun`. Every edit the auth phase made to them (security `Prepared` alternatives, credentials in `LiveBroker::open`, the dev-mode banner, names 44–47 in `errorCodeName`) moves with them.
- Changes (the only behavioural additions):
  ```cpp
  namespace rune::cli::engine {
  struct PrepareOptions {
      uint16_t expectedVersion = 0;   // every `alter topic … <ops>` asserts it (0 = no check)
      bool     validate        = false;   // every `topic …` is defined with validate = true
  };
  std::optional<Prepared> prepare(const rune::ql::Statement&, const PrepareOptions&, std::string& why);

  /// What LiveBroker::open needs (was: the `rune ql` ParsedArgs).
  struct LiveOptions {
      std::string endpoint;            // host:port or an aeron: channel
      std::string egress;              // "" = RUNE_EGRESS, else derived for UDP (localEgressFor)
      std::string streamIn, streamOut, dataDir;   // hidden test overrides ("" = default)
      bool        verbose = false;
      // credentials (auth Task 8 — moved with LiveBroker unchanged): user / password file
      std::string user, passwordFile;
  };
  class LiveBroker { public: bool open(const LiveOptions&, std::ostream& err); /* rest unchanged */ };

  enum class OutputStyle : uint8_t { Ql, Console };
  struct QlRun {
      LiveBroker& broker; std::ostream& out; std::ostream& err;
      bool script = false; std::size_t index = 1;
      OutputStyle style = OutputStyle::Ql;
      uint16_t lastErrorCode = 0;      // the broker code of the last failure (0: local / transport)
      // exec / run(...) as today; `fail(msg)` keeps its text; the broker-failure paths set lastErrorCode
  };
  }
  ```
  `PDefine` carries the plan's `validate` (from `PrepareOptions`); `QlRun::run(const PDefine&)` passes it to `defineTopicAndWait` instead of the literal `false`. `prepare` passes `opts.expectedVersion` to `ql::applyAlterPlan(s, opts.expectedVersion)`.
- `rune ql` usage gains `[--validate] [--expected-version N]` (N 0..65535; a bad value → usage error 2: `--expected-version must be a non-negative integer (0..65535)`).

- [ ] **Step 1: Write the failing tests.** Append to `tests/RuneCliCatalogTest.cpp`:

```cpp
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
```
(`printPlan` prints ` validate` after the key for a validating define and `  expected version N` under an alter's op list when N ≠ 0.)

Append to the broker's `RuneCliBrokerRegressionTest.cpp`:

```cpp
TEST(RuneCliBrokerRegression, QlExpectedVersionConflictIsTheBrokersSchemaConflictRegression) {
    OneBroker b("cli_ev");
    std::stringstream o, e;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "topic ev { id int64 } partitions 1 key id;", "--broker", b.endpoint()}, o, e), 0) << e.str();
    std::stringstream o2, e2;
    ASSERT_EQ(rune::cli::run({"ql", "-e", "alter topic ev add column a int32;", "--expected-version", "1",
                              "--broker", b.endpoint()}, o2, e2), 0) << e2.str();
    std::stringstream o3, e3;
    EXPECT_EQ(rune::cli::run({"ql", "-e", "alter topic ev add column b int32;", "--expected-version", "1",
                              "--broker", b.endpoint()}, o3, e3), 1);
    EXPECT_NE(e3.str().find("SchemaConflict"), std::string::npos) << e3.str();
}
```

- [ ] **Step 2: Run to verify they fail** — client: `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R RuneCliCatalog --output-on-failure` → the two new tests fail (`--expected-version` / `--validate` unknown → exit 2 with "unknown option").

- [ ] **Step 3: Move the engine.** Create `src/cli/QlEngine.h` with the declarations above (plus the moved type definitions: the `P*` structs, `Prepared`, `IdSeq`, `LiveBroker`, `QlRun` — `QlRun`'s member functions stay inline in the header or move to `QlEngine.cpp`, the implementer's choice) and `src/cli/QlEngine.cpp` with the moved bodies, inside `namespace rune::cli::engine` and the same `#ifdef RUNE_CLI_LIVE_ENABLED` guards as today. In `CatalogCommands.cpp` replace the moved code with `#include "QlEngine.h"` and `using namespace engine;` inside `namespace rune::cli`. `cmdQl`:
  - parse flags: `parseArgs("ql", args, {"-e", "-f", "--request-id", "--broker", "--expected-version", "--user", "--password-file"}, {"--dry-run", "--verbose", "--validate"}, a, err)` (keep whatever credential flags the auth phase added — do not duplicate them);
  - `PrepareOptions popts; popts.validate = a.has("--validate");` and, when `--expected-version` is present, parse it with `std::from_chars` into 0..65535 or `return usageError("ql", err, "--expected-version must be a non-negative integer (0..65535)");`
  - `prepare(statements[i], popts, why)`;
  - `LiveOptions lo{endpoint, a.value("--cli-egress"), a.value("--cli-stream-in"), a.value("--cli-stream-out"), a.value("--cli-data-dir"), a.has("--verbose"), …credentials as the auth phase reads them…}; broker.open(lo, err)`.
  - `QlRun run{broker, out, err, script, 1};` — `style` defaults to `Ql`, so `rune ql` output is byte-identical.
  In `QlRun`, every `return fail(brokerFailure(r.errorCode, r.detail));` becomes `lastErrorCode = r.errorCode; return fail(brokerFailure(r.errorCode, r.detail));` (a helper `failBroker(code, detail)` is fine).
- [ ] **Step 4: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli' --output-on-failure -j14` → PASS (every pre-existing `RuneCliCatalog*` case unchanged); broker `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'RuneCliBroker' --output-on-failure -j14` → PASS.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli tests/RuneCliCatalogTest.cpp CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "refactor(cli): the rune ql engine moves to QlEngine.{h,cpp} (shared with the console); rune ql --validate / --expected-version

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs/node/tests/client/RuneCliBrokerRegressionTest.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(cli): rune ql --expected-version conflict answers SchemaConflict from a real broker

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (controller):** full `*Regression*` in rune-ql, rune-client-cpp (+ `build_tsan`), broker `build-fast` (+ ASan / TSan).

---

### Task 3: `rune console` — REPL state machine, script runner, dry-run, offline tests

**Files:**
- Create: `rune-client-cpp/include/rune/cli/Console.h`, `src/cli/Console.cpp`
- Modify: `rune-client-cpp/include/rune/cli/Cli.h` (the `run(args, in, out, err)` overload), `src/cli/Cli.cpp` (`console` dispatch, usage)
- Modify: `rune-client-cpp/CMakeLists.txt` (`rune_cli_lib`: `src/cli/Console.cpp`; `rune_cli_offline_tests`: `tests/RuneConsoleOfflineTest.cpp`)
- Create: `rune-client-cpp/tests/RuneConsoleOfflineTest.cpp`

**Interfaces:**
- Consumes: `rune::ql::parseScript`, `rune::ql::classifyInput` (Task 1); `engine::prepare`, `engine::printPlan`, `engine::splitStatements`, `engine::readScript`, `engine::PrepareOptions` (Task 2).
- Produces:
  - Public: `int rune::cli::run(std::vector<std::string> args, std::istream& in, std::ostream& out, std::ostream& err);` — the existing `run(args, out, err)` forwards `std::cin`.
  - `include/rune/cli/Console.h` (namespace `rune::cli::console`) declares `cmdConsole` (and, in Task 4, `runLiveOnFdForTest`); everything else below is file-local to `Console.cpp`:
    ```cpp
    int cmdConsole(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err);

    struct ConsoleOptions {
        std::string scriptPath;               // -f; "" = interactive
        bool dryRun = false;
        engine::PrepareOptions prep;          // --validate, --expected-version
        std::optional<catalog::RequestId> requestIdBase;   // --request-id (-f only)
        engine::LiveOptions live;             // --broker / --cli-egress / credentials / hidden flags
    };
    /// A line of input; false at EOF.  StreamLineSource: std::getline on an istream.
    /// Task 4 adds PollingFdLineSource (TTY; pumps an idle callback between polls).
    struct LineSource { virtual ~LineSource() = default; virtual bool next(std::string& line) = 0; };
    /// The executor concept every runner is templated on:
    ///   int execute(const engine::Prepared&, const std::string& text, std::size_t index);
    ///   0 = ok; otherwise the code a script stops with (the broker code, or 1).
    ///   [[nodiscard]] bool sessionEnded() const;   // SessionRevoked: the interactive loop leaves
    ```
- Exit codes and output: see Global Constraints and DD2 / DD12.

- [ ] **Step 1: Write the failing tests.** Create `rune-client-cpp/tests/RuneConsoleOfflineTest.cpp`:

```cpp
// tests/RuneConsoleOfflineTest.cpp — Phase 2b Part D: `rune console` offline (--dry-run, no broker).
// Drives rune::cli::run(args, in, out, err) with an injected istream: no TTY needed.
// Also carries the migrated `rune schema apply|alter --dry-run` assertions (Task 5 table).
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "rune/cli/Cli.h"

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
```

CMake: add `tests/RuneConsoleOfflineTest.cpp` to `rune_cli_offline_tests`' sources.

- [ ] **Step 2: Run to verify they fail** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -j14 --target rune_cli_offline_tests` → `no matching function for call to 'run'` (no 4-argument overload yet).

- [ ] **Step 3: Public overload.** In `include/rune/cli/Cli.h` add `#include <istream>` and, after the existing declaration:

```cpp
/// As run(args, out, err), reading the console's interactive / `-f -` input from @p in
/// instead of std::cin (lets `rune console` be tested without a TTY).
int run(std::vector<std::string> args, std::istream& in, std::ostream& out, std::ostream& err);
```

- [ ] **Step 4: Create `include/rune/cli/Console.h`** declaring `cmdConsole` (namespace `rune::cli::console`; includes `<istream>`, `<ostream>`, `<string>`, `<vector>` only). `ConsoleOptions` and `LineSource` are defined at the top of `Console.cpp`'s anonymous namespace (it includes `QlEngine.h`).

- [ ] **Step 5: Create `src/cli/Console.cpp`**

```cpp
// src/cli/Console.cpp — Phase 2b Part D: `rune console`.
//
//   rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run]
//                [--validate] [--expected-version N] [--request-id <32 hex>] [--verbose]
//                [--user <u>] [--password-file <p>]
//
// Interactive: "rune> " / "   ...> " until the buffered text ends with a ';' outside
// comments and strings (ql::classifyInput); each completed input is parsed (parseScript),
// prepared (engine::prepare) and run in order; a failure skips the rest of THAT input and
// never ends the session (except SessionRevoked, DD11).  exit / quit / EOF leave (0).
// Script (-f): every statement parsed and prepared first (DD6); then run in order; the first
// failure stops it (exit = the broker code, or 1).  Control plane only; single-threaded.
#include "rune/cli/Console.h"
#include "rune/cli/CatalogCommands.h"   // parseRequestId
#include "QlEngine.h"

#include "rune/ql/Parser.h"

#include <charconv>
#include <istream>
#include <ostream>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace rune::cli::console {
namespace {

constexpr std::string_view kPrompt             = "rune> ";
constexpr std::string_view kContinuationPrompt = "   ...> ";

void printConsoleUsage(std::ostream& err) {
    err << "Usage: rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run]\n"
           "                    [--validate] [--expected-version N] [--request-id <32 hex>] [--verbose]\n"
           "                    [--user <u>] [--password-file <p>]\n";
}

int usage(std::ostream& err, const std::string& msg) {
    err << "rune console: " << msg << "\n";
    printConsoleUsage(err);
    return 2;
}

/// Strict parse: a typo never falls through to an interactive session blocked on stdin.
/// Returns 0 when ok, else the usage exit code (2).
int parseConsoleOptions(const std::vector<std::string>& args, ConsoleOptions& o, std::ostream& err) {
    static constexpr std::string_view kValue[] = {"-f", "--broker", "--cli-egress", "--expected-version", "--request-id",
                                                  "--user", "--password-file",
                                                  "--cli-stream-in", "--cli-stream-out", "--cli-data-dir"};   // last 3 hidden
    static constexpr std::string_view kBool[]  = {"--dry-run", "--validate", "--verbose"};
    auto in = [](std::string_view a, auto& set) {
        for (const auto s : set) if (s == a) return true;
        return false;
    };
    std::string requestId, expected;
    for (std::size_t i = 1; i < args.size(); ++i) {
        const std::string& a = args[i];
        if (in(a, kBool)) {
            if (a == "--dry-run") o.dryRun = true;
            else if (a == "--validate") o.prep.validate = true;
            else o.live.verbose = true;
            continue;
        }
        if (!in(a, kValue)) {
            return usage(err, (a.rfind('-', 0) == 0 ? "unknown option '" : "unexpected argument '") + a + "'");
        }
        if (i + 1 >= args.size()) return usage(err, a + " needs a value");
        const std::string& v = args[++i];
        if (a == "-f") o.scriptPath = v;
        else if (a == "--broker") o.live.endpoint = v;
        else if (a == "--cli-egress") o.live.egress = v;
        else if (a == "--expected-version") expected = v;
        else if (a == "--request-id") requestId = v;
        else if (a == "--user") o.live.user = v;
        else if (a == "--password-file") o.live.passwordFile = v;
        else if (a == "--cli-stream-in") o.live.streamIn = v;
        else if (a == "--cli-stream-out") o.live.streamOut = v;
        else o.live.dataDir = v;
    }
    if (!expected.empty()) {
        int value = -1;
        const auto r = std::from_chars(expected.data(), expected.data() + expected.size(), value);
        if (r.ec != std::errc{} || r.ptr != expected.data() + expected.size() || value < 0 || value > 0xFFFF)
            return usage(err, "--expected-version must be a non-negative integer (0..65535)");
        o.prep.expectedVersion = static_cast<uint16_t>(value);
    }
    if (!requestId.empty()) {
        if (o.scriptPath.empty()) return usage(err, "--request-id needs -f (statement N uses lo + N - 1)");
        o.requestIdBase = parseRequestId(requestId);
        if (!o.requestIdBase || o.requestIdBase->none()) return usage(err, "--request-id must be 32 hex digits, not all zero");
    }
    return 0;
}

// ── Prepared batches ─────────────────────────────────────────────────────────

struct PreparedStatement { engine::Prepared p; std::string text; };

/// Parse + prepare every statement of `src`.  false (message on err, prefixed by `origin`) when any
/// statement fails: nothing of `src` runs (DD6).
bool prepareAll(std::string_view origin, std::string_view src, const engine::PrepareOptions& opts,
                std::vector<PreparedStatement>& out, std::ostream& err) {
    auto parsed = rune::ql::parseScript(src);
    auto prefix = [&](std::size_t n) {
        if (!origin.empty()) err << origin << ":statement " << n << ": ";
    };
    if (!parsed) {
        const auto& e = parsed.error();
        prefix(e.statementIndex);
        err << e.error.line << ":" << e.error.col << ": " << e.error.message << "\n";
        return false;
    }
    const std::vector<std::string> texts = engine::splitStatements(src);
    for (std::size_t i = 0; i < parsed->size(); ++i) {
        std::string why;
        auto p = engine::prepare((*parsed)[i], opts, why);
        if (!p) {
            prefix(i + 1);
            err << why << "\n";
            return false;
        }
        out.push_back({std::move(*p), i < texts.size() ? texts[i] : std::string()});
    }
    return true;
}

template <class Exec>
int runBatch(Exec& exec, const std::vector<PreparedStatement>& batch, std::size_t firstIndex) {
    for (std::size_t i = 0; i < batch.size(); ++i) {
        if (const int rc = exec.execute(batch[i].p, batch[i].text, firstIndex + i); rc != 0) return rc;
    }
    return 0;
}

// ── Script mode (-f) ─────────────────────────────────────────────────────────

template <class Exec>
int runScript(Exec& exec, const ConsoleOptions& o, std::string_view src, std::ostream& err) {
    std::vector<PreparedStatement> batch;
    if (!prepareAll(o.scriptPath, src, o.prep, batch, err)) return 1;
    return runBatch(exec, batch, 1);
}

// ── Interactive mode: REPL state machine (CLAUDE.md #3) ──────────────────────
//
//   AwaitingStatement ──line, Empty──────> AwaitingStatement
//   AwaitingStatement ──exit / quit──────> leave (0)
//   any ──line, Incomplete───────────────> ContinuingStatement{text so far}
//   any ──line, Complete─────────────────> prepare + run the batch ──> AwaitingStatement
//   any ──EOF──> leave (0); a ContinuingStatement is reported, never run
//   any ──executor.sessionEnded()────────> leave (46, DD11)

struct AwaitingStatement {};
struct ContinuingStatement { std::string text; };
using ReplState = std::variant<AwaitingStatement, ContinuingStatement>;

std::string_view trimmed(std::string_view s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string_view::npos) return {};
    return s.substr(b, s.find_last_not_of(" \t\r\n") - b + 1);
}

bool isExitCommand(std::string_view line) {
    const auto t = trimmed(line);
    return t == "exit" || t == "quit" || t == "exit;" || t == "quit;";
}

template <class Exec>
int runInteractive(Exec& exec, const ConsoleOptions& o, LineSource& in, std::ostream& out, std::ostream& err) {
    ReplState state = AwaitingStatement{};
    std::string line;
    std::size_t index = 1;
    for (;;) {
        out << (std::holds_alternative<AwaitingStatement>(state) ? kPrompt : kContinuationPrompt) << std::flush;
        if (!in.next(line)) {
            out << "\n";
            if (std::holds_alternative<ContinuingStatement>(state))
                err << "error: input ended inside a statement (missing ';'); it was not executed\n";
            return 0;
        }
        std::string text;
        if (auto* c = std::get_if<ContinuingStatement>(&state)) text = std::move(c->text);
        else if (isExitCommand(line)) return 0;
        text += line;
        text += '\n';
        switch (rune::ql::classifyInput(text)) {
            case rune::ql::InputState::Empty: state = AwaitingStatement{}; break;
            case rune::ql::InputState::Incomplete: state = ContinuingStatement{std::move(text)}; break;
            case rune::ql::InputState::Complete: {
                state = AwaitingStatement{};
                std::vector<PreparedStatement> batch;
                if (prepareAll({}, text, o.prep, batch, err)) {
                    (void)runBatch(exec, batch, index);   // DD7: a failure skips the rest of this input
                    index += batch.size();
                }
                if (exec.sessionEnded()) return 46;
                break;
            }
        }
    }
}

// ── Sources and executors ────────────────────────────────────────────────────

struct StreamLineSource final : LineSource {
    std::istream& in;
    explicit StreamLineSource(std::istream& s) : in(s) {}
    bool next(std::string& line) override {
        if (!std::getline(in, line)) return false;
        if (!line.empty() && line.back() == '\r') line.pop_back();   // CRLF terminals / files
        return true;
    }
};

struct DryRunExecutor {
    std::ostream& out;
    std::optional<catalog::RequestId> base;
    int execute(const engine::Prepared& p, const std::string&, std::size_t index) {
        std::optional<catalog::RequestId> rid;
        if (base) rid = catalog::RequestId{base->hi, base->lo + (index - 1)};
        engine::printPlan(p, rid, out);
        return 0;
    }
    [[nodiscard]] bool sessionEnded() const noexcept { return false; }
};

// Task 4 replaces this with the connected session.
int runLive(const ConsoleOptions&, std::istream&, const std::string&, std::ostream&, std::ostream& err) {
    err << "rune console: live mode requires RUNE_CLI_LIVE_ENABLED and a real Aeron build. Use --dry-run.\n";
    return 1;
}

} // namespace

int cmdConsole(const std::vector<std::string>& args, std::istream& in, std::ostream& out, std::ostream& err) {
    ConsoleOptions o;
    if (const int rc = parseConsoleOptions(args, o, err); rc != 0) return rc;
    std::string script;
    if (!o.scriptPath.empty() && !engine::readScript(o.scriptPath, script, err)) return 1;
    if (o.dryRun) {
        DryRunExecutor exec{out, o.requestIdBase};
        if (!o.scriptPath.empty()) return runScript(exec, o, script, err);
        StreamLineSource src(in);
        return runInteractive(exec, o, src, out, err);
    }
    return runLive(o, in, script, out, err);
}

} // namespace rune::cli::console
```

(`engine::readScript(path, …)` reads `std::cin` for `-`; change its signature to `readScript(const std::string& path, std::istream& stdinStream, std::string& text, std::ostream& err)` in `QlEngine` so the console passes `in` — `cmdQl` passes `std::cin`. `parseRequestId` is the existing public helper from `CatalogCommands.h`.)

- [ ] **Step 6: Wire `run` + usage in `src/cli/Cli.cpp`.** Add `#include "rune/cli/Console.h"`. `printUsage` gains, first:

```cpp
        << "  rune console [--broker <ep>] [-f <script.rql|->] [--dry-run] [--validate] [--expected-version N]\n"
        << "      Run Rune QL statements interactively (prompt `rune> `) or from a script (-f; stops at\n"
        << "      the first error). --dry-run prints each statement's plan (no broker).\n"
```
Replace the `rune::cli::run` definition with the two overloads:

```cpp
int run(std::vector<std::string> args, std::ostream& out, std::ostream& err) {
    return run(std::move(args), std::cin, out, err);
}

int run(std::vector<std::string> args, std::istream& in, std::ostream& out, std::ostream& err) {
    if (args.empty()) { printUsage(err); return 1; }
    const std::string& cmd = args[0];
    if (cmd == "console") return console::cmdConsole(args, in, out, err);
    if (cmd == "schema") return cmdSchema(args, out, err);
    if (cmd == "ql") return cmdQl(std::vector<std::string>(args.begin() + 1, args.end()), out, err);
    if (cmd == "catalog") return cmdCatalog(std::vector<std::string>(args.begin() + 1, args.end()), out, err);
    // keep every other command the auth phase added (e.g. "security") exactly as it dispatches today
    err << "rune: unknown command '" << cmd << "'\n";
    printUsage(err);
    return 1;
}
```
CMake: `rune_cli_lib` gains `src/cli/Console.cpp`.

- [ ] **Step 7: Run** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|Console' --output-on-failure -j14` → every `ConsoleOffline*` PASSES with all pre-existing CLI tests.
- [ ] **Step 8: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli include/rune/cli/Cli.h tests/RuneConsoleOfflineTest.cpp CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(cli): rune console — REPL state machine, -f scripts, --dry-run on the shared rune ql engine

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Live console — one session, `ok` lines, broker errors by name, idle pumping, login

**Files:**
- Modify: `rune-client-cpp/src/cli/Console.cpp` (`runLive`, `LiveExecutor`, `PollingFdLineSource`, the password prompt), `include/rune/cli/Console.h` (`runLiveOnFdForTest`)
- Modify: `rune-client-cpp/src/cli/QlEngine.{h,cpp}` (`OutputStyle::Console` lines in `QlRun`)
- Create: `rune-broker/libs/node/tests/client/RuneConsoleBrokerRegressionTest.cpp`; modify `rune-broker/libs/node/CMakeLists.txt` (add it to `rune_client_broker_tests`)

**Interfaces:**
- Consumes: `engine::LiveBroker`, `engine::QlRun` (Task 2); `Client::doWork()`; `Client::authStatus()` (auth Task 7); the auth-enabled `OneBroker` option the auth plan adds for `RuneCliSecurityBrokerRegression` (find its constructor / factory in `libs/node/tests/client/OneBroker.h`; if the auth phase named it differently, use that name — do not add a second one).
- Produces:
  ```cpp
  namespace rune::cli::console {
  /// TTY input: poll(2) on `fd` every 50 ms; between polls calls idle(ctx) (pumps the Invoker client, DD9).
  /// Owns its read buffer (fixed 64 KiB; a longer line is an error line "input line too long").
  struct PollingFdLineSource final : LineSource {
      PollingFdLineSource(int fd, void (*idle)(void*) noexcept, void* ctx);
      bool next(std::string& line) override;
      uint64_t idlePumps = 0;   // test seam
  };
  /// Test seam: run the live console reading `fd` (a pipe in tests) through PollingFdLineSource.
  int runLiveOnFdForTest(const std::vector<std::string>& args, int fd, std::ostream& out, std::ostream& err,
                         uint64_t* idlePumps);
  }
  ```
  `LiveExecutor::execute` runs `QlRun::exec(prepared, text, rid)` with `style = OutputStyle::Console`; returns 0, else `run.lastErrorCode != 0 ? run.lastErrorCode : 1`; `sessionEnded()` is true once a failure's code is `SessionRevoked (46)` (it also prints `error: SessionRevoked (46): the session ended`).

- [ ] **Step 1: Write the failing tests** — `RuneConsoleBrokerRegressionTest.cpp` (broker repo, real `OneBroker`, real CLI; catalog ruling F7):

```cpp
// Phase 2b Part D: the real `rune console` against a real in-process broker.
#include "client/OneBroker.h"

#include <rune/cli/Cli.h>
#include <rune/cli/Console.h>   // runLiveOnFdForTest

#include <gtest/gtest.h>

#include <chrono>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
using rune::node::test::OneBroker;

namespace {
struct Run { int rc; std::string out, err; };
Run consoleLive(OneBroker& b, std::vector<std::string> extra, const std::string& input) {
    std::vector<std::string> args{"console", "--broker", b.endpoint()};
    args.insert(args.end(), extra.begin(), extra.end());
    std::istringstream in(input);
    std::ostringstream out, err;
    const int rc = rune::cli::run(args, in, out, err);
    return {rc, out.str(), err.str()};
}
} // namespace

TEST(RuneConsoleBrokerRegression, InteractiveDefinesAltersAndReadsInOneSessionRegression) {
    OneBroker b("console_live");
    const auto r = consoleLive(b, {},
        "topic player { id int64, name string } partitions 8 key id;\n"
        "alter topic player add column level int32;\n"
        "transient topic ticks { id int64 } partitions 1 key id;\n"
        "show history topic player;\n");
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic player v1 (8 partitions)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic player v2"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok transient topic ticks v1 (1 partition)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("CreateTopic"), std::string::npos) << "show history ran in the same session: " << r.out;
    const auto ev = b.catalogEvents();
    ASSERT_GE(ev.size(), 3u);
    EXPECT_EQ(ev[0].event.statement, "topic player { id int64, name string } partitions 8 key id;")
        << "each DDL records its own statement text";
}

TEST(RuneConsoleBrokerRegression, OneSessionForEveryStatementRegression) {
    OneBroker b("console_one");
    const uint64_t before = b.node().testConnectsAccepted();   // a BrokerNode test probe; add it if absent (gateway accepted-connect counter)
    const auto r = consoleLive(b, {}, "topic a partitions 1;\ntopic b partitions 1;\ntopic c partitions 1;\n");
    ASSERT_EQ(r.rc, 0) << r.err;
    EXPECT_EQ(b.node().testConnectsAccepted() - before, 1u) << "one connected client for the whole session";
}

TEST(RuneConsoleBrokerRegression, BrokerErrorKeepsTheSessionRegression) {
    OneBroker b("console_err");
    const auto r = consoleLive(b, {}, "topic t partitions 1;\ntopic t partitions 1;\ntopic u partitions 1;\n");
    EXPECT_EQ(r.rc, 0);
    EXPECT_NE(r.err.find("TopicExists (25)"), std::string::npos) << r.err;
    EXPECT_NE(r.out.find("ok topic u (1 partition)"), std::string::npos) << "the session continues: " << r.out;
}

TEST(RuneConsoleBrokerRegression, ScriptStopsAtTheFirstBrokerErrorWithItsCodeRegression) {
    OneBroker b("console_script");
    const std::string f = (std::filesystem::temp_directory_path() / ("console_" + std::to_string(::getpid()) + ".rql")).string();
    { std::ofstream o(f); o << "topic ev { id int64, a int32 } partitions 1 key id;\n"
                               "alter topic ev set compatibility forward;\n"
                               "alter topic ev drop column a;\n"          // a DROP under FORWARD: SchemaIncompatible
                               "topic never partitions 1;\n"; }
    const auto r = consoleLive(b, {"-f", f}, "");
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::SchemaIncompatible)) << r.err;
    EXPECT_NE(r.err.find(f + ":statement 3: "), std::string::npos) << r.err;
    for (const auto& e : b.catalogEvents()) EXPECT_NE(e.event.objectName, "never") << "nothing after the failure is issued";
}

TEST(RuneConsoleBrokerRegression, ValidateAndExpectedVersionReachTheBrokerRegression) {
    OneBroker b("console_flags");
    const std::string f1 = "/tmp/console_flags_" + std::to_string(::getpid()) + "_1.rql";
    { std::ofstream o(f1); o << "topic fv { id int64 } partitions 1 key id;\nalter topic fv add column a int32;\n"; }
    ASSERT_EQ(consoleLive(b, {"-f", f1, "--validate", "--expected-version", "1"}, "").rc, 0);
    const std::string f2 = "/tmp/console_flags_" + std::to_string(::getpid()) + "_2.rql";
    { std::ofstream o(f2); o << "alter topic fv add column b int32;\n"; }
    const auto r = consoleLive(b, {"-f", f2, "--expected-version", "1"}, "");
    EXPECT_EQ(r.rc, static_cast<int>(rune::protocol::client::ErrorCode::SchemaConflict)) << r.err;
    // validate = true was recorded on the topic (catalog state)
    // (assert through the catalog: the CreateTopic event's op carries validate — read it with b.catalogEvents())
}

TEST(RuneConsoleBrokerRegression, IdlePromptKeepsTheSessionAliveRegression) {
    // Review Focus #6 / DD9: an interactive console idle past the session's keepalive window stays connected.
    OneBroker b("console_idle");
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    std::ostringstream out, err;
    uint64_t pumps = 0;
    int rc = -1;
    std::thread t([&] {
        rc = rune::cli::console::runLiveOnFdForTest({"console", "--broker", b.endpoint()}, fds[0], out, err, &pumps);
    });
    const std::string s1 = "topic idle1 partitions 1;\n";
    ASSERT_EQ(::write(fds[1], s1.data(), s1.size()), static_cast<ssize_t>(s1.size()));
    std::this_thread::sleep_for(std::chrono::nanoseconds(rune::client::session::SessionConfig{}.responseTimeoutNs) + 2s);
    const std::string s2 = "topic idle2 partitions 1;\n";
    ASSERT_EQ(::write(fds[1], s2.data(), s2.size()), static_cast<ssize_t>(s2.size()));
    ::close(fds[1]);   // EOF: the console leaves
    t.join();
    ::close(fds[0]);
    EXPECT_EQ(rc, 0) << err.str();
    EXPECT_NE(out.str().find("ok topic idle2"), std::string::npos) << err.str();
    EXPECT_GT(pumps, 10u) << "the client was pumped while the prompt waited";
}

TEST(RuneConsoleBrokerRegression, LoginWithAPasswordFileAndNotAuthorizedKeepsTheSessionRegression) {
    // Auth (D18 op table): CreateTopic needs CREATE TOPIC.  A user with CREATE on PREFIX 'ok_' only.
    // Build the broker with the auth plan's auth-enabled OneBroker option, bootstrap + create the user the way
    // RuneCliSecurityBrokerRegression does (reuse its helpers), then:
    //   rune console --broker … --user u --password-file <file> with input
    //     "topic ok_a partitions 1;\ntopic other partitions 1;\ntopic ok_b partitions 1;\n"
    //   → rc 0; "ok topic ok_a" and "ok topic ok_b" on stdout; "NotAuthorized (45)" on stderr for `other`.
    //   Without --password-file and with a non-TTY stdin → rc 2 ("--password-file") and no connect.
}

TEST(RuneConsoleBrokerRegression, SessionRevokedEndsTheConsoleRegression) {
    // Auth D26 / DD11: with the auth-enabled broker, a console logged in as u; the admin runs
    // `ALTER USER u DISABLE;` through a second client between two input lines (pipe source as in the idle test);
    // the console's next statement fails SessionRevoked → rc 46, stderr "the session ended", no further prompt.
}
```

(The last two bodies are specified step by step; write them in full with the auth phase's helpers. If `BrokerNode` has no accepted-connect test probe, add `testConnectsAccepted()` reading the gateway's existing connect counter — a test-only accessor on the control thread via `onControlThreadForTest`, the same way `onAdminThreadForTest` is used in `OneBroker.h`.)

Add `tests/client/RuneConsoleBrokerRegressionTest.cpp` to `rune_client_broker_tests` in `libs/node/CMakeLists.txt`.

- [ ] **Step 2: Run to verify they fail** — `cmake --build /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -j14 --target rune_client_broker_tests && ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R RuneConsoleBroker --output-on-failure` → fail (`runLive` stub: "live mode requires …").

- [ ] **Step 3: Console success lines in `QlRun`** (`OutputStyle::Console`, DD2). In each DDL `run(...)`, after the success branch, print:

```cpp
if (style == OutputStyle::Console) {
    // run(const PDefine& s, …):
    out << "ok " << (d.persistence == ql::Persistence::Transient ? "transient topic " : "topic ") << d.name;
    if (!d.descriptorBlob.empty()) out << " v" << r.version;
    out << " (" << d.partitions << (d.partitions == 1 ? " partition)" : " partitions)") << "\n";
    // run(const PAlter&):       out << "ok topic " << d.name << " v" << r.newVersion << "\n";
    // run(const PAlterConfig&): out << "ok topic " << s.plan.name << " config v" << r.newVersion << "\n";
    // run(const PDropTopic&):   out << "ok dropped topic " << s.name << "\n";
    // run(const PDropView&):    out << "ok dropped view " << s.name << "\n";
    // any other DDL kind (the auth phase's security statements): out << "ok " << <the Ql line>;
} else {
    /* the existing rune ql line, unchanged */
}
```

- [ ] **Step 4: `runLive`, the executor, the sources, the prompt** (in `Console.cpp`, `#ifdef RUNE_CLI_LIVE_ENABLED`; the stub stays in the `#else`):

```cpp
struct LiveExecutor {
    engine::LiveBroker& broker;
    engine::QlRun       run;
    std::optional<catalog::RequestId> base;
    bool ended = false;
    int execute(const engine::Prepared& p, const std::string& text, std::size_t index) {
        std::optional<catalog::RequestId> rid;
        if (base) rid = catalog::RequestId{base->hi, base->lo + (index - 1)};
        run.index = index;
        run.lastErrorCode = 0;
        if (run.exec(p, text, rid)) return 0;
        if (run.lastErrorCode == static_cast<uint16_t>(rune::protocol::client::ErrorCode::SessionRevoked)) {
            ended = true;
            run.err << "error: SessionRevoked (46): the session ended\n";
        }
        return run.lastErrorCode != 0 ? run.lastErrorCode : 1;
    }
    [[nodiscard]] bool sessionEnded() const noexcept { return ended; }
};

void pumpClient(void* ctx) noexcept {
    try { (void)static_cast<engine::LiveBroker*>(ctx)->client().doWork(); } catch (...) {}
}

bool PollingFdLineSource::next(std::string& line) {
    for (;;) {
        if (const auto nl = std::find(buf_.begin(), buf_.begin() + len_, '\n'); nl != buf_.begin() + len_) {
            line.assign(buf_.begin(), nl);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            const std::size_t used = static_cast<std::size_t>(nl - buf_.begin()) + 1;
            std::memmove(buf_.data(), buf_.data() + used, len_ - used);
            len_ -= used;
            return true;
        }
        if (len_ == buf_.size()) { len_ = 0; line = "-- input line too long (64 KiB)"; return true; }
        pollfd p{fd_, POLLIN, 0};
        const int n = ::poll(&p, 1, 50);
        if (n == 0) { ++idlePumps; idle_(ctx_); continue; }
        if (n < 0) { if (errno == EINTR) continue; return false; }
        const ssize_t got = ::read(fd_, buf_.data() + len_, buf_.size() - len_);
        if (got <= 0) {
            if (len_ == 0) return false;
            line.assign(buf_.begin(), buf_.begin() + len_);   // a last line without '\n'
            len_ = 0;
            return true;
        }
        len_ += static_cast<std::size_t>(got);
    }
}
// members: int fd_; void (*idle_)(void*) noexcept; void* ctx_; std::array<char, 64 * 1024> buf_{}; std::size_t len_ = 0;
```

`runLive(o, in, script, out, err)`:
1. Resolve `o.live.endpoint` (`--broker`, else `RUNE_BROKER`; none → usage 2 "--broker host:port is required (or --dry-run)").
2. Credentials (DD10): when `o.live.user` (or `RUNE_USER`) is set and no password file is given: if `o.scriptPath.empty() && isatty(STDIN_FILENO)` → read the password with echo off (`tcgetattr` / `tcsetattr` clearing `ECHO`, restored on every path), store it in a local `std::string`, pass it to the client config through `LiveOptions` (add `std::string password;` to `LiveOptions`; `LiveBroker::open` sets `ClientConfig::password`), then `explicit_bzero` it after `open` returns; otherwise usage 2 "--password-file is required with --user (passwords are never read from argv or the environment)".
3. `engine::LiveBroker broker; if (!broker.open(o.live, err)) return 1;` (the banner and the `PasswordChangeRequired` warning: `if (broker.client().authStatus().kind == AuthStatus::Kind::PasswordChangeRequired) err << "WARNING: password change required — run ALTER USER " << user << " SET PASSWORD '…';\n";`).
4. `LiveExecutor exec{broker, engine::QlRun{broker, out, err, /*script*/ !o.scriptPath.empty(), 1, engine::OutputStyle::Console}, o.requestIdBase};`
5. `-f` → `runScript(exec, o, script, err)`; interactive → if the caller's `in` is `std::cin` and `isatty(STDIN_FILENO)`: `PollingFdLineSource src(STDIN_FILENO, &pumpClient, &broker)`; else `StreamLineSource src(in)`; `runInteractive(exec, o, src, out, err)`.
6. `broker.flushVerbose(err)`; return.

`runLiveOnFdForTest(args, fd, out, err, idlePumps)` parses options exactly as `cmdConsole` and runs steps 3–5 with `PollingFdLineSource(fd, &pumpClient, &broker)`, storing `src.idlePumps` into `*idlePumps`; it is declared in `include/rune/cli/Console.h` (already on `rune_cli_lib`'s public include path, which `rune_client_broker_tests` links).

- [ ] **Step 5: Run** — client `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R 'RuneCli|Console' --output-on-failure -j14` → PASS; broker `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-broker/build-fast -R 'RuneConsoleBroker|RuneCliBroker' --output-on-failure -j14` → PASS.
- [ ] **Step 6: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli include/rune/cli CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(cli): live rune console — one session, ok lines, broker error codes, idle pumping, login (password prompt, SessionRevoked ends it)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs/node/tests/client/RuneConsoleBrokerRegressionTest.cpp libs/node/CMakeLists.txt libs/node/include libs/node/src
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(cli): the real rune console against real brokers (one session, errors keep it, -f codes, idle, auth)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Remove `rune schema apply|alter`; finish the test migration

**Files:**
- Modify: `rune-client-cpp/src/cli/Cli.cpp` (delete `reportAlterResult`, `cmdSchemaApply`, `cmdSchemaAlter`, `printAlterPlanText` / `alterOpKindName` / `primTypeNameForCli` if nothing else uses them; dispatch; usage)
- Modify: `rune-client-cpp/tests/RuneCliOfflineTest.cpp` (rewrite), `tests/RuneCliLiveTest.cpp` (delete migrated tests, header comment)
- Modify: `rune-broker/libs/node/tests/client/RuneConsoleBrokerRegressionTest.cpp` (append the live migrations)

**Migration map** (old test → the test that carries its assertions):

| Removed test | Carried by |
|---|---|
| `RuneCliOffline.ApplyDryRunPrintsDescriptor`, `RuneCliOfflineRegression.ApplyDryRunPrintsDescriptor`, `RuneCliOffline.ApplyDryRunStillDefinesTopicFile` | `ConsoleOfflineRegression.DryRunScriptPrintsDescriptor` |
| `RuneCliOffline.ApplyReportsParseErrorWithLocation`, `RuneCliOfflineRegression.ApplyReportsParseErrorWithLocationPrefix` | `ConsoleOfflineRegression.ScriptParseErrorHasFileLineColPrefix` |
| `RuneCliOffline.AlterDryRunPrintsParsedOps`, `RuneCliOffline.ApplyDryRunRoutesAlterFileToAlterPath`, `RuneCliOfflineRegression.ApplyDispatchesOnParseVariant` | `ConsoleOfflineRegression.DryRunDispatchesEachStatementKind` |
| `RuneCliOfflineRegression.AlterDryRunPrintsAllOpKinds` | `ConsoleOfflineRegression.DryRunPrintsAllAlterOpKinds` |
| `RuneCliOffline.AlterRejects{NonNumeric,OutOfRange,Negative}ExpectedVersion`, `RuneCliOfflineRegression.ExpectedVersionMalformedIsCleanErrorNotCrash` | `ConsoleOfflineRegression.ExpectedVersionMalformedIsCleanErrorNotCrash` |
| `RuneCliOffline.AlterRejectsTopicStatement`, `RuneCliOfflineRegression.AlterRejectsTopicStatement`, `RuneCliOfflineRegression.SchemaApplyRefusesAMultiStatementFileRegression`, `…SchemaApplyRefusesStatementsItWouldDrop`, `…SchemaAlterRefusesNonOpsStatementsRegression` (they guarded `schema apply|alter` itself) | `RuneCliOfflineRegression.SchemaApplyAndAlterAreRemovedWithConsolePointer` |
| `CliLive.SchemaApplyLive`, `CliLiveRegression.ApplyLive_TopicNameInOutput` | `RuneConsoleBrokerRegression.InteractiveDefinesAltersAndReadsInOneSessionRegression` |
| `CliLive.SchemaApplyTopicExists` | `RuneConsoleBrokerRegression.BrokerErrorKeepsTheSessionRegression` (DD8: now an error) |
| `CliLive.AlterTopicLive`, `CliLiveRegression.AlterLive_NewVersionInOutput`, `CliLive.ApplyAlterFileLive` | `RuneConsoleBrokerRegression.ScriptDefinesThenAltersWithExpectedVersionRegression` (new, below) |
| `CliLive.AlterTopicSchemaIncompatible` | `RuneConsoleBrokerRegression.ScriptStopsAtTheFirstBrokerErrorWithItsCodeRegression` |
| `CliLive.AlterTopicSchemaConflict` | `RuneConsoleBrokerRegression.ValidateAndExpectedVersionReachTheBrokerRegression` |
| `CliLiveRegression.ApplyTransientTopicSendsTransientPersistence` | `RuneConsoleBrokerRegression.InteractiveDefinesAltersAndReadsInOneSessionRegression` (the `ticks` line) |

`CliLive.SchemaGet*`, `CliLive.SchemaList*`, `CliLiveRegression.GetLive_*`, `ListLive_*`, `QlDefinesATopic…` stay.

- [ ] **Step 1: Write the failing tests.** Replace `tests/RuneCliOfflineTest.cpp`:

```cpp
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
```

Append to `RuneConsoleBrokerRegressionTest.cpp`:

```cpp
TEST(RuneConsoleBrokerRegression, ScriptDefinesThenAltersWithExpectedVersionRegression) {
    OneBroker b("console_evo");
    const std::string f = "/tmp/console_evo_" + std::to_string(::getpid()) + ".rql";
    { std::ofstream o(f); o << "topic characters { id int64, userId int64 } partitions 4 key userId;\n"
                               "alter topic characters add column nickname string;\n"; }
    const auto r = consoleLive(b, {"-f", f, "--expected-version", "1"}, "");
    ASSERT_EQ(r.rc, 0) << r.err;
    EXPECT_NE(r.out.find("ok topic characters v1 (4 partitions)"), std::string::npos) << r.out;
    EXPECT_NE(r.out.find("ok topic characters v2"), std::string::npos) << "the new version is printed: " << r.out;
}
```

- [ ] **Step 2: Run to verify they fail** — `ctest --test-dir /Users/vradu/Projects/se-worktrees/rune-client-cpp/build -R RuneCliOffline --output-on-failure` → the removed-subcommand test fails (`schema apply` still runs).
- [ ] **Step 3: Implement.** In `cmdSchema`: `if (sub == "apply" || sub == "alter") { err << "rune schema " << sub << " was removed: use `rune console -f <file.rql>` (or `rune ql -f`)\n"; return 2; }`; delete `cmdSchemaApply`, `cmdSchemaAlter`, `reportAlterResult` and any helper only they used; `printUsage` loses the two `schema apply|alter` entries; the schema subcommand error lists `get|list|generate`. Delete the migrated `CliLive*` tests from `RuneCliLiveTest.cpp` and fix its header comment (the gateway now serves `get`/`list`/`generate` and the `ql` cases only).
- [ ] **Step 4: Run** — client `ctest … -R 'RuneCli|Console|CliLive' --output-on-failure -j14`; broker `ctest … -R 'RuneConsoleBroker|RuneCliBroker' --output-on-failure -j14` → PASS. `grep -rn "schema apply\|schema alter" /Users/vradu/Projects/se-worktrees/rune-client-cpp/src /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests --include='*.cpp'` → only the removal message and its regression test.
- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add src/cli tests CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(cli)!: rune schema is read-only (apply/alter removed -> rune console -f); tests migrated

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-broker add libs/node/tests/client/RuneConsoleBrokerRegressionTest.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(cli): console carries the migrated schema apply/alter live assertions

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (controller):** full `*Regression*` in rune-ql, rune-client-cpp (+ TSan), broker (+ ASan / TSan).

---

### Task 6: Real-path e2e — the e2e scripts drive `rune console -f` (k3s VM)

**Files:**
- Modify: `rune-client-cpp/docker/rune-ql-e2e.sh`, `docker/Dockerfile.rune-ql-probe`, `docker/docker-compose.rune-ql.yml`, `tests/e2e/characters.rql`, `tests/e2e/schema_evolution/v1.rql`, `tests/e2e/rune_ql_probe.cpp`, `tests/e2e/evo_probe_v{1,2,3,4}.cpp` (comments), `rune-client-cpp/CMakeLists.txt` (comments)
- Modify: `rune-broker/docker/schema-evolution-e2e.sh`

**Interfaces:**
- Consumes: the console exit-code contract (Task 4: `-f` returns the broker `errorCode`; stderr carries `SchemaIncompatible (30)` / `SchemaConflict (31)`), `--validate`, `--expected-version`; the auth phase's `docker/lib/auth.sh` (`rune_bootstrap`, `rune_create_e2e_user`, `RUNE_USER` / `RUNE_PASSWORD_FILE` for every CLI / probe container).

- [ ] **Step 1: List every call site:**

```bash
grep -rn "rune schema \(apply\|alter\)" /Users/vradu/Projects/se-worktrees/rune-client-cpp/docker /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e /Users/vradu/Projects/se-worktrees/rune-client-cpp/CMakeLists.txt /Users/vradu/Projects/se-worktrees/rune-broker/docker
```
Expected (2026-10-19): `rune-ql-e2e.sh` (header line 12, the call at ~147, the summary at ~209), `Dockerfile.rune-ql-probe:13`, `docker-compose.rune-ql.yml:60`, `schema-evolution-e2e.sh` (header 14–28, calls at ~229, 243, 255, 268, 281, 285, 319, 323), the `.rql` / probe comments.

- [ ] **Step 2: Rewrite the invocations.** The flags carry over unchanged (`--broker`, `--validate`, `--expected-version`, `--cli-egress`, `--cli-data-dir`):

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
  /Users/vradu/Projects/se-worktrees/rune-broker/docker/schema-evolution-e2e.sh
```
Then by hand:
(a) `schema-evolution-e2e.sh`: the comment explaining the exit code now reads "`rune console -f` stops at the first failing statement and returns the broker's wire errorCode as its exit code (`QlRun::lastErrorCode`)". Phase 5 expects exit 30 and "incompatible"; phase 7's two racers (`rune console -f race_X.rql --expected-version 5`) expect exactly one 0 and one 31 whose stderr matches `stale\|conflict` (`SchemaConflict (31) …`). Read the phase 5 / 7 blocks after the pass and adjust only the grep patterns if the message text differs.
(b) Credentials: if the auth phase already sources `lib/auth.sh` in `schema-evolution-e2e.sh` and passes `RUNE_USER` / `RUNE_PASSWORD_FILE`, nothing more (the console reads the same env). `rune-client-cpp/docker/rune-ql-e2e.sh` is not in the auth plan's launcher list: if it runs an auth-enabled broker, source the broker's `docker/lib/auth.sh` the same way (`rune_gen_cluster_key`, `rune_bootstrap`, `rune_create_e2e_user`) and pass the env to the CLI container; if the auth phase already migrated it, leave it.
(c) Never add `RUNE_TOPICS` (the catalog grep gate `scripts/rune_topics_allowlist.txt` fails on it).

- [ ] **Step 3: Static checks** — the Step 1 grep prints nothing; `bash -n` on both scripts passes; `rune console --dry-run -f` parses every `.rql` the scripts use:

```bash
for f in /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/*.rql /Users/vradu/Projects/se-worktrees/rune-client-cpp/tests/e2e/schema_evolution/*.rql; do
  /Users/vradu/Projects/se-worktrees/rune-client-cpp/build/rune console --dry-run -f "$f" > /dev/null || echo "FAIL $f"
done
```

- [ ] **Step 4: Run the real-broker e2e on the VM** (never on the Mac):

```bash
rsync -a --delete /Users/vradu/Projects/se-worktrees/ root@178.104.16.102:/root/se-worktrees/
ssh root@178.104.16.102 'flock /root/vm.lock bash -c "cd /root/se-worktrees/rune-client-cpp && bash docker/rune-ql-e2e.sh"'
ssh root@178.104.16.102 'flock /root/vm.lock bash -c "cd /root/se-worktrees/rune-broker && bash docker/schema-evolution-e2e.sh"'
```
Expected: `rune-ql-e2e.sh` ends with its PASSED banner; `schema-evolution-e2e.sh` passes phases 1–8 (phase 5: exit 30 + "incompatible"; phase 7: one success, one SchemaConflict).

- [ ] **Step 5: Commit (two repos)**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add docker tests/e2e CMakeLists.txt
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "test(e2e): rune-ql e2e drives rune console -f

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git -C /Users/vradu/Projects/se-worktrees/rune-broker add docker/schema-evolution-e2e.sh
git -C /Users/vradu/Projects/se-worktrees/rune-broker commit -m "test(e2e): schema-evolution e2e drives rune console -f (schema apply/alter removed)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint C (controller):** full `*Regression*` in every touched repo (+ sanitizers); both e2e green on the VM; then the final whole-branch review.

---

## Self-review

- **Spec coverage (§D):** `parseScript` → already in the code (catalog Task 14), pinned in Task 1; `classifyInput` → Task 1; flags `--broker` / `--cli-egress` / `-f` + `RUNE_BROKER` / `RUNE_EGRESS` → Tasks 3–4 (`LiveOptions`, `LiveBroker::open`); prompts → Task 3; one client per session → Task 4 (`OneSessionForEveryStatementRegression`); `ok` lines → Task 4 (DD2); `line:col: message` → Tasks 1, 3; broker error name → Task 4 (`brokerFailure`); errors keep the session → Tasks 3–4; `exit` / `quit` / EOF → Task 3; `-f` stops at the first error, non-zero → Tasks 3–4; statements → DD1 (a superset); `rune schema` read-only with migrated tests → Task 5; e2e → Task 6.
- **Auth:** login flags / env, prompt, banner, 44–47 names, `SessionRevoked` ends the console, `NotAuthorized` keeps it → Task 4; no new grants (no new request type).
- **Type consistency:** `engine::Prepared` / `prepare(…, PrepareOptions, why)` / `LiveOptions` / `QlRun{…, style, lastErrorCode}` defined in Task 2, used in Tasks 3–4; `ScriptError{statementIndex, error}` used as in the code.

## Spec questions (resolved by the Decisions above; the spec text is not edited)

1. `--dry-run`, `--validate`, `--expected-version` are not in spec §D but needed to migrate `schema apply|alter` (DD3–DD5).
2. Spec lists three statement kinds; the shared engine runs all of them (DD1).
3. `TopicExists` was "success" in the old CLI; now an error, idempotency via `--request-id` (DD8).
4. Spec does not say how a session authenticates or what `SessionRevoked` does to a console (DD10, DD11).
5. Spec does not mention idle sessions; an Invoker CLI client must be pumped (DD9).
