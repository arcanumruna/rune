# Rune Phase 2b — Part E: Transient Topics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Memory topics become **transient topics** everywhere (enum, config, logs, docs), and a transient topic can be defined at runtime from Rune QL (`transient topic …;`), the client (`defineTopicAndWait(..., Persistence::Transient)`) and the wire (`DefineTopic` appended persistence byte), surviving a broker restart as a transient (empty) topic.

**Architecture:** One append-only persistence byte travels the whole DDL path: Rune QL `TopicDef.persistence` → client `Persistence` → protocol `DefineTopic.persistence` (written only when non-disk, so a disk frame stays byte-identical) → gateway → `DefineTopicMsg` → `AdminWorker` → `CreateTopicReqMsg` → control thread `TopicSpec.persistence = Transient` (the existing `InMemoryLog` path in `TopicRegistry` / `createRuntimeUserReplicas`). The durable `TopicSchema` record gains an optional trailing persistence byte after the `topicName` trailer so `recoverRuntimeTopicSpecs` recreates the topic as transient on boot. Views over a transient source are made to fold (today the worker skips non-disk sources) with two explicit cursor states for the cases a disk log never produces (source restarted empty below the view's cursor; ring eviction ahead of the cursor).

**Tech Stack:** C++23, GoogleTest, Google Benchmark, CMake (broker: Ninja `build-fast`; protocol/client/ql: each repo's configured Makefiles build dir), docker e2e (single node, real Aeron).

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` — section **E. Transient topics** is binding; also the "Global constraints" section. Parent: `docs/superpowers/specs/2026-10-08-rune-view-engine-single-source-design.md` (§9.7 transient views re-fold).

**Builds on / order:** Phase 2b build order is C → **E** → D → B → A. E does not consume anything from C. D consumes E's `transient topic` parser support (its console executes it through `Client::defineTopicAndWait(..., Persistence)`).

## Execution context

- **Repos and branches.** Cut `feat/phase2b-e-transient-topics` from each repo's integration branch:
  - broker `/Users/vradu/Projects/rune-broker-matviews` from `feature/materialized-views`
  - protocol `/Users/vradu/Projects/se-worktrees/rune-protocol` from `feat/view-engine-phase2a` (== `feature/materialized-views`)
  - client `/Users/vradu/Projects/se-worktrees/rune-client-cpp` from `feat/view-engine-phase2a` (== `feature/materialized-views`)
  - ql `/Users/vradu/Projects/se-worktrees/rune-ql` from `feat/schema-evolution-durable-tags`
  ```bash
  git -C /Users/vradu/Projects/rune-broker-matviews switch -c feat/phase2b-e-transient-topics feature/materialized-views
  git -C /Users/vradu/Projects/se-worktrees/rune-protocol switch -c feat/phase2b-e-transient-topics feat/view-engine-phase2a
  git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp switch -c feat/phase2b-e-transient-topics feat/view-engine-phase2a
  git -C /Users/vradu/Projects/se-worktrees/rune-ql switch -c feat/phase2b-e-transient-topics feat/schema-evolution-durable-tags
  ```
- **Builds (canonical sibling layout).** The client/ql/protocol checkouts are siblings under `se-worktrees/` (the client compiles `../rune-ql`, `../rune-codec`, `../rune-protocol` from there). The broker checkout's siblings (`/Users/vradu/Projects/rune-protocol`, `…/rune-codec`) are NOT these branches, so configure the broker's `build-fast` once with explicit sibling overrides:
  ```bash
  cd /Users/vradu/Projects/rune-broker-matviews
  cmake -S . -B build-fast -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON \
    -DCMAKE_TOOLCHAIN_FILE=/Users/vradu/Projects/rune-broker/build/Debug/generators/conan_toolchain.cmake \
    -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/se-worktrees/rune-protocol/include \
    -DRUNE_CODEC_ROOT=/Users/vradu/Projects/se-worktrees/rune-codec
  ```
  - ql: existing build dir `build/build/Release` (Makefiles; `cmake --build build/build/Release -j --target rune_ql_tests`).
  - protocol: existing build dir `build` (`cmake --build build -j --target rune_protocol_tests`).
  - client: existing build dir `build` (Debug, aeron found; targets `rune_schema_client_tests`, `rune_cli_live_tests`, `rune_cli`).
- **Regression gate before EVERY commit (CLAUDE.md #7), in every repo the commit touches:**
  - broker: `ctest --test-dir build-fast -R Regression --output-on-failure -j8`
  - protocol: `ctest --test-dir build -R Regression --output-on-failure`
  - client: `ctest --test-dir build -R Regression --output-on-failure -j8`
  - ql: `ctest --test-dir build/build/Release -R Regression --output-on-failure`
  then the touched repo's full `ctest` for the touched targets.
- Nothing is pushed. Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Global Constraints

- HARD RULES (CLAUDE.md): shared-nothing; ring buffers are the only cross-thread passing; state machines where each state holds only its variables; zero heap allocation on hot paths / in steady state; test + benchmark everything; a `*Regression*`-named test on every fix; full `*Regression*` suite first on every change; fix-on-sight.
- Spec E (verbatim): "`PersistenceMode::Memory` → `PersistenceMode::Transient`; `RUNE_TOPICS … persistence=transient`; docs, logs, metrics labels. `persistence=memory` stays accepted as an alias that logs a one-line deprecation warning (existing compose files keep working)."
- Spec E: "`transient topic <name> { … } partitions N key K;` → `TopicDef.persistence`. `alter topic` cannot change persistence (rejected with a clear error)."
- Spec E: "`DefineTopic` gains an appended `persistence` byte (0 = disk, 1 = transient; old clients send nothing → disk). `Client::defineTopicAndWait(..., Persistence::Transient)` overload."
- Spec E: "`TopicSchema` records the mode (appended trailer field, same pattern as Phase 1's `topicName`) so restart recovery (`recoverRuntimeTopicSpecs`) recreates the topic as transient: metadata survives, data does not."
- Spec E: "Views over transient topics: allowed. A persisted view over a transient topic keeps its checkpoint; a transient view over a transient topic is empty after a full restart."
- Cross-plan interface (fixed names): rune-ql `enum class Persistence : uint8_t { Disk = 0, Transient = 1 }` with `TopicDef.persistence`; broker `PersistenceMode::Transient`; client `Client::defineTopicAndWait(..., Persistence)`.
- Wire enums/fields append-only: persistence byte values 0 = disk, 1 = transient; any other value is rejected (never cast).
- A disk `DefineTopic` frame and a disk `TopicSchema` record stay **byte-identical** to today (goldens unchanged).
- Internal storage-backend names stay (`InMemoryLog`, `isMemoryBacked()`, `memoryLog()`, `memoryLogRingBytes`/`RUNE_MEMORY_LOG_RING_BYTES`, the on-disk `memory-epoch-fence/` dir): they name the backend, not the topic class (see Spec questions §5).

## Review Focus

Most likely to bite a user, each pinned by a named test in its owning task:

1. **An old client (no persistence byte) must still create a disk topic** — the byte is optional on the wire and in every ring frame; absence is disk, never garbage → `SchemaMessagesRegression.DefineTopicWithoutPersistenceByteDecodesAsDisk` (Task 2) and `SchemaGatewayRegression.DefineTopicWithoutPersistenceByteCreatesDiskTopic` (Task 5).
2. **ALTER on a transient topic must keep it transient across a restart** — the ALTER's new `TopicSchema` version is what recovery reads, so it must carry the mode forward → `AdminWorkerRegression.AlterCarriesTransientPersistenceForward` (Task 5) and `RuntimeTopicRecoveryRegression.AlteredTransientTopicStaysTransient` (Task 6).
3. **Re-defining an existing disk topic as transient (or vice versa) must answer `TopicExists` and leave the mode unchanged** — never silently flip → `AdminWorkerRegression.RedefineExistingTopicWithOtherPersistenceIsTopicExists` (Task 5).
4. **A persisted view over a transient topic after a full restart** keeps its rows and folds every post-restart record (the restarted source reuses offsets 0.. below the checkpoint cursor — without handling, new records are silently skipped) → `WorkerViewSetRegression.TransientSourceRestartedBelowCursorRefoldsFromLogStart` (Task 7).
5. **Existing compose files with `persistence=memory` keep booting** as transient topics with a one-line deprecation warning → `NodeConfigRegression.PersistenceMemoryIsDeprecatedAliasForTransient` (Task 3).

Also pinned (less likely, cheap): a topic or field literally named `transient` still parses (`ParserRegression.TopicNamedTransientStillParses`, Task 1); `alter topic x set persistence …` is a located, clear error (`ParserRegression.AlterCannotChangePersistence`, Task 1).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-ql/include/rune/ql/Ast.h` | `enum class Persistence`, `TopicDef.persistence` | 1 |
| `rune-ql/src/Parser.cpp` | `transient topic …;`, alter-persistence rejection, `parseStatement()` dispatch | 1 |
| `rune-ql/include/rune/ql/Backends.h`, `src/Backends.cpp` | `DefineTopicPlan.persistence` | 1 |
| `rune-ql/tests/ParserTest.cpp`, `tests/BackendsTest.cpp`, `benchmark/ParseBench.cpp` | tests + `BM_ParseTransientTopic` | 1 |
| `rune-protocol/include/rune/protocol/client/SchemaMessages.h` | `DefineTopic.persistence` appended byte | 2 |
| `rune-protocol/tests/protocol/client/SchemaMessagesTest.cpp`, `SchemaConformanceTest.cpp` | round-trip, old-frame, transient golden | 2 |
| `libs/node/include/rune/node/PersistenceMode.h` (new) | `PersistenceMode {Disk, Transient}` + wire helpers | 3 |
| `libs/node/include/rune/node/NodeConfig.h`, `src/NodeConfig.cpp`, `src/main.cpp` | config rename, `memory` alias + `ConfigResult.warnings` | 3 |
| `libs/node/src/BrokerNode.cpp`, `libs/gateway/src/TopicRegistry.cpp`, `libs/node/include/rune/node/Worker.h` | rename + log text | 3 |
| `libs/node/include/rune/node/SchemaRegistry.h`, `src/SchemaRegistry.cpp` | `TopicSchema.persistence` trailer, `RegistryEntry.persistence` | 4 |
| `libs/gateway/include/rune/gateway/IAdminRequestSink.h`, `libs/gateway/src/ClientGateway.cpp` | pass persistence to the admin sink | 5 |
| `libs/node/include/rune/node/AdminChannels.h` | `DefineTopicMsg.persistence`, `CreateTopicReqMsg.persistence` ring frames | 5 |
| `libs/node/include/rune/node/AdminWorker.h`, `src/AdminWorker.cpp`, `include/rune/node/BrokerNode.h` | carry persistence through DDL + ALTER | 5 |
| `libs/node/src/BrokerNode.cpp` (control drain) | `spec.persistence` from the request | 5 |
| `libs/node/src/RuntimeTopicRecovery.cpp` | recover persistence | 6 |
| `libs/node/tests/support/ShardTestClient.h` | `defineTopicUntil(..., persistence)` | 6 |
| `libs/node/include/rune/node/WorkerViewSet.h`, `src/WorkerViewSet.cpp`, `include/rune/node/Worker.h` | fold views over transient sources | 7 |
| `rune-client-cpp/include/rune/client/Client.h`, `src/Client.cpp`, `src/cli/Cli.cpp` | `Persistence`, overloads, `schema apply` honours `transient` | 8 |
| `docs/SELF-HOSTING.md` | `RUNE_TOPICS` + transient topics docs | 9 |
| `rune-client-cpp/examples/view_probe.cpp`, `docker/view-engine-e2e.sh` | e2e transient leg (BLOCKED — Spec question 1) | 10 |

---

### Task 1: Rune QL — `transient topic`, `TopicDef.persistence`, alter rejection

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-ql`

**Files:**
- Modify: `include/rune/ql/Ast.h`
- Modify: `src/Parser.cpp` (grammar comment lines 1-13; `parseAlterOp` "set" branch ~L376-400; `parseAlterStatement` ~L408-445; `parse` ~L465-486)
- Modify: `include/rune/ql/Backends.h`, `src/Backends.cpp:20-27`
- Test: `tests/ParserTest.cpp`, `tests/BackendsTest.cpp`
- Bench: `benchmark/ParseBench.cpp`

**Interfaces:**
- Produces (consumed by Task 8 and by Part D's console):
  - `namespace rune::ql { enum class Persistence : uint8_t { Disk = 0, Transient = 1 }; }`
  - `struct TopicDef { std::string name; std::vector<FieldDef> fields; uint32_t partitions = 1; std::string keyField; Persistence persistence = Persistence::Disk; };` (new member appended — existing 4-element aggregate inits still compile)
  - `struct DefineTopicPlan { std::string name; uint32_t partitions; std::string keyField; std::vector<uint8_t> descriptorBlob; Persistence persistence = Persistence::Disk; };`
  - `parse("transient topic t {…} partitions N key K;")` → `TopicDef` with `persistence == Persistence::Transient`.
  - Internal `Parser::parseStatement()` — the single statement dispatcher (`topic` | `transient topic` | `alter topic`); Part D's `parseScript` should call it per statement.

- [ ] **Step 1: Write the failing tests** — append to `tests/ParserTest.cpp`:

```cpp
// ── Phase 2b E: transient topics ─────────────────────────────────────────────
TEST(Parser, TransientTopicSetsPersistence) {
    auto r = rune::ql::parse(
        "transient topic presence { userId int64, online bool } partitions 4 key userId;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto* t = std::get_if<rune::ql::TopicDef>(&*r);
    ASSERT_NE(t, nullptr);
    EXPECT_EQ(t->name, "presence");
    EXPECT_EQ(t->partitions, 4u);
    EXPECT_EQ(t->keyField, "userId");
    EXPECT_EQ(t->persistence, rune::ql::Persistence::Transient);
}

TEST(Parser, PlainTopicDefaultsToDisk) {
    auto r = rune::ql::parse("topic users { id int64 } partitions 4 key id;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(std::get<rune::ql::TopicDef>(*r).persistence, rune::ql::Persistence::Disk);
    EXPECT_EQ(static_cast<uint8_t>(rune::ql::Persistence::Disk), 0u);
    EXPECT_EQ(static_cast<uint8_t>(rune::ql::Persistence::Transient), 1u);
}

TEST(ParserRegression, TransientWithoutTopicIsLocatedError) {
    auto r = rune::ql::parse("transient presence { a int32 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("'topic' after 'transient'"), std::string::npos)
        << r.error().message;
    EXPECT_EQ(r.error().line, 1u);
    EXPECT_EQ(r.error().col, 11u) << "error points at the token after 'transient'";
}

// 'transient' is a statement keyword only in statement-leading position: a topic
// or a field literally named "transient" must keep parsing.
TEST(ParserRegression, TopicNamedTransientStillParses) {
    auto r = rune::ql::parse("topic transient { transient int32 } partitions 1 key transient;");
    ASSERT_TRUE(r.has_value()) << r.error().message;
    const auto& t = std::get<rune::ql::TopicDef>(*r);
    EXPECT_EQ(t.name, "transient");
    EXPECT_EQ(t.fields[0].name, "transient");
    EXPECT_EQ(t.persistence, rune::ql::Persistence::Disk);
}

// Spec E: alter topic cannot change persistence — both spellings a user would
// try are rejected with an explicit message (not a generic syntax error).
TEST(ParserRegression, AlterCannotChangePersistence) {
    auto a = rune::ql::parse("alter topic presence set persistence transient;");
    ASSERT_FALSE(a.has_value());
    EXPECT_NE(a.error().message.find("cannot change persistence"), std::string::npos)
        << a.error().message;
    EXPECT_EQ(a.error().col, 26u) << "points at 'persistence'";

    auto b = rune::ql::parse("alter transient topic presence add column x int32;");
    ASSERT_FALSE(b.has_value());
    EXPECT_NE(b.error().message.find("cannot change persistence"), std::string::npos)
        << b.error().message;
    EXPECT_EQ(b.error().col, 7u) << "points at 'transient'";
}

TEST(ParserRegression, UnknownStatementMentionsTransient) {
    auto r = rune::ql::parse("drop topic users;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("'transient'"), std::string::npos) << r.error().message;
}
```

Append to `tests/BackendsTest.cpp`:

```cpp
TEST(BackendsRegression, ApplyPlanCarriesPersistence) {
    auto t = std::get<TopicDef>(
        parse("transient topic p { id int64 } partitions 2 key id;").value());
    EXPECT_EQ(applyPlan(t).persistence, Persistence::Transient);
    auto d = std::get<TopicDef>(parse("topic q { id int64 } partitions 2 key id;").value());
    EXPECT_EQ(applyPlan(d).persistence, Persistence::Disk);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build/build/Release -j --target rune_ql_tests`
Expected: compile error `no member named 'persistence' in 'rune::ql::TopicDef'` / `no type named 'Persistence' in namespace 'rune::ql'`.

- [ ] **Step 3: Implement**

`include/rune/ql/Ast.h` — replace the `TopicDef` line:

```cpp
// Topic persistence (Phase 2b E).  Values are the DefineTopic wire byte and are
// APPEND-ONLY: Disk = durable log (default), Transient = replicated in memory,
// never written to disk (metadata survives a restart, data does not).
enum class Persistence : uint8_t { Disk = 0, Transient = 1 };

struct TopicDef {
    std::string name;
    std::vector<FieldDef> fields;
    uint32_t partitions = 1;
    std::string keyField;
    Persistence persistence = Persistence::Disk;   // `transient topic …;` → Transient
};
```

`src/Parser.cpp` — grammar header comment (lines 2-4) becomes:

```cpp
// Grammar:
//   statement   := topicStmt | transientStmt | alterStmt
//   transientStmt := "transient" topicStmt                 (Phase 2b E: persistence = Transient)
//   topicStmt   := "topic" IDENT "{" field ("," field)* ","? "}" "partitions" INT "key" IDENT ";"
//   alterStmt   := "alter" "topic" IDENT alterOp ("," alterOp)* ";"
//                  ("alter transient …" and "set persistence …" are rejected:
//                   a topic's persistence is fixed when it is defined)
```

In `parseAlterOp`, replace the whole `if (cur.text == "set") { … }` block with:

```cpp
        if (cur.text == "set") {
            advance();
            // Spec E: persistence is fixed at definition — reject the spelling a
            // user would try with an explicit message, not "expected 'compatibility'".
            if (cur.kind == TK::Ident && cur.text == "persistence") {
                return std::unexpected(err(
                    "alter topic cannot change persistence: a topic is transient or "
                    "durable for life (define a new topic instead)"));
            }
            auto compat = expectKeyword("compatibility");
            if (!compat) return std::unexpected(std::move(compat.error()));

            if (cur.kind != TK::Ident) {
                return std::unexpected(err("expected 'backward' or 'forward'"));
            }
            Compatibility mode;
            if (cur.text == "backward")      mode = Compatibility::Backward;
            else if (cur.text == "forward")  mode = Compatibility::Forward;
            else {
                return std::unexpected(err(
                    std::string("expected 'backward' or 'forward', got '") + std::string(cur.text) + "'"));
            }
            advance();

            AlterOp op;
            op.kind = AlterOpKind::SetCompatibility;
            op.mode = mode;
            return op;
        }
```

In `parseAlterStatement`, between the `alterTok` check and `auto topicTok = expectKeyword("topic");` insert:

```cpp
        // Spec E: "alter transient topic …" would read as changing persistence.
        if (cur.kind == TK::Ident && cur.text == "transient") {
            return std::unexpected(err(
                "alter topic cannot change persistence: a topic is transient or "
                "durable for life (define a new topic instead)"));
        }
```

Add a dispatcher member at the end of `struct Parser` (after `parseAlterStatement`):

```cpp
    // ---- statement dispatch (Phase 2b E; Part D's parseScript reuses this) ----
    //   "topic" … | "transient" "topic" … | "alter" "topic" …
    std::expected<std::variant<TopicDef, AlterTopicDef>, ParseError> parseStatement() noexcept {
        if (cur.kind != TK::Ident) {
            return std::unexpected(err("expected 'topic', 'transient' or 'alter'"));
        }
        if (cur.text == "topic") {
            auto r = parseTopicStatement();
            if (!r) return std::unexpected(std::move(r.error()));
            return std::variant<TopicDef, AlterTopicDef>(std::in_place_type<TopicDef>, std::move(*r));
        }
        if (cur.text == "transient") {
            advance();
            if (cur.kind != TK::Ident || cur.text != "topic") {
                return std::unexpected(err(
                    std::string("expected 'topic' after 'transient', got '") +
                    std::string(cur.text) + "'"));
            }
            auto r = parseTopicStatement();
            if (!r) return std::unexpected(std::move(r.error()));
            r->persistence = Persistence::Transient;
            return std::variant<TopicDef, AlterTopicDef>(std::in_place_type<TopicDef>, std::move(*r));
        }
        if (cur.text == "alter") {
            auto r = parseAlterStatement();
            if (!r) return std::unexpected(std::move(r.error()));
            return std::variant<TopicDef, AlterTopicDef>(std::in_place_type<AlterTopicDef>, std::move(*r));
        }
        return std::unexpected(err(
            std::string("expected 'topic', 'transient' or 'alter', got '") +
            std::string(cur.text) + "'"));
    }
```

Replace the body of the public `parse` with:

```cpp
std::expected<std::variant<TopicDef, AlterTopicDef>, ParseError> parse(std::string_view src) noexcept {
    Parser p{src};
    return p.parseStatement();
}
```

`include/rune/ql/Backends.h` — `DefineTopicPlan` becomes:

```cpp
struct DefineTopicPlan {
    std::string          name;
    uint32_t             partitions;
    std::string          keyField;
    std::vector<uint8_t> descriptorBlob;
    Persistence          persistence = Persistence::Disk;   // Phase 2b E
};
```

`src/Backends.cpp` — `applyPlan`:

```cpp
DefineTopicPlan applyPlan(const TopicDef& td) {
    return {
        td.name,
        td.partitions,
        td.keyField,
        rune::codec::serialize(toDescriptor(td)),
        td.persistence
    };
}
```

`benchmark/ParseBench.cpp` — append:

```cpp
static constexpr std::string_view kTransientDDL =
    "transient topic presence { userId int64, online bool } partitions 8 key userId;";

static void BM_ParseTransientTopic(benchmark::State& state) {
    for (auto _ : state) {
        auto result = rune::ql::parse(kTransientDDL);
        benchmark::DoNotOptimize(result);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_ParseTransientTopic);
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build/build/Release -j --target rune_ql_tests && ctest --test-dir build/build/Release -R Regression --output-on-failure && ctest --test-dir build/build/Release --output-on-failure`
Expected: all PASS (the existing `Parser.RejectsUnknownStatementKeyword`, `ParserRegression.AlterUnknownOpIsLocatedError` etc. unchanged).
Bench (record the number in the commit body, no gate — DDL is a cold path): `cmake --build build/build/Release -j --target rune_ql_bench && ./build/build/Release/rune_ql_bench --benchmark_filter='Parse'` — `BM_ParseTransientTopic` within 2x of `BM_ParseCharacters`.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-ql add include/rune/ql/Ast.h include/rune/ql/Backends.h src/Parser.cpp src/Backends.cpp tests/ParserTest.cpp tests/BackendsTest.cpp benchmark/ParseBench.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-ql commit -m "feat(ql): transient topic statement; TopicDef.persistence; alter cannot change persistence

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Protocol — `DefineTopic` appended persistence byte

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-protocol`

**Files:**
- Modify: `include/rune/protocol/client/SchemaMessages.h:12-57` (`struct DefineTopic`)
- Test: `tests/protocol/client/SchemaMessagesTest.cpp`, `tests/protocol/client/SchemaConformanceTest.cpp`

**Interfaces:**
- Produces: `rune::protocol::client::DefineTopic::persistence` (`uint8_t`, default 0), constants `DefineTopic::kPersistenceDisk = 0`, `DefineTopic::kPersistenceTransient = 1`. Encoded as ONE trailing byte after `descriptorBlob` **only when non-zero**; decode reads it when present, absent ⇒ 0, a value > 1 ⇒ `std::nullopt`.

- [ ] **Step 1: Write the failing tests** — append to `tests/protocol/client/SchemaMessagesTest.cpp`:

```cpp
// ── Phase 2b E: appended persistence byte ────────────────────────────────────
TEST(SchemaMessagesRegression, DefineTopicPersistenceRoundTrips) {
    std::vector<uint8_t> blob = {9, 8, 7};
    DefineTopic in;
    in.sessionId = 1; in.correlationId = 2;
    in.topicName = "presence"; in.partitionCount = 4; in.keyField = "userId";
    in.descriptorBlob = blob.data(); in.descriptorLen = 3;
    in.persistence = DefineTopic::kPersistenceTransient;

    std::vector<uint8_t> buf(in.encodedSize());
    ASSERT_EQ(in.encode(buf.data()), buf.size());
    auto out = DefineTopic::decode(buf.data(), buf.size());
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->persistence, DefineTopic::kPersistenceTransient);
    EXPECT_EQ(out->topicName, "presence");
    ASSERT_EQ(out->descriptorLen, 3u);
}

// Old clients send nothing after the blob: that is a DISK topic, and a disk frame
// from a new client is byte-identical to an old one (no trailing byte written).
TEST(SchemaMessagesRegression, DefineTopicWithoutPersistenceByteDecodesAsDisk) {
    std::vector<uint8_t> blob = {1};
    DefineTopic disk;
    disk.topicName = "t"; disk.descriptorBlob = blob.data(); disk.descriptorLen = 1;
    DefineTopic transientMsg = disk;
    transientMsg.persistence = DefineTopic::kPersistenceTransient;
    EXPECT_EQ(transientMsg.encodedSize(), disk.encodedSize() + 1)
        << "the persistence byte is written only for a non-disk topic";

    std::vector<uint8_t> buf(disk.encodedSize());
    disk.encode(buf.data());
    auto out = DefineTopic::decode(buf.data(), buf.size());
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->persistence, DefineTopic::kPersistenceDisk);
}

TEST(SchemaMessagesRegression, DefineTopicUnknownPersistenceValueIsRejected) {
    std::vector<uint8_t> blob = {1};
    DefineTopic in;
    in.topicName = "t"; in.descriptorBlob = blob.data(); in.descriptorLen = 1;
    in.persistence = DefineTopic::kPersistenceTransient;
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    buf.back() = 2;   // one past Transient: append-only enum, never cast
    EXPECT_FALSE(DefineTopic::decode(buf.data(), buf.size()).has_value());
}
```

Append to `tests/protocol/client/SchemaConformanceTest.cpp` (after `DefineTopicEncodeGolden`):

```cpp
// Phase 2b E: the transient form is the disk golden + ONE trailing 0x01 byte.
TEST(SchemaConformance, DefineTopicTransientGoldenIsDiskGoldenPlusOneByte) {
    static constexpr uint8_t kBlob[] = {0xAA, 0xBB, 0xCC};
    rpc::DefineTopic msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicName      = "schema_test";
    msg.partitionCount = 4;
    msg.keyField       = "id";
    msg.validate       = true;
    msg.descriptorBlob = kBlob;
    msg.descriptorLen  = 3;
    msg.persistence    = rpc::DefineTopic::kPersistenceTransient;

    ASSERT_EQ(msg.encodedSize(), kDefineTopicGoldenSize + 1);
    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, kDefineTopicGoldenSize + 1);
    EXPECT_EQ(std::memcmp(enc.data(), kDefineTopicGolden, kDefineTopicGoldenSize), 0)
        << "everything before the persistence byte is the unchanged disk golden";
    EXPECT_EQ(enc[kDefineTopicGoldenSize], 0x01);

    auto back = rpc::DefineTopic::decode(enc.data(), n);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->persistence, 1u);
    // The unchanged 53-byte golden still decodes, as disk.
    auto old = rpc::DefineTopic::decode(kDefineTopicGolden, kDefineTopicGoldenSize);
    ASSERT_TRUE(old.has_value());
    EXPECT_EQ(old->persistence, 0u);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j --target rune_protocol_tests`
Expected: compile error `no member named 'persistence' in 'rune::protocol::client::DefineTopic'`.

- [ ] **Step 3: Implement** — replace `struct DefineTopic` (and its header comment) in `SchemaMessages.h`:

```cpp
// DefineTopic (64): body = partitionCount:u32, validate:u8, then var fields
//   topicName(varstr), keyField(varstr), descriptorBlob(varbytes32),
//   [persistence:u8]  — Phase 2b E, OPTIONAL trailing byte: written only when
//   non-zero (a disk frame is byte-identical to the pre-2b layout); absent on
//   decode ⇒ disk (old clients).  Values are append-only: 0 = disk,
//   1 = transient; anything else is rejected.
struct DefineTopic {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint32_t partitionCount = 1;
    std::string_view keyField;
    bool validate = false;
    const uint8_t* descriptorBlob = nullptr;
    uint32_t descriptorLen = 0;
    uint8_t persistence = 0;   // kPersistenceDisk | kPersistenceTransient

    static constexpr uint8_t kPersistenceDisk      = 0;
    static constexpr uint8_t kPersistenceTransient = 1;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DefineTopic);
    static constexpr uint16_t BODY_FIXED  = 5;  // u32 partitionCount + u8 validate

    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + topicName.size()
             + 2u + keyField.size()
             + 4u + descriptorLen
             + (persistence != kPersistenceDisk ? 1u : 0u);
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), static_cast<int32_t>(partitionCount)); pos += 4;
        buf[pos++] = validate ? 1 : 0;
        pos += putVarStr(buf + pos, topicName);
        pos += putVarStr(buf + pos, keyField);
        pos += putVarBytes32(buf + pos, descriptorBlob, descriptorLen);
        if (persistence != kPersistenceDisk) buf[pos++] = persistence;
        return pos;
    }
    static std::optional<DefineTopic> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        DefineTopic m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.partitionCount = static_cast<uint32_t>(detail::getLE32(buf, static_cast<int>(pos))); pos += 4;
        m.validate = buf[pos++] != 0;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        if (!getVarStr(buf, avail, pos, m.keyField)) return std::nullopt;
        if (!getVarBytes32(buf, avail, pos, m.descriptorBlob, m.descriptorLen)) return std::nullopt;
        if (pos < avail) {
            const uint8_t p = buf[pos++];
            if (p > kPersistenceTransient) return std::nullopt;   // append-only: never cast
            m.persistence = p;
        }
        return m;
    }
};
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j --target rune_protocol_tests && ctest --test-dir build -R Regression --output-on-failure && ctest --test-dir build --output-on-failure`
Expected: all PASS, including the unchanged `SchemaConformance.DefineTopicDecodeGolden` / `DefineTopicEncodeGolden` and `SchemaMessages.DecodeRejectsTruncated`.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-protocol add include/rune/protocol/client/SchemaMessages.h tests/protocol/client/SchemaMessagesTest.cpp tests/protocol/client/SchemaConformanceTest.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-protocol commit -m "feat(protocol): DefineTopic optional trailing persistence byte (0 disk, 1 transient)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Broker — rename `PersistenceMode::Memory` → `Transient`; `persistence=transient` + deprecated `memory` alias

**Repo:** broker `/Users/vradu/Projects/rune-broker-matviews`

**Files:**
- Create: `libs/node/include/rune/node/PersistenceMode.h`
- Modify: `libs/node/include/rune/node/NodeConfig.h:57-65` (enum moves out), `:118-121` (comment), `:942-945` (`ConfigResult`)
- Modify: `libs/node/src/NodeConfig.cpp:485-488` (warnings local), `:783` (comment), `:850-859` (persistence parse), `:1491` (return)
- Modify: `src/main.cpp:143-147`
- Modify: `libs/node/src/BrokerNode.cpp` (every `PersistenceMode::Memory`, every `isMemory`, the debug log at `:1140`)
- Modify: `libs/gateway/src/TopicRegistry.cpp:63-71`
- Modify: `libs/node/include/rune/node/Worker.h:6520-6523` (warn text)
- Test: `libs/node/tests/node/NodeConfigTest.cpp:1209-1252`, `libs/node/tests/node/BrokerNodeTest.cpp:991-1041`, `libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp:32-34,718-830`

**Interfaces:**
- Produces:
  ```cpp
  namespace rune::node {
  enum class PersistenceMode : uint8_t { Disk = 0, Transient = 1 };
  [[nodiscard]] constexpr PersistenceMode persistenceFromWire(uint8_t b) noexcept;   // 1 → Transient, else Disk
  [[nodiscard]] constexpr uint8_t persistenceToWire(PersistenceMode m) noexcept;    // Disk 0, Transient 1
  }
  struct ConfigResult { std::optional<NodeConfig> config; std::string error; std::vector<std::string> warnings; };
  ```

- [ ] **Step 1: Write the failing tests** — in `libs/node/tests/node/NodeConfigTest.cpp`, change the two existing assertions that name the old enumerator (lines 1226 and 1249) from `rune::node::PersistenceMode::Memory` to `rune::node::PersistenceMode::Transient`, and append:

```cpp
// ===========================================================================
// Phase 2b E: transient topics (rename of memory topics)
// ===========================================================================
namespace {
rune::node::ConfigResult parseWithTopics(const char* topics) {
    return rune::node::parseNodeConfig([topics](const char* k) -> const char* {
        std::string s = k;
        if (s == "RUNE_NODE_ID") return "0";
        if (s == "RUNE_PEERS") return "0@h:1";
        if (s == "RUNE_CLIENT_ENDPOINT") return "h:2";
        if (s == "RUNE_DATA_DIR") return "/tmp/x";
        if (s == "RUNE_TOPICS") return topics;
        return nullptr;
    });
}
} // namespace

TEST(NodeConfig, PersistenceTransientParses) {
    auto res = parseWithTopics("presence:2:persistence=transient,orders:4:persistence=disk");
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->topics[0].persistence, rune::node::PersistenceMode::Transient);
    EXPECT_EQ(res.config->topics[1].persistence, rune::node::PersistenceMode::Disk);
    EXPECT_TRUE(res.warnings.empty()) << "the canonical spelling never warns";
}

// Existing compose files say persistence=memory: they must keep booting, as
// TRANSIENT topics, with exactly one deprecation warning per such entry.
TEST(NodeConfigRegression, PersistenceMemoryIsDeprecatedAliasForTransient) {
    auto res = parseWithTopics("presence:2:persistence=MEMORY,chat:1:persistence=memory");
    ASSERT_TRUE(res.config.has_value()) << res.error;
    EXPECT_EQ(res.config->topics[0].persistence, rune::node::PersistenceMode::Transient);
    EXPECT_EQ(res.config->topics[1].persistence, rune::node::PersistenceMode::Transient);
    ASSERT_EQ(res.warnings.size(), 2u);
    EXPECT_NE(res.warnings[0].find("deprecated"), std::string::npos) << res.warnings[0];
    EXPECT_NE(res.warnings[0].find("persistence=transient"), std::string::npos) << res.warnings[0];
    EXPECT_NE(res.warnings[0].find("presence"), std::string::npos) << res.warnings[0];
    EXPECT_NE(res.warnings[1].find("chat"), std::string::npos) << res.warnings[1];
}

TEST(NodeConfig, PersistenceBadValueNamesTransient) {
    auto res = parseWithTopics("t:1:persistence=ram");
    ASSERT_FALSE(res.config.has_value());
    EXPECT_NE(res.error.find("'disk' or 'transient'"), std::string::npos) << res.error;
}

TEST(NodeConfig, PersistenceModeWireMapping) {
    using rune::node::PersistenceMode;
    EXPECT_EQ(rune::node::persistenceToWire(PersistenceMode::Disk), 0u);
    EXPECT_EQ(rune::node::persistenceToWire(PersistenceMode::Transient), 1u);
    EXPECT_EQ(rune::node::persistenceFromWire(0), PersistenceMode::Disk);
    EXPECT_EQ(rune::node::persistenceFromWire(1), PersistenceMode::Transient);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_node_tests`
Expected: compile errors `no member named 'Transient' in 'rune::node::PersistenceMode'`, `no member named 'warnings' in 'rune::node::ConfigResult'`.

- [ ] **Step 3: Implement**

Create `libs/node/include/rune/node/PersistenceMode.h`:

```cpp
#pragma once
// PersistenceMode — a topic's log backend (Phase 2b E: "memory topics" are now
// TRANSIENT topics).  Its own header so TopicSchema (SchemaRegistry.h) can carry
// it without pulling in NodeConfig.h.
//
// Values equal the DefineTopic / TopicSchema wire byte and are APPEND-ONLY.
#include <cstdint>

namespace rune::node {

enum class PersistenceMode : uint8_t {
    Disk      = 0,   ///< On-disk Log (default). Survives node restarts.
    Transient = 1,   ///< InMemoryLog: replicated to the ISR before ack, NEVER written to
                     ///  disk.  Metadata survives a restart; data does not (a restarted
                     ///  node rejoins empty; after a full-cluster restart the topic is
                     ///  empty).  A transient topic that must survive a node death
                     ///  must use majority or all-in-sync durability.
};

/// Wire byte → mode.  Only 1 is Transient; callers reject values > 1 at decode.
[[nodiscard]] constexpr PersistenceMode persistenceFromWire(uint8_t b) noexcept {
    return b == 1 ? PersistenceMode::Transient : PersistenceMode::Disk;
}
[[nodiscard]] constexpr uint8_t persistenceToWire(PersistenceMode m) noexcept {
    return static_cast<uint8_t>(m);
}

} // namespace rune::node
```

`libs/node/include/rune/node/NodeConfig.h` — delete the `PersistenceMode` block (the comment banner + enum, lines 57-65) and add `#include <rune/node/PersistenceMode.h>` next to the other `rune/node` includes at the top of the file. Replace the `TopicSpec` comment lines 118-121 with:

```cpp
    // Task M1 / Phase 2b E: persistence and durability modes.
    // persistence: selects the log backend (Disk = default, Transient = InMemoryLog).
    // durability: 0 = Majority (default), 1 = AllInSync.  Forwarded to PartitionConfig.
    //   Transient topics that must survive a node death MUST use majority or all-in-sync
    //   durability so the data is replicated to peer nodes before acknowledgement.
```

Replace `struct ConfigResult`:

```cpp
struct ConfigResult {
    std::optional<NodeConfig> config;
    std::string               error;
    // Non-fatal, one line each, logged by the caller (main) at startup — e.g. a
    // deprecated RUNE_TOPICS spelling (Phase 2b E: persistence=memory).
    std::vector<std::string>  warnings;
};
```

`libs/node/src/NodeConfig.cpp` — at the top of `parseNodeConfig` (right after the opening `{` on line 487) add:

```cpp
    std::vector<std::string> warnings;   // non-fatal (deprecated spellings); returned on success
```

Line 783 comment: `//       persistence=memory|disk, durability=majority|all-in-sync.` → `//       persistence=disk|transient (memory = deprecated alias), durability=majority|all-in-sync.`

Replace the persistence branch (lines 850-859):

```cpp
                    } else if (klower == "persistence") {
                        std::string vlow(kval);
                        for (auto& ch : vlow) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                        if (vlow == "transient") {
                            ts.persistence = PersistenceMode::Transient;
                        } else if (vlow == "memory") {
                            // Phase 2b E: memory topics were renamed transient topics.
                            // Existing compose files keep working; one line per entry.
                            ts.persistence = PersistenceMode::Transient;
                            warnings.push_back("RUNE_TOPICS persistence=memory is deprecated — use "
                                               "persistence=transient (topic '" + ts.name + "')");
                        } else if (vlow == "disk") {
                            ts.persistence = PersistenceMode::Disk;
                        } else {
                            return {{}, std::string("RUNE_TOPICS persistence must be 'disk' or 'transient', got: ") + std::string(kval) + " in entry: " + std::string(entry)};
                        }
```

Final return (line 1491): `return {std::move(cfg), {}, std::move(warnings)};`

`src/main.cpp` lines 143-147 become:

```cpp
    auto [cfg, err, warnings] = rune::node::parseNodeConfigFromEnv();
    if (!cfg.has_value()) {
        std::cerr << "rune-broker: configuration error: " << err << "\n";
        return 2;
    }
    for (const auto& w : warnings) spdlog::warn("rune-broker: {}", w);
```

`libs/node/src/BrokerNode.cpp` — mechanical rename, then the log line:

```bash
sed -i '' -e 's/PersistenceMode::Memory/PersistenceMode::Transient/g' -e 's/\bisMemory\b/isTransient/g' libs/node/src/BrokerNode.cpp
grep -n 'PersistenceMode::Memory\|isMemory' libs/node/src/BrokerNode.cpp   # expect: no output
```
(BSD sed has no `\b`; if the second expression matched nothing, run `sed -i '' -e 's/isMemory /isTransient /g' -e 's/isMemory)/isTransient)/g' -e 's/isMemory$/isTransient/g' libs/node/src/BrokerNode.cpp` and re-run the grep.)
Then replace the boot-loop debug log (line ~1140):

```cpp
                spdlog::debug("BrokerNode[{}]: transient topic '{}' partition {} on InMemoryLog",
                              config_.nodeId, spec->name, part);
```
and the two comments `// Memory-backed: create an InMemoryLog …` / `// Memory-backed partition: create an InMemoryLog.` / `// Recovery SKIPS memory partitions …` → `// Transient: create an InMemoryLog …` / `// Transient partition: create an InMemoryLog.` / `// Recovery SKIPS transient partitions …`.

`libs/gateway/src/TopicRegistry.cpp` lines 63-71:

```cpp
        // Transient topics (PersistenceMode::Transient) do NOT create disk
        // directories or Log objects.  The PartitionEntry is left with log=nullptr.
        // BrokerNode::buildReplication() detects this and substitutes an InMemoryLog.
        if (spec.persistence == rune::node::PersistenceMode::Transient) {
            spdlog::debug(
                "TopicRegistry: topic='{}' partition={} [transient, no disk log]",
                spec.name, p);
            entry.partitions.push_back(std::move(pe));
            continue;
        }
```

`libs/node/include/rune/node/Worker.h` lines 6520-6523:

```cpp
                // Owned but transient (in-memory log): Internal.
                spdlog::warn("Worker {}: WorkerIngress: owned but transient (in-memory) log for "
                             "topicId={} partition={}; Internal error ack",
                             workerId_, f.topicId, f.partition);
```

Tests — rename the old enumerator and test names (test names are documentation):

```bash
sed -i '' 's/PersistenceMode::Memory/PersistenceMode::Transient/g' libs/node/tests/node/BrokerNodeTest.cpp
sed -i '' 's/M1_MemoryBackedPartitionSkipsCleaner/M1_TransientPartitionSkipsCleaner/; s/persistence=Memory/persistence=Transient/g' libs/node/tests/node/BrokerNodeTest.cpp
sed -i '' 's/MemoryTopic_/TransientTopic_/g' libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp
grep -rn 'PersistenceMode::Memory\|MemoryTopic_\|M1_MemoryBacked' libs src   # expect: no output
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'NodeConfig|TransientTopic_|M1_Transient|TopicRegistry' --output-on-failure`
Expected: all PASS; `grep -rni 'persistence=memory' docker README.md docs/SELF-HOSTING.md` shows nothing (compose files never used it).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/PersistenceMode.h libs/node/include/rune/node/NodeConfig.h libs/node/src/NodeConfig.cpp src/main.cpp libs/node/src/BrokerNode.cpp libs/gateway/src/TopicRegistry.cpp libs/node/include/rune/node/Worker.h libs/node/tests/node/NodeConfigTest.cpp libs/node/tests/node/BrokerNodeTest.cpp libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp
git commit -m "refactor(node): memory topics are transient topics; persistence=memory is a deprecated alias

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Broker — `TopicSchema` records the persistence mode

**Files:**
- Modify: `libs/node/include/rune/node/SchemaRegistry.h:34-71` (`TopicSchema`, `RegistryEntry`)
- Modify: `libs/node/src/SchemaRegistry.cpp:79-120` (serialize), `:121-200` (deserialize), `applyRecord`
- Test: `libs/node/tests/node/SchemaRegistryTest.cpp`

**Interfaces:**
- Consumes: `PersistenceMode` (Task 3).
- Produces: `TopicSchema::persistence` and `RegistryEntry::persistence` (`PersistenceMode`, default `Disk`). Record layout after `compatMode`: `[nameLen:u16][name][persistence:u8]?` — the name trailer is written when the name is non-empty **or** the mode is transient; the persistence byte is written **only** when transient (value 1). Accepted tails after the descriptor: `0`, `3`, `3+2+len`, `3+2+len+1` (that last byte must be 1).

- [ ] **Step 1: Write the failing tests** — append to `libs/node/tests/node/SchemaRegistryTest.cpp`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E: TopicSchema records the persistence mode so boot recovery can
// recreate a runtime topic as TRANSIENT.  Disk records stay byte-identical.
// ---------------------------------------------------------------------------
TEST(SchemaRegistryRegression, TransientPersistenceTrailerRoundTrips) {
    TopicSchema ts = makeSchema(0x40000020, "desc", false);
    ts.topicName   = "presence";
    ts.persistence = PersistenceMode::Transient;
    auto back = TopicSchema::deserialize(ts.serialize());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->persistence, PersistenceMode::Transient);
    EXPECT_EQ(back->topicName, "presence");
}

TEST(SchemaRegistryRegression, DiskRecordIsByteIdenticalToPrePersistenceLayout) {
    TopicSchema ts = makeSchema(0x40000021, "desc", false);
    ts.topicName = "orders";
    const auto disk = ts.serialize();               // persistence defaults to Disk
    ts.persistence = PersistenceMode::Transient;
    const auto transientBytes = ts.serialize();
    ASSERT_EQ(transientBytes.size(), disk.size() + 1);
    EXPECT_TRUE(std::equal(disk.begin(), disk.end(), transientBytes.begin()));
    EXPECT_EQ(transientBytes.back(), 1u);
    auto back = TopicSchema::deserialize(disk);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->persistence, PersistenceMode::Disk);
}

// A transient record with NO topic name still carries the mode (empty name
// trailer + the byte), and a pre-name record (3-byte tail) parses as Disk.
TEST(SchemaRegistryRegression, TransientWithoutNameAndPreNameRecords) {
    TopicSchema ts = makeSchema(0x40000022, "desc", false);
    ts.persistence = PersistenceMode::Transient;
    auto back = TopicSchema::deserialize(ts.serialize());
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->persistence, PersistenceMode::Transient);
    EXPECT_TRUE(back->topicName.empty());

    TopicSchema pre = makeSchema(0x40000023, "desc", false);
    auto preBack = TopicSchema::deserialize(pre.serialize());
    ASSERT_TRUE(preBack.has_value());
    EXPECT_EQ(preBack->persistence, PersistenceMode::Disk);
}

TEST(SchemaRegistryRegression, UnknownPersistenceByteIsRejected) {
    TopicSchema ts = makeSchema(0x40000024, "desc", false);
    ts.topicName   = "p";
    ts.persistence = PersistenceMode::Transient;
    auto bytes = ts.serialize();
    bytes.back() = 2;                                // append-only: never cast
    EXPECT_FALSE(TopicSchema::deserialize(bytes).has_value());
    bytes.back() = 0;                                // a writer never emits an explicit 0
    EXPECT_FALSE(TopicSchema::deserialize(bytes).has_value());
    bytes.back() = 1;
    bytes.push_back(1);                              // two bytes after the name: malformed
    EXPECT_FALSE(TopicSchema::deserialize(bytes).has_value());
}

TEST(SchemaRegistryRegression, ApplyRecordCarriesPersistenceToEntry) {
    SchemaRegistry reg;
    TopicSchema ts = makeSchema(0x40000025, "desc", false);
    ts.persistence = PersistenceMode::Transient;
    reg.applyRecord(ts);
    ASSERT_NE(reg.byId(0x40000025), nullptr);
    EXPECT_EQ(reg.byId(0x40000025)->persistence, PersistenceMode::Transient);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_node_tests`
Expected: compile error `no member named 'persistence' in 'rune::node::TopicSchema'`.

- [ ] **Step 3: Implement**

`SchemaRegistry.h` — add `#include <rune/node/PersistenceMode.h>` with the other includes; in `struct TopicSchema` after `std::string topicName;`:

```cpp
    // Phase 2b E: the topic's persistence mode.  Optional trailing byte AFTER the
    // topicName trailer, written only when Transient (a disk record is
    // byte-identical to the pre-2b layout).  Boot recovery recreates a runtime
    // topic as transient from it (RuntimeTopicRecovery); ALTER carries it forward.
    PersistenceMode       persistence    = PersistenceMode::Disk;
```

and in `struct RegistryEntry` after `std::string topicName;`:

```cpp
    PersistenceMode                  persistence    = PersistenceMode::Disk;   // Phase 2b E
```

`SchemaRegistry.cpp` — replace the topicName-trailer block at the end of `serialize()`:

```cpp
    // [restart recovery] optional topicName trailer: u16 length + bytes, written
    // when a name is set OR the topic is transient (the persistence byte rides
    // after it), so a nameless disk record stays byte-identical to the pre-name
    // layout (exact 3-byte tail).
    const bool transient = (persistence == PersistenceMode::Transient);
    if (!topicName.empty() || transient) {
        writeU16LE(buf, static_cast<uint16_t>(topicName.size()));
        for (unsigned char c : topicName) buf.push_back(c);
        // [Phase 2b E] persistence byte — only for a transient topic.
        if (transient) writeU8(buf, persistenceToWire(persistence));
    }
```

In `deserialize()` replace the `if (remaining >= 5) { … }` name block:

```cpp
        // [restart recovery] optional topicName trailer, then [Phase 2b E] an
        // optional persistence byte.  Accepted only as EXACTLY the name bytes
        // (disk) or the name bytes + ONE byte equal to 1 (transient) — any other
        // tail is malformed, same as every other check here.
        if (remaining >= 5) {
            uint16_t nameLen = 0;
            if (!readU16LE(buf, pos, nameLen))          return std::nullopt;
            const size_t tail = buf.size() - pos;
            if (tail != nameLen && tail != static_cast<size_t>(nameLen) + 1) return std::nullopt;
            ts.topicName.assign(reinterpret_cast<const char*>(buf.data() + pos), nameLen);
            pos += nameLen;
            if (tail == static_cast<size_t>(nameLen) + 1) {
                uint8_t p = 0;
                if (!readU8(buf, pos, p))                return std::nullopt;
                if (p != persistenceToWire(PersistenceMode::Transient)) return std::nullopt;
                ts.persistence = PersistenceMode::Transient;
            }
        }
```

Also update the layout comment above `TopicSchema::serialize` to append the line `//         [nextTag:u16, compatMode:u8 [, nameLen:u16 + name [, persistence:u8 (=1)]]]`.

In `SchemaRegistry::applyRecord`, after `e.topicName      = ts.topicName;` add:

```cpp
    e.persistence    = ts.persistence;
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j --target rune_node_tests && ctest --test-dir build-fast -R 'SchemaRegistry' --output-on-failure`
Expected: all PASS, including the unchanged `TruncatedTopicNameTrailerIsRejected` (its "extra 0x7F" case is still rejected: the byte is not 1).

- [ ] **Step 5: Commit** (after the broker Regression gate)

```bash
git add libs/node/include/rune/node/SchemaRegistry.h libs/node/src/SchemaRegistry.cpp libs/node/tests/node/SchemaRegistryTest.cpp
git commit -m "feat(schema): TopicSchema optional persistence trailer byte (transient only)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Broker — DefineTopic persistence end to end (gateway → admin → control), ALTER carry-forward

**Files:**
- Modify: `libs/gateway/include/rune/gateway/IAdminRequestSink.h:30-36`
- Modify: `libs/gateway/src/ClientGateway.cpp:5661-5664`
- Modify: `libs/node/include/rune/node/AdminChannels.h` (`DefineTopicMsg` L104-112, `encodeDefineTopic`/`decodeDefineTopic` L202-240, `CreateTopicReqMsg` L166-172, `encodeCreateTopicReq`/`decodeCreateTopicReq` L367-404)
- Modify: `libs/node/include/rune/node/AdminWorker.h:50-55` (`requestCreateTopic` signature)
- Modify: `libs/node/src/AdminWorker.cpp` (`onCreateTopicReply` ~L200-214, `startNext_` L395-396, `startCreateView_` L499-500, `startAlter_` L584-592, `advance_` CREATE commit L694-703)
- Modify: `libs/node/include/rune/node/BrokerNode.h:217-232` (`submitDefineTopic`)
- Modify: `libs/node/src/BrokerNode.cpp:3273-3285` (deps lambda), `:3855-3866` (control drain)
- Modify (signature ripple, `uint8_t` 6th param): `libs/node/tests/integration/SchemaGatewayTest.cpp:149,215`, `GetViewRowRegressionTest.cpp:435`, `SchemaPersistenceTest.cpp:237,496,630,740,1057,1911`, `CreateViewRegressionTest.cpp:143`, `libs/node/tests/node/AdminWorkerTest.cpp:91`
- Test: `libs/node/tests/node/AdminWorkerTest.cpp`, `libs/node/tests/integration/SchemaGatewayTest.cpp`

**Interfaces:**
- Consumes: protocol `DefineTopic::persistence` (Task 2), `PersistenceMode` + `persistenceFromWire` (Task 3), `TopicSchema::persistence` / `RegistryEntry::persistence` (Task 4).
- Produces:
  - `IAdminRequestSink::submitDefineTopic(int64_t sessionId, int64_t correlationId, std::string_view name, uint32_t partitionCount, std::string_view keyField, bool validate, const uint8_t* descriptorBlob, uint32_t descriptorLen, uint8_t persistence)`
  - `DefineTopicMsg::persistence` (`uint8_t`, 0/1) — ring frame appends it after the blob; decode tolerates absence (⇒ 0).
  - `CreateTopicReqMsg::persistence` (`uint8_t`) — appended after `cleanupPolicy`; tolerant decode.
  - `AdminDeps::requestCreateTopic` = `std::function<bool(int64_t ticket, int32_t partitionCount, const std::string& name, const std::string& keyField, uint8_t cleanupPolicy, uint8_t persistence)>`

- [ ] **Step 1: Write the failing tests**

In `libs/node/tests/node/AdminWorkerTest.cpp`: add `#include <rune/node/AdminChannels.h>` to the includes; in `struct Harness` add the member `uint8_t lastCreatePersistence = 0xff;`; change the `requestCreateTopic` lambda to

```cpp
        d.requestCreateTopic = [this](int64_t ticket, int32_t, const std::string&,
                                      const std::string&, uint8_t, uint8_t persistence) -> bool {
            ++creates; pendingTicket = ticket; lastCreatePersistence = persistence; return true;
        };
```

and give `runDefine` a trailing parameter `uint8_t persistence = 0` that sets `m.persistence = persistence;`. Then append:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E: transient topics through the DDL state machine.
// ---------------------------------------------------------------------------
TEST(AdminWorkerRegression, TransientDefineRequestsTransientTopicAndRecordsMode) {
    Harness h;
    h.runDefine(1, 2, "characters", "userId", 4, false, charsBlob(), /*persistence=*/1);
    ASSERT_EQ(h.lastErr, 0);
    EXPECT_EQ(h.lastCreatePersistence, 1u) << "control thread must be asked for a transient topic";
    const RegistryEntry* e = h.reg.byName("characters");
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->persistence, PersistenceMode::Transient);

    // The DURABLE record (what boot recovery reads) carries the mode too.
    const std::vector<uint8_t> raw = h.log.read(0, 1 << 20);
    ASSERT_FALSE(raw.empty());
    rune::protocol::RecordBatchReader rd(raw.data(), raw.size());
    ASSERT_TRUE(rd.ensureValid());
    std::optional<TopicSchema> rec;
    rd.forEachRecord([&](std::string_view, std::string_view v, int64_t, int64_t) {
        rec = TopicSchema::deserialize(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(v.data()), v.size()));
    });
    ASSERT_TRUE(rec.has_value());
    EXPECT_EQ(rec->persistence, PersistenceMode::Transient);
}

TEST(AdminWorkerRegression, DiskDefineRequestsDiskTopic) {
    Harness h;
    h.runDefine(1, 2, "characters", "userId", 4, false, charsBlob());
    ASSERT_EQ(h.lastErr, 0);
    EXPECT_EQ(h.lastCreatePersistence, 0u);
    EXPECT_EQ(h.reg.byName("characters")->persistence, PersistenceMode::Disk);
}

// ALTER writes a NEW TopicSchema version — the one recovery reads.  It must
// carry the mode forward or a restart would recreate the topic on disk.
TEST(AdminWorkerRegression, AlterCarriesTransientPersistenceForward) {
    Harness h;
    h.runDefine(1, 2, "characters", "userId", 4, false, charsBlob(), /*persistence=*/1);
    ASSERT_EQ(h.lastErr, 0);
    h.runAlter(1, 3, "characters", /*expectedVersion=*/1, opsBlob({addOp("email", PrimType::String)}));
    ASSERT_EQ(h.lastAlterErr, 0);
    ASSERT_EQ(h.lastAlterVersion, 2u);
    const RegistryEntry* v2 = h.reg.byName("characters");
    ASSERT_NE(v2, nullptr);
    EXPECT_EQ(v2->version, 2u);
    EXPECT_EQ(v2->persistence, PersistenceMode::Transient);
}

// Re-defining an existing name with the OTHER persistence is TopicExists and
// never flips the mode (no second create, registry unchanged).
TEST(AdminWorkerRegression, RedefineExistingTopicWithOtherPersistenceIsTopicExists) {
    Harness h;
    h.runDefine(1, 2, "characters", "userId", 4, false, charsBlob(), /*persistence=*/0);
    ASSERT_EQ(h.lastErr, 0);
    h.lastErr = 0xffff;
    h.runDefine(1, 3, "characters", "userId", 4, false, charsBlob(), /*persistence=*/1);
    EXPECT_EQ(h.lastErr, 25) << "TopicExists";
    EXPECT_EQ(h.creates, 1);
    EXPECT_EQ(h.reg.byName("characters")->persistence, PersistenceMode::Disk);
}

// The in-process ring frames carry the byte; a frame without it decodes as disk.
TEST(AdminWorkerRegression, AdminRingFramesCarryPersistence) {
    OneToOneByteRing ring{kAdminRequestRingBytes};
    DefineTopicMsg m; m.session = 7; m.corr = 8; m.name = "p"; m.blob = {1, 2}; m.persistence = 1;
    ASSERT_TRUE(encodeDefineTopic(ring, m));
    DefineTopicMsg got;
    std::size_t frameLen = 0;
    ring.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                   aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        got = decodeDefineTopic(p, static_cast<std::size_t>(len));
        frameLen = static_cast<std::size_t>(len);
        // Same frame minus the trailing persistence byte = a pre-2b frame → disk.
        const DefineTopicMsg legacy = decodeDefineTopic(p, frameLen - 1);
        EXPECT_EQ(legacy.persistence, 0u);
        EXPECT_EQ(legacy.blob, (std::vector<uint8_t>{1, 2}));
    }, 1);
    EXPECT_EQ(got.persistence, 1u);
    EXPECT_EQ(got.blob, (std::vector<uint8_t>{1, 2}));

    OneToOneByteRing creq{kCreateTopicRingBytes};
    CreateTopicReqMsg c; c.ticket = 3; c.partitionCount = 2; c.name = "p"; c.persistence = 1;
    ASSERT_TRUE(encodeCreateTopicReq(creq, c));
    CreateTopicReqMsg cgot;
    creq.drain([&](int32_t, aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                   aeron::util::index_t len) {
        const auto* p = reinterpret_cast<const uint8_t*>(b.buffer() + off);
        cgot = decodeCreateTopicReq(p, static_cast<std::size_t>(len));
        EXPECT_EQ(decodeCreateTopicReq(p, static_cast<std::size_t>(len) - 1).persistence, 0u);
    }, 1);
    EXPECT_EQ(cgot.persistence, 1u);
    EXPECT_EQ(cgot.cleanupPolicy, 0u);
}
```

(`AdminWorkerTest.cpp` also needs `#include <rune/protocol/RecordBatch.h>` and `#include <optional>`.)

In `libs/node/tests/integration/SchemaGatewayTest.cpp`: the `SinkAdapter::submitDefineTopic` override gains `uint8_t persistence` (sets `m.persistence = persistence;`); the harness `requestCreateTopic` lambda gains `uint8_t persistence` and sets `m.persistence = persistence;`; add `std::optional<CreateTopicReqMsg> lastCreateReq;` to `SgHarness` and in `serviceControl()` after decoding `req` add `lastCreateReq = req;`. `encodeDefineFrame` gains a trailing `uint8_t persistence = 0` assigned to `m.persistence`. Append:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E: the wire persistence byte reaches the control thread's
// create-topic request and the durable registry entry — and an OLD client's
// frame (no byte) still creates a disk topic.
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, DefineTopicTransientByteReachesControlAndRegistry) {
    SgHarness h;
    auto blob = charsBlob();
    auto frame = encodeDefineFrame(1, 5, "characters", 2, "userId", false, blob,
                                   pc::DefineTopic::kPersistenceTransient);
    h.gw.applyDefineTopicForTest(frame.data(), frame.size());
    h.pump();
    ASSERT_TRUE(h.lastCreateReq.has_value());
    EXPECT_EQ(h.lastCreateReq->persistence, 1u);
    ASSERT_NE(h.schemaReg.byName("characters"), nullptr);
    EXPECT_EQ(h.schemaReg.byName("characters")->persistence, PersistenceMode::Transient);
}

TEST(SchemaGatewayRegression, DefineTopicWithoutPersistenceByteCreatesDiskTopic) {
    SgHarness h;
    auto blob = charsBlob();
    auto frame = encodeDefineFrame(1, 6, "characters", 2, "userId", false, blob);   // old client
    ASSERT_EQ(frame.size(), pc::MessageHeader::ENCODED_SIZE + pc::DefineTopic::BODY_FIXED
                                + 2 + 10 + 2 + 6 + 4 + blob.size())
        << "precondition: a disk frame carries no persistence byte";
    h.gw.applyDefineTopicForTest(frame.data(), frame.size());
    h.pump();
    ASSERT_TRUE(h.lastCreateReq.has_value());
    EXPECT_EQ(h.lastCreateReq->persistence, 0u);
    EXPECT_EQ(h.schemaReg.byName("characters")->persistence, PersistenceMode::Disk);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_node_tests rune_schema_gateway_tests`
Expected: compile errors (`no member named 'persistence' in 'rune::node::DefineTopicMsg'`, lambda arity mismatch for `requestCreateTopic`).

- [ ] **Step 3: Implement**

`IAdminRequestSink.h` — the pure virtual becomes:

```cpp
    // persistence (Phase 2b E): DefineTopic's optional trailing byte — 0 = disk
    // (also what an old client's frame decodes to), 1 = transient.
    virtual bool submitDefineTopic(int64_t sessionId, int64_t correlationId,
                                   std::string_view name,
                                   uint32_t partitionCount,
                                   std::string_view keyField,
                                   bool validate,
                                   const uint8_t* descriptorBlob,
                                   uint32_t descriptorLen,
                                   uint8_t persistence) = 0;
```

`ClientGateway.cpp:5661-5664`:

```cpp
    (void)adminSink_->submitDefineTopic(m.sessionId, m.correlationId,
                                        m.topicName, m.partitionCount,
                                        m.keyField, m.validate,
                                        m.descriptorBlob, m.descriptorLen,
                                        m.persistence);
```

`AdminChannels.h` — `DefineTopicMsg` gains (last member) `uint8_t persistence = 0;   // Phase 2b E: 0 = disk, 1 = transient`. `encodeDefineTopic`: the length expression gains `+ 1`, and after the blob copy:

```cpp
    std::memcpy(p, m.blob.data(), m.blob.size()); p += m.blob.size();
    *p = m.persistence;   // Phase 2b E (trailing; decode tolerates its absence)
    ring.commit(idx);
    return true;
```

`decodeDefineTopic` tail (replacing the last 4 lines):

```cpp
    if (pos + 4 > len) return m;
    const int32_t bl = getI32(buf + pos); pos += 4;
    if (bl < 0 || pos + static_cast<std::size_t>(bl) > len) return m;
    if (bl > 0) m.blob.assign(buf + pos, buf + pos + bl);
    pos += static_cast<std::size_t>(bl);
    if (pos < len) m.persistence = buf[pos];   // Phase 2b E; absent ⇒ disk
    return m;
```

`CreateTopicReqMsg` gains (last member) `uint8_t persistence = 0;   // Phase 2b E: 0 = disk, 1 = transient (InMemoryLog)`. `encodeCreateTopicReq`: length `+ 1 + 1` instead of `+ 1`, and `*p++ = m.cleanupPolicy; *p = m.persistence;`. `decodeCreateTopicReq` tail:

```cpp
    if (pos + 1 > len) return m;
    m.cleanupPolicy = buf[pos++];
    if (pos < len) m.persistence = buf[pos];   // Phase 2b E; absent ⇒ disk
    return m;
```

`AdminWorker.h` — the `requestCreateTopic` member:

```cpp
    // Phase 2b E: `persistence` (0 = disk, 1 = transient) selects the topic's log
    // backend on the control thread.  A view sink is always disk (0).
    std::function<bool(int64_t ticket, int32_t partitionCount,
                       const std::string& name, const std::string& keyField,
                       uint8_t cleanupPolicy, uint8_t persistence)> requestCreateTopic;
```

`AdminWorker.cpp`:
- `onCreateTopicReply` after `ts.topicName = inFlight_->msg.name;` add `ts.persistence = persistenceFromWire(inFlight_->msg.persistence);`
- `startNext_` call becomes `deps_.requestCreateTopic(op.ticket, op.msg.partitionCount, op.msg.name, /*keyField=*/"", /*cleanupPolicy=*/0, op.msg.persistence)`
- `startCreateView_` call becomes `deps_.requestCreateTopic(op.ticket, sinkPartitions, op.view.viewName, op.view.keyField, /*cleanupPolicy=*/1 /*Compact*/, /*persistence=*/0 /*a view sink is durable*/)`
- `startAlter_` after `ts.topicName = latest->topicName;` add `ts.persistence = latest->persistence;              // carried forward unchanged (Phase 2b E)`
- `advance_` CREATE commit after `ts.topicName = inFlight_->msg.name;` add `ts.persistence = persistenceFromWire(inFlight_->msg.persistence);   // same as the durable record`

`BrokerNode.h` `AdminWorkerAgent::submitDefineTopic` override gains `uint8_t persistence` after `descriptorLen` and sets `m.persistence = persistence;` before `encodeDefineTopic`.

`BrokerNode.cpp` deps lambda (~L3273):

```cpp
        deps.requestCreateTopic = [this](int64_t ticket, int32_t partitionCount,
                                         const std::string& name,
                                         const std::string& keyField,
                                         uint8_t cleanupPolicy,
                                         uint8_t persistence) -> bool {
            if (!adminWorkerAgent_) return false;
            CreateTopicReqMsg m;
            m.ticket         = ticket;
            m.partitionCount = partitionCount;
            m.name           = name;
            m.keyField       = keyField;         // Task 19: view sink key field
            m.cleanupPolicy  = cleanupPolicy;    // Task 19: Compact for a view sink table
            m.persistence    = persistence;      // Phase 2b E: 1 = transient (InMemoryLog)
            return encodeCreateTopicReq(adminWorkerAgent_->createReqRing(), m);
        };
```

Control drain (~L3863, after `if (!req.keyField.empty()) spec.partitionKey = req.keyField;`):

```cpp
                // Phase 2b E: a transient runtime topic is created exactly like a
                // static persistence=transient one — TopicRegistry skips its disk
                // dirs and createRuntimeUserReplicas builds it on an InMemoryLog.
                spec.persistence    = rune::node::persistenceFromWire(req.persistence);
```

Ripple — every other `requestCreateTopic` lambda gets a 6th `uint8_t` parameter (unnamed where unused): `SchemaPersistenceTest.cpp` lines 237 (`uint8_t /*cleanupPolicy*/, uint8_t /*persistence*/`), 496, 630, 740, 1057, 1911; `CreateViewRegressionTest.cpp:143`; `GetViewRowRegressionTest.cpp:435`. Verify: `grep -rn "requestCreateTopic *=" libs | wc -l` → 11, and the build succeeds.

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'AdminWorker|SchemaGateway|SchemaPersistence|CreateView|GetViewRow' --output-on-failure`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/gateway/include/rune/gateway/IAdminRequestSink.h libs/gateway/src/ClientGateway.cpp libs/node/include/rune/node/AdminChannels.h libs/node/include/rune/node/AdminWorker.h libs/node/src/AdminWorker.cpp libs/node/include/rune/node/BrokerNode.h libs/node/src/BrokerNode.cpp libs/node/tests/node/AdminWorkerTest.cpp libs/node/tests/integration/SchemaGatewayTest.cpp libs/node/tests/integration/SchemaPersistenceTest.cpp libs/node/tests/integration/CreateViewRegressionTest.cpp libs/node/tests/integration/GetViewRowRegressionTest.cpp
git commit -m "feat(admin): DefineTopic persistence byte creates transient runtime topics; ALTER carries it

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Broker — restart recovery recreates transient topics; live single-node gate; acks=All gate

**Files:**
- Modify: `libs/node/src/RuntimeTopicRecovery.cpp:80-132` (`DefinedTopic`, spec build)
- Modify: `libs/node/tests/support/ShardTestClient.h:256-276` (`defineTopicUntil`)
- Test: `libs/node/tests/node/RuntimeTopicRecoveryTest.cpp`, `libs/node/tests/node/BrokerNodeTest.cpp`, `libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp`

**Interfaces:**
- Consumes: `TopicSchema::persistence` (Task 4); runtime transient creation (Task 5).
- Produces: `recoverRuntimeTopicSpecs` returns `TopicSpec.persistence` from the **latest** schema version; `ShardTestClient::defineTopicUntil(..., Clock::time_point deadline, uint8_t persistence = 0)`.

- [ ] **Step 1: Write the failing tests**

`RuntimeTopicRecoveryTest.cpp` — give `schemaRecord` a trailing parameter `rune::node::PersistenceMode persistence = rune::node::PersistenceMode::Disk` assigned to `ts.persistence`, then append:

```cpp
// Phase 2b E: a transient runtime topic is recreated TRANSIENT (metadata
// survives, data does not — the TopicRegistry/InMemoryLog path starts it empty).
TEST(RuntimeTopicRecoveryRegression, TransientTopicIsRecoveredTransient) {
    TempDataDir d("transient");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 2, "presence", "presence_desc",
                                rune::node::PersistenceMode::Transient),
                   schemaRecord(1, 1, 1, "orders", "orders_desc")});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 2u);
    EXPECT_EQ(specs[0].name, "presence");
    EXPECT_EQ(specs[0].persistence, rune::node::PersistenceMode::Transient);
    EXPECT_EQ(specs[1].persistence, rune::node::PersistenceMode::Disk);
}

TEST(RuntimeTopicRecoveryRegression, AlteredTransientTopicStaysTransient) {
    TempDataDir d("transient-altered");
    appendRecords(d.path / "__topic_schemas",
                  {schemaRecord(0, 1, 1, "presence", "d1", rune::node::PersistenceMode::Transient),
                   schemaRecord(0, 2, 1, "presence", "d2", rune::node::PersistenceMode::Transient)});
    const auto specs = rune::node::recoverRuntimeTopicSpecs(d.path.string(), 0);
    ASSERT_EQ(specs.size(), 1u);
    EXPECT_EQ(specs[0].persistence, rune::node::PersistenceMode::Transient);
}
```

`ShardTestClient.h` — `defineTopicUntil` gains a trailing `uint8_t persistence = 0` and sets `m.persistence = persistence;` right after `m.validate = validate;`.

`BrokerNodeTest.cpp` — append (inside the file's `rune::node` namespace, next to `DefineTopicValidatorReachesEveryWorkerEndToEndRegression`):

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E (spec gate): a runtime DefineTopic(persistence=transient) creates
// the topic on an InMemoryLog (no disk partition dir), and after a node restart
// the topic EXISTS again as a transient topic and is EMPTY (data never on disk).
// ---------------------------------------------------------------------------
TEST(BrokerNodeRegression, RuntimeTransientTopicIsInMemoryAndRestartsEmptyRegression) {
    namespace pc = rune::protocol::client;
    using Clock = std::chrono::steady_clock;
    using rune::replication::PartitionReplica;
    const fs::path dir = makeTempDataDir("transient_rt_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    const uint16_t clientPort = rune::node::test::freeUdpPort();
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(clientPort);
    const std::string& gw = cfg.clientEndpoint.channel;
    const int32_t stream  = cfg.clientEndpoint.streamId;

    const auto desc = rune::codec::DescriptorBuilder("presence")
        .add("userId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("online", rune::codec::PrimType::Bool)
        .build();
    const std::vector<uint8_t> blob = rune::codec::serialize(desc);

    auto awaitLeaderReplica = [](BrokerNode& n, int32_t topicId, Clock::time_point deadline) {
        while (Clock::now() < deadline) {
            if (rune::node::test::hasReplica(n, topicId, 0) &&
                rune::node::test::roleOnOwner(n, topicId, 0) == rune::replication::Role::Leader)
                return true;
            std::this_thread::sleep_for(5ms);   // poll interval, not correctness
        }
        return false;
    };

    int32_t topicId = -1;
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        rune::node::test::ShardTestClient client(dir / "client-aeron");
        const int64_t sid = client.connect(gw, stream);
        ASSERT_GT(sid, 0);

        const auto deadline = Clock::now() + 20s;
        while (topicId < 0) {
            ASSERT_LT(Clock::now(), deadline) << "DefineTopic did not commit within the deadline";
            auto def = client.defineTopicUntil(gw, stream, sid, "presence", 1, /*validate=*/false,
                                               blob, std::min(deadline, Clock::now() + 5s),
                                               pc::DefineTopic::kPersistenceTransient);
            ASSERT_TRUE(def.has_value()) << "no DefineTopicResponse";
            if (def->errorCode == 0 ||
                (def->errorCode == static_cast<uint16_t>(pc::ErrorCode::TopicExists) && def->topicId >= 0)) {
                topicId = def->topicId;
                break;
            }
            ASSERT_EQ(def->errorCode, static_cast<uint16_t>(pc::ErrorCode::Internal))
                << "non-retryable DefineTopic error " << def->errorCode;
            std::this_thread::sleep_for(20ms);   // admin leadership settling (short backoff)
        }
        ASSERT_TRUE(awaitLeaderReplica(node, topicId, Clock::now() + 10s));

        const auto [mem, disk] = node.onOwnerThreadForTest(topicId, 0,
            [](PartitionReplica& r) { return std::pair{r.isMemoryBacked(), r.isDiskBacked()}; });
        EXPECT_TRUE(mem) << "a transient runtime topic must be InMemoryLog-backed";
        EXPECT_FALSE(disk);
        EXPECT_FALSE(fs::exists(dir / "topics" / "presence"))
            << "a transient topic must never create a disk partition directory";

        const std::vector<uint8_t> rec(24, 0x5A);
        ASSERT_GE(rune::node::test::appendOnOwner(node, rec, /*acksAll=*/false, topicId, 0), 0);
        ASSERT_GT(rune::node::test::leoOnOwner(node, topicId, 0), 0) << "data present before restart";
        node.stop();
    }
    {
        BrokerNode node{cfg};
        node.start();
        ASSERT_TRUE(node.awaitLeader(30s));
        ASSERT_TRUE(awaitLeaderReplica(node, topicId, Clock::now() + 20s))
            << "the transient topic's metadata must survive the restart (recovered from __topic_schemas)";
        const auto [mem, leo] = node.onOwnerThreadForTest(topicId, 0,
            [](PartitionReplica& r) { return std::pair{r.isMemoryBacked(), r.logEndOffset()}; });
        EXPECT_TRUE(mem) << "recovered as TRANSIENT, not as a disk topic";
        EXPECT_EQ(leo, 0) << "a transient topic restarts EMPTY — its data was never on disk";
        EXPECT_FALSE(fs::exists(dir / "topics" / "presence"));
        node.stop();
    }
    std::error_code ec; fs::remove_all(dir, ec);
}
```

`RebalanceFailoverIntegrationTest.cpp` — append (spec gate "replicated before ack under acks=All"; runtime topics are single-node in 2b, so this is proven where the decision is made — the transient replica's HW gate):

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E (spec gate): a TRANSIENT (InMemoryLog) partition under acks=All is
// replicated BEFORE ack — the leader's HW (the ack gate) does not move past
// what every in-sync follower holds.
// ---------------------------------------------------------------------------
TEST(RebalanceFailoverRegression, TransientTopicAcksAllReplicatesBeforeAck) {
    constexpr int32_t kTopic = 150, kPart = 0;
    const std::vector<NodeId> all = {1, 2, 3};
    rune::storage::InMemoryLog mem1(1 << 20, 4096), mem2(1 << 20, 4096), mem3(1 << 20, 4096);
    InMemoryTransport transport;
    auto cfg = [&](NodeId s) { return userCfg(s, kTopic, kPart, 3, DurabilityMode::AllInSync); };
    PartitionReplica r1(cfg(1), Role::Leader,   1, all, all, &mem1, &transport);
    PartitionReplica r2(cfg(2), Role::Follower, 1, all, all, &mem2, &transport);
    PartitionReplica r3(cfg(3), Role::Follower, 1, all, all, &mem3, &transport);
    transport.setLeader(&r1); transport.addFollower(2, &r2); transport.addFollower(3, &r3);
    ASSERT_TRUE(r1.isMemoryBacked());

    auto p = payload(0x77);
    ASSERT_GE(r1.append(std::span<const uint8_t>(p.data(), p.size()), /*acksAll=*/true), 0);
    const Offset leo = r1.logEndOffset();
    EXPECT_LT(r1.highWatermark(), leo) << "no ack before any follower has the record";

    int64_t clock = 0;
    for (int i = 0; i < kDriverCap; ++i) {
        clock += 1'000'000LL;
        r1.doWork(clock); r2.doWork(clock); r3.doWork(clock);
        transport.pump(clock);
        // Invariant at EVERY step: the ack gate never runs ahead of the slowest ISR member.
        ASSERT_LE(r1.highWatermark(), std::min(r2.logEndOffset(), r3.logEndOffset()))
            << "HW (ack) advanced past a follower that does not hold the record";
        if (r1.highWatermark() >= leo) break;
    }
    EXPECT_EQ(r1.highWatermark(), leo) << "stalled: all followers must catch up and HW reach LEO";
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_node_tests rune_broker_node_tests rune_rebalance_failover_tests && ctest --test-dir build-fast -R 'TransientTopicIsRecoveredTransient|AlteredTransientTopicStaysTransient|RuntimeTransientTopicIsInMemory' --output-on-failure`
Expected: the two recovery tests FAIL (`persistence` is Disk), the BrokerNode test FAILS at "recovered as TRANSIENT" (or earlier at `isMemoryBacked` if Task 5 regressed). `TransientTopicAcksAllReplicatesBeforeAck` should already PASS (it pins existing behaviour as a gate).

- [ ] **Step 3: Implement** — `RuntimeTopicRecovery.cpp`:

`struct DefinedTopic` gains `PersistenceMode persistence = PersistenceMode::Disk;   // latest version's (Phase 2b E)`. In the `__topic_schemas` callback after `t.partitionCount = ts->partitionCount;` add `t.persistence    = ts->persistence;`. In the spec build:

```cpp
        TopicSpec spec;
        spec.topicId        = id;
        spec.name           = t.name;
        spec.partitionCount = t.partitionCount;
        // Phase 2b E: a transient topic comes back TRANSIENT — TopicRegistry skips
        // its disk dirs and buildReplication puts it on a fresh (empty) InMemoryLog.
        spec.persistence    = t.persistence;
        // AdminWorker DefineTopic → requestCreateTopic(name, partitions, keyField="",
        // cleanupPolicy=Delete, persistence): defaults already match otherwise.
        runtime.emplace(id, std::move(spec));
```

Also add one sentence to `RuntimeTopicRecovery.h`'s header comment after "recreates them like static topics.": `A transient topic (TopicSchema persistence trailer) is recreated transient: metadata survives, data does not.`

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'RuntimeTopicRecovery|RuntimeTransientTopic|TransientTopic' --output-on-failure`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/node/src/RuntimeTopicRecovery.cpp libs/node/include/rune/node/RuntimeTopicRecovery.h libs/node/tests/support/ShardTestClient.h libs/node/tests/node/RuntimeTopicRecoveryTest.cpp libs/node/tests/node/BrokerNodeTest.cpp libs/node/tests/integration/RebalanceFailoverIntegrationTest.cpp
git commit -m "feat(recovery): runtime transient topics restart as transient (empty); acks=All gate

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Broker — views fold over transient sources

Today `Worker::driveViews_` skips every non-disk source (`if (!replica->isDiskBacked()) continue;  // Phase 1`, `Worker.h:5156`). Consequences the spec's "views over transient topics: allowed" does not survive: a view over a transient topic never folds, and a **recovery** view over one never reaches Ready — which keeps `allViewsReady()` false and gates the node's election eligibility forever. A transient source also reaches two cursor states a disk log never produces; each gets one explicit resolution:

| state | condition (checked each pass, transient source only) | resolution |
|---|---|---|
| `SOURCE_RESET` | `foldCursor > logEnd` — the source restarted empty and reuses offsets from `logStart` under a checkpoint taken before | keep the rows (spec: "keeps its checkpoint"), `foldCursor = logStart`, `transientSourceResets += 1`, one warn line |
| `EVICTED` | `foldCursor < logStart` — the ring (`DropOldest`) dropped records the view had not folded | `transientRecordsEvicted += logStart - foldCursor`, `foldCursor = logStart` (never a silent stall) |

Re-folding retained records is idempotent under the views' table semantics (Replace and aggregate contributors are keyed by the wire key), and checkpoint replay applies deltas in file order with `recovered = last O`, so a post-reset lower O is restored correctly.

**Files:**
- Modify: `libs/node/include/rune/node/WorkerViewSet.h:114-120` (`ViewFoldCounters`), add `reconcileTransientSource` declaration next to `materializeRecoveryInstancesFor` (~L233)
- Modify: `libs/node/src/WorkerViewSet.cpp` (new function after `materializeRecoveryInstancesFor`)
- Modify: `libs/node/include/rune/node/Worker.h:5147-5183` (`driveViews_`)
- Test: `libs/node/tests/node/WorkerViewSetTest.cpp`, `libs/node/tests/node/WorkerTest.cpp`

**Interfaces:**
- Produces: `int WorkerViewSet::reconcileTransientSource(int32_t srcTopicId, int32_t partition, int64_t logStart, int64_t logEnd);` (returns instances adjusted); `ViewFoldCounters::transientSourceResets`, `ViewFoldCounters::transientRecordsEvicted` (`uint64_t`).

- [ ] **Step 1: Write the failing tests**

Append to `libs/node/tests/node/WorkerViewSetTest.cpp`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E: views over a TRANSIENT source.
// ---------------------------------------------------------------------------
namespace {
std::vector<uint8_t> upsertAt(int64_t base, int64_t key, int32_t hp) {
    rune::protocol::RecordBatchBuilder b(base, 0);
    const auto k = keyBytes(key);
    const auto v = encodeCharacter(key, hp, 1);
    b.append(asSv(k), asSv(v), 0);
    return b.build();
}
} // namespace

// A persisted view over a transient topic after a full restart: the source
// restarts EMPTY and reuses offsets 0.. below the checkpoint cursor.  The rows
// are kept, and every post-restart record must fold (without the reset the
// cursor 2 would silently skip the new record at offset 0).
TEST(WorkerViewSetRegression, TransientSourceRestartedBelowCursorRefoldsFromLogStart) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000031).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-transient-reset");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    const auto before = twoUpsertsKey7();                                    // offsets [0,2)
    auto rr0 = [&](int64_t, int64_t) { return rune::storage::LogReadView{before.data(), before.size(), 0, 2}; };
    vs.driveFolds(5, 0, rr0, 2, 0);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000031, 0, 7, L), nullptr);

    // Source restarted empty: logStart 0, logEnd 0 < cursor 2.
    EXPECT_EQ(vs.reconcileTransientSource(5, 0, /*logStart*/ 0, /*logEnd*/ 0), 1);
    ASSERT_NE(vs.foldCounters(0x40000031, 0), nullptr);
    EXPECT_EQ(vs.foldCounters(0x40000031, 0)->transientSourceResets, 1u);

    const auto after = upsertAt(0, 9, 77);                                   // reused offset 0
    auto rr1 = [&](int64_t, int64_t) { return rune::storage::LogReadView{after.data(), after.size(), 0, 1}; };
    vs.driveFolds(5, 0, rr1, 1, 0);
    const uint8_t* row9 = vs.getRow(0x40000031, 0, 9, L);
    ASSERT_NE(row9, nullptr) << "a post-restart record at a reused offset must fold";
    EXPECT_EQ(rune::view::readSlot(row9, *L, 1).i, 77);
    EXPECT_NE(vs.getRow(0x40000031, 0, 7, L), nullptr) << "pre-restart rows are kept";
    EXPECT_EQ(vs.reconcileTransientSource(5, 0, 0, 1), 0) << "steady state: nothing to adjust";
}

// The ring evicted records the view had not folded yet: skip to logStart and
// count the loss — never stall on an offset the source no longer has.
TEST(WorkerViewSetRegression, TransientSourceEvictionAheadOfCursorSkipsAndCounts) {
    auto src = charSourceV1();
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(hpView(1), srcs, 0x40000032).value();
    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-transient-evict");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    auto empty = [](int64_t, int64_t) { return rune::storage::LogReadView{}; };
    vs.driveFolds(5, 0, empty, 0, 0);                       // instance exists, cursor 0
    ASSERT_NE(vs.foldCounters(0x40000032, 0), nullptr);

    EXPECT_EQ(vs.reconcileTransientSource(5, 0, /*logStart*/ 3, /*logEnd*/ 4), 1);
    EXPECT_EQ(vs.foldCounters(0x40000032, 0)->transientRecordsEvicted, 3u);

    const auto b = upsertAt(3, 11, 5);                      // the surviving record at offset 3
    auto rr = [&](int64_t from, int64_t) {
        EXPECT_EQ(from, 3) << "the fold resumes at logStart, not at the evicted cursor";
        return rune::storage::LogReadView{b.data(), b.size(), 3, 4};
    };
    vs.driveFolds(5, 0, rr, 4, 0);
    const rune::view::RowLayout* L = nullptr;
    ASSERT_NE(vs.getRow(0x40000032, 0, 11, L), nullptr);
}
```

Append to `libs/node/tests/node/WorkerTest.cpp` (it already builds an `InMemoryLog`-backed Leader `ReplicaFixture`); add includes `#include <rune/node/WorkerViewSet.h>`, `#include <rune/node/ViewSnapshotChannel.h>`, `#include <rune/view/PlanCompiler.h>`, `#include <rune/view/ViewRow.h>`, `#include <rune/codec/Descriptor.h>`, `#include <rune/protocol/RecordBatch.h>`, and to `rune_worker_tests`' `target_link_libraries` in `libs/node/CMakeLists.txt` add `rune_view rune_codec`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b E: a view over a TRANSIENT (InMemoryLog) source folds on the real
// worker pass.  Before the fix driveViews_ skipped every non-disk source, so
// the row never appeared (and a recovery view would never become Ready).
// ---------------------------------------------------------------------------
TEST(WorkerRegression, ViewFoldsOverTransientSourceRegression) {
    constexpr int32_t kSrc = 21, kView = 0x40000041;
    ReplicaFixture f(kSrc, 0);
    ASSERT_TRUE(f.replica->isMemoryBacked());

    const auto src = rune::codec::DescriptorBuilder("character", 1)
        .add("characterId", rune::codec::PrimType::Int64, 1, /*isKey*/ true)
        .add("hp", rune::codec::PrimType::Int32)
        .build();
    rune::view::ViewPlan v;
    v.viewName = "char_hp"; v.key = "characterId"; v.sources = {{kSrc, "characterId"}};
    v.output = {{"characterId", rune::codec::PrimType::Int64, 0, "characterId", rune::view::MergeRule::Replace},
                {"hp", rune::codec::PrimType::Int32, 0, "hp", rune::view::MergeRule::Replace}};
    v.persistence = 1;
    const rune::codec::Descriptor* srcs[] = {&src};
    auto comp = rune::view::compileView(v, srcs, kView).value();

    rune::node::ViewSnapshotChannel ch;
    rune::node::WorkerViewSet vs(ch, "/tmp/rune-views-worker-transient");
    vs.installView(comp.plan, comp.rowLayout, src, /*persisted*/ false);

    std::vector<uint8_t> value;
    auto le = [&](uint64_t x, int n) { for (int i = 0; i < n; ++i) value.push_back(uint8_t(x >> (8 * i))); };
    le(12, 2); le(1, 2); le(7, 8); le(55, 4);                 // blockLength, version, id=7, hp=55
    std::vector<uint8_t> key(8, 0); key[0] = 7;
    rune::protocol::RecordBatchBuilder bb(0, 0);
    bb.append(std::string_view(reinterpret_cast<const char*>(key.data()), key.size()),
              std::string_view(reinterpret_cast<const char*>(value.data()), value.size()), 0);
    const auto batch = bb.build();
    ASSERT_GE(f.replica->append(std::span<const uint8_t>(batch.data(), batch.size()), /*acksAll=*/false), 0);

    Worker w(/*workerId=*/0);
    w.setOwnedDirect(Worker::OwnedSet{f.replica.get()});
    w.setViewSet(&vs);
    const rune::view::RowLayout* L = nullptr;
    const uint8_t* row = nullptr;
    for (int i = 0; i < 200 && row == nullptr; ++i) {          // bounded: RC build + HW + fold
        w.doWork(/*nowNs=*/1'000'000LL * (i + 1));
        row = vs.getRow(kView, 0, 7, L);
    }
    ASSERT_NE(row, nullptr) << "the worker must fold a view over a transient source";
    EXPECT_EQ(rune::view::readSlot(row, *L, 1).i, 55);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build-fast -j --target rune_node_tests rune_worker_tests`
Expected: compile error `no member named 'reconcileTransientSource'`; after adding only the declaration the Worker test FAILS with "the worker must fold a view over a transient source".

- [ ] **Step 3: Implement**

`WorkerViewSet.h` — `ViewFoldCounters` gains:

```cpp
    uint64_t transientSourceResets   = 0;   // transient source restarted below the cursor (Phase 2b E)
    uint64_t transientRecordsEvicted = 0;   // transient source ring dropped records not yet folded
```

Declaration (next to `materializeRecoveryInstancesFor`):

```cpp
    // Phase 2b E — TRANSIENT (InMemoryLog) source only; the worker calls it each
    // pass BEFORE driveRebuild/driveFolds for (srcTopicId, partition), with the
    // source's current [logStart, logEnd).  Resolves the two cursor states a disk
    // log never produces, one explicit code path each:
    //   SOURCE_RESET (foldCursor > logEnd): the source restarted empty and reuses
    //     offsets from logStart under an older checkpoint — rows kept, cursor →
    //     logStart, transientSourceResets++ and one warn line.
    //   EVICTED (foldCursor < logStart): the ring dropped records not yet folded —
    //     cursor → logStart, transientRecordsEvicted += the gap.
    // Returns the number of instances adjusted.  No allocation (hot path).
    int reconcileTransientSource(int32_t srcTopicId, int32_t partition,
                                 int64_t logStart, int64_t logEnd);
```

`WorkerViewSet.cpp` — after `materializeRecoveryInstancesFor`:

```cpp
int WorkerViewSet::reconcileTransientSource(int32_t srcTopicId, int32_t partition,
                                            int64_t logStart, int64_t logEnd) {
    auto it = subscriptionIndex_.find(srcTopicId);
    if (it == subscriptionIndex_.end()) return 0;
    int adjusted = 0;
    for (int32_t viewTopicId : it->second) {
        auto vit = views_.find(instKey(viewTopicId, partition));
        if (vit == views_.end()) continue;
        ViewInstance& inst = vit->second;
        if (inst.foldCursor > logEnd) {                       // SOURCE_RESET
            ++inst.counters.transientSourceResets;
            spdlog::warn("WorkerViewSet: view {} partition {}: transient source {} restarted below "
                         "the view's cursor (cursor={} > source end={}) — keeping the rows, "
                         "re-folding from offset {}",
                         viewTopicId, partition, srcTopicId, inst.foldCursor, logEnd, logStart);
            inst.foldCursor = logStart;
            ++adjusted;
        } else if (inst.foldCursor < logStart) {              // EVICTED
            inst.counters.transientRecordsEvicted +=
                static_cast<uint64_t>(logStart - inst.foldCursor);
            inst.foldCursor = logStart;
            ++adjusted;
        }
    }
    return adjusted;
}
```

`Worker.h` — replace `driveViews_` (lines 5147-5183) and add a private helper right after it:

```cpp
    int driveViews_(int64_t nowNs) {
        int work = 0;
        for (auto* replica : owned_) {
            if (replica == nullptr || replica->topicId() < 0) continue;
            const int32_t topicId = replica->topicId();
            // Short-circuit: skip the rc/bound computation for an owned
            // partition no installed view is interested in.
            if (!viewSet_->interestedIn(topicId)) continue;
            if (replica->role() != rune::replication::Role::Leader) continue;
            const RcState* rcState = rcServe_(replica, rcOf_(replica));
            if (rcState == nullptr) continue;         // RC not Ready: fail closed, no bound
            const int64_t liveHw = replica->highWatermark();
            const int64_t bound  = rcState->lso(liveHw);
            const int32_t partition = static_cast<int32_t>(replica->partitionId());
            if (replica->isDiskBacked()) {
                rune::storage::Log* log = replica->log();
                if (log == nullptr) continue;
                auto readRange = [this, log](int64_t from, int64_t to) -> rune::storage::LogReadView {
                    return readNoFault_(log, from, kPushMaxChunkBytes, to);
                };
                work += driveViewsOf_(topicId, partition, readRange, bound, nowNs, nullptr);
            } else {
                // Phase 2b E: a TRANSIENT source — zero-copy view into the
                // replica's own ring (same thread).  The fold itself stops at its
                // passBound, so the ring's view needs no upper bound.
                rune::storage::InMemoryLog* mem = replica->memoryLog();
                if (mem == nullptr) continue;
                auto readRange = [mem](int64_t from, int64_t /*to*/) -> rune::storage::LogReadView {
                    return mem->readView(from, kPushMaxChunkBytes);
                };
                work += driveViewsOf_(topicId, partition, readRange, bound, nowNs, mem);
            }
        }
        return work;
    }

    // One owned source partition's view work.  Order matters: materialize a
    // recovery instance from its checkpoint FIRST, then (transient source only)
    // reconcile its cursor against the source's live range, then rebuild/fold.
    // Task 19b (restart recovery): materializeRecoveryInstancesFor + driveRebuild
    // are gated on hasRecovery() — a broker with no durable views runs neither.
    template <class ReadRangeFn>
    int driveViewsOf_(int32_t topicId, int32_t partition, ReadRangeFn& readRange,
                      int64_t bound, int64_t nowNs, rune::storage::InMemoryLog* transientSrc) {
        int work = 0;
        const bool recovering = viewSet_->hasRecovery();
        if (recovering) [[unlikely]]
            viewSet_->materializeRecoveryInstancesFor(topicId, partition, bound);
        if (transientSrc != nullptr)
            work += viewSet_->reconcileTransientSource(topicId, partition,
                                                       transientSrc->logStartOffset(),
                                                       transientSrc->logEndOffset());
        if (recovering) [[unlikely]]
            work += viewSet_->driveRebuild(topicId, partition, readRange, bound, nowNs);
        work += viewSet_->driveFolds(topicId, partition, readRange, bound, nowNs);
        return work;
    }
```

Also update the comment block above `driveViews_` ("Phase 1: disk-backed sources only (the fold reads the committed log via readNoFault_ …") to: "Disk sources fold through readNoFault_ (one pread, no page fault); TRANSIENT sources (Phase 2b E) fold through the replica's InMemoryLog::readView (zero-copy, same thread) after reconcileTransientSource."

- [ ] **Step 4: Run to verify they pass + latency gate**

Run: `cmake --build build-fast -j && ctest --test-dir build-fast -R Regression --output-on-failure -j8 && ctest --test-dir build-fast -R 'WorkerViewSet|Worker\.|WorkerRegression|ViewRecovery|ViewReadiness|GetViewRow' --output-on-failure`
Expected: all PASS.
Bench (disk fold path must be unchanged — the branch is per partition, not per record): `cmake --build build-fast -j --target rune_broker_benchmark && $(find build-fast -name rune_broker_benchmark -type f | head -1) --benchmark_filter='ViewFold'` — `BM_ViewFoldReplace` record_p99 < 5 µs, batch_p99 < 10 µs (§14.0), within noise of the pre-change run (run once on the base commit first and record both in the commit body).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/WorkerViewSet.h libs/node/src/WorkerViewSet.cpp libs/node/include/rune/node/Worker.h libs/node/tests/node/WorkerViewSetTest.cpp libs/node/tests/node/WorkerTest.cpp libs/node/CMakeLists.txt
git commit -m "feat(view): views fold over transient sources (source reset + eviction states)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Client — `Persistence`, `defineTopic*` overloads, `schema apply` honours `transient topic`

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-client-cpp` (compiles `../rune-ql` + `../rune-protocol` from Tasks 1-2)

**Files:**
- Modify: `include/rune/client/Client.h:83-90` (enum), `:569-571` and `:622-625` (overloads)
- Modify: `src/Client.cpp:1109-1124`, `:1509-1524`
- Modify: `src/cli/Cli.cpp:367-376`
- Test: `tests/client/ClientSchemaTest.cpp`, `tests/RuneCliLiveTest.cpp`

**Interfaces:**
- Consumes: `rpc::DefineTopic::persistence` (Task 2), `rune::ql::DefineTopicPlan::persistence` (Task 1).
- Produces (Part D's console calls the AndWait overload):
  ```cpp
  namespace rune::client { enum class Persistence : uint8_t { Disk = 0, Transient = 1 }; }
  OpResult Client::defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                               bool validate, std::span<const uint8_t> descriptorBlob,
                               Persistence persistence, CompletionTarget target, Token token);
  DefineTopicResult Client::defineTopicAndWait(std::string_view name, uint32_t partitions,
                                               std::string_view keyField, bool validate,
                                               std::span<const uint8_t> descriptorBlob,
                                               Persistence persistence,
                                               std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
  ```
  The existing overloads (no `Persistence`) are unchanged and mean `Persistence::Disk`.

- [ ] **Step 1: Write the failing tests**

`tests/client/ClientSchemaTest.cpp` — in `SchemaTestGateway` add the member `uint8_t lastDefinePersistence = 0xff;` and, in the `DefineTopic` case right after `definesSeen.fetch_add(...)`, `lastDefinePersistence = opt->persistence;`. In `SchemaHarness` add:

```cpp
    test_support::PendingOp defineTopicOp(std::string_view name, uint32_t partitions,
                                          std::string_view keyField, bool validate,
                                          std::span<const uint8_t> blob, Persistence persistence) {
        return cq->issue([&](Completions& q, Token t) {
            return client->defineTopic(name, partitions, keyField, validate, blob, persistence, q, t);
        });
    }
```

Append:

```cpp
// ===========================================================================
// Phase 2b E: Persistence on DefineTopic.
// ===========================================================================
TEST(ClientSchemaRegression, DefineTopicTransientSendsPersistenceByte) {
    SchemaHarness h(1140, 1141);
    h.build();
    h.connectAndWait();
    std::vector<uint8_t> blob = {0x01};
    auto fut = h.defineTopicOp("presence", 4, "userId", false, std::span<const uint8_t>(blob),
                               Persistence::Transient);
    h.pumpGwUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady());
    EXPECT_EQ(fut.defineResult().errorCode, 0u);
    EXPECT_EQ(h.gw->lastDefinePersistence, rpc::DefineTopic::kPersistenceTransient);
}

TEST(ClientSchemaRegression, DefineTopicDefaultOverloadIsDisk) {
    SchemaHarness h(1142, 1143);
    h.build();
    h.connectAndWait();
    std::vector<uint8_t> blob = {0x01};
    auto fut = h.defineTopicOp("orders", 4, "id", false, std::span<const uint8_t>(blob));
    h.pumpGwUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady());
    EXPECT_EQ(h.gw->lastDefinePersistence, rpc::DefineTopic::kPersistenceDisk);
    EXPECT_EQ(static_cast<uint8_t>(Persistence::Disk), 0u);
    EXPECT_EQ(static_cast<uint8_t>(Persistence::Transient), 1u);
}
```

`tests/RuneCliLiveTest.cpp` — `CliTestGateway` gets `uint8_t lastDefinePersistence = 0xff;` set in the `DefineTopic` case next to `lastDefineKeyField`; append:

```cpp
// Phase 2b E: `transient topic …;` through `rune schema apply` must reach the
// broker as a TRANSIENT DefineTopic (silently creating a disk topic would be a bug).
TEST(CliLiveRegression, ApplyTransientTopicSendsTransientPersistence) {
    static constexpr int32_t kIn = 1240, kOut = 1241;
    const std::string baseDir = cliUniqueDir();
    const std::string aeronDir = baseDir + "/aeron";
    const std::string rqlPath = writeTmpRql(
        baseDir, "transient topic presence { userId int64, online bool } partitions 4 key userId;");
    CliTestGateway gw(aeronDir, kIn, kOut);
    gw.defineTopicId = 0x40000002; gw.defineVersion = 1; gw.defineErrorCode = 0;
    std::vector<std::string> args = {
        "schema", "apply", rqlPath,
        "--cli-data-dir", baseDir,
        "--cli-stream-in",  std::to_string(kIn),
        "--cli-stream-out", std::to_string(kOut),
    };
    auto r = runCli(gw, aeronDir, args);
    EXPECT_EQ(r.rc, 0) << r.err;
    EXPECT_EQ(gw.definesSeen.load(), 1);
    EXPECT_EQ(gw.lastDefineName, "presence");
    EXPECT_EQ(gw.lastDefinePersistence, rpc::DefineTopic::kPersistenceTransient);
    gw.stop();
    try { std::filesystem::remove_all(baseDir); } catch (...) {}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j --target rune_schema_client_tests rune_cli_live_tests`
Expected: compile error `no type named 'Persistence' in namespace 'rune::client'`.

- [ ] **Step 3: Implement**

`Client.h` — before `struct DefineTopicResult`:

```cpp
// Topic persistence for defineTopic (Phase 2b E).  The DefineTopic wire byte;
// values are APPEND-ONLY.
//   Disk      — durable log (default; what the overloads without it send).
//   Transient — replicated to the ISR before ack, never written to disk.  The
//               topic's metadata survives a broker restart, its data does not
//               (a restarted node rejoins empty; after a full-cluster restart
//               the topic is empty).
enum class Persistence : uint8_t { Disk = 0, Transient = 1 };
```

After the existing `defineTopic(...)` declaration:

```cpp
    /// Phase 2b E: as above, with an explicit Persistence (Transient = a
    /// transient topic).  The overload without it is Persistence::Disk.
    OpResult defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                         bool validate, std::span<const uint8_t> descriptorBlob,
                         Persistence persistence, CompletionTarget target, Token token);
```

After the existing `defineTopicAndWait(...)` declaration:

```cpp
    /// Phase 2b E: defineTopic(..., persistence), blocking.  Same result contract.
    DefineTopicResult defineTopicAndWait(std::string_view name, uint32_t partitions,
                                         std::string_view keyField, bool validate,
                                         std::span<const uint8_t> descriptorBlob,
                                         Persistence persistence,
                                         std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
```

`Client.cpp` — replace `Client::defineTopic` and `Client::defineTopicAndWait` with:

```cpp
OpResult Client::defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                             bool validate, std::span<const uint8_t> descriptorBlob,
                             CompletionTarget target, Token token) {
    return defineTopic(name, partitions, keyField, validate, descriptorBlob,
                       Persistence::Disk, target, token);
}

OpResult Client::defineTopic(std::string_view name, uint32_t partitions, std::string_view keyField,
                             bool validate, std::span<const uint8_t> descriptorBlob,
                             Persistence persistence, CompletionTarget target, Token token) {
    namespace rpc = rune::protocol::client;
    rpc::DefineTopic msg;
    msg.sessionId      = 0;
    msg.correlationId  = 0;
    msg.topicName      = name;
    msg.partitionCount = partitions;
    msg.keyField       = keyField;
    msg.validate       = validate;
    msg.descriptorBlob = descriptorBlob.data();
    msg.descriptorLen  = static_cast<uint32_t>(descriptorBlob.size());
    msg.persistence    = static_cast<uint8_t>(persistence);   // Disk writes no byte (old layout)
    std::vector<uint8_t> frame(msg.encodedSize() + 32u);
    const std::size_t n = msg.encode(frame.data());
    if (n == 0) return OpResult::InvalidArgument;
    return issueControl_(OpKind::DefineTopic, target, token, std::span<const uint8_t>(frame.data(), n), 0);
}
```

```cpp
DefineTopicResult Client::defineTopicAndWait(std::string_view name, uint32_t partitions,
                                             std::string_view keyField, bool validate,
                                             std::span<const uint8_t> descriptorBlob,
                                             std::chrono::nanoseconds timeout) {
    return defineTopicAndWait(name, partitions, keyField, validate, descriptorBlob,
                              Persistence::Disk, timeout);
}

DefineTopicResult Client::defineTopicAndWait(std::string_view name, uint32_t partitions,
                                             std::string_view keyField, bool validate,
                                             std::span<const uint8_t> descriptorBlob,
                                             Persistence persistence,
                                             std::chrono::nanoseconds timeout) {
    DefineTopicResult result;
    andWait_("defineTopic()", timeout, 0,
             [&](Completions& q, Token t) {
                 return defineTopic(name, partitions, keyField, validate, descriptorBlob,
                                    persistence, q, t);
             },
             [&](const Completion& c, std::span<const uint8_t> p) {
                 if (c.status != OpStatus::BrokerError) throwForStatus_("defineTopic()", c, p);
                 result.topicId   = static_cast<int32_t>(c.value);
                 result.version   = static_cast<uint16_t>(c.aux);
                 result.errorCode = c.errorCode;
             });
    return result;
}
```

`src/cli/Cli.cpp` — the `defineTopicAndWait` call in `schema apply` (lines 371-378) becomes:

```cpp
            const rune::client::Persistence persistence =
                plan.persistence == rune::ql::Persistence::Transient
                    ? rune::client::Persistence::Transient
                    : rune::client::Persistence::Disk;
            r = client.defineTopicAndWait(
                plan.name,
                plan.partitions,
                plan.keyField,
                validate,
                std::span<const uint8_t>(plan.descriptorBlob.data(),
                                         plan.descriptorBlob.size()),
                persistence,
                kCliWait);
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j && ctest --test-dir build -R Regression --output-on-failure -j8 && ctest --test-dir build -R 'ClientSchema|CliLive|CliOffline' --output-on-failure`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp add include/rune/client/Client.h src/Client.cpp src/cli/Cli.cpp tests/client/ClientSchemaTest.cpp tests/RuneCliLiveTest.cpp
git -C /Users/vradu/Projects/se-worktrees/rune-client-cpp commit -m "feat(client): Persistence on defineTopic/defineTopicAndWait; schema apply honours transient topic

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Docs — transient topics in the broker's self-hosting guide

**Files:**
- Modify: `docs/SELF-HOSTING.md:35-51` (config table + the stale "Not env-configurable (yet): topics/partitions" note)

- [ ] **Step 1: Edit** — add this row to the configuration table after `RUNE_STARTUP_CANVASS_TIMEOUT_MS`:

```markdown
| `RUNE_TOPICS` | no | — | list | Boot topics, comma-separated `name:partitions[:policy][:key=value…]`. Options: `persistence=disk\|transient` (default `disk`; `memory` is a **deprecated** alias for `transient` and logs one warning line per entry), `durability=majority\|all-in-sync`, `tier=leader\|majority\|strict`, `group=<co-partition group>`, `key=<partition key field>`. Topics can also be defined at runtime (Rune QL `topic …;` / `transient topic …;`, or the client's `defineTopic`). |
```

Replace the note `> Not env-configurable (yet): topics/partitions (Milestone B), dynamic membership, TLS/auth.` with `> Not env-configurable (yet): dynamic membership.` and add after the `/dev/shm` note:

```markdown
### Transient topics

A **transient** topic (`persistence=transient` in `RUNE_TOPICS`, `transient topic …;` in Rune QL,
or `Persistence::Transient` on the client's `defineTopic`) is replicated to the in-sync replicas
before a produce is acknowledged but is **never written to disk**.

- Its *metadata* survives restarts: a runtime-defined transient topic is recreated as transient on boot.
- Its *data* does not: a restarted node rejoins with the topic empty; after a **full-cluster**
  restart the topic is empty. Offsets restart from 0.
- Persistence is fixed at definition: `alter topic` cannot change it.
- Views over a transient topic are allowed. A persisted view keeps its checkpointed rows across a
  full restart and folds every record produced after it; a transient view over a transient topic
  is empty after a full restart.
- Use `durability=majority` (default) or `all-in-sync` if a transient topic must survive a node death.
```

- [ ] **Step 2: Verify** — `grep -n "transient\|RUNE_TOPICS" docs/SELF-HOSTING.md` shows the row and section; `grep -rni "memory topic\|persistence=memory" docs/SELF-HOSTING.md README.md docker` shows only the deprecated-alias mention.

- [ ] **Step 3: Commit**

```bash
git add docs/SELF-HOSTING.md
git commit -m "docs: transient topics (RUNE_TOPICS persistence=transient; memory deprecated)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: e2e — define a transient topic, produce + subscribe, restart → exists and empty

> **BLOCKED — do not start until Spec question 1 is resolved.** On the current code a transient topic cannot be consumed in any worker mode and cannot be produced to in the default `RUNE_WORKER_MODE=shard` (see Spec question 1), so this leg fails for reasons outside Part E's tasks. The code below is final; it becomes runnable once the data-plane decision lands.

The spec's e2e says "console defines a transient topic"; the console is Part D (after E). This leg defines it through the client API (`defineTopicAndWait(..., Persistence::Transient)`), which is exactly what D's console calls; D re-runs this leg through `rune console` (D's own e2e).

**Files:**
- Modify: `rune-client-cpp/examples/view_probe.cpp` (setup step 6; verify_restart step (e); header comment)
- Modify: `docker/view-engine-e2e.sh:283-306` (extract/handoff), `:343-355` (verify env), `:396-405` (summary)

- [ ] **Step 1: Probe — setup step 6** — in `view_probe.cpp` add `#include <rune/client/subscriber/Subscription.h>`; add a helper above `main`:

```cpp
// Subscribe from EARLIEST and wait (bounded) until the subscription's published
// position reaches `atLeast`; returns the final position after unsubscribe +
// finished().  position() comes off the subscription's own status ring — no
// state is shared with the handler (which runs on the client's duty thread).
static int64_t subscribeEarliestUntil(Client& c, int32_t topicId, int64_t atLeast, int boundMs) {
    SubscribeOptions opts;
    opts.from = From::earliest();
    auto sub = c.subscribe(TopicHandle{topicId, 1}, /*partition=*/0, opts, [](const Record&) {});
    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (sub.position() < atLeast && std::chrono::steady_clock::now() < dl)
        std::this_thread::sleep_for(5ms);   // poll interval, not correctness
    sub.unsubscribe();
    const auto fin = std::chrono::steady_clock::now() + 5s;
    while (!sub.finished() && std::chrono::steady_clock::now() < fin) std::this_thread::sleep_for(1ms);
    return sub.position();
}

static bool produceOne(Client& c, int32_t topicId, int64_t key, int timeoutMs, int64_t& offsetOut) {
    ProducerConfig pcfg;
    pcfg.acks             = Acks::All;   // transient: replicated before ack
    pcfg.produceTimeoutMs = static_cast<int64_t>(timeoutMs);
    auto p = c.producer(pcfg);
    Completions cq(c);
    Completion  done;
    const auto value = encodeCharacter(key, 1, /*version=*/1);
    const bool ok = sendAndWaitAck(cq, p, TopicHandle{topicId, 1}, Key(static_cast<uint64_t>(key)),
                                   std::span<const uint8_t>(value), 1, timeoutMs, done);
    offsetOut = done.value;
    return ok && done.status == OpStatus::Ok;
}
```

In `mode == "setup"`, before the handoff block insert:

```cpp
            // ---- 6. Phase 2b E: a TRANSIENT topic — define, produce, subscribe ----
            DefineTopicResult tt = c.defineTopicAndWait(
                "presence_t", /*partitions=*/1, /*keyField=*/"id", /*validate=*/false,
                std::span<const uint8_t>(blob), Persistence::Transient, wait);
            if (tt.errorCode != 0) { std::cerr << "PROBE: FAIL defineTopic(presence_t transient) errorCode=" << tt.errorCode << "\n"; return 50; }
            for (int64_t k = 1; k <= 3; ++k) {
                int64_t off = -1;
                if (!produceOne(c, tt.topicId, k, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t)\n"; return 51; }
            }
            const int64_t pos = subscribeEarliestUntil(c, tt.topicId, 3, boundMs);
            if (pos < 3) { std::cerr << "PROBE: FAIL subscribe(presence_t) position=" << pos << " (want >= 3)\n"; return 52; }
            std::cerr << "PROBE: transient topic presence_t topicId=" << tt.topicId << " produced+consumed 3\n";
```

and add to the handoff lines `std::cout << "VIEW_TT_TOPIC_ID=" << tt.topicId << "\n";`.

- [ ] **Step 2: Probe — verify_restart step (e)** — read `const int32_t ttTopic = static_cast<int32_t>(getEnvI64("RUNE_VIEW_TT_TOPIC_ID", -1));` with the other env reads, and before the final `VIEW-ENGINE-E2E: OK` line insert:

```cpp
            // ---- (e) Phase 2b E: the transient topic EXISTS and is EMPTY ------
            if (ttTopic < 0) { std::cerr << "PROBE: FAIL missing RUNE_VIEW_TT_TOPIC_ID\n"; return 60; }
            {
                // Metadata survived: re-defining answers TopicExists with the SAME id.
                const auto blob = rune::codec::serialize(characterDescriptor());
                DefineTopicResult again = c.defineTopicAndWait(
                    "presence_t", 1, "id", false, std::span<const uint8_t>(blob),
                    Persistence::Transient, wait);
                if (again.errorCode != 25 || again.topicId != ttTopic) {
                    std::cerr << "PROBE: FAIL presence_t after restart errorCode=" << again.errorCode
                              << " topicId=" << again.topicId << " (want TopicExists, id " << ttTopic << ")\n";
                    return 61;
                }
                // Data did not: the first post-restart record lands at offset 0.
                int64_t off = -1;
                if (!produceOne(c, ttTopic, 9, timeoutMs, off)) { std::cerr << "PROBE: FAIL produce(presence_t) after restart\n"; return 62; }
                if (off != 0) { std::cerr << "PROBE: FAIL presence_t not empty after restart: first offset=" << off << "\n"; return 63; }
                const int64_t pos = subscribeEarliestUntil(c, ttTopic, 1, boundMs);
                if (pos != 1) { std::cerr << "PROBE: FAIL presence_t subscribe after restart position=" << pos << " (want 1)\n"; return 64; }
                std::cerr << "PROBE: presence_t exists after restart, empty, produce+subscribe OK\n";
            }
```

Add to the file header's mode description: `6. (Phase 2b E) defineTopicAndWait("presence_t", Persistence::Transient), produce 3 (acks=All), subscribe from earliest → position 3; after restart: TopicExists with the same id, first produce at offset 0, subscribe → position 1.`

- [ ] **Step 3: Script** — in `docker/view-engine-e2e.sh` add `VIEW_TT_TOPIC_ID="$(extract VIEW_TT_TOPIC_ID)"` after `VIEW_FP_AGG=…`; add `VIEW_TT_TOPIC_ID` to the `for v in …` required list; append ` transientTopicId=${VIEW_TT_TOPIC_ID}` to the "Setup OK" log; add `-e RUNE_VIEW_TT_TOPIC_ID="${VIEW_TT_TOPIC_ID}" \` to the `run_client verify_restart` call; add `log "  -> transient topic presence_t: produce+subscribe, restart -> exists (TopicExists, same id) and empty (first offset 0)"` to the final summary.

- [ ] **Step 4: Run**

Run (from the broker repo root, sibling layout `../rune-protocol ../rune-codec ../rune-crypto ../rune-fr ../rune-ql ../rune-client-cpp` on the `feat/phase2b-e-transient-topics` branches): `bash docker/view-engine-e2e.sh`
Expected: `VIEW-ENGINE-E2E: OK, exit 0.` with the `presence_t` lines in the probe log.

- [ ] **Step 5: Commit** (two repos)

```bash
git -C ../rune-client-cpp add examples/view_probe.cpp
git -C ../rune-client-cpp commit -m "test(e2e): transient topic leg in view_probe (define, produce, subscribe, restart empty)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git add docker/view-engine-e2e.sh
git commit -m "test(e2e): view-engine e2e carries the transient-topic leg

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Self-review

- **Spec coverage (E):** rename everywhere → Task 3 (enum, config, logs, tests) + Task 9 (docs); metrics labels: none exist that name memory topics (checked: no metric label containing "memory" in `libs/` or `src/`). `persistence=memory` alias + one-line warning → Task 3. Rune QL `transient topic` → `TopicDef.persistence`, alter rejection → Task 1. Wire byte, old clients ⇒ disk → Task 2 + Task 5. `defineTopicAndWait(..., Persistence::Transient)` → Task 8. AdminWorker creates on InMemoryLog → Task 5 (+ existing `TopicRegistry`/`createRuntimeUserReplicas` path). `TopicSchema` records the mode, recovery recreates transient → Tasks 4, 6. Views over transient topics → Task 7. Gates: QL parse/backend tests (Task 1); DefineTopic round-trip with/without byte `*Regression*` (Tasks 2, 5); broker test created on InMemoryLog + survives restart as transient empty (Task 6); replicated before ack under acks=All (Task 6, replica level — see Spec question 4); e2e (Task 10, BLOCKED — Spec question 1).
- **Placeholders:** none; every code step carries the code. The two mechanical renames are given as exact commands with a verifying `grep` that must print nothing.
- **Type consistency:** wire byte is `uint8_t` everywhere (`DefineTopic::persistence`, `DefineTopicMsg::persistence`, `CreateTopicReqMsg::persistence`, `requestCreateTopic`'s 6th param); typed enums are `rune::ql::Persistence`, `rune::client::Persistence`, `rune::node::PersistenceMode`, each `{Disk = 0, Transient = 1}`; conversions only via `persistenceFromWire`/`persistenceToWire` (broker) and the explicit mapping in `Cli.cpp`.
- **Review Focus:** all five lines have their named test in the owning task (Tasks 2/5, 5/6, 5, 7, 3).

## Spec questions

1. **BLOCKING for the e2e and the "produce + subscribe work" gate — transient topics have no read path, and no produce path in the default worker mode.** Verified on `feature/materialized-views`:
   - Subscribe/push: the gateway push loop skips any subscription whose topic has no disk `Log` (`libs/gateway/src/ClientGateway.cpp:3935-3937`: `topicRegistry_->log(...)` is `nullptr` for a transient partition → `continue`); the worker PushRead path answers "unanswered" when `logPtr` is null (`Worker.h:3211-3217`), and `logPtr` is set only for disk replicas (`Worker.h:3431`, `:5047`). So a memory/transient topic cannot be consumed in **any** mode today.
   - Produce: in shard mode (`NodeConfig.h:230`: `WorkerMode::Shard` is the **default**; `NodeConfig.cpp:1469-1473` forces per-worker ingress) `getLogAndReplicaForPartition_` returns no `Log*` for a transient replica (`Worker.h:6155-6170`) and the produce is answered `Internal` (`Worker.h:6519-6524`). `produceCore_` is `Log&`-typed throughout (PSM gate, `handleProduceBatch(*log, …)`).
   The spec's "Today" paragraph assumes memory topics work end to end; they only replicate. Making them consumable (gateway push + worker PushRead from `InMemoryLog`) and producible in shard mode is hot-path data-plane work of its own (templating `produceCore_`/`servePushRead_` over the log type, PSM over `InMemoryLog`). **Decision needed:** extend Part E with a data-plane sub-plan (my estimate: 3-4 tasks, Worker + ClientGateway, with §14.0-style produce/consume latency benches), or split it into a new Part E2 that Task 10 depends on. Task 10 is written but marked BLOCKED.
2. **New client → old broker.** An old broker's `DefineTopic::decode` ignores trailing bytes, so a transient define against an un-upgraded broker silently creates a **disk** topic. Plan assumes "upgrade brokers before clients" (documented in Task 9 only implicitly). Confirm, or require a version handshake (e.g. a client-version gate like `ConnectRequest.clientVersion`).
3. **Views over a transient source — plan's interpretation (Task 7).** The spec says "allowed, no special casing", but the code skipped such sources, and after a full restart the source reuses offsets below a persisted view's checkpoint cursor. The plan keeps the checkpoint rows and re-folds from the source's `logStart` (`SOURCE_RESET`), and skips + counts ring-evicted records (`EVICTED`). Confirm both resolutions (the alternative for `SOURCE_RESET` — dropping the checkpoint rows — contradicts "keeps its checkpoint").
4. **Multi-node runtime transient topics.** Runtime topics are single-node in this line (`RuntimeTopicRecovery.h`: "Phase 1 is single-node; multi-node catalog replication is the deferred §11 boundary"), so "a restarted node rejoins empty and catches up from peers" holds only for static `RUNE_TOPICS` transient topics in 2b. The acks=All gate is therefore proven at the replica layer (Task 6, `TransientTopicAcksAllReplicatesBeforeAck`) plus the existing multi-replica `TransientTopic_*` failover tests. Confirm that is acceptable for 2b.
5. **Internal names kept.** `InMemoryLog`, `isMemoryBacked()`/`memoryLog()`, `NodeConfig::memoryLogRingBytes`, the env var `RUNE_MEMORY_LOG_RING_BYTES`, and the on-disk `<dataDir>/memory-epoch-fence/` directory keep their names: they name the storage backend, and renaming the directory would orphan existing epoch fences. `RUNE_MEMORY_LOG_RING_BYTES` is user-visible — rename it (with an alias) too, or keep?
6. **rune-site docs.** `rune/selfhost.html` documents `RUNE_TOPICS` as `name:partitions[:delete|compact]` with no persistence option; the site is outside this part's four repos. Update it separately.
7. **`alter topic` rejection form.** The grammar has no persistence alter op; the plan rejects the two spellings a user would try (`alter topic x set persistence …`, `alter transient topic …`) at parse time with "alter topic cannot change persistence…". Confirm this is the intended "clear error" (vs. a broker-side error code).
