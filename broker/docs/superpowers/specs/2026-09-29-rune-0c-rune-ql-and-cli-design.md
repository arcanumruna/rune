# Rune 0c — Rune QL & CLI

**Sub-project:** 0c of the Materialized-Views foundation. Builds on **0a
(rune-codec)** and **0b (schema registry & typed transport)** — both complete
and on `main`. Precedes the view-engine foundation (query execution).

**Parent spec:** `2026-09-29-rune-materialized-views-typed-topics-design.md`
(§5 sketches Rune QL & CLI; this document is its full design and supersedes that
sketch where they differ).

**Goal:** a small client-side DDL language (**Rune QL**) for defining typed
topics, plus the **`rune`** operator CLI — so an operator writes
`topic characters { … } partitions 16 key userId;`, and `rune schema apply`
creates the topic, `rune schema generate` emits the typed C++ header, and
`rune schema get|list` inspect the registry. Introduces a **fixed-size string**
primitive in rune-codec.

---

## 1. Scope

**In scope (0c):**
- A **rune-codec (0a) extension**: a fixed-size string primitive (`string(N)`),
  strictly additive.
- **rune-ql** (new standalone library repo): hand-written recursive-descent
  parser for the DDL, an AST, and pure backends (`toDescriptor`, apply-args).
  Depends only on rune-codec. No networking.
- The **`rune` CLI** (in rune-client-cpp, absorbing the existing
  `rune_schema_gen`): `rune schema apply|get|generate|list`.

**Out of scope (deferred to later foundations):**
- The **query** language surface, plan-IR, and query execution — they need the
  **view engine** (a later foundation); nothing to execute yet, so no query
  grammar/plan-IR ships in 0c (no dead code). No `rune query`.
- `create view` / views.
- Schema **evolution** (`ALTER`, multiple versions) — 0c is create-topic only,
  single version (additive evolution is a later foundation).
- Enum field syntax in the DDL (rune-codec supports Enum at the descriptor
  level; 0c does not expose the keyword yet).

## 2. Decisions (locked during 0c brainstorming)

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | **DDL + CLI only; defer all query surface.** | Query execution needs the view engine; parsing queries with nothing to run is dead code (YAGNI). |
| D2 | **rune-ql = new standalone library repo** (pure parser/AST/backends, depends only on rune-codec); the **`rune` CLI lives in rune-client-cpp/tools** (links rune-ql + client + rune-codec), superseding `rune_schema_gen`. | Reusable, isolation-testable parser (rune-codec precedent); the CLI is where the client + codegen already live. |
| D3 | **Two string types:** `string` = variable-length (existing rune-codec var-data `String`); `string(N)` = **fixed N-byte block field**. | Fixed-width strings are zero-indirection, cache-friendly, ultra-low-latency-friendly, and a common MMORPG need. |
| D4 | **The fixed-size string is a rune-codec (0a) extension**, folded into the 0c plan as its first tasks. | 0c depends on it; one spec/plan covers the feature; the change is additive (new PrimType; existing schemas/fingerprints unaffected). |
| D5 | **Over-length assignment to a `string(N)` is a validation error**, not silent truncation; shorter values are NUL-padded. | Matches the engineering philosophy (no silent data loss). |
| D6 | **`rune schema list` reuses the existing `MetadataRequest`** (topic names + ids); `--verbose` fetches version/fingerprint per topic via `getTopicSchema`. | No new wire op; 0c does not touch rune-protocol or the broker. |

## 3. Program constraints inherited

- **Engineering philosophy (16 HARD RULES)** apply — notably: zero hot-path
  allocation, cache-friendly/compact structures, test + benchmark everything,
  a `*Regression*` test on every fix, run the full regression suite first,
  fix-on-sight, and never report done without the REAL e2e (default config)
  green. rune-ql parsing is a cold path (client setup), but the parser is still
  allocation-disciplined and benchmarked for its microsecond-parse claim.
- **rune-codec is the type model**; **the broker never parses text** — Rune QL
  is entirely client-side, producing a rune-codec descriptor + `DefineTopic`
  API calls. The broker only ever sees the typed 0b API.
- **Additive-only**: the rune-codec extension appends a `PrimType` and a
  `FieldDescriptor` field; old descriptor blobs still deserialize.

## 4. Architecture & module map

```
rune-codec (0a, EXTENDED — additive)
  include/rune/codec/PrimType.h       + FixedStr (appended enum value)
  include/rune/codec/Descriptor.h     FieldDescriptor += uint16_t fixedLen; build() lays FixedStr in the block
  src/Serialize.cpp                   (de)serialize fixedLen (appended per-field; old blobs read 0)
  include/rune/codec/Fingerprint.h    absorb fixedLen (string(8) != string(16))
  src/Record.cpp                      readField: FixedStr → string_view over block bytes
  src/Validator.cpp                   FixedStr bounds check (field within block)
  src/Generator.cpp                   FixedStr → std::array<char,N> member + string_view accessor + block read/write

rune-ql (NEW standalone lib repo ~/Projects/rune-ql; depends only on rune-codec)
  include/rune/ql/Ast.h               TopicDef, FieldDef, ParseError
  include/rune/ql/Parser.h  src/Parser.cpp    tokenizer + recursive-descent parse(text) -> TopicDef | ParseError
  include/rune/ql/Backends.h src/Backends.cpp toDescriptor(TopicDef)->Descriptor; applyArgs(TopicDef)
  tests/                              parser + backend + golden + parse-latency bench
  CMakeLists.txt / conanfile.py       (rune-codec sibling source lib, like the broker/client consume it)

rune-client-cpp (CLI added; rune_schema_gen absorbed)
  tools/rune_cli.cpp                  `rune` umbrella: schema apply|get|generate|list
  (links rune-ql + rune-codec + the client lib; RUNE_QL_ROOT overridable like RUNE_CODEC_ROOT)
  tools/rune_schema_gen.cpp           superseded by `rune schema generate` (keep as thin alias or remove)
```

### 4.1 Build integration
rune-ql consumes rune-codec as a sibling source static lib via an overridable
`RUNE_CODEC_ROOT` (the established pattern). The `rune` CLI in rune-client-cpp
consumes rune-ql via an overridable `RUNE_QL_ROOT` (default `../rune-ql`) — same
sibling-source-lib pattern used for rune-codec. rune-ql is a new repo with its
own CMake + Conan (mirrors rune-codec's standalone setup) and its own git
remote decision at finish (default: local, like rune-codec).

## 5. rune-codec fixed-size string primitive (0a extension)

Strictly additive; existing schemas and fingerprints are unaffected.

- **`PrimType::FixedStr`** appended to the enum (existing ordinals unchanged).
  `fixedSize(PrimType)` cannot map it (width is per-field), so call sites that
  need a fixed width read it from the `FieldDescriptor` (see below); `isVarLen`
  returns **false** for `FixedStr` (it lives in the block, not the var tail).
- **`FieldDescriptor` gains `uint16_t fixedLen = 0`** (0 for all non-FixedStr
  fields). `DescriptorBuilder::add(...)` gains an optional fixed-length
  parameter (or a dedicated `addFixedStr(name, len, …)` overload).
- **`DescriptorBuilder::build()`**: a `FixedStr` field is laid out **in the
  fixed block** — assigned an `offset` in the first pass and contributing
  `fixedLen` bytes to `blockLength`, exactly like a scalar. It is NOT assigned a
  var-index and does NOT appear in the var-data tail.
- **Encode (generated + framing):** write exactly `fixedLen` bytes at the
  field's block offset; source shorter than `fixedLen` → NUL-pad the remainder;
  source longer → **validation error** (reject; do not truncate — D5).
- **`readField`:** `FixedStr` → a `string_view` over the `fixedLen` bytes at
  the block offset (zero-copy; caller may trim trailing NULs). Bounds-checked
  (`offset + fixedLen <= blockLength <= record size`).
- **Validator:** verify the field's `[offset, offset+fixedLen)` lies within the
  fixed block; no whitelist. (Over-length is caught at encode time, not read.)
- **Fingerprint:** absorb `fixedLen` (LE16) per FixedStr field so `string(8)`
  and `string(16)` are distinct types.
- **Serialize/deserialize:** append `fixedLen` (u16) to the per-field record;
  old blobs (which lack it) deserialize with `fixedLen = 0` for their
  (non-FixedStr) fields — backward compatible. Bump no magic; the field is a
  pure append guarded by the field's type.
- **Generator:** `FixedStr` → a `std::array<char, N>` member (or `char[N]`) with
  a `std::string_view` accessor and block-offset read/write in `encode`/`decode`;
  `kBlockLength` includes it.

## 6. Rune QL — grammar, AST, backends (rune-ql lib)

### 6.1 Grammar (single-pass recursive-descent; `--` line comments)
```
statement := "topic" IDENT "{" field ("," field)* ","? "}"
             "partitions" INT "key" IDENT ";"
field     := IDENT type
type      := "int32" | "int64" | "uint16" | "uint32" | "uint64"
           | "float64" | "bool" | "bytes"
           | "string" [ "(" INT ")" ]        -- no (N): variable; (N): fixed N bytes
```
Whitespace-insensitive; `--` to end-of-line is a comment. IDENT =
`[A-Za-z_][A-Za-z0-9_]*`. INT = decimal.

### 6.2 AST
```cpp
namespace rune::ql {
struct FieldDef { std::string name; rune::codec::PrimType type; uint16_t fixedLen = 0; };
struct TopicDef { std::string name; std::vector<FieldDef> fields; uint32_t partitions = 1; std::string keyField; };
struct ParseError { std::size_t line; std::size_t col; std::string message; };
// parse returns a TopicDef on success or a ParseError (std::expected<TopicDef, ParseError>-style).
std::expected<TopicDef, ParseError> parse(std::string_view src) noexcept;
}
```
Type keywords map to `PrimType`: `int32→Int32 … float64→Float64`, `bool→Bool`,
`bytes→Bytes`, `string→String` (variable), `string(N)→FixedStr` with
`fixedLen=N`.

### 6.3 Semantic checks (in the parser/backend, before any broker call)
- duplicate field name → `ParseError`
- `key <field>` names a field not in the topic → `ParseError`
- `partitions` < 1 → `ParseError`
- `string(N)` with `N < 1` → `ParseError`
- unknown type keyword / malformed statement → located `ParseError` (line, col)

### 6.4 Backends (pure functions on `TopicDef`)
- `rune::codec::Descriptor toDescriptor(const TopicDef&)` — builds via
  `DescriptorBuilder` (name, version=1; each field with its `PrimType`,
  `fixedLen`, `isKey = (name == keyField)`). Feeds codegen and apply.
- `struct DefineTopicPlan { std::string name; uint32_t partitions; std::string keyField; std::vector<uint8_t> descriptorBlob; }`
  via `applyPlan(const TopicDef&)` = `{ name, partitions, keyField,
  serialize(toDescriptor(td)) }`. The CLI feeds this to `Client::defineTopic`.

## 7. `rune` CLI (rune-client-cpp/tools/rune_cli.cpp)

Umbrella binary `rune`. Broker endpoint via `--broker <host:port>` or
`RUNE_BROKER` env. Connects via the 0b `Client`.

- **`rune schema apply <file|-> [--validate] [--dry-run]`** — read Rune QL from a
  file or stdin → `parse` → on `ParseError` print `file:line:col: message` and
  exit non-zero → `applyPlan` → `client.defineTopic(name, partitions, keyField,
  validate, blob)`. `--dry-run` parses + prints the resulting descriptor
  (`toText`) without contacting the broker. `TopicExists` is reported clearly
  (exit 0 with a "already exists" note, or a distinct code — pin in the plan).
  `--validate` sets the topic's server-side validation flag (default off).
- **`rune schema get <topic>`** — `getTopicSchema` → `deserialize` → print
  `toText(descriptor)`; `UnknownTopic` → clear error + non-zero.
- **`rune schema generate <topic> --out <dir>`** or **`--from-file <blob>`** —
  supersedes `rune_schema_gen`: fetch (or read) the descriptor → `generateHeader`
  → write `<dir>/<topic>.gen.h`.
- **`rune schema list [--verbose]`** — reuse `MetadataRequest` to list topic
  names + ids; `--verbose` additionally fetches each topic's version +
  fingerprint via `getTopicSchema`.

Exit codes: 0 success; non-zero on parse error, unknown topic, or broker error,
each with a clear one-line message. No `rune query` in 0c.

## 8. Error handling

| Condition | Behavior |
|-----------|----------|
| Malformed Rune QL | `rune` prints `<file>:<line>:<col>: <message>`, exits non-zero; no broker contact |
| `key` names an undeclared field / dup field / bad partitions / `string(0)` | located `ParseError`, non-zero exit |
| Over-length value into `string(N)` at encode | validation error (rejected), surfaced by the encoder/validator (D5) |
| `apply` on an existing topic | `TopicExists` reported (idempotent-ish; no clobber — 0c has no ALTER) |
| `get`/`generate` unknown topic | `UnknownTopic`, clear error, non-zero |
| broker unreachable | clear connection error, non-zero |

## 9. Testing & benchmarks (HARD RULES 5–12)

- **rune-codec fixed-string:** unit tests — `build()` lays FixedStr in the block
  at the right offset/blockLength; encode NUL-pads short + rejects over-length
  (D5); `readField` returns the right `string_view`; Validator bounds-check;
  fingerprint distinguishes `string(8)`/`string(16)`; serialize/deserialize
  round-trip incl. an old-blob-without-fixedLen backward-compat case; generator
  emits a compilable typed member (round-trip via the generated header). A
  `*Regression*` test. Existing rune-codec suite stays green (additive).
- **rune-ql:** parser unit tests (every type incl. `string`/`string(N)`; a full
  `characters` example; located errors for dup-field, missing-key, `partitions 0`,
  `string(0)`, syntax errors); backend tests (`toDescriptor` field/offset/isKey
  correctness; `applyPlan` blob deserializes + round-trips fingerprint; codegen
  via `generateHeader` compiles + round-trips). Golden tests. Pure, no
  networking. A **parse-latency benchmark** (Google Benchmark) validating the
  microsecond-parse claim.
- **rune CLI:** arg-parse + offline-path unit tests (`generate --from-file`,
  `apply --dry-run` parse-only, `apply` on a malformed file → located error +
  non-zero). Plus a **single-node Docker real-Aeron e2e** (the 0c #11 gate,
  reusing the T13 harness): `rune schema apply characters.rql` (real broker) →
  `rune schema generate characters` → compile the generated header into a probe
  → typed produce/consume → assert field round-trip. Real client↔broker, default
  config; not in-process.

## 10. Module boundaries (isolation check)

- **rune-ql parser** — text → AST; depends on nothing (returns `PrimType`
  values by keyword). Testable in isolation.
- **rune-ql backends** — AST → rune-codec `Descriptor` / apply plan; depend only
  on rune-codec. No networking.
- **rune-codec FixedStr** — additive primitive; self-contained within the codec.
- **`rune` CLI** — orchestration only: parse (rune-ql) + fetch/define (0b client)
  + codegen (rune-codec). No language or codec logic in the CLI itself.

## 11. Open items deferred to plan/implementation

- Exact `DescriptorBuilder` surface for FixedStr (`addFixedStr` overload vs an
  extra param on `add`) — pin in the plan against the current builder.
- The generated FixedStr member representation (`std::array<char,N>` vs `char[N]`)
  + accessor name — pin in the plan against the current generator output.
- Whether the client already exposes a topic-list API for `rune schema list`, or
  a thin client helper over `MetadataRequest`/`MetadataCache` is needed.
- Whether to keep `rune_schema_gen` as a thin alias of `rune schema generate` or
  remove it (the T13 e2e used `rune_codec_gen`/`rune_schema_gen` — update the
  harness to the `rune` verb).
- `apply` exit-code convention on `TopicExists` (0 with note vs distinct code).
