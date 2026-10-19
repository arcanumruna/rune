# Rune 0c — Rune QL & CLI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A client-side DDL language (Rune QL) + the `rune` operator CLI for defining/inspecting typed topics, built on a new fixed-size string primitive in rune-codec.

**Architecture:** Three phases. (1) Extend rune-codec with a fixed-size string primitive `FixedStr` (a fixed-width block field), strictly additive. (2) A new standalone `rune-ql` library — hand-written recursive-descent parser → AST → pure backends (`toDescriptor`, `applyPlan`), depending only on rune-codec. (3) The `rune` CLI in rune-client-cpp (`schema apply|get|generate|list`, absorbing `rune_schema_gen`), plus a single-node Docker real-Aeron e2e.

**Tech Stack:** C++23, CMake ≥3.25, Conan 2, GoogleTest, Google Benchmark. Repos: `rune-codec` (extended), new `rune-ql`, `rune-client-cpp` (CLI). rune-broker/rune-protocol unchanged.

**Spec:** `/Users/vradu/Projects/rune-broker-matviews/docs/superpowers/specs/2026-09-29-rune-0c-rune-ql-and-cli-design.md`

## Global Constraints

- **Engineering philosophy (16 HARD RULES):** shared-nothing; ring-buffers-only cross-thread (N/A here — 0c is client-side/library); rich-state where stateful; zero hot-path allocation; no virtual calls on hot paths; cache-friendly/compact + benchmark-validated; test everything + benchmark everything; a `*Regression*` test on every fix; failing repro test FIRST; run the FULL regression suite first on every fix; never report done without the REAL e2e (default config) green — shortcut/in-process tests don't count; fix-on-sight.
- **Additive-only rune-codec change:** append `PrimType::FixedStr` (existing ordinals unchanged); `FieldDescriptor.fixedLen` defaults 0; existing descriptor blobs/fingerprints MUST be unaffected (fixedLen serialized + fingerprinted ONLY for `FixedStr` fields).
- **Two string types:** `string` = variable (existing var-data `String`); `string(N)` = fixed N-byte block field (`FixedStr`, `fixedLen=N`).
- **Over-length into `string(N)` = error, not truncation; short = NUL-padded.**
- **The broker never parses text.** Rune QL is client-side; it produces a rune-codec `Descriptor` + `DefineTopic` API calls (0b `Client`).
- **No query/plan-IR/ALTER/enum-syntax in 0c** (create-topic only, single version).
- **rune-codec public API (existing, from 0a/0b):** `DescriptorBuilder(name,version).add(name,PrimType,sinceVersion,isKey,enumValues).build()`; `serialize`/`deserialize`/`toText`/`fingerprint`; `Validator`; `readField`; `generateHeader`. `PrimType{Int32,Int64,UInt16,UInt32,UInt64,Float64,Bool,Enum,String,Bytes}` (FixedStr appended by this plan). Record framing `[blockLength:u16][version:u16][fixed block][var tail]` LE.
- **0b client API (on main):** `Client::defineTopic(name,partitions,keyField,validate,blob)→future<DefineTopicResult>`, `getTopicSchema(name)→future<TopicSchemaResult{topicId,version,descriptorBlob,errorCode}>`, `openTopic<T>`. `rune_schema_gen` in rune-client-cpp/tools.
- **Toolchains (NOT on PATH):** rune-codec/rune-ql cmake `/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake` (native arm64); rune-client-cpp via Conan 2 x86_64 cross flow (`conan install . --build=missing -s arch=x86_64` + `-DCMAKE_OSX_ARCHITECTURES=x86_64` + generated toolchain; pass `-DRUNE_CODEC_ROOT=/Users/vradu/Projects/rune-codec -DRUNE_QL_ROOT=/Users/vradu/Projects/rune-ql -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/rune-protocol/include`). Docker for the e2e.
- **Worktrees (SDD setup):** rune-codec + rune-client-cpp get a `feature/0c-rune-ql` worktree off `main`; rune-ql is a brand-new repo (`git init`, first task). Consumers point `RUNE_CODEC_ROOT`/`RUNE_QL_ROOT` at the right paths.

---

## File structure

**rune-codec (extended):** `include/rune/codec/PrimType.h` (+FixedStr), `include/rune/codec/Descriptor.h` (+`fixedLen`, `addFixedStr`, `fieldWidth`, build layout), `src/Serialize.cpp` (+fixedLen ser/deser/toText), `include/rune/codec/Fingerprint.h` (+fixedLen), `src/Record.cpp` (+FixedStr read), `src/Validator.cpp` (+FixedStr entry/bounds), `src/Generator.cpp` (+FixedStr member/encode/decode). Tests under `tests/`.

**rune-ql (new repo `/Users/vradu/Projects/rune-ql`):** `include/rune/ql/Ast.h`, `include/rune/ql/Parser.h` + `src/Parser.cpp`, `include/rune/ql/Backends.h` + `src/Backends.cpp`, `tests/`, `benchmark/ParseBench.cpp`, `CMakeLists.txt`, `conanfile.py`, `.gitignore`.

**rune-client-cpp:** `tools/rune_cli.cpp` (new), `CMakeLists.txt` (+`RUNE_QL_ROOT`, `rune_ql` sibling lib, `rune` executable), `tests/`, a Docker e2e (`docker/`-style) + a checked-in `characters.rql`.

---

## PHASE 1 — rune-codec fixed-size string primitive

## Task 1: `FixedStr` type + descriptor layout

**Files:**
- Modify: `include/rune/codec/PrimType.h`
- Modify: `include/rune/codec/Descriptor.h`
- Test: `tests/FixedStrLayoutTest.cpp` (new; add to `rune_codec_tests`)

**Interfaces:**
- Produces: `PrimType::FixedStr`; `FieldDescriptor.fixedLen` (u16); `DescriptorBuilder::addFixedStr(name, len, sinceVersion=1, isKey=false)`; `inline uint32_t fieldWidth(const FieldDescriptor&)`; `build()` lays FixedStr in the fixed block.

- [ ] **Step 1: Write the failing test (`tests/FixedStrLayoutTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Descriptor.h>
using namespace rune::codec;

TEST(FixedStr, LaidInFixedBlockWithWidth) {
    Descriptor d = DescriptorBuilder("t", 1)
        .add("id", PrimType::Int64)          // offset 0, 8 bytes
        .addFixedStr("code", 8)              // offset 8, 8 bytes (FixedStr)
        .add("n", PrimType::Int32)           // offset 16, 4 bytes
        .build();
    const FieldDescriptor* code = d.find("code");
    ASSERT_NE(code, nullptr);
    EXPECT_EQ(code->type, PrimType::FixedStr);
    EXPECT_EQ(code->fixedLen, 8u);
    EXPECT_FALSE(isVarLen(code->type));      // block field, not var-data
    EXPECT_EQ(code->offset, 8u);             // after int64
    EXPECT_EQ(fieldWidth(*code), 8u);
    EXPECT_EQ(d.blockLength, 20u);           // 8 + 8 + 4
    EXPECT_EQ(d.find("n")->offset, 16u);
}
```

- [ ] **Step 2: Run it, verify it fails to compile** (`FixedStr`/`addFixedStr`/`fieldWidth` undefined). Build `rune_codec_tests`.

- [ ] **Step 3: `PrimType.h` — append `FixedStr`; keep `isVarLen` false for it; `fixedSize` returns 0 for it (per-field width lives on the descriptor).**
```cpp
enum class PrimType : uint8_t { Int32, Int64, UInt16, UInt32, UInt64, Float64, Bool, Enum, String, Bytes, FixedStr };
constexpr bool isVarLen(PrimType t) { return t == PrimType::String || t == PrimType::Bytes; }  // FixedStr is a BLOCK field
constexpr uint32_t fixedSize(PrimType t) {
  switch (t) {
    case PrimType::Int32: case PrimType::UInt32: case PrimType::Enum: return 4;
    case PrimType::Int64: case PrimType::UInt64: case PrimType::Float64: return 8;
    case PrimType::UInt16: return 2;
    case PrimType::Bool: return 1;
    case PrimType::String: case PrimType::Bytes: return 0;
    case PrimType::FixedStr: return 0;   // width is per-field (FieldDescriptor.fixedLen)
  }
  return 0;
}
```

- [ ] **Step 4: `Descriptor.h` — add `fixedLen`, `fieldWidth`, `addFixedStr`, and FixedStr-aware block layout in `build()`.**
  - In `struct FieldDescriptor`, add `uint16_t fixedLen = 0;`.
  - After `struct FieldDescriptor`, add:
```cpp
// Byte width of a field in the fixed block (0 for var-len). FixedStr uses its per-field length.
inline uint32_t fieldWidth(const FieldDescriptor& f) noexcept {
    return f.type == PrimType::FixedStr ? f.fixedLen : fixedSize(f.type);
}
```
  - In `DescriptorBuilder`, add an `addFixedStr` overload (mirrors `add`, sets type=FixedStr + fixedLen):
```cpp
DescriptorBuilder& addFixedStr(std::string name, uint16_t len,
                               uint16_t sinceVersion = 1, bool isKey = false) {
    FieldDescriptor f;
    f.name = std::move(name);
    f.id = static_cast<uint16_t>(fields_.size());
    f.type = PrimType::FixedStr;
    f.fixedLen = len;
    f.offset = 0;
    f.sinceVersion = sinceVersion;
    f.isKey = isKey;
    fields_.push_back(std::move(f));
    return *this;
}
```
  - In `build()`, the first pass (fixed-offset assignment) must use `fieldWidth`:
```cpp
uint32_t offset = 0;
for (auto& f : d.fields) {
    if (!isVarLen(f.type)) {   // FixedStr is not var-len → laid in the block
        f.offset = offset;
        offset  += fieldWidth(f);   // was fixedSize(f.type)
    }
}
d.blockLength = offset;
```
  (The var-index pass and the additive-invariant check are unchanged — FixedStr is never a var field.)

- [ ] **Step 5: Run the tests, verify pass. Run the FULL `rune_codec_tests` suite — all existing green (additive change).**

- [ ] **Step 6: Commit**
```bash
git add include/rune/codec/PrimType.h include/rune/codec/Descriptor.h tests/FixedStrLayoutTest.cpp CMakeLists.txt
git commit -m "feat(codec): FixedStr primitive + fixed-block layout (0c)"
```

---

## Task 2: `FixedStr` serialize / deserialize / fingerprint / toText

**Files:**
- Modify: `src/Serialize.cpp`, `include/rune/codec/Fingerprint.h`
- Test: `tests/FixedStrSerializeTest.cpp` (new)

**Interfaces:**
- Consumes: Task 1 (`FixedStr`, `fixedLen`, `addFixedStr`).
- Produces: descriptor blob carries `fixedLen` for FixedStr fields (backward-compatible); fingerprint absorbs `fixedLen`; `toText` renders `string(N)`.

- [ ] **Step 1: Write the failing test (`tests/FixedStrSerializeTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;

static Descriptor mk(uint16_t len) {
    return DescriptorBuilder("t",1).add("id",PrimType::Int64).addFixedStr("code",len).build();
}
TEST(FixedStrSerialize, RoundTripPreservesWidth) {
    Descriptor d = mk(8);
    Descriptor back = deserialize(serialize(d));
    ASSERT_NE(back.find("code"), nullptr);
    EXPECT_EQ(back.find("code")->type, PrimType::FixedStr);
    EXPECT_EQ(back.find("code")->fixedLen, 8u);
    EXPECT_EQ(back.blockLength, d.blockLength);
}
TEST(FixedStrSerialize, FingerprintDistinguishesWidth) {
    EXPECT_NE(fingerprint(mk(8)), fingerprint(mk(16)));
}
TEST(FixedStrSerialize, ExistingSchemasFingerprintUnchanged) {
    // A schema with NO FixedStr field must fingerprint exactly as before this change.
    Descriptor d = DescriptorBuilder("chars",1).add("id",PrimType::Int64)
        .add("userId",PrimType::Int64,1,true).add("name",PrimType::String).build();
    // Golden value captured from pre-0c rune-codec for this exact descriptor:
    // (implementer: paste the actual pre-change fingerprint here after computing it once)
    EXPECT_EQ(fingerprint(d), /*GOLDEN*/ fingerprint(deserialize(serialize(d))));
}
TEST(FixedStrSerialize, ToTextRendersFixedWidth) {
    std::string t = toText(mk(8));
    EXPECT_NE(t.find("string(8)"), std::string::npos);
}
```
> NOTE for the implementer: for `ExistingSchemasFingerprintUnchanged`, compute the fingerprint of that descriptor on **current main rune-codec** (pre-change) once and hard-code it as the golden, to prove the change did not perturb existing fingerprints. The self-referential form above is a placeholder to make the test compile; replace with the captured constant.

- [ ] **Step 2: Run it, verify the width/fingerprint assertions fail** (fixedLen not serialized/fingerprinted; toText lacks `(N)`).

- [ ] **Step 3: `Serialize.cpp` — serialize/deserialize `fixedLen` for FixedStr ONLY (backward-compatible), update `primTypeName`/`toText`, bump `kMaxType`.**
  - `primTypeName`: add `case PrimType::FixedStr: return "string";` (toText appends the width separately, below).
  - In `serialize()`'s per-field loop, AFTER the enum-values block, append (guarded by type):
```cpp
    if (f.type == PrimType::FixedStr) {
        push_u16le(buf, f.fixedLen);   // only FixedStr fields carry a width → old blobs unaffected
    }
```
  - In `deserialize()`: bump the type-range check to allow FixedStr, and read `fixedLen` after enum values when type is FixedStr; use `addFixedStr`:
```cpp
    constexpr uint8_t kMaxType = static_cast<uint8_t>(PrimType::FixedStr);   // was PrimType::Bytes
    ...
    // after reading enumValues:
    if (type == PrimType::FixedStr) {
        uint16_t fixedLen = read_u16le(blob, pos);
        builder.addFixedStr(std::move(fieldName), fixedLen, sinceVersion, isKeyByte != 0);
    } else {
        builder.add(std::move(fieldName), type, sinceVersion, isKeyByte != 0, std::move(enumValues));
    }
```
    (Restructure the existing single `builder.add(...)` call into this if/else.)
  - In `toText()`'s field loop, render the width for FixedStr. Replace the type column emit so a FixedStr shows `string(N)`:
```cpp
    if (f.type == PrimType::FixedStr) {
        out << padRight("string(" + std::to_string(f.fixedLen) + ")", kTypeWidth);
    } else {
        out << padRight(std::string(primTypeName(f.type)), kTypeWidth);
    }
```
    and it prints `@offset` (FixedStr is not var-len, so the existing `isVarLen` branch already emits `@`).

- [ ] **Step 4: `Fingerprint.h` — absorb `fixedLen` for FixedStr fields ONLY (so existing fingerprints are unchanged).** In the per-field loop, after the enum-values loop:
```cpp
        if (fp->type == PrimType::FixedStr) {
            hash = fnv1a_u16le(hash, fp->fixedLen);
        }
```

- [ ] **Step 5: Run the tests, verify pass; run the FULL `rune_codec_tests` — all existing green (existing fingerprints unchanged — prove via the golden test).**

- [ ] **Step 6: Commit**
```bash
git add src/Serialize.cpp include/rune/codec/Fingerprint.h tests/FixedStrSerializeTest.cpp CMakeLists.txt
git commit -m "feat(codec): FixedStr serialize/deserialize/fingerprint/toText (backward-compatible)"
```

---

## Task 3: `FixedStr` runtime read + validate

**Files:**
- Modify: `src/Record.cpp`, `src/Validator.cpp`
- Test: `tests/FixedStrReadValidateTest.cpp` (new)

**Interfaces:**
- Consumes: Tasks 1–2.
- Produces: `readField` returns a `string_view` over the FixedStr block bytes; `Validator` bounds-checks FixedStr within the block.

- [ ] **Step 1: Write the failing test (`tests/FixedStrReadValidateTest.cpp`)** — hand-frame a record for `{ id:int64@0, code:string(8)@8 }` (blockLength=16), then read + validate:
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Record.h>
#include <rune/codec/Validator.h>
#include <cstring>
using namespace rune::codec;

TEST(FixedStrRead, ReadsBlockBytesAsView) {
    Descriptor d = DescriptorBuilder("t",1).add("id",PrimType::Int64).addFixedStr("code",8).build();
    ASSERT_EQ(d.blockLength, 16u);
    std::vector<uint8_t> rec(4 + 16, 0);
    rec[0] = 16; rec[1] = 0; rec[2] = 1; rec[3] = 0;   // blockLength=16, version=1
    std::memcpy(rec.data()+4+8, "ABCD\0\0\0\0", 8);      // code at block offset 8
    FieldValue fv = readField(d, rec, d.find("code")->id);
    ASSERT_TRUE(fv.present);
    EXPECT_EQ(fv.type, PrimType::FixedStr);
    EXPECT_EQ(fv.bytes.size(), 8u);
    EXPECT_EQ(std::string_view(fv.bytes.data(), 4), "ABCD");   // caller trims trailing NULs
    Validator v(d);
    EXPECT_TRUE(v.validate(rec).ok);
}
TEST(FixedStrRead, ValidatorRejectsShortBlock) {
    Descriptor d = DescriptorBuilder("t",1).add("id",PrimType::Int64).addFixedStr("code",8).build();
    std::vector<uint8_t> rec(4 + 10, 0);   // blockLength claims 16 but only 10 present
    rec[0] = 16; rec[1] = 0; rec[2] = 1; rec[3] = 0;
    Validator v(d);
    EXPECT_FALSE(v.validate(rec).ok);
}
```

- [ ] **Step 2: Run it, verify it fails** (readField hits `default: return {}` for FixedStr; validator uses `fixedSize`=0 so no bounds check).

- [ ] **Step 3: `Record.cpp` — in the fixed branch, use `fieldWidth(*fd)` for `sz` and add a FixedStr case returning a view over the block bytes.**
  - Change `size_t sz = fixedSize(fd->type);` → `size_t sz = fieldWidth(*fd);`.
  - In the `switch (fd->type)`, before `default:`, add:
```cpp
            case PrimType::FixedStr:
                fv.bytes = std::string_view(reinterpret_cast<const char*>(record.data() + off), sz);
                break;
```
  (FixedStr is not var-len, so it correctly takes the fixed branch; `off = 4 + fd->offset`, `sz = fieldWidth`.)

- [ ] **Step 4: `Validator.cpp` — set the field-table entry `size` via `fieldWidth`.** Change `e.size = isVarLen(f.type) ? 0u : fixedSize(f.type);` → `e.size = isVarLen(f.type) ? 0u : fieldWidth(f);`. (The existing fixed-field bounds check `off + e.size > fixedEnd` then covers FixedStr; no whitelist applies.)

- [ ] **Step 5: Run the tests, verify pass; FULL suite green.**

- [ ] **Step 6: Commit**
```bash
git add src/Record.cpp src/Validator.cpp tests/FixedStrReadValidateTest.cpp CMakeLists.txt
git commit -m "feat(codec): FixedStr runtime read + validator bounds"
```

---

## Task 4: `FixedStr` codegen + end-to-end round-trip

**Files:**
- Modify: `src/Generator.cpp`
- Test: `tests/FixedStrGeneratedRoundTripTest.cpp` (new) + build-time gen wiring (mirror the existing generated round-trip test)

**Interfaces:**
- Consumes: Tasks 1–3.
- Produces: generated struct emits a `std::array<char,N>` member for FixedStr, encodes N bytes (NUL-pad short, reject over-length via `encode` returning 0 / an `encodeChecked`), decodes to the array; generated `encode` ⇄ runtime `readField` agree.

- [ ] **Step 1: Read the existing generated round-trip test** (`tests/GeneratedRoundTripTest.cpp`) + the build-time codegen custom command, to mirror the pattern for a descriptor containing a FixedStr.

- [ ] **Step 2: Write the failing test (`tests/FixedStrGeneratedRoundTripTest.cpp`)** — generate a header for `{ id:int64, code:string(8), name:string }` at build time (checked-in blob), then:
```cpp
#include <gtest/gtest.h>
#include "fixedstr_sample.gen.h"   // generated at build time from a checked-in blob
#include <rune/codec/Record.h>
#include <rune/codec/Validator.h>
using namespace rune::codec;
TEST(FixedStrGen, EncodeRoundTripsThroughReadField) {
    rune::gen::FixedstrSample s;
    s.id = 42;
    std::memcpy(s.code.data(), "ABCD", 4);   // std::array<char,8>, rest zero
    s.name = "hello";
    std::vector<uint8_t> buf(s.encodedSize());
    size_t n = s.encode(buf.data());
    buf.resize(n);
    const Descriptor& d = rune::gen::FixedstrSample::descriptor();
    EXPECT_TRUE(Validator(d).validate(buf).ok);
    FieldValue code = readField(d, buf, d.find("code")->id);
    ASSERT_TRUE(code.present);
    EXPECT_EQ(std::string_view(code.bytes.data(), 4), "ABCD");
    FieldValue name = readField(d, buf, d.find("name")->id);
    EXPECT_EQ(name.bytes, "hello");
}
```
  Provide the checked-in blob via a small committed generator step (reuse `make_characters_blob`-style) producing `{id:int64, code:FixedStr(8), name:String}`.

- [ ] **Step 3: Run it, verify it fails** (generator has no FixedStr case → member/encode/decode missing or wrong).

- [ ] **Step 4: `Generator.cpp` — handle FixedStr in member emission, encode, decode.**
  - `cppType`: FixedStr has a per-field width, so member emission can't use `cppType` alone. In the fixed-field member loop, special-case:
```cpp
    for (const auto& f : d.fields) {
        if (!isVarLen(f.type)) {
            if (f.type == PrimType::FixedStr)
                o << "    std::array<char, " << f.fixedLen << "> " << f.name << " = {};\n";
            else
                o << "    " << cppType(f.type) << " " << f.name << " = {};\n";
        }
    }
```
    Add `#include <array>` to the emitted header includes.
  - In the fixed-block `encode` switch, add a FixedStr case (write exactly fixedLen bytes from the array):
```cpp
                case PrimType::FixedStr:
                    o << "        std::memcpy(p, " << f->name << ".data(), " << f->fixedLen << "u); p += " << f->fixedLen << "u;\n";
                    break;
```
    (The array is fixed-size N; the member already holds exactly N bytes, NUL-padded — over-length is impossible at the typed layer since the member is `std::array<char,N>`. Document that assigning a longer C-string is the caller's responsibility; the array bound enforces N.)
  - In the fixed-block `decode` switch, add a FixedStr case (copy N block bytes into the array); the bounds guard uses `fieldWidth`:
    - Change the fixed-field guard `... + " << fixedSize(f->type) << "u ...` to emit `fieldWidth(*f)` for the width (so FixedStr uses `f->fixedLen`). Concretely, compute `const uint32_t w = fieldWidth(*f);` in the generator and emit `w` where `fixedSize(f->type)` was used for the guard and advance.
```cpp
                case PrimType::FixedStr:
                    o << "            std::memcpy(" << f->name << ".data(), p, " << f->fixedLen << "u); p += " << f->fixedLen << "u;\n";
                    break;
```
  - `encodedSize()` already sums `kBlockLength` (which includes FixedStr) + var fields — no change.

- [ ] **Step 5: Run the tests, verify pass; FULL `rune_codec_tests` green (incl. the existing GeneratedRoundTrip).**

- [ ] **Step 6: Commit**
```bash
git add src/Generator.cpp tests/FixedStrGeneratedRoundTripTest.cpp CMakeLists.txt
git commit -m "feat(codec): FixedStr codegen + end-to-end round-trip"
```

---

## PHASE 2 — rune-ql standalone library

## Task 5: rune-ql repo scaffold + AST

**Files:**
- Create: `/Users/vradu/Projects/rune-ql/` — `CMakeLists.txt`, `conanfile.py`, `.gitignore`, `include/rune/ql/Ast.h`, `tests/AstSmokeTest.cpp`

**Interfaces:**
- Produces: the repo builds; `rune::ql::{FieldDef, TopicDef, ParseError}` exist; consumes rune-codec via `RUNE_CODEC_ROOT` sibling source lib.

- [ ] **Step 1: `git init` the repo; scaffold CMake mirroring rune-codec.** `CMakeLists.txt`: C++23, `rune_ql` STATIC lib over `src/*.cpp`, `RUNE_CODEC_ROOT` CACHE PATH default `../rune-codec` compiled in as a sibling source lib (mirror how the broker consumed rune-codec: add `${RUNE_CODEC_ROOT}/src/{Serialize,Record,Validator,Generator}.cpp` + include `${RUNE_CODEC_ROOT}/include`), a `rune_ql_tests` GoogleTest target with `gtest_discover_tests`, and a `rune_ql_bench` target (Google Benchmark). `conanfile.py`/`.gitignore` mirror rune-codec.

- [ ] **Step 2: Write `include/rune/ql/Ast.h`**
```cpp
#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <rune/codec/PrimType.h>
namespace rune::ql {
struct FieldDef { std::string name; rune::codec::PrimType type = rune::codec::PrimType::Int32; uint16_t fixedLen = 0; };
struct TopicDef { std::string name; std::vector<FieldDef> fields; uint32_t partitions = 1; std::string keyField; };
struct ParseError { std::size_t line = 0; std::size_t col = 0; std::string message; };
} // namespace rune::ql
```

- [ ] **Step 3: Write a smoke test (`tests/AstSmokeTest.cpp`)** constructing a `TopicDef` and asserting fields; register in `rune_ql_tests`.
```cpp
#include <gtest/gtest.h>
#include "rune/ql/Ast.h"
TEST(Ast, Constructs) {
    rune::ql::TopicDef t{"characters", {{"id", rune::codec::PrimType::Int64, 0}}, 16, "id"};
    EXPECT_EQ(t.name, "characters");
    EXPECT_EQ(t.partitions, 16u);
    EXPECT_EQ(t.fields.size(), 1u);
}
```

- [ ] **Step 4: Configure + build + run** (native cmake), verify green.
Run: `<cmake> -S . -B build -DBUILD_TESTING=ON -DRUNE_CODEC_ROOT=/Users/vradu/Projects/rune-codec && <cmake> --build build && (cd build && ctest --output-on-failure)`

- [ ] **Step 5: Commit**
```bash
git add -A && git commit -m "chore(rune-ql): scaffold repo + AST"
```

---

## Task 6: tokenizer + recursive-descent parser

**Files:**
- Create: `include/rune/ql/Parser.h`, `src/Parser.cpp`, `tests/ParserTest.cpp`

**Interfaces:**
- Consumes: `Ast.h` (Task 5).
- Produces: `std::expected<TopicDef, ParseError> rune::ql::parse(std::string_view src) noexcept;` implementing the §6.1 grammar with §6.3 semantic checks.

- [ ] **Step 1: Write the failing tests (`tests/ParserTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include "rune/ql/Parser.h"
using namespace rune::ql; using rune::codec::PrimType;

TEST(Parser, ParsesCharacters) {
    auto r = parse("topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;");
    ASSERT_TRUE(r.has_value());
    const TopicDef& t = *r;
    EXPECT_EQ(t.name, "characters");
    EXPECT_EQ(t.partitions, 16u);
    EXPECT_EQ(t.keyField, "userId");
    ASSERT_EQ(t.fields.size(), 4u);
    EXPECT_EQ(t.fields[0].type, PrimType::Int64);
    EXPECT_EQ(t.fields[2].type, PrimType::String);   // variable
    EXPECT_EQ(t.fields[3].type, PrimType::FixedStr);  // string(8)
    EXPECT_EQ(t.fields[3].fixedLen, 8u);
}
TEST(Parser, LineCommentsAndTrailingComma) {
    auto r = parse("-- a topic\ntopic t { a int32, } partitions 1 key a;");
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->fields.size(), 1u);
}
TEST(Parser, RejectsUnknownType) {
    auto r = parse("topic t { a int128 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("type"), std::string::npos);
}
TEST(Parser, RejectsDuplicateField) {
    auto r = parse("topic t { a int32, a int64 } partitions 1 key a;");
    ASSERT_FALSE(r.has_value());
}
TEST(Parser, RejectsKeyNotAField) {
    auto r = parse("topic t { a int32 } partitions 1 key b;");
    ASSERT_FALSE(r.has_value());
    EXPECT_NE(r.error().message.find("key"), std::string::npos);
}
TEST(Parser, RejectsZeroPartitionsAndZeroWidth) {
    EXPECT_FALSE(parse("topic t { a int32 } partitions 0 key a;").has_value());
    EXPECT_FALSE(parse("topic t { a string(0) } partitions 1 key a;").has_value());
}
TEST(Parser, ErrorHasLocation) {
    auto r = parse("topic t { a int32 }\npartitions x key a;");   // 'x' not an int
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().line, 2u);
}
```

- [ ] **Step 2: Run, verify fail** (no parser).

- [ ] **Step 3: Implement `Parser.h`/`Parser.cpp`.** A tiny tokenizer (tracks line/col; skips whitespace + `--` line comments; token kinds: IDENT, INT, `{ } ( ) , ;`, EOF) and a single-pass recursive-descent parser producing `TopicDef` or `ParseError{line,col,message}`. Keyword→type map: `int32→Int32, int64→Int64, uint16→UInt16, uint32→UInt32, uint64→UInt64, float64→Float64, bool→Bool, bytes→Bytes, string→String` and `string ( INT )→FixedStr, fixedLen=INT`. Grammar: `topic IDENT { field (, field)* ,? } partitions INT key IDENT ;`. Semantic checks per §6.3: duplicate field name, key references a declared field, `partitions>=1`, `string(N)` `N>=1`, unknown type. Signature:
```cpp
// Parser.h
#pragma once
#include <expected>
#include <string_view>
#include "rune/ql/Ast.h"
namespace rune::ql { std::expected<TopicDef, ParseError> parse(std::string_view src) noexcept; }
```
  Keep it allocation-lean but correctness-first (cold path). No exceptions escape (`noexcept`, returns `ParseError`).

- [ ] **Step 4: Run the tests, verify all pass.**

- [ ] **Step 5: Commit**
```bash
git add include/rune/ql/Parser.h src/Parser.cpp tests/ParserTest.cpp CMakeLists.txt && git commit -m "feat(rune-ql): tokenizer + recursive-descent DDL parser"
```

---

## Task 7: backends (`toDescriptor`, `applyPlan`) + parse benchmark

**Files:**
- Create: `include/rune/ql/Backends.h`, `src/Backends.cpp`, `tests/BackendsTest.cpp`, `benchmark/ParseBench.cpp`

**Interfaces:**
- Consumes: `Ast.h`, `Parser.h`, rune-codec (`DescriptorBuilder`, `serialize`, `fingerprint`, `generateHeader`).
- Produces: `rune::codec::Descriptor toDescriptor(const TopicDef&)`; `struct DefineTopicPlan{std::string name; uint32_t partitions; std::string keyField; std::vector<uint8_t> descriptorBlob;}`; `DefineTopicPlan applyPlan(const TopicDef&)`.

- [ ] **Step 1: Write the failing tests (`tests/BackendsTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include "rune/ql/Parser.h"
#include "rune/ql/Backends.h"
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Generator.h>
using namespace rune::ql;
TEST(Backends, ToDescriptorMatchesFields) {
    auto t = parse("topic characters { id int64, userId int64, name string, code string(8) } partitions 16 key userId;").value();
    auto d = toDescriptor(t);
    EXPECT_EQ(d.name, "characters");
    EXPECT_TRUE(d.find("userId")->isKey);
    EXPECT_EQ(d.find("code")->type, rune::codec::PrimType::FixedStr);
    EXPECT_EQ(d.find("code")->fixedLen, 8u);
}
TEST(Backends, ApplyPlanBlobDeserializes) {
    auto t = parse("topic t { id int64, name string } partitions 4 key id;").value();
    auto plan = applyPlan(t);
    EXPECT_EQ(plan.name, "t");
    EXPECT_EQ(plan.partitions, 4u);
    EXPECT_EQ(plan.keyField, "id");
    auto back = rune::codec::deserialize(plan.descriptorBlob);
    EXPECT_EQ(rune::codec::fingerprint(back), rune::codec::fingerprint(toDescriptor(t)));
}
TEST(Backends, CodegenCompilesFromDescriptor) {
    auto t = parse("topic t { id int64, code string(8) } partitions 1 key id;").value();
    std::string hdr = rune::codec::generateHeader(toDescriptor(t));
    EXPECT_NE(hdr.find("struct T"), std::string::npos);
    EXPECT_NE(hdr.find("std::array<char, 8>"), std::string::npos);
}
```

- [ ] **Step 2: Run, verify fail.**

- [ ] **Step 3: Implement `Backends.h`/`Backends.cpp`.** `toDescriptor`: build via `DescriptorBuilder`, using `addFixedStr(name,fixedLen,1,isKey)` for FixedStr fields and `add(name,type,1,isKey)` otherwise, with `isKey = (name == td.keyField)`. `applyPlan`: `{ td.name, td.partitions, td.keyField, serialize(toDescriptor(td)) }`.
```cpp
// Backends.h
#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "rune/ql/Ast.h"
#include <rune/codec/Descriptor.h>
namespace rune::ql {
[[nodiscard]] rune::codec::Descriptor toDescriptor(const TopicDef&);
struct DefineTopicPlan { std::string name; uint32_t partitions; std::string keyField; std::vector<uint8_t> descriptorBlob; };
[[nodiscard]] DefineTopicPlan applyPlan(const TopicDef&);
}
```

- [ ] **Step 4: Run tests, verify pass.**

- [ ] **Step 5: Parse benchmark (`benchmark/ParseBench.cpp`)** — Google Benchmark measuring `parse()` of the `characters` DDL; report ns/parse to validate the microsecond-parse claim. Register `rune_ql_bench`.

- [ ] **Step 6: Commit**
```bash
git add include/rune/ql/Backends.h src/Backends.cpp tests/BackendsTest.cpp benchmark/ParseBench.cpp CMakeLists.txt && git commit -m "feat(rune-ql): toDescriptor/applyPlan backends + parse benchmark"
```

---

## PHASE 3 — `rune` CLI + e2e

## Task 8: CLI build integration + `generate` + `apply --dry-run`

**Files:**
- Modify: `rune-client-cpp/CMakeLists.txt` (add `RUNE_QL_ROOT` + `rune_ql` sibling lib + `rune` executable)
- Create: `rune-client-cpp/tools/rune_cli.cpp`, `rune-client-cpp/tests/RuneCliOfflineTest.cpp`

**Interfaces:**
- Consumes: rune-ql (`parse`, `toDescriptor`, `applyPlan`), rune-codec (`generateHeader`, `deserialize`, `toText`), the existing `rune_schema_gen` logic (`rune::tools::generateFromFile`).
- Produces: a `rune` executable with `schema generate` (offline `--from-file` + live) and `schema apply --dry-run`; a testable `runCli(args)->int` entrypoint.

- [ ] **Step 1: CMake — add `RUNE_QL_ROOT` (CACHE PATH default `../rune-ql`) as a sibling source lib** (compile `${RUNE_QL_ROOT}/src/*.cpp`, include `${RUNE_QL_ROOT}/include`), and an `add_executable(rune tools/rune_cli.cpp)` linking rune-ql + rune_codec + the client lib. Mirror the Task-2/9 rune-codec integration from 0b.

- [ ] **Step 2: Write the failing offline test (`tests/RuneCliOfflineTest.cpp`)** — factor CLI logic into `int rune::cli::run(std::vector<std::string> args, std::ostream& out, std::ostream& err)` so it's testable:
```cpp
#include <gtest/gtest.h>
#include <sstream>
#include <filesystem>
#include <fstream>
#include "rune/cli/Cli.h"   // exposes rune::cli::run
namespace fs = std::filesystem;
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
```

- [ ] **Step 3: Implement `tools/rune_cli.cpp` (+ a small `include/rune/cli/Cli.h`).** `rune::cli::run(args,out,err)` dispatches `schema apply|get|generate|list`. For this task implement: `apply --dry-run` (read file/stdin → `rune::ql::parse` → on error print `<file>:<line>:<col>: <msg>` to err, return 2 → else `toText(toDescriptor(td))` to out); `generate --from-file <blob> --out <dir>` (reuse `rune::tools::generateFromFile`) and `generate <topic> --out <dir> --broker <ep>` (live: connect, `getTopicSchema`, `generateHeader`). `main()` = `return rune::cli::run(argv→vector, std::cout, std::cerr);`. Broker endpoint from `--broker` or `RUNE_BROKER`.

- [ ] **Step 4: Build (x86_64 cross flow) + run the offline tests, verify pass.** Build the `rune` target + the offline test target; the offline tests link rune-ql + rune-codec (no aeron needed). Report commands.

- [ ] **Step 5: Commit**
```bash
git add CMakeLists.txt tools/rune_cli.cpp include/rune/cli/Cli.h tests/RuneCliOfflineTest.cpp && git commit -m "feat(client): rune CLI skeleton + schema generate + apply --dry-run"
```

---

## Task 9: `rune schema apply` (live) + `get` + `list`

**Files:**
- Modify: `tools/rune_cli.cpp`, `include/rune/cli/Cli.h`
- Test: `tests/RuneCliLiveTest.cpp` (loopback, mirroring `ClientSchemaTest`)

**Interfaces:**
- Consumes: 0b `Client::defineTopic`/`getTopicSchema` + the metadata/topic-list path; rune-ql `applyPlan`; rune-codec `deserialize`/`toText`.
- Produces: `schema apply` (live define), `schema get` (fetch+toText), `schema list` (metadata).

- [ ] **Step 1: Investigate** how the client exposes a topic list (MetadataRequest/MetadataCache) — confirm the API for `schema list` (names+ids), or add a thin client helper. Confirm the in-process loopback harness used by `ClientSchemaTest` (Task 10 of 0b) for testing live paths without a real broker.

- [ ] **Step 2: Write the failing test (`tests/RuneCliLiveTest.cpp`)** — using the in-process loopback gateway (as `ClientSchemaTest` did): `rune::cli::run({"schema","apply","c.rql","--broker",<loopback>})` → assert the loopback saw a `DefineTopic` with the right name/partitions/keyField and the descriptor blob deserializes to the parsed topic's fingerprint; `schema get` returns the toText; `schema list` lists the topic. (Reuse the loopback fixture.)

- [ ] **Step 3: Implement** the live `apply` (parse → `applyPlan` → `client.defineTopic(name,partitions,keyField,validate,blob)`; `--validate` flag; `TopicExists` → print note, return 0), `get` (`getTopicSchema` → `deserialize` → `toText`; `UnknownTopic` → err+nonzero), `list` (metadata topic list; `--verbose` fetches version/fingerprint per topic).

- [ ] **Step 4: Build + run, verify pass.**

- [ ] **Step 5: Commit**
```bash
git add tools/rune_cli.cpp include/rune/cli/Cli.h tests/RuneCliLiveTest.cpp && git commit -m "feat(client): rune schema apply/get/list (live)"
```

---

## Task 10: single-node Docker real-Aeron e2e (0c #11 gate)

**Files:**
- Create: `rune-client-cpp/tests/e2e/characters.rql` (checked-in), a probe + orchestration (adapt the 0b T13 `docker/schema-e2e.sh` + `Dockerfile.schema-probe`)

**Interfaces:**
- Consumes: the `rune` CLI, the 0b Docker broker image wiring (rune-codec build context), the T13 typed-probe harness.

- [ ] **Step 1: Adapt the 0b T13 harness** so the probe/e2e uses the **`rune` CLI** end to end: (a) `rune schema apply tests/e2e/characters.rql --broker <node> --validate` against the real single-node broker; (b) `rune schema generate characters --out <gen> --broker <node>` → `characters.gen.h`; (c) compile a typed probe using the generated header; (d) probe: `openTopic<Characters>` → typed produce (incl. a `string(N)` field) → consume → decode → assert field round-trip; (e) assert a malformed record is rejected (validate=on). The Docker broker image must build with the rune-codec + rune-ql build contexts (extend the 0b compose/Dockerfile). `characters.rql` includes a `string(N)` field to exercise FixedStr end-to-end.

- [ ] **Step 2: Run `bash docker/rune-ql-e2e.sh`** (or the adapted script). Verify it prints the success marker + exits 0 against a REAL broker over Aeron. If the Docker build cannot complete in the environment, report BLOCKED with specifics + the in-process fallback (Tasks 8–9 offline/loopback green) — do NOT fabricate.

- [ ] **Step 3: Commit**
```bash
git add tests/e2e/ docker/ && git commit -m "test(client): 0c rune-ql CLI single-node real-Aeron e2e"
```

---

## Notes on cross-repo ordering & execution
- **Order:** rune-codec Phase 1 (Tasks 1–4) → rune-ql Phase 2 (Tasks 5–7, consume the extended rune-codec via `RUNE_CODEC_ROOT`) → rune-client-cpp Phase 3 (Tasks 8–10, consume rune-ql via `RUNE_QL_ROOT` + rune-codec + the client). Sequential execution satisfies the dependencies.
- **rune-codec is modified this sub-project** (unlike 0b): the FixedStr change lands on a `feature/0c-rune-ql` worktree of rune-codec off `main`; consumers point `RUNE_CODEC_ROOT` at it during 0c so they build against FixedStr.
- **#11 real-e2e gate:** the CLI→broker typed round-trip (Task 10) over real Aeron is the branch gate; intermediate parser/backend/offline tests validate the rest.
- **Regression suite:** name regression tests `*Regression*`; run the full regression suite first on every fix.
