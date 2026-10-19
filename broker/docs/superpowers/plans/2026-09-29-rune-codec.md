# rune-codec (Sub-project 0a) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `rune-codec`, a standalone C++ library + generator that defines a topic record's type as a serializable/human-readable **descriptor**, generates a zero-copy fixed-layout **codec**, computes a stable **fingerprint**, and provides broker-side **runtime field access + validation** — with no compiled-in knowledge of any specific schema.

**Architecture:** A `Descriptor` is a flat, ordered field set laid out SBE-style: fixed-width fields occupy a contiguous **block** at deterministic offsets; variable-length fields (`string`/`bytes`) follow as length-prefixed **var-data**. A record on the wire is `[blockLength:u16][version:u16][fixed block][var-data]`. Everything downstream (generated codec, runtime reader, validator, fingerprint) is derived from the `Descriptor`, so the broker can read/validate any topic's records at runtime given only its descriptor.

**Tech Stack:** C++23, CMake (≥3.25), Conan 2, GoogleTest, Google Benchmark. Fingerprint uses FNV-1a-64 (no external dep). No other runtime dependencies.

**Spec:** `docs/superpowers/specs/2026-09-29-rune-materialized-views-typed-topics-design.md` (see §3 for 0a).

## Global Constraints

- **Language:** C++23. **Build:** CMake ≥3.25 + Conan 2. Match rune-protocol conventions (header-first, `namespace rune::codec`).
- **New standalone repo** `rune-codec` at `/Users/vradu/Projects/rune-codec` (sibling to rune-protocol). No dependency on rune-broker / rune-protocol.
- **Zero-copy on read:** `readField`/generated `decode` return views into the caller's buffer; no allocation on the read path.
- **Determinism:** the fingerprint is a pure function of the canonical descriptor — identical across machines/runs.
- **Primitive set (v1):** `INT32, INT64, UINT16, UINT32, UINT64, FLOAT64, BOOL, ENUM(int32-backed), STRING, BYTES`. `STRING`/`BYTES` are var-data; all others are fixed-width.
- **Record framing:** `[blockLength:u16 LE][version:u16 LE][fixed block][ for each var field: len:u32 LE + bytes ]`. All integers little-endian.
- **Additive-only evolution:** new fields append (higher `sinceVersion`); offsets of existing fields never change.

---

### Task 1: Repo scaffolding + toolchain smoke test

**Files:**
- Create: `/Users/vradu/Projects/rune-codec/CMakeLists.txt`
- Create: `/Users/vradu/Projects/rune-codec/conanfile.py`
- Create: `/Users/vradu/Projects/rune-codec/include/rune/codec/Version.h`
- Test: `/Users/vradu/Projects/rune-codec/tests/SmokeTest.cpp`

**Interfaces:**
- Produces: a buildable CMake project with a `rune_codec` library target and a `rune_codec_tests` gtest target; `rune::codec::kVersion` constant.

- [ ] **Step 1: Write the failing test**
```cpp
// tests/SmokeTest.cpp
#include <gtest/gtest.h>
#include <rune/codec/Version.h>
TEST(Smoke, VersionIsSet) { EXPECT_GE(rune::codec::kVersion, 1); }
```

- [ ] **Step 2: Scaffold the project**
```python
# conanfile.py
from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout
class RuneCodec(ConanFile):
    name = "rune-codec"; version = "0.1.0"; settings = "os","compiler","build_type","arch"
    generators = "CMakeDeps","CMakeToolchain"
    def requirements(self):
        self.test_requires("gtest/1.14.0"); self.test_requires("benchmark/1.8.2")
    def layout(self): cmake_layout(self)
```
```cmake
# CMakeLists.txt
cmake_minimum_required(VERSION 3.25)
project(rune_codec CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(GTest REQUIRED)
add_library(rune_codec INTERFACE)                 # header-only for now; src added later
target_include_directories(rune_codec INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
enable_testing()
add_executable(rune_codec_tests tests/SmokeTest.cpp)
target_link_libraries(rune_codec_tests PRIVATE rune_codec GTest::gtest_main)
include(GoogleTest); gtest_discover_tests(rune_codec_tests)
```
```cpp
// include/rune/codec/Version.h
#pragma once
namespace rune::codec { inline constexpr int kVersion = 1; }
```

- [ ] **Step 3: Configure + build**
Run: `conan install . --build=missing -s build_type=Debug && cmake --preset conan-debug && cmake --build --preset conan-debug`
Expected: builds, `rune_codec_tests` links.

- [ ] **Step 4: Run test**
Run: `ctest --preset conan-debug --output-on-failure`
Expected: PASS (1 test).

- [ ] **Step 5: Commit**
```bash
git init && git add -A && git commit -m "chore: scaffold rune-codec (CMake+Conan+gtest smoke)"
```

---

### Task 2: PrimType + fixed sizes

**Files:**
- Create: `include/rune/codec/PrimType.h`
- Test: `tests/PrimTypeTest.cpp` (add to `rune_codec_tests` sources)

**Interfaces:**
- Produces: `enum class PrimType : uint8_t { Int32, Int64, UInt16, UInt32, UInt64, Float64, Bool, Enum, String, Bytes };`
  `constexpr bool isVarLen(PrimType);` `constexpr uint32_t fixedSize(PrimType);` (var-len → 0).

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/PrimType.h>
using rune::codec::PrimType; using rune::codec::fixedSize; using rune::codec::isVarLen;
TEST(PrimType, FixedSizes) {
  EXPECT_EQ(fixedSize(PrimType::Int32), 4u);
  EXPECT_EQ(fixedSize(PrimType::Int64), 8u);
  EXPECT_EQ(fixedSize(PrimType::UInt16), 2u);
  EXPECT_EQ(fixedSize(PrimType::Float64), 8u);
  EXPECT_EQ(fixedSize(PrimType::Bool), 1u);
  EXPECT_EQ(fixedSize(PrimType::Enum), 4u);            // int32-backed
  EXPECT_TRUE(isVarLen(PrimType::String));
  EXPECT_TRUE(isVarLen(PrimType::Bytes));
  EXPECT_EQ(fixedSize(PrimType::String), 0u);
}
```

- [ ] **Step 2: Run test to verify it fails** — Run: `ctest --preset conan-debug -R PrimType`; Expected: FAIL (header not found).

- [ ] **Step 3: Implement**
```cpp
// include/rune/codec/PrimType.h
#pragma once
#include <cstdint>
namespace rune::codec {
enum class PrimType : uint8_t { Int32, Int64, UInt16, UInt32, UInt64, Float64, Bool, Enum, String, Bytes };
constexpr bool isVarLen(PrimType t) { return t == PrimType::String || t == PrimType::Bytes; }
constexpr uint32_t fixedSize(PrimType t) {
  switch (t) {
    case PrimType::Int32: case PrimType::UInt32: case PrimType::Enum: return 4;
    case PrimType::Int64: case PrimType::UInt64: case PrimType::Float64: return 8;
    case PrimType::UInt16: return 2;
    case PrimType::Bool: return 1;
    case PrimType::String: case PrimType::Bytes: return 0;
  }
  return 0;
}
} // namespace rune::codec
```
Add `tests/PrimTypeTest.cpp` to the `add_executable(rune_codec_tests ...)` source list.

- [ ] **Step 4: Run test** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): PrimType + fixed sizes"`

---

### Task 3: Descriptor + DescriptorBuilder (layout & id/offset assignment)

**Files:**
- Create: `include/rune/codec/Descriptor.h`
- Test: `tests/DescriptorTest.cpp`

**Interfaces:**
- Consumes: `PrimType`, `fixedSize`, `isVarLen`.
- Produces:
```cpp
struct FieldDescriptor {
  std::string name; uint16_t id; PrimType type;
  uint32_t offset;        // byte offset within the fixed block (fixed fields); var index for var fields
  uint16_t sinceVersion;
  bool     isKey = false;
  // enum/range constraints (optional, used by validator):
  std::vector<int32_t> enumValues;    // non-empty ⇒ Enum whitelist
};
struct Descriptor {
  std::string name; uint16_t version = 1; uint32_t blockLength = 0;
  std::vector<FieldDescriptor> fields;   // in declaration order
  const FieldDescriptor* find(std::string_view n) const noexcept;
};
class DescriptorBuilder {
 public:
  explicit DescriptorBuilder(std::string schemaName, uint16_t version = 1);
  DescriptorBuilder& add(std::string name, PrimType type,
                         uint16_t sinceVersion = 1, bool isKey = false,
                         std::vector<int32_t> enumValues = {});
  Descriptor build();     // assigns ids (declaration order, 0-based), offsets, blockLength
};
```
Layout rule: **fixed fields first (in declaration order) at increasing offsets = running block size; then var fields (var index 0,1,…). `blockLength` = total fixed bytes.** `id` = declaration index.

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Descriptor.h>
using namespace rune::codec;
TEST(Descriptor, LayoutFixedThenVar) {
  auto d = DescriptorBuilder("characters")
    .add("id", PrimType::Int64)
    .add("userId", PrimType::Int64, 1, /*isKey=*/true)
    .add("name", PrimType::String)         // var
    .add("strength", PrimType::Int32)
    .build();
  EXPECT_EQ(d.name, "characters");
  EXPECT_EQ(d.blockLength, 20u);           // int64+int64+int32 = 8+8+4
  EXPECT_EQ(d.find("id")->offset, 0u);
  EXPECT_EQ(d.find("userId")->offset, 8u);
  EXPECT_TRUE(d.find("userId")->isKey);
  EXPECT_EQ(d.find("strength")->offset, 16u);
  EXPECT_EQ(d.find("name")->offset, 0u);   // var index 0
  EXPECT_TRUE(isVarLen(d.find("name")->type));
  EXPECT_EQ(d.find("id")->id, 0u);
  EXPECT_EQ(d.find("strength")->id, 3u);
}
```

- [ ] **Step 2: Run to verify fail.**

- [ ] **Step 3: Implement** `Descriptor.h` — builder stores fields in declaration order (assigning `id` = index), then `build()` walks fields twice: first pass assigns fixed offsets (running sum of `fixedSize`) and computes `blockLength`; second pass assigns var-index offsets to var-len fields. `find` is a linear scan (field counts are small).

- [ ] **Step 4: Run test** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): Descriptor + builder with SBE-style layout"`

---

### Task 4: Fingerprint (FNV-1a-64 over the canonical descriptor)

**Files:**
- Create: `include/rune/codec/Fingerprint.h`
- Test: `tests/FingerprintTest.cpp`

**Interfaces:**
- Consumes: `Descriptor`.
- Produces: `uint64_t fingerprint(const Descriptor&) noexcept;` — hashes, in canonical order (fields sorted by `id`): schema name, then per field `{id, type, sinceVersion, isKey, name, enumValues}`. Independent of `offset`/`blockLength` (derived) but includes everything that affects wire compatibility.

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;
static Descriptor base() {
  return DescriptorBuilder("t").add("a", PrimType::Int64).add("b", PrimType::Int32).build();
}
TEST(Fingerprint, DeterministicAndSensitive) {
  EXPECT_EQ(fingerprint(base()), fingerprint(base()));                 // deterministic
  auto added = DescriptorBuilder("t").add("a",PrimType::Int64)
                 .add("b",PrimType::Int32).add("c",PrimType::Bool,2).build();
  EXPECT_NE(fingerprint(base()), fingerprint(added));                  // additive change ⇒ new fp
  auto retyped = DescriptorBuilder("t").add("a",PrimType::Int32).add("b",PrimType::Int32).build();
  EXPECT_NE(fingerprint(base()), fingerprint(retyped));                // type change ⇒ new fp
}
```

- [ ] **Step 2: Run to verify fail.**

- [ ] **Step 3: Implement** — FNV-1a-64: seed `0xcbf29ce484222325`, prime `0x100000001b3`; feed the canonical bytes (schema name, then for each field by id: id LE16, `uint8_t(type)`, sinceVersion LE16, isKey byte, name bytes, each enum value LE32). Pure `constexpr`-friendly loop.

- [ ] **Step 4: Run test** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): deterministic FNV-1a fingerprint"`

---

### Task 5: Descriptor binary (de)serialization

**Files:**
- Create: `include/rune/codec/Serialize.h`, `src/Serialize.cpp`
- Test: `tests/SerializeTest.cpp`
- Modify: `CMakeLists.txt` (switch `rune_codec` to a compiled STATIC lib; add `src/Serialize.cpp`)

**Interfaces:**
- Consumes: `Descriptor`.
- Produces: `std::vector<uint8_t> serialize(const Descriptor&);` `Descriptor deserialize(std::span<const uint8_t>);` (throws `std::runtime_error` on malformed input). This blob is the `descriptorBlob` that 0b ships over the wire and stores in the registry.

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;
TEST(Serialize, RoundTrip) {
  auto d = DescriptorBuilder("characters")
    .add("id",PrimType::Int64).add("userId",PrimType::Int64,1,true)
    .add("type",PrimType::Enum,1,false,{0,1,2}).add("name",PrimType::String)
    .add("strength",PrimType::Int32).build();
  auto blob = serialize(d);
  auto d2 = deserialize(blob);
  EXPECT_EQ(fingerprint(d), fingerprint(d2));      // structurally identical
  EXPECT_EQ(d2.blockLength, d.blockLength);
  EXPECT_EQ(d2.find("type")->enumValues, (std::vector<int32_t>{0,1,2}));
}
TEST(Serialize, RejectsTruncated) {
  auto blob = serialize(DescriptorBuilder("t").add("a",PrimType::Int64).build());
  blob.resize(blob.size()/2);
  EXPECT_THROW(deserialize(blob), std::runtime_error);
}
```

- [ ] **Step 2: Run to verify fail.**

- [ ] **Step 3: Implement** a compact LE format: `[magic u32 "RCD1"][nameLen u16][name][version u16][fieldCount u16]` then per field `[nameLen u16][name][id u16][type u8][sinceVersion u16][isKey u8][enumCount u16][enum values u32…]`. `deserialize` bounds-checks every read and re-runs `DescriptorBuilder` (so offsets/blockLength are recomputed, not trusted from the blob). Update `CMakeLists.txt`: `add_library(rune_codec STATIC src/Serialize.cpp)` + `target_include_directories(... PUBLIC include)`.

- [ ] **Step 4: Run test** — Expected: PASS (both).
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): descriptor binary serialize/deserialize"`

---

### Task 6: Human-readable descriptor render (`toText`)

**Files:**
- Modify: `include/rune/codec/Serialize.h`, `src/Serialize.cpp`
- Test: `tests/ToTextTest.cpp`

**Interfaces:**
- Produces: `std::string toText(const Descriptor&);` — the human-readable form from spec §3.1 (`schema <name> fp=0x… block=N version=V { <field> … }`, one field per line with `name type @offset [key] [sinceVersion=X] [enum(…)] / vardata#i`).

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Serialize.h>
using namespace rune::codec;
TEST(ToText, Golden) {
  auto d = DescriptorBuilder("characters").add("id",PrimType::Int64)
    .add("userId",PrimType::Int64,1,true).add("strength",PrimType::Int32)
    .add("name",PrimType::String).build();
  const std::string t = toText(d);
  EXPECT_NE(t.find("schema characters"), std::string::npos);
  EXPECT_NE(t.find("id        int64   @0"), std::string::npos);
  EXPECT_NE(t.find("userId    int64   @8    key"), std::string::npos);
  EXPECT_NE(t.find("name      string  vardata#0"), std::string::npos);
}
```

- [ ] **Step 2: Run to verify fail.**
- [ ] **Step 3: Implement** `toText` — format the header line (with `fingerprint`), then each field aligned; fixed fields print `@offset`, var fields print `vardata#<index>`; append `key`, `sinceVersion=N` (when >1), `enum(v,…)` as applicable.
- [ ] **Step 4: Run test** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): human-readable descriptor render"`

---

### Task 7: Record framing + runtime field reader

**Files:**
- Create: `include/rune/codec/Record.h`, `src/Record.cpp`
- Test: `tests/RecordReaderTest.cpp`

**Interfaces:**
- Consumes: `Descriptor`.
- Produces:
  - Framing constants: record = `[blockLength u16][version u16][fixed block][ (len u32 + bytes) per var field ]`.
  - `struct FieldValue { PrimType type; bool present; int64_t i=0; uint64_t u=0; double f=0; bool b=false; std::string_view bytes; };`
  - `FieldValue readField(const Descriptor&, std::span<const uint8_t> record, uint16_t fieldId) noexcept;` — reads the record's own `version`/`blockLength` from the header; a field with `sinceVersion > record.version` returns `present=false`; fixed fields read at `header + offset`; var fields walk the var-data tail by index. Zero-copy (`string_view` into `record`).

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Record.h>
using namespace rune::codec;
// hand-frame a v1 record for {id:int64, userId:int64, strength:int32, name:string}
static std::vector<uint8_t> frame(int64_t id,int64_t uid,int32_t str,std::string_view name){
  std::vector<uint8_t> b; auto le=[&](auto v){ for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i))); };
  uint16_t block=20, ver=1; le(block); le(ver); le(id); le(uid); le(str);
  uint32_t nlen=uint32_t(name.size()); le(nlen); b.insert(b.end(),name.begin(),name.end()); return b;
}
TEST(RecordReader, ReadsFields) {
  auto d = DescriptorBuilder("c").add("id",PrimType::Int64).add("userId",PrimType::Int64)
             .add("strength",PrimType::Int32).add("name",PrimType::String).build();
  auto rec = frame(1001, 42, 7, "Bob");
  EXPECT_EQ(readField(d, rec, d.find("id")->id).i, 1001);
  EXPECT_EQ(readField(d, rec, d.find("userId")->id).i, 42);
  EXPECT_EQ(readField(d, rec, d.find("strength")->id).i, 7);
  EXPECT_EQ(readField(d, rec, d.find("name")->id).bytes, "Bob");
}
TEST(RecordReader, FieldNewerThanRecordVersionAbsent) {
  auto d = DescriptorBuilder("c").add("id",PrimType::Int64)
             .add("luck",PrimType::Int32,/*sinceVersion=*/2).build();
  // v1 record (block=8, ver=1, just id)
  std::vector<uint8_t> b; auto le=[&](auto v){for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i)));};
  uint16_t block=8,ver=1; le(block); le(ver); le(int64_t(5));
  EXPECT_TRUE(readField(d, b, d.find("id")->id).present);
  EXPECT_FALSE(readField(d, b, d.find("luck")->id).present);   // sinceVersion 2 > record v1
}
```

- [ ] **Step 2: Run to verify fail.**
- [ ] **Step 3: Implement** `readField`: read `blockLength`/`version` from bytes[0..3]; if `field.sinceVersion > version` → `{present=false}`; for fixed fields read the primitive at `4 + field.offset` (LE); for var fields, walk from `4 + blockLength` skipping `varIndex` length-prefixed segments, return the `string_view`. All reads bounds-checked (return `present=false` on overflow).
- [ ] **Step 4: Run test** — Expected: PASS (both).
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): record framing + zero-copy runtime field reader"`

---

### Task 8: Validator (version-aware) + benchmark

**Files:**
- Create: `include/rune/codec/Validator.h`, `src/Validator.cpp`
- Test: `tests/ValidatorTest.cpp`
- Create: `benchmark/ValidatorBenchmark.cpp`; Modify: `CMakeLists.txt` (add `rune_codec_bench` target)

**Interfaces:**
- Consumes: `Descriptor`, record framing.
- Produces:
  - `struct ValResult { bool ok; uint16_t fieldId; const char* reason; };`
  - `class Validator { public: explicit Validator(const Descriptor&); ValResult validate(std::span<const uint8_t> record) const noexcept; };` — built once from the descriptor into a precomputed field table (offset, size, kind, enum set, sinceVersion); `validate` is a tight, allocation-free pass. Checks: header present; declared `blockLength` matches the descriptor's for the record's version; every fixed field with `sinceVersion ≤ record.version` fits; enum fields carry a whitelisted value; every var field length fits the buffer.

- [ ] **Step 1: Write the failing test**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Validator.h>
#include <rune/codec/Record.h>
using namespace rune::codec;
static Descriptor D(){ return DescriptorBuilder("c").add("id",PrimType::Int64)
  .add("type",PrimType::Enum,1,false,{0,1,2}).add("name",PrimType::String).build(); }
static std::vector<uint8_t> frame(int64_t id,int32_t type,std::string_view name){
  std::vector<uint8_t> b; auto le=[&](auto v){for(size_t i=0;i<sizeof v;i++) b.push_back(uint8_t(v>>(8*i)));};
  uint16_t block=12,ver=1; le(block); le(ver); le(id); le(type);
  uint32_t n=uint32_t(name.size()); le(n); b.insert(b.end(),name.begin(),name.end()); return b; }
TEST(Validator, AcceptsValid)   { Validator v(D()); EXPECT_TRUE(v.validate(frame(1,1,"ok")).ok); }
TEST(Validator, RejectsBadEnum) { Validator v(D()); auto r=v.validate(frame(1,9,"x")); EXPECT_FALSE(r.ok); EXPECT_EQ(r.fieldId, 1); }
TEST(Validator, RejectsTruncatedVar){ Validator v(D()); auto rec=frame(1,1,"hello"); rec.resize(rec.size()-2);
  EXPECT_FALSE(v.validate(rec).ok); }
TEST(Validator, RejectsShortBlock){ Validator v(D()); std::vector<uint8_t> b={12,0,1,0}; EXPECT_FALSE(v.validate(b).ok); }
```

- [ ] **Step 2: Run to verify fail.**
- [ ] **Step 3: Implement** the precomputed-table validator (target impl from spec §3.4). Then the benchmark:
```cpp
// benchmark/ValidatorBenchmark.cpp
#include <benchmark/benchmark.h>
#include <rune/codec/Validator.h>
using namespace rune::codec;
static void BM_Validate_Small(benchmark::State& s){
  Validator v(DescriptorBuilder("c").add("id",PrimType::Int64).add("s",PrimType::Int32).build());
  std::vector<uint8_t> rec(4+12,0); rec[0]=12;              // blockLength=12,ver=0
  for (auto _ : s){ auto r=v.validate(rec); benchmark::DoNotOptimize(r.ok); }
}
BENCHMARK(BM_Validate_Small)->Unit(benchmark::kNanosecond);
BENCHMARK_MAIN();
```
Add `rune_codec_bench` executable (link `rune_codec` + `benchmark::benchmark`).

- [ ] **Step 4: Run tests + bench** — `ctest --preset conan-debug -R Validator` (PASS); build+run `rune_codec_bench` (record ns/validate; expect ~tens of ns). *Decision point (spec §12): if the table impl isn't clearly faster than a straightforward OTF-style pass, note the number; table is the target.*
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): version-aware validator + benchmark"`

---

### Task 9: Codec generator + end-to-end round-trip

**Files:**
- Create: `include/rune/codec/Generator.h`, `src/Generator.cpp`
- Create: `tools/rune_codec_gen_main.cpp` (CLI: descriptor-blob file → header file)
- Test: `tests/GeneratorTest.cpp`, `tests/generated/` (build-time generated header consumed by the test)
- Modify: `CMakeLists.txt` (add `rune_codec_gen` tool; custom command to generate a header for the test)

**Interfaces:**
- Consumes: `Descriptor`, `fingerprint`, `toText`, `serialize`.
- Produces: `std::string generateHeader(const Descriptor&, std::string_view nspace = "rune::gen");` — emits a struct with typed members + `encode(uint8_t*)→size_t` (writes header + fixed block + var-data), `decode`, `encodedSize()`, `kBlockLength`, `kFingerprint`, `kSchemaName`, and `static const Descriptor& descriptor()`. CLI `rune_codec_gen <descriptor.blob> <out.h>`.

- [ ] **Step 1: Write the failing test** (round-trip against the runtime reader — the generated codec and `readField` must agree):
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Generator.h>
#include <rune/codec/Record.h>
using namespace rune::codec;
TEST(Generator, EmitsCompilableStructWithFingerprint) {
  auto d = DescriptorBuilder("characters").add("id",PrimType::Int64)
             .add("name",PrimType::String).build();
  const std::string hdr = generateHeader(d);
  EXPECT_NE(hdr.find("struct Characters"), std::string::npos);
  EXPECT_NE(hdr.find("kFingerprint"), std::string::npos);
  EXPECT_NE(hdr.find(std::to_string(fingerprint(d))), std::string::npos);
}
```
Plus a **generated-header round-trip test** (`tests/GeneratedRoundTripTest.cpp`) that `#include`s a header produced at build time by `rune_codec_gen` for a fixed descriptor, does `Characters{...}.encode()` into a buffer, then asserts `readField(descriptor, buf, id)` returns the same values — proving generated codec ⇄ runtime reader agreement.

- [ ] **Step 2: Run to verify fail.**
- [ ] **Step 3: Implement** `generateHeader` (string-build the struct: PascalCase the schema name; map `PrimType`→C++ type; fixed members then var members as `std::string_view`; `encode` writes `[blockLength][version]`, fixed fields at offsets, var fields length-prefixed; `kFingerprint = fingerprint(d)`; embed `descriptor()` by emitting the serialized blob as a `static constexpr uint8_t[]` + a `deserialize` call). Implement `tools/rune_codec_gen_main.cpp` (read blob file → `deserialize` → `generateHeader` → write file). Add a CMake `add_custom_command` generating `tests/generated/characters.gen.h` from a checked-in `tests/generated/characters.blob`, and a `rune_codec_gen` executable.
- [ ] **Step 4: Run tests** — Expected: PASS (generator test + generated round-trip). This closes 0a: descriptor → codegen → encode ⇄ runtime read/validate all agree.
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): descriptor→C++ codec generator + end-to-end round-trip"`

---

## Self-Review

- **Spec coverage (§3):** descriptor (T3) ✓; serializable form (T5) ✓; human-readable form (T6) ✓; fingerprint (T4) ✓; generated codec (T9) ✓; runtime reader `readField` (T7) ✓; validator, version-aware, table impl + benchmark (T8) ✓; SBE-style layout + record framing (T3/T7) ✓; additive/version-aware behavior (T7 absent-field test, T8) ✓. No 0a requirement left unmapped.
- **Placeholders:** none — every code step has real code; "same for the rest" avoided by the table-driven primitive handling.
- **Type consistency:** `Descriptor`, `FieldDescriptor`, `PrimType`, `fixedSize/isVarLen`, `fingerprint`, `serialize/deserialize`, `toText`, `readField`/`FieldValue`, `Validator`/`ValResult`, `generateHeader` are used consistently across tasks with the signatures introduced in their Interfaces blocks.
