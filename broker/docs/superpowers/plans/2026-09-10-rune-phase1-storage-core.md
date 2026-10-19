# Rune Phase 1 — Protocol + Single-Node Storage Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `rune-protocol` (record-batch v2 codec) and `rune-broker` (`librune_storage`: segments, indexes, log, recovery, retention) projects with full unit-test coverage and a working single-node in-process produce/consume API.

**Architecture:** Two Conan 2 C++23 projects mirroring `world-server`/`common`. `rune-protocol` owns the on-disk/on-wire batch format (single source of truth). `rune-broker`'s `librune_storage` builds a partitioned append-only log of memory-mapped segments on top of that format, with crash recovery, retention, and leader-epoch/producer-state bookkeeping reserved for later phases. No networking, replication, or consensus in Phase 1 — the "API" is an in-process `Log` facade proving the storage engine end-to-end.

**Tech Stack:** C++23, CMake ≥3.31, Conan 2, gtest 1.16.0, `aeron` (ConanCenter), `common@vradu/dev` (Agrona buffers), Woodpecker CI.

**Spec:** `rune-broker/docs/specs/2026-09-10-rune-broker-design.md` (§6 Storage internals, §6.4 record batch, §10 phasing, §12 testing).

## Global Constraints

- **Language/std:** C++23 (`set(CMAKE_CXX_STANDARD 23)`), matching `common`.
- **Build system:** CMake ≥ 3.31; Conan 2 with `conan_provider.cmake` dependency provider; `CMakeDeps` + `CMakeToolchain`.
- **Versioning:** `VERSION.txt` = `0.1.0-snapshot`; `set_version()` reads it. Packages: `rune-protocol/x.y.z@vradu/dev`, `rune-broker` app.
- **Publishing:** the agent writes `build.sh`/`release.sh`/CI but **NEVER runs `conan create/export/upload`** — publishing is CI-only (user-owned packages).
- **Local build/test:** cross-compile `-s arch=x86_64` on ARM Macs for Aeron compatibility (as `world-server/build.sh`); enable tests with `-DBUILD_TESTING=ON`; run via `ctest`/`gtest_discover_tests`.
- **Namespace:** all C++ in namespace `rune` (sub-namespaces `rune::protocol`, `rune::storage`).
- **Record batch v2 layout (bytes, from Kafka `DefaultRecordBatch`, copied verbatim):** `BASE_OFFSET`(0,8) `LENGTH`(8,4) `PARTITION_LEADER_EPOCH`(12,4) `MAGIC`(16,1) `CRC`(17,4) `ATTRIBUTES`(21,2) `LAST_OFFSET_DELTA`(23,4) `BASE_TIMESTAMP`(27,8) `MAX_TIMESTAMP`(35,8) `PRODUCER_ID`(43,8) `PRODUCER_EPOCH`(51,2) `BASE_SEQUENCE`(53,4) `RECORD_COUNT`(57,4) `records`(61…). `RECORD_BATCH_OVERHEAD = 61`.
- **CRC:** CRC-32C (Castagnoli) over bytes `[ATTRIBUTES_OFFSET(21) .. end]`; `PARTITION_LEADER_EPOCH` is **excluded** so the broker can stamp it without recomputing CRC. Hardware intrinsic (SSE4.2 `_mm_crc32_u*` / ARMv8 `__crc32c*`) with a software table fallback.
- **OffsetIndex entry = 8 bytes:** `int32 relativeOffset` + `int32 physicalPosition`; offsets relative to segment base; warm section = last `8192 / 8 = 1024` entries.

---

## File Structure

**`rune-protocol/`**
- `conanfile.py`, `conandata.yml`, `CMakeLists.txt`, `conan_provider.cmake`, `VERSION.txt`, `build.sh`, `release.sh`, `update-deps.sh`, `bump_version.py`, `.gitignore`, `.woodpecker/build.yml`
- `include/rune/protocol/Crc32c.h` — hardware/software CRC-32C.
- `include/rune/protocol/Varint.h` — zigzag varint/varlong encode/decode.
- `include/rune/protocol/Errors.h` — `ErrorCode` enum + `RuneException`.
- `include/rune/protocol/RecordBatch.h` — layout constants, `RecordBatchBuilder`, `RecordBatchReader`, `Record` view.
- `src/dummy.cpp` — CLion static-lib helper (as in `common`).
- `tests/protocol/{Crc32cTest,VarintTest,RecordBatchTest}.cpp`

**`rune-broker/`**
- Same scaffolding files (app-type conanfile) + `Dockerfile`, `build_docker.sh`.
- `libs/storage/include/rune/storage/OffsetIndex.h`, `LogSegment.h`, `Log.h`, `LeaderEpochCache.h`, `ProducerState.h`, `RetentionPolicy.h`, `MappedFile.h` (TimeIndex deferred to Phase 4 with time-based retention — spec §6.2/§9)
- `libs/storage/src/*.cpp`
- `libs/storage/tests/storage/*Test.cpp`
- `src/main.cpp` — placeholder broker entrypoint (prints version; real wiring in later phases).

---

## Task 1: Scaffold `rune-protocol` project (builds + empty test runs)

**Files:**
- Create: `rune-protocol/VERSION.txt`, `conanfile.py`, `conandata.yml`, `CMakeLists.txt`, `.gitignore`, `src/dummy.cpp`, `include/rune/protocol/.keep`, `tests/protocol/SmokeTest.cpp`
- Copy: `rune-protocol/conan_provider.cmake` (from `common/conan_provider.cmake`)

**Interfaces:**
- Produces: a buildable `rune_protocol` INTERFACE library target and a `rune_protocol_tests` gtest executable.

- [ ] **Step 1: Create `VERSION.txt`**

```
0.1.0-snapshot
```

- [ ] **Step 2: Create `conanfile.py`** (library type, mirrors `common`)

```python
from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake, cmake_layout
import os

class RuneProtocolConan(ConanFile):
    name = "rune-protocol"
    package_type = "library"
    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False]}
    default_options = {"shared": False}
    exports_sources = "src/*", "include/*", "CMakeLists.txt", "VERSION.txt"

    def set_version(self):
        self.version = open(os.path.join(os.path.dirname(__file__), "VERSION.txt")).read().strip()

    def requirements(self):
        for r in self.conan_data.get("requirements", []):
            self.requires(r)

    def build_requirements(self):
        for r in self.conan_data.get("test_requirements", []):
            self.test_requires(r)

    def generate(self):
        CMakeDeps(self).generate()
        CMakeToolchain(self).generate()

    def layout(self):
        cmake_layout(self)

    def build(self):
        cmake = CMake(self)
        cmake.configure()

    def package(self):
        CMake(self).install()

    def package_info(self):
        self.cpp_info.set_property("cmake_target_name", "rune-protocol")
        self.cpp_info.includedirs = ["include"]
```

- [ ] **Step 3: Create `conandata.yml`**

```yaml
requirements:
  - "common/1.0.10@vradu/dev"
  - "aeron/1.51.0"
test_requirements:
  - "gtest/1.16.0"
```

- [ ] **Step 4: Create `CMakeLists.txt`**

```cmake
cmake_minimum_required(VERSION 3.31)
project(rune_protocol)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(aeron)

add_library(rune_protocol INTERFACE)
target_include_directories(rune_protocol INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

# CLion helper (see common): a STATIC target so the IDE indexes headers.
add_library(rune_protocol_dummy STATIC src/dummy.cpp)
target_include_directories(rune_protocol_dummy PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
    find_package(GTest REQUIRED)
    add_executable(rune_protocol_tests tests/protocol/SmokeTest.cpp)
    target_include_directories(rune_protocol_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(rune_protocol_tests PRIVATE GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_protocol_tests)
endif()
```

- [ ] **Step 5: Create `src/dummy.cpp`**

```cpp
// Intentionally empty translation unit so CLion indexes header-only code.
namespace rune { void rune_protocol_dummy() {} }
```

- [ ] **Step 6: Create `tests/protocol/SmokeTest.cpp`**

```cpp
#include <gtest/gtest.h>

TEST(Smoke, Builds) {
    EXPECT_EQ(1 + 1, 2);
}
```

- [ ] **Step 7: Create `.gitignore`**

```
build/
cmake-build-*/
CMakeUserPresets.json
conan-graph.json
conan-pkglist.json
*.log
```

- [ ] **Step 8: Copy the Conan provider**

Run: `cp ~/Projects/common/conan_provider.cmake ~/Projects/rune-protocol/conan_provider.cmake`

- [ ] **Step 9: Configure + build + run the smoke test**

Run:
```bash
cd ~/Projects/rune-protocol
conan install . --build=missing -s build_type=Debug -s arch=x86_64 --format=json > conan-graph.json
cmake . -B build -DCMAKE_OSX_ARCHITECTURES=x86_64 \
  -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake \
  -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators" \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build -j
cd build && ctest --output-on-failure
```
Expected: `rune_protocol_tests` PASS (1 test).

- [ ] **Step 10: Init git repo + commit**

```bash
cd ~/Projects/rune-protocol && git init -q
git add -A && git commit -m "chore(protocol): scaffold rune-protocol conan/cmake project"
```

---

## Task 2: CRC-32C (hardware + software fallback)

**Files:**
- Create: `rune-protocol/include/rune/protocol/Crc32c.h`
- Test: `rune-protocol/tests/protocol/Crc32cTest.cpp`

**Interfaces:**
- Produces: `uint32_t rune::protocol::crc32c(const uint8_t* data, size_t len) noexcept;` (also an incremental `crc32c(uint32_t seed, ...)`).

- [ ] **Step 1: Write the failing test** (`Crc32cTest.cpp`)

```cpp
#include <gtest/gtest.h>
#include <rune/protocol/Crc32c.h>
#include <string>

using rune::protocol::crc32c;

TEST(Crc32c, KnownVectors) {
    // CRC-32C ("Castagnoli") reference values.
    EXPECT_EQ(crc32c(nullptr, 0), 0x00000000u);
    const std::string a = "123456789";
    EXPECT_EQ(crc32c(reinterpret_cast<const uint8_t*>(a.data()), a.size()), 0xE3069283u);
}

TEST(Crc32c, IncrementalMatchesOneShot) {
    const std::string s = "the quick brown fox";
    const auto* p = reinterpret_cast<const uint8_t*>(s.data());
    uint32_t seed = crc32c(p, 9);
    uint32_t inc  = rune::protocol::crc32c(seed, p + 9, s.size() - 9);
    EXPECT_EQ(inc, crc32c(p, s.size()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build -j && cd build && ctest -R Crc32c --output-on-failure`
Expected: FAIL — `Crc32c.h` not found.

- [ ] **Step 3: Write `Crc32c.h`** (correct software table now; the SSE4.2/ARMv8 hardware intrinsic is added in Phase 3 alongside the p99<100µs gate — this software impl is the correctness baseline the hardware path is tested against)

```cpp
#pragma once
#include <cstddef>
#include <cstdint>
#include <array>

namespace rune::protocol {

namespace detail {
inline constexpr std::array<uint32_t, 256> makeTable() {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int k = 0; k < 8; ++k)
            c = (c & 1) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
        t[i] = c;
    }
    return t;
}
inline constexpr auto kTable = makeTable();
} // namespace detail

// Incremental: continue from a previous CRC value (already ~complemented internally).
inline uint32_t crc32c(uint32_t seed, const uint8_t* data, size_t len) noexcept {
    uint32_t crc = ~seed;
    for (size_t i = 0; i < len; ++i)
        crc = detail::kTable[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return ~crc;
}

inline uint32_t crc32c(const uint8_t* data, size_t len) noexcept {
    return crc32c(0u, data, len);
}

} // namespace rune::protocol
```

- [ ] **Step 4: Add the test file to `CMakeLists.txt`**

In the `BUILD_TESTING` block, change the `add_executable` to:
```cmake
    add_executable(rune_protocol_tests
            tests/protocol/SmokeTest.cpp
            tests/protocol/Crc32cTest.cpp)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cmake --build build -j && cd build && ctest -R Crc32c --output-on-failure`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add include/rune/protocol/Crc32c.h tests/protocol/Crc32cTest.cpp CMakeLists.txt
git commit -m "feat(protocol): CRC-32C (Castagnoli) with incremental API"
```

---

## Task 3: Zigzag varint/varlong codec

**Files:**
- Create: `rune-protocol/include/rune/protocol/Varint.h`
- Test: `rune-protocol/tests/protocol/VarintTest.cpp`

**Interfaces:**
- Produces:
  - `size_t writeVarlong(int64_t value, uint8_t* out) noexcept;` (zigzag, returns bytes written, max 10)
  - `int64_t readVarlong(const uint8_t* in, size_t& pos) noexcept;`
  - `size_t writeVarint(int32_t, uint8_t*) noexcept;` / `int32_t readVarint(const uint8_t*, size_t&) noexcept;`

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <rune/protocol/Varint.h>
using namespace rune::protocol;

TEST(Varint, RoundTripLong) {
    for (int64_t v : {0LL, 1LL, -1LL, 127LL, -128LL, 300LL, -300LL,
                      9223372036854775807LL, -9223372036854775807LL - 1}) {
        uint8_t buf[10]; size_t n = writeVarlong(v, buf);
        size_t pos = 0; int64_t got = readVarlong(buf, pos);
        EXPECT_EQ(got, v);
        EXPECT_EQ(pos, n);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cmake --build build -j && cd build && ctest -R Varint --output-on-failure`
Expected: FAIL — header missing.

- [ ] **Step 3: Write `Varint.h`**

```cpp
#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::protocol {

inline size_t writeVarlong(int64_t value, uint8_t* out) noexcept {
    uint64_t v = (static_cast<uint64_t>(value) << 1) ^ (value >> 63); // zigzag
    size_t i = 0;
    while (v >= 0x80) { out[i++] = static_cast<uint8_t>(v) | 0x80; v >>= 7; }
    out[i++] = static_cast<uint8_t>(v);
    return i;
}

inline int64_t readVarlong(const uint8_t* in, size_t& pos) noexcept {
    uint64_t v = 0; int shift = 0; uint8_t b;
    do { b = in[pos++]; v |= static_cast<uint64_t>(b & 0x7F) << shift; shift += 7; } while (b & 0x80);
    return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1); // un-zigzag
}

inline size_t writeVarint(int32_t value, uint8_t* out) noexcept {
    return writeVarlong(static_cast<int64_t>(value), out);
}
inline int32_t readVarint(const uint8_t* in, size_t& pos) noexcept {
    return static_cast<int32_t>(readVarlong(in, pos));
}

} // namespace rune::protocol
```

- [ ] **Step 4: Add `tests/protocol/VarintTest.cpp` to the `rune_protocol_tests` sources in `CMakeLists.txt`.**

- [ ] **Step 5: Run test to verify it passes**

Run: `cmake --build build -j && cd build && ctest -R Varint --output-on-failure`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add include/rune/protocol/Varint.h tests/protocol/VarintTest.cpp CMakeLists.txt
git commit -m "feat(protocol): zigzag varint/varlong codec"
```

---

## Task 4: Record-batch v2 layout constants + `ErrorCode`

**Files:**
- Create: `rune-protocol/include/rune/protocol/Errors.h`, `include/rune/protocol/RecordBatch.h` (constants portion only in this task)
- Test: `rune-protocol/tests/protocol/RecordBatchTest.cpp` (layout assertions)

**Interfaces:**
- Produces: `namespace rune::protocol::batch` constants (`kBaseOffset`, `kLength`, … `kRecordBatchOverhead = 61`, `kAttributesOffset = 21`, `kCrcOffset = 17`, `kPartitionLeaderEpochOffset = 12`, `kMagicOffset = 16`, `kMagicValue = 2`); `enum class ErrorCode`.

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <rune/protocol/RecordBatch.h>
namespace b = rune::protocol::batch;

TEST(RecordBatchLayout, FieldOffsetsMatchKafkaV2) {
    EXPECT_EQ(b::kBaseOffsetOffset, 0);
    EXPECT_EQ(b::kLengthOffset, 8);
    EXPECT_EQ(b::kPartitionLeaderEpochOffset, 12);
    EXPECT_EQ(b::kMagicOffset, 16);
    EXPECT_EQ(b::kCrcOffset, 17);
    EXPECT_EQ(b::kAttributesOffset, 21);
    EXPECT_EQ(b::kLastOffsetDeltaOffset, 23);
    EXPECT_EQ(b::kBaseTimestampOffset, 27);
    EXPECT_EQ(b::kProducerIdOffset, 43);
    EXPECT_EQ(b::kBaseSequenceOffset, 53);
    EXPECT_EQ(b::kRecordCountOffset, 57);
    EXPECT_EQ(b::kRecordBatchOverhead, 61);
}
```

- [ ] **Step 2: Run test to verify it fails** — `RecordBatch.h` missing.

- [ ] **Step 3: Write `Errors.h`**

```cpp
#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>

namespace rune::protocol {
enum class ErrorCode : int16_t {
    None = 0,
    CorruptRecord = 2,
    OffsetOutOfRange = 1,
    InvalidRecordBatch = 3,
    IndexOffsetOverflow = 4,
};
struct RuneException : std::runtime_error {
    ErrorCode code;
    RuneException(ErrorCode c, const std::string& msg) : std::runtime_error(msg), code(c) {}
};
} // namespace rune::protocol
```

- [ ] **Step 4: Write the constants block of `RecordBatch.h`**

```cpp
#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::protocol::batch {
inline constexpr int kBaseOffsetOffset = 0;
inline constexpr int kLengthOffset = 8;               // int32: bytes after this field
inline constexpr int kPartitionLeaderEpochOffset = 12; // int32, EXCLUDED from CRC
inline constexpr int kMagicOffset = 16;                // int8
inline constexpr int kCrcOffset = 17;                  // uint32 CRC-32C
inline constexpr int kAttributesOffset = 21;           // int16; CRC starts here
inline constexpr int kLastOffsetDeltaOffset = 23;      // int32
inline constexpr int kBaseTimestampOffset = 27;        // int64
inline constexpr int kMaxTimestampOffset = 35;         // int64
inline constexpr int kProducerIdOffset = 43;           // int64
inline constexpr int kProducerEpochOffset = 51;        // int16
inline constexpr int kBaseSequenceOffset = 53;         // int32
inline constexpr int kRecordCountOffset = 57;          // int32
inline constexpr int kRecordBatchOverhead = 61;        // records start here
inline constexpr int8_t kMagicValue = 2;
} // namespace rune::protocol::batch
```

- [ ] **Step 5: Add `RecordBatchTest.cpp` to `CMakeLists.txt`; build; run.**

Run: `cmake --build build -j && cd build && ctest -R RecordBatchLayout --output-on-failure`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add include/rune/protocol/Errors.h include/rune/protocol/RecordBatch.h tests/protocol/RecordBatchTest.cpp CMakeLists.txt
git commit -m "feat(protocol): record-batch v2 layout constants + error codes"
```

---

## Task 5: `RecordBatchBuilder` — encode a batch (header + delta records + CRC)

**Files:**
- Modify: `rune-protocol/include/rune/protocol/RecordBatch.h`
- Test: `rune-protocol/tests/protocol/RecordBatchTest.cpp`

**Interfaces:**
- Consumes: `batch::*` constants, `crc32c`, varint codec.
- Produces:
  - `class RecordBatchBuilder { RecordBatchBuilder(int64_t baseOffset, int64_t baseTimestamp); void append(std::string_view key, std::string_view value, int64_t timestamp); std::vector<uint8_t> build(); };`
  - Big-endian field writes; `LENGTH` and `CRC` and `LAST_OFFSET_DELTA` and `RECORD_COUNT` filled on `build()`.

- [ ] **Step 1: Write the failing test** (encode then sanity-check header fields)

```cpp
#include <rune/protocol/Crc32c.h>
static uint32_t beU32(const std::vector<uint8_t>& b, int off) {
    return (b[off]<<24)|(b[off+1]<<16)|(b[off+2]<<8)|b[off+3];
}

TEST(RecordBatchBuilder, EncodesHeaderAndCrc) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(100 /*baseOffset*/, 5000 /*baseTs*/);
    rb.append("k1", "v1", 5000);
    rb.append("k2", "v2", 5001);
    auto bytes = rb.build();

    EXPECT_EQ(bytes[batch::kMagicOffset], batch::kMagicValue);
    EXPECT_EQ(beU32(bytes, batch::kRecordCountOffset), 2u);
    EXPECT_EQ(beU32(bytes, batch::kLastOffsetDeltaOffset), 1u); // 2 records -> delta 1
    // LENGTH = total - (baseOffset(8) + lengthField(4))
    EXPECT_EQ(beU32(bytes, batch::kLengthOffset), bytes.size() - 12);
    // CRC covers [attributes .. end]
    uint32_t expect = crc32c(bytes.data() + batch::kAttributesOffset,
                             bytes.size() - batch::kAttributesOffset);
    EXPECT_EQ(beU32(bytes, batch::kCrcOffset), expect);
}
```

- [ ] **Step 2: Run test to verify it fails** — `RecordBatchBuilder` undefined.

- [ ] **Step 3: Implement `RecordBatchBuilder` in `RecordBatch.h`**

```cpp
#include <vector>
#include <string_view>
#include <cstring>
#include "Crc32c.h"
#include "Varint.h"

namespace rune::protocol {

namespace detail {
inline void putBE(std::vector<uint8_t>& b, int off, uint32_t v) {
    b[off]=v>>24; b[off+1]=v>>16; b[off+2]=v>>8; b[off+3]=v;
}
inline void appendBE64(std::vector<uint8_t>& b, int64_t v) {
    for (int i=7;i>=0;--i) b.push_back(static_cast<uint8_t>(v>>(i*8)));
}
inline void appendBE32(std::vector<uint8_t>& b, int32_t v) {
    for (int i=3;i>=0;--i) b.push_back(static_cast<uint8_t>(v>>(i*8)));
}
inline void appendBE16(std::vector<uint8_t>& b, int16_t v) {
    b.push_back(static_cast<uint8_t>(v>>8)); b.push_back(static_cast<uint8_t>(v));
}
} // namespace detail

class RecordBatchBuilder {
public:
    RecordBatchBuilder(int64_t baseOffset, int64_t baseTimestamp)
        : baseOffset_(baseOffset), baseTimestamp_(baseTimestamp) {}

    void append(std::string_view key, std::string_view value, int64_t timestamp) {
        // Kafka v2 record: length(varint) attributes(int8) tsDelta(varlong)
        //   offsetDelta(varint) keyLen(varint) key valueLen(varint) value headerCount(varint=0)
        std::vector<uint8_t> body;
        auto put = [&](auto fn){ uint8_t tmp[10]; size_t n = fn(tmp); body.insert(body.end(), tmp, tmp+n); };
        body.push_back(0); // attributes
        put([&](uint8_t* o){ return writeVarlong(timestamp - baseTimestamp_, o); });
        put([&](uint8_t* o){ return writeVarint(recordCount_, o); }); // offsetDelta
        put([&](uint8_t* o){ return writeVarint(static_cast<int32_t>(key.size()), o); });
        body.insert(body.end(), key.begin(), key.end());
        put([&](uint8_t* o){ return writeVarint(static_cast<int32_t>(value.size()), o); });
        body.insert(body.end(), value.begin(), value.end());
        put([&](uint8_t* o){ return writeVarint(0, o); }); // header count

        uint8_t lenbuf[10]; size_t ln = writeVarint(static_cast<int32_t>(body.size()), lenbuf);
        records_.insert(records_.end(), lenbuf, lenbuf+ln);
        records_.insert(records_.end(), body.begin(), body.end());
        maxTimestamp_ = std::max(maxTimestamp_, timestamp);
        ++recordCount_;
    }

    std::vector<uint8_t> build() {
        using namespace batch;
        std::vector<uint8_t> b;
        detail::appendBE64(b, baseOffset_);                 // 0
        detail::appendBE32(b, 0);                           // 8 length (patched)
        detail::appendBE32(b, -1);                          // 12 partitionLeaderEpoch (unset)
        b.push_back(static_cast<uint8_t>(kMagicValue));     // 16
        detail::appendBE32(b, 0);                           // 17 CRC (patched)
        detail::appendBE16(b, 0);                           // 21 attributes
        detail::appendBE32(b, recordCount_ - 1);            // 23 lastOffsetDelta
        detail::appendBE64(b, baseTimestamp_);              // 27
        detail::appendBE64(b, maxTimestamp_);               // 35
        detail::appendBE64(b, -1);                          // 43 producerId
        detail::appendBE16(b, -1);                          // 51 producerEpoch
        detail::appendBE32(b, -1);                          // 53 baseSequence
        detail::appendBE32(b, recordCount_);                // 57 recordCount
        b.insert(b.end(), records_.begin(), records_.end());// 61 records

        detail::putBE(b, kLengthOffset, static_cast<uint32_t>(b.size() - 12));
        uint32_t crc = crc32c(b.data() + kAttributesOffset, b.size() - kAttributesOffset);
        detail::putBE(b, kCrcOffset, crc);
        return b;
    }
    int32_t recordCount() const { return recordCount_; }

private:
    int64_t baseOffset_, baseTimestamp_;
    int64_t maxTimestamp_ = baseTimestamp_;
    int32_t recordCount_ = 0;
    std::vector<uint8_t> records_;
};
} // namespace rune::protocol
```

- [ ] **Step 4: Build; run**

Run: `cmake --build build -j && cd build && ctest -R RecordBatchBuilder --output-on-failure`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add include/rune/protocol/RecordBatch.h tests/protocol/RecordBatchTest.cpp
git commit -m "feat(protocol): RecordBatchBuilder encodes v2 batch with CRC-32C"
```

---

## Task 6: `RecordBatchReader` — decode, `ensureValid()` (CRC), iterate; stamp epoch

**Files:**
- Modify: `rune-protocol/include/rune/protocol/RecordBatch.h`
- Test: `rune-protocol/tests/protocol/RecordBatchTest.cpp`

**Interfaces:**
- Consumes: encoded bytes from `RecordBatchBuilder`.
- Produces:
  - `class RecordBatchReader { explicit RecordBatchReader(const uint8_t* data, size_t len); int64_t baseOffset() const; int32_t lastOffsetDelta() const; int32_t recordCount() const; int32_t totalLength() const; bool ensureValid() const; void forEach(const std::function<void(std::string_view key, std::string_view value, int64_t offset, int64_t timestamp)>&) const; static void stampLeaderEpoch(uint8_t* data, int32_t epoch); };`
  - `totalLength()` = `LENGTH` field + 12 (full on-disk size), used by the log to walk batches.

- [ ] **Step 1: Write the failing test** (round-trip + corruption + epoch stamp does not change CRC)

```cpp
#include <functional>
TEST(RecordBatchReader, RoundTripAndValidation) {
    using namespace rune::protocol;
    RecordBatchBuilder rb(100, 5000);
    rb.append("k1", "v1", 5000);
    rb.append("k2", "v2", 5001);
    auto bytes = rb.build();

    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 100);
    EXPECT_EQ(r.recordCount(), 2);
    EXPECT_EQ(r.totalLength(), (int)bytes.size());

    std::vector<std::pair<std::string,int64_t>> seen;
    r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t){
        seen.emplace_back(std::string(v), off);
    });
    ASSERT_EQ(seen.size(), 2u);
    EXPECT_EQ(seen[0].first, "v1"); EXPECT_EQ(seen[0].second, 100);
    EXPECT_EQ(seen[1].first, "v2"); EXPECT_EQ(seen[1].second, 101);

    // Stamping the (CRC-excluded) leader epoch must NOT invalidate the batch.
    RecordBatchReader::stampLeaderEpoch(bytes.data(), 7);
    RecordBatchReader r2(bytes.data(), bytes.size());
    EXPECT_TRUE(r2.ensureValid());

    // Corrupt a value byte -> ensureValid() fails.
    bytes.back() ^= 0xFF;
    EXPECT_FALSE(RecordBatchReader(bytes.data(), bytes.size()).ensureValid());
}
```

- [ ] **Step 2: Run test to verify it fails** — reader undefined.

- [ ] **Step 3: Implement `RecordBatchReader`**

```cpp
#include <functional>
namespace rune::protocol {
namespace detail {
inline uint32_t getBE32(const uint8_t* p){ return (p[0]<<24)|(p[1]<<16)|(p[2]<<8)|p[3]; }
inline uint64_t getBE64(const uint8_t* p){ uint64_t v=0; for(int i=0;i<8;++i) v=(v<<8)|p[i]; return v; }
} // namespace detail

class RecordBatchReader {
public:
    RecordBatchReader(const uint8_t* data, size_t len) : d_(data), len_(len) {}
    int64_t baseOffset() const { return static_cast<int64_t>(detail::getBE64(d_ + batch::kBaseOffsetOffset)); }
    int32_t lastOffsetDelta() const { return detail::getBE32(d_ + batch::kLastOffsetDeltaOffset); }
    int32_t recordCount() const { return detail::getBE32(d_ + batch::kRecordCountOffset); }
    int32_t totalLength() const { return static_cast<int32_t>(detail::getBE32(d_ + batch::kLengthOffset)) + 12; }
    int64_t baseTimestamp() const { return static_cast<int64_t>(detail::getBE64(d_ + batch::kBaseTimestampOffset)); }

    bool ensureValid() const {
        if (len_ < static_cast<size_t>(batch::kRecordBatchOverhead)) return false;
        uint32_t stored = detail::getBE32(d_ + batch::kCrcOffset);
        uint32_t actual = crc32c(d_ + batch::kAttributesOffset, totalLength() - batch::kAttributesOffset);
        return stored == actual;
    }

    void forEach(const std::function<void(std::string_view,std::string_view,int64_t,int64_t)>& fn) const {
        size_t pos = batch::kRecordBatchOverhead;
        int64_t base = baseOffset(); int64_t baseTs = baseTimestamp();
        for (int32_t i = 0; i < recordCount(); ++i) {
            (void)readVarint(d_, pos);            // record length
            ++pos;                                // attributes
            int64_t tsDelta = readVarlong(d_, pos);
            int32_t offDelta = readVarint(d_, pos);
            int32_t klen = readVarint(d_, pos);
            std::string_view key(reinterpret_cast<const char*>(d_ + pos), klen<0?0:klen); if (klen>0) pos += klen;
            int32_t vlen = readVarint(d_, pos);
            std::string_view val(reinterpret_cast<const char*>(d_ + pos), vlen<0?0:vlen); if (vlen>0) pos += vlen;
            int32_t hcount = readVarint(d_, pos); (void)hcount; // Phase 1: headers unused
            fn(key, val, base + offDelta, baseTs + tsDelta);
        }
    }

    // Stamp partitionLeaderEpoch in place. This field is EXCLUDED from the CRC,
    // so the broker sets it on arrival without recomputing the checksum.
    static void stampLeaderEpoch(uint8_t* data, int32_t epoch) {
        for (int i = 0; i < 4; ++i)
            data[batch::kPartitionLeaderEpochOffset + i] = static_cast<uint8_t>(epoch >> ((3 - i) * 8));
    }
private:
    const uint8_t* d_; size_t len_;
};
} // namespace rune::protocol
```

- [ ] **Step 4: Build; run**

Run: `cmake --build build -j && cd build && ctest -R RecordBatchReader --output-on-failure`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add include/rune/protocol/RecordBatch.h tests/protocol/RecordBatchTest.cpp
git commit -m "feat(protocol): RecordBatchReader decode + ensureValid + epoch stamp"
```

---

## Task 7: Add `build.sh`, `release.sh`, `update-deps.sh`, `.woodpecker` to `rune-protocol`

**Files:**
- Create: `rune-protocol/build.sh`, `release.sh`, `update-deps.sh`, `bump_version.py`, `.woodpecker/build.yml`

**Interfaces:** none (tooling). These mirror `common`/`world-server`; the agent never runs the publish steps.

- [ ] **Step 1: Create `build.sh`** (adapted from `common/build.sh`, x86_64 for Aeron)

```bash
#!/bin/bash
set -e
VERSION=$(cat VERSION.txt | tr -d '[:space:]')
PACKAGE="rune-protocol/${VERSION}@vradu/dev"
ARCH="-s arch=x86_64"
echo "📦 $PACKAGE"
conan install . --build=missing -s build_type=Release $ARCH --format=json > conan-graph.json
cmake . -B build -DCMAKE_OSX_ARCHITECTURES=x86_64 \
  -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
  -DCMAKE_PREFIX_PATH="$PWD/build/Release/generators" -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON
cmake --build build -- VERBOSE=1
(cd build && ctest --output-on-failure)
conan create . --user=vradu --channel=dev $ARCH
conan export . --user=vradu --channel=dev
conan list --graph=conan-graph.json --graph-binaries=build --format=json > conan-pkglist.json
conan upload --list=conan-pkglist.json --remote=artifactory -c
conan upload "$PACKAGE" --remote=artifactory -c
echo "✅ Done."
```

- [ ] **Step 2: Copy `release.sh`, `update-deps.sh`, `bump_version.py` from `common`**

```bash
cp ~/Projects/common/release.sh ~/Projects/rune-protocol/release.sh
cp ~/Projects/common/update-deps.sh ~/Projects/rune-protocol/update-deps.sh
cp ~/Projects/common/bump_version.py ~/Projects/rune-protocol/bump_version.py
chmod +x ~/Projects/rune-protocol/*.sh
```

- [ ] **Step 3: Create `.woodpecker/build.yml`** (build + test only; publish gated to CI creds)

```yaml
steps:
  build-test:
    image: rune-cpp-build:latest
    commands:
      - conan install . --build=missing -s build_type=Debug -s arch=x86_64
      - cmake . -B build -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators" -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
      - cmake --build build -j
      - cd build && ctest --output-on-failure
```

- [ ] **Step 4: Commit** (do NOT run `build.sh`)

```bash
git add build.sh release.sh update-deps.sh bump_version.py .woodpecker/
git commit -m "chore(protocol): CI + build/release scripts (publish via CI only)"
```

---

## Task 8: Scaffold `rune-broker` project + empty `librune_storage`

**Files:**
- Create: `rune-broker/VERSION.txt`, `conanfile.py`, `conandata.yml`, `CMakeLists.txt`, `.gitignore`, `src/main.cpp`, `libs/storage/CMakeLists.txt`, `libs/storage/src/dummy.cpp`, `libs/storage/tests/storage/SmokeTest.cpp`, `Dockerfile`, `build_docker.sh`
- Copy: `conan_provider.cmake` from `common`
- Note: this replaces the temporary spec-only git repo already at `rune-broker/` (keep its `docs/` history).

**Interfaces:**
- Produces: `rune_storage` static library target; `rune_broker` executable; `rune_storage_tests`.

- [ ] **Step 1: Create `VERSION.txt`** → `0.1.0-snapshot`

- [ ] **Step 2: Create `conanfile.py`** (application type, mirrors `world-server`)

```python
from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMakeToolchain, CMakeDeps
import os

class RuneBrokerConan(ConanFile):
    name = "rune-broker"
    package_type = "application"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps"

    def set_version(self):
        self.version = open(os.path.join(os.path.dirname(__file__), "VERSION.txt")).read().strip()

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self); tc.user_presets_path = False; tc.generate()

    def requirements(self):
        for r in self.conan_data.get("requirements", []):
            self.requires(r)
```

- [ ] **Step 3: Create `conandata.yml`**

```yaml
requirements:
  - "rune-protocol/0.1.0-snapshot@vradu/dev"
  - "common/1.0.10@vradu/dev"
  - "aeron/1.51.0"
  - "spdlog/1.14.0"
  - "gtest/1.16.0"
  - "benchmark/1.8.2"
```

- [ ] **Step 4: Create root `CMakeLists.txt`**

```cmake
cmake_minimum_required(VERSION 3.31)
project(rune_broker)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(rune-protocol REQUIRED)
find_package(aeron)
find_package(spdlog REQUIRED)

add_subdirectory(libs/storage)

add_executable(rune_broker src/main.cpp)
target_link_libraries(rune_broker PRIVATE rune_storage spdlog::spdlog)

option(BUILD_TESTING "Build tests" OFF)
if(BUILD_TESTING)
    enable_testing()
endif()
```

- [ ] **Step 5: Create `libs/storage/CMakeLists.txt`**

```cmake
add_library(rune_storage
        src/dummy.cpp)
target_include_directories(rune_storage PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
target_link_libraries(rune_storage PUBLIC rune-protocol)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)
    add_executable(rune_storage_tests tests/storage/SmokeTest.cpp)
    target_link_libraries(rune_storage_tests PRIVATE rune_storage GTest::gtest_main)
    include(GoogleTest)
    gtest_discover_tests(rune_storage_tests)
endif()
```

- [ ] **Step 6: Create `libs/storage/src/dummy.cpp`, `src/main.cpp`, `tests/storage/SmokeTest.cpp`**

```cpp
// libs/storage/src/dummy.cpp
namespace rune::storage { void dummy() {} }
```
```cpp
// src/main.cpp
#include <cstdio>
int main() { std::puts("rune-broker 0.1.0-snapshot"); return 0; }
```
```cpp
// libs/storage/tests/storage/SmokeTest.cpp
#include <gtest/gtest.h>
TEST(StorageSmoke, Builds) { EXPECT_TRUE(true); }
```

- [ ] **Step 7: Create `.gitignore` (same as Task 1 Step 7), copy `conan_provider.cmake`, create `Dockerfile` + `build_docker.sh`**

`Dockerfile` (skeleton mirroring world-server shape; multi-stage, installs deps, builds, runs):
```dockerfile
FROM rune-cpp-build:latest AS build
WORKDIR /app
COPY . .
ARG ARTIFACTORY_HOST
ARG CONAN_PASSWORD
RUN conan install . --build=missing -s build_type=Release -s arch=x86_64
RUN cmake . -B build -DCMAKE_TOOLCHAIN_FILE=build/Release/generators/conan_toolchain.cmake \
      -DCMAKE_PREFIX_PATH="/app/build/Release/generators" -DCMAKE_BUILD_TYPE=Release
RUN cmake --build build -j

FROM debian:bookworm-slim
COPY --from=build /app/build/rune_broker /usr/local/bin/rune_broker
ENTRYPOINT ["/usr/local/bin/rune_broker"]
```

- [ ] **Step 8: Configure, build, run smoke test**

Run (same conan+cmake incantation as Task 1 Step 9, from `~/Projects/rune-broker`, adding `-DBUILD_TESTING=ON`).
Expected: `rune_storage_tests` PASS; `rune_broker` binary prints version.

- [ ] **Step 9: Commit**

```bash
cd ~/Projects/rune-broker
git add -A && git commit -m "chore(broker): scaffold rune-broker app + librune_storage"
```

---

## Task 9: `MappedFile` — RAII mmap wrapper (grow, sync, unmap)

**Files:**
- Create: `rune-broker/libs/storage/include/rune/storage/MappedFile.h`, `libs/storage/src/MappedFile.cpp`
- Test: `libs/storage/tests/storage/MappedFileTest.cpp`

**Interfaces:**
- Produces: `class MappedFile { MappedFile(std::string path, size_t initialBytes, bool writable); uint8_t* data(); size_t size() const; void resize(size_t); void sync(); ~MappedFile(); };` (POSIX `mmap`/`msync`/`ftruncate`).

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <rune/storage/MappedFile.h>
#include <filesystem>
using rune::storage::MappedFile;

TEST(MappedFile, WriteReadPersistAcrossReopen) {
    auto p = std::filesystem::temp_directory_path() / "rune_mf.bin";
    std::filesystem::remove(p);
    { MappedFile mf(p.string(), 4096, true); mf.data()[0] = 42; mf.data()[4095] = 7; mf.sync(); }
    { MappedFile mf(p.string(), 4096, false); EXPECT_EQ(mf.data()[0], 42); EXPECT_EQ(mf.data()[4095], 7); }
    std::filesystem::remove(p);
}
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `MappedFile`** (`.h` + `.cpp`)

```cpp
// MappedFile.h
#pragma once
#include <cstdint>
#include <cstddef>
#include <string>
namespace rune::storage {
class MappedFile {
public:
    MappedFile(std::string path, size_t initialBytes, bool writable);
    ~MappedFile();
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    uint8_t* data() noexcept { return data_; }
    const uint8_t* data() const noexcept { return data_; }
    size_t size() const noexcept { return size_; }
    void resize(size_t newBytes);
    void sync();
private:
    std::string path_; uint8_t* data_ = nullptr; size_t size_ = 0; int fd_ = -1; bool writable_;
    void mapNow();
};
} // namespace rune::storage
```
```cpp
// MappedFile.cpp
#include <rune/storage/MappedFile.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <stdexcept>
namespace rune::storage {
MappedFile::MappedFile(std::string path, size_t initialBytes, bool writable)
    : path_(std::move(path)), size_(initialBytes), writable_(writable) {
    fd_ = ::open(path_.c_str(), writable ? (O_RDWR|O_CREAT) : O_RDONLY, 0644);
    if (fd_ < 0) throw std::runtime_error("open failed: " + path_);
    if (writable && ::ftruncate(fd_, static_cast<off_t>(size_)) != 0)
        throw std::runtime_error("ftruncate failed: " + path_);
    mapNow();
}
void MappedFile::mapNow() {
    int prot = writable_ ? (PROT_READ|PROT_WRITE) : PROT_READ;
    void* p = ::mmap(nullptr, size_, prot, MAP_SHARED, fd_, 0);
    if (p == MAP_FAILED) throw std::runtime_error("mmap failed: " + path_);
    data_ = static_cast<uint8_t*>(p);
}
void MappedFile::resize(size_t newBytes) {
    if (data_) ::munmap(data_, size_);
    size_ = newBytes;
    if (writable_ && ::ftruncate(fd_, static_cast<off_t>(size_)) != 0)
        throw std::runtime_error("ftruncate failed: " + path_);
    mapNow();
}
void MappedFile::sync() { if (data_) ::msync(data_, size_, MS_SYNC); }
MappedFile::~MappedFile() { if (data_) ::munmap(data_, size_); if (fd_>=0) ::close(fd_); }
} // namespace rune::storage
```

- [ ] **Step 4: Add `MappedFile.cpp` to `rune_storage` sources and `MappedFileTest.cpp` to `rune_storage_tests` in `libs/storage/CMakeLists.txt`; build; run.**
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/MappedFile.h libs/storage/src/MappedFile.cpp libs/storage/tests/storage/MappedFileTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): RAII mmap file wrapper"
```

---

## Task 10: `OffsetIndex` — append + `lookup` (binary search, relative offsets)

**Files:**
- Create: `rune-broker/libs/storage/include/rune/storage/OffsetIndex.h`, `src/OffsetIndex.cpp`
- Test: `libs/storage/tests/storage/OffsetIndexTest.cpp`

**Interfaces:**
- Consumes: `MappedFile`.
- Produces: `struct OffsetPosition { int64_t offset; int32_t position; };` and
  `class OffsetIndex { OffsetIndex(std::string path, int64_t baseOffset, int32_t maxBytes); void append(int64_t offset, int32_t position); OffsetPosition lookup(int64_t targetOffset) const; int entries() const; };`
  `lookup` returns the largest entry with `offset <= target` (or `{baseOffset,0}` if none).

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <rune/storage/OffsetIndex.h>
#include <filesystem>
using namespace rune::storage;

TEST(OffsetIndex, AppendAndLookupLargestLowerBound) {
    auto p = (std::filesystem::temp_directory_path() / "rune.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 100 /*baseOffset*/, 8 * 1024);
    idx.append(105, 0);
    idx.append(110, 200);
    idx.append(120, 512);
    EXPECT_EQ(idx.entries(), 3);
    EXPECT_EQ(idx.lookup(104).position, 0);     // below first entry -> base
    EXPECT_EQ(idx.lookup(105).offset, 105);
    EXPECT_EQ(idx.lookup(115).offset, 110);     // largest <= 115
    EXPECT_EQ(idx.lookup(999).offset, 120);
    std::filesystem::remove(p);
}
```

- [ ] **Step 2: Run test to verify it fails.**

- [ ] **Step 3: Implement `OffsetIndex`** (8-byte entries, big/native-endian consistent within file)

```cpp
// OffsetIndex.h
#pragma once
#include <rune/storage/MappedFile.h>
#include <string>
#include <memory>
namespace rune::storage {
struct OffsetPosition { int64_t offset; int32_t position; };
class OffsetIndex {
public:
    OffsetIndex(std::string path, int64_t baseOffset, int32_t maxBytes);
    void append(int64_t offset, int32_t position);
    OffsetPosition lookup(int64_t targetOffset) const;
    int entries() const noexcept { return entries_; }
    int64_t baseOffset() const noexcept { return baseOffset_; }
protected:
    int slotFor(int64_t target) const;         // largest-lower-bound slot, -1 if none
    OffsetPosition parseEntry(int slot) const;
    static constexpr int kEntrySize = 8;
    int64_t baseOffset_; int entries_ = 0;
    std::unique_ptr<MappedFile> file_;
};
} // namespace rune::storage
```
```cpp
// OffsetIndex.cpp
#include <rune/storage/OffsetIndex.h>
#include <cstring>
#include <stdexcept>
namespace rune::storage {
OffsetIndex::OffsetIndex(std::string path, int64_t baseOffset, int32_t maxBytes)
    : baseOffset_(baseOffset) {
    file_ = std::make_unique<MappedFile>(std::move(path), maxBytes, true);
}
void OffsetIndex::append(int64_t offset, int32_t position) {
    int32_t rel = static_cast<int32_t>(offset - baseOffset_);
    if (rel < 0) throw std::runtime_error("offset below base");
    uint8_t* p = file_->data() + entries_ * kEntrySize;
    std::memcpy(p, &rel, 4);
    std::memcpy(p + 4, &position, 4);
    ++entries_;
}
OffsetPosition OffsetIndex::parseEntry(int slot) const {
    const uint8_t* p = file_->data() + slot * kEntrySize;
    int32_t rel, pos; std::memcpy(&rel, p, 4); std::memcpy(&pos, p + 4, 4);
    return { baseOffset_ + rel, pos };
}
int OffsetIndex::slotFor(int64_t target) const {
    if (entries_ == 0) return -1;
    int lo = 0, hi = entries_ - 1;
    if (parseEntry(0).offset > target) return -1;
    while (lo < hi) {
        int mid = (lo + hi + 1) >> 1;
        if (parseEntry(mid).offset <= target) lo = mid; else hi = mid - 1;
    }
    return lo;
}
OffsetPosition OffsetIndex::lookup(int64_t target) const {
    int slot = slotFor(target);
    if (slot < 0) return { baseOffset_, 0 };
    return parseEntry(slot);
}
} // namespace rune::storage
```

- [ ] **Step 4: Wire sources/tests into `libs/storage/CMakeLists.txt`; build; run.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/OffsetIndex.h libs/storage/src/OffsetIndex.cpp libs/storage/tests/storage/OffsetIndexTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): mmap offset index with relative-offset binary search"
```

---

## Task 11: `OffsetIndex` warm-section search

**Files:**
- Modify: `rune-broker/libs/storage/include/rune/storage/OffsetIndex.h`, `src/OffsetIndex.cpp`
- Test: `libs/storage/tests/storage/OffsetIndexTest.cpp`

**Interfaces:**
- Consumes/Produces: unchanged public API; `slotFor` now searches the warm section (last `8192/8 = 1024` entries) first, matching Kafka `AbstractIndex`. Adds `static constexpr int kWarmEntries = 8192 / kEntrySize;`.

- [ ] **Step 1: Write the failing test** (correctness must hold across the warm/cold boundary)

```cpp
TEST(OffsetIndex, WarmSectionBoundaryCorrectness) {
    auto p = (std::filesystem::temp_directory_path() / "rune_warm.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < 4000; ++i) idx.append(i * 10, i * 32); // 4000 entries > 1024 warm
    EXPECT_EQ(idx.lookup(5).offset, 0);
    EXPECT_EQ(idx.lookup(10 * 500 + 3).offset, 10 * 500);     // cold section
    EXPECT_EQ(idx.lookup(10 * 3999 + 1).offset, 10 * 3999);   // warm section (tail)
    EXPECT_EQ(idx.lookup(10 * 3000).offset, 10 * 3000);       // near boundary
    std::filesystem::remove(p);
}
```

- [ ] **Step 2: Run test — it PASSES with the plain search (correctness), but is not yet warm-optimized. To make the intent testable, first assert warm behavior via a counting hook.** Add a `mutable int lastSearchLow_ = 0;` recording the `begin` index used, and:

```cpp
TEST(OffsetIndex, WarmLookupSearchesTailOnly) {
    auto p = (std::filesystem::temp_directory_path() / "rune_warm2.index").string();
    std::filesystem::remove(p);
    OffsetIndex idx(p, 0, 64 * 1024);
    for (int i = 0; i < 4000; ++i) idx.append(i * 10, i);
    (void)idx.lookup(10 * 3999);
    EXPECT_GE(idx.lastSearchLow(), 4000 - 1024 - 1); // searched only the warm tail
    std::filesystem::remove(p);
}
```
Run: expect FAIL — `lastSearchLow()` undefined / not warm.

- [ ] **Step 3: Implement warm-section search** — replace `slotFor`:

```cpp
int OffsetIndex::slotFor(int64_t target) const {
    if (entries_ == 0) return -1;
    int firstWarm = entries_ - 1 - kWarmEntries; if (firstWarm < 0) firstWarm = 0;
    int begin, end;
    if (parseEntry(firstWarm).offset < target) { begin = firstWarm; end = entries_ - 1; }
    else {
        if (parseEntry(0).offset > target) { lastSearchLow_ = 0; return -1; }
        begin = 0; end = firstWarm;
    }
    lastSearchLow_ = begin;
    int lo = begin, hi = end;
    while (lo < hi) { int mid = (lo + hi + 1) >> 1;
        if (parseEntry(mid).offset <= target) lo = mid; else hi = mid - 1; }
    return lo;
}
```
Add to header: `static constexpr int kWarmEntries = 8192 / kEntrySize; mutable int lastSearchLow_ = 0; public: int lastSearchLow() const { return lastSearchLow_; }`

- [ ] **Step 4: Build; run both warm tests + the Task 10 tests (regression).** Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/OffsetIndex.h libs/storage/src/OffsetIndex.cpp libs/storage/tests/storage/OffsetIndexTest.cpp
git commit -m "feat(storage): cache-friendly warm-section index search"
```

---

## Task 12: `LogSegment` — append a batch, index it, read from offset

**Files:**
- Create: `rune-broker/libs/storage/include/rune/storage/LogSegment.h`, `src/LogSegment.cpp`
- Test: `libs/storage/tests/storage/LogSegmentTest.cpp`

**Interfaces:**
- Consumes: `RecordBatchBuilder`/`RecordBatchReader` (rune-protocol), `OffsetIndex`.
- Produces:
  - `class LogSegment { LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096); int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset); std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes); int64_t baseOffset() const; int64_t sizeBytes() const; int64_t nextOffset() const; };`
  - Segment files: `<dir>/<20-digit baseOffset>.log` and `.index`. `read` returns raw batch bytes beginning at the first batch whose lastOffset ≥ startOffset.

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
using namespace rune::storage; using namespace rune::protocol;

TEST(LogSegment, AppendThenReadBack) {
    auto dir = std::filesystem::temp_directory_path() / "rune_seg";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    LogSegment seg(dir.string(), 0);

    RecordBatchBuilder b0(0, 1000); b0.append("k", "v0", 1000);
    int64_t next = seg.append(b0.build(), 0);
    EXPECT_EQ(next, 1);
    RecordBatchBuilder b1(1, 1001); b1.append("k", "v1", 1001); b1.append("k", "v2", 1002);
    next = seg.append(b1.build(), 2);
    EXPECT_EQ(next, 3);
    EXPECT_EQ(seg.nextOffset(), 3);

    auto bytes = seg.read(1, 1 << 20);
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 1);
    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Run test — FAIL (`LogSegment` undefined).**

- [ ] **Step 3: Implement `LogSegment`** (append writes batch bytes to `.log`, records a sparse index entry every `indexIntervalBytes`)

```cpp
// LogSegment.h
#pragma once
#include <rune/storage/OffsetIndex.h>
#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
namespace rune::storage {
class LogSegment {
public:
    LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes = 4096);
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset); // returns nextOffset
    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t baseOffset() const noexcept { return baseOffset_; }
    int64_t sizeBytes() const noexcept { return logSize_; }
    int64_t nextOffset() const noexcept { return nextOffset_; }
    std::string logPath() const { return logPath_; }
private:
    static std::string fname(std::string dir, int64_t base, const char* ext);
    int64_t baseOffset_; int32_t indexIntervalBytes_;
    std::string logPath_; std::fstream log_;
    std::unique_ptr<OffsetIndex> index_;
    int64_t logSize_ = 0; int64_t nextOffset_; int64_t bytesSinceIndex_ = 0;
};
} // namespace rune::storage
```
```cpp
// LogSegment.cpp
#include <rune/storage/LogSegment.h>
#include <rune/protocol/RecordBatch.h>
#include <iomanip>
#include <sstream>
namespace rune::storage {
std::string LogSegment::fname(std::string dir, int64_t base, const char* ext) {
    std::ostringstream os; os << dir << "/" << std::setw(20) << std::setfill('0') << base << ext;
    return os.str();
}
LogSegment::LogSegment(std::string dir, int64_t baseOffset, int32_t indexIntervalBytes)
    : baseOffset_(baseOffset), indexIntervalBytes_(indexIntervalBytes), nextOffset_(baseOffset) {
    logPath_ = fname(dir, baseOffset, ".log");
    log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    if (!log_.is_open()) { std::ofstream(logPath_, std::ios::binary).close();
        log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary | std::ios::app); }
    index_ = std::make_unique<OffsetIndex>(fname(dir, baseOffset, ".index"), baseOffset, 8 * 1024 * 1024);
}
int64_t LogSegment::append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
    if (bytesSinceIndex_ == 0 || bytesSinceIndex_ >= indexIntervalBytes_) {
        rune::protocol::RecordBatchReader r(batch.data(), batch.size());
        index_->append(r.baseOffset(), static_cast<int32_t>(logSize_));
        bytesSinceIndex_ = 0;
    }
    log_.seekp(0, std::ios::end);
    log_.write(reinterpret_cast<const char*>(batch.data()), static_cast<std::streamsize>(batch.size()));
    log_.flush();
    logSize_ += static_cast<int64_t>(batch.size());
    bytesSinceIndex_ += static_cast<int64_t>(batch.size());
    nextOffset_ = lastOffset + 1;
    return nextOffset_;
}
std::vector<uint8_t> LogSegment::read(int64_t startOffset, int32_t maxBytes) {
    OffsetPosition op = index_->lookup(startOffset);
    log_.seekg(op.position, std::ios::beg);
    int64_t remaining = logSize_ - op.position;
    int64_t toRead = std::min<int64_t>(remaining, maxBytes);
    std::vector<uint8_t> buf(static_cast<size_t>(toRead));
    if (toRead > 0) log_.read(reinterpret_cast<char*>(buf.data()), toRead);
    // Skip batches whose lastOffset < startOffset (index is sparse -> may start earlier).
    size_t pos = 0;
    while (pos < buf.size()) {
        rune::protocol::RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int64_t batchLast = r.baseOffset() + r.lastOffsetDelta();
        if (batchLast >= startOffset) { return std::vector<uint8_t>(buf.begin() + pos, buf.end()); }
        pos += r.totalLength();
    }
    return {};
}
} // namespace rune::storage
```

- [ ] **Step 4: Wire into CMake; build; run.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/LogSegment.h libs/storage/src/LogSegment.cpp libs/storage/tests/storage/LogSegmentTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): log segment append + sparse-index-backed read"
```

---

## Task 13: `LogSegment::recover()` — validate CRC, truncate torn tail, rebuild index

**Files:**
- Modify: `rune-broker/libs/storage/include/rune/storage/LogSegment.h`, `src/LogSegment.cpp`
- Test: `libs/storage/tests/storage/LogSegmentTest.cpp`

**Interfaces:**
- Produces: `int64_t LogSegment::recover();` — scans the log from the start, `ensureValid()` each batch, stops at the first invalid/short batch, truncates the file there, rebuilds the offset index, sets `nextOffset_`/`logSize_`. Returns the recovered `nextOffset`.

- [ ] **Step 1: Write the failing test** (append two batches, corrupt the tail bytes on disk, reopen + recover → only valid prefix survives)

```cpp
#include <fstream>
TEST(LogSegment, RecoverTruncatesTornTail) {
    auto dir = std::filesystem::temp_directory_path() / "rune_seg_rec";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    std::string logPath;
    {
        LogSegment seg(dir.string(), 0);
        RecordBatchBuilder b0(0, 1000); b0.append("k","v0",1000); seg.append(b0.build(), 0);
        RecordBatchBuilder b1(1, 1001); b1.append("k","v1",1001); seg.append(b1.build(), 1);
        logPath = seg.logPath();
    }
    // Append 9 bytes of garbage to simulate a torn write.
    { std::ofstream f(logPath, std::ios::binary | std::ios::app); const char junk[9] = {1,2,3,4,5,6,7,8,9}; f.write(junk, 9); }
    LogSegment seg2(dir.string(), 0);
    int64_t next = seg2.recover();
    EXPECT_EQ(next, 2);                          // both good batches kept, junk dropped
    auto tail = seg2.read(1, 1<<20);
    EXPECT_TRUE(RecordBatchReader(tail.data(), tail.size()).ensureValid());
    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Run test — FAIL (`recover` undefined).**

- [ ] **Step 3: Implement `recover()`**

```cpp
int64_t LogSegment::recover() {
    log_.seekg(0, std::ios::end);
    int64_t fileSize = log_.tellg();
    std::vector<uint8_t> buf(static_cast<size_t>(fileSize));
    log_.seekg(0, std::ios::beg);
    if (fileSize > 0) log_.read(reinterpret_cast<char*>(buf.data()), fileSize);
    index_ = std::make_unique<OffsetIndex>(
        logPath_.substr(0, logPath_.size()-4) + ".index2", baseOffset_, 8*1024*1024); // rebuild fresh
    size_t pos = 0; int64_t validBytes = 0; int64_t next = baseOffset_; bytesSinceIndex_ = indexIntervalBytes_;
    using rune::protocol::RecordBatchReader;
    while (pos + rune::protocol::batch::kRecordBatchOverhead <= buf.size()) {
        RecordBatchReader r(buf.data() + pos, buf.size() - pos);
        int32_t total = r.totalLength();
        if (total <= 0 || pos + static_cast<size_t>(total) > buf.size() || !r.ensureValid()) break;
        if (bytesSinceIndex_ >= indexIntervalBytes_) { index_->append(r.baseOffset(), static_cast<int32_t>(validBytes)); bytesSinceIndex_ = 0; }
        validBytes += total; bytesSinceIndex_ += total;
        next = r.baseOffset() + r.lastOffsetDelta() + 1;
        pos += total;
    }
    // Truncate torn tail.
    std::filesystem::resize_file(logPath_, static_cast<uintmax_t>(validBytes));
    log_.close();
    log_.open(logPath_, std::ios::in | std::ios::out | std::ios::binary);
    logSize_ = validBytes; nextOffset_ = next;
    return nextOffset_;
}
```
Add `#include <filesystem>` to the `.cpp`.

- [ ] **Step 4: Build; run recover test + regression.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/LogSegment.h libs/storage/src/LogSegment.cpp libs/storage/tests/storage/LogSegmentTest.cpp
git commit -m "feat(storage): segment crash recovery (CRC-validate + truncate torn tail)"
```

---

## Task 14: `Log` — multi-segment partition log with rolling

**Files:**
- Create: `rune-broker/libs/storage/include/rune/storage/Log.h`, `src/Log.cpp`
- Test: `libs/storage/tests/storage/LogTest.cpp`

**Interfaces:**
- Consumes: `LogSegment`.
- Produces:
  - `class Log { Log(std::string dir, int64_t segmentRollBytes = 64*1024*1024); int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset); std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes); int64_t logEndOffset() const; int64_t recover(); std::vector<int64_t> segmentBaseOffsets() const; };`
  - Rolls a new segment when the active segment exceeds `segmentRollBytes`. `read` locates the owning segment by base offset (upper_bound − 1).

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <rune/storage/Log.h>
#include <rune/protocol/RecordBatch.h>
#include <filesystem>
using namespace rune::storage; using namespace rune::protocol;

TEST(Log, RollsSegmentsAndReadsAcross) {
    auto dir = std::filesystem::temp_directory_path() / "rune_log";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    Log log(dir.string(), /*segmentRollBytes*/ 256); // tiny to force rolls
    int64_t off = 0;
    for (int i = 0; i < 50; ++i) {
        RecordBatchBuilder b(off, 1000 + i); b.append("k", "val" + std::to_string(i), 1000 + i);
        off = log.append(b.build(), off);
    }
    EXPECT_EQ(log.logEndOffset(), 50);
    EXPECT_GT(log.segmentBaseOffsets().size(), 1u);   // rolled
    auto bytes = log.read(25, 1 << 20);
    RecordBatchReader r(bytes.data(), bytes.size());
    EXPECT_TRUE(r.ensureValid());
    EXPECT_EQ(r.baseOffset(), 25);
    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Run test — FAIL (`Log` undefined).**

- [ ] **Step 3: Implement `Log`**

```cpp
// Log.h
#pragma once
#include <rune/storage/LogSegment.h>
#include <map>
#include <memory>
#include <vector>
namespace rune::storage {
class Log {
public:
    Log(std::string dir, int64_t segmentRollBytes = 64LL*1024*1024);
    int64_t append(const std::vector<uint8_t>& batch, int64_t lastOffset);
    std::vector<uint8_t> read(int64_t startOffset, int32_t maxBytes);
    int64_t logEndOffset() const noexcept { return logEndOffset_; }
    int64_t recover();
    std::vector<int64_t> segmentBaseOffsets() const;
private:
    LogSegment& activeSegment() { return *segments_.rbegin()->second; }
    void maybeRoll();
    std::string dir_; int64_t rollBytes_; int64_t logEndOffset_ = 0;
    std::map<int64_t, std::unique_ptr<LogSegment>> segments_;
};
} // namespace rune::storage
```
```cpp
// Log.cpp
#include <rune/storage/Log.h>
#include <filesystem>
#include <algorithm>
namespace rune::storage {
Log::Log(std::string dir, int64_t segmentRollBytes) : dir_(std::move(dir)), rollBytes_(segmentRollBytes) {
    // Discover existing segments; else create the first at offset 0.
    std::vector<int64_t> bases;
    for (auto& e : std::filesystem::directory_iterator(dir_))
        if (e.path().extension() == ".log")
            bases.push_back(std::stoll(e.path().stem().string()));
    std::sort(bases.begin(), bases.end());
    if (bases.empty()) bases.push_back(0);
    for (int64_t b : bases) segments_[b] = std::make_unique<LogSegment>(dir_, b);
    logEndOffset_ = activeSegment().nextOffset();
}
void Log::maybeRoll() {
    if (activeSegment().sizeBytes() >= rollBytes_) {
        int64_t base = logEndOffset_;
        segments_[base] = std::make_unique<LogSegment>(dir_, base);
    }
}
int64_t Log::append(const std::vector<uint8_t>& batch, int64_t lastOffset) {
    maybeRoll();
    logEndOffset_ = activeSegment().append(batch, lastOffset);
    return logEndOffset_;
}
std::vector<uint8_t> Log::read(int64_t startOffset, int32_t maxBytes) {
    auto it = segments_.upper_bound(startOffset);
    if (it == segments_.begin()) return {};
    --it;
    return it->second->read(startOffset, maxBytes);
}
int64_t Log::recover() {
    int64_t next = 0;
    for (auto& [base, seg] : segments_) next = seg->recover();
    logEndOffset_ = next;
    return next;
}
std::vector<int64_t> Log::segmentBaseOffsets() const {
    std::vector<int64_t> v; for (auto& [b, _] : segments_) v.push_back(b); return v;
}
} // namespace rune::storage
```

- [ ] **Step 4: Wire into CMake; build; run.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/Log.h libs/storage/src/Log.cpp libs/storage/tests/storage/LogTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): partition Log with segment rolling + cross-segment read"
```

---

## Task 15: `RetentionPolicy` — delete whole segments below the horizon

**Files:**
- Create: `rune-broker/libs/storage/include/rune/storage/RetentionPolicy.h`, `src/RetentionPolicy.cpp`
- Modify: `Log.h`/`Log.cpp` (add `int deleteOldSegments(int64_t retainBytes)` and `int64_t totalBytes()`)
- Test: `libs/storage/tests/storage/RetentionTest.cpp`

**Interfaces:**
- Produces:
  - `Log`: `int64_t totalBytes() const;` and `int deleteOldSegments(int64_t retainBytes);` (deletes oldest whole non-active segments until `totalBytes ≤ retainBytes`; returns count deleted).
  - `struct RetentionPolicy { int64_t retainBytes; int64_t retainMillis; };` (Phase 1 uses size only; time hook stubbed with a comment for Phase 4).

- [ ] **Step 1: Write the failing test**

```cpp
TEST(Retention, DeletesOldestSegmentsBySize) {
    auto dir = std::filesystem::temp_directory_path() / "rune_ret";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    rune::storage::Log log(dir.string(), 128); // small segments
    int64_t off = 0;
    for (int i = 0; i < 40; ++i) { rune::protocol::RecordBatchBuilder b(off, i);
        b.append("k", "payloadpayload" + std::to_string(i), i); off = log.append(b.build(), off); }
    size_t before = log.segmentBaseOffsets().size();
    int deleted = log.deleteOldSegments(/*retainBytes*/ 256);
    EXPECT_GT(deleted, 0);
    EXPECT_LT(log.segmentBaseOffsets().size(), before);
    EXPECT_LE(log.totalBytes(), 256 + 128); // within one active segment of target
    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Run test — FAIL (`deleteOldSegments` undefined).**

- [ ] **Step 3: Implement** — add to `Log.cpp`:

```cpp
int64_t Log::totalBytes() const {
    int64_t t = 0; for (auto& [b, seg] : segments_) t += seg->sizeBytes(); return t;
}
int Log::deleteOldSegments(int64_t retainBytes) {
    int deleted = 0;
    while (segments_.size() > 1 && totalBytes() > retainBytes) {
        auto it = segments_.begin();
        std::string base = it->second->logPath();
        std::string idx = base.substr(0, base.size()-4) + ".index";
        segments_.erase(it);
        std::filesystem::remove(base);
        std::filesystem::remove(idx);
        ++deleted;
    }
    return deleted;
}
```
Declare both in `Log.h`. Create `RetentionPolicy.h` with the struct + doc comment noting time-based retention is wired in Phase 4 using segment first-batch `baseTimestamp`.

- [ ] **Step 4: Build; run.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/RetentionPolicy.h libs/storage/src/RetentionPolicy.cpp libs/storage/include/rune/storage/Log.h libs/storage/src/Log.cpp libs/storage/tests/storage/RetentionTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): size-based retention via whole-segment deletion"
```

---

## Task 16: `LeaderEpochCache` + `ProducerState` skeletons (reserved for Phases 3/5)

**Files:**
- Create: `rune-broker/libs/storage/include/rune/storage/LeaderEpochCache.h`, `src/LeaderEpochCache.cpp`, `include/rune/storage/ProducerState.h`, `src/ProducerState.cpp`
- Test: `libs/storage/tests/storage/LeaderEpochCacheTest.cpp`, `ProducerStateTest.cpp`

**Interfaces:**
- Produces:
  - `class LeaderEpochCache { void assign(int32_t epoch, int64_t startOffset); int64_t startOffsetFor(int32_t epoch) const; int32_t latestEpoch() const; int64_t truncateOffsetForEpoch(int32_t leaderEpoch) const; };` (used by Phase 3 failover truncation)
  - `class ProducerState { enum class Result { Ok, Duplicate, OutOfSequence }; Result validateAndUpdate(int64_t producerId, int16_t epoch, int32_t baseSequence, int32_t recordCount); };` (idempotency dedup, wired in Phase 5)

- [ ] **Step 1: Write the failing tests**

```cpp
// LeaderEpochCacheTest.cpp
#include <gtest/gtest.h>
#include <rune/storage/LeaderEpochCache.h>
using rune::storage::LeaderEpochCache;
TEST(LeaderEpochCache, TracksEpochStartsAndTruncation) {
    LeaderEpochCache c;
    c.assign(0, 0); c.assign(1, 100); c.assign(2, 250);
    EXPECT_EQ(c.latestEpoch(), 2);
    EXPECT_EQ(c.startOffsetFor(1), 100);
    // A follower on epoch 1 must truncate to the start of the next epoch (250).
    EXPECT_EQ(c.truncateOffsetForEpoch(1), 250);
}
```
```cpp
// ProducerStateTest.cpp
#include <gtest/gtest.h>
#include <rune/storage/ProducerState.h>
using rune::storage::ProducerState;
TEST(ProducerState, DetectsDuplicatesAndGaps) {
    ProducerState ps;
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 0, 3), ProducerState::Result::Ok);   // seq 0..2
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 0, 3), ProducerState::Result::Duplicate);
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 3, 2), ProducerState::Result::Ok);   // seq 3..4
    EXPECT_EQ(ps.validateAndUpdate(7, 0, 10, 1), ProducerState::Result::OutOfSequence);
}
```

- [ ] **Step 2: Run tests — FAIL.**

- [ ] **Step 3: Implement both**

```cpp
// LeaderEpochCache.h
#pragma once
#include <cstdint>
#include <map>
namespace rune::storage {
class LeaderEpochCache {
public:
    void assign(int32_t epoch, int64_t startOffset) { epochs_[epoch] = startOffset; }
    int64_t startOffsetFor(int32_t epoch) const { auto it = epochs_.find(epoch); return it==epochs_.end()? -1 : it->second; }
    int32_t latestEpoch() const { return epochs_.empty()? -1 : epochs_.rbegin()->first; }
    int64_t truncateOffsetForEpoch(int32_t leaderEpoch) const {
        auto it = epochs_.upper_bound(leaderEpoch);
        return it == epochs_.end() ? -1 : it->second;
    }
private:
    std::map<int32_t,int64_t> epochs_;
};
} // namespace rune::storage
```
```cpp
// ProducerState.h
#pragma once
#include <cstdint>
#include <unordered_map>
namespace rune::storage {
class ProducerState {
public:
    enum class Result { Ok, Duplicate, OutOfSequence };
    Result validateAndUpdate(int64_t producerId, int16_t epoch, int32_t baseSequence, int32_t recordCount) {
        auto it = last_.find(producerId);
        int32_t expected = (it == last_.end()) ? 0 : it->second.nextSeq;
        if (it != last_.end() && baseSequence < expected) return Result::Duplicate;
        if (baseSequence != expected) return Result::OutOfSequence;
        last_[producerId] = { epoch, baseSequence + recordCount };
        return Result::Ok;
    }
private:
    struct Entry { int16_t epoch; int32_t nextSeq; };
    std::unordered_map<int64_t, Entry> last_;
};
} // namespace rune::storage
```
(Header-only; create empty `.cpp` files only if CMake needs a TU, else omit from sources.)

- [ ] **Step 4: Wire tests into CMake; build; run.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/LeaderEpochCache.h libs/storage/include/rune/storage/ProducerState.h libs/storage/tests/storage/LeaderEpochCacheTest.cpp libs/storage/tests/storage/ProducerStateTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): leader-epoch cache + producer-state dedup (reserved for P3/P5)"
```

---

## Task 17: Single-node in-process `PartitionApi` (produce/consume facade)

**Files:**
- Create: `rune-broker/libs/storage/include/rune/storage/PartitionApi.h`, `src/PartitionApi.cpp`
- Test: `libs/storage/tests/storage/PartitionApiTest.cpp`

**Interfaces:**
- Consumes: `Log`, `RecordBatchBuilder`, `RecordBatchReader`.
- Produces:
  - `struct ConsumedRecord { std::string key, value; int64_t offset; int64_t timestamp; };`
  - `class PartitionApi { explicit PartitionApi(std::string dir); int64_t produce(std::string_view key, std::string_view value, int64_t timestamp); std::vector<ConsumedRecord> consume(int64_t fromOffset, int32_t maxBytes = 1<<20); int64_t highWatermark() const; };`
  - Phase-1 semantics: single node, so `highWatermark == logEndOffset` (no replication yet). One record per batch for simplicity; batching added when the network producer lands (Phase 3).

- [ ] **Step 1: Write the failing test** (end-to-end produce → consume, ordering, restart durability)

```cpp
#include <gtest/gtest.h>
#include <rune/storage/PartitionApi.h>
#include <filesystem>
using namespace rune::storage;

TEST(PartitionApi, ProduceConsumeAndSurviveRestart) {
    auto dir = std::filesystem::temp_directory_path() / "rune_api";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    {
        PartitionApi api(dir.string());
        EXPECT_EQ(api.produce("a", "1", 1000), 1);
        EXPECT_EQ(api.produce("b", "2", 1001), 2);
        EXPECT_EQ(api.produce("a", "3", 1002), 3);
        EXPECT_EQ(api.highWatermark(), 3);
        auto recs = api.consume(0);
        ASSERT_EQ(recs.size(), 3u);
        EXPECT_EQ(recs[0].value, "1"); EXPECT_EQ(recs[0].offset, 0);
        EXPECT_EQ(recs[2].value, "3"); EXPECT_EQ(recs[2].offset, 2);
        auto tail = api.consume(2);
        ASSERT_EQ(tail.size(), 1u); EXPECT_EQ(tail[0].value, "3");
    }
    { // reopen: data persisted
        PartitionApi api(dir.string());
        EXPECT_EQ(api.highWatermark(), 3);
        EXPECT_EQ(api.consume(0).size(), 3u);
    }
    std::filesystem::remove_all(dir);
}
```

- [ ] **Step 2: Run test — FAIL (`PartitionApi` undefined).**

- [ ] **Step 3: Implement `PartitionApi`**

```cpp
// PartitionApi.h
#pragma once
#include <rune/storage/Log.h>
#include <string>
#include <string_view>
#include <vector>
namespace rune::storage {
struct ConsumedRecord { std::string key, value; int64_t offset; int64_t timestamp; };
class PartitionApi {
public:
    explicit PartitionApi(std::string dir);
    int64_t produce(std::string_view key, std::string_view value, int64_t timestamp);
    std::vector<ConsumedRecord> consume(int64_t fromOffset, int32_t maxBytes = 1 << 20);
    int64_t highWatermark() const { return log_.logEndOffset(); }
private:
    Log log_;
};
} // namespace rune::storage
```
```cpp
// PartitionApi.cpp
#include <rune/storage/PartitionApi.h>
#include <rune/protocol/RecordBatch.h>
namespace rune::storage {
PartitionApi::PartitionApi(std::string dir) : log_(std::move(dir)) { log_.recover(); }

int64_t PartitionApi::produce(std::string_view key, std::string_view value, int64_t timestamp) {
    int64_t baseOffset = log_.logEndOffset();
    rune::protocol::RecordBatchBuilder b(baseOffset, timestamp);
    b.append(key, value, timestamp);
    return log_.append(b.build(), baseOffset); // single record -> lastOffset == baseOffset
}

std::vector<ConsumedRecord> PartitionApi::consume(int64_t fromOffset, int32_t maxBytes) {
    std::vector<ConsumedRecord> out;
    int64_t cursor = fromOffset;
    int64_t hw = highWatermark();
    while (cursor < hw) {
        auto bytes = log_.read(cursor, maxBytes);
        if (bytes.empty()) break;
        size_t pos = 0;
        bool advanced = false;
        while (pos < bytes.size()) {
            rune::protocol::RecordBatchReader r(bytes.data() + pos, bytes.size() - pos);
            if (!r.ensureValid()) return out;
            r.forEach([&](std::string_view k, std::string_view v, int64_t off, int64_t ts) {
                if (off >= fromOffset && off < hw) out.push_back({std::string(k), std::string(v), off, ts});
            });
            cursor = r.baseOffset() + r.lastOffsetDelta() + 1;
            pos += r.totalLength();
            advanced = true;
        }
        if (!advanced) break;
    }
    return out;
}
} // namespace rune::storage
```

- [ ] **Step 4: Wire into CMake; build; run.** Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/PartitionApi.h libs/storage/src/PartitionApi.cpp libs/storage/tests/storage/PartitionApiTest.cpp libs/storage/CMakeLists.txt
git commit -m "feat(storage): single-node produce/consume PartitionApi facade"
```

---

## Task 18: Coverage gate + `benchmark` latency harness skeleton + docs

**Files:**
- Create: `rune-broker/benchmark/StorageBenchmark.cpp`, `rune-broker/benchmark/BenchmarkMain.cpp`, `rune-broker/README.md`, `rune-protocol/README.md`
- Modify: root `CMakeLists.txt` (BUILD_BENCHMARKS option + coverage flags), `.woodpecker/build.yml` (coverage step)

**Interfaces:** none (tooling/docs). Establishes the p99 latency harness (real numbers gated from Phase 3) and a coverage report the CI enforces.

- [ ] **Step 1: Add coverage flags to root `CMakeLists.txt`**

```cmake
option(ENABLE_COVERAGE "Enable coverage instrumentation" OFF)
if(ENABLE_COVERAGE)
    add_compile_options(--coverage -O0 -g)
    add_link_options(--coverage)
endif()
```

- [ ] **Step 2: Add benchmark harness**

```cpp
// benchmark/StorageBenchmark.cpp
#include <benchmark/benchmark.h>
#include <rune/storage/PartitionApi.h>
#include <filesystem>
static void BM_Produce(benchmark::State& state) {
    auto dir = std::filesystem::temp_directory_path() / "rune_bench";
    std::filesystem::remove_all(dir); std::filesystem::create_directories(dir);
    rune::storage::PartitionApi api(dir.string());
    int64_t ts = 0;
    for (auto _ : state) { api.produce("k", "value-payload", ts++); }
    std::filesystem::remove_all(dir);
}
BENCHMARK(BM_Produce);
```
```cpp
// benchmark/BenchmarkMain.cpp
#include <benchmark/benchmark.h>
BENCHMARK_MAIN();
```
Add a `BUILD_BENCHMARKS` block to `CMakeLists.txt` linking `benchmark::benchmark` + `rune_storage` (mirroring `common`).

- [ ] **Step 3: Write `README.md` for each project** — one paragraph: what it is, how to build (`conan install` + `cmake` incantation from Task 1 Step 9), how to run tests (`ctest`), link to the spec. Note the p99<100µs target becomes a hard CI gate at Phase 3.

- [ ] **Step 4: Add a coverage step to `.woodpecker/build.yml`**

```yaml
  coverage:
    image: rune-cpp-build:latest
    commands:
      - cmake . -B cov -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DENABLE_COVERAGE=ON -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators"
      - cmake --build cov -j
      - cd cov && ctest --output-on-failure
      - gcovr -r .. --fail-under-line 90
```

- [ ] **Step 5: Build with benchmarks locally to verify it compiles (do not gate on numbers yet)**

Run: reconfigure with `-DBUILD_BENCHMARKS=ON`, `cmake --build build -j`, run `./build/rune_broker_benchmark` once.
Expected: benchmark runs and prints timings.

- [ ] **Step 6: Commit**

```bash
git add benchmark/ README.md CMakeLists.txt .woodpecker/build.yml
git commit -m "chore(broker): coverage gate, latency benchmark harness, READMEs"
```

---

## Phase 1 Exit Criteria

- `rune-protocol` and `rune-broker` build clean (x86_64) with Conan 2 + CMake, mirroring `common`/`world-server`.
- All unit tests green via `ctest`; coverage ≥ 90% line on `rune_protocol` + `librune_storage` (target 100% on pure logic; documented exclusions).
- Record-batch v2 encode/decode with CRC-32C validation; leader-epoch stamping does not invalidate CRC.
- Offset index with warm-section search; segment append/read/roll; `recover()` truncates torn tails; size-based retention; single-node produce/consume survives restart.
- Benchmark harness runs (p99 gate activates in Phase 3).
- No `conan create/export/upload` executed by the agent.

## Deferred to later phase plans

- **Phase 2:** `librune_consensus` — C++ Raft metadata quorum (Election, QuorumState, log replication, snapshots) over Aeron; `librune_gateway` Aeron ingress + session/auth.
- **Phase 3:** `librune_replication` — ISR, high-watermark, leader failover, follower catch-up + truncation (uses `LeaderEpochCache`); Aeron Archive integration (`librune_archive`); `rune-client` network library; **p99<100µs gate**; Testcontainers cluster chaos suite. Latency work lands here: **`mlock`/pin** the active-segment index + log tail (spec §6.3/§9) and the **hardware CRC-32C** intrinsic (spec §9) — both introduced with, and justified by, the latency gate.
- **Phase 4:** consumer groups + coordinator; log compaction (`LogCleaner`); time-based retention; `__offsets` topic.
- **Phase 5:** idempotent producer + transactions (uses `ProducerState`); transaction coordinator + control markers.
