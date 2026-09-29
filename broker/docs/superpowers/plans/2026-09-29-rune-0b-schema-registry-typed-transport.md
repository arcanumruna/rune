# Rune 0b — Schema Registry & Typed Transport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a runtime, replicated, versioned schema registry to the broker plus a compile-time-typed client path (`openTopic<T>()`), built on the completed rune-codec (0a).

**Architecture:** New rune-protocol messages (templateIds 64–67) carry a rune-codec descriptor blob. The broker persists schemas in a compacted, replicated `__topic_schemas` topic and serves DDL from a dedicated **AdminWorker** thread that owns the in-memory `SchemaRegistry` and does all blocking work off the hot path. Produce workers optionally validate records against per-worker validator handles pushed over their ingress rings. The client fetches a topic's descriptor, checks its generated type's fingerprint (exact match), and produces typed records through the existing producer path.

**Tech Stack:** C++23, CMake ≥3.31, Conan 2, GoogleTest, Google Benchmark, Aeron. Three repos: `rune-protocol` (header-only, sibling include), `rune-broker` (x86_64 prod), `rune-client-cpp`; `rune-codec` (0a) consumed read-only as a sibling source static library.

**Spec:** `/Users/vradu/Projects/rune-broker-matviews/docs/superpowers/specs/2026-09-29-rune-0b-schema-registry-typed-transport-design.md`

## Global Constraints

- **Engineering philosophy (HARD RULES, apply everywhere):** shared-nothing (no shared mutable state/locks/atomics-as-coordination/`thread_local`); ring buffers are the ONLY cross-thread event passing; state machines where each state holds exactly its variables; zero heap allocation on hot paths / steady state; test + benchmark everything via real e2e/integration; regression test on every fix (suite named `*Regression*`); run the full regression suite FIRST on every fix; fix-on-sight for any philosophy violation you read.
- **Coexist, don't replace:** the env (`RUNE_TOPICS`) topic-creation path keeps working; `DefineTopic` adds runtime creation.
- **Split topicId space:** env `[0..N_env)`; internal negative sentinels (`__topic_schemas = -4`); runtime `DefineTopic` ids from `RUNTIME_TOPIC_ID_BASE = 0x40000000` counting up.
- **Exact fingerprint match** at `openTopic<T>()` (no additive-compat, no evolution in 0b — every topic is version 1).
- **Validation off by default**; opt-in per topic via `DefineTopic.validate`.
- **DDL runs on the AdminWorker thread**, never the gateway or produce workers. Gateway only enqueues DDL onto the AdminWorker ring. Cross-thread passing is ring-buffer-only.
- **rune-codec consumed as sibling source static lib** (the `rune-fr` template) via an overridable `RUNE_CODEC_ROOT`; compiled under the consumer's own toolchain.
- **Wire framing:** all little-endian; `MessageHeader` is 24 bytes and already carries `sessionId`+`correlationId` — message bodies never repeat them. Use `rune::protocol::client` `detail::putLE16/getLE16/putLE32/getLE32`, `putVarStr/getVarStr`, `putVarBytes32/getVarBytes32`.
- **rune-codec public API (0a, do not modify):** `rune::codec::Descriptor`, `FieldDescriptor`, `DescriptorBuilder(name,version).add(name,PrimType,sinceVersion,isKey,enumValues).build()`; `serialize(const Descriptor&)->std::vector<uint8_t>`; `deserialize(std::span<const uint8_t>)->Descriptor` (throws `std::runtime_error`/`std::invalid_argument` on malformed); `fingerprint(const Descriptor&)->uint64_t` (noexcept); `class Validator{ explicit Validator(const Descriptor&); ValResult validate(std::span<const uint8_t>) const noexcept; }` with `ValResult{bool ok; uint16_t fieldId; const char* reason;}`; `generateHeader(const Descriptor&, std::string_view nspace="rune::gen")->std::string`. `PrimType{Int32,Int64,UInt16,UInt32,UInt64,Float64,Bool,Enum,String,Bytes}`.
- **Toolchains (NOT on PATH):** rune-codec builds used cmake `/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin/cmake`. rune-broker/rune-client/rune-protocol use Conan 2 (`conan install . --build=missing` then the generated CMake preset) — follow each repo's existing README/Dockerfile; on macos arm64 the aeron stub target lets configure succeed.
- **Worktrees (created at execution time, SDD setup step):** one per repo touched — `rune-protocol`, `rune-broker`, `rune-client-cpp` — branch `feature/0b-schema-registry` off each repo's `main`. rune-codec is NOT worktreed (read-only from its `main`). Worktree builds pass `-DRUNE_PROTOCOL_INCLUDE=<sibling-worktree>/include` and `-DRUNE_CODEC_ROOT=<rune-codec>` so sibling paths resolve.

---

## File structure

**rune-protocol** (`include/rune/protocol/client/`):
- Create `SchemaMessages.h` — the 4 message codecs.
- Modify `Protocol.h` — `TemplateId` 64–67, `ErrorCode` 23–25, range comment.
- Modify `Dispatch.h` — `isKnownTemplate` accepts 64–67.
- Create `conformance/schema_messages_vectors.h` (or the repo's vector format) — golden bytes.

**rune-broker:**
- Modify `CMakeLists.txt` — `RUNE_PROTOCOL_INCLUDE`→CACHE, add `RUNE_CODEC_ROOT` + `rune_codec` static lib target.
- Create `libs/node/include/rune/node/SchemaRegistry.h`, `libs/node/src/SchemaRegistry.cpp` — `TopicSchema` record + (de)serialize + in-memory registry.
- Create `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp` — DDL op state machine (dependency-injected).
- Modify `libs/node/include/rune/node/NodeConfig.h` — `TopicSpec += {keyField, validate}`.
- Modify `libs/gateway/include/rune/gateway/TopicRegistry.h`, `.../src/TopicRegistry.cpp` — runtime `createTopic(...)`.
- Modify `libs/node/src/BrokerNode.cpp` — `__topic_schemas` log/replica wiring + spawn AdminWorker + wire real deps.
- Modify `libs/gateway/src/ClientGateway.cpp` — route 64/66 to AdminWorker; response egress.
- Modify `libs/node/include/rune/node/Worker.h` — validation hook + `SchemaUpdate` handling.

**rune-client-cpp:**
- Modify `CMakeLists.txt` — same CACHE + `RUNE_CODEC_ROOT` treatment.
- Modify `include/rune/client/Client.h`, `src/Client.cpp` — `pendingSchemaOps_`, `defineTopic`, `getTopicSchema`, `openTopic<T>`.
- Create `include/rune/client/TypedTopic.h` — `TypedTopic<T>`.
- Modify `include/rune/client/metadata/MetadataCache.h` — `{schemaVersion, validate, keyField}`.
- Create `tools/rune_schema_gen.cpp` — the generate CLI.
- Create the e2e test + a checked-in `tests/generated/characters.blob`.

---

## Task 1: rune-protocol schema messages (templateIds 64–67)

**Files:**
- Create: `include/rune/protocol/client/SchemaMessages.h`
- Modify: `include/rune/protocol/client/Protocol.h` (enums + comment)
- Modify: `include/rune/protocol/client/Dispatch.h` (`isKnownTemplate`)
- Test: `tests/SchemaMessagesTest.cpp` (add to the repo's test target via `gtest_discover_tests`)

**Interfaces:**
- Consumes: `MessageHeader` (24 bytes, fields `sessionId`,`correlationId`,`templateId`,`schemaId`,`version`,`blockLength`); `kSchemaId`,`kProtocolVersion`; `detail::putLE16/getLE16/putLE32/getLE32`; `putVarStr/getVarStr`; `putVarBytes32/getVarBytes32`.
- Produces: `struct DefineTopic{int64_t sessionId,correlationId; std::string_view topicName; uint32_t partitionCount; std::string_view keyField; bool validate; const uint8_t* descriptorBlob; uint32_t descriptorLen; …encode/decode}`; `struct DefineTopicResponse{…; int32_t topicId; uint16_t version; uint16_t errorCode;}`; `struct GetTopicSchema{…; std::string_view topicName;}`; `struct TopicSchemaResponse{…; int32_t topicId; uint16_t version; const uint8_t* descriptorBlob; uint32_t descriptorLen; uint16_t errorCode;}`. `TemplateId::{DefineTopic=64,DefineTopicResponse=65,GetTopicSchema=66,TopicSchemaResponse=67}`; `ErrorCode::{InvalidSchema=23,SchemaValidationFailed=24,TopicExists=25}` (reuse `UnknownTopic=7`).

- [ ] **Step 1: Add the enum values (Protocol.h)**

After `EndTxnResponse = 54,` in `enum class TemplateId`, add:
```cpp
    // schema/view range (64-95) — 0b schema registry
    DefineTopic          = 64,
    DefineTopicResponse  = 65,
    GetTopicSchema       = 66,
    TopicSchemaResponse  = 67,
```
After `MessageTooLarge = 22,` in `enum class ErrorCode`, add (note `UnknownTopic=7` already exists — reuse it, do NOT redefine):
```cpp
    // schema-registry error codes (0b)
    InvalidSchema          = 23,  // descriptor blob malformed / invariant-violating
    SchemaValidationFailed = 24,  // produced record failed validation
    TopicExists            = 25,  // DefineTopic on an existing topic name
```

- [ ] **Step 2: Extend `isKnownTemplate` (Dispatch.h)**

Before `return false;` add:
```cpp
    // 0b schema range: 64..67
    if (tid >= static_cast<uint16_t>(TemplateId::DefineTopic) &&
        tid <= static_cast<uint16_t>(TemplateId::TopicSchemaResponse)) {
        return true;
    }
```

- [ ] **Step 3: Write the failing test (`tests/SchemaMessagesTest.cpp`)**

```cpp
#include <gtest/gtest.h>
#include <vector>
#include <cstring>
#include "rune/protocol/client/SchemaMessages.h"
#include "rune/protocol/client/Dispatch.h"

using namespace rune::protocol::client;

TEST(SchemaMessages, DefineTopicRoundTrip) {
    std::vector<uint8_t> blob = {1, 2, 3, 4, 5};  // opaque descriptor bytes
    DefineTopic in;
    in.sessionId = 7; in.correlationId = 99;
    in.topicName = "characters";
    in.partitionCount = 16;
    in.keyField = "userId";
    in.validate = true;
    in.descriptorBlob = blob.data();
    in.descriptorLen = static_cast<uint32_t>(blob.size());

    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    EXPECT_EQ(peekTemplateId(buf.data(), n),
              static_cast<uint16_t>(TemplateId::DefineTopic));

    auto out = DefineTopic::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->sessionId, 7);
    EXPECT_EQ(out->correlationId, 99);
    EXPECT_EQ(out->topicName, "characters");
    EXPECT_EQ(out->partitionCount, 16u);
    EXPECT_EQ(out->keyField, "userId");
    EXPECT_TRUE(out->validate);
    ASSERT_EQ(out->descriptorLen, blob.size());
    EXPECT_EQ(0, std::memcmp(out->descriptorBlob, blob.data(), blob.size()));
}

TEST(SchemaMessages, DefineTopicResponseRoundTrip) {
    DefineTopicResponse in;
    in.sessionId = 7; in.correlationId = 99;
    in.topicId = 0x40000000; in.version = 1;
    in.errorCode = static_cast<uint16_t>(ErrorCode::OK);
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = DefineTopicResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicId, 0x40000000);
    EXPECT_EQ(out->version, 1);
    EXPECT_EQ(out->errorCode, 0);
}

TEST(SchemaMessages, GetTopicSchemaRoundTrip) {
    GetTopicSchema in; in.sessionId = 1; in.correlationId = 2;
    in.topicName = "characters";
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = GetTopicSchema::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicName, "characters");
}

TEST(SchemaMessages, TopicSchemaResponseRoundTrip) {
    std::vector<uint8_t> blob = {9, 8, 7};
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 5; in.version = 1;
    in.descriptorBlob = blob.data(); in.descriptorLen = 3;
    in.errorCode = static_cast<uint16_t>(ErrorCode::OK);
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    auto out = TopicSchemaResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicId, 5);
    ASSERT_EQ(out->descriptorLen, 3u);
    EXPECT_EQ(0, std::memcmp(out->descriptorBlob, blob.data(), 3));
}

TEST(SchemaMessages, DecodeRejectsTruncated) {
    DefineTopic in; in.topicName = "x"; in.keyField = "";
    std::vector<uint8_t> buf(in.encodedSize());
    in.encode(buf.data());
    EXPECT_FALSE(DefineTopic::decode(buf.data(), buf.size() - 1).has_value());
}
```

- [ ] **Step 4: Run the test, verify it fails to compile** (`SchemaMessages.h` not yet written). Build the protocol test target; expect a compile error on the missing header.

- [ ] **Step 5: Implement `SchemaMessages.h`**

Follow `MetadataMessages.h` exactly (header via `MessageHeader`, `BODY_FIXED=0`, var-fields after). `topicName`/`keyField` use `putVarStr/getVarStr`; the descriptor uses `putVarBytes32/getVarBytes32` (blobs may exceed 65535). Fixed scalars (`partitionCount`,`topicId`,`version`,`errorCode`,`validate`) go in the body as LE at fixed offsets before the var fields.
```cpp
#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include "Codec.h"
#include "MessageHeader.h"
#include "Protocol.h"

namespace rune::protocol::client {

// DefineTopic (64): body = partitionCount:u32, validate:u8, then var fields
//   topicName(varstr), keyField(varstr), descriptorBlob(varbytes32).
struct DefineTopic {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    uint32_t partitionCount = 1;
    std::string_view keyField;
    bool validate = false;
    const uint8_t* descriptorBlob = nullptr;
    uint32_t descriptorLen = 0;

    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DefineTopic);
    static constexpr uint16_t BODY_FIXED  = 5;  // u32 partitionCount + u8 validate

    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED
             + 2u + topicName.size()
             + 2u + keyField.size()
             + 4u + descriptorLen;
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
        return m;
    }
};

// DefineTopicResponse (65): body = topicId:i32, version:u16, errorCode:u16.
struct DefineTopicResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::DefineTopicResponse);
    static constexpr uint16_t BODY_FIXED  = 8;
    std::size_t encodedSize() const noexcept { return MessageHeader::ENCODED_SIZE + BODY_FIXED; }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), topicId); pos += 4;
        detail::putLE16(buf, static_cast<int>(pos), version); pos += 2;
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        return pos;
    }
    static std::optional<DefineTopicResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        DefineTopicResponse m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.topicId = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        m.version = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        m.errorCode = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        return m;
    }
};

// GetTopicSchema (66): body = topicName(varstr).
struct GetTopicSchema {
    int64_t sessionId = 0, correlationId = 0;
    std::string_view topicName;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::GetTopicSchema);
    static constexpr uint16_t BODY_FIXED  = 0;
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + 2u + topicName.size();
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        pos += putVarStr(buf + pos, topicName);
        return pos;
    }
    static std::optional<GetTopicSchema> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        GetTopicSchema m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        if (!getVarStr(buf, avail, pos, m.topicName)) return std::nullopt;
        return m;
    }
};

// TopicSchemaResponse (67): body = topicId:i32, version:u16, errorCode:u16, then descriptorBlob(varbytes32).
struct TopicSchemaResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0;
    const uint8_t* descriptorBlob = nullptr; uint32_t descriptorLen = 0;
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::TopicSchemaResponse);
    static constexpr uint16_t BODY_FIXED  = 8;
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u + descriptorLen;
    }
    std::size_t encode(uint8_t* buf) const noexcept {
        MessageHeader hdr; hdr.blockLength = BODY_FIXED; hdr.templateId = TEMPLATE_ID;
        hdr.schemaId = kSchemaId; hdr.version = kProtocolVersion;
        hdr.sessionId = sessionId; hdr.correlationId = correlationId; hdr.encode(buf);
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        detail::putLE32(buf, static_cast<int>(pos), topicId); pos += 4;
        detail::putLE16(buf, static_cast<int>(pos), version); pos += 2;
        detail::putLE16(buf, static_cast<int>(pos), errorCode); pos += 2;
        pos += putVarBytes32(buf + pos, descriptorBlob, descriptorLen);
        return pos;
    }
    static std::optional<TopicSchemaResponse> decode(const uint8_t* buf, std::size_t avail) noexcept {
        auto h = MessageHeader::decode(buf, avail);
        if (!h || h->schemaId != kSchemaId || h->templateId != TEMPLATE_ID) return std::nullopt;
        std::size_t pos = MessageHeader::ENCODED_SIZE;
        if (avail < pos + BODY_FIXED) return std::nullopt;
        TopicSchemaResponse m; m.sessionId = h->sessionId; m.correlationId = h->correlationId;
        m.topicId = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        m.version = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        m.errorCode = detail::getLE16(buf, static_cast<int>(pos)); pos += 2;
        if (!getVarBytes32(buf, avail, pos, m.descriptorBlob, m.descriptorLen)) return std::nullopt;
        return m;
    }
};

} // namespace rune::protocol::client
```

- [ ] **Step 6: Run the tests, verify all pass.** Build + run the rune-protocol test target (`ctest` / `gtest_discover_tests`).

- [ ] **Step 7: Add a conformance vector for each new message** in the repo's conformance format (mirror an existing vector; capture golden bytes for one `DefineTopic` and one `TopicSchemaResponse`), and assert decode of the golden bytes.

- [ ] **Step 8: Commit**
```bash
git add include/rune/protocol/client/SchemaMessages.h include/rune/protocol/client/Protocol.h \
        include/rune/protocol/client/Dispatch.h tests/SchemaMessagesTest.cpp conformance/
git commit -m "feat(protocol): 0b schema messages (DefineTopic/GetTopicSchema, ids 64-67)"
```

---

## Task 2: rune-broker build integration (consume rune-codec)

**Files:**
- Modify: `CMakeLists.txt:6-7` (make `RUNE_PROTOCOL_INCLUDE` a cache var) and the deps block (add `rune_codec`).
- Test: `libs/node/tests/SchemaCodecSmokeTest.cpp` (new; wire into the node test target).

**Interfaces:**
- Produces: a `rune_codec` STATIC CMake target with `PUBLIC` include `${RUNE_CODEC_ROOT}/include`; broker targets that use schemas link it.

- [ ] **Step 1: Make sibling include paths overridable (`CMakeLists.txt`)**

Change line 7 from a plain `set(...)` to:
```cmake
set(RUNE_PROTOCOL_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/../rune-protocol/include"
    CACHE PATH "rune-protocol include dir")
```

- [ ] **Step 2: Add the `rune_codec` sibling static lib** (mirror the `rune_fr` block at `CMakeLists.txt:37-52`), after the `rune_fr` block:
```cmake
# ── rune-codec (0a) — consumed as a SIBLING source static lib (like rune-fr) ──
set(RUNE_CODEC_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../rune-codec"
    CACHE PATH "Path to the rune-codec sibling repo")
add_library(rune_codec STATIC
    ${RUNE_CODEC_ROOT}/src/Serialize.cpp
    ${RUNE_CODEC_ROOT}/src/Record.cpp
    ${RUNE_CODEC_ROOT}/src/Validator.cpp
    ${RUNE_CODEC_ROOT}/src/Generator.cpp)
target_include_directories(rune_codec PUBLIC ${RUNE_CODEC_ROOT}/include)
target_compile_features(rune_codec PUBLIC cxx_std_23)
```
Link it into the node library (find the `add_library(rune_node ...)`/`target_link_libraries(rune_node ...)` block and add `rune_codec`).

- [ ] **Step 3: Write the failing smoke test (`libs/node/tests/SchemaCodecSmokeTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include <rune/codec/Descriptor.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

TEST(SchemaCodecSmoke, FingerprintAndRoundTrip) {
    using namespace rune::codec;
    Descriptor d = DescriptorBuilder("characters", 1)
        .add("id", PrimType::Int64)
        .add("userId", PrimType::Int64, 1, /*isKey=*/true)
        .add("name", PrimType::String)
        .build();
    const uint64_t fp = fingerprint(d);
    EXPECT_NE(fp, 0u);
    auto blob = serialize(d);
    Descriptor back = deserialize(blob);
    EXPECT_EQ(fingerprint(back), fp);
}
```
Register it with the node test target (`gtest_discover_tests`), linking `rune_codec`.

- [ ] **Step 4: Configure + build + run**, verify it fails first if `rune_codec` is not yet linked, then passes after linking. Use the repo's Conan+CMake flow; on macos arm64 pass `-DRUNE_CODEC_ROOT=/Users/vradu/Projects/rune-codec`.

- [ ] **Step 5: Commit**
```bash
git add CMakeLists.txt libs/node/tests/SchemaCodecSmokeTest.cpp
git commit -m "build(broker): consume rune-codec as sibling static lib; overridable include paths"
```

---

## Task 3: SchemaRegistry + TopicSchema record (rune-broker, pure)

**Files:**
- Create: `libs/node/include/rune/node/SchemaRegistry.h`, `libs/node/src/SchemaRegistry.cpp`
- Test: `libs/node/tests/SchemaRegistryTest.cpp`

**Interfaces:**
- Consumes: rune-codec `Descriptor`, `deserialize`, `fingerprint`, `Validator`.
- Produces:
```cpp
namespace rune::node {
inline constexpr int32_t  kSchemaTopicId       = -4;
inline constexpr uint32_t RUNTIME_TOPIC_ID_BASE = 0x40000000u;

struct TopicSchema {                 // the __topic_schemas value
    int32_t  topicId = -1;
    uint16_t version = 1;
    std::string keyField;
    bool     validate = false;
    std::vector<uint8_t> descriptorBlob;
    std::vector<uint8_t> serialize() const;                 // LE record
    static std::optional<TopicSchema> deserialize(std::span<const uint8_t>);
};

struct RegistryEntry {
    int32_t   topicId = -1;
    rune::codec::Descriptor descriptor;
    std::vector<uint8_t> descriptorBlob;              // original blob (for GetTopicSchema, no re-serialize)
    uint64_t  fingerprint = 0;
    std::optional<rune::codec::Validator> validator;  // present iff validate
    std::string keyField;
    uint16_t  version = 1;
    int32_t   partitionCount = 1;
};

class SchemaRegistry {               // single-threaded; AdminWorker-owned
public:
    void applyRecord(const TopicSchema&);                   // leader-commit + replica-apply
    const RegistryEntry* byId(int32_t topicId) const;
    const RegistryEntry* byName(std::string_view) const;
    int32_t allocateRuntimeId();                            // returns hw, then hw++
    void rebuildFromRecords(std::span<const TopicSchema>);  // startup replay
    uint32_t runtimeIdHighWater() const { return runtimeIdHighWater_; }
private:
    std::unordered_map<int32_t, RegistryEntry> byId_;
    std::unordered_map<std::string, int32_t>   nameToId_;
    uint32_t runtimeIdHighWater_ = RUNTIME_TOPIC_ID_BASE;
};
} // namespace rune::node
```

- [ ] **Step 1: Write the failing test (`SchemaRegistryTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include "rune/node/SchemaRegistry.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::node;
using namespace rune::codec;

static TopicSchema makeSchema(int32_t id, const char* name, bool validate) {
    Descriptor d = DescriptorBuilder(name, 1)
        .add("id", PrimType::Int64)
        .add("userId", PrimType::Int64, 1, true)
        .add("strength", PrimType::Int32)
        .build();
    TopicSchema ts; ts.topicId = id; ts.version = 1; ts.keyField = "userId";
    ts.validate = validate; ts.descriptorBlob = serialize(d); return ts;
}

TEST(SchemaRegistry, TopicSchemaRecordRoundTrip) {
    TopicSchema ts = makeSchema(0x40000000, "characters", true);
    auto bytes = ts.serialize();
    auto back = TopicSchema::deserialize(bytes);
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->topicId, 0x40000000);
    EXPECT_EQ(back->keyField, "userId");
    EXPECT_TRUE(back->validate);
    EXPECT_EQ(back->descriptorBlob, ts.descriptorBlob);
}

TEST(SchemaRegistry, ApplyBuildsValidatorIffValidate) {
    SchemaRegistry reg;
    reg.applyRecord(makeSchema(0x40000000, "chars", true));
    reg.applyRecord(makeSchema(0x40000001, "pos", false));
    const auto* a = reg.byName("chars");
    ASSERT_NE(a, nullptr);
    EXPECT_TRUE(a->validator.has_value());
    EXPECT_EQ(a->fingerprint, fingerprint(a->descriptor));
    const auto* b = reg.byName("pos");
    ASSERT_NE(b, nullptr);
    EXPECT_FALSE(b->validator.has_value());
    EXPECT_EQ(reg.byId(0x40000000), a);
}

TEST(SchemaRegistry, RuntimeIdHighWaterAdvancesAndRecovers) {
    SchemaRegistry reg;
    EXPECT_EQ(reg.allocateRuntimeId(), static_cast<int32_t>(RUNTIME_TOPIC_ID_BASE));
    EXPECT_EQ(reg.allocateRuntimeId(), static_cast<int32_t>(RUNTIME_TOPIC_ID_BASE) + 1);
    // Rebuild from records with a higher id → high-water jumps past it.
    std::vector<TopicSchema> recs{ makeSchema(0x40000005, "z", false) };
    SchemaRegistry reg2; reg2.rebuildFromRecords(recs);
    EXPECT_EQ(reg2.allocateRuntimeId(), 0x40000006);
}
```

- [ ] **Step 2: Run it, verify it fails** (missing header). Build the node test target; expect compile failure.

- [ ] **Step 3: Implement `SchemaRegistry.h` / `.cpp`.** `TopicSchema::serialize` writes LE: `topicId:i32, version:u16, partitionCount:i32, validate:u8, keyFieldLen:u16 + bytes, descriptorLen:u32 + bytes`; `deserialize` bounds-checks each read and returns `nullopt` on truncation. `applyRecord`: build `RegistryEntry e; e.topicId = ts.topicId; e.descriptorBlob = ts.descriptorBlob; e.descriptor = rune::codec::deserialize(ts.descriptorBlob); e.fingerprint = rune::codec::fingerprint(e.descriptor); e.validator = ts.validate ? std::optional<Validator>(std::in_place, e.descriptor) : std::nullopt; e.keyField = ts.keyField; e.version = ts.version; e.partitionCount = ts.partitionCount;` then `byId_[ts.topicId] = std::move(e); nameToId_[name-from-descriptor-or-record] = ts.topicId;`. The topic **name** for `nameToId_` is `e.descriptor.name` (the schema name). If `topicId >= RUNTIME_TOPIC_ID_BASE` then `runtimeIdHighWater_ = std::max(runtimeIdHighWater_, uint32_t(topicId) + 1)`. `allocateRuntimeId` returns `int32_t(runtimeIdHighWater_++)`. `rebuildFromRecords` clears then `applyRecord`s each.

  > NOTE: `TopicSchema` carries `int32_t partitionCount` (add it to the struct + round-trip test, set it in `makeSchema`). `byName`/`byId` return `const RegistryEntry*`; the entry's `topicId` field lets callers (AdminWorker) read the id on a name hit.

- [ ] **Step 4: Run the tests, verify all pass.**

- [ ] **Step 5: Commit**
```bash
git add libs/node/include/rune/node/SchemaRegistry.h libs/node/src/SchemaRegistry.cpp libs/node/tests/SchemaRegistryTest.cpp
git commit -m "feat(broker): SchemaRegistry + TopicSchema record (AdminWorker-owned)"
```

---

## Task 4: AdminWorker op state machine (rune-broker, dependency-injected)

**Files:**
- Create: `libs/node/include/rune/node/AdminWorker.h`, `libs/node/src/AdminWorker.cpp`
- Test: `libs/node/tests/AdminWorkerTest.cpp`

**Interfaces:**
- Consumes: `SchemaRegistry`, `TopicSchema`, rune-codec `deserialize`/`fingerprint`.
- Produces: a testable state machine with **injected** effects (so it needs no real broker):
```cpp
namespace rune::node {
struct AdminDeps {                 // injected effects (real impls wired in Task 6)
    std::function<int32_t(int32_t topicId, int32_t partitionCount,
                          const std::string& name)> createTopic;   // CREATING (blocking I/O)
    std::function<bool(const TopicSchema&)> appendDurable;         // AWAITING_COMMIT; true=committed
    std::function<void(const RegistryEntry&, int32_t topicId)> pushValidators; // PUBLISHING
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, uint16_t errorCode)> respondDefine;
    std::function<void(int64_t session, int64_t corr, int32_t topicId, uint16_t version,
                       const std::vector<uint8_t>& blob, uint16_t errorCode)> respondGet;
};

struct DefineTopicArgs { int64_t session, corr; std::string name, keyField;
                         int32_t partitionCount; bool validate; std::vector<uint8_t> blob; };
struct GetSchemaArgs   { int64_t session, corr; std::string name; };

class AdminWorker {
public:
    AdminWorker(SchemaRegistry& reg, AdminDeps deps);
    void onDefineTopic(const DefineTopicArgs&);   // runs the state machine
    void onGetTopicSchema(const GetSchemaArgs&);
};
} // namespace rune::node
```

- [ ] **Step 1: Write the failing test (`AdminWorkerTest.cpp`)** — drive ops through fakes:
```cpp
#include <gtest/gtest.h>
#include "rune/node/AdminWorker.h"
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
using namespace rune::node; using namespace rune::codec;

namespace {
std::vector<uint8_t> charsBlob() {
    return serialize(DescriptorBuilder("characters",1)
        .add("id",PrimType::Int64).add("userId",PrimType::Int64,1,true)
        .add("name",PrimType::String).build());
}
struct Recorder {
    int creates=0, appends=0, pushes=0;
    int32_t lastTopicId=-1; uint16_t lastErr=0xffff;
    AdminDeps make(bool commit=true) {
        AdminDeps d;
        d.createTopic=[this](int32_t id,int32_t,const std::string&){ ++creates; return id; };
        d.appendDurable=[this,commit](const TopicSchema&){ ++appends; return commit; };
        d.pushValidators=[this](const RegistryEntry&,int32_t){ ++pushes; };
        d.respondDefine=[this](int64_t,int64_t,int32_t tid,uint16_t,uint16_t e){ lastTopicId=tid; lastErr=e; };
        d.respondGet=[this](int64_t,int64_t,int32_t,uint16_t,const std::vector<uint8_t>&,uint16_t e){ lastErr=e; };
        return d;
    }
};
}

TEST(AdminWorker, DefineTopicHappyPath) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"characters","userId",16,true,charsBlob()});
    EXPECT_EQ(r.creates,1);
    EXPECT_EQ(r.appends,1);
    EXPECT_EQ(r.pushes,1);                       // validate=true → validators pushed
    EXPECT_EQ(r.lastErr,0);                      // OK
    EXPECT_GE(r.lastTopicId, static_cast<int32_t>(RUNTIME_TOPIC_ID_BASE));
    ASSERT_NE(reg.byName("characters"), nullptr); // registry updated on commit
}

TEST(AdminWorker, RejectsMalformedDescriptor) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"bad","",1,false,{0,1,2}});   // not a valid blob
    EXPECT_EQ(r.creates,0);
    EXPECT_EQ(r.appends,0);
    EXPECT_EQ(r.lastErr,23);                            // InvalidSchema
}

TEST(AdminWorker, DuplicateNameReturnsTopicExists) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"characters","userId",16,false,charsBlob()});
    r.lastErr=0xffff;
    w.onDefineTopic({1,3,"characters","userId",16,false,charsBlob()});
    EXPECT_EQ(r.lastErr,25);                            // TopicExists
    EXPECT_EQ(r.creates,1);                             // no second create
}

TEST(AdminWorker, NoValidatorPushWhenValidateOff) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onDefineTopic({1,2,"characters","userId",16,false,charsBlob()});
    EXPECT_EQ(r.pushes,0);
    EXPECT_EQ(r.lastErr,0);
}

TEST(AdminWorker, GetSchemaUnknownTopic) {
    SchemaRegistry reg; Recorder r; AdminWorker w(reg, r.make());
    w.onGetTopicSchema({1,2,"nope"});
    EXPECT_EQ(r.lastErr,7);                             // UnknownTopic
}
```

- [ ] **Step 2: Run it, verify it fails** (missing header).

- [ ] **Step 3: Implement the state machine (`AdminWorker.cpp`).** `onDefineTopic`: (a) PARSING — `try { d = deserialize(blob); } catch (...) { respondDefine(session,corr,-1,0,InvalidSchema=23); return; }`; (b) name lookup — `if (const auto* ex = reg.byName(name)) { respondDefine(session,corr, ex->topicId, ex->version, TopicExists=25); return; }`; (c) CREATING — `int32_t id = reg.allocateRuntimeId(); deps.createTopic(id, partitionCount, name);` (blocking I/O in the real impl); (d) AWAITING_COMMIT — build `TopicSchema{ .topicId=id, .version=1, .keyField=keyField, .validate=validate, .descriptorBlob=blob, .partitionCount=partitionCount }`; `if (!deps.appendDurable(ts)) { respondDefine(session,corr,-1,0,Internal=6); return; }`; (e) on commit — `reg.applyRecord(ts); if (validate) deps.pushValidators(*reg.byId(id), id); respondDefine(session,corr,id,1,OK=0);`. `onGetTopicSchema`: `const auto* e = reg.byName(name); if (!e) { respondGet(session,corr,-1,0,{},UnknownTopic=7); return; } respondGet(session,corr, e->topicId, e->version, e->descriptorBlob, OK=0);`.

  Encode error codes via `static_cast<uint16_t>(rune::protocol::client::ErrorCode::...)` — but AdminWorker is in `rune::node`; pass integer codes to keep it protocol-agnostic (the values 0/6/7/23/24/25 are fixed by Task 1). Document the mapping in a comment.

- [ ] **Step 4: Run the tests, verify all pass.**

- [ ] **Step 5: Commit**
```bash
git add libs/node/include/rune/node/AdminWorker.h libs/node/src/AdminWorker.cpp libs/node/tests/AdminWorkerTest.cpp
git commit -m "feat(broker): AdminWorker DDL state machine (dependency-injected, unit-tested)"
```

---

## Task 5: `TopicSpec` fields + runtime `TopicRegistry::createTopic`

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (`TopicSpec`: reuse the existing `partitionKey` field as the key field; add only `bool validate = false`). The AdminWorker's `keyField` comes from the `DefineTopic` message, not `TopicSpec`, so no new key field is needed on `TopicSpec`.
- Modify: `libs/gateway/include/rune/gateway/TopicRegistry.h`, `libs/gateway/src/TopicRegistry.cpp` (add `createTopic`).
- Test: `libs/gateway/tests/TopicRegistryRuntimeTest.cpp`

**Interfaces:**
- Consumes: existing `TopicRegistry` ctor (from `std::vector<TopicSpec>` + `dataDir`), `Log::recover()`, the balancer/placement calls made at construction.
- Produces: `int32_t TopicRegistry::createTopic(const TopicSpec& spec);` — creates the partition dirs + logs, runs `Log::recover()`, registers with placement, returns the topicId. Idempotent guard: if a topic with that id/name already exists, return it without recreating.

- [ ] **Step 1: Read the current construction path.** In `TopicRegistry.cpp`, locate the constructor loop that, per `TopicSpec`, creates directories and calls `Log::recover()` and registers partitions. `createTopic` factors that per-topic body into a callable method.

- [ ] **Step 2: Write the failing test (`TopicRegistryRuntimeTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include <filesystem>
#include "rune/gateway/TopicRegistry.h"
#include "rune/node/NodeConfig.h"
namespace fs = std::filesystem;
using namespace rune;

TEST(TopicRegistryRuntime, CreateTopicMakesProducibleTopic) {
    fs::path dir = fs::temp_directory_path() / "rune_tr_runtime_test";
    fs::remove_all(dir); fs::create_directories(dir);
    node::TopicSpec base; // no env topics
    gateway::TopicRegistry reg(std::vector<node::TopicSpec>{}, dir.string());

    node::TopicSpec spec;
    spec.name = "characters";
    spec.topicId = 0x40000000;
    spec.partitionCount = 2;
    const int32_t id = reg.createTopic(spec);
    EXPECT_EQ(id, 0x40000000);
    // The partition log directories now exist and are recoverable.
    EXPECT_TRUE(fs::exists(dir / "characters-0"));  // adjust to the repo's dir naming
    // Idempotent: creating again returns the same id, no throw.
    EXPECT_EQ(reg.createTopic(spec), 0x40000000);
}
```
Adjust the partition-directory assertion to the repo's actual naming (read `TopicRegistry.cpp` to confirm, e.g. `<name>-<partition>` or `<topicId>/<partition>`).

- [ ] **Step 3: Run it, verify it fails** (no `createTopic`).

- [ ] **Step 4: Implement `createTopic`** by extracting the constructor's per-topic body into `createTopic(const TopicSpec&)`, having the constructor call it in its loop (DRY). Guard re-creation by checking the existing partition map first.

- [ ] **Step 5: Run the tests, verify pass.** Also re-run the existing `TopicRegistry` tests to confirm the refactor didn't regress boot-time creation.

- [ ] **Step 6: Commit**
```bash
git add libs/node/include/rune/node/NodeConfig.h libs/gateway/include/rune/gateway/TopicRegistry.h \
        libs/gateway/src/TopicRegistry.cpp libs/gateway/tests/TopicRegistryRuntimeTest.cpp
git commit -m "feat(broker): runtime TopicRegistry::createTopic + TopicSpec validate/keyField"
```

---

## Task 6: `__topic_schemas` persistence + spawn AdminWorker (rune-broker wiring)

**Files:**
- Modify: `libs/node/src/BrokerNode.cpp` (replicate the `__transaction_state` block; spawn AdminWorker + ring; wire real `AdminDeps`).
- Modify: `libs/node/include/rune/node/BrokerNode.h` (members: `schemaLog_`, `schemaReplica_`, `schemaRegistry_`, `adminWorker_`, admin ring).
- Test: `libs/node/tests/integration/SchemaPersistenceTest.cpp`

**Interfaces:**
- Consumes: the `__transaction_state` wiring at `BrokerNode.cpp:~975-1069` (Log under `dataDir`, `PartitionConfig.topicId` sentinel, `cleanupPolicy=Compact`, `PartitionReplica` at `Role::Follower`, `replicaRegistry_` + `replTransport_->registerReplica(...)`, `rebuildFromLog()`); `ReplicatedTxnStateStore::rebuildFromLog`; `SchemaRegistry`, `AdminWorker`, `AdminDeps`; `TopicRegistry::createTopic`.
- Produces: a running per-node AdminWorker whose `AdminDeps` are bound to real effects: `createTopic` → `TopicRegistry::createTopic`; `appendDurable` → append `TopicSchema::serialize()` to `__topic_schemas` and await majority-durable commit; `pushValidators` → enqueue `SchemaUpdate` on each worker ring (Task 7 consumes); `respondDefine/respondGet` → enqueue the response frame on the gateway egress path.

- [ ] **Step 1: Replicate the `__topic_schemas` log/replica block.** Copy the `__transaction_state` block (`BrokerNode.cpp:~975-1069`) as a new block, substituting: directory `dataDir/"__topic_schemas"`; `tcfg.topicId = rune::node::kSchemaTopicId` (`-4`); `tcfg.cleanupPolicy = CleanupPolicy::Compact`; register replica at `(-4, 0)` in `replicaRegistry_` and `replTransport_->registerReplica(-4,0,...)`. Build a `SchemaRegistry schemaRegistry_` member and `rebuildFromLog`-equivalent that decodes each compacted `__topic_schemas` record via `TopicSchema::deserialize` and calls `schemaRegistry_.applyRecord(...)` (mirror `ReplicatedTxnStateStore::rebuildFromLog`). Ensure the replica-apply path (follower `applyReplicated`) also calls `schemaRegistry_.applyRecord` so followers converge.

- [ ] **Step 2: Spawn the AdminWorker + its ingress ring.** Create a dedicated ring (reuse the project's ring type used for worker ops; SPSC if the gateway is single-threaded for control, MPSC if multiple ingress threads — confirm against the ingress topology) and a dedicated thread running the AdminWorker poll loop (drain ring → dispatch `onDefineTopic`/`onGetTopicSchema`). This thread is separate from the gateway and produce workers. Bind `AdminDeps` as in Interfaces above.

- [ ] **Step 3: Write the failing integration test (`SchemaPersistenceTest.cpp`)** — a single-node broker: enqueue a `DefineTopicArgs` directly onto the admin ring (or call `adminWorker_.onDefineTopic` on its thread via a test hook), assert the `__topic_schemas` log has one record and `schemaRegistry_.byName("characters")` is present; then simulate restart (rebuild a fresh `SchemaRegistry` from the log) and assert the entry and the runtime-id high-water survive.

- [ ] **Step 4: Run it, verify it fails then passes** after wiring.

- [ ] **Step 5: Replication assertion.** Extend (or add `SchemaReplicationTest`) to bring up a follower and assert its `SchemaRegistry` rebuilds identically from `__topic_schemas` (mirror the existing txn-state replication test).

- [ ] **Step 6: Commit**
```bash
git add libs/node/src/BrokerNode.cpp libs/node/include/rune/node/BrokerNode.h libs/node/tests/integration/
git commit -m "feat(broker): __topic_schemas persistence + AdminWorker thread wiring"
```

---

## Task 7: Per-worker validation hook + `SchemaUpdate` (rune-broker)

**Files:**
- Modify: `libs/node/include/rune/node/Worker.h` (`executeProduceWire_` hook + local `topicId→Validator` map + `SchemaUpdate` op handling).
- Test: `libs/node/tests/integration/ProduceValidationTest.cpp`; benchmark `libs/node/benchmark/ProduceValidateBench.cpp`.

**Interfaces:**
- Consumes: `executeProduceWire_` at `Worker.h:~1134-1238`, the `appendNoSyncRestamp` call at `:~1176`; the `WorkerOp` framing on the worker ingress ring; rune-codec `Validator`, `Descriptor`, `deserialize`.
- Produces: worker-local `std::unordered_map<int32_t, rune::codec::Validator> validators_`; a new `WorkerOp` variant `SchemaUpdate{topicId, descriptorBlob}` the worker applies by building a local `Validator` from the blob; a produce rejection path returning `SchemaValidationFailed` (code 24) with no append.

- [ ] **Step 1: Add `SchemaUpdate` to the worker op set.** Reuse the existing `WorkerOp` encoding (find its definition); add a variant carrying `topicId` + descriptor bytes. On dequeue, the worker does `validators_.insert_or_assign(topicId, rune::codec::Validator(rune::codec::deserialize(blob)))` — built on the worker's own thread (shared-nothing).

- [ ] **Step 2: Write the failing integration test (`ProduceValidationTest.cpp`)** — define a topic with `validate=on` (drive via AdminWorker as in Task 6, which pushes the `SchemaUpdate` to the worker), then produce (a) a well-formed record for that descriptor → expect a durable append + `ProduceResponse OK`; (b) a malformed record (e.g. too short for the fixed block, or a bad enum) → expect `SchemaValidationFailed` (24) and NO append (log end offset unchanged).

- [ ] **Step 3: Run it, verify it fails** (no hook yet).

- [ ] **Step 4: Implement the hook.** In `executeProduceWire_`, immediately before the `appendNoSyncRestamp` call: `auto it = validators_.find(topicId); if (it != validators_.end()) { auto res = it->second.validate(std::span<const uint8_t>(recordPtr, recordLen)); if (!res.ok) { /* respond SchemaValidationFailed=24, skip append */ return; } }`. When no entry exists → single branch, no cost. Iterate records in the batch if the batch carries multiple. Use preallocated per-worker scratch only; no heap allocation on this path (HARD RULE 4).

- [ ] **Step 5: Run the tests, verify pass.**

- [ ] **Step 6: Benchmark (`ProduceValidateBench.cpp`)** — measure the produce path with validation off vs on for the `characters` descriptor; report ns/record delta (Google Benchmark, mirror an existing broker bench).

- [ ] **Step 7: Commit**
```bash
git add libs/node/include/rune/node/Worker.h libs/node/tests/integration/ProduceValidationTest.cpp libs/node/benchmark/ProduceValidateBench.cpp
git commit -m "feat(broker): opt-in per-worker validation hook + SchemaUpdate op + bench"
```

---

## Task 8: Gateway routing (DefineTopic/GetTopicSchema → AdminWorker)

**Files:**
- Modify: `libs/gateway/src/ClientGateway.cpp` (`onFragment` cases 64/66 → enqueue on admin ring; response egress).
- Test: `libs/gateway/tests/integration/SchemaGatewayTest.cpp`

**Interfaces:**
- Consumes: `onFragment` dispatch (`ClientGateway.cpp:570-638`), the `handleProduce` routing shape (`:1188`), the admin ring + `respondDefine/respondGet` egress from Task 6, the session lookup used by other handlers.
- Produces: `onFragment` decodes only enough of 64/66 to build `DefineTopicArgs`/`GetSchemaArgs` (copying the descriptor bytes into an owned buffer for the ring) and enqueues on the admin ring, then returns — no registry access, no blocking. Responses (`DefineTopicResponse`/`TopicSchemaResponse`) are encoded and sent on the client's egress by the admin-response callback.

- [ ] **Step 1: Add the dispatch cases.** In `onFragment`'s switch, add `case TemplateId::DefineTopic:` and `case TemplateId::GetTopicSchema:` that: validate the frame (`checkFrame` already ran), require a known session (mirror `handleProduce`'s session check), `DefineTopic::decode`/`GetTopicSchema::decode`, copy fields into the args struct (own the descriptor bytes), enqueue on the admin ring. Do NOT touch the registry here.

- [ ] **Step 2: Wire the response egress.** Implement the `respondDefine`/`respondGet` callbacks (bound in Task 6) to encode `DefineTopicResponse`/`TopicSchemaResponse` (setting header `sessionId`/`correlationId` from the args) and send on the originating session's egress publication, mirroring how `ProduceResponse` is sent.

- [ ] **Step 3: Write the failing integration test (`SchemaGatewayTest.cpp`)** — over the real wire against a single-node broker: send `DefineTopic{characters, 16, userId, validate=false}`, await `DefineTopicResponse` → assert `errorCode=OK` and `topicId >= 0x40000000`; send `GetTopicSchema{characters}` → assert `TopicSchemaResponse` returns a blob that `rune::codec::deserialize` accepts and whose `fingerprint` equals the client-built descriptor's; send `GetTopicSchema{missing}` → assert `UnknownTopic`.

- [ ] **Step 4: Gateway-non-blocking regression test (`*Regression*`).** With a deliberately stalled AdminWorker (inject an `appendDurable` that blocks on a latch for N ms via a test hook), assert that `Produce` requests continue to be routed/answered by the gateway within a tight bound while the DDL is parked — proving DDL never blocks the hot path. Name it `SchemaGatewayRegression.DdlDoesNotBlockProduce`.

- [ ] **Step 5: Run it, verify fails then passes.**

- [ ] **Step 6: Commit**
```bash
git add libs/gateway/src/ClientGateway.cpp libs/gateway/tests/integration/SchemaGatewayTest.cpp
git commit -m "feat(broker): gateway routes DDL to AdminWorker (enqueue-only, non-blocking)"
```

---

## Task 9: rune-client-cpp build integration

**Files:**
- Modify: `CMakeLists.txt` (mirror Task 2: `RUNE_PROTOCOL_INCLUDE`→CACHE, add `RUNE_CODEC_ROOT` + `rune_codec` static lib target, link into the client lib and tools).
- Test: `tests/SchemaCodecSmokeTest.cpp` (same body as Task 2 Step 3).

- [ ] **Step 1:** Make `RUNE_PROTOCOL_INCLUDE` (line ~7) a `CACHE PATH` as in Task 2 Step 1.
- [ ] **Step 2:** Add the identical `rune_codec` STATIC target block (Task 2 Step 2) and link it into the client library target and the tools.
- [ ] **Step 3:** Add the smoke test (copy Task 2 Step 3), register with a client test target.
- [ ] **Step 4:** Configure/build/run; verify pass (pass `-DRUNE_CODEC_ROOT=/Users/vradu/Projects/rune-codec` on macos arm64).
- [ ] **Step 5: Commit**
```bash
git add CMakeLists.txt tests/SchemaCodecSmokeTest.cpp
git commit -m "build(client): consume rune-codec as sibling static lib; overridable paths"
```

---

## Task 10: Client DDL round-trips (`defineTopic`, `getTopicSchema`)

**Files:**
- Modify: `include/rune/client/Client.h` (`pendingSchemaOps_` map + method decls), `src/Client.cpp` (impl + Pong-style response handling).
- Test: `tests/ClientSchemaTest.cpp` (against a single-node broker, or a loopback harness mirroring existing client tests).

**Interfaces:**
- Consumes: the `ping()` correlation→promise pattern (`Client.cpp:376-450`): `nextCorrId_.fetch_add(1)`, `pendingPings_` map under `mtx_`, `agent_.submitSend(...)`, the response handler that resolves the promise, the timeout sweep (`:619-626`). `SchemaMessages.h` codecs.
- Produces:
```cpp
struct DefineTopicResult { int32_t topicId; uint16_t version; uint16_t errorCode; };
struct TopicSchemaResult { int32_t topicId; uint16_t version; std::vector<uint8_t> descriptorBlob; uint16_t errorCode; };
std::future<DefineTopicResult> Client::defineTopic(std::string name, uint32_t partitions,
                                                   std::string keyField, bool validate,
                                                   std::span<const uint8_t> descriptorBlob);
std::future<TopicSchemaResult> Client::getTopicSchema(std::string name);
```

- [ ] **Step 1: Write the failing test (`ClientSchemaTest.cpp`)** — connect a client to a single-node broker; `auto f = client.defineTopic("characters",16,"userId",false, serialize(charsDescriptor())); auto r = f.get();` assert `errorCode==OK`, `topicId>=0x40000000`; then `auto s = client.getTopicSchema("characters").get();` assert its blob deserializes and its `fingerprint` matches; assert `getTopicSchema("missing")` yields `errorCode==UnknownTopic(7)`.

- [ ] **Step 2: Run it, verify it fails** (no methods).

- [ ] **Step 3: Implement** `defineTopic`/`getTopicSchema` mirroring `ping()`: allocate a correlationId, register a `std::promise` in `pendingSchemaOps_` (a map keyed by correlationId; value is a variant/struct holding the promise for either result type — use two maps `pendingDefine_`/`pendingGet_` for type simplicity), encode via `SchemaMessages.h`, `submitSend`. Add response handling in the agent's receive path: on `DefineTopicResponse`/`TopicSchemaResponse` decode, look up the correlationId, set the promise value, erase. Add these correlationIds to the timeout sweep so a lost response fails the promise (consistent with `pendingPings_`).

- [ ] **Step 4: Run the tests, verify pass.**

- [ ] **Step 5: Commit**
```bash
git add include/rune/client/Client.h src/Client.cpp tests/ClientSchemaTest.cpp
git commit -m "feat(client): defineTopic/getTopicSchema round-trips (ping-pattern)"
```

---

## Task 11: `openTopic<T>()` + `TypedTopic<T>`

**Files:**
- Create: `include/rune/client/TypedTopic.h`
- Modify: `include/rune/client/Client.h` (`openTopic<T>` template), `include/rune/client/metadata/MetadataCache.h` (`{schemaVersion, validate, keyField}`), `src/Client.cpp` if needed.
- Test: `tests/OpenTopicTest.cpp`

**Interfaces:**
- Consumes: `Client::getTopicSchema` (Task 10); rune-codec `deserialize`/`fingerprint`; the existing producer API (`Client::producer(...)` / the produce send path) keyed by `topicId`; the generated-struct contract from 0a: `T::kFingerprint` (`uint64_t`), `T::kSchemaName`, `T::encode(uint8_t*)->size_t`, `T::encodedSize()`.
- Produces:
```cpp
template <class T> class TypedTopic {              // in TypedTopic.h
public:
    TypedTopic(Producer producer, int32_t topicId); // Producer = the existing handle type
    void send(const T& rec);                         // T::encode into a reusable buffer, route by topicId
private:
    Producer producer_; int32_t topicId_;
    std::vector<uint8_t> buf_;                        // reused; grown to encodedSize(), never per-send freed
};

template <class T>
std::future<TypedTopic<T>> Client::openTopic(std::string name);  // Client.h
```

- [ ] **Step 1: Write the failing test (`OpenTopicTest.cpp`)** — two-part:
  - Unit (no broker): a fake `getTopicSchema` result whose blob is `serialize(charsDescriptor())`. Assert that when `T::kFingerprint == fingerprint(descriptor)` the open resolves, and when it differs the open fails with a clear error. Use a tiny stub `T` with a `static constexpr uint64_t kFingerprint` set to the matching / mismatching value. (Factor the compare into a free function `bool schemaMatches(uint64_t expected, std::span<const uint8_t> blob)` so it is unit-testable without a live client.)
  - Skip the live-broker leg here; the e2e (Task 13) covers it.
```cpp
#include <gtest/gtest.h>
#include "rune/client/TypedTopic.h"     // exposes rune::client::schemaMatches
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
#include <rune/codec/Fingerprint.h>
using namespace rune::codec;
TEST(OpenTopic, FingerprintGate) {
    Descriptor d = DescriptorBuilder("characters",1)
        .add("id",PrimType::Int64).add("userId",PrimType::Int64,1,true)
        .add("name",PrimType::String).build();
    auto blob = serialize(d);
    EXPECT_TRUE(rune::client::schemaMatches(fingerprint(d), blob));
    EXPECT_FALSE(rune::client::schemaMatches(fingerprint(d) ^ 0x1, blob));
}
```

- [ ] **Step 2: Run it, verify it fails** (no header/function).

- [ ] **Step 3: Implement `schemaMatches` + `TypedTopic<T>` + `openTopic<T>`.** `schemaMatches(expected, blob)` = `try { return fingerprint(deserialize(blob)) == expected; } catch (...) { return false; }`. `openTopic<T>(name)`: chain on `getTopicSchema(name)` — on `errorCode!=OK` fail; else `if (!schemaMatches(T::kFingerprint, blob)) fail("generated type is stale — rerun rune schema generate");` else populate `MetadataCache` `{schemaVersion, validate, keyField}` and resolve with `TypedTopic<T>(producer(topicId), topicId)`. `TypedTopic<T>::send`: `buf_.resize(std::max(buf_.size(), rec.encodedSize())); size_t n = rec.encode(buf_.data());` then route those `n` bytes through the existing produce path for `topicId_` (no per-send allocation — `buf_` capacity persists).

- [ ] **Step 4: Run the tests, verify pass.**

- [ ] **Step 5: Commit**
```bash
git add include/rune/client/TypedTopic.h include/rune/client/Client.h \
        include/rune/client/metadata/MetadataCache.h src/Client.cpp tests/OpenTopicTest.cpp
git commit -m "feat(client): openTopic<T> + TypedTopic<T> with exact-fingerprint gate"
```

---

## Task 12: `rune_schema_gen` tool

**Files:**
- Create: `tools/rune_schema_gen.cpp` + a CMake `add_executable(rune_schema_gen ...)` linking the client lib + `rune_codec`.
- Test: `tests/SchemaGenToolTest.cpp` (drives `--from-file`).

**Interfaces:**
- Consumes: `rune::codec::deserialize`, `generateHeader`; `Client::getTopicSchema` (for the live path); std filesystem.
- Produces: CLI `rune_schema_gen generate <topic> --out <dir> [--broker <endpoint>]` and `rune_schema_gen generate --from-file <blob> --out <dir>`. Writes `<dir>/<topic>.gen.h` (topic name derived from the descriptor's schema name for `--from-file`).

- [ ] **Step 1: Write the failing test (`SchemaGenToolTest.cpp`)** — build a descriptor, `serialize` it to a temp `.blob` file, invoke the tool's `generateFromFile(blobPath, outDir)` entry (factor the file-mode logic into a testable function, not only `main`), assert `<outDir>/characters.gen.h` exists and contains `struct Characters` and `kFingerprint`.
```cpp
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include "rune/tools/SchemaGen.h"       // exposes generateFromFile(blobPath, outDir)->genPath
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>
namespace fs = std::filesystem;
TEST(SchemaGenTool, FromFileEmitsHeader) {
    using namespace rune::codec;
    auto blob = serialize(DescriptorBuilder("characters",1)
        .add("id",PrimType::Int64).add("name",PrimType::String).build());
    fs::path dir = fs::temp_directory_path() / "rune_gen_tool_test";
    fs::remove_all(dir); fs::create_directories(dir);
    fs::path blobPath = dir / "characters.blob";
    { std::ofstream o(blobPath, std::ios::binary); o.write((const char*)blob.data(), blob.size()); }
    auto genPath = rune::tools::generateFromFile(blobPath.string(), dir.string());
    ASSERT_TRUE(fs::exists(genPath));
    std::ifstream in(genPath); std::string s((std::istreambuf_iterator<char>(in)), {});
    EXPECT_NE(s.find("struct Characters"), std::string::npos);
    EXPECT_NE(s.find("kFingerprint"), std::string::npos);
}
```

- [ ] **Step 2: Run it, verify it fails** (no `SchemaGen.h`).

- [ ] **Step 3: Implement** `rune::tools::generateFromFile(blobPath, outDir)`: read blob bytes → `Descriptor d = deserialize(bytes)` → `std::string hdr = generateHeader(d)` → write `outDir/(d.name + ".gen.h")`; return the path. Implement the live `generate <topic>` path in `main` by connecting a `Client`, `getTopicSchema(topic).get()`, then the same codegen. `main` parses args and dispatches file-mode vs live-mode.

- [ ] **Step 4: Run the test, verify pass.**

- [ ] **Step 5: Commit**
```bash
git add tools/rune_schema_gen.cpp include/rune/tools/SchemaGen.h tests/SchemaGenToolTest.cpp CMakeLists.txt
git commit -m "feat(client): rune_schema_gen tool (--from-file + live generate)"
```

---

## Task 13: Headline end-to-end typed round-trip

**Files:**
- Create: `tests/generated/characters.blob` (checked-in descriptor blob).
- Create: `tests/E2ETypedTopicTest.cpp` + a CMake `add_custom_command` that runs `rune_schema_gen generate --from-file tests/generated/characters.blob --out ${CMAKE_CURRENT_BINARY_DIR}/gen` producing `characters.gen.h`, added to the test target's include dir and `add_custom_target` dependency (mirror rune-codec's own build-time-gen round-trip test).

**Interfaces:**
- Consumes: `rune_schema_gen` (Task 12), the generated `Characters` struct (0a codegen contract), `Client::defineTopic`/`openTopic<T>` (Tasks 10–11), the existing consume/subscribe path.

- [ ] **Step 1: Produce the checked-in blob.** Add a tiny build step or a committed helper that writes `serialize(DescriptorBuilder("characters",1).add("id",Int64).add("userId",Int64,1,true).add("strength",Int32).add("name",String).build())` to `tests/generated/characters.blob`. (Generate it once and commit the bytes; document how in a comment.)

- [ ] **Step 2: Wire the build-time generation.** Add the `add_custom_command`/`add_custom_target` so `characters.gen.h` is generated into `${CMAKE_CURRENT_BINARY_DIR}/gen` before the E2E test compiles; add that dir to the test target includes. (Copy the pattern from rune-codec's `generate_characters_header` custom command.)

- [ ] **Step 3: Write the failing e2e test (`E2ETypedTopicTest.cpp`)**
```cpp
#include <gtest/gtest.h>
#include <fstream>
#include "characters.gen.h"          // build-time generated from characters.blob
#include "rune/client/Client.h"
// ... test harness includes to bring up a single-node broker ...

TEST(E2ETypedTopic, DefineOpenSendConsumeRoundTrip) {
    // 1. bring up a single-node broker (reuse the existing integration harness)
    // 2. connect a client
    // 3. read tests/generated/characters.blob and defineTopic("characters", 4, "userId", true, blob)
    // 4. auto topic = client.openTopic<rune::gen::Characters>("characters").get();
    // 5. rune::gen::Characters c{...}; topic.send(c);
    // 6. subscribe/consume the topic; decode the record via rune::gen::Characters::decode
    // 7. EXPECT the decoded fields equal what was sent (id/userId/strength/name)
}
```
Fill in the harness calls from the existing broker integration tests (single-node bring-up, connect, subscribe/consume). Assert the full field round-trip and that `openTopic` succeeded (fingerprint matched the registered schema).

- [ ] **Step 4: Run it, verify it fails then passes.**

- [ ] **Step 5: Commit**
```bash
git add tests/generated/characters.blob tests/E2ETypedTopicTest.cpp CMakeLists.txt
git commit -m "test(client): headline e2e typed round-trip (define→open→send→consume)"
```

---

## Notes on cross-repo ordering & execution

- **Repo order:** rune-protocol (Task 1) → rune-broker (Tasks 2–8) → rune-client-cpp (Tasks 9–13). Broker tasks depend on the protocol header; client tasks depend on the protocol header and a running broker for integration legs. Each repo is its own `feature/0b-schema-registry` worktree off `main`.
- **rune-codec is read-only.** If any task appears to need a rune-codec change, STOP and ledger a ruling — 0a is complete and frozen for 0b.
- **Regression suite:** name every regression test `*Regression*` (e.g. `SchemaGatewayRegression.DdlDoesNotBlockProduce`) and run the regression suite FIRST on every fix (HARD RULE 7).
- **No hot-path allocation** in Task 7's validation hook and Task 11's `TypedTopic::send` (reused buffers only).
