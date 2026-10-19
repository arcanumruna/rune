# Rune Phase 2b — Part C: Typed Topics Route by Key — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `TypedTopic<T>::send(rec)` routes each record exactly like `Producer::send(TopicHandle{id, partitionCount}, Key(rec.<keyField>), bytes)`: right partition, real record key. Today every typed record goes to partition 0 with an empty key.

**Architecture:** The broker appends the topic's `partitionCount` as a trailing field of the `GetTopicSchema` response. The client carries it through `Completion.reserved0` into `TopicSchemaResult.partitionCount`. The client's `TypedTopic` then stores `TopicHandle{topicId, partitionCount}` instead of `int32_t topicId`. rune-codec's generator emits a `key()` accessor that returns the `isKey` field in the form `rune::client::Key` consumes: scalars by value, `string(N)` as its array, and var-len `string`/`bytes` as a **byte span**, never `std::string_view`. `TypedTopic::send` sends `Key(rec.key())` when `T` has a `key()`. Otherwise it sends `Key{}` (partition 0, empty key), which is today's documented behaviour for schemas with no key field.

**Tech Stack:** C++23, GoogleTest, Google Benchmark, CMake (broker: Ninja `build-fast`; protocol/codec/client: their existing `build` dirs, Unix Makefiles), docker e2e on the k3s VM.

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` §C (binding). Parent: `docs/superpowers/specs/2026-10-08-rune-view-engine-single-source-design.md` (only for context; nothing in §C depends on it).

**Builds on:** `feature/materialized-views` (broker `69abb14`, protocol `d0578c4`, client `0e8505c`); codec on its integration line `feat/schema-evolution-durable-tags` (`b76d171`).

## Execution context

- **Checkouts (canonical sibling layout, the one the broker's `build-fast` is configured against):** `/Users/vradu/Projects/se-worktrees/{rune-broker,rune-protocol,rune-codec,rune-client-cpp}`. `build-fast/CMakeCache.txt` resolves `RUNE_PROTOCOL_INCLUDE=../rune-protocol/include` and `RUNE_CODEC_ROOT=../rune-codec`. The client's `build/` resolves the same siblings. Every path below is relative to the repo named in the task.
- **Branches.** Cut `feat/phase2b-c-typed-topic-routing` in all four repos before Task 1:
  ```bash
  cd /Users/vradu/Projects/se-worktrees
  git -C rune-protocol   checkout -b feat/phase2b-c-typed-topic-routing feature/materialized-views
  git -C rune-broker     checkout -b feat/phase2b-c-typed-topic-routing feature/materialized-views
  git -C rune-client-cpp checkout -b feat/phase2b-c-typed-topic-routing feature/materialized-views
  git -C rune-codec      checkout -b feat/phase2b-c-typed-topic-routing feat/schema-evolution-durable-tags
  ```
  (`feature/materialized-views` is checked out in the `rune-broker-matviews` worktree. Creating a *new* branch from it in `se-worktrees/rune-broker` is allowed.)
- **Build dirs.** protocol: `build` (Release, Makefiles). codec: `build` (Debug, Makefiles). client: `build` (Debug, Makefiles, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`). broker: `build-fast` (Debug, Ninja). All four are already configured. A changed `CMakeLists.txt` re-runs configure automatically on the next `cmake --build`.
- **Regression gate (CLAUDE.md #7).** Before **every** commit, run the full regression suite of every repo the commit touches **first**, then the task's other tests:
  - protocol: `ctest --test-dir build -R Regression --output-on-failure`
  - codec: `ctest --test-dir build -R Regression --output-on-failure`
  - client: `cmake --build build -j8 && ctest --test-dir build -R Regression --output-on-failure`
  - broker: `cmake --build build-fast && ctest --test-dir build-fast -R Regression --output-on-failure`
- **Build order:** protocol, then broker, then codec, then client, then e2e. The broker and client compile the *working tree* of `../rune-protocol` and `../rune-codec`, so each upstream task must be committed (or at least on disk) before the downstream task builds.
- Wire fields are append-only. Nothing is pushed. Every commit message ends with:
  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  ```

## Global Constraints

- CLAUDE.md hard rules apply: shared-nothing; ring buffers are the only cross-thread passing; state machines; **zero steady-state allocation** on `TypedTopic::send`; test and benchmark everything; a `*Regression*`-named test on every fix; the full `*Regression*` suite runs first on every change; fix-on-sight.
- Wire enums and fields are append-only. `TopicSchemaResponse` keeps `BODY_FIXED = 8`, `blockLength = 8`, templateId 67 and header version `kProtocolVersion` (3). `partitionCount` is **appended after the descriptor blob**, so pre-2b decoders, which stop after the blob, ignore it.
- A typed topic whose schema has **no key field** keeps today's behaviour: partition 0, empty key. This is documented in `TypedTopic.h` and `Generator.h`.
- `TypedTopic::send(rec)` routes exactly like `Producer::send(TopicHandle{id, partitionCount}, Key(rec.key()), bytes)`.
- The public API names published on arcanum.regressorspath.com are unchanged by this part. `Client::openTopicAndWait<T>`, `TypedTopic::fromSchema` and `TypedTopic::send` keep their signatures. Only the low-level `TypedTopic(ProducerT, …)` constructor changes its second parameter from `int32_t topicId` to `TopicHandle`. Part B relies on that.
- Cross-plan interface (consumed by Part B): `TopicSchemaResult.partitionCount`, generated `key()`, `TypedTopic` holding `TopicHandle{id, partitionCount}` (exposed as `handle()`), and `rune::client::HasRecordKey<T>`.

## Review Focus

These failure modes are the most likely to hurt a user. Each one is pinned by a named test in the task that owns it:

1. **A new client talking to a pre-2b broker** (the response has no trailing `partitionCount`). The typed topic must fall back to 1 partition: same partition 0 as before, with the key now set. It must never use partition count 0 or garbage. Pinned by `SchemaMessagesRegression.TopicSchemaResponseWithoutTrailingPartitionCountDecodesAsNotReported` (Task 1), `OpenTopicGateRegression.PreTwoBBrokerReportsZeroPartitionCount` (Task 4) and `OpenTopicGateRegression.PreTwoBBrokerTypedTopicFallsBackToOnePartition` (Task 5).
2. **A `string`/`bytes` key field** must be hashed by its *content*. If the generated accessor returned `std::string_view`, `Key`'s trivially-copyable template constructor would hash the view object (pointer + length), so identical keys would scatter across partitions. Pinned by `GeneratorRegression.EmitsByteSpanKeyForStringKeyNeverStringView` and `GeneratedKeyRegression.StringKeyIsAByteSpanOverTheContent` (Task 3), and by `TypedTopicRoutingRegression.StringKeyRoutesByContentBytesNotByViewObject` (Task 5).
3. **An old client reading the new 44-byte response** must decode exactly as before. Pinned by `SchemaConformanceRegression.TopicSchemaResponseV2IsV1GoldenPlusTrailingPartitionCount` (Task 1), which checks that the new frame is the old golden byte-for-byte plus 4 trailing bytes.
4. **A client bound to a *historical* schema version** (fingerprint-selected `GetTopicSchema`) must still get the topic's real partition count, not 0 or 1. Pinned by `AdminWorkerRegression.GetSchemaByFingerprintReportsPartitionCount` (Task 2).
5. **A key wider than `Key::kMaxInline` (16 bytes)** must fail closed. `Key` silently truncates, which would merge distinct record keys (compaction and views would then fold different entities together). A var-len key over 16 bytes throws `std::invalid_argument` and nothing is sent. A fixed key wider than 16 bytes is a compile error. Pinned by `TypedTopicRoutingRegression.VarKeyLongerThan16BytesIsRejectedNotTruncated` (Task 5).

The spec's own regression test is `TypedTopicRoutingRegression.EightPartitionTypedTopicRoutesLikeProducerSendWithSameKey` (Task 5). The real-broker proof is the `typed_route` leg of the view-engine e2e (Task 6).

---

## File Structure

| Repo | File | Responsibility | Task |
|---|---|---|---|
| protocol | `include/rune/protocol/client/SchemaMessages.h` | `TopicSchemaResponse.partitionCount` (trailing i32) | 1 |
| protocol | `tests/protocol/client/SchemaMessagesTest.cpp`, `tests/protocol/client/SchemaConformanceTest.cpp` | round-trip, truncation, V1/V2 goldens | 1 |
| broker | `libs/node/include/rune/node/AdminWorker.h` | `AdminDeps::respondGet` gains `int32_t partitionCount` | 2 |
| broker | `libs/node/src/AdminWorker.cpp` | `submitGetSchema` reports the topic's partition count | 2 |
| broker | `libs/node/src/BrokerNode.cpp` | encode `partitionCount`; encode in place in the egress ring slot (fix-on-sight: no per-response `std::vector`) | 2 |
| broker | `libs/node/tests/node/AdminWorkerTest.cpp`, `libs/node/tests/integration/SchemaGatewayTest.cpp` + 3 harness files | regressions + signature update | 2 |
| codec | `src/Generator.cpp`, `include/rune/codec/Generator.h` | emit `key()`; reject a field named `key` that would collide | 3 |
| codec | `tools/make_str_keyed_blob.cpp` (new), `tests/generated/str_keyed.blob` (new), `tests/GeneratedKeyTest.cpp` (new), `tests/GeneratorTest.cpp`, `CMakeLists.txt` | generator tests incl. a compiled string-keyed header | 3 |
| client | `include/rune/client/Completion.h`, `include/rune/client/Client.h`, `src/Client.cpp`, `tests/support/TestCompletions.h` | `partitionCount` → `Completion.reserved0` → `TopicSchemaResult.partitionCount` | 4 |
| client | `tests/client/OpenTopicGateTest.cpp` | gateway sends `partitionCount`; old-broker simulation; regressions | 4, 5 |
| client | `include/rune/client/TypedTopic.h` | `TopicHandle` + `Key(rec.key())` routing, `HasRecordKey`, `typedPartitionCount`, `handle()` | 5 |
| client | `tests/client/TypedTopicRoutingRegressionTest.cpp` (new), `tests/client/OpenTopicTest.cpp`, `CMakeLists.txt`, `bench/TypedTopicSendBench.cpp` (new) | routing regression on the real generated `Characters`, zero-alloc, bench | 5 |
| client | `examples/view_probe.cpp`, `CMakeLists.txt` | e2e `typed_route` leg (8 partitions, real broker) | 6 |
| broker | `docker/view-engine-e2e.sh` | assert the `TYPED-ROUTING: OK` sentinel | 6 |

---

### Task 1: Protocol — `TopicSchemaResponse` gains a trailing `partitionCount`

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-protocol`

**Files:**
- Modify: `include/rune/protocol/client/SchemaMessages.h:118-157` (the `TopicSchemaResponse` comment + struct)
- Test: `tests/protocol/client/SchemaMessagesTest.cpp` (append)
- Test: `tests/protocol/client/SchemaConformanceTest.cpp:229-307` (golden section)

**Interfaces:**
- Produces: `rune::protocol::client::TopicSchemaResponse::partitionCount` (`int32_t`, default `0` = "not reported"). It is encoded as LE i32 **after** `descriptorBlob`. `encodedSize()` grows by 4. `decode()` reads it only when 4 more bytes are available; otherwise it leaves `0`. `BODY_FIXED` stays 8.

- [ ] **Step 1: Write the failing tests.** Append to `tests/protocol/client/SchemaMessagesTest.cpp`:

```cpp
// Phase 2b Part C: the topic's partition count rides as a TRAILING i32 after
// the descriptor blob (append-only: BODY_FIXED/blockLength unchanged, so a
// pre-2b decoder — which stops after the blob — ignores it).
TEST(SchemaMessagesRegression, TopicSchemaResponseCarriesTrailingPartitionCount) {
    std::vector<uint8_t> blob = {9, 8, 7};
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 5; in.version = 3; in.errorCode = 0;
    in.descriptorBlob = blob.data(); in.descriptorLen = 3;
    in.partitionCount = 8;
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());
    ASSERT_EQ(n, in.encodedSize());
    ASSERT_EQ(n, MessageHeader::ENCODED_SIZE + TopicSchemaResponse::BODY_FIXED + 4u + 3u + 4u);
    EXPECT_EQ(TopicSchemaResponse::BODY_FIXED, 8u) << "fixed block must not grow (append-only)";
    auto out = TopicSchemaResponse::decode(buf.data(), n);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(out->topicId, 5);
    EXPECT_EQ(out->version, 3u);
    ASSERT_EQ(out->descriptorLen, 3u);
    EXPECT_EQ(0, std::memcmp(out->descriptorBlob, blob.data(), 3));
    EXPECT_EQ(out->partitionCount, 8);
}

// A pre-2b broker's frame has no trailing bytes: it must still decode, with
// partitionCount = 0 ("not reported") — never a reject, never garbage.  A
// frame cut inside the trailing field (1-3 bytes) is treated the same way.
TEST(SchemaMessagesRegression, TopicSchemaResponseWithoutTrailingPartitionCountDecodesAsNotReported) {
    std::vector<uint8_t> blob = {1, 2, 3, 4};
    TopicSchemaResponse in; in.sessionId = 1; in.correlationId = 2;
    in.topicId = 6; in.version = 1;
    in.descriptorBlob = blob.data(); in.descriptorLen = 4;
    in.partitionCount = 8;
    std::vector<uint8_t> buf(in.encodedSize());
    const std::size_t n = in.encode(buf.data());

    auto noTrailer = TopicSchemaResponse::decode(buf.data(), n - 4);
    ASSERT_TRUE(noTrailer.has_value());
    EXPECT_EQ(noTrailer->topicId, 6);
    ASSERT_EQ(noTrailer->descriptorLen, 4u);
    EXPECT_EQ(noTrailer->partitionCount, 0);

    auto cut = TopicSchemaResponse::decode(buf.data(), n - 2);
    ASSERT_TRUE(cut.has_value());
    EXPECT_EQ(cut->partitionCount, 0);
}
```

In `tests/protocol/client/SchemaConformanceTest.cpp`, replace everything from the line `// TopicSchemaResponse golden vector` (the banner comment block that starts at `// ===...` just above it, line 229) through the end of `TEST(SchemaConformance, TopicSchemaResponseEncodeGolden)` (line 307) with:

```cpp
// ===========================================================================
// TopicSchemaResponse golden vectors
//
// V1 = the pre-2b frame (no trailing partitionCount) — still what an old
// broker sends; must keep decoding (partitionCount = 0, "not reported").
// V2 = Phase 2b Part C: V1 byte-for-byte + a trailing partitionCount:i32.
//
// Fields:
//   topicId        = 42
//   version        = 1
//   errorCode      = 0 (OK)
//   descriptorBlob = {0xDE, 0xAD, 0xBE, 0xEF}  (4 bytes)
//   partitionCount = 8   (V2 only)
//
// Wire layout (V1 40 bytes, V2 44 bytes):
//   [0..1]   blockLength=8         : 08 00
//   [2..3]   templateId=67         : 43 00
//   [4..5]   schemaId=120          : 78 00
//   [6..7]   version=3             : 03 00
//   [8..15]  sessionId  (LE64)     : 88 77 66 55 44 33 22 11
//   [16..23] correlationId (LE64)  : 11 10 0F 0E 0D 0C 0B 0A
//   [24..27] topicId=42 (LE32)     : 2A 00 00 00
//   [28..29] version=1 (LE16)      : 01 00
//   [30..31] errorCode=0 (LE16)    : 00 00
//   [32..35] descriptorLen=4(LE32) : 04 00 00 00
//   [36..39] DE AD BE EF
//   [40..43] partitionCount=8 (LE32, V2 only) : 08 00 00 00
// ===========================================================================

static constexpr uint8_t kTopicSchemaResponseGolden[] = {
    // header
    0x08, 0x00,              // blockLength = 8
    0x43, 0x00,              // templateId  = 67
    0x78, 0x00,              // schemaId    = 120
    0x03, 0x00,              // version     = 3
    0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
    0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
    // body fixed (8 bytes)
    0x2A, 0x00, 0x00, 0x00,  // topicId = 42
    0x01, 0x00,              // version = 1
    0x00, 0x00,              // errorCode = 0
    // descriptorBlob varbytes32
    0x04, 0x00, 0x00, 0x00,  // len = 4
    0xDE, 0xAD, 0xBE, 0xEF,
};
static constexpr std::size_t kTopicSchemaResponseGoldenSize = sizeof(kTopicSchemaResponseGolden);  // 40

static constexpr uint8_t kTopicSchemaResponseGoldenV2[] = {
    // header
    0x08, 0x00,              // blockLength = 8 (unchanged: the new field is trailing)
    0x43, 0x00,              // templateId  = 67
    0x78, 0x00,              // schemaId    = 120
    0x03, 0x00,              // version     = 3
    0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,  // sessionId
    0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A,  // correlationId
    // body fixed (8 bytes)
    0x2A, 0x00, 0x00, 0x00,  // topicId = 42
    0x01, 0x00,              // version = 1
    0x00, 0x00,              // errorCode = 0
    // descriptorBlob varbytes32
    0x04, 0x00, 0x00, 0x00,  // len = 4
    0xDE, 0xAD, 0xBE, 0xEF,
    // Phase 2b Part C: trailing partitionCount
    0x08, 0x00, 0x00, 0x00,  // partitionCount = 8
};
static constexpr std::size_t kTopicSchemaResponseGoldenV2Size = sizeof(kTopicSchemaResponseGoldenV2);  // 44

TEST(SchemaConformance, TopicSchemaResponseDecodeGolden) {
    ASSERT_EQ(kTopicSchemaResponseGoldenSize, 40u);

    assertHeader(kTopicSchemaResponseGolden, kTopicSchemaResponseGoldenSize,
                 rpc::TopicSchemaResponse::TEMPLATE_ID, rpc::TopicSchemaResponse::BODY_FIXED);

    auto msgOpt = rpc::TopicSchemaResponse::decode(kTopicSchemaResponseGolden, kTopicSchemaResponseGoldenSize);
    ASSERT_TRUE(msgOpt.has_value());
    EXPECT_EQ(msgOpt->sessionId,     kSessionId);
    EXPECT_EQ(msgOpt->correlationId, kCorrelationId);
    EXPECT_EQ(msgOpt->topicId,       42);
    EXPECT_EQ(msgOpt->version,       1u);
    EXPECT_EQ(msgOpt->errorCode,     0u);
    ASSERT_EQ(msgOpt->descriptorLen, 4u);
    static constexpr uint8_t kBlob[] = {0xDE, 0xAD, 0xBE, 0xEF};
    EXPECT_EQ(std::memcmp(msgOpt->descriptorBlob, kBlob, 4), 0);
    EXPECT_EQ(msgOpt->partitionCount, 0) << "a pre-2b (V1) frame reports no partition count";
}

TEST(SchemaConformance, TopicSchemaResponseEncodeGolden) {
    static constexpr uint8_t kBlob[] = {0xDE, 0xAD, 0xBE, 0xEF};

    rpc::TopicSchemaResponse msg;
    msg.sessionId      = kSessionId;
    msg.correlationId  = kCorrelationId;
    msg.topicId        = 42;
    msg.version        = 1;
    msg.errorCode      = 0;
    msg.descriptorBlob = kBlob;
    msg.descriptorLen  = 4;
    msg.partitionCount = 8;

    ASSERT_EQ(msg.encodedSize(), kTopicSchemaResponseGoldenV2Size);

    std::array<uint8_t, 64> enc{};
    const std::size_t n = msg.encode(enc.data());
    ASSERT_EQ(n, kTopicSchemaResponseGoldenV2Size);
    EXPECT_EQ(std::memcmp(enc.data(), kTopicSchemaResponseGoldenV2, n), 0)
        << "Encoded TopicSchemaResponse bytes differ from the V2 golden";
}

// Phase 2b Part C (append-only wire): the V2 frame is the V1 golden
// byte-for-byte plus 4 trailing bytes — so a pre-2b decoder (reads the fixed
// block + blob, never past it) decodes a V2 frame to exactly the V1 fields.
TEST(SchemaConformanceRegression, TopicSchemaResponseV2IsV1GoldenPlusTrailingPartitionCount) {
    ASSERT_EQ(kTopicSchemaResponseGoldenV2Size, kTopicSchemaResponseGoldenSize + 4u);
    EXPECT_EQ(std::memcmp(kTopicSchemaResponseGoldenV2, kTopicSchemaResponseGolden,
                          kTopicSchemaResponseGoldenSize), 0)
        << "the V2 prefix must equal the V1 frame — old decoders depend on it";

    assertHeader(kTopicSchemaResponseGoldenV2, kTopicSchemaResponseGoldenV2Size,
                 rpc::TopicSchemaResponse::TEMPLATE_ID, rpc::TopicSchemaResponse::BODY_FIXED);
    auto v2 = rpc::TopicSchemaResponse::decode(kTopicSchemaResponseGoldenV2, kTopicSchemaResponseGoldenV2Size);
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(v2->topicId, 42);
    EXPECT_EQ(v2->version, 1u);
    ASSERT_EQ(v2->descriptorLen, 4u);
    EXPECT_EQ(v2->partitionCount, 8);
}
```

(`TEST(SchemaConformance, TopicSchemaResponseFrameCheck)`, which follows, stays as it is.)

- [ ] **Step 2: Run the tests and confirm they fail.**

Run: `cmake --build build --target rune_protocol_tests -j8 && ctest --test-dir build -R 'SchemaMessages|SchemaConformance' --output-on-failure`
Expected: compile error `no member named 'partitionCount' in 'rune::protocol::client::TopicSchemaResponse'`.

- [ ] **Step 3: Implement.** In `include/rune/protocol/client/SchemaMessages.h`, replace the comment block and struct from `// TopicSchemaResponse (67): body = ...` through the closing `};` of `struct TopicSchemaResponse` with:

```cpp
// TopicSchemaResponse (67): body = topicId:i32, version:u16, errorCode:u16, then
//   descriptorBlob(varbytes32), then (Phase 2b Part C, APPENDED) partitionCount:i32.
// `version` is the schema version bound to the returned descriptorBlob (the
// requested fingerprint's version, or latest when requestedFingerprint was 0).
// `partitionCount` is the TOPIC's partition count (typed clients route records
// by key over it).  It is a TRAILING field after the var data, so blockLength /
// BODY_FIXED are unchanged: a pre-2b decoder stops after the blob and ignores
// it, and a pre-2b frame (no trailing bytes) decodes here with
// partitionCount = 0 ("not reported").
struct TopicSchemaResponse {
    int64_t sessionId = 0, correlationId = 0;
    int32_t topicId = -1; uint16_t version = 0; uint16_t errorCode = 0;
    const uint8_t* descriptorBlob = nullptr; uint32_t descriptorLen = 0;
    int32_t partitionCount = 0;  // Phase 2b Part C; 0 = not reported (pre-2b broker)
    static constexpr uint16_t TEMPLATE_ID = static_cast<uint16_t>(TemplateId::TopicSchemaResponse);
    static constexpr uint16_t BODY_FIXED  = 8;
    std::size_t encodedSize() const noexcept {
        return MessageHeader::ENCODED_SIZE + BODY_FIXED + 4u + descriptorLen + 4u;
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
        detail::putLE32(buf, static_cast<int>(pos), partitionCount); pos += 4;
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
        // Trailing (Phase 2b): absent / cut short on a pre-2b frame -> 0.
        if (avail >= pos + 4) {
            m.partitionCount = detail::getLE32(buf, static_cast<int>(pos)); pos += 4;
        }
        return m;
    }
};
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build --target rune_protocol_tests -j8 && ctest --test-dir build -R 'SchemaMessages|SchemaConformance' --output-on-failure`
Expected: all PASS, including the three new `*Regression*` tests and the updated goldens.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir build -R Regression --output-on-failure      # full protocol Regression suite FIRST
ctest --test-dir build --output-on-failure                    # then everything
git add include/rune/protocol/client/SchemaMessages.h \
        tests/protocol/client/SchemaMessagesTest.cpp tests/protocol/client/SchemaConformanceTest.cpp
git commit -m "feat(protocol): TopicSchemaResponse appends trailing partitionCount (Phase 2b C)

Typed clients route records by key over the topic's partitions; the
GetTopicSchema response now carries the count as a trailing i32 after the
descriptor blob. BODY_FIXED/blockLength unchanged; pre-2b frames decode
with partitionCount=0.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Broker — `GetTopicSchema` reports the topic's partition count

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-broker` (compiles `../rune-protocol` from Task 1)

**Files:**
- Modify: `libs/node/include/rune/node/AdminWorker.h:73-76` (`respondGet` signature)
- Modify: `libs/node/src/AdminWorker.cpp:139-171` (`submitGetSchema`)
- Modify: `libs/node/src/BrokerNode.cpp:3357-3378` (`deps.respondGet`)
- Modify (signature only): `libs/node/tests/integration/SchemaGatewayTest.cpp:236-247`, `libs/node/tests/integration/SchemaPersistenceTest.cpp:254-260, 503-504, 633-634, 743-744, 1067-1068, 1921-1922`, `libs/node/tests/integration/GetViewRowRegressionTest.cpp:444`, `libs/node/tests/integration/CreateViewRegressionTest.cpp:154`
- Test: `libs/node/tests/node/AdminWorkerTest.cpp` (harness at :60-101 + new tests), `libs/node/tests/integration/SchemaGatewayTest.cpp` (new test)

**Interfaces:**
- Consumes: `pc::TopicSchemaResponse::partitionCount` (Task 1).
- Produces: `AdminDeps::respondGet` is `std::function<void(int64_t session, int64_t corr, int32_t topicId, uint16_t version, int32_t partitionCount, const std::vector<uint8_t>& blob, uint16_t errorCode)>`. `partitionCount` is the **topic's** count, `RegistryEntry::partitionCount` of the latest entry (`byName`), even when the descriptor is fingerprint-selected. On any error it is `0`.

- [ ] **Step 1: Write the failing tests.**

In `libs/node/tests/node/AdminWorkerTest.cpp`, inside `struct Harness`, replace:

```cpp
    int getCount = 0;
```
with:
```cpp
    int getCount = 0;
    int32_t lastGetPartitionCount = -1;   // Phase 2b C: respondGet's partitionCount
```

and replace the harness `d.respondGet` lambda:

```cpp
        d.respondGet = [this](int64_t,int64_t,int32_t,uint16_t,const std::vector<uint8_t>&,uint16_t e) {
            ++getCount; lastErr = e;
        };
```
with:
```cpp
        d.respondGet = [this](int64_t,int64_t,int32_t,uint16_t,int32_t parts,
                              const std::vector<uint8_t>&,uint16_t e) {
            ++getCount; lastErr = e; lastGetPartitionCount = parts;
        };
```

Append these tests to the end of `AdminWorkerTest.cpp`:

```cpp
// Regression (Phase 2b Part C): GetTopicSchema must report the topic's
// partition count — the typed client routes records by key over it.  Before
// 2b the response carried none and TypedTopic sent every record to partition 0.
TEST(AdminWorkerRegression, GetSchemaReportsRegistryPartitionCount) {
    Harness h;
    h.runDefine(1,2,"characters","userId",8,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);
    h.lastErr = 0xffff;
    h.w->submitGetSchema(GetSchemaMsg{1,3,"characters"});
    EXPECT_EQ(h.lastErr,0);
    EXPECT_EQ(h.lastGetPartitionCount, 8);
}

// Regression (Phase 2b Part C): a client bound to a HISTORICAL version
// (fingerprint-selected GetTopicSchema) routes over the same partitions —
// partition count is a property of the topic, not of a schema version.
TEST(AdminWorkerRegression, GetSchemaByFingerprintReportsPartitionCount) {
    Harness h;
    h.runDefine(1,2,"characters","userId",8,false,charsBlob());
    ASSERT_EQ(h.lastErr,0);
    const uint64_t v1Fp = h.reg.byName("characters")->fingerprint;
    h.runAlter(10,11,"characters",/*expected=*/0, opsBlob({addOp("score",PrimType::Int64)}));
    ASSERT_EQ(h.lastAlterErr, 0);
    ASSERT_EQ(h.lastAlterVersion, 2);

    h.lastErr = 0xffff;
    h.lastGetPartitionCount = -1;
    h.w->submitGetSchema(GetSchemaMsg{1,4,"characters", v1Fp});
    EXPECT_EQ(h.lastErr,0) << "v1's fingerprint must resolve to the v1 descriptor";
    EXPECT_EQ(h.lastGetPartitionCount, 8);
}

// Regression (Phase 2b Part C): an error response reports 0 ("not reported"),
// never a stale or default-1 count.
TEST(AdminWorkerRegression, GetSchemaUnknownTopicReportsZeroPartitions) {
    Harness h;
    h.w->submitGetSchema(GetSchemaMsg{1,2,"nope"});
    EXPECT_EQ(h.lastErr,7);                         // UnknownTopic
    EXPECT_EQ(h.lastGetPartitionCount, 0);
}
```

In `libs/node/tests/integration/SchemaGatewayTest.cpp`, replace the `SgHarness` `deps.respondGet` lambda (lines 236-247):

```cpp
        deps.respondGet = [this](int64_t session, int64_t corr, int32_t tid,
                                 uint16_t ver, const std::vector<uint8_t>& blob, uint16_t err) {
            pc::TopicSchemaResponse r; r.sessionId=session; r.correlationId=corr;
            r.topicId=tid; r.version=ver; r.errorCode=err;
            r.descriptorBlob = blob.empty()?nullptr:blob.data();
            r.descriptorLen = (uint32_t)blob.size();
            std::vector<uint8_t> f(r.encodedSize());
            std::size_t n = r.encode(f.data());
            auto idx = egressRing.tryClaim(kMsgEgressFrame, 8+n);
            if (idx < 0) return;
            uint8_t* p = egressRing.base()+idx;
            std::memcpy(p, &session, 8); std::memcpy(p+8, f.data(), n);
            egressRing.commit(idx);
        };
```
with this lambda. It mirrors the production `BrokerNode` respondGet after Step 3, which encodes in place:
```cpp
        deps.respondGet = [this](int64_t session, int64_t corr, int32_t tid,
                                 uint16_t ver, int32_t parts,
                                 const std::vector<uint8_t>& blob, uint16_t err) {
            pc::TopicSchemaResponse r; r.sessionId=session; r.correlationId=corr;
            r.topicId=tid; r.version=ver; r.errorCode=err; r.partitionCount=parts;
            r.descriptorBlob = blob.empty()?nullptr:blob.data();
            r.descriptorLen = (uint32_t)blob.size();
            const std::size_t sz = r.encodedSize();
            auto idx = egressRing.tryClaim(kMsgEgressFrame, 8+sz);
            if (idx < 0) return;
            uint8_t* p = egressRing.base()+idx;
            std::memcpy(p, &session, 8);
            (void)r.encode(p+8);
            egressRing.commit(idx);
        };
```

Then add this test after `TEST(SchemaGateway, GetTopicSchemaRoundTrip)`:

```cpp
// ---------------------------------------------------------------------------
// SchemaGatewayRegression.GetTopicSchemaCarriesPartitionCount — Phase 2b
// Part C: a topic defined with 8 partitions through the gateway answers
// GetTopicSchema with partitionCount == 8 on the session egress (the typed
// client routes by key over it; pre-2b every typed record hit partition 0).
// ---------------------------------------------------------------------------
TEST(SchemaGatewayRegression, GetTopicSchemaCarriesPartitionCount) {
    SgHarness h;
    auto blob = charsBlob();
    const auto def = encodeDefineFrame(1, 1, "characters", 8, "userId", false, blob);
    h.gw.applyDefineTopicForTest(def.data(), def.size());
    h.pump();
    h.captured.clear();

    pc::GetTopicSchema g; g.sessionId=1; g.correlationId=7; g.topicName="characters";
    std::vector<uint8_t> gf(g.encodedSize()); g.encode(gf.data());
    h.gw.applyGetTopicSchemaForTest(gf.data(), gf.size());
    h.pump();

    std::optional<pc::TopicSchemaResponse> resp;
    for (auto& [sid, bytes] : h.captured) {
        auto d = pc::TopicSchemaResponse::decode(bytes.data(), bytes.size());
        if (d) { resp = d; break; }
    }
    ASSERT_TRUE(resp.has_value());
    EXPECT_EQ(resp->correlationId, 7);
    EXPECT_EQ(resp->errorCode, 0u);
    EXPECT_GT(resp->descriptorLen, 0u);
    EXPECT_EQ(resp->partitionCount, 8);
}
```

- [ ] **Step 2: Run the tests and confirm they fail.**

Run: `cmake --build build-fast --target rune_node_tests rune_schema_gateway_tests`
Expected: compile errors. The test lambdas no longer match `AdminDeps::respondGet` (`no viable overloaded '='`).

- [ ] **Step 3: Implement.**

`libs/node/include/rune/node/AdminWorker.h`: replace:
```cpp
    // Send the GetTopicSchema response back to the client (encode + egress).
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, const std::vector<uint8_t>& blob,
                       uint16_t errorCode)> respondGet;
```
with:
```cpp
    // Send the GetTopicSchema response back to the client (encode + egress).
    // `partitionCount` (Phase 2b Part C) is the TOPIC's partition count — the
    // typed client routes records by key over it; 0 on any error response.
    std::function<void(int64_t session, int64_t corr, int32_t topicId,
                       uint16_t version, int32_t partitionCount,
                       const std::vector<uint8_t>& blob,
                       uint16_t errorCode)> respondGet;
```

`libs/node/src/AdminWorker.cpp` `submitGetSchema`: replace both error calls:
```cpp
        deps_.respondGet(msg.session, msg.corr, -1, 0, {}, kUnknownTopic);
```
(two occurrences) with:
```cpp
        deps_.respondGet(msg.session, msg.corr, -1, 0, /*partitionCount=*/0, {}, kUnknownTopic);
```
and replace the success call:
```cpp
    deps_.respondGet(msg.session, msg.corr,
                     e->topicId, e->version,
                     rune::codec::serialize(e->descriptor), kOK);
```
with:
```cpp
    // Phase 2b Part C: report the TOPIC's partition count (`named`, the latest
    // entry — partition count is a topic property, carried unchanged by every
    // ALTER), so a typed client bound to ANY version routes records by key
    // across every partition instead of all to partition 0.
    deps_.respondGet(msg.session, msg.corr,
                     e->topicId, e->version, named->partitionCount,
                     rune::codec::serialize(e->descriptor), kOK);
```

`libs/node/src/BrokerNode.cpp`: replace the whole `deps.respondGet = [this](...) { ... };` lambda (lines 3357-3378). Fix-on-sight: the old lambda built a per-response `std::vector<uint8_t> frame` and copied it into the ring. The new one encodes straight into the claimed ring slot, which is 1 copy and no allocation:
```cpp
        deps.respondGet = [this](int64_t session, int64_t corr, int32_t topicId,
                                 uint16_t version, int32_t partitionCount,
                                 const std::vector<uint8_t>& blob,
                                 uint16_t errorCode) {
            if (!adminWorkerAgent_) return;
            namespace pc = rune::protocol::client;
            pc::TopicSchemaResponse resp;
            resp.sessionId      = session;
            resp.correlationId  = corr;
            resp.topicId        = topicId;
            resp.version        = version;
            resp.errorCode      = errorCode;
            resp.partitionCount = partitionCount;   // Phase 2b Part C
            resp.descriptorBlob = blob.empty() ? nullptr : blob.data();
            resp.descriptorLen  = static_cast<uint32_t>(blob.size());
            // Encode IN PLACE in the claimed egress slot (no per-response frame
            // vector; encodedSize() == encode()'s return by construction).
            const std::size_t sz = resp.encodedSize();
            auto& ring = adminWorkerAgent_->egressRing();
            const auto idx = ring.tryClaim(kMsgEgressFrame, 8 + sz);
            if (idx < 0) return;  // ring full — DDL response dropped (rare); client retries
            uint8_t* p = ring.base() + idx;
            std::memcpy(p, &session, 8);
            (void)resp.encode(p + 8);
            ring.commit(idx);
        };
```

Update the remaining test-harness lambdas, which change signature only. Each one gains an `int32_t` parameter after the `uint16_t` version:

`libs/node/tests/integration/SchemaPersistenceTest.cpp` near line 254, replace:
```cpp
        deps.respondGet = [this](int64_t /*session*/, int64_t /*corr*/,
                                 int32_t topicId, uint16_t /*version*/,
                                 const std::vector<uint8_t>& /*blob*/,
                                 uint16_t errorCode) {
```
with:
```cpp
        deps.respondGet = [this](int64_t /*session*/, int64_t /*corr*/,
                                 int32_t topicId, uint16_t /*version*/,
                                 int32_t /*partitionCount*/,
                                 const std::vector<uint8_t>& /*blob*/,
                                 uint16_t errorCode) {
```
Near line 503, replace:
```cpp
    deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t,
                          const std::vector<uint8_t>&, uint16_t) {};
```
with:
```cpp
    deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t, int32_t,
                          const std::vector<uint8_t>&, uint16_t) {};
```
Near lines 633 and 743 (identical text, so use replace-all on this file), replace:
```cpp
    noopDeps.respondGet         = [](int64_t, int64_t, int32_t, uint16_t,
                                     const std::vector<uint8_t>&, uint16_t) {};
```
with:
```cpp
    noopDeps.respondGet         = [](int64_t, int64_t, int32_t, uint16_t, int32_t,
                                     const std::vector<uint8_t>&, uint16_t) {};
```
Near lines 1067 and 1921 (identical text, replace-all), replace:
```cpp
    deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t,
                         const std::vector<uint8_t>&, uint16_t) {};
```
with:
```cpp
    deps.respondGet = [](int64_t, int64_t, int32_t, uint16_t, int32_t,
                         const std::vector<uint8_t>&, uint16_t) {};
```
`libs/node/tests/integration/GetViewRowRegressionTest.cpp:444` and `libs/node/tests/integration/CreateViewRegressionTest.cpp:154`: in each file, replace:
```cpp
        deps.respondGet   = [](int64_t, int64_t, int32_t, uint16_t, const std::vector<uint8_t>&, uint16_t) {};
```
with:
```cpp
        deps.respondGet   = [](int64_t, int64_t, int32_t, uint16_t, int32_t, const std::vector<uint8_t>&, uint16_t) {};
```
Then confirm no stale site remains:
```bash
grep -rn "respondGet" libs | grep -v "int32_t, *const std::vector\|int32_t parts\|int32_t /\*partitionCount\*/\|int32_t partitionCount\|deps_.respondGet\|respondDefine"
```
Expected: only comment lines (`BrokerNode.cpp:41`, `:2927`, `:3309`, `SchemaGatewayTest.cpp:255`) are listed.

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build-fast && ctest --test-dir build-fast -R 'AdminWorker|SchemaGateway|SchemaPersistence|CreateView|GetViewRow' --output-on-failure`
Expected: all PASS, including `AdminWorkerRegression.GetSchemaReportsRegistryPartitionCount`, `AdminWorkerRegression.GetSchemaByFingerprintReportsPartitionCount`, `AdminWorkerRegression.GetSchemaUnknownTopicReportsZeroPartitions` and `SchemaGatewayRegression.GetTopicSchemaCarriesPartitionCount`.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir build-fast -R Regression --output-on-failure   # full broker Regression suite FIRST
ctest --test-dir build-fast --output-on-failure                 # then everything
git add libs/node/include/rune/node/AdminWorker.h libs/node/src/AdminWorker.cpp libs/node/src/BrokerNode.cpp \
        libs/node/tests/node/AdminWorkerTest.cpp libs/node/tests/integration/SchemaGatewayTest.cpp \
        libs/node/tests/integration/SchemaPersistenceTest.cpp libs/node/tests/integration/GetViewRowRegressionTest.cpp \
        libs/node/tests/integration/CreateViewRegressionTest.cpp
git commit -m "fix(schema): GetTopicSchema reports the topic's partitionCount (Phase 2b C)

Typed clients had no partition count and routed every record to partition
0. AdminWorker now reports the topic's count (also for fingerprint-selected
historical versions); BrokerNode encodes the response in place in the egress
ring slot (fix-on-sight: no per-response frame vector).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Codec — the generated struct exposes `key()`

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-codec`

**Files:**
- Modify: `src/Generator.cpp` (includes; insert after the var-len members block that ends at line 123 `o << "\n";`)
- Modify: `include/rune/codec/Generator.h` (doc)
- Create: `tools/make_str_keyed_blob.cpp`, `tests/generated/str_keyed.blob` (generated once, checked in), `tests/GeneratedKeyTest.cpp`
- Modify: `CMakeLists.txt` (tool, custom command, test source, dependency)
- Test: `tests/GeneratorTest.cpp` (append)

**Interfaces:**
- Produces: for a descriptor with an `isKey` field `f`, the generated struct has exactly one accessor:
  - scalar (`int32/int64/uint16/uint32/uint64/float64/bool/enum`): `[[nodiscard]] <cppType> key() const noexcept { return f; }`, for example `int64_t key()`
  - `string(N)`: `[[nodiscard]] const std::array<char, N>& key() const noexcept { return f; }`
  - `string`/`bytes`: `[[nodiscard]] std::span<const uint8_t> key() const noexcept`, a zero-copy byte view of the field's content
  - no `isKey` field: no `key()` at all
- `generateHeader` throws `std::invalid_argument` when the descriptor has an `isKey` field **and** a field named `key`, because the member would collide with the accessor.

- [ ] **Step 1: Write the failing tests.**

Create `tools/make_str_keyed_blob.cpp`:

```cpp
// tools/make_str_keyed_blob.cpp
// One-shot helper: writes tests/generated/str_keyed.blob
// Descriptor: sku string (key), qty int32 — the var-len-key case of the
// Phase 2b Part C generated key() accessor (must be a BYTE span, never a
// std::string_view: rune::client::Key would hash the view object itself).

#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>
#include <vector>

int main(int argc, char* argv[]) {
    const char* outPath = (argc >= 2) ? argv[1] : "str_keyed.blob";

    auto d = rune::codec::DescriptorBuilder("str_keyed")
        .add("sku", rune::codec::PrimType::String, 1, true)
        .add("qty", rune::codec::PrimType::Int32,  1, false)
        .build();

    auto blob = rune::codec::serialize(d);

    std::ofstream f(outPath, std::ios::binary | std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "make_str_keyed_blob: cannot open '%s'\n", outPath);
        return 1;
    }
    f.write(reinterpret_cast<const char*>(blob.data()),
            static_cast<std::streamsize>(blob.size()));
    return 0;
}
```

In `CMakeLists.txt`, after the `make_fixedstr_sample_blob` helper block (the line `target_link_libraries(make_fixedstr_sample_blob PRIVATE rune_codec)`), add:

```cmake

# ── Helper: make_str_keyed_blob (generates tests/generated/str_keyed.blob) ──
# Phase 2b Part C: a string-KEYED schema so the generated key() byte-span
# accessor is compiled and exercised (tests/GeneratedKeyTest.cpp).
add_executable(make_str_keyed_blob tools/make_str_keyed_blob.cpp)
target_link_libraries(make_str_keyed_blob PRIVATE rune_codec)
```

After the line `add_custom_target(generate_fixedstr_sample_header DEPENDS "${FIXEDSTR_SAMPLE_GEN_H}")`, add:

```cmake

# ── Custom command: generate str_keyed.gen.h from str_keyed.blob (Phase 2b C) ─
set(STR_KEYED_BLOB "${CMAKE_CURRENT_SOURCE_DIR}/tests/generated/str_keyed.blob")
set(STR_KEYED_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/str_keyed.gen.h")

add_custom_command(
    OUTPUT  "${STR_KEYED_GEN_H}"
    COMMAND rune_codec_gen "${STR_KEYED_BLOB}" "${STR_KEYED_GEN_H}"
    DEPENDS rune_codec_gen "${STR_KEYED_BLOB}"
    COMMENT "Generating str_keyed.gen.h from str_keyed.blob"
)
add_custom_target(generate_str_keyed_header DEPENDS "${STR_KEYED_GEN_H}")
```

In the `add_executable(rune_codec_tests ...)` list, replace `    tests/DecodeBoundaryMatrixTest.cpp)` with:
```cmake
    tests/DecodeBoundaryMatrixTest.cpp
    tests/GeneratedKeyTest.cpp)
```
and replace `add_dependencies(rune_codec_tests generate_characters_header generate_fixedstr_sample_header)` with:
```cmake
add_dependencies(rune_codec_tests generate_characters_header generate_fixedstr_sample_header
                 generate_str_keyed_header)
```

Generate the blob once and check it in:
```bash
cmake --build build --target make_str_keyed_blob -j8 && ./build/make_str_keyed_blob tests/generated/str_keyed.blob
```

Create `tests/GeneratedKeyTest.cpp`:

```cpp
// tests/GeneratedKeyTest.cpp — Phase 2b Part C regression: the generated
// struct's key() accessor, compiled from REAL generated headers:
//   characters.gen.h      userId int64 is the key  -> int64_t key()
//   fixedstr_sample.gen.h no key field             -> no key() at all
//   str_keyed.gen.h       sku string is the key    -> std::span<const uint8_t> key()
// TypedTopic::send routes by Key(rec.key()); before 2b there was no key()
// and every typed record went to partition 0 with an empty key.

#include <gtest/gtest.h>
#include "generated/characters.gen.h"
#include "generated/fixedstr_sample.gen.h"
#include "generated/str_keyed.gen.h"

#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

namespace {
template <class R>
concept HasKey = requires(const R& r) { r.key(); };
} // namespace

TEST(GeneratedKeyRegression, ScalarKeyReturnsTheIsKeyFieldByValue) {
    static_assert(std::is_same_v<decltype(std::declval<const rune::gen::Characters&>().key()), int64_t>,
                  "an int64 key field must surface as int64_t key() (Key(int64_t) = 8 LE bytes)");
    rune::gen::Characters c;
    c.id     = 7;
    c.userId = 42;
    EXPECT_EQ(c.key(), 42) << "key() must return the isKey field (userId), not id";
}

TEST(GeneratedKeyRegression, SchemaWithoutKeyFieldHasNoKeyAccessor) {
    static_assert(!HasKey<rune::gen::FixedstrSample>,
                  "no isKey field -> no key(); TypedTopic keeps partition 0 + empty key");
    SUCCEED();
}

TEST(GeneratedKeyRegression, StringKeyIsAByteSpanOverTheContent) {
    static_assert(std::is_same_v<decltype(std::declval<const rune::gen::StrKeyed&>().key()),
                                 std::span<const uint8_t>>,
                  "a var-len key must be a BYTE span — a std::string_view is trivially copyable "
                  "and rune::client::Key would hash the view object (pointer+len), not the content");
    const std::string_view s = "sku-42";
    rune::gen::StrKeyed r;
    r.sku = s;
    r.qty = 3;
    const std::span<const uint8_t> k = r.key();
    ASSERT_EQ(k.size(), s.size());
    EXPECT_EQ(std::memcmp(k.data(), s.data(), s.size()), 0);
    EXPECT_EQ(static_cast<const void*>(k.data()), static_cast<const void*>(s.data()))
        << "key() is a zero-copy view of the field";
}
```

Append to `tests/GeneratorTest.cpp`:

```cpp
// ── Phase 2b Part C: key() accessor emission ──────────────────────────────────

TEST(GeneratorRegression, EmitsKeyAccessorForScalarKey) {
    auto d = DescriptorBuilder("characters").add("id",PrimType::Int64)
               .add("userId",PrimType::Int64,1,true).add("name",PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("[[nodiscard]] int64_t key() const noexcept { return userId; }"),
              std::string::npos) << hdr;
}

TEST(GeneratorRegression, EmitsKeyAccessorForFixedStrKey) {
    auto d = DescriptorBuilder("codes").addFixedStr("code", 8, 1, true)
               .add("n", PrimType::Int32).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("[[nodiscard]] const std::array<char, 8>& key() const noexcept { return code; }"),
              std::string::npos) << hdr;
}

TEST(GeneratorRegression, EmitsByteSpanKeyForStringKeyNeverStringView) {
    auto d = DescriptorBuilder("str_keyed").add("sku",PrimType::String,1,true)
               .add("qty",PrimType::Int32).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("[[nodiscard]] std::span<const uint8_t> key() const noexcept {"),
              std::string::npos) << hdr;
    EXPECT_EQ(hdr.find("std::string_view key()"), std::string::npos)
        << "a string_view key would make rune::client::Key hash the view object, not the content";
}

TEST(GeneratorRegression, NoKeyFieldEmitsNoKeyAccessor) {
    auto d = DescriptorBuilder("nokey").add("id",PrimType::Int64).add("name",PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_EQ(hdr.find("key()"), std::string::npos) << hdr;
}

TEST(GeneratorRegression, FieldNamedKeyCollidingWithAccessorIsRejected) {
    auto d = DescriptorBuilder("kv").add("key",PrimType::Int64,1,true)
               .add("value",PrimType::String).build();
    EXPECT_THROW((void)generateHeader(d), std::invalid_argument)
        << "a member named `key` and the key() accessor cannot coexist — fail with a clear error, "
           "never emit a header that does not compile";
}
```

Add `#include <stdexcept>` and `#include <string>` under the existing includes at the top of `tests/GeneratorTest.cpp`.

- [ ] **Step 2: Run the tests and confirm they fail.**

Run: `cmake --build build --target rune_codec_tests -j8 && ctest --test-dir build -R 'Generator|GeneratedKey' --output-on-failure`
Expected: compile error in `GeneratedKeyTest.cpp` (`no member named 'key' in 'rune::gen::Characters'`).

- [ ] **Step 3: Implement.**

`src/Generator.cpp`: add `#include <stdexcept>` after `#include <algorithm>`. Then replace the end of the members section:

```cpp
    // ── Members: var-len fields ──────────────────────────────────────────────
    for (const auto& f : d.fields) {
        if (isVarLen(f.type)) {
            o << "    " << cppType(f.type) << " " << f.name << " = {};\n";
        }
    }
    o << "\n";
```
with:
```cpp
    // ── Members: var-len fields ──────────────────────────────────────────────
    for (const auto& f : d.fields) {
        if (isVarLen(f.type)) {
            o << "    " << cppType(f.type) << " " << f.name << " = {};\n";
        }
    }
    o << "\n";

    // ── key() — Phase 2b Part C ─────────────────────────────────────────────
    // The isKey field's value in the exact form rune::client::Key consumes, so
    // TypedTopic::send(rec) routes like Producer::send(h, Key(rec.key()), ...):
    //   scalar      -> by value  (Key(int64_t)/Key(uint64_t) = 8 LE bytes;
    //                             other scalars = their raw bytes)
    //   string(N)   -> const std::array<char, N>&  (its N raw bytes)
    //   string/bytes-> std::span<const uint8_t> over the CONTENT — never a
    //                  std::string_view: that is trivially copyable, so Key
    //                  would hash the view object (pointer + length) instead.
    // No isKey field -> no key(): TypedTopic sends partition 0 + empty key.
    {
        const FieldDescriptor* kf = nullptr;
        for (const auto& f : d.fields) {
            if (f.isKey) { kf = &f; break; }
        }
        if (kf != nullptr) {
            if (d.find("key") != nullptr) {
                throw std::invalid_argument(
                    "generateHeader: schema '" + d.name + "' has a field named 'key', which "
                    "collides with the generated key() accessor of its key field '" + kf->name +
                    "' — rename the field");
            }
            if (isVarLen(kf->type)) {
                o << "    [[nodiscard]] std::span<const uint8_t> key() const noexcept {\n";
                o << "        return {reinterpret_cast<const uint8_t*>(" << kf->name << ".data()), "
                  << kf->name << ".size()};\n";
                o << "    }\n\n";
            } else if (kf->type == PrimType::FixedStr) {
                o << "    [[nodiscard]] const std::array<char, " << kf->fixedLen
                  << ">& key() const noexcept { return " << kf->name << "; }\n\n";
            } else {
                o << "    [[nodiscard]] " << cppType(kf->type)
                  << " key() const noexcept { return " << kf->name << "; }\n\n";
            }
        }
    }
```

`include/rune/codec/Generator.h`: replace:
```cpp
///   - static const Descriptor& descriptor()   (embeds the serialized blob).
///
```
with:
```cpp
///   - static const Descriptor& descriptor()   (embeds the serialized blob).
///   - key() const (Phase 2b Part C) — ONLY when a field isKey: the key
///     field's value in the form rune::client::Key consumes (scalar by value,
///     string(N) as const std::array<char,N>&, string/bytes as a
///     std::span<const uint8_t> over the content).  TypedTopic::send routes by
///     Key(rec.key()); a schema with no key field gets no key() and its typed
///     records go to partition 0 with an empty record key.
///
/// @throws std::invalid_argument if the descriptor has a key field AND a field
///         named "key" (the member would collide with the key() accessor).
///
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build --target rune_codec_tests -j8 && ctest --test-dir build -R 'Generator|GeneratedKey|GeneratedRoundTrip|FixedStrGen' --output-on-failure`
Expected: all PASS. Also check that the generated header still compiles with the existing round-trip tests (`GeneratedRoundTrip.*`, `FixedStrGen.*`).

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir build -R Regression --output-on-failure   # full codec Regression suite FIRST
ctest --test-dir build --output-on-failure                 # then everything
git add src/Generator.cpp include/rune/codec/Generator.h tools/make_str_keyed_blob.cpp \
        tests/generated/str_keyed.blob tests/GeneratedKeyTest.cpp tests/GeneratorTest.cpp CMakeLists.txt
git commit -m "feat(codegen): generated struct exposes key() for the isKey field (Phase 2b C)

TypedTopic routes by Key(rec.key()). Scalars by value, string(N) as its
array, string/bytes as a byte span (never string_view: Key would hash the
view object). No key field -> no key(). A field named 'key' that would
collide with the accessor is rejected with a clear error.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Client — the broker's `partitionCount` reaches `TopicSchemaResult`

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-client-cpp` (compiles `../rune-protocol` from Task 1)

**Files:**
- Modify: `include/rune/client/Completion.h:73` (`reserved0` doc)
- Modify: `include/rune/client/Client.h:92-97` (`TopicSchemaResult`), `:988-990` (`completeCtl_` decl), `:555-557` (op doc)
- Modify: `src/Client.cpp:1311-1321` (`completeCtl_`), `:1531-1542` (`getTopicSchemaAndWait`), `:2656-2676` (`TopicSchemaResponse` case)
- Modify: `tests/support/TestCompletions.h:99-110` (`schemaResult()`)
- Test: `tests/client/OpenTopicGateTest.cpp` (gateway + new tests)

**Interfaces:**
- Consumes: `rpc::TopicSchemaResponse::partitionCount` (Task 1).
- Produces:
  - `Completion.reserved0` for `OpKind::GetTopicSchema` = the broker-reported partition count (`uint32_t` bit pattern of the i32; 0 = not reported). The `Completion` layout is unchanged (40 bytes).
  - `struct TopicSchemaResult { int32_t topicId; uint16_t version; std::vector<uint8_t> descriptorBlob; uint16_t errorCode; int32_t partitionCount = 0; }`. The new field is appended, and 0 means the broker did not report one.
  - `Client::completeCtl_(PendingCtl&, OpStatus, int64_t value = 0, int64_t aux = 0, uint16_t errorCode = 0, std::span<const uint8_t> payload = {}, detail::CompletionRing::Overflow ov = Truncate, uint32_t reserved0 = 0)`.

- [ ] **Step 1: Write the failing tests.** In `tests/client/OpenTopicGateTest.cpp`:

Extend `GateGateway`. Replace:
```cpp
    int32_t              getTopicId   = 0x40000001;
    uint16_t             getVersion   = 1;
    uint16_t             getErrorCode = 0;
    std::vector<uint8_t> getBlob;
```
with:
```cpp
    int32_t              getTopicId   = 0x40000001;
    uint16_t             getVersion   = 1;
    uint16_t             getErrorCode = 0;
    std::vector<uint8_t> getBlob;
    int32_t              getPartitionCount = 0;    // Phase 2b C: trailing partitionCount
    bool                 omitPartitionCount = false; // simulate a pre-2b broker (no trailing i32)
```
Replace:
```cpp
    struct FingerprintOverride {
        int32_t               topicId;
        uint16_t              version;
        std::vector<uint8_t>  blob;
    };
```
with:
```cpp
    struct FingerprintOverride {
        int32_t               topicId;
        uint16_t              version;
        std::vector<uint8_t>  blob;
        int32_t               partitionCount = 0;   // Phase 2b C
    };
```
In `onFrame`'s `GetTopicSchema` case, replace:
```cpp
                    resp.descriptorBlob = it->second.blob.data();
                    resp.descriptorLen  = static_cast<uint32_t>(it->second.blob.size());
```
with:
```cpp
                    resp.descriptorBlob = it->second.blob.data();
                    resp.descriptorLen  = static_cast<uint32_t>(it->second.blob.size());
                    resp.partitionCount = it->second.partitionCount;
```
replace:
```cpp
                resp.descriptorBlob = getBlob.data();
                resp.descriptorLen  = static_cast<uint32_t>(getBlob.size());
            }
```
with:
```cpp
                resp.descriptorBlob = getBlob.data();
                resp.descriptorLen  = static_cast<uint32_t>(getBlob.size());
                resp.partitionCount = getPartitionCount;
            }
```
and replace:
```cpp
            std::vector<uint8_t> scratch(resp.encodedSize() + 32u);
            const std::size_t n = resp.encode(scratch.data());
            if (n > 0) sendFrame(scratch.data(), n);
```
with:
```cpp
            std::vector<uint8_t> scratch(resp.encodedSize() + 32u);
            const std::size_t n = resp.encode(scratch.data());
            // A pre-2b broker's frame stops after the descriptor blob.
            if (n > 0) sendFrame(scratch.data(), omitPartitionCount ? n - 4u : n);
```

Append at the end of the file:

```cpp
// ===========================================================================
// Phase 2b Part C — the broker-reported partition count reaches the client:
// TopicSchemaResponse trailing partitionCount -> Completion.reserved0 ->
// TopicSchemaResult.partitionCount.
// ===========================================================================
TEST(OpenTopicGateRegression, GetTopicSchemaReportsBrokerPartitionCount) {
    GateHarness h(1316, 1317);
    h.build();
    h.connectAndWait();

    h.gw->getBlob           = serialize(makeCharsDescriptor());
    h.gw->getTopicId        = 0x40000001;
    h.gw->getVersion        = 1;
    h.gw->getErrorCode      = 0;
    h.gw->getPartitionCount = 8;

    // Token form: the completion itself carries it in reserved0.
    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });
    h.pumpUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady());
    ASSERT_TRUE(fut.ok()) << fut.describe();
    EXPECT_EQ(fut.outcome().c.reserved0, 8u);
    EXPECT_EQ(fut.schemaResult().partitionCount, 8);

    // *AndWait form.
    GateGatewayPump pump(*h.gw);
    const TopicSchemaResult r = h.client->getTopicSchemaAndWait("characters");
    EXPECT_EQ(r.errorCode, 0u);
    EXPECT_EQ(r.topicId, 0x40000001);
    EXPECT_EQ(r.partitionCount, 8);
}

TEST(OpenTopicGateRegression, PreTwoBBrokerReportsZeroPartitionCount) {
    GateHarness h(1318, 1319);
    h.build();
    h.connectAndWait();

    h.gw->getBlob            = serialize(makeCharsDescriptor());
    h.gw->getTopicId         = 0x40000001;
    h.gw->getVersion         = 1;
    h.gw->getErrorCode       = 0;
    h.gw->getPartitionCount  = 8;      // encoded, then cut off below
    h.gw->omitPartitionCount = true;   // the 40-byte pre-2b frame

    GateGatewayPump pump(*h.gw);
    const TopicSchemaResult r = h.client->getTopicSchemaAndWait("characters");
    EXPECT_EQ(r.errorCode, 0u);
    EXPECT_FALSE(r.descriptorBlob.empty()) << "the pre-2b frame must still decode fully";
    EXPECT_EQ(r.partitionCount, 0) << "not reported -> 0, never garbage";
}
```

- [ ] **Step 2: Run the tests and confirm they fail.**

Run: `cmake --build build --target rune_open_topic_gate_tests -j8`
Expected: compile error `no member named 'partitionCount' in 'rune::client::TopicSchemaResult'`.

- [ ] **Step 3: Implement.**

`include/rune/client/Completion.h`: replace:
```cpp
    uint32_t reserved0 = 0;
};
static_assert(sizeof(Completion) == 40);
```
with:
```cpp
    uint32_t reserved0 = 0;      ///< BatchAck: producerEpoch; GetTopicSchema: the topic's partitionCount
                                 ///< (Phase 2b Part C; 0 = a pre-2b broker that does not report it)
};
static_assert(sizeof(Completion) == 40);
```

`include/rune/client/Client.h`: replace:
```cpp
struct TopicSchemaResult {
    int32_t              topicId   = -1;
    uint16_t             version   = 0;
    std::vector<uint8_t> descriptorBlob;
    uint16_t             errorCode = 0;
};
```
with:
```cpp
struct TopicSchemaResult {
    int32_t              topicId   = -1;
    uint16_t             version   = 0;
    std::vector<uint8_t> descriptorBlob;
    uint16_t             errorCode = 0;
    /// Phase 2b Part C: the topic's partition count as reported by the broker
    /// (TopicSchemaResponse trailing field); 0 = not reported (pre-2b broker).
    int32_t              partitionCount = 0;
};
```
Replace:
```cpp
    //   GetTopicSchema value = topicId, aux = version, payload = descriptor
    //                  blob (polled form: up to maxResultBytes, else
    //                  ResultTooLarge with value = the real size).
```
with:
```cpp
    //   GetTopicSchema value = topicId, aux = version, payload = descriptor
    //                  blob (polled form: up to maxResultBytes, else
    //                  ResultTooLarge with value = the real size),
    //                  reserved0 = the topic's partitionCount (Phase 2b; 0 =
    //                  not reported by a pre-2b broker).
```
Replace:
```cpp
    void completeCtl_(PendingCtl& p, OpStatus st, int64_t value = 0, int64_t aux = 0,
                      uint16_t errorCode = 0, std::span<const uint8_t> payload = {},
                      detail::CompletionRing::Overflow ov = detail::CompletionRing::Overflow::Truncate);
```
with:
```cpp
    void completeCtl_(PendingCtl& p, OpStatus st, int64_t value = 0, int64_t aux = 0,
                      uint16_t errorCode = 0, std::span<const uint8_t> payload = {},
                      detail::CompletionRing::Overflow ov = detail::CompletionRing::Overflow::Truncate,
                      uint32_t reserved0 = 0);
```

`src/Client.cpp`: replace:
```cpp
void Client::completeCtl_(PendingCtl& p, OpStatus st, int64_t value, int64_t aux, uint16_t errorCode,
                          std::span<const uint8_t> payload, detail::CompletionRing::Overflow ov) {
    Completion c;
    c.token     = p.token;
    c.value     = value;
    c.aux       = aux;
    c.op        = p.op;
    c.status    = st;
    c.errorCode = errorCode;
    deliver_(p.target, c, payload, ov);
}
```
with:
```cpp
void Client::completeCtl_(PendingCtl& p, OpStatus st, int64_t value, int64_t aux, uint16_t errorCode,
                          std::span<const uint8_t> payload, detail::CompletionRing::Overflow ov,
                          uint32_t reserved0) {
    Completion c;
    c.token     = p.token;
    c.value     = value;
    c.aux       = aux;
    c.op        = p.op;
    c.status    = st;
    c.errorCode = errorCode;
    c.reserved0 = reserved0;   // GetTopicSchema: partitionCount (Phase 2b C)
    deliver_(p.target, c, payload, ov);
}
```
In `getTopicSchemaAndWait(name, requestedFingerprint, timeout)`, replace:
```cpp
                 result.topicId   = static_cast<int32_t>(c.value);
                 result.version   = static_cast<uint16_t>(c.aux);
                 result.errorCode = c.errorCode;
                 if (c.status == OpStatus::Ok) result.descriptorBlob.assign(p.begin(), p.end());
```
with:
```cpp
                 result.topicId        = static_cast<int32_t>(c.value);
                 result.version        = static_cast<uint16_t>(c.aux);
                 result.errorCode      = c.errorCode;
                 result.partitionCount = static_cast<int32_t>(c.reserved0);  // Phase 2b C
                 if (c.status == OpStatus::Ok) result.descriptorBlob.assign(p.begin(), p.end());
```
In the `case codec::TemplateId::TopicSchemaResponse:` handler, replace:
```cpp
            completeCtl_(p, opt->errorCode == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         opt->topicId, opt->version, opt->errorCode, blob,
                         detail::CompletionRing::Overflow::TooLarge);
```
with:
```cpp
            // Phase 2b Part C: the topic's partition count rides in reserved0
            // (0 = a pre-2b broker that does not report it).
            completeCtl_(p, opt->errorCode == 0 ? OpStatus::Ok : OpStatus::BrokerError,
                         opt->topicId, opt->version, opt->errorCode, blob,
                         detail::CompletionRing::Overflow::TooLarge,
                         static_cast<uint32_t>(opt->partitionCount));
```

`tests/support/TestCompletions.h` `schemaResult()` mirrors `getTopicSchemaAndWait`. Replace:
```cpp
        r.errorCode = outcome().c.errorCode;
        if (status() == rune::client::OpStatus::Ok) r.descriptorBlob = outcome().payload;
        return r;
    }
    /// AlterTopic: as defineResult(), with (topicId, newVersion, errorCode).
```
with:
```cpp
        r.errorCode = outcome().c.errorCode;
        r.partitionCount = static_cast<int32_t>(outcome().c.reserved0);  // Phase 2b C
        if (status() == rune::client::OpStatus::Ok) r.descriptorBlob = outcome().payload;
        return r;
    }
    /// AlterTopic: as defineResult(), with (topicId, newVersion, errorCode).
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build -j8 && ctest --test-dir build -R 'OpenTopicGate|ClientSchema|CliLive' --output-on-failure`
Expected: all PASS, including `OpenTopicGateRegression.GetTopicSchemaReportsBrokerPartitionCount` and `OpenTopicGateRegression.PreTwoBBrokerReportsZeroPartitionCount`. The other gateways (`RuneCliLiveTest`, `ClientSchemaTest`) still default to `partitionCount = 0`.

- [ ] **Step 5: Regression gate, then commit.**

```bash
ctest --test-dir build -R Regression --output-on-failure   # full client Regression suite FIRST
ctest --test-dir build --output-on-failure                 # then everything
git add include/rune/client/Completion.h include/rune/client/Client.h src/Client.cpp \
        tests/support/TestCompletions.h tests/client/OpenTopicGateTest.cpp
git commit -m "feat(client): TopicSchemaResult.partitionCount from the broker (Phase 2b C)

The GetTopicSchema response's trailing partitionCount is carried in
Completion.reserved0 (layout unchanged) into TopicSchemaResult. A pre-2b
broker reports 0.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Client — `TypedTopic` routes by `Key(rec.key())` over the topic's partitions

**Repo:** `/Users/vradu/Projects/se-worktrees/rune-client-cpp` (compiles `../rune-codec` from Task 3)

**Files:**
- Rewrite: `include/rune/client/TypedTopic.h` (full content below)
- Modify: `include/rune/client/Client.h:680-692` (`openTopicAndWait`)
- Modify: `tests/client/OpenTopicTest.cpp:126-127`, `tests/client/OpenTopicGateTest.cpp` (3 constructor sites + new tests)
- Create: `tests/client/TypedTopicRoutingRegressionTest.cpp`, `bench/TypedTopicSendBench.cpp`
- Modify: `CMakeLists.txt` (shared `generate_characters_header`, `rune_open_topic_tests`, bench target)

**Interfaces:**
- Consumes: `TopicSchemaResult.partitionCount`, `Completion.reserved0` (Task 4); generated `key()` (Task 3).
- Produces (Part B builds on these):
  - `template <class R> concept rune::client::HasRecordKey = requires(const R& r) { r.key(); };`
  - `[[nodiscard]] constexpr int32_t rune::client::typedPartitionCount(int32_t reported) noexcept`: returns `reported > 0 ? reported : 1`
  - `TypedTopic<T, ProducerT>::TypedTopic(ProducerT producer, TopicHandle topic, uint16_t version)` (**the second parameter was `int32_t topicId`**)
  - `[[nodiscard]] TopicHandle TypedTopic::handle() const noexcept`
  - `TypedTopic::send(const T&)`: with `HasRecordKey<T>` it sends `producer.send(handle(), Key(rec.key()), bytes)`, otherwise `producer.send(handle(), Key{}, bytes)`. A var-len key over `Key::kMaxInline` (16) throws `std::invalid_argument` and sends nothing. A fixed key type wider than 16 bytes fails a `static_assert`.

- [ ] **Step 1: Write the failing tests.**

Create `tests/client/TypedTopicRoutingRegressionTest.cpp`:

```cpp
// tests/client/TypedTopicRoutingRegressionTest.cpp
//
// Phase 2b Part C regression (spec §C): TypedTopic::send sent every record as
// producer.send(TopicHandle{id, 1}, Key{}, bytes) — all on partition 0, no
// record key.  It must route EXACTLY like Producer::send(TopicHandle{id, n},
// Key(<key field>), bytes): same handle (the broker-reported partition count),
// same key bytes, hence the same partition (Producer::send routes to
// key.partition(handle.partitionCount) — src/producer/Producer.cpp).
//
// Runs on the REAL generated rune::gen::Characters (tests/generated/
// characters.blob -> characters.gen.h; userId int64 is the isKey field)
// behind a recording ProducerT.  No Aeron.

#include "characters.gen.h"
#include "../support/AllocCounter.h"

#include <gtest/gtest.h>
#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/TypedTopic.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <set>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace rc = rune::client;

namespace {

struct Sent {
    rc::TopicHandle                           handle;
    std::array<uint8_t, rc::Key::kMaxInline>  key{};
    std::size_t                               keyLen    = 0;
    int32_t                                   partition = -1;  // where Producer::send routes it
};

// Records exactly what TypedTopic hands the producer.  `out` must have its
// capacity reserved by the test: send() itself never allocates.
struct RecordingProducer {
    std::vector<Sent>* out = nullptr;
    void send(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t>) noexcept {
        Sent s;
        s.handle = t;
        s.keyLen = k.size();
        std::copy(k.bytes().begin(), k.bytes().end(), s.key.begin());
        s.partition = k.partition(t.partitionCount);
        out->push_back(s);
    }
};

// A schema with NO key field: the generator emits no key().
struct NoKeyRecord {
    static constexpr uint64_t         kFingerprint = 0x1111;
    static constexpr std::string_view kSchemaName  = "nokey";
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4; }
    std::size_t encode(uint8_t* b) const noexcept { b[0] = 0; b[1] = 0; b[2] = 1; b[3] = 0; return 4; }
};

// Mirrors the generator's var-len key(): a BYTE span over the content
// (rune-codec Generator.cpp, Phase 2b Part C).
struct StrKeyRecord {
    static constexpr uint64_t         kFingerprint = 0x2222;
    static constexpr std::string_view kSchemaName  = "str_keyed";
    std::string_view sku;
    [[nodiscard]] std::span<const uint8_t> key() const noexcept {
        return {reinterpret_cast<const uint8_t*>(sku.data()), sku.size()};
    }
    [[nodiscard]] std::size_t encodedSize() const noexcept { return 4 + sku.size(); }
    std::size_t encode(uint8_t* b) const noexcept {
        b[0] = 0; b[1] = 0; b[2] = 1; b[3] = 0;
        std::memcpy(b + 4, sku.data(), sku.size());
        return 4 + sku.size();
    }
};

bool keyEquals(const Sent& s, const rc::Key& want) {
    return s.keyLen == want.size() &&
           std::equal(want.bytes().begin(), want.bytes().end(), s.key.begin());
}

} // namespace

// The spec's regression: an 8-partition typed topic distributes records by key
// exactly like Producer::send with the same Key.
TEST(TypedTopicRoutingRegression, EightPartitionTypedTopicRoutesLikeProducerSendWithSameKey) {
    static_assert(rc::HasRecordKey<rune::gen::Characters>);
    static_assert(std::is_same_v<decltype(std::declval<const rune::gen::Characters&>().key()), int64_t>);

    constexpr int32_t kParts = 8;
    std::vector<Sent> sent;
    sent.reserve(1000);
    rc::TypedTopic<rune::gen::Characters, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{5, kParts}, /*version=*/1);

    std::set<int32_t> hit, expectedHit;
    for (int64_t u = 0; u < 1000; ++u) {
        rune::gen::Characters rec;
        rec.id       = 10'000 + u;
        rec.userId   = u;
        rec.strength = 1;
        rec.name     = "n";
        topic.send(rec);

        ASSERT_EQ(sent.size(), static_cast<std::size_t>(u + 1));
        const Sent& s = sent.back();
        const rc::Key want(u);   // what a Producer::send caller passes for this record
        EXPECT_EQ(s.handle.topicId, 5);
        EXPECT_EQ(s.handle.partitionCount, kParts) << "must route over the topic's partitions, not 1";
        ASSERT_TRUE(keyEquals(s, want)) << "record key must be the userId field (8 LE bytes), not empty";
        EXPECT_EQ(s.partition, want.partition(kParts));
        hit.insert(s.partition);
        expectedHit.insert(want.partition(kParts));
    }
    EXPECT_EQ(hit, expectedHit);
    EXPECT_GT(hit.size(), 1u) << "the bug sent every record to partition 0";
}

// Spec: a typed topic whose schema has no key field keeps today's behaviour.
TEST(TypedTopicRoutingRegression, SchemaWithoutKeyFieldKeepsPartitionZeroAndEmptyKey) {
    static_assert(!rc::HasRecordKey<NoKeyRecord>);
    std::vector<Sent> sent;
    sent.reserve(4);
    rc::TypedTopic<NoKeyRecord, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{6, 8}, /*version=*/1);
    topic.send(NoKeyRecord{});
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].handle.partitionCount, 8);
    EXPECT_EQ(sent[0].keyLen, 0u);
    EXPECT_EQ(sent[0].partition, 0);
}

// A string key must hash its CONTENT: two records whose keys are equal but
// live in different buffers must land on the same partition with the same
// key bytes (a string_view-typed key would hash pointer+len instead).
TEST(TypedTopicRoutingRegression, StringKeyRoutesByContentBytesNotByViewObject) {
    std::vector<Sent> sent;
    sent.reserve(4);
    rc::TypedTopic<StrKeyRecord, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{7, 8}, /*version=*/1);
    const char bufA[] = "sku-42";
    const char bufB[] = "sku-42";
    ASSERT_NE(static_cast<const void*>(bufA), static_cast<const void*>(bufB));
    topic.send(StrKeyRecord{std::string_view(bufA, 6)});
    topic.send(StrKeyRecord{std::string_view(bufB, 6)});
    ASSERT_EQ(sent.size(), 2u);
    const std::string_view content = "sku-42";
    const rc::Key want(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(content.data()),
                                                content.size()));
    EXPECT_TRUE(keyEquals(sent[0], want));
    EXPECT_TRUE(keyEquals(sent[1], want));
    EXPECT_EQ(sent[0].partition, want.partition(8));
    EXPECT_EQ(sent[0].partition, sent[1].partition);
}

// rune::client::Key holds at most 16 bytes and silently truncates.  A typed
// topic must FAIL CLOSED instead: a truncated record key would merge distinct
// entities (compaction / views fold by key).  Nothing may be sent.
TEST(TypedTopicRoutingRegression, VarKeyLongerThan16BytesIsRejectedNotTruncated) {
    std::vector<Sent> sent;
    sent.reserve(4);
    rc::TypedTopic<StrKeyRecord, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{7, 8}, /*version=*/1);
    EXPECT_THROW(topic.send(StrKeyRecord{"0123456789abcdefX"}), std::invalid_argument);  // 17 bytes
    EXPECT_TRUE(sent.empty()) << "a rejected record must not reach the producer";
    EXPECT_NO_THROW(topic.send(StrKeyRecord{"0123456789abcdef"}));                        // exactly 16
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_EQ(sent[0].keyLen, 16u);
}

// CLAUDE.md #4: keyed routing adds no allocation to the send hot path.
TEST(TypedTopicRoutingRegression, KeyedSendAllocatesNothingInSteadyState) {
    std::vector<Sent> sent;
    sent.reserve(2000);
    rc::TypedTopic<rune::gen::Characters, RecordingProducer> topic(
        RecordingProducer{&sent}, rc::TopicHandle{5, 8}, /*version=*/1);
    rune::gen::Characters rec;
    rec.id = 1; rec.userId = 1; rec.strength = 1; rec.name = "warm";
    topic.send(rec);   // warm-up: the reuse buffer grows once to its high-water mark

    alloc_counter::startThisThread();
    for (int64_t u = 0; u < 1000; ++u) {
        rec.userId = u;
        topic.send(rec);
    }
    const long allocs = alloc_counter::stopThisThread();
    EXPECT_EQ(allocs, 0) << "TypedTopic::send must not allocate in steady state";
    EXPECT_EQ(sent.size(), 1001u);
}
```

Add these to `tests/client/OpenTopicGateTest.cpp`, appended at the end of the file:

```cpp
// ===========================================================================
// Phase 2b Part C — the typed topic binds the broker-reported partition count
// (both open paths), and falls back to 1 against a pre-2b broker.
// ===========================================================================
TEST(OpenTopicGateRegression, OpenTopicAndWaitBindsBrokerPartitionCount) {
    GateHarness h(1320, 1321);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId        = 0x40000002;
    h.gw->getVersion        = 2;
    h.gw->getBlob           = serialize(makeCharsV2Descriptor());
    h.gw->getErrorCode      = 0;
    h.gw->getPartitionCount = 8;
    GateGateway::FingerprintOverride ov{/*topicId=*/0x40000001, /*version=*/1,
                                        serialize(makeCharsV1Descriptor())};
    ov.partitionCount = 8;
    h.gw->fingerprintOverrides[StubV1::kFingerprint] = ov;

    GateGatewayPump pump(*h.gw);
    TypedTopic<StubV1> tt = h.client->openTopicAndWait<StubV1>("characters");
    EXPECT_EQ(tt.handle().topicId, 0x40000001);
    EXPECT_EQ(tt.handle().partitionCount, 8)
        << "regression: TypedTopic was hard-wired to TopicHandle{id, 1}";
    EXPECT_EQ(tt.boundVersion(), 1u);
}

TEST(OpenTopicGateRegression, FromSchemaBindsBrokerPartitionCount) {
    GateHarness h(1322, 1323);
    h.build();
    h.connectAndWait();

    h.gw->getBlob           = serialize(makeCharsDescriptor());
    h.gw->getTopicId        = 0x40000001;
    h.gw->getVersion        = 1;
    h.gw->getErrorCode      = 0;
    h.gw->getPartitionCount = 8;

    test_support::TestCq cq(*h.client);
    auto fut = cq.issue([&](rune::client::Completions& q, rune::client::Token t) {
        return h.client->getTopicSchema("characters", q, t);
    });
    h.pumpUntil([&]() { return fut.ready(); });
    ASSERT_TRUE(fut.waitReady());

    TypedTopic<StubT_Match> tt =
        TypedTopic<StubT_Match>::fromSchema(*h.client, fut.outcome().c, fut.outcome().payload);
    EXPECT_EQ(tt.handle().topicId, 0x40000001);
    EXPECT_EQ(tt.handle().partitionCount, 8);
}

TEST(OpenTopicGateRegression, PreTwoBBrokerTypedTopicFallsBackToOnePartition) {
    GateHarness h(1324, 1325);
    h.build();
    h.connectAndWait();

    h.gw->getTopicId         = 0x40000001;
    h.gw->getVersion         = 1;
    h.gw->getBlob            = serialize(makeCharsDescriptor());
    h.gw->getErrorCode       = 0;
    h.gw->omitPartitionCount = true;   // pre-2b frame: no trailing partitionCount
    h.gw->fingerprintOverrides[StubT_Match::kFingerprint] = GateGateway::FingerprintOverride{
        /*topicId=*/0x40000001, /*version=*/1, serialize(makeCharsDescriptor())};

    GateGatewayPump pump(*h.gw);
    TypedTopic<StubT_Match> tt = h.client->openTopicAndWait<StubT_Match>("characters");
    EXPECT_EQ(tt.handle().partitionCount, 1)
        << "unreported (0) must become 1 — partition 0 as before 2b, never an out-of-range partition";
}
```

Update the three existing `TypedTopic<…, StubCaptureProducer>` constructor sites in `tests/client/OpenTopicGateTest.cpp`:
- replace `StubCaptureProducer{&captured}, /*topicId=*/0x40000001, tt.boundVersion());` with `StubCaptureProducer{&captured}, TopicHandle{0x40000001, 1}, tt.boundVersion());`
- replace `StubCaptureProducer{&captured}, /*topicId=*/1, /*version=*/7);` with `StubCaptureProducer{&captured}, TopicHandle{1, 1}, /*version=*/7);`
- replace `StubCaptureProducer{&captured}, /*topicId=*/0x40000010, tt.boundVersion());` with `StubCaptureProducer{&captured}, TopicHandle{0x40000010, 1}, tt.boundVersion());`

In `tests/client/OpenTopicTest.cpp`, replace:
```cpp
        StubProducer{&callCount}, /*topicId=*/42, /*version=*/1);
```
with:
```cpp
        StubProducer{&callCount}, rune::client::TopicHandle{42, 1}, /*version=*/1);
```

`CMakeLists.txt`: make the generated `rune::gen::Characters` header available to tests and benches as well as examples. Insert immediately **before** the line `# ── Benchmarks ────...` (which precedes `if(BUILD_BENCHMARKS AND aeron_FOUND)`):

```cmake
# ── Phase 2b Part C: characters.gen.h for tests/benches too ──────────────────
# The typed-routing regression and the TypedTopic send bench compile against
# the REAL generated rune::gen::Characters (userId is the isKey field -> key()).
# The examples block above defines this codegen when BUILD_EXAMPLES is on;
# define the identical rule here when it is not.
if((BUILD_TESTING OR BUILD_BENCHMARKS) AND NOT TARGET generate_characters_header)
    add_executable(rune_codec_gen "${RUNE_CODEC_ROOT}/tools/rune_codec_gen_main.cpp")
    target_link_libraries(rune_codec_gen PRIVATE rune_codec)
    set(CHARACTERS_BLOB  "${CMAKE_CURRENT_SOURCE_DIR}/tests/generated/characters.blob")
    set(CHARACTERS_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/gen/characters.gen.h")
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/gen")
    add_custom_command(
        OUTPUT  "${CHARACTERS_GEN_H}"
        COMMAND $<TARGET_FILE:rune_codec_gen> "${CHARACTERS_BLOB}" "${CHARACTERS_GEN_H}"
        DEPENDS rune_codec_gen "${CHARACTERS_BLOB}"
        COMMENT "Phase 2b C: generating characters.gen.h from characters.blob"
    )
    add_custom_target(generate_characters_header DEPENDS "${CHARACTERS_GEN_H}")
endif()

```

Replace the `rune_open_topic_tests` block:
```cmake
    add_executable(rune_open_topic_tests
        tests/client/OpenTopicTest.cpp
    )
    target_include_directories(rune_open_topic_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUNE_CODEC_ROOT}/include
        ${RUNE_PROTOCOL_INCLUDE}
    )
    target_link_libraries(rune_open_topic_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_open_topic_tests)
```
with:
```cmake
    add_executable(rune_open_topic_tests
        tests/client/OpenTopicTest.cpp
        tests/client/TypedTopicRoutingRegressionTest.cpp   # Phase 2b C
        tests/support/AllocCounter.cpp                     # zero-alloc send check
    )
    add_dependencies(rune_open_topic_tests generate_characters_header)
    target_include_directories(rune_open_topic_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUNE_CODEC_ROOT}/include
        ${RUNE_PROTOCOL_INCLUDE}
        "${CMAKE_CURRENT_BINARY_DIR}/gen"                  # characters.gen.h
    )
    target_link_libraries(rune_open_topic_tests PRIVATE rune_codec GTest::gtest_main)
    gtest_discover_tests(rune_open_topic_tests)
```

- [ ] **Step 2: Run the tests and confirm they fail.**

Run: `cmake --build build --target rune_open_topic_tests rune_open_topic_gate_tests -j8`
Expected: compile errors: `no member named 'HasRecordKey' in namespace 'rune::client'`, no `TypedTopic` constructor taking `TopicHandle`, and `no member named 'handle'`.

- [ ] **Step 3: Implement.** Replace the whole of `include/rune/client/TypedTopic.h` with:

```cpp
// include/rune/client/TypedTopic.h
//
// Task 11 — TypedTopic<T>: typed, zero-per-send-alloc producer wrapper.
// Phase 2b Part C — routes each record by its key field over the topic's
// partitions (was: always partition 0, empty key).
//
// schemaMatches(expected, blob):
//   Deserialises `blob` and computes its fingerprint; returns true iff it
//   equals `expected`.  Returns false (never throws) on any decode error.
//   Unit-testable without a live broker.
//
// TypedTopic<T, ProducerT>:
//   Wraps a producer handle and the topic's TopicHandle{topicId,
//   partitionCount} (partitionCount as reported by the broker's GetTopicSchema
//   response).  send(rec) encodes `rec` into a REUSED buffer (buf_ capacity
//   persists; never freed per send — RULE #4) and routes it EXACTLY like
//   ProducerT::send(handle, Key(rec.key()), bytes): same partition, real
//   record key.
//
//   T contract (generated struct):
//     static constexpr uint64_t     kFingerprint
//     static constexpr std::string_view kSchemaName
//     size_t encodedSize() const noexcept
//     size_t encode(uint8_t*)  const noexcept
//     key() const noexcept     — OPTIONAL: generated only when the schema has a
//                                key field (rune-codec generateHeader).  A
//                                schema with NO key field keeps the pre-2b
//                                behaviour: partition 0, empty record key.
//
//   Keys are at most Key::kMaxInline (16) bytes and are NEVER truncated: a
//   fixed key type wider than that fails to compile; a string/bytes key longer
//   than that makes send() throw std::invalid_argument (nothing is sent).
//
//   ProducerT contract (default = Producer):
//     void send(TopicHandle, const Key&, std::span<const uint8_t>)
//
//   The second template parameter exists as a test seam: production code uses
//   TypedTopic<MyStruct> (ProducerT defaults to Producer); unit tests that must
//   not link Aeron use TypedTopic<StubT, StubProducer>.
//
// Threading:
//   TypedTopic is NOT thread-safe.  It must be called from a single thread
//   (the app thread that owns it).  The underlying Producer::send is lock-free
//   and thread-safe, but the reuse buffer is owned here.

#pragma once

#include <rune/client/Completion.h>
#include <rune/client/Key.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/TopicHandle.h>
#include <rune/codec/Fingerprint.h>
#include <rune/codec/Serialize.h>

#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace rune::client {

// ---------------------------------------------------------------------------
// schemaMatches — unit-testable, no live broker required.
//
// Returns true iff deserialise(blob) succeeds AND fingerprint(d) == expected.
// Returns false on any exception (malformed blob, truncation, etc.).
// ---------------------------------------------------------------------------
[[nodiscard]] inline bool schemaMatches(uint64_t                    expected,
                                         std::span<const uint8_t>   blob) noexcept
{
    try {
        return rune::codec::fingerprint(rune::codec::deserialize(blob)) == expected;
    } catch (...) {
        // Intentionally maps ANY failure — malformed blob OR std::bad_alloc from
        // fingerprint()'s internal allocation — to false.  Cold path: called once
        // per openTopic<T>; OOM here is fatal to the open anyway.
        return false;
    }
}

/// Phase 2b Part C: true iff the generated record type exposes its key field
/// (rune-codec emits key() only for schemas with an isKey field).
template <class R>
concept HasRecordKey = requires(const R& r) { r.key(); };

/// Phase 2b Part C: the partition count a typed topic routes over, from the
/// broker-reported value (TopicSchemaResult::partitionCount /
/// Completion::reserved0).  A pre-2b broker reports nothing (0): fall back to
/// 1 — every record lands on partition 0 exactly as before 2b, never on an
/// out-of-range partition.
[[nodiscard]] constexpr int32_t typedPartitionCount(int32_t reported) noexcept {
    return reported > 0 ? reported : 1;
}

// ---------------------------------------------------------------------------
// TypedTopic<T, ProducerT>
//
// ProducerT defaults to Producer (the real Aeron-backed producer).  The second
// parameter is a test seam: unit tests that must not link Aeron supply a
// no-op stub so buf_ reuse can be asserted without any live transport.
// ---------------------------------------------------------------------------
template <class T, class ProducerT = Producer>
class TypedTopic {
public:
    /// Construct from an existing producer handle, the topic's handle
    /// {topicId, partitionCount} (Phase 2b Part C: partitionCount as reported
    /// by the broker — see typedPartitionCount), and the schema VERSION this
    /// topic is bound to (Task 19).  `version` is the version whose
    /// fingerprint matched T::kFingerprint when the broker answered the open
    /// gate (Client::openTopicAndWait / TypedTopic::fromSchema) — NOT
    /// necessarily T::kVersion (the codegen-baked compile-time version): an
    /// old generated client stays bound to the version it compiled against
    /// even after the topic evolves.  Every send() stamps this bound version
    /// onto the encoded record, overwriting the codegen-baked kVersion.
    TypedTopic(ProducerT producer, TopicHandle topic, uint16_t version)
        : producer_(std::move(producer))
        , topic_(topic)
        , version_(version)
    {}

    // F8c C7 (spec §3.1 rule 3): move-only — it owns its Producer, which is
    // move-only (one owning thread: the reuse buffer is not thread-safe).
    TypedTopic(const TypedTopic&) = delete;
    TypedTopic& operator=(const TypedTopic&) = delete;
    TypedTopic(TypedTopic&&) = default;
    TypedTopic& operator=(TypedTopic&&) = default;

    ~TypedTopic() = default;

    /// F8c C3: build a typed topic from a GetTopicSchema completion (spec
    /// §2.5), on the CALLER's thread — the replacement for openTopic's
    /// async-task continuation.  `client` is the Client the op was issued on
    /// (any type with producer(): a template, so this header does not include
    /// Client.h).  Throws std::runtime_error unless the completion is Ok AND
    /// the descriptor's fingerprint equals T::kFingerprint exactly.
    template <class ClientT>
    [[nodiscard]] static TypedTopic fromSchema(ClientT& client, const Completion& c,
                                               std::span<const uint8_t> descriptorBlob) {
        if (c.status != OpStatus::Ok) {
            throw std::runtime_error("openTopic: schema lookup failed (status " +
                                     std::to_string(static_cast<int>(c.status)) + ", errorCode " +
                                     std::to_string(c.errorCode) + ")");
        }
        if (!schemaMatches(T::kFingerprint, descriptorBlob)) {
            throw std::runtime_error("openTopic: generated type is stale — rerun rune schema generate");
        }
        // GetTopicSchema completion wire contract: value = topicId, aux =
        // version (Task 18), reserved0 = partitionCount (Phase 2b Part C).
        return TypedTopic(client.producer(),
                          TopicHandle{static_cast<int32_t>(c.value),
                                      typedPartitionCount(static_cast<int32_t>(c.reserved0))},
                          static_cast<uint16_t>(c.aux));
    }

    // ---- Hot path — ZERO per-send heap allocation ----------------------------

    /// Encode `rec` into the reused buffer, then route it like
    /// ProducerT::send(handle(), Key(rec.key()), bytes) — or with an empty key
    /// (partition 0) when the schema has no key field.
    ///
    /// RULE #4: buf_ capacity is never freed between sends (it only grows to
    /// the high-water mark); the Key is a 17-byte inline value.
    ///
    /// Throws std::invalid_argument (nothing sent) when a string/bytes key is
    /// longer than Key::kMaxInline — a record key is never truncated.
    void send(const T& rec) {
        if constexpr (HasRecordKey<T>) {
            sendWithKey_(rec, recordKey_(rec));
        } else {
            // No key field in the schema: partition 0, empty record key
            // (documented pre-2b behaviour, kept by spec §C).
            sendWithKey_(rec, Key{});
        }
    }

    // ---- Accessors / test seams ------------------------------------------------

    /// The topic handle every send routes over: {topicId, partitionCount}.
    [[nodiscard]] TopicHandle handle() const noexcept { return topic_; }

    /// Returns the current capacity of the reuse buffer.
    /// Used in unit tests to assert that buf_ does not reallocate across sends
    /// (capacity must be non-decreasing — proves RULE #4 on the zero-alloc path).
    [[nodiscard]] std::size_t bufCapacity() const noexcept { return buf_.capacity(); }

    /// The schema version this topic is bound to (Task 19 test seam).
    [[nodiscard]] uint16_t boundVersion() const noexcept { return version_; }

private:
    /// Phase 2b Part C: the record key, exactly as a Producer::send caller
    /// would build it from the key field — never truncated.
    [[nodiscard]] static Key recordKey_(const T& rec) {
        using KeyT = std::remove_cvref_t<decltype(rec.key())>;
        if constexpr (std::is_same_v<KeyT, std::span<const uint8_t>>) {
            const std::span<const uint8_t> kb = rec.key();
            if (kb.size() > Key::kMaxInline) [[unlikely]] {
                throw std::invalid_argument(
                    "TypedTopic::send: key field is " + std::to_string(kb.size()) +
                    " bytes; a record key holds at most " + std::to_string(Key::kMaxInline) +
                    " (it is never truncated)");
            }
            return Key(kb);
        } else {
            static_assert(std::is_trivially_copyable_v<KeyT>,
                          "TypedTopic: key() must return a trivially-copyable value or a byte span");
            static_assert(sizeof(KeyT) <= Key::kMaxInline,
                          "TypedTopic: the key field is wider than Key::kMaxInline (16 bytes) and "
                          "cannot be a record key without truncation");
            return Key(rec.key());
        }
    }

    void sendWithKey_(const T& rec, const Key& key) {
        const std::size_t needed = rec.encodedSize();
        // Grow buf_ if needed but never shrink (capacity persists — RULE #4).
        if (needed > buf_.size()) {
            buf_.resize(needed);
        }
        const std::size_t n = rec.encode(buf_.data());
        if (n == 0) return;

        // Task 19: stamp the BOUND version onto the encoded record header,
        // overwriting the codegen-baked T::kVersion at bytes [2..3] (LE u16 —
        // Generator.cpp's encode() layout: [blockLength:u16][version:u16]...).
        // This is what keeps a client compiled against an older schema
        // version writing records tagged with that older version after the
        // topic has evolved out from under it.
        if (n >= 4) {
            buf_[2] = static_cast<uint8_t>(version_ & 0xffu);
            buf_[3] = static_cast<uint8_t>((version_ >> 8) & 0xffu);
        }

        producer_.send(topic_, key, std::span<const uint8_t>(buf_.data(), n));
    }

    ProducerT              producer_;
    TopicHandle            topic_;         // {topicId, partitionCount} — Phase 2b Part C
    uint16_t               version_ = 0;   // bound schema version; stamped onto every send()
    std::vector<uint8_t>   buf_;           // reused; grown to high-water mark, never freed per send
};

} // namespace rune::client
```

`include/rune/client/Client.h` `openTopicAndWait`: replace:
```cpp
        return TypedTopic<T>(producer(), r.topicId, r.version);
```
with:
```cpp
        // Phase 2b Part C: route over the topic's partitions (broker-reported;
        // a pre-2b broker reports 0 -> 1, the pre-2b partition-0 behaviour).
        return TypedTopic<T>(producer(), TopicHandle{r.topicId, typedPartitionCount(r.partitionCount)},
                             r.version);
```

Create `bench/TypedTopicSendBench.cpp`:

```cpp
// bench/TypedTopicSendBench.cpp — Phase 2b Part C: TypedTopic::send cost with
// key routing (in-process, no Aeron).  The producer stub does the same
// routing work Producer::send does (key.partition(handle.partitionCount)) and
// nothing else, so the delta between the two benches is the cost of building
// Key(rec.key()) + hashing it.
//
//   BM_TypedTopicSendKeyed    real generated rune::gen::Characters (userId key), 8 partitions
//   BM_TypedTopicSendUnkeyed  the same record behind a wrapper with NO key() (pre-2b path)
//
// Gate (CLAUDE.md #4): 0 allocations per send — pinned by
// TypedTopicRoutingRegression.KeyedSendAllocatesNothingInSteadyState; this bench
// records the latency delta.

#include "characters.gen.h"

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/TypedTopic.h>

#include <benchmark/benchmark.h>

#include <cstdint>
#include <span>
#include <string_view>

namespace rc = rune::client;

namespace {

struct RoutingNullProducer {
    int32_t lastPartition = 0;
    void send(rc::TopicHandle t, const rc::Key& k, std::span<const uint8_t> v) noexcept {
        lastPartition = k.partition(t.partitionCount);   // Producer::send's routing step
        benchmark::DoNotOptimize(lastPartition);
        benchmark::DoNotOptimize(v.data());
    }
};

// The pre-2b shape: same bytes, no key() -> Key{} -> partition 0.
struct UnkeyedCharacters {
    static constexpr uint64_t         kFingerprint = rune::gen::Characters::kFingerprint;
    static constexpr std::string_view kSchemaName  = rune::gen::Characters::kSchemaName;
    rune::gen::Characters c;
    [[nodiscard]] std::size_t encodedSize() const noexcept { return c.encodedSize(); }
    std::size_t encode(uint8_t* b) const noexcept { return c.encode(b); }
};

void BM_TypedTopicSendKeyed(benchmark::State& state) {
    rc::TypedTopic<rune::gen::Characters, RoutingNullProducer> topic(
        RoutingNullProducer{}, rc::TopicHandle{1, 8}, /*version=*/1);
    rune::gen::Characters rec;
    rec.id = 1; rec.userId = 0; rec.strength = 3; rec.name = "bench";
    for (auto _ : state) {
        ++rec.userId;
        topic.send(rec);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_TypedTopicSendKeyed);

void BM_TypedTopicSendUnkeyed(benchmark::State& state) {
    rc::TypedTopic<UnkeyedCharacters, RoutingNullProducer> topic(
        RoutingNullProducer{}, rc::TopicHandle{1, 8}, /*version=*/1);
    UnkeyedCharacters rec;
    rec.c.id = 1; rec.c.userId = 0; rec.c.strength = 3; rec.c.name = "bench";
    for (auto _ : state) {
        ++rec.c.userId;
        topic.send(rec);
    }
    state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_TypedTopicSendUnkeyed);

} // namespace

BENCHMARK_MAIN();
```

Register the bench in `CMakeLists.txt` inside `if(BUILD_BENCHMARKS AND aeron_FOUND)`, immediately after the `rune_producer_encode_bench` block (after its `target_link_libraries(rune_producer_encode_bench PRIVATE ... benchmark::benchmark)` line):

```cmake

    # Phase 2b Part C — TypedTopic::send keyed-routing cost (in-process, no Aeron)
    add_executable(rune_typed_topic_send_bench bench/TypedTopicSendBench.cpp)
    add_dependencies(rune_typed_topic_send_bench generate_characters_header)
    target_include_directories(rune_typed_topic_send_bench PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${RUNE_CODEC_ROOT}/include
        ${RUNE_PROTOCOL_INCLUDE}
        "${CMAKE_CURRENT_BINARY_DIR}/gen")
    target_link_libraries(rune_typed_topic_send_bench PRIVATE rune_codec benchmark::benchmark)
```

- [ ] **Step 4: Run the tests and confirm they pass.**

Run: `cmake --build build -j8 && ctest --test-dir build -R 'TypedTopicRouting|OpenTopic|HandleLifetime' --output-on-failure`
Expected: all PASS. That includes the five `TypedTopicRoutingRegression.*`, `OpenTopicGateRegression.OpenTopicAndWaitBindsBrokerPartitionCount`, `…FromSchemaBindsBrokerPartitionCount`, `…PreTwoBBrokerTypedTopicFallsBackToOnePartition`, and the existing version-stamp tests (`SendStampsBoundVersionNotCodegenVersionRegression`, `LineageBindsExactVersionForEachCompiledClientRegression`).

Check that the e2e probes still compile against the new `TypedTopic`, since `examples` are built by `BUILD_EXAMPLES=ON`: `cmake --build build --target rune_schema_probe view_probe -j8` must succeed.

- [ ] **Step 5: Bench (record the numbers in the commit body).**

```bash
cmake -S . -B build -DBUILD_BENCHMARKS=ON && cmake --build build --target rune_typed_topic_send_bench -j8
./build/rune_typed_topic_send_bench --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
cmake -S . -B build -DBUILD_BENCHMARKS=OFF   # restore the configured test build
```
Expected: both benches run. The `build` dir is a Debug build, so treat the numbers as a relative comparison (keyed vs unkeyed in one binary), not an absolute gate. The keyed path costs one 8-byte FNV-1a hash more than the unkeyed path. The zero-allocation gate is the unit test, not this bench.

- [ ] **Step 6: Regression gate, then commit.**

```bash
ctest --test-dir build -R Regression --output-on-failure   # full client Regression suite FIRST
ctest --test-dir build --output-on-failure                 # then everything
git add include/rune/client/TypedTopic.h include/rune/client/Client.h \
        tests/client/TypedTopicRoutingRegressionTest.cpp tests/client/OpenTopicTest.cpp \
        tests/client/OpenTopicGateTest.cpp bench/TypedTopicSendBench.cpp CMakeLists.txt
git commit -m "fix(client): TypedTopic routes by key over the topic's partitions (Phase 2b C)

TypedTopic::send always sent TopicHandle{id,1} with an empty Key: every typed
record landed on partition 0 with no record key. It now holds the broker-
reported TopicHandle{id, partitionCount} and sends Key(rec.key()) - exactly
what Producer::send with the same Key does. Schemas without a key field keep
partition 0 + empty key. Keys are never truncated (>16 bytes: compile error
for fixed types, std::invalid_argument for string/bytes). Zero allocations
per send (regression-tested); bench: <paste BM_TypedTopicSendKeyed/Unkeyed medians>.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Real-broker e2e — an 8-partition typed topic routes by key

**Repos:** client `/Users/vradu/Projects/se-worktrees/rune-client-cpp`, broker `/Users/vradu/Projects/se-worktrees/rune-broker`

**Files:**
- Modify: `rune-client-cpp/examples/view_probe.cpp` (includes; new `typedRoutingLeg`; one call in `setup` mode)
- Modify: `rune-client-cpp/CMakeLists.txt` (`view_probe` gets the generated header)
- Modify: `rune-broker/docker/view-engine-e2e.sh` (assert the sentinel)

**Interfaces:**
- Consumes: everything from Tasks 1-5, running against a real broker built from this branch.
- Produces: the stdout sentinel `TYPED-ROUTING: OK` from `view_probe` (setup mode), which the e2e script asserts.

- [ ] **Step 1: Add the probe leg.**

In `examples/view_probe.cpp`, add these after `#include <rune/client/view/ViewRowCodec.h>`:
```cpp
#include <rune/client/TypedTopic.h>
#include <rune/client/subscriber/Subscription.h>

#include "characters.gen.h"   // Phase 2b C: build-time codegen (tests/generated/characters.blob)
```
and add these to the standard includes:
```cpp
#include <algorithm>
#include <mutex>
#include <set>
```

Add this function immediately before `int main() {`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b Part C: typed topics route by key (real broker, real Aeron).
//
// defineTopic "typed_route" with 8 partitions and the generated Characters
// schema (userId is the key field) -> GetTopicSchema must report
// partitionCount 8 -> openTopicAndWait<Characters> binds TopicHandle{id, 8}
// -> 64 typed sends -> subscribe to EVERY partition from earliest -> every
// record must sit on Key(userId).partition(8) with record key == Key(userId)
// bytes, and the set of partitions hit must equal the expected set (>1).
// Pre-2b every typed record landed on partition 0 with an empty key.
// Returns 0 on success, else a probe exit code (40..47).
// ---------------------------------------------------------------------------
static int typedRoutingLeg(Client& c, std::chrono::milliseconds wait, int boundMs) {
    constexpr int32_t kParts   = 8;
    constexpr int     kRecords = 64;

    const auto blob = rune::codec::serialize(rune::gen::Characters::descriptor());
    DefineTopicResult def = c.defineTopicAndWait("typed_route", kParts, /*keyField=*/"userId",
                                                 /*validate=*/false, std::span<const uint8_t>(blob), wait);
    if (def.errorCode != 0) {
        std::cerr << "PROBE: FAIL defineTopic(typed_route) errorCode=" << def.errorCode << "\n";
        return 40;
    }

    const TopicSchemaResult sr = c.getTopicSchemaAndWait("typed_route", wait);
    if (sr.errorCode != 0 || sr.partitionCount != kParts) {
        std::cerr << "PROBE: FAIL getTopicSchema(typed_route) errorCode=" << sr.errorCode
                  << " partitionCount=" << sr.partitionCount << " (want " << kParts << ")\n";
        return 41;
    }

    TypedTopic<rune::gen::Characters> topic =
        c.openTopicAndWait<rune::gen::Characters>("typed_route", wait);
    if (topic.handle().topicId != def.topicId || topic.handle().partitionCount != kParts) {
        std::cerr << "PROBE: FAIL typed handle {" << topic.handle().topicId << ","
                  << topic.handle().partitionCount << "} want {" << def.topicId << "," << kParts << "}\n";
        return 42;
    }

    std::set<int32_t> expectedParts;
    for (int i = 0; i < kRecords; ++i) {
        rune::gen::Characters rec;
        rec.id       = 9000 + i;
        rec.userId   = 1000 + i;
        rec.strength = i;
        rec.name     = "typed";
        topic.send(rec);
        expectedParts.insert(Key(static_cast<int64_t>(rec.userId)).partition(kParts));
    }

    struct Seen { int32_t partition; int64_t userId; bool keyMatches; };
    std::mutex        mtx;
    std::vector<Seen> seen;
    seen.reserve(static_cast<std::size_t>(kRecords));

    SubscribeOptions opts;
    opts.from      = From::earliest();
    opts.isolation = Isolation::ReadUncommitted;
    const TopicHandle th{def.topicId, kParts};
    std::vector<Subscription> subs;
    subs.reserve(static_cast<std::size_t>(kParts));
    for (int32_t p = 0; p < kParts; ++p) {
        subs.push_back(c.subscribe(th, p, opts, [&, p](const Record& r) {
            rune::gen::Characters d;
            d.decode(r.value);
            const Key want(static_cast<int64_t>(d.userId));
            const bool keyOk = r.key.size() == want.size() &&
                               std::equal(r.key.begin(), r.key.end(), want.bytes().begin());
            std::lock_guard<std::mutex> lk(mtx);
            seen.push_back(Seen{p, d.userId, keyOk});
        }));
    }

    const auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(boundMs);
    while (std::chrono::steady_clock::now() < dl) {
        { std::lock_guard<std::mutex> lk(mtx); if (static_cast<int>(seen.size()) >= kRecords) break; }
        std::this_thread::sleep_for(10ms);   // poll to a deadline — the count decides, not the sleep
    }
    for (auto& s : subs) s.unsubscribe();

    std::lock_guard<std::mutex> lk(mtx);
    if (static_cast<int>(seen.size()) != kRecords) {
        std::cerr << "PROBE: FAIL typed_route consumed " << seen.size() << " / " << kRecords << "\n";
        return 43;
    }
    std::set<int32_t> gotParts;
    for (const Seen& s : seen) {
        const int32_t want = Key(static_cast<int64_t>(s.userId)).partition(kParts);
        if (!s.keyMatches) {
            std::cerr << "PROBE: FAIL typed_route userId=" << s.userId << " record key != Key(userId)\n";
            return 44;
        }
        if (s.partition != want) {
            std::cerr << "PROBE: FAIL typed_route userId=" << s.userId << " on partition " << s.partition
                      << " want " << want << "\n";
            return 45;
        }
        gotParts.insert(s.partition);
    }
    if (gotParts != expectedParts || gotParts.size() < 2) {
        std::cerr << "PROBE: FAIL typed_route partitions hit=" << gotParts.size()
                  << " expected=" << expectedParts.size() << "\n";
        return 46;
    }
    std::cerr << "PROBE: typed_route OK — " << kRecords << " records over " << gotParts.size()
              << " partitions, each on Key(userId).partition(8) with record key == Key(userId)\n";
    std::cout << "TYPED-ROUTING: OK parts=" << kParts << " records=" << kRecords
              << " partitionsHit=" << gotParts.size() << "\n";
    return 0;
}
```

In `main`, setup mode, insert the leg right before `// ---- Handoff for the restart leg (scraped by the e2e script) ----`:

```cpp
            // ---- 6. Phase 2b Part C: typed topics route by key -------------
            if (const int rc = typedRoutingLeg(c, wait, boundMs); rc != 0) return rc;
```

In `CMakeLists.txt`, the `view_probe` target gets the generated header. Replace:
```cmake
    add_executable(view_probe
        examples/view_probe.cpp
    )
    target_include_directories(view_probe PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
    )
```
with:
```cmake
    add_executable(view_probe
        examples/view_probe.cpp
    )
    add_dependencies(view_probe generate_characters_header)   # Phase 2b C typed_route leg
    target_include_directories(view_probe PRIVATE
        ${RUNE_PROTOCOL_INCLUDE}
        "${CMAKE_CURRENT_BINARY_DIR}/gen"   # for characters.gen.h
    )
```
Also update the comment above it. Replace `# Unlike rune_schema_probe above, view_probe needs no build-time codegen:` with `# view_probe's VIEW legs need no build-time codegen (its Phase 2b C typed_route leg uses characters.gen.h):`.

`Dockerfile.view-probe` needs no change. Its context keeps `tests/generated/` (see `Dockerfile.view-probe.dockerignore`), and it copies `rune-codec/tools` (the `rune_codec_gen` source).

- [ ] **Step 2: Build the probe locally.**

Run: `cmake --build build --target view_probe -j8`
Expected: it builds.

- [ ] **Step 3: Assert the sentinel in the e2e script.** In `rune-broker/docker/view-engine-e2e.sh`, replace:
```bash
    "orderTopicId=${VIEW_ORDER_TOPIC_ID} viewAgg={id=${VIEW_TOPIC_ID_AGG},fp=${VIEW_FP_AGG}}"
```
with:
```bash
    "orderTopicId=${VIEW_ORDER_TOPIC_ID} viewAgg={id=${VIEW_TOPIC_ID_AGG},fp=${VIEW_FP_AGG}}"

# Phase 2b Part C: typed_route (8 partitions) — typed records routed by key.
echo "${setup_out}" | grep -q "^TYPED-ROUTING: OK" \
    || fail "setup client did not print 'TYPED-ROUTING: OK' (typed topics must route by key)"
log "Typed routing OK: $(echo "${setup_out}" | grep -E '^TYPED-ROUTING: OK' | tail -1)"
```
and in the final PASSED banner, after `log "     + post-restart update retracts OK"`, add:
```bash
log "  -> typed_route: 8-partition typed topic routed by key (Phase 2b C)"
```

- [ ] **Step 4: Run the e2e on the VM.** Honor `/root/vm.lock`: claim it with an owner marker and release it afterwards.

```bash
# Stage the four Part C repos (git archive HEAD of each, from se-worktrees/) into
# /root/view-engine-e2e/<repo> on root@178.104.16.102 exactly as the Phase 1/2a
# e2e runs did — broker, protocol, CODEC (new for this part), client — then:
ssh root@178.104.16.102 'cd /root/view-engine-e2e/rune-broker && bash docker/view-engine-e2e.sh'
```
Expected output: `PROBE: typed_route OK — 64 records over N partitions …` (N > 1), `Typed routing OK: TYPED-ROUTING: OK parts=8 records=64 …`, the full `VIEW-ENGINE-E2E: OK`, and `SCRIPT_EXIT=0`. A failure exit code in 40..46 identifies which typed-routing assertion failed.

- [ ] **Step 5: Regression gate, then commit (two repos).**

```bash
# client
cd /Users/vradu/Projects/se-worktrees/rune-client-cpp
ctest --test-dir build -R Regression --output-on-failure
git add examples/view_probe.cpp CMakeLists.txt
git commit -m "test(e2e): view_probe typed_route leg — 8-partition typed topic routes by key (Phase 2b C)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
# broker
cd /Users/vradu/Projects/se-worktrees/rune-broker
ctest --test-dir build-fast -R Regression --output-on-failure
git add docker/view-engine-e2e.sh
git commit -m "test(e2e): assert typed-topic key routing on a real broker (Phase 2b C)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Finish

- [ ] Run the full `*Regression*` suite on the final tree of all four repos. protocol: `ctest --test-dir build -R Regression`. codec: `ctest --test-dir build -R Regression`. client: `ctest --test-dir build -R Regression`. broker: `ctest --test-dir build-fast -R Regression`. Each must report 0 failures (CLAUDE.md #7).
- [ ] Run the full suites (`ctest --test-dir <dir>`) in all four repos: 0 failures.
- [ ] The e2e on the VM is green (Task 6), with `TYPED-ROUTING: OK` and `VIEW-ENGINE-E2E: OK`.
- [ ] Nothing is pushed. The controller merges `feat/phase2b-c-typed-topic-routing` into the integration line of each repo (protocol/broker/client: `feature/materialized-views`; codec: `feat/schema-evolution-durable-tags`) before Part E starts.

## Self-review

- **Spec coverage (§C):** protocol `partitionCount` appended → Task 1. Broker fills it from `RegistryEntry.partitionCount` → Task 2. Client `TopicSchemaResult.partitionCount` → Task 4. `TypedTopic` stores `TopicHandle{id, partitionCount}` → Task 5. Codegen `key()` → Task 3. `send` uses `Key(rec.key())` → Task 5. The no-key schema keeps partition 0 + empty key, documented in `TypedTopic.h` / `Generator.h` → Tasks 3 and 5 (`SchemaWithoutKeyFieldKeepsPartitionZeroAndEmptyKey`). The spec's regression (8-partition topic distributes by key exactly like `Producer::send`) → `TypedTopicRoutingRegression.EightPartitionTypedTopicRoutesLikeProducerSendWithSameKey` (Task 5), plus the real-broker leg (Task 6).
- **Type consistency:** `partitionCount` is `int32_t` everywhere (wire, `RegistryEntry`, `TopicSchemaResult`, `TopicHandle`), except `Completion.reserved0` (`uint32_t`), which is cast both ways at the two documented points. `respondGet` has the parameter order `(session, corr, topicId, version, partitionCount, blob, errorCode)` at every site in Task 2. `TypedTopic(ProducerT, TopicHandle, uint16_t)` is used at every construction site in Tasks 5 and 6.
- **Placeholders:** the only fill-in is the bench medians in the Task 5 commit body, which are measured in Step 5.

## Spec questions

1. **Keys wider than 16 bytes.** `rune::client::Key` stores at most 16 bytes and silently truncates. §C says nothing about this. The plan **fails closed**: a fixed-size key type wider than 16 bytes (for example `string(32)`) is a compile error in `TypedTopic::send`, and a `string`/`bytes` key over 16 bytes throws `std::invalid_argument` without sending. Confirm this, or choose another rule (for example, hash the full key but store it truncated, or lift `Key::kMaxInline`).
2. **A field literally named `key`.** The generated `key()` accessor cannot coexist with a member named `key`. Rune QL treats `key` as a keyword, so this can only arise from the raw descriptor API. The plan makes `generateHeader` throw `std::invalid_argument` for such schemas **when they have a key field**; schemas without a key field are unaffected. Part B's `XEncoder::key()` has the same collision. Confirm this, or choose an accessor-renaming rule.
3. **New client against a pre-2b broker.** §C covers old clients ignoring the new field, but not the reverse. The plan treats "not reported" (0) as 1 partition: records go to partition 0 as before, now with the key set. Confirm this.
4. **How `partitionCount` reaches the token API.** §C names `TopicSchemaResult.partitionCount` but not the `Completion` field. The plan uses the existing `Completion.reserved0` (already used by `BatchAck` for `producerEpoch`), which keeps the 40-byte `Completion` layout. Confirm this.
5. **`keyField` vs `isKey`.** `DefineTopic` carries a `keyField` string, and the descriptor carries `isKey` per field. The broker does not cross-check them. Rune QL sets both consistently, but the raw `defineTopic` API can make them disagree. §C says routing follows `isKey`, and the plan does that. Should DefineTopic reject a mismatch (a separate broker fix)?
6. **Public docs.** "Documented" is met in `TypedTopic.h` and `Generator.h`. The published site page (rune-site `rune/docs.html`) shows only composite-key examples, which are Phase 3, and is not changed. Say whether it needs a sentence about keyless typed topics.
