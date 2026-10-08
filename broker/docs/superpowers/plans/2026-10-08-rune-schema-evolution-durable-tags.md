# Rune Schema Evolution (Durable Tags) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a topic's schema evolve non-additively (ADD/DROP/RENAME COLUMN) via durable never-reused per-topic field tags, versioned descriptors with version-aware decode, a BACKWARD/FORWARD compatibility gate, and AdminWorker-serialized DDL.

**Architecture:** `rune-codec` gains durable field tags (the existing `FieldDescriptor::id` *is* the tag, no longer the positional index), a per-field `untilVersion`, a pure `applyOps`/`checkCompatibility` evolution engine, and `materializeAsOf(descriptor, version)` which recomputes offsets for the fields live at a version so the already-version-aware `readField`/`Validator` work unchanged. `rune-protocol` adds an `AlterTopic` wire op and a fingerprint-selecting `GetTopicSchema`. `rune-broker` makes `SchemaRegistry` multi-version, handles `ALTER` on the single serialized AdminWorker DDL state machine (with optimistic CAS), and fans out per-version descriptors to workers. `rune-ql` adds `ALTER TOPIC` grammar; `rune-client-cpp` adds the CLI verb and binds a typed client to the schema version whose fingerprint it presents.

**Tech Stack:** C++23, CMake, Conan 2, GoogleTest, Google Benchmark, Aeron, Docker. Five sibling repos.

**Spec:** `/Users/vradu/Projects/rune-broker-matviews/docs/superpowers/specs/2026-10-08-rune-schema-evolution-durable-tags-design.md`

## Global Constraints

- **Engineering philosophy (binding):** shared-nothing (no `std::atomic`/mutex/`thread_local`/futures as coordination — the broker's `scripts/check_no_atomics.py` + `check_single_driver_pumper.py` ctest gates enforce this); ring-only cross-thread passing (reuse SPSC `OneToOneByteRing`, no second driver-pumper); zero hot-path/steady-state allocation; test + benchmark everything; a `*Regression*` test on every fix; run the full regression suite first; fix-on-sight.
- **Field identity = `(topicId, tag)`.** `tag` is a per-topic monotonic `uint16` allocated by the registry, **never reused**; retired tags are burned forever. Tags never appear on the record wire (SBE is positional) — they live only in descriptors/offset tables.
- **v1 operations:** `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN`, `SET COMPATIBILITY`. **No `ALTER COLUMN TYPE` (retype)** — a tag's type is fixed for life in v1.
- **Compatibility modes:** `{BACKWARD, FORWARD}`, default **BACKWARD**. v1 gate rule: `DROP` is rejected under `FORWARD`, allowed under `BACKWARD`; `ADD`/`RENAME` pass under both. (`FULL`/`NONE` deferred with retype.)
- **Record wire unchanged:** `[blockLength u16][version u16]` + fixed block from byte 4 + var-section. Each record self-identifies its writer version.
- **Descriptor blob:** bump magic `RCD1 (0x31444352) → RCD2`; the deserializer still reads `RCD1` (interpreting `tag = id`, `untilVersion = ∞`) — lossless migration.
- **Golden fingerprint `0x8FC60841B8667539`** (rune-codec `FixedStrSerializeTest.cpp:28`) **must remain unchanged** for the existing v1 schema — any new absorbed bytes are appended and guarded so additive schemas' fingerprints are byte-stable.
- **DDL is serialized** through the single AdminWorker thread (`DdlState` Idle/AwaitingId/AwaitingCommit, one op in-flight + `pending_` deque). ALTER carries an optional `expectedVersion` for optimistic CAS (mismatch ⇒ conflict reject).
- **Sentinels:** `untilVersion = 0xFFFF` means ∞ (live). `expectedVersion = 0` / `requestedFingerprint = 0` mean "unspecified / latest".
- **Repos & roots:** rune-codec `/Users/vradu/Projects/rune-codec`; rune-protocol `/Users/vradu/Projects/rune-protocol`; rune-broker `/Users/vradu/Projects/rune-broker-matviews`; rune-ql `/Users/vradu/Projects/rune-ql`; rune-client-cpp `/Users/vradu/Projects/rune-client-cpp`. Each repo's tests build the sibling rune-codec via its existing `RUNE_CODEC_ROOT`/sibling-source mechanism.

## AlterOps wire encoding (shared contract — rune-codec is the single source of truth)

Little-endian. Produced by rune-ql (Task 15) and the client (Task 17), consumed by the broker (Task 11). Defined + unit-tested once in rune-codec (Task 5):

```
opsCount        u16
per op:
  opType        u8     // 1=ADD, 2=DROP, 3=RENAME, 4=SET_COMPATIBILITY
  ADD:          nameLen u16 + name bytes, primType u8, fixedLen u16
  DROP:         nameLen u16 + name bytes
  RENAME:       oldLen  u16 + old bytes, newLen u16 + new bytes
  SET_COMPAT:   mode    u8    // 0=BACKWARD, 1=FORWARD
```

## Review Focus

Input classes the spec implies but ordinary task tests might miss; each gets a test pinned to the owning task:

- **Read a record whose field was later dropped** (offset-reclaim correctness): a v1 record decoded against the v3 schema must still yield the field; a v3 record must report it absent. → Task 2 (`materializeAsOf`) + Task 18 (e2e).
- **Concurrent ALTER with stale `expectedVersion`** must be a CAS *conflict rejection*, never a lost update. → Task 11.
- **Truncated/garbage RCD2 blob and truncated AlterOps blob** must be bounds-checked errors, never OOB reads. → Task 3 + Task 5.
- **ADD a column whose name equals a previously-DROPPED column's name** must succeed with a fresh tag (name is an alias), not collide. → Task 6.
- **Golden-fingerprint stability**: the existing additive v1 schema's fingerprint stays `0x8FC60841B8667539` after the tag/`untilVersion`/RCD2 changes (else every existing typed client breaks). → Task 4.

---

# rune-codec (`/Users/vradu/Projects/rune-codec`)

### Task 1: Durable tags + `untilVersion` in the descriptor model

**Files:**
- Modify: `include/rune/codec/Descriptor.h` (FieldDescriptor ~L12-21, DescriptorBuilder ~L42-124)
- Test: `tests/DescriptorTest.cpp`

**Interfaces:**
- Produces: `FieldDescriptor::untilVersion` (`uint16_t`, default `0xFFFF`); free fn `bool isLiveAt(const FieldDescriptor& f, uint16_t v) noexcept`; `DescriptorBuilder::addTagged(name, uint16_t tag, PrimType, uint16_t sinceVersion, uint16_t untilVersion, bool isKey, std::vector<int32_t> enumValues)` and `addFixedStrTagged(name, uint16_t tag, uint16_t len, uint16_t sinceVersion, uint16_t untilVersion, bool isKey)`. Existing `add`/`addFixedStr` keep auto-tag (for fresh v1 schemas) by delegating with `tag = fields_.size()`, `untilVersion = 0xFFFF`.
- Note: `FieldDescriptor::id` is the durable tag. `build()` must NOT reassign `id` for fields added via the `*Tagged` methods — it preserves the provided tag and only computes offsets.

- [ ] **Step 1: Write failing tests**

```cpp
// tests/DescriptorTest.cpp
TEST(Descriptor, UntilVersionDefaultsToInfinity) {
    rune::codec::FieldDescriptor f;
    EXPECT_EQ(f.untilVersion, 0xFFFF);
}
TEST(Descriptor, IsLiveAtRespectsSinceAndUntil) {
    rune::codec::FieldDescriptor f; f.sinceVersion = 2; f.untilVersion = 4;
    EXPECT_FALSE(isLiveAt(f, 1));
    EXPECT_TRUE (isLiveAt(f, 2));
    EXPECT_TRUE (isLiveAt(f, 3));
    EXPECT_FALSE(isLiveAt(f, 4));   // until is exclusive
}
TEST(Descriptor, AddTaggedPreservesExplicitTagThroughBuild) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("a", 7, rune::codec::PrimType::Int32, 1, 0xFFFF, false, {});
    b.addTagged("b", 3, rune::codec::PrimType::Int64, 1, 0xFFFF, false, {});
    auto d = b.build();
    EXPECT_EQ(d.find("a")->id, 7);   // NOT 0
    EXPECT_EQ(d.find("b")->id, 3);   // NOT 1
}
```

- [ ] **Step 2: Run — expect FAIL** (`untilVersion`/`isLiveAt`/`addTagged` undefined).
  `cmake --build build --target rune_codec_tests && ctest --test-dir build -R Descriptor`
- [ ] **Step 3: Implement.** Add `uint16_t untilVersion = 0xFFFF;` to `FieldDescriptor`. Add inline `isLiveAt`. Add `addTagged`/`addFixedStrTagged` that push a `FieldDescriptor` with the explicit `id = tag`. Refactor `build()` so the fixed/var offset passes stay, but it no longer overwrites `id` (the `add`/`addFixedStr` paths already set `id` before `build()`; make `build()` leave `id` as-is). Keep the additive-only invariant check.
- [ ] **Step 4: Run — expect PASS.** Also run the full suite to confirm no regression of existing id-as-index assumptions: `ctest --test-dir build`.
- [ ] **Step 5: Commit** — `git -C /Users/vradu/Projects/rune-codec add -A && git commit -m "feat(codec): durable field tags + untilVersion in descriptor model"`

### Task 2: `materializeAsOf(descriptor, version)` — per-version offsets

**Files:**
- Create: `include/rune/codec/Evolve.h`, `src/Evolve.cpp`
- Modify: `CMakeLists.txt` (add `src/Evolve.cpp` to `rune_codec` lib L6-10; add `tests/EvolveTest.cpp` to the `rune_codec_tests` list L52-66)
- Test: `tests/EvolveTest.cpp`

**Interfaces:**
- Produces: `Descriptor rune::codec::materializeAsOf(const Descriptor& evolving, uint16_t version)` — returns a Descriptor containing only the fields live at `version` (preserving each field's `id`=tag, `type`, `fixedLen`, `sinceVersion`, `isKey`, `enumValues`, with `untilVersion` reset to `0xFFFF` in the view), with offsets/`blockLength` recomputed for exactly that live set in tag-allocation order. `readField(materializeAsOf(d,V), record, tag)` then decodes correctly.

- [ ] **Step 1: Write failing tests** (offset reclaim is the Review-Focus case):

```cpp
// tests/EvolveTest.cpp  — build an evolving descriptor by hand
static rune::codec::Descriptor evolving() {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id",   1, rune::codec::PrimType::Int64, 1, 0xFFFF, true,  {});
    b.addTagged("age",  2, rune::codec::PrimType::Int32, 1, 3,      false, {}); // dropped at v3
    b.addTagged("score",3, rune::codec::PrimType::Int32, 2, 0xFFFF, false, {}); // added at v2
    return b.build();
}
TEST(Evolve, V1HasIdAndAgeNotScore) {
    auto d = rune::codec::materializeAsOf(evolving(), 1);
    EXPECT_NE(d.find("id"), nullptr);
    EXPECT_NE(d.find("age"), nullptr);
    EXPECT_EQ(d.find("score"), nullptr);
    EXPECT_EQ(d.find("age")->offset, 8u);        // after int64 id
    EXPECT_EQ(d.blockLength, 12u);               // 8 + 4
}
TEST(Evolve, V3DropsAgeAndReclaimsItsSlot) {
    auto d = rune::codec::materializeAsOf(evolving(), 3);
    EXPECT_EQ(d.find("age"), nullptr);           // dropped
    EXPECT_NE(d.find("score"), nullptr);
    EXPECT_EQ(d.find("score")->offset, 8u);      // reclaims age's slot
    EXPECT_EQ(d.blockLength, 12u);
}
TEST(Evolve, ReadFieldDecodesOldRecordAgainstItsVersion) {
    // a v1 record (version=1) carrying id + age; decode age via v1 view
    auto v1 = rune::codec::materializeAsOf(evolving(), 1);
    std::vector<uint8_t> rec(4 + 12, 0);
    rec[0]=12; rec[1]=0; rec[2]=1; rec[3]=0;          // blockLength=12, version=1
    int64_t id=42; std::memcpy(&rec[4], &id, 8);
    int32_t age=7; std::memcpy(&rec[12], &age, 4);
    auto fv = rune::codec::readField(v1, rec, /*tag=*/2);
    EXPECT_TRUE(fv.present); EXPECT_EQ(fv.i, 7);
}
```

- [ ] **Step 2: Run — expect FAIL** (undefined `materializeAsOf`).
- [ ] **Step 3: Implement** `materializeAsOf`: copy fields with `isLiveAt(f, version)`, in existing (tag-allocation) order, into a fresh `Descriptor`; recompute fixed offsets then var-indices exactly as `build()` does (factor a shared `computeOffsets(std::vector<FieldDescriptor>&)` helper used by both if convenient); set `version`, `blockLength`.
- [ ] **Step 4: Run — expect PASS.**
- [ ] **Step 5: Commit** — `"feat(codec): materializeAsOf for per-version decode offsets"`

### Task 3: RCD2 blob serialization (durable tag + untilVersion) with RCD1 back-compat

**Files:**
- Modify: `include/rune/codec/Serialize.h` (format doc L12-25), `src/Serialize.cpp` (kMagic L41, serialize L114-159, deserialize L161-222)
- Test: `tests/SerializeTest.cpp`, `tests/FixedStrSerializeTest.cpp`

**Interfaces:**
- Produces: blob magic `kMagicV2 = 0x32444352` ("RCD2"). `serialize()` emits RCD2 with, per field, an explicit `tag u16` (= `id`) immediately after the name, and `untilVersion u16` appended after the existing fields (and after the FixedStr `fixedLen`). `deserialize()` reads either magic: RCD1 → `tag = id` (positional, unchanged), `untilVersion = 0xFFFF`; RCD2 → explicit `tag`, no `id==i` sequence enforcement (relax `Serialize.cpp:208-210`), `untilVersion` as stored. Both build via `addTagged`/`addFixedStrTagged`.

- [ ] **Step 1: Write failing tests**

```cpp
TEST(Serialize, Rcd2RoundTripPreservesTagsAndUntil) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 5, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",2, rune::codec::PrimType::Int32, 1, 3,      false,{});
    auto d = b.build();
    auto blob = rune::codec::serialize(d);
    EXPECT_EQ(blob[0], 0x52); EXPECT_EQ(blob[1], 0x43); EXPECT_EQ(blob[2], 0x44); EXPECT_EQ(blob[3], 0x32); // "RCD2"
    auto d2 = rune::codec::deserialize(blob);
    EXPECT_EQ(d2.find("id")->id, 5);
    EXPECT_EQ(d2.find("age")->untilVersion, 3);
}
TEST(Serialize, ReadsLegacyRcd1AsInfiniteUntilAndPositionalTag) {
    // hand-craft a minimal RCD1 blob or reuse a committed fixture; assert untilVersion==0xFFFF, id==index
}
TEST(Serialize, Rcd2TruncatedBlobThrows) {
    rune::codec::DescriptorBuilder b("t"); b.add("id", rune::codec::PrimType::Int64);
    auto blob = rune::codec::serialize(b.build());
    blob.resize(blob.size()/2);
    EXPECT_THROW(rune::codec::deserialize(blob), std::runtime_error);
}
```

- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** the dual-magic read + RCD2 write; relax the sequential-`id` check only on the RCD2 path; keep all reads bounds-checked.
- [ ] **Step 4: Run — expect PASS**, and confirm the existing RCD1 round-trip + golden tests still pass.
- [ ] **Step 5: Commit** — `"feat(codec): RCD2 blob with durable tags + untilVersion, RCD1 back-compat"`

### Task 4: Version-aware fingerprint (`fingerprintAsOf`) with golden stability

**Files:**
- Modify: `include/rune/codec/Fingerprint.h` (absorb loop L70-87)
- Test: `tests/FingerprintTest.cpp`, `tests/FixedStrSerializeTest.cpp` (golden L28)

**Interfaces:**
- Produces: `uint64_t rune::codec::fingerprintAsOf(const Descriptor& evolving, uint16_t version)` = `fingerprint(materializeAsOf(evolving, version))`. `fingerprint(Descriptor)` is extended to absorb `untilVersion` **only when it is not `0xFFFF`** (guarded, appended last), so additive schemas (all `untilVersion == ∞`) hash identically to today.

- [ ] **Step 1: Write failing tests**

```cpp
TEST(Fingerprint, GoldenUnchangedForAdditiveSchema) {
    rune::codec::DescriptorBuilder b("chars");
    b.add("id", rune::codec::PrimType::Int64);
    b.add("userId", rune::codec::PrimType::Int64, 1, true);
    b.add("name", rune::codec::PrimType::String);
    EXPECT_EQ(rune::codec::fingerprint(b.build()), 0x8FC60841B8667539ULL);
}
TEST(Fingerprint, DiffersAcrossVersionsWhenFieldDropped) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id",  1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age", 2, rune::codec::PrimType::Int32, 1, 3,      false,{});
    auto ev = b.build();
    EXPECT_NE(rune::codec::fingerprintAsOf(ev, 1), rune::codec::fingerprintAsOf(ev, 3));
}
```

- [ ] **Step 2: Run — expect FAIL.**
- [ ] **Step 3: Implement** the guarded `untilVersion` absorption + `fingerprintAsOf`.
- [ ] **Step 4: Run — expect PASS** incl. the existing golden tests and generated-header fp tests.
- [ ] **Step 5: Commit** — `"feat(codec): fingerprintAsOf, golden-stable for additive schemas"`

### Task 5: AlterOp encode/decode (shared wire contract)

**Files:**
- Create: `include/rune/codec/AlterOps.h`, `src/AlterOps.cpp`
- Modify: `CMakeLists.txt` (lib + `tests/AlterOpsTest.cpp`)
- Test: `tests/AlterOpsTest.cpp`

**Interfaces:**
- Produces: `enum class AlterOpType : uint8_t { Add=1, Drop=2, Rename=3, SetCompatibility=4 };` `enum class CompatMode : uint8_t { Backward=0, Forward=1 };` `struct AlterOp { AlterOpType type; std::string name; std::string newName; PrimType primType; uint16_t fixedLen; CompatMode mode; };` `std::vector<uint8_t> encodeAlterOps(const std::vector<AlterOp>&);` `std::optional<std::vector<AlterOp>> decodeAlterOps(std::span<const uint8_t>);` — per the "AlterOps wire encoding" section above; `decode` returns `nullopt` on truncation (bounds-checked).

- [ ] **Step 1: Write failing tests** — round-trip each op type; a truncated-blob decode returns `nullopt`.

```cpp
TEST(AlterOps, RoundTripAllOps) {
    using namespace rune::codec;
    std::vector<AlterOp> ops = {
        {AlterOpType::Add, "score", "", PrimType::Int32, 0, CompatMode::Backward},
        {AlterOpType::Drop, "age", "", PrimType::Int32, 0, CompatMode::Backward},
        {AlterOpType::Rename, "name", "fullName", PrimType::Int32, 0, CompatMode::Backward},
        {AlterOpType::SetCompatibility, "", "", PrimType::Int32, 0, CompatMode::Forward},
    };
    auto blob = encodeAlterOps(ops);
    auto got = decodeAlterOps(blob);
    ASSERT_TRUE(got.has_value());
    ASSERT_EQ(got->size(), 4u);
    EXPECT_EQ((*got)[2].newName, "fullName");
    EXPECT_EQ((*got)[3].mode, CompatMode::Forward);
}
TEST(AlterOps, TruncatedReturnsNullopt) {
    auto blob = rune::codec::encodeAlterOps({{rune::codec::AlterOpType::Drop,"age","",{},0,{}}});
    blob.resize(2);
    EXPECT_FALSE(rune::codec::decodeAlterOps(blob).has_value());
}
```

- [ ] **Step 2-4:** Run FAIL → implement the LE encode/decode → run PASS.
- [ ] **Step 5: Commit** — `"feat(codec): AlterOps wire encode/decode"`

### Task 6: Evolution engine — `applyOps` + `checkCompatibility` (pure)

**Files:**
- Modify: `include/rune/codec/Evolve.h`, `src/Evolve.cpp`
- Test: `tests/EvolveTest.cpp`

**Interfaces:**
- Consumes: `materializeAsOf` (Task 2), `AlterOp`/`CompatMode` (Task 5).
- Produces:
  - `struct EvolveError { std::string message; };`
  - `std::expected<void, EvolveError> checkCompatibility(const std::vector<AlterOp>& ops, CompatMode mode)` — returns error iff `mode==Forward` and any op is `Drop`.
  - `std::expected<Descriptor, EvolveError> applyOps(const Descriptor& currentEvolving, const std::vector<AlterOp>& ops, uint16_t& nextTag, uint16_t newVersion)` — applies ops against the cumulative intermediate state: ADD → `addTagged(name, nextTag++, type, sinceVersion=newVersion, untilVersion=∞)` (reject if name collides with a **live** field; a retired name is allowed); DROP → set the live field's `untilVersion = newVersion` (reject if absent/already-dropped, or if it is the key field, or the last live field); RENAME → rename a live field (reject if absent, or new name collides with a live field). Returns the new evolving `Descriptor` (all fields, live + retired) or the first error.

- [ ] **Step 1: Write failing tests** (the retired-name-reuse Review-Focus case is here):

```cpp
TEST(Evolve, CheckCompatibilityRejectsDropUnderForward) {
    std::vector<rune::codec::AlterOp> ops = {{rune::codec::AlterOpType::Drop,"age","",{},0,{}}};
    EXPECT_FALSE(rune::codec::checkCompatibility(ops, rune::codec::CompatMode::Forward).has_value());
    EXPECT_TRUE (rune::codec::checkCompatibility(ops, rune::codec::CompatMode::Backward).has_value());
}
TEST(Evolve, AddAllocatesFreshTagAndDropRetiresTag) {
    rune::codec::DescriptorBuilder b("t");
    b.addTagged("id", 1, rune::codec::PrimType::Int64, 1, 0xFFFF, true, {});
    b.addTagged("age",2, rune::codec::PrimType::Int32, 1, 0xFFFF, false,{});
    auto cur = b.build(); uint16_t nextTag = 3;
    auto d1 = rune::codec::applyOps(cur, {{rune::codec::AlterOpType::Drop,"age","",{},0,{}}}, nextTag, 2);
    ASSERT_TRUE(d1.has_value());
    EXPECT_EQ(d1->find("age")->untilVersion, 2);     // retired at v2
    auto d2 = rune::codec::applyOps(*d1, {{rune::codec::AlterOpType::Add,"age","",rune::codec::PrimType::Int64,0,{}}}, nextTag, 3);
    ASSERT_TRUE(d2.has_value());
    // retired name reused -> FRESH tag 3, old retired tag 2 still present & retired
    EXPECT_EQ(nextTag, 4);
    EXPECT_EQ(std::count_if(d2->fields.begin(), d2->fields.end(), [](auto&f){return f.name=="age";}), 2);
}
TEST(Evolve, DropKeyFieldRejected) {
    rune::codec::DescriptorBuilder b("t"); b.addTagged("id",1,rune::codec::PrimType::Int64,1,0xFFFF,true,{});
    auto cur=b.build(); uint16_t nt=2;
    EXPECT_FALSE(rune::codec::applyOps(cur, {{rune::codec::AlterOpType::Drop,"id","",{},0,{}}}, nt, 2).has_value());
}
```

- [ ] **Step 2-4:** Run FAIL → implement → run PASS.
- [ ] **Step 5: Commit** — `"feat(codec): applyOps + checkCompatibility evolution engine"`

---

# rune-protocol (`/Users/vradu/Projects/rune-protocol`)

### Task 7: `AlterTopic`/`AlterTopicResponse` wire ops

**Files:**
- Modify: `include/rune/protocol/client/Protocol.h` (TemplateId L62-65, ErrorCode L96-98), `include/rune/protocol/client/SchemaMessages.h` (copy DefineTopic L14-57 / DefineTopicResponse L60-87), `include/rune/protocol/client/Dispatch.h` (range L94-98)
- Test: `tests/protocol/client/SchemaMessagesTest.cpp`, `SchemaConformanceTest.cpp`, `DispatchTest.cpp`

**Interfaces:**
- Produces: `TemplateId::AlterTopic = 68`, `AlterTopicResponse = 69`; `ErrorCode::SchemaIncompatible = 29`, `SchemaConflict = 30`. `struct AlterTopic { int64_t sessionId, correlationId; std::string_view topicName; uint16_t expectedVersion; const uint8_t* opsBlob; uint32_t opsLen; };` (`BODY_FIXED = 2` for `expectedVersion`; then `putVarStr(topicName)`, `putVarBytes32(opsBlob,opsLen)`). `struct AlterTopicResponse { int64_t sessionId, correlationId; int32_t topicId; uint16_t newVersion; uint16_t errorCode; };` (`BODY_FIXED = 8`). Both mirror DefineTopic's `encode`/`decode`/`encodedSize`.

- [ ] **Step 1:** Add round-trip + truncation tests in `SchemaMessagesTest.cpp` (copy `DefineTopicRoundTrip` 9-36 / `DefineTopicResponseRoundTrip` 38-50), a `kAlterTopicGolden`/`kAlterTopicResponseGolden` with byte-layout comments in `SchemaConformanceTest.cpp` (copy the DefineTopic golden pattern), and update `DispatchTest.cpp`: `UnknownTemplateSixtyEight` (L725) becomes `KnownTemplateSixtyEight`/`SixtyNine`, move the first-unassigned boundary to 70.
- [ ] **Step 2:** Run — expect FAIL / the old `UnknownTemplateSixtyEight` now wrong.
  `cmake -S . -B build -DBUILD_TESTING=ON && cmake --build build --target rune_protocol_tests && ctest --test-dir build -R 'Schema|Dispatch'`
- [ ] **Step 3:** Add the enum values, the two structs, widen `isKnownTemplate` schema range upper bound to `AlterTopicResponse` (update the `// 64..67` comment to `64..69`).
- [ ] **Step 4:** Run — expect PASS.
- [ ] **Step 5: Commit** — `git -C /Users/vradu/Projects/rune-protocol add -A && git commit -m "feat(protocol): AlterTopic/AlterTopicResponse client wire ops"`

### Task 8: Fingerprint-selecting `GetTopicSchema`

**Files:**
- Modify: `include/rune/protocol/client/SchemaMessages.h` (GetTopicSchema L89-114, TopicSchemaResponse L117-149)
- Test: `tests/protocol/client/SchemaMessagesTest.cpp`, `SchemaConformanceTest.cpp`

**Interfaces:**
- Produces: `GetTopicSchema` gains `uint64_t requestedFingerprint` (0 = latest) → `BODY_FIXED = 8` (was 0), encoded before the `topicName` var field. `TopicSchemaResponse` is unchanged on the wire (it already returns `topicId`, `version`, `descriptorBlob`); `version` is now the *bound* version for the returned blob.

- [ ] **Step 1:** Update `GetTopicSchemaRoundTrip`/golden to carry a non-zero `requestedFingerprint`; add a decode-truncation assertion.
- [ ] **Step 2-4:** Run FAIL → add the field + `BODY_FIXED`/encode/decode/encodedSize → run PASS (recompute the golden byte array).
- [ ] **Step 5: Commit** — `"feat(protocol): fingerprint-selecting GetTopicSchema"`

---

# rune-broker (`/Users/vradu/Projects/rune-broker-matviews`)

### Task 9: Multi-version `SchemaRegistry` + per-topic tag allocator / compat mode

**Files:**
- Modify: `libs/node/include/rune/node/SchemaRegistry.h` (TopicSchema L22-32, RegistryEntry L35-44, applyRecord L54, byId_/nameToId_), `libs/node/src/SchemaRegistry.cpp` (serialize L84-134, applyRecord L139-164, fingerprint L144)
- Test: `libs/node/tests/node/SchemaRegistryTest.cpp`

**Interfaces:**
- Produces: `TopicSchema` gains `uint16_t nextTag` and `uint8_t compatMode` (serialized, appended after the existing fields so RCD-style back-compat holds; default `compatMode=0` BACKWARD, `nextTag = fieldCount` for migrated single-version topics). `SchemaRegistry` keeps **all versions**: `byId_` becomes `topicId → std::vector<RegistryEntry>` (index by `version`, 1-based) or `topicId → map<uint16_t,RegistryEntry>`; add `const RegistryEntry* byIdVersion(int32_t topicId, uint16_t version)`, `const RegistryEntry* latest(int32_t topicId)`, `const RegistryEntry* byFingerprint(int32_t topicId, uint64_t fp)`, and `uint16_t nextTagFor(int32_t topicId)`. `applyRecord` **appends** a version instead of overwriting (`SchemaRegistry.cpp:154`). Each entry stores the **materialized-as-of-its-version** descriptor + `fingerprintAsOf`.

- [ ] **Step 1: Write failing tests**

```cpp
TEST(SchemaRegistry, KeepsMultipleVersionsAndLooksUpByFingerprint) {
    rune::node::SchemaRegistry reg;
    reg.applyRecord(makeTopicSchema(/*topicId=*/1, /*version=*/1, evolvingV1()));
    reg.applyRecord(makeTopicSchema(1, 2, evolvingV2()));
    ASSERT_NE(reg.byIdVersion(1,1), nullptr);
    ASSERT_NE(reg.byIdVersion(1,2), nullptr);
    EXPECT_EQ(reg.latest(1)->version, 2);
    auto fp1 = reg.byIdVersion(1,1)->fingerprint;
    EXPECT_EQ(reg.byFingerprint(1, fp1)->version, 1);   // old fingerprint resolves to v1
}
```

- [ ] **Step 2-4:** Run FAIL → implement multi-version storage + the new accessors + serialize the two new TopicSchema fields → run PASS. Keep single-writer, no atomics (admin-thread-owned).
- [ ] **Step 5: Commit** — `git -C /Users/vradu/Projects/rune-broker-matviews add -A && git commit -m "feat(broker): multi-version SchemaRegistry + tag allocator + compat mode"`

### Task 10: Un-hardcode schema version in the DefineTopic path

**Files:**
- Modify: `libs/node/src/AdminWorker.cpp` (`ts.version = 1` L97, `respondDefine(... version=1 ...)` L221/L230)
- Test: `libs/node/tests/node/AdminWorkerTest.cpp`

**Interfaces:**
- Consumes: Task 9. Produces: DefineTopic creates version **1** explicitly via a named constant `kInitialSchemaVersion = 1` and `nextTag = fieldCount`; the create path now routes through the same version-assignment helper ALTER will use (`version = prevLatest ? prevLatest+1 : 1`). No external behavior change for create (still v1), but the hardcode becomes a single source of truth.

- [ ] **Step 1:** Add a test asserting a freshly-defined topic reports `version==1` and `nextTag == fieldCount` via the registry.
- [ ] **Step 2-4:** Run FAIL → replace the literals with the helper/constant → run PASS.
- [ ] **Step 5: Commit** — `"refactor(broker): single source of truth for schema version assignment"`

### Task 11: `ALTER` on the AdminWorker (serialized, CAS, compat gate, atomic multi-op)

**Files:**
- Modify: `libs/node/include/rune/node/AdminWorker.h` (AdminDeps L41-59, submit* L89-90, DdlState L129-133, DdlOp L136-143), `libs/node/src/AdminWorker.cpp` (startNext_ L158-198, advance_ L201-238), `libs/node/include/rune/node/AdminChannels.h` (msg ids L50-57, encode/decode L157-301, dispatch L357-375)
- Test: `libs/node/tests/node/AdminWorkerTest.cpp`, `libs/node/tests/integration/SchemaGatewayTest.cpp`

**Interfaces:**
- Consumes: codec `decodeAlterOps`/`applyOps`/`checkCompatibility`/`materializeAsOf`/`fingerprintAsOf` (Tasks 2-6), multi-version registry (Task 9). Produces: `AdminWorker::submitAlterTopic(const AlterTopicMsg&)`; `AdminChannels` gains `kMsgAlterTopic` + `AlterTopicMsg {sessionId, correlationId, topicName, expectedVersion, opsBlob...}` + encode/decode; `AdminDeps` gains `respondAlter(sessionId, correlationId, topicId, newVersion, errorCode)`. The op reuses the `DdlState` machine (so it is serialized with CREATE and other ALTERs), but **skips `AwaitingId`** (topic exists): `startNext_` for an ALTER does, on the admin thread:
  1. resolve `reg_.byName(topicName)` → `UnknownTopic` if missing;
  2. **CAS**: if `expectedVersion != 0 && expectedVersion != latest.version` → respond `SchemaConflict`, done;
  3. `decodeAlterOps(opsBlob)` → `InvalidSchema` on failure;
  4. the `SetCompatibility` op (if present) updates the mode used for the gate; `checkCompatibility(ops, mode)` → `SchemaIncompatible` on failure;
  5. `applyOps(latest.evolving, ops, nextTag, latest.version+1)` → `InvalidSchema` (message) on failure — **atomic**: any op failing aborts the whole statement;
  6. build a new `TopicSchema` (same `topicId`, `version = latest.version+1`, new `nextTag`, new `compatMode`, serialized new evolving descriptor), `appendKeyed` on the `topicId` key → `AwaitingCommit`;
  7. `advance_`: HW-gate, `reg_.applyRecord`, fan out (Task 12), `respondAlter(... newVersion, OK)`.

- [ ] **Step 1: Write failing tests** (CAS conflict is a Review-Focus case):

```cpp
// AdminWorkerTest.cpp — drive the worker directly
TEST(AdminWorker, AlterAddColumnBumpsVersionAndAllocatesTag) { /* submit define v1, then alter ADD, pump, assert registry latest.version==2 and new field tag==fieldCount */ }
TEST(AdminWorker, AlterDropUnderForwardRejected) { /* set compat FORWARD, submit ALTER DROP, assert respondAlter errorCode==SchemaIncompatible and version unchanged */ }
TEST(AdminWorker, AlterWithStaleExpectedVersionConflicts) {
    // define v1; apply ALTER (no expected) -> v2; now submit ALTER with expectedVersion=1
    // assert errorCode==SchemaConflict and registry latest still v2 (no lost update)
}
TEST(AdminWorker, MultiOpAlterIsAtomicOnFailure) { /* ADD validFirst + DROP missingField -> whole statement rejected, version unchanged */ }
```

- [ ] **Step 2-4:** Run FAIL → implement the ring msg + submit path + the ALTER branch in the state machine (reuse `inFlight_`/`pending_` for serialization; no new thread, no atomics — keep `check_no_atomics.py`/`check_single_driver_pumper.py` green) → run PASS. Run the full regression suite first: `ctest --test-dir build -R Regression` then the node suite.
- [ ] **Step 5: Commit** — `"feat(broker): serialized ALTER TOPIC on AdminWorker with CAS + compat gate"`

### Task 12: Per-version descriptor fan-out + version-aware worker validation

**Files:**
- Modify: `libs/node/include/rune/node/SchemaFanout.h` (post L31, pump L37-68), `libs/node/include/rune/node/WorkerOp.h` (SchemaUpdate L90,296-304), `libs/node/include/rune/node/Worker.h` (validators_ L4157, executeSchemaUpdateWire_ L2391-2415, schemaRejects_ L2646-2672)
- Test: `libs/node/tests/node/WorkerReplicationValidatorTest.cpp`, `libs/node/tests/integration/ProduceValidationTest.cpp`

**Interfaces:**
- Consumes: Task 11. Produces: `SchemaFanout::post(topicId, version, blob)` and the `SchemaUpdate` `WorkerOp` carry `version`. Worker `validators_` is keyed by `(topicId, version)` (`std::map<std::pair<int32_t,uint16_t>, rune::codec::Validator>` or `unordered_map` with a packed key); `executeSchemaUpdateWire_` installs a `Validator` built from the materialized-as-of-version descriptor; `schemaRejects_` reads the record's version (bytes [2..3]) and selects `validators_[(topicId, recVersion)]` (falls back to reject "unknown schema version" if absent).

- [ ] **Step 1: Write failing test** (the drop-then-read Review-Focus case at the worker level):

```cpp
TEST(WorkerValidator, ValidatesRecordAgainstItsOwnSchemaVersion) {
    // install v1 validator (id+age) and v2 validator (id+score, age dropped)
    // a record stamped version=1 with age validates OK; a record stamped version=2 with score validates OK
    // a v2 record still carrying the dropped age layout is rejected
}
```

- [ ] **Step 2-4:** Run FAIL → rekey the cache + thread `version` through fan-out + version-select in `schemaRejects_` → run PASS. Keep the produce hot path zero-alloc (the lookup is a map/array hit; preallocate).
- [ ] **Step 5: Commit** — `"feat(broker): version-keyed worker schema validators + versioned fan-out"`

### Task 13: Version-selecting `GetTopicSchema` read path

**Files:**
- Modify: `libs/node/src/AdminWorker.cpp` (submitGetSchema L66-75), `libs/node/include/rune/node/AdminWorker.h` (respondGet L56, GetSchemaMsg), `libs/node/src/ClientGateway.cpp` (handleGetTopicSchema L5069)
- Test: `libs/node/tests/integration/SchemaGatewayTest.cpp`

**Interfaces:**
- Consumes: Task 9 (`byFingerprint`), Task 8 (protocol `requestedFingerprint`). Produces: `submitGetSchema` honors `requestedFingerprint`: `0` → `reg_.latest(topicId)`; non-zero → `reg_.byFingerprint(topicId, fp)` (→ `UnknownTopic`/not-found error if no version matches). Responds with that entry's `topicId`, `version` (the bound version), and `descriptorBlob` (the materialized-as-of-version blob).

- [ ] **Step 1:** Integration test: define v1, ALTER to v2, `GetTopicSchema(requestedFingerprint = fp(v1))` returns `version==1` + v1 blob; `requestedFingerprint=0` returns v2.
- [ ] **Step 2-4:** Run FAIL → thread `requestedFingerprint` from gateway → `submitGetSchema` → run PASS.
- [ ] **Step 5: Commit** — `"feat(broker): fingerprint-selecting GetTopicSchema read path"`

### Task 14: Gateway routing for the `AlterTopic` frame

**Files:**
- Modify: `libs/node/src/ClientGateway.cpp` (frame routing L876-877,928-933; new `handleAlterTopic` near `handleDefineTopic` L5035), the AdminWorkerAgent ingress (`submitAlterTopic` onto `requestRing_`)
- Test: `libs/node/tests/integration/SchemaGatewayTest.cpp`

**Interfaces:**
- Consumes: Task 7 (`AlterTopic` decode), Task 11 (`AdminWorker::submitAlterTopic`). Produces: a decoded `AlterTopic` frame is placed on the admin `requestRing_` (as `kMsgAlterTopic`), and the `AlterTopicResponse` is encoded back to the client via the existing `respond*`/egress path used by `respondDefine`.

- [ ] **Step 1:** Integration test: an `AlterTopic` frame through the gateway rings yields an `AlterTopicResponse` with the new version (mirror `SchemaGatewayTest` DefineTopic round-trip).
- [ ] **Step 2-4:** Run FAIL → wire `handleAlterTopic` + ring ingress + response egress → run PASS.
- [ ] **Step 5: Commit** — `"feat(broker): gateway routing for AlterTopic frames"`

### Task 15: Boot replay & follower rebuild keep all versions

**Files:**
- Modify: `libs/node/src/BrokerNode.cpp` (admin log replay L2867-2930), `libs/node/src/AdminWorker.cpp` (`rebuildFromLog_` L241-278)
- Test: `libs/node/tests/integration/SchemaPersistenceTest.cpp`

**Interfaces:**
- Consumes: Task 9/11. Produces: replaying `__topic_schemas` `[0,HW)` applies **every** `TopicSchema` record in order, so the multi-version registry (and `nextTag`/`compatMode`) is reconstructed exactly — a topic with v1,v2,v3 records replays to three registry versions, latest=3.

- [ ] **Step 1: Write failing test** (crash/restart recovery):

```cpp
TEST(SchemaPersistence, ReplayReconstructsAllVersionsAndNextTag) {
    // append define v1 + alter-derived v2 + v3 records to __topic_schemas, rebuild, assert
    // registry has versions {1,2,3}, latest==3, nextTag and compatMode preserved
}
```

- [ ] **Step 2-4:** Run FAIL → ensure replay calls `applyRecord` per record (append semantics from Task 9) and does not collapse versions → run PASS.
- [ ] **Step 5: Commit** — `"feat(broker): multi-version schema recovery on replay/rebuild"`

---

# rune-ql (`/Users/vradu/Projects/rune-ql`)

### Task 16: `ALTER TOPIC` grammar + AST

**Files:**
- Modify: `include/rune/ql/Ast.h` (add AST L6-10), `include/rune/ql/Parser.h` (parse L6), `src/Parser.cpp` (parseStatement dispatch L216-221)
- Test: `tests/ParserTest.cpp`

**Interfaces:**
- Produces: `enum class Compatibility { Backward, Forward };` `struct AlterOp { AlterOpKind kind; FieldDef field; std::string name; std::string newName; Compatibility mode; };` (`AlterOpKind{Add,Drop,Rename,SetCompatibility}`), `struct AlterTopicDef { std::string name; std::vector<AlterOp> ops; };`, and the parse entry widens to `std::expected<std::variant<TopicDef, AlterTopicDef>, ParseError> parse(std::string_view)`. The statement dispatcher branches on the first identifier: `topic` → existing path (wrapped in the variant), `alter` → new path.

- [ ] **Step 1: Write failing tests**

```cpp
TEST(Parser, AlterTopicAddDropRename) {
    auto r = rune::ql::parse("alter topic users add column score int32, drop column age, rename column name to fullName;");
    ASSERT_TRUE(r.has_value());
    auto* a = std::get_if<rune::ql::AlterTopicDef>(&*r);
    ASSERT_NE(a, nullptr);
    ASSERT_EQ(a->ops.size(), 3u);
    EXPECT_EQ(a->ops[0].kind, rune::ql::AlterOpKind::Add);
    EXPECT_EQ(a->ops[2].newName, "fullName");
}
TEST(Parser, AlterSetCompatibility) {
    auto r = rune::ql::parse("alter topic users set compatibility forward;");
    ASSERT_TRUE(r.has_value());
    auto* a = std::get_if<rune::ql::AlterTopicDef>(&*r);
    ASSERT_NE(a, nullptr);
    EXPECT_EQ(a->ops[0].mode, rune::ql::Compatibility::Forward);
}
TEST(Parser, ExistingTopicStatementStillParses) {
    auto r = rune::ql::parse("topic users { id int64 } partitions 4 key id;");
    ASSERT_TRUE(r.has_value());
    EXPECT_NE(std::get_if<rune::ql::TopicDef>(&*r), nullptr);
}
```

- [ ] **Step 2-4:** Run FAIL → widen `parse()` return to the variant, add the `alter` statement parser (reuse `parseType` for ADD COLUMN; text-match `add`/`drop`/`rename`/`column`/`to`/`set`/`compatibility`/`backward`/`forward`; `,`-separated ops; `;` + EOF guard), update `Backends.cpp`/`tests` call sites for the new return type → run PASS.
- [ ] **Step 5: Commit** — `git -C /Users/vradu/Projects/rune-ql add -A && git commit -m "feat(ql): ALTER TOPIC grammar + AST"`

### Task 17: `AlterTopicPlan` backend (emit AlterOps blob)

**Files:**
- Modify: `include/rune/ql/Backends.h` (add plan + fn, after L17), `src/Backends.cpp` (after applyPlan L19-26)
- Test: `tests/BackendsTest.cpp`

**Interfaces:**
- Consumes: codec `AlterOp`/`encodeAlterOps` (Task 5), `AlterTopicDef` (Task 16). Produces: `struct AlterTopicPlan { std::string name; uint16_t expectedVersion; std::vector<uint8_t> opsBlob; };` and `AlterTopicPlan applyAlterPlan(const AlterTopicDef&, uint16_t expectedVersion = 0);` — maps each `rune::ql::AlterOp` to a `rune::codec::AlterOp` and `encodeAlterOps`.

- [ ] **Step 1:** Test that `applyAlterPlan(parse(...))` yields an `opsBlob` that `rune::codec::decodeAlterOps` round-trips to the same ops.
- [ ] **Step 2-4:** Run FAIL → implement the mapping → run PASS.
- [ ] **Step 5: Commit** — `"feat(ql): AlterTopicPlan backend emitting AlterOps blob"`

---

# rune-client-cpp (`/Users/vradu/Projects/rune-client-cpp`)

### Task 18: `Client::alterTopic*` + fingerprint-selecting `getTopicSchema`

**Files:**
- Modify: `include/rune/client/Client.h` (DDL methods L431-435, *AndWait L454-465, result structs L83-94)
- Test: `tests/client/ClientSchemaTest.cpp`

**Interfaces:**
- Consumes: protocol Task 7/8. Produces: `AlterTopicResult { int32_t topicId; uint16_t newVersion; uint16_t errorCode; }`; `Client::alterTopic(name, expectedVersion, opsBlob span)` + `alterTopicAndWait(...)`; `getTopicSchemaAndWait(name, uint64_t requestedFingerprint = 0)` overload threading `requestedFingerprint` onto the `GetTopicSchema` frame.

- [ ] **Step 1:** Loopback test (mirror existing schema loopback): an `AlterTopic` frame is encoded and an injected `AlterTopicResponse` is surfaced as `AlterTopicResult`; `getTopicSchemaAndWait(name, fp)` sends the fingerprint.
- [ ] **Step 2-4:** Run FAIL → add the methods + wire encode/decode → run PASS.
- [ ] **Step 5: Commit** — `git -C /Users/vradu/Projects/rune-client-cpp add -A && git commit -m "feat(client): alterTopic + fingerprint-selecting getTopicSchema"`

### Task 19: Version-binding typed gate (`openTopic`/`TypedTopic` stamp bound version)

**Files:**
- Modify: `include/rune/client/Client.h` (openTopicAndWait L471-483), `include/rune/client/TypedTopic.h` (ctor L80-83, fromSchema L100-112, send L127-144, members L153-157)
- Test: `tests/client/OpenTopicGateTest.cpp`

**Interfaces:**
- Consumes: Task 18. Produces: `openTopicAndWait<T>` calls `getTopicSchemaAndWait(name, T::kFingerprint)`; if the broker returns a matching version it binds `TypedTopic<T>(producer(), topicId, boundVersion)` (new `uint16_t version_` member); otherwise it throws "schema stale". `TypedTopic::send` stamps `version_` into the encoded record bytes `[2..3]` (overwriting the codegen-baked `kVersion`) before routing to the producer — so a client compiled against an older version writes records stamped with that bound version.

- [ ] **Step 1: Write failing test** (version binding against a non-latest version):

```cpp
TEST(OpenTopicGate, BindsToVersionWhoseFingerprintMatchesAndStamps) {
    // loopback gateway: topic is at v2; GetTopicSchema(requestedFingerprint = StubV1::kFingerprint)
    // returns {version=1, v1 blob}. openTopicAndWait<StubV1> binds version_==1.
    // send(rec) -> captured producer bytes have [2..3]==1 (not StubV1::kVersion if it differed).
}
TEST(OpenTopicGate, StaleFingerprintStillThrows) {
    // GetTopicSchema returns not-found for an unknown fingerprint -> openTopicAndWait throws stale
}
```

- [ ] **Step 2-4:** Run FAIL → add `version_` + stamp in `send()` + bind in `openTopicAndWait`/`fromSchema` → run PASS.
- [ ] **Step 5: Commit** — `"feat(client): version-binding typed gate + record version stamping"`

### Task 20: CLI `rune schema alter` + extend `apply`

**Files:**
- Modify: `src/cli/Cli.cpp` (cmdSchema router L482-513, printUsage L84-97; new `cmdSchemaAlter` near cmdSchemaApply L169-260)
- Test: `tests/RuneCliOfflineTest.cpp` (offline parse/plan), `tests/RuneCliLiveTest.cpp` (live, gated)

**Interfaces:**
- Consumes: ql Task 16/17, client Task 18. Produces: `rune schema alter <file.rql> [--broker ...] [--expected-version N]` → parse the `alter topic` statement, `applyAlterPlan`, call `client.alterTopicAndWait(name, expectedVersion, opsBlob)`; prints `newVersion` or the error (SchemaIncompatible/SchemaConflict messages). `apply` is extended to accept a file whose statement is `alter topic` and route it the same way (a `topic` statement keeps the existing `defineTopicAndWait` path). `printUsage` advertises `alter`.

- [ ] **Step 1:** Offline test: `rune schema alter` with `--dry-run` on an `.rql` alter file prints the parsed ops; a `topic` file through `apply` still defines.
- [ ] **Step 2-4:** Run FAIL → add the `alter` branch + `cmdSchemaAlter` + the `apply` variant-dispatch on `parse()`'s result → run PASS.
- [ ] **Step 5: Commit** — `"feat(client): rune schema alter + apply for ALTER TOPIC"`

---

# End-to-end

### Task 21: Real-Aeron schema-evolution e2e

**Files:**
- Create: `/Users/vradu/Projects/rune-broker-matviews/docker/schema-evolution-e2e.sh` (model on `docker/schema-e2e.sh`), reuse `docker/docker-compose.single.yml` and the probe image from `../rune-client-cpp`
- Modify (if a new probe is needed): a probe that opens a topic with an older generated header

**Interfaces:** full-stack, default config.

- [ ] **Step 1: Write the e2e script** asserting, against a single real-Aeron broker:
  1. `rune schema apply` defines topic v1; produce typed v1 records; consume/decode OK.
  2. `rune schema alter … add column` → v2; produce v2 records; consume both v1+v2 from the log, each decoding per its own version.
  3. `rune schema alter … drop column` under default BACKWARD succeeds; old records still decode the dropped field; new records omit it.
  4. `rune schema alter … rename column` → decode byte-identical; new name resolves.
  5. `rune schema alter … set compatibility forward` then a `drop column` is **rejected** by the broker (non-zero exit, `SchemaIncompatible`).
  6. A client built against the **v1** generated header still `openTopic`s successfully (binds to v1 by fingerprint) after the topic has evolved to v3.
  7. Two `alter` calls with the same `--expected-version` → the second returns `SchemaConflict`.
- [ ] **Step 2: Run it** (firsthand, default config): `bash docker/schema-evolution-e2e.sh`. Expected: all assertions pass, script exits 0.
- [ ] **Step 3: Commit** — `"test(broker): real-Aeron schema-evolution e2e"`

---

## Regression & benchmark coverage (applies across tasks)

- Every bug fixed during execution adds a `*Regression*` test in the owning repo's suite; the full regression suite runs **first** on every change.
- Benchmarks (add/extend in the owning repo's `benchmark/` dir): `materializeAsOf` + `readField`-by-tag decode cost (O(1), zero-alloc); worker `schemaRejects_` version-select adds no steady-state allocation; `ALTER` latency measured on the AdminWorker (off the produce/consume hot path); produce/consume ABAB regression (n ≥ 15) confirming zero hot-path cost when no DDL is in flight.
