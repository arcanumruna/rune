# Rune Phase 2b — Part B: Zero-Copy Produce Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Updated 2026-10-19** — re-checked against the code after catalog history (`feat/catalog-history`) and the authentication plan. Order is now **auth → D → B → F → A**. Changes:
> 1. **Part C is in the code** (verified: `HasRecordKey`, `typedPartitionCount`, `TypedTopic{producer_, topic_, version_}`, `send(rec)` → `sendWithKey_(rec, recordKey_(rec))`, the generator's `key()` in `rune-codec/src/Generator.cpp`). Task 8's Step 0 still runs, but its grep now targets `recordKey_` / `sendWithKey_` (C landed with those names: `send` no longer spells `Key(rec.key())` inline; `recordKey_` builds the key, including the >16-byte-key `std::invalid_argument` refusal the typed claim must mirror).
> 2. **Branch base:** cut from the integration tip **after Part D merged** (D is on the auth tip); rune-codec is the `feat/catalog-history` lineage. Paths are the `/Users/vradu/Projects/se-worktrees/*` worktrees.
> 3. **Line hints refreshed** (the producer files moved a few lines with the TransientLogFull retry): `ProduceFrameBuilder.h` `append` is lines 66–99 (comment + body); `ProducerImpl.h` message ids lines 75–77 (`kMsgSendRecord` … `kMsgTier2Batch`); `ProducerImpl.cpp` `drainRing` 1295–1381, `appendRecord_` 1384–1539 (comment + body), `discardRing_` 2910–2942, `tryAppendDirect_` 476–536, `throwForSendResult` 915–941, `encodeAndSend` 1546, `failBuilder_` 2632, `~ProducerImpl` 3191; `Producer.h` `SendResult` 229–245, the `AckToken` `send` 431, `private:` 569; `Completions.h` friends 297–299. Function names are the anchors — trust them over the numbers.
> 4. **Copy count (spec §B) unchanged** by catalog history / auth: the produce path did not move. Auth adds one WRITE-bitmap check on the broker per produce (auth Task 13) — nothing client-side; a claim committed to a topic the session may not write completes `NotAuthorized (45)` exactly like a `send` (no new request type, no new grant: WRITE TOPIC).
> 5. **Part F follows B and reuses its claim paths:** keep `SendResult::ClaimHeld` / `RejectReason::ClaimHeld` appended **last** in B so F can append `NotInTransaction` after them; keep `kMsgClaimBatch = 4` so F's `kMsgTxnControl = 5` follows. B must not add transactional behaviour (F does).
> 6. **Fix-on-sight noted (owned by F, not B):** `Tier2Entry::batchBytes` is a per-batch heap `std::vector` behind a heap `Tier2Entry*` in the ring, and `PartitionState::pendingTier2Entries` is a `std::deque`; F Task 3 replaces them with a preallocated slot pool. B's `kMsgClaimBatch` carries its records inline in the ring (no heap), as written.
> 7. Test gate wording aligned with CLAUDE.md #7: per task the affected executables; the full `*Regression*` suite (+ client `build_tsan`) at the checkpoints after Task 3, Task 6 and Task 9.

**Goal:** Make the published zero-copy produce API real: `Producer::tryClaim(topic, key, len)` → a `Claim` (`buffer()` / `commit()` / `abort()`), `Producer::claimBatch(topic, key, n, recLen)` → a `BatchClaim` (`record(i)` / `commit()`, ONE atomic RecordBatch), SBE-style generated `XEncoder` / `XDecoder` flyweights for all-fixed-size schemas, and `TypedTopic::claim(keyValue)` returning the generated encoder over the claimed bytes.

**Architecture:** Thread mode claims a slot in the producer's existing M2O ring (`kMsgSendRecord` for one record, a new `kMsgClaimBatch = 4` for a batch) and hands back the value bytes; `commit()` commits the ring slot and the agent path is unchanged (2 internal copies remain: ring → Produce frame → Aeron). Invoker mode reserves the record(s) directly in the partition's `ProduceFrameBuilder` (1 copy: frame → Aeron), guarded by an explicit per-producer claim state machine (`NoDirectClaim` / `DirectClaimOpen{slot, mark, …}`). rune-codec's `generateHeader` additionally emits `XEncoder` / `XDecoder` for schemas whose fields are all fixed-size; they load/store with `memcpy` through a small `rune::codec::fly` helper header (the ring value offset 38 is unaligned).

**Tech Stack:** C++23, GoogleTest, CMake (client: Unix Makefiles `build/` + conan toolchain; codec: `build/`), Aeron IPC (in-process `InvokerGateway`), a custom in-process benchmark harness (no Aeron).

**Spec:** `docs/superpowers/specs/2026-10-11-rune-phase2b-design.md` — section **B. Zero-copy produce** is binding (copy count, API, implementation shape, generated flyweights, gates). Global constraints at the end of the spec.

**Builds on / depends on:** Part **C** (typed topics route by key) — merged (see the update note): the generated struct's `key()`, `TopicSchemaResult.partitionCount`, `TypedTopic` holding `TopicHandle{id, partitionCount}` and routing through `recordKey_(rec)`. Build order **C → E → E2 → (catalog history, auth) → D → B → F → A**. Parts E and D touch disjoint files (B does not consume them).

**Repos and branches:** `rune-codec` and `rune-client-cpp` only. In each (`/Users/vradu/Projects/se-worktrees/rune-codec`, `…/rune-client-cpp`), cut `feat/phase2b-b-zero-copy-produce` from that repo's integration tip **after Part D merged** (D sits on the authentication tip; rune-codec's line is `feat/catalog-history` → `feat/authentication`). The client compiles rune-codec from the canonical sibling `../rune-codec` (`RUNE_CODEC_ROOT`), so the codec checkout next to the client must be on this branch while client tasks run. Nothing is pushed.

## Global Constraints

- CLAUDE.md hard rules (binding): shared-nothing; ring buffers are the only cross-thread passing; state machines whose states hold exactly their variables; **zero steady-state allocation** on every claim path; test + benchmark everything; a `*Regression*`-named test per fix; the **full `*Regression*` suite runs first** before every commit (CLAUDE.md #7); fix-on-sight.
- Spec B (verbatim): "`valueLen` / `recLen` ≤ the existing per-record cap (16 KiB incl. key); `n × recLen` ≤ the ring's `maxMsgLength`. Over-cap → `MessageTooLarge` (non-retryable) as today."
- Spec B (verbatim): "**2 internal copies remain in Thread mode** (ring → frame → Aeron). In **Invoker mode** (owner-thread, no ring) a claim writes straight into the Produce frame being built: **1 copy** (frame → Aeron)." Pinned by `ProducerClaim.RingClaimIsCopiedOnceIntoTheFrame` (Task 4) and `ProducerClaimDirect.ClaimWritesStraightIntoTheProduceFrame` (Task 6).
- Spec B: "A held claim blocks the producer's single consumer: claims are RAII, must not span blocking work, and an abandoned claim is aborted by its destructor."
- Spec B: "Thread rules unchanged: Thread mode any thread; Invoker mode owner thread only (`WrongThread` otherwise)."
- Spec B: "`claimBatch`: new ring message type **`kMsgClaimBatch = 4`** … the agent appends all `n` records into **one** RecordBatch (atomic append, same ordering rules as Tier-2 batches)."
- Spec B: "Value offset 38 in the ring is not 8-aligned and generated records are packed: flyweights use `memcpy` loads/stores (no aliasing UB)."
- Spec B: flyweights only for schemas whose fields are **all fixed-size**; schemas with `string` / `bytes` keep today's struct + `encode()` unchanged.
- Spec B gate: "claim paths **0 allocations** steady state"; benchmarks `send(span)` vs `tryClaim` vs `TypedTopic::send` vs typed `claim`, per-record p50/p99 and allocations.
- Published API names (the published Arcanum site/rune/docs.html) implemented exactly: `Producer::tryClaim(topic, key, len)` with `buffer()`, `commit()`, `abort()`; `Producer::claimBatch(topic, key, n, recLen)` with `record(i)`, `commit()`; generated `XEncoder(span)` with chainable setters and `XEncoder::kBlockLength`, `XDecoder`; `TypedTopic::claim(keyValue)` returning the encoder with `commit()`.
- Wire enums / ring message ids are append-only: `kMsgClaimBatch = 4` is appended after `kMsgTier2Batch = 3`; `SendResult::ClaimHeld` and `RejectReason::ClaimHeld` are appended last.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Nothing is pushed.

## Decisions (added 2026-10-19)

- **BD1 — Typed-claim keys follow Part C's `recordKey_` rules exactly** (fixed-width keys must fit `Key::kMaxInline` at compile time; a `string(N)` key longer than 16 bytes throws `std::invalid_argument` before anything is claimed). C landed after this plan was written and made that refusal explicit; `claim(keyValue)` built `Key(keyValue)` directly, which would have truncated or failed to compile.
- **BD2 — B is transaction-agnostic.** Every claim path goes through the producer ring / builder, so Part F's transactional gate and batch attributes apply to claims without B knowing; B only keeps its appended enum / ring-id values last (update note 5).

## Review Focus

Most likely to bite a user, each pinned by a named test in its owning task:

1. **A typed claim must land on the same partition with the same record key as a typed send of the same key value** — otherwise a service mixing `send(rec)` and `claim(key)` loses per-key ordering → `TypedClaim.RoutesLikeTypedSendForEveryKey` (Task 8).
2. **An Invoker claim abandoned under the caller (closeImmediately / Client close / a failed partition) must never be committed into a reset builder** (corrupt frame, phantom record): `commit()` returns false, nothing is sent, nothing completes → `ProducerClaimDirect.CloseAbandonsAnOpenClaimAndItsCommitReturnsFalse` (Task 6).
3. **Holding an Invoker claim and then sending / pumping on the same thread** must fail fast (`ClaimHeld`, a blocking `send()` throws `std::logic_error`) and a duty pass must never seal the half-written record → `ProducerClaimDirect.OpenClaimBlocksSendsAndIsNeverFlushedHalfWritten` (Task 6).
4. **Claiming `XEncoder::kBlockLength` bytes (the published docs example) instead of `kEncodedLength`** must throw `std::length_error` from the encoder, never write 4 bytes past the claim → `FlyweightGen.EncoderRejectsABufferShorterThanKEncodedLength` (Task 2).
5. **Batch-ack credit for claims** must be exact — n units for a `claimBatch`, returned by one BatchAck of n, and every unit returned for an aborted / destroyed claim (otherwise the queue drifts into permanent `QueueFull`) → `ClaimClient.BatchAckCreditIsReturnedForCommittedAndAbortedClaims` (Task 7).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `rune-codec/include/rune/codec/Flyweight.h` (new) | `rune::codec::fly::store/load/storeFixedStr/loadFixedStr` — memcpy LE access for flyweights | 1 |
| `rune-codec/tests/FlyweightTest.cpp` (new) | helper unit tests | 1 |
| `rune-codec/src/Generator.cpp`, `include/rune/codec/Generator.h` | emit `XEncoder` / `XDecoder` + `T::Encoder` / `T::Decoder` aliases for all-fixed schemas | 2 |
| `rune-codec/tools/make_allfixed_sample_blob.cpp` (new) | build-time fixture descriptor (every fixed type, odd offsets, a key) | 2 |
| `rune-codec/tests/FlyweightGeneratedRoundTripTest.cpp` (new), `tests/GeneratorTest.cpp` | compiled flyweight round-trip vs `encode()`/`decode()`; emitted-text checks | 2 |
| `rune-codec/CMakeLists.txt` | new tests, fixture tool, generated header target | 1, 2 |
| `rune-client-cpp/include/rune/client/producer/ProduceFrameBuilder.h` | `reserveRecord`, `reserveExtra`, `Mark` / `mark()` / `rollback()`, `kRecordOverheadMax` | 3 |
| `rune-client-cpp/tests/producer/ProduceFrameBuilderTest.cpp` | reserve == append byte identity, rollback | 3 |
| `rune-client-cpp/include/rune/client/producer/Producer.h` | `Claim`, `BatchClaim`, `detail::ClaimOpen`, `detail::ClaimCredit`; `tryClaim` ×3, `claimBatch`; `SendResult::ClaimHeld` | 4, 5, 6 |
| `rune-client-cpp/src/producer/Claim.cpp` (new) | `Claim` / `BatchClaim` / `ClaimCredit` bodies | 4, 5 |
| `rune-client-cpp/src/producer/Producer.cpp` | `tryClaim` ×3, `claimPart_`, `claimBatch`, `reserveBatchUnits` | 4, 5 |
| `rune-client-cpp/include/rune/client/Completions.h` | `friend struct detail::ClaimCredit` | 4 |
| `rune-client-cpp/include/rune/client/producer/ProducerImpl.h`, `src/producer/ProducerImpl.cpp` | `ClaimGrant`, `claimRecord` / `claimBatch` / `commitClaim` / `abortClaim`; `kMsgClaimBatch` drain + discard; `admitRecord_` / `admitBatch_` / `appendBatch_` / `noteAppended_` / `ensureBuilder_`; Direct claim state machine + guards | 4, 5, 6 |
| `rune-client-cpp/tests/support/FrameCapture.h` (new) | SendFn sink that decodes and records every produced record | 4 |
| `rune-client-cpp/tests/producer/ProducerClaimTest.cpp` (new) | Ring + Direct claim unit tests, copy-count proofs, alloc gates | 4, 5, 6 |
| `rune-client-cpp/tests/client/ClaimClientTest.cpp` (new) | real Client (both runner modes) over Aeron IPC to `InvokerGateway` | 7 |
| `rune-client-cpp/include/rune/client/TypedTopic.h` | `TypedClaim<E>`, `TypedTopic::claim(keyValue)` / `claim()` | 8 |
| `rune-client-cpp/tests/support/GenClaimSchemas.cpp` (new) | build-time generated `position.gen.h` / `tick_sample.gen.h` | 8 |
| `rune-client-cpp/tests/client/TypedClaimTest.cpp` (new) | typed claim routing parity, version stamp, refusal, alloc gate | 8 |
| `rune-client-cpp/bench/ProducerClaimBench.cpp` (new) | send vs tryClaim vs typed send vs typed claim: p50/p99 + allocations, alloc gate | 9 |
| `rune-client-cpp/CMakeLists.txt` | `Claim.cpp`; test files; claim-schema codegen target; bench target | 4, 6, 7, 8, 9 |
| `rune-client-cpp/CLAUDE.md` | §2.1 producer: claims | 9 |

Build commands used throughout (run from each repo's root):
- codec: `cmake --build build -j8` then `ctest --test-dir build …` (existing Debug / Unix Makefiles dir).
- client: `cmake --build build -j8` then `ctest --test-dir build …` (existing Debug dir configured with the conan toolchain, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`). If `build/` is missing: `conan install . -of build -s build_type=Debug -s arch=x86_64 -s compiler.cppstd=gnu17 --build=missing && cmake -S . -B build -DBUILD_TESTING=ON -DBUILD_EXAMPLES=ON -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators"`.
- Test gate (CLAUDE.md #7, updated 2026-10-19): per task, the executables covering what it touches plus every new or changed test (each task's Run step). The full `*Regression*` suite (`ctest --test-dir build -R Regression --output-on-failure -j14`, plus the client `build_tsan` build with `BUILD_TESTING=ON`) runs at the checkpoints — after Task 3, after Task 6, after Task 9 (before the final review). Where a task step below still says "Regression gate + full suite, then commit", the affected-area run binds per task and the full suite belongs to the checkpoint.

---

### Task 1: rune-codec — `fly` load/store helpers

**Files:**
- Create: `rune-codec/include/rune/codec/Flyweight.h`
- Create: `rune-codec/tests/FlyweightTest.cpp`
- Modify: `rune-codec/CMakeLists.txt` (the `add_executable(rune_codec_tests …)` source list)

**Interfaces:**
- Produces (namespace `rune::codec::fly`, header-only):
  `template <class T> void store(uint8_t* p, T v) noexcept;` (T: integral, `bool`, `double`)
  `template <class T> T load(const uint8_t* p) noexcept;`
  `void storeFixedStr(uint8_t* p, std::string_view s, std::size_t n) noexcept;`
  `std::string_view loadFixedStr(const uint8_t* p, std::size_t n) noexcept;`

- [ ] **Step 1: Write the failing tests** — create `rune-codec/tests/FlyweightTest.cpp`:

```cpp
// tests/FlyweightTest.cpp — Phase 2b Part B: rune::codec::fly load/store helpers
// (the memcpy little-endian access every generated flyweight uses).
#include <gtest/gtest.h>
#include <rune/codec/Flyweight.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

namespace fly = rune::codec::fly;

TEST(Flyweight, StoresPackedLittleEndianAtAnOddAddress) {
    uint8_t buf[16] = {};
    fly::store<uint32_t>(buf + 1, 0x11223344u);
    EXPECT_EQ(buf[1], 0x44);
    EXPECT_EQ(buf[2], 0x33);
    EXPECT_EQ(buf[3], 0x22);
    EXPECT_EQ(buf[4], 0x11);
    EXPECT_EQ(fly::load<uint32_t>(buf + 1), 0x11223344u);
    fly::store<int64_t>(buf + 3, int64_t{-2});
    EXPECT_EQ(buf[3], 0xFE);
    for (int i = 4; i < 11; ++i) EXPECT_EQ(buf[i], 0xFF) << i;
    EXPECT_EQ(fly::load<int64_t>(buf + 3), int64_t{-2});
}

TEST(Flyweight, RoundTripsEveryFixedScalarType) {
    uint8_t buf[9] = {};
    auto rt = [&](auto v) {
        using T = decltype(v);
        fly::store<T>(buf + 1, v);
        return fly::load<T>(buf + 1);
    };
    EXPECT_EQ(rt(std::numeric_limits<int32_t>::min()), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(rt(std::numeric_limits<int64_t>::min()), std::numeric_limits<int64_t>::min());
    EXPECT_EQ(rt(uint16_t{65535}), uint16_t{65535});
    EXPECT_EQ(rt(uint32_t{4'000'000'000u}), uint32_t{4'000'000'000u});
    EXPECT_EQ(rt(std::numeric_limits<uint64_t>::max()), std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(rt(-3.25), -3.25);
    EXPECT_TRUE(std::signbit(rt(-0.0)));
    EXPECT_TRUE(rt(true));
    EXPECT_FALSE(rt(false));
}

TEST(Flyweight, DoubleIsItsIeeeBitPatternLittleEndian) {
    uint8_t buf[8] = {};
    fly::store<double>(buf, 1.0);  // 0x3FF0000000000000
    const uint8_t want[8] = {0, 0, 0, 0, 0, 0, 0xF0, 0x3F};
    for (int i = 0; i < 8; ++i) EXPECT_EQ(buf[i], want[i]) << i;
}

TEST(Flyweight, BoolIsOneByteAndAnyNonZeroReadsTrue) {
    uint8_t b = 0xAA;
    fly::store<bool>(&b, true);
    EXPECT_EQ(b, 1);
    fly::store<bool>(&b, false);
    EXPECT_EQ(b, 0);
    b = 7;
    EXPECT_TRUE(fly::load<bool>(&b));
}

TEST(Flyweight, FixedStrPadsWithZeroesAndTruncates) {
    uint8_t buf[6];
    std::memset(buf, 0xEE, sizeof buf);
    fly::storeFixedStr(buf, "AB", 6);
    EXPECT_EQ(fly::loadFixedStr(buf, 6), std::string_view("AB\0\0\0\0", 6));
    fly::storeFixedStr(buf, "TOO-LONG", 6);
    EXPECT_EQ(fly::loadFixedStr(buf, 6), std::string_view("TOO-LO", 6));
    fly::storeFixedStr(buf, "", 6);
    EXPECT_EQ(fly::loadFixedStr(buf, 6), std::string_view("\0\0\0\0\0\0", 6));
}
```

Add it to the test executable in `rune-codec/CMakeLists.txt` — the list becomes:

```cmake
add_executable(rune_codec_tests
    tests/SmokeTest.cpp
    tests/PrimTypeTest.cpp
    tests/DescriptorTest.cpp
    tests/FingerprintTest.cpp
    tests/SerializeTest.cpp
    tests/ToTextTest.cpp
    tests/RecordReaderTest.cpp
    tests/ValidatorTest.cpp
    tests/GeneratorTest.cpp
    tests/GeneratedRoundTripTest.cpp
    tests/FixedStrLayoutTest.cpp
    tests/FixedStrSerializeTest.cpp
    tests/FixedStrReadValidateTest.cpp
    tests/FixedStrGeneratedRoundTripTest.cpp
    tests/EvolveTest.cpp
    tests/AlterOpsTest.cpp
    tests/MixedVersionReadTest.cpp
    tests/DecodeBoundaryMatrixTest.cpp
    tests/FlyweightTest.cpp)
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build build -j8 --target rune_codec_tests`
Expected: compile error `'rune/codec/Flyweight.h' file not found`.

- [ ] **Step 3: Implement** — create `rune-codec/include/rune/codec/Flyweight.h`:

```cpp
// include/rune/codec/Flyweight.h — Phase 2b Part B: load/store helpers for the
// generated zero-copy flyweights (XEncoder / XDecoder).
//
// Records are packed little-endian and a claimed buffer is not aligned (the
// producer ring's value offset is 38), so every access is a memcpy — never a
// reinterpret_cast load/store (no aliasing / alignment UB).  On a
// little-endian host the byteswap branches compile away.
#pragma once
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>
#include <type_traits>

namespace rune::codec::fly {

/// Store `v` at `p` as packed little-endian (bool: one byte 0/1; double: its
/// IEEE-754 bit pattern).
template <class T>
inline void store(uint8_t* p, T v) noexcept {
    if constexpr (std::is_same_v<T, bool>) {
        *p = v ? uint8_t{1} : uint8_t{0};
    } else if constexpr (std::is_same_v<T, double>) {
        store<uint64_t>(p, std::bit_cast<uint64_t>(v));
    } else {
        static_assert(std::is_integral_v<T>, "fly::store: integral, bool or double");
        if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1) v = std::byteswap(v);
        std::memcpy(p, &v, sizeof v);
    }
}

/// Load a packed little-endian T from `p` (bool: any non-zero byte is true).
template <class T>
[[nodiscard]] inline T load(const uint8_t* p) noexcept {
    if constexpr (std::is_same_v<T, bool>) {
        return *p != 0;
    } else if constexpr (std::is_same_v<T, double>) {
        return std::bit_cast<double>(load<uint64_t>(p));
    } else {
        static_assert(std::is_integral_v<T>, "fly::load: integral, bool or double");
        T v;
        std::memcpy(&v, p, sizeof v);
        if constexpr (std::endian::native == std::endian::big && sizeof(T) > 1) v = std::byteswap(v);
        return v;
    }
}

/// A string(N) field: the first min(N, s.size()) bytes of `s`, zero-padded to N.
inline void storeFixedStr(uint8_t* p, std::string_view s, std::size_t n) noexcept {
    const std::size_t k = s.size() < n ? s.size() : n;
    if (k != 0) std::memcpy(p, s.data(), k);
    if (k < n) std::memset(p + k, 0, n - k);
}

/// A string(N) field's N raw bytes, zero padding included — exactly the bytes
/// the generated struct's decode() copies into its std::array<char, N>.
[[nodiscard]] inline std::string_view loadFixedStr(const uint8_t* p, std::size_t n) noexcept {
    return {reinterpret_cast<const char*>(p), n};
}

}  // namespace rune::codec::fly
```

- [ ] **Step 4: Run to verify it passes**

Run: `cmake --build build -j8 --target rune_codec_tests && ctest --test-dir build -R 'Flyweight\.' --output-on-failure`
Expected: 5 `Flyweight.*` tests PASS.

- [ ] **Step 5: Regression gate + full suite, then commit**

```bash
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add include/rune/codec/Flyweight.h tests/FlyweightTest.cpp CMakeLists.txt
git commit -m "feat(codec): fly load/store helpers for zero-copy flyweights (memcpy, packed LE)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: rune-codec — generated `XEncoder` / `XDecoder` for all-fixed schemas

**Files:**
- Modify: `rune-codec/src/Generator.cpp` (helpers after `cppType()`; the include block; before `// ── Struct ──`; after the `kSchemaName` line; after `}; // struct`)
- Modify: `rune-codec/include/rune/codec/Generator.h` (doc comment)
- Create: `rune-codec/tools/make_allfixed_sample_blob.cpp`
- Create: `rune-codec/tests/FlyweightGeneratedRoundTripTest.cpp`
- Modify: `rune-codec/tests/GeneratorTest.cpp` (append)
- Modify: `rune-codec/CMakeLists.txt`

**Interfaces:**
- Consumes: Task 1 `rune::codec::fly::*`; Part C's generator changes (struct `key()`), already merged — the anchors below are text anchors, not line numbers, so C's insertions do not move them.
- Produces, for every schema whose fields are all fixed-size (no `String` / `Bytes`) and that has no NON-key field named `key` (struct name `X` = PascalCase schema name):
  - in `struct X`: `using Encoder = XEncoder; using Decoder = XDecoder;`
  - `class XEncoder`: `static constexpr uint32_t kBlockLength`, `static constexpr uint32_t kEncodedLength = 4u + kBlockLength`, `static constexpr uint16_t kVersion`, `static constexpr bool kHasKey`, `using KeyType = <member type of the key field>` (only when `kHasKey`); `explicit XEncoder(std::span<uint8_t>)` (throws `std::length_error` if shorter than `kEncodedLength`; writes `[blockLength][version]`, zeroes the block); one setter per field `XEncoder& <field>(T v) noexcept` (`string(N)`: `std::string_view`, zero-padded / truncated); when `kHasKey`: `XEncoder& key(KeyType)` (omitted when the key field itself is named `key`) and `KeyType key() const`.
  - `class XDecoder`: `kBlockLength`, `kHasKey`, `KeyType`; `explicit XDecoder(std::span<const uint8_t>) noexcept`; one getter per field (`string(N)`: `std::string_view` of N raw bytes); `KeyType key() const` (when `kHasKey` and the key field is not named `key`). A field outside the record's own `blockLength` or past its end reads `T{}` / empty.

- [ ] **Step 1: Write the failing tests**

Create `rune-codec/tools/make_allfixed_sample_blob.cpp`:

```cpp
// tools/make_allfixed_sample_blob.cpp — Phase 2b Part B fixture: writes the
// allfixed_sample descriptor blob (every fixed field type, odd offsets, a key
// field) the flyweight round-trip test compiles its generated header from.
//   id Int64 KEY @0 | a Int32 @8 | b UInt16 @12 | c UInt32 @14 | d UInt64 @18 |
//   e Float64 @26 | f Bool @34 | g Enum{1,2,3} @35 | code string(6) @39
//   -> blockLength 45, kEncodedLength 49
#include <rune/codec/Descriptor.h>
#include <rune/codec/Serialize.h>

#include <cstdio>
#include <fstream>

int main(int argc, char* argv[]) {
    const char* outPath = (argc >= 2) ? argv[1] : "allfixed_sample.blob";
    using rune::codec::PrimType;
    const auto d = rune::codec::DescriptorBuilder("allfixed_sample")
        .add("id", PrimType::Int64, 1, true)
        .add("a", PrimType::Int32)
        .add("b", PrimType::UInt16)
        .add("c", PrimType::UInt32)
        .add("d", PrimType::UInt64)
        .add("e", PrimType::Float64)
        .add("f", PrimType::Bool)
        .add("g", PrimType::Enum, 1, false, {1, 2, 3})
        .addFixedStr("code", 6)
        .build();
    const auto blob = rune::codec::serialize(d);
    std::ofstream f(outPath, std::ios::binary | std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "make_allfixed_sample_blob: cannot open '%s'\n", outPath);
        return 1;
    }
    f.write(reinterpret_cast<const char*>(blob.data()), static_cast<std::streamsize>(blob.size()));
    return 0;
}
```

In `rune-codec/CMakeLists.txt`, after the `generate_fixedstr_sample_header` block (before `enable_testing()`), add:

```cmake
# ── Phase 2b Part B: allfixed_sample flyweight fixture (built, not checked in) ─
add_executable(make_allfixed_sample_blob tools/make_allfixed_sample_blob.cpp)
target_link_libraries(make_allfixed_sample_blob PRIVATE rune_codec)

set(ALLFIXED_SAMPLE_BLOB  "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/allfixed_sample.blob")
set(ALLFIXED_SAMPLE_GEN_H "${CMAKE_CURRENT_BINARY_DIR}/tests/generated/allfixed_sample.gen.h")

add_custom_command(
    OUTPUT  "${ALLFIXED_SAMPLE_BLOB}"
    COMMAND make_allfixed_sample_blob "${ALLFIXED_SAMPLE_BLOB}"
    DEPENDS make_allfixed_sample_blob
    COMMENT "Generating allfixed_sample.blob (2b-B flyweight fixture)"
)
add_custom_command(
    OUTPUT  "${ALLFIXED_SAMPLE_GEN_H}"
    COMMAND rune_codec_gen "${ALLFIXED_SAMPLE_BLOB}" "${ALLFIXED_SAMPLE_GEN_H}"
    DEPENDS rune_codec_gen "${ALLFIXED_SAMPLE_BLOB}"
    COMMENT "Generating allfixed_sample.gen.h from allfixed_sample.blob"
)
add_custom_target(generate_allfixed_sample_header DEPENDS "${ALLFIXED_SAMPLE_GEN_H}")
```

Append `tests/FlyweightGeneratedRoundTripTest.cpp` to the `rune_codec_tests` source list (after `tests/FlyweightTest.cpp`), and change the dependency line to:

```cmake
add_dependencies(rune_codec_tests generate_characters_header generate_fixedstr_sample_header
                 generate_allfixed_sample_header)
```

Create `rune-codec/tests/FlyweightGeneratedRoundTripTest.cpp`:

```cpp
// tests/FlyweightGeneratedRoundTripTest.cpp — Phase 2b Part B: the generated
// AllfixedSampleEncoder / AllfixedSampleDecoder (build-time generated from
// allfixed_sample.blob) against the generated struct's encode() / decode():
// byte-identical output, field-identical reads, every fixed type, odd offsets.
#include <gtest/gtest.h>
#include "generated/allfixed_sample.gen.h"   // generated at build time
#include <rune/codec/Flyweight.h>
#include <rune/codec/Validator.h>

#include <cstring>
#include <span>
#include <stdexcept>
#include <string_view>
#include <type_traits>
#include <vector>

using rune::gen::AllfixedSample;
using Enc = rune::gen::AllfixedSampleEncoder;
using Dec = rune::gen::AllfixedSampleDecoder;

static_assert(std::is_same_v<AllfixedSample::Encoder, Enc>);
static_assert(std::is_same_v<AllfixedSample::Decoder, Dec>);
static_assert(Enc::kBlockLength == 45u);
static_assert(Enc::kEncodedLength == 49u);
static_assert(Enc::kHasKey && Dec::kHasKey);
static_assert(std::is_same_v<Enc::KeyType, int64_t>);

namespace {
AllfixedSample sample() {
    AllfixedSample s;
    s.id = -1'234'567'890'123LL;
    s.a  = -42;
    s.b  = 65535;
    s.c  = 4'000'000'000u;
    s.d  = 0x0123456789ABCDEFull;
    s.e  = -3.25;
    s.f  = true;
    s.g  = 2;
    std::memcpy(s.code.data(), "ABC", 3);  // the rest stays zero
    return s;
}
std::vector<uint8_t> encoded(const AllfixedSample& s) {
    std::vector<uint8_t> v(s.encodedSize());
    v.resize(s.encode(v.data()));
    return v;
}
}  // namespace

TEST(FlyweightGen, EncoderBytesEqualStructEncode) {
    const AllfixedSample s = sample();
    const std::vector<uint8_t> want = encoded(s);
    std::vector<uint8_t> got(Enc::kEncodedLength, 0xEE);  // dirty memory, like a reused ring slot
    Enc(got).id(s.id).a(s.a).b(s.b).c(s.c).d(s.d).e(s.e).f(s.f).g(s.g).code("ABC");
    EXPECT_EQ(got, want);
    EXPECT_EQ(want.size(), static_cast<std::size_t>(Enc::kEncodedLength));
    EXPECT_EQ(Enc::kBlockLength, static_cast<uint32_t>(AllfixedSample::kBlockLength));
    EXPECT_EQ(Enc::kVersion, AllfixedSample::kVersion);
}

TEST(FlyweightGen, DecoderReadsWhatDecodeReads) {
    const std::vector<uint8_t> rec = encoded(sample());
    AllfixedSample back;
    back.decode(rec);
    const Dec dec(rec);
    EXPECT_EQ(dec.id(), back.id);
    EXPECT_EQ(dec.a(), back.a);
    EXPECT_EQ(dec.b(), back.b);
    EXPECT_EQ(dec.c(), back.c);
    EXPECT_EQ(dec.d(), back.d);
    EXPECT_EQ(dec.e(), back.e);
    EXPECT_EQ(dec.f(), back.f);
    EXPECT_EQ(dec.g(), back.g);
    EXPECT_EQ(dec.code(), std::string_view(back.code.data(), 6));
    EXPECT_EQ(dec.key(), back.id);
}

TEST(FlyweightGen, EncoderZeroesTheBlockOverDirtyMemory) {
    std::vector<uint8_t> buf(Enc::kEncodedLength, 0xEE);
    Enc(buf).id(7);
    const Dec dec(buf);
    EXPECT_EQ(dec.id(), 7);
    EXPECT_EQ(dec.a(), 0);
    EXPECT_EQ(dec.d(), 0u);
    EXPECT_EQ(dec.e(), 0.0);
    EXPECT_FALSE(dec.f());
    EXPECT_EQ(dec.code(), std::string_view("\0\0\0\0\0\0", 6));
}

// Review Focus 4: the published docs claim PositionEncoder::kBlockLength bytes;
// the encoder needs kEncodedLength (4 header bytes more) and must refuse loudly
// instead of writing past the claim.
TEST(FlyweightGen, EncoderRejectsABufferShorterThanKEncodedLength) {
    std::vector<uint8_t> buf(Enc::kBlockLength, 0xEE);
    EXPECT_THROW(Enc{std::span<uint8_t>(buf)}, std::length_error);
    for (const uint8_t b : buf) EXPECT_EQ(b, 0xEE) << "nothing written";
}

TEST(FlyweightGen, FixedStrSetterPadsAndTruncates) {
    std::vector<uint8_t> buf(Enc::kEncodedLength);
    Enc(buf).code("TOO-LONG-VALUE");
    EXPECT_EQ(Dec(buf).code(), std::string_view("TOO-LO", 6));
    Enc(buf).code("Z");
    EXPECT_EQ(Dec(buf).code(), std::string_view("Z\0\0\0\0\0", 6));
}

TEST(FlyweightGen, DecoderToleratesNewerLongerAndOlderShorterBlocks) {
    const AllfixedSample s = sample();
    const std::vector<uint8_t> rec = encoded(s);
    // A newer version: its block is 8 bytes longer (a field appended at the end).
    std::vector<uint8_t> newer = rec;
    newer.insert(newer.end(), 8, uint8_t{0x55});
    rune::codec::fly::store<uint16_t>(newer.data(), static_cast<uint16_t>(Enc::kBlockLength + 8));
    const Dec dn(newer);
    EXPECT_EQ(dn.id(), s.id);
    EXPECT_EQ(dn.g(), s.g);
    EXPECT_EQ(dn.code(), std::string_view(s.code.data(), 6));
    // An older version whose block held only `id` (8 bytes).
    std::vector<uint8_t> older(4 + 8);
    rune::codec::fly::store<uint16_t>(older.data(), uint16_t{8});
    rune::codec::fly::store<uint16_t>(older.data() + 2, uint16_t{1});
    rune::codec::fly::store<int64_t>(older.data() + 4, s.id);
    const Dec dold(older);
    EXPECT_EQ(dold.id(), s.id);
    EXPECT_EQ(dold.a(), 0);
    EXPECT_FALSE(dold.f());
    EXPECT_TRUE(dold.code().empty());
    // Truncated input never reads out of bounds.
    const Dec dt(std::span<const uint8_t>(rec.data(), 10));
    EXPECT_EQ(dt.id(), 0) << "id @0..8 needs bytes 4..12 of a 10-byte input";
    EXPECT_EQ(dt.a(), 0);
}

TEST(FlyweightGen, EncoderOutputPassesTheValidator) {
    const AllfixedSample s = sample();
    std::vector<uint8_t> got(Enc::kEncodedLength);
    Enc(got).id(s.id).a(s.a).b(s.b).c(s.c).d(s.d).e(s.e).f(s.f).g(s.g).code("ABC");
    EXPECT_TRUE(rune::codec::Validator(AllfixedSample::descriptor()).validate(got).ok);
}

TEST(FlyweightGen, KeyHelpersAreTheKeyField) {
    std::vector<uint8_t> buf(Enc::kEncodedLength);
    Enc e(buf);
    e.key(99);
    EXPECT_EQ(e.key(), 99);
    EXPECT_EQ(Dec(buf).id(), 99);
    EXPECT_EQ(Dec(buf).key(), 99);
}
```

Append to `rune-codec/tests/GeneratorTest.cpp`:

```cpp
TEST(Generator, EmitsZeroCopyFlyweightsForAnAllFixedSchema) {
    auto d = DescriptorBuilder("position").add("userId", PrimType::Int64, 1, true)
               .add("zone", PrimType::Int32).addFixedStr("tag", 8).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("class PositionEncoder {"), std::string::npos);
    EXPECT_NE(hdr.find("class PositionDecoder {"), std::string::npos);
    EXPECT_NE(hdr.find("using Encoder = PositionEncoder;"), std::string::npos);
    EXPECT_NE(hdr.find("using Decoder = PositionDecoder;"), std::string::npos);
    EXPECT_NE(hdr.find("static constexpr uint32_t kEncodedLength = 4u + kBlockLength;"), std::string::npos);
    EXPECT_NE(hdr.find("static constexpr bool     kHasKey        = true;"), std::string::npos);
    EXPECT_NE(hdr.find("using KeyType = int64_t;"), std::string::npos);
    EXPECT_NE(hdr.find("PositionEncoder& zone(int32_t v) noexcept {"), std::string::npos);
    EXPECT_NE(hdr.find("::rune::codec::fly::store<int32_t>(p_ + 4u + 8u, v);"), std::string::npos)
        << "zone sits at block offset 8";
    EXPECT_NE(hdr.find("PositionEncoder& tag(std::string_view v) noexcept {"), std::string::npos);
    EXPECT_NE(hdr.find("#include <rune/codec/Flyweight.h>"), std::string::npos);
}

TEST(Generator, NoFlyweightsForASchemaWithVarLenFields) {
    auto d = DescriptorBuilder("characters").add("id", PrimType::Int64, 1, true)
               .add("name", PrimType::String).build();
    const std::string hdr = generateHeader(d);
    EXPECT_EQ(hdr.find("Encoder"), std::string::npos);
    EXPECT_EQ(hdr.find("Decoder"), std::string::npos);
    EXPECT_NE(hdr.find("size_t encode(uint8_t* buf) const noexcept"), std::string::npos)
        << "var-len schemas keep today's struct + encode()";
}

TEST(Generator, KeylessAllFixedSchemaHasNoKeyHelpers) {
    auto d = DescriptorBuilder("tick").add("seq", PrimType::UInt64).add("value", PrimType::Float64).build();
    const std::string hdr = generateHeader(d);
    EXPECT_NE(hdr.find("class TickEncoder {"), std::string::npos);
    EXPECT_NE(hdr.find("static constexpr bool     kHasKey        = false;"), std::string::npos);
    EXPECT_EQ(hdr.find("KeyType"), std::string::npos);
}

TEST(Generator, ANonKeyFieldNamedKeySuppressesTheFlyweights) {
    auto d = DescriptorBuilder("odd").add("id", PrimType::Int64, 1, true)
               .add("key", PrimType::Int32).build();
    const std::string hdr = generateHeader(d);
    EXPECT_EQ(hdr.find("class OddEncoder"), std::string::npos)
        << "a field setter key(int32_t) would collide with the key helpers";
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j8 --target rune_codec_tests`
Expected: compile error in `FlyweightGeneratedRoundTripTest.cpp`: `no member named 'AllfixedSampleEncoder' in namespace 'rune::gen'` (the generated header has no flyweights yet).

- [ ] **Step 3: Implement**

In `rune-codec/src/Generator.cpp`, directly after the `cppType()` function, add:

```cpp
// ─── Phase 2b Part B: zero-copy flyweights (all-fixed schemas) ───────────────

/// True when every field lives in the fixed block (no String / Bytes): only
/// then is a record's encoded length known before it is written.
static bool allFixed(const Descriptor& d) {
    return std::none_of(d.fields.begin(), d.fields.end(),
                        [](const FieldDescriptor& f) { return isVarLen(f.type); });
}

/// The schema's single key field; nullptr when it has none or several
/// (composite keys are Phase 3).
static const FieldDescriptor* singleKeyField(const Descriptor& d) {
    const FieldDescriptor* k = nullptr;
    for (const auto& f : d.fields) {
        if (!f.isKey) continue;
        if (k != nullptr) return nullptr;
        k = &f;
    }
    return k;
}

/// Flyweights are emitted for all-fixed schemas, unless a NON-key field is
/// named `key` (its setter would collide with the encoder's key helpers).
static bool wantsFlyweights(const Descriptor& d) {
    if (!allFixed(d)) return false;
    const FieldDescriptor* k = singleKeyField(d);
    for (const auto& f : d.fields) {
        if (f.name == "key" && &f != k) return false;
    }
    return true;
}

/// The fly::load / fly::store template argument of a fixed scalar field.
static std::string_view flyType(PrimType t) noexcept {
    switch (t) {
        case PrimType::Int32:
        case PrimType::Enum:    return "int32_t";
        case PrimType::Int64:   return "int64_t";
        case PrimType::UInt16:  return "uint16_t";
        case PrimType::UInt32:  return "uint32_t";
        case PrimType::UInt64:  return "uint64_t";
        case PrimType::Float64: return "double";
        case PrimType::Bool:    return "bool";
        default:                return "/* not a fixed scalar */";
    }
}

/// The generated struct's member type for a field (string(N): std::array<char, N>).
static std::string memberType(const FieldDescriptor& f) {
    if (f.type == PrimType::FixedStr) return "std::array<char, " + std::to_string(f.fixedLen) + ">";
    return std::string(cppType(f.type));
}

static void emitEncoder(std::ostringstream& o, const Descriptor& d, const std::string& structName) {
    const std::string cls = structName + "Encoder";
    const FieldDescriptor* k = singleKeyField(d);
    o << "// " << cls << " — zero-copy writer over a claimed buffer (Producer::tryClaim,\n"
      << "// TypedTopic::claim).  Same packed little-endian bytes as " << structName << "::encode();\n"
      << "// every field is stored with memcpy (a claimed buffer is not aligned).\n";
    o << "class " << cls << " {\n";
    o << "public:\n";
    o << "    static constexpr uint32_t kBlockLength   = " << d.blockLength << "u;\n";
    o << "    static constexpr uint32_t kEncodedLength = 4u + kBlockLength;\n";
    o << "    static constexpr uint16_t kVersion       = " << d.version << "u;\n";
    o << "    static constexpr bool     kHasKey        = " << (k != nullptr ? "true" : "false") << ";\n";
    if (k != nullptr) o << "    using KeyType = " << memberType(*k) << ";\n";
    o << "\n";
    o << "    /// Binds to `buf` (>= kEncodedLength bytes, else std::length_error), writes\n";
    o << "    /// the record header [blockLength][version] and zeroes the block, so an\n";
    o << "    /// unset field reads as 0 / empty (never stale ring bytes).\n";
    o << "    explicit " << cls << "(std::span<uint8_t> buf) : p_(buf.data()) {\n";
    o << "        if (buf.size() < kEncodedLength) {\n";
    o << "            throw std::length_error(\"" << cls << ": buffer is smaller than kEncodedLength\");\n";
    o << "        }\n";
    o << "        ::rune::codec::fly::store<uint16_t>(p_, static_cast<uint16_t>(kBlockLength));\n";
    o << "        ::rune::codec::fly::store<uint16_t>(p_ + 2, kVersion);\n";
    o << "        std::memset(p_ + 4, 0, kBlockLength);\n";
    o << "    }\n\n";
    for (const auto& f : d.fields) {
        if (f.type == PrimType::FixedStr) {
            o << "    " << cls << "& " << f.name << "(std::string_view v) noexcept {\n"
              << "        ::rune::codec::fly::storeFixedStr(p_ + 4u + " << f.offset << "u, v, "
              << f.fixedLen << "u);\n"
              << "        return *this;\n"
              << "    }\n";
        } else {
            o << "    " << cls << "& " << f.name << "(" << cppType(f.type) << " v) noexcept {\n"
              << "        ::rune::codec::fly::store<" << flyType(f.type) << ">(p_ + 4u + " << f.offset
              << "u, v);\n"
              << "        return *this;\n"
              << "    }\n";
        }
    }
    if (k != nullptr) {
        if (k->name != "key") {  // a key field named `key` already has this setter
            if (k->type == PrimType::FixedStr) {
                o << "    " << cls << "& key(const KeyType& v) noexcept {\n"
                  << "        std::memcpy(p_ + 4u + " << k->offset << "u, v.data(), " << k->fixedLen << "u);\n"
                  << "        return *this;\n"
                  << "    }\n";
            } else {
                o << "    " << cls << "& key(KeyType v) noexcept { return " << k->name << "(v); }\n";
            }
        }
        if (k->type == PrimType::FixedStr) {
            o << "    [[nodiscard]] KeyType key() const noexcept {\n"
              << "        KeyType v{};\n"
              << "        std::memcpy(v.data(), p_ + 4u + " << k->offset << "u, " << k->fixedLen << "u);\n"
              << "        return v;\n"
              << "    }\n";
        } else {
            o << "    [[nodiscard]] KeyType key() const noexcept {\n"
              << "        return ::rune::codec::fly::load<" << flyType(k->type) << ">(p_ + 4u + " << k->offset
              << "u);\n"
              << "    }\n";
        }
    }
    o << "\nprivate:\n";
    o << "    uint8_t* p_;\n";
    o << "};\n\n";
}

static void emitDecoder(std::ostringstream& o, const Descriptor& d, const std::string& structName) {
    const std::string cls = structName + "Decoder";
    const FieldDescriptor* k = singleKeyField(d);
    o << "// " << cls << " — zero-copy reader over one record of any version: a field\n"
      << "// outside the record's own block (an older, shorter version) or past its end\n"
      << "// reads as 0 / empty; a longer block (a newer version) is fine (fixed offsets).\n";
    o << "class " << cls << " {\n";
    o << "public:\n";
    o << "    static constexpr uint32_t kBlockLength = " << d.blockLength << "u;\n";
    o << "    static constexpr bool     kHasKey      = " << (k != nullptr ? "true" : "false") << ";\n";
    if (k != nullptr) o << "    using KeyType = " << memberType(*k) << ";\n";
    o << "\n";
    o << "    explicit " << cls << "(std::span<const uint8_t> rec) noexcept\n"
      << "        : p_(rec.data()), size_(rec.size()),\n"
      << "          bl_(rec.size() >= 2 ? ::rune::codec::fly::load<uint16_t>(rec.data()) : 0u) {}\n\n";
    for (const auto& f : d.fields) {
        const uint32_t w = fieldWidth(f);
        if (f.type == PrimType::FixedStr) {
            o << "    [[nodiscard]] std::string_view " << f.name << "() const noexcept {\n"
              << "        return has_(" << f.offset << "u, " << w << "u)\n"
              << "            ? ::rune::codec::fly::loadFixedStr(p_ + 4u + " << f.offset << "u, " << w << "u)\n"
              << "            : std::string_view{};\n"
              << "    }\n";
        } else {
            o << "    [[nodiscard]] " << cppType(f.type) << " " << f.name << "() const noexcept {\n"
              << "        return has_(" << f.offset << "u, " << w << "u)\n"
              << "            ? ::rune::codec::fly::load<" << flyType(f.type) << ">(p_ + 4u + " << f.offset << "u)\n"
              << "            : " << cppType(f.type) << "{};\n"
              << "    }\n";
        }
    }
    if (k != nullptr && k->name != "key") {
        if (k->type == PrimType::FixedStr) {
            o << "    [[nodiscard]] KeyType key() const noexcept {\n"
              << "        KeyType v{};\n"
              << "        if (has_(" << k->offset << "u, " << k->fixedLen << "u)) {\n"
              << "            std::memcpy(v.data(), p_ + 4u + " << k->offset << "u, " << k->fixedLen << "u);\n"
              << "        }\n"
              << "        return v;\n"
              << "    }\n";
        } else {
            o << "    [[nodiscard]] KeyType key() const noexcept { return " << k->name << "(); }\n";
        }
    }
    o << "\nprivate:\n";
    o << "    [[nodiscard]] bool has_(uint32_t off, uint32_t w) const noexcept {\n";
    o << "        return off + w <= bl_ && 4u + off + w <= size_;\n";
    o << "    }\n";
    o << "    const uint8_t* p_;\n";
    o << "    std::size_t    size_;\n";
    o << "    uint32_t       bl_;\n";
    o << "};\n\n";
}
```

In `generateHeader`, replace the include block (the lines from `o << "#include <array>\n";` through `o << "#include <rune/codec/Serialize.h>\n";`) with:

```cpp
    o << "#include <array>\n";
    o << "#include <cstddef>\n";
    o << "#include <cstdint>\n";
    o << "#include <cstring>\n";
    o << "#include <span>\n";
    o << "#include <stdexcept>\n";
    o << "#include <string_view>\n";
    o << "#include <vector>\n";
    o << "#include <rune/codec/Descriptor.h>\n";
    o << "#include <rune/codec/Fingerprint.h>\n";
    o << "#include <rune/codec/Flyweight.h>\n";
    o << "#include <rune/codec/Serialize.h>\n";
```

Directly before the `// ── Struct ──` comment, insert:

```cpp
    // ── 2b Part B: flyweight forward declarations (all-fixed schemas) ────────
    const bool fly = wantsFlyweights(d);
    if (fly) {
        o << "class " << structName << "Encoder;\n";
        o << "class " << structName << "Decoder;\n\n";
    }
```

Directly after the line that emits `kSchemaName` (`o << "    static constexpr std::string_view kSchemaName = \"" << d.name << "\";\n";`), insert:

```cpp
    if (fly) {
        o << "    using Encoder = " << structName << "Encoder;  // zero-copy writer (all-fixed schema)\n";
        o << "    using Decoder = " << structName << "Decoder;  // zero-copy reader\n";
    }
```

Directly after the line `o << "}; // struct " << structName << "\n";`, insert:

```cpp
    if (fly) {
        o << "\n";
        emitEncoder(o, d, structName);
        emitDecoder(o, d, structName);
    }
```

In `rune-codec/include/rune/codec/Generator.h`, extend the doc list (after the `descriptor()` bullet):

```cpp
///   - Phase 2b Part B — for a schema whose fields are ALL fixed-size (no
///     string / bytes): `<Struct>Encoder` (zero-copy writer over a claimed
///     std::span<uint8_t>: chainable setters, kBlockLength, kEncodedLength,
///     key helpers) and `<Struct>Decoder` (zero-copy reader), aliased as
///     `<Struct>::Encoder` / `<Struct>::Decoder`.  Not emitted when a non-key
///     field is named `key`.
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j8 --target rune_codec_tests && ctest --test-dir build -R 'FlyweightGen|Generator|GeneratedRoundTrip|FixedStrGen' --output-on-failure`
Expected: all PASS (the existing `GeneratedRoundTrip*` / `FixedStrGen.*` tests prove var-len schemas are unchanged).

- [ ] **Step 5: Regression gate + full suite, then commit**

```bash
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add src/Generator.cpp include/rune/codec/Generator.h tools/make_allfixed_sample_blob.cpp \
        tests/FlyweightGeneratedRoundTripTest.cpp tests/GeneratorTest.cpp CMakeLists.txt
git commit -m "feat(codec): generated XEncoder/XDecoder flyweights for all-fixed schemas (2b-B)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 3: client — `ProduceFrameBuilder` in-place record reservation

**Files:**
- Modify: `rune-client-cpp/include/rune/client/producer/ProduceFrameBuilder.h:66-99` (the `append` comment + body) and its `private:` section (line 153)
- Test: `rune-client-cpp/tests/producer/ProduceFrameBuilderTest.cpp` (append; already in `rune_pure_tests`)

**Interfaces:**
- Produces (class `rune::client::producer::ProduceFrameBuilder`):
  `static constexpr std::size_t kRecordOverheadMax = 32;`
  `std::size_t reserveRecord(std::string_view key, std::size_t valueLen, int64_t timestamp);` — appends one record whose value bytes the caller writes later at `data() + returned offset`; byte-identical to `append()` once written.
  `void reserveExtra(std::size_t n);` — capacity for n more bytes (appends / reservations within it never reallocate, never throw).
  `struct Mark { std::size_t size; int32_t recordCount; int64_t maxTimestamp; };` `Mark mark() const noexcept;` `void rollback(const Mark&) noexcept;`

- [ ] **Step 1: Write the failing tests** — append to `rune-client-cpp/tests/producer/ProduceFrameBuilderTest.cpp`:

```cpp
// Phase 2b Part B: an Invoker claim reserves a record in the frame and the caller
// writes the value in place — the sealed frame must equal append()'s exactly.
TEST(ProduceFrameBuilder, ReservedRecordFilledInPlaceIsByteIdenticalToAppend) {
    std::mt19937_64 rng(0x2B0B);
    for (int iter = 0; iter < 200; ++iter) {
        const int64_t base = 1'700'000'000'000;
        ProduceFrameBuilder a(0, base);
        ProduceFrameBuilder b(0, base);
        const int recs = 1 + static_cast<int>(rng() % 20);
        for (int r = 0; r < recs; ++r) {
            const std::string key = randomBytes(rng, rng() % 17);
            const std::string val = randomBytes(rng, rng() % 3000);
            const int64_t ts = base + static_cast<int64_t>(rng() % 100'000);
            a.append(key, val, ts);
            const std::size_t at = b.reserveRecord(key, val.size(), ts);
            std::memcpy(b.data() + at, val.data(), val.size());
        }
        ASSERT_EQ(a.recordCount(), b.recordCount());
        ASSERT_EQ(a.bodyBytes(), b.bodyBytes());
        const rpc::Produce m = header(rng);
        const std::size_t la = a.seal(m);
        const std::size_t lb = b.seal(m);
        ASSERT_EQ(std::vector<uint8_t>(a.data(), a.data() + la), std::vector<uint8_t>(b.data(), b.data() + lb))
            << "iter " << iter;
    }
}

TEST(ProduceFrameBuilder, RollbackRestoresTheExactFrameAndMaxTimestamp) {
    ProduceFrameBuilder fb(0, 1000);
    fb.append("k", "first", 1005);
    const ProduceFrameBuilder::Mark m = fb.mark();
    const std::vector<uint8_t> before(fb.data(), fb.data() + fb.size());
    (void)fb.reserveRecord("k", 40, 9000);  // a later max timestamp
    (void)fb.reserveRecord("k", 2, 9001);
    EXPECT_EQ(fb.recordCount(), 3);
    fb.rollback(m);
    EXPECT_EQ(fb.recordCount(), 1);
    EXPECT_EQ(std::vector<uint8_t>(fb.data(), fb.data() + fb.size()), before);
    ProduceFrameBuilder ref(0, 1000);
    ref.append("k", "first", 1005);
    rpc::Produce msg{};
    const std::size_t lf = fb.seal(msg);
    const std::size_t lr = ref.seal(msg);
    EXPECT_EQ(std::vector<uint8_t>(fb.data(), fb.data() + lf), std::vector<uint8_t>(ref.data(), ref.data() + lr))
        << "maxTimestamp must roll back too";
}

TEST(ProduceFrameBuilder, ReserveExtraKeepsDataStableAcrossReservations) {
    ProduceFrameBuilder fb(0, 0);
    fb.reserveExtra(10 * (100 + ProduceFrameBuilder::kRecordOverheadMax));
    const uint8_t* d = fb.data();
    for (int i = 0; i < 10; ++i) (void)fb.reserveRecord("", 100, 0);
    EXPECT_EQ(fb.data(), d) << "reservations inside reserveExtra() never reallocate";
    EXPECT_EQ(fb.recordCount(), 10);
}
```

Add `#include <cstring>` to the file's includes.

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j8 --target rune_pure_tests`
Expected: compile error `no member named 'reserveRecord' in 'rune::client::producer::ProduceFrameBuilder'`.

- [ ] **Step 3: Implement** — in `ProduceFrameBuilder.h`, add after `kRecordsStart`:

```cpp
    /// Upper bound of one record's framing bytes beyond key + value: length
    /// varint (5) + attributes (1) + tsDelta varlong (10) + offsetDelta (5) +
    /// keyLen (5) + valueLen (5) + headerCount (1).
    static constexpr std::size_t kRecordOverheadMax = 32;
```

Replace the whole `append(...)` member (its doc comment stays) with:

```cpp
    void append(std::string_view key, std::string_view value, int64_t timestamp) {
        const auto* v = reinterpret_cast<const uint8_t*>(value.data());
        (void)writePrefix_(key, value.size(), timestamp);
        frame_.insert(frame_.end(), v, v + value.size());
        frame_.push_back(0);  // headerCount = varint(0)
        noteRecord_(timestamp);
    }

    /// Phase 2b Part B (Invoker claims): append one record whose `valueLen`
    /// value bytes the caller writes later, in place, at data() + the returned
    /// offset.  Byte-identical to append() once the value is written.  Call
    /// reserveExtra() first when data() must not move (it then cannot throw).
    std::size_t reserveRecord(std::string_view key, std::size_t valueLen, int64_t timestamp) {
        const std::size_t at = writePrefix_(key, valueLen, timestamp);
        frame_.resize(at + valueLen + 1);
        frame_[at + valueLen] = 0;  // headerCount = varint(0)
        noteRecord_(timestamp);
        return at;
    }

    /// Capacity for `n` more bytes: appends / reservations within it never
    /// reallocate (so they never throw and data() never moves).
    void reserveExtra(std::size_t n) { frame_.reserve(frame_.size() + n); }

    /// A point to roll back to (an aborted / abandoned claim).
    struct Mark {
        std::size_t size         = 0;
        int32_t     recordCount  = 0;
        int64_t     maxTimestamp = 0;
    };
    [[nodiscard]] Mark mark() const noexcept { return Mark{frame_.size(), recordCount_, maxTimestamp_}; }
    void rollback(const Mark& m) noexcept {
        frame_.resize(m.size);  // shrink only: never allocates
        recordCount_  = m.recordCount;
        maxTimestamp_ = m.maxTimestamp;
    }
```

In the `private:` section (before `static void putLE`), add:

```cpp
    /// [length varint | attrs | tsDelta | offsetDelta | keyLen | key | valueLen]
    /// — the exact bytes RecordBatchBuilder::append produces before the value;
    /// returns the frame offset where the value goes.
    std::size_t writePrefix_(std::string_view key, std::size_t valueLen, int64_t timestamp) {
        using ::rune::protocol::writeVarint;
        using ::rune::protocol::writeVarlong;
        uint8_t pre[48];
        std::size_t n = 0;
        pre[n++] = 0;  // attributes
        n += writeVarlong(timestamp - baseTimestamp_, pre + n);
        n += writeVarint(recordCount_, pre + n);
        n += writeVarint(static_cast<int32_t>(key.size()), pre + n);
        uint8_t vlen[10];
        const std::size_t vn = writeVarint(static_cast<int32_t>(valueLen), vlen);
        const std::size_t bodySize = n + key.size() + vn + valueLen + 1;  // +1 headerCount
        uint8_t blen[10];
        const std::size_t bn = writeVarint(static_cast<int32_t>(bodySize), blen);
        const auto* k = reinterpret_cast<const uint8_t*>(key.data());
        frame_.insert(frame_.end(), blen, blen + bn);
        frame_.insert(frame_.end(), pre, pre + n);
        frame_.insert(frame_.end(), k, k + key.size());
        frame_.insert(frame_.end(), vlen, vlen + vn);
        return frame_.size();
    }
    void noteRecord_(int64_t timestamp) noexcept {
        if (timestamp > maxTimestamp_) maxTimestamp_ = timestamp;
        ++recordCount_;
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j8 --target rune_pure_tests && ctest --test-dir build -R 'ProduceFrameBuilder' --output-on-failure`
Expected: all `ProduceFrameBuilder.*` PASS, including the existing `SealedFrameIsByteIdenticalToTheProtocolEncoders` (append is unchanged on the wire).

- [ ] **Step 5: Regression gate + full suite, then commit**

```bash
cmake --build build -j8
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add include/rune/client/producer/ProduceFrameBuilder.h tests/producer/ProduceFrameBuilderTest.cpp
git commit -m "feat(producer): ProduceFrameBuilder reserveRecord/mark/rollback for in-place claims (2b-B)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: client — `Claim` + Thread-mode (Ring) `Producer::tryClaim`

**Files:**
- Modify: `rune-client-cpp/include/rune/client/producer/Producer.h` (after `enum class SendResult` (lines 229–245); the public API after the AckToken `send` overloads (≈ line 431); the `private:` section (≈ line 569))
- Create: `rune-client-cpp/src/producer/Claim.cpp`
- Modify: `rune-client-cpp/src/producer/Producer.cpp` (new section before `// Tier-2: explicit atomic batch`)
- Modify: `rune-client-cpp/include/rune/client/Completions.h` (forward declaration + friend at ≈ line 297)
- Modify: `rune-client-cpp/include/rune/client/producer/ProducerImpl.h` (public claim API next to `trySubmit`)
- Modify: `rune-client-cpp/src/producer/ProducerImpl.cpp` (claim bodies before `ProducerImpl::~ProducerImpl()` at ≈ line 3191)
- Create: `rune-client-cpp/tests/support/FrameCapture.h`
- Create: `rune-client-cpp/tests/producer/ProducerClaimTest.cpp`
- Modify: `rune-client-cpp/CMakeLists.txt` (`rune_client_producer` sources; `rune_invoker_tests` sources)

**Interfaces:**
- Consumes: Task 3 (nothing yet — Ring claims never touch the builder).
- Produces (namespace `rune::client`):
  - `namespace detail { struct ClaimOpen { producer::ProducerImpl* impl; int64_t token; Completions* cq; Completions* bq; uint32_t units; }; struct ClaimCredit { static bool commit(const ClaimOpen&) noexcept; static void abort(const ClaimOpen&) noexcept; }; }`
  - `class Claim` — move-only RAII: `explicit operator bool() const noexcept`, `SendResult result() const noexcept`, `std::span<uint8_t> buffer() const noexcept`, `bool commit() noexcept`, `void abort() noexcept`, destructor aborts. States `Refused{why}` / `Open{core, value, len}` / `Settled`.
  - `Producer::tryClaim(TopicHandle, const Key&, uint32_t valueLen) noexcept -> Claim`; `tryClaim(..., Completions& cq, Token) noexcept -> Claim`; `tryClaim(..., AckToken) noexcept -> Claim`.
  - `ProducerImpl::ClaimGrant { SendResult result; int64_t token; uint8_t* base; const uint32_t* offsets; }`; `ClaimGrant claimRecord(int32_t topicId, int32_t partition, const Key&, uint32_t valueLen, uint64_t ackHandle) noexcept`; `bool commitClaim(int64_t token) noexcept`; `void abortClaim(int64_t token) noexcept`.
  - Test helper `test_support::FrameCapture` (`recs`, `corrs`, `frames`, `watch`, `watchInFrame`, `fn()`, `values()`), `test_support::CapturedRecord{partition, frame, offset, corr, key, value}`.

- [ ] **Step 1: Write the failing tests**

Create `rune-client-cpp/tests/support/FrameCapture.h`:

```cpp
// tests/support/FrameCapture.h — Phase 2b Part B test helper: a SendFn sink that
// decodes every Produce frame a bare ProducerImpl offers and records each record
// (partition, frame ordinal, offset within its batch, key, value) in order.
// `watch` (optional): a claimed buffer address; watchInFrame turns true when an
// offered frame contains it (the Invoker "the claim IS the frame" proof).
#pragma once
#include <rune/client/codec/DataCodec.h>
#include <rune/client/producer/ProducerImpl.h>
#include <rune/protocol/RecordBatch.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace test_support {

struct CapturedRecord {
    int32_t     partition = 0;
    int         frame     = 0;
    int64_t     offset    = 0;  // offsetDelta within its batch (the client's baseOffset is 0)
    int64_t     corr      = 0;  // the frame's correlation id
    std::string key;
    std::string value;
};

struct FrameCapture {
    std::vector<CapturedRecord> recs;
    std::vector<int64_t>        corrs;  // one per frame
    int                         frames       = 0;
    const uint8_t*              watch        = nullptr;
    bool                        watchInFrame = false;

    rune::client::producer::ProducerImpl::SendFn fn() {
        return [this](int32_t, int32_t partition, int32_t, const uint8_t* b, std::size_t n) {
            if (watch != nullptr && watch >= b && watch < b + n) watchInFrame = true;
            const auto v = rune::client::codec::decodeProduce(b, n);
            if (!v) return true;
            const int f = frames++;
            corrs.push_back(v->correlationId);
            rune::protocol::RecordBatchReader r(v->recordBatch.data(), v->recordBatch.size());
            r.forEachRecord([&](std::string_view k, std::string_view val, int64_t off, int64_t) {
                recs.push_back(CapturedRecord{partition, f, off, v->correlationId, std::string(k), std::string(val)});
            });
            return true;
        };
    }
    [[nodiscard]] std::vector<std::string> values() const {
        std::vector<std::string> out;
        for (const auto& r : recs) out.push_back(r.value);
        return out;
    }
};

}  // namespace test_support
```

Create `rune-client-cpp/tests/producer/ProducerClaimTest.cpp`:

```cpp
// tests/producer/ProducerClaimTest.cpp — Phase 2b Part B: zero-copy produce
// (Producer::tryClaim / claimBatch) on a bare ProducerImpl, Thread (Ring) and
// Invoker (Direct) ingress: commit / abort / destructor-abort, back-pressure,
// limits, ordering vs sends and Tier-2 batches, the copy-count proofs, and the
// zero-allocation gates.  Every drive loop is iteration-capped; nothing sleeps.
#include "../support/AckLedger.h"
#include "../support/AllocCounter.h"
#include "../support/FrameCapture.h"

#include <rune/client/Key.h>
#include <rune/client/TopicHandle.h>
#include <rune/client/producer/BatchBuilder.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace rc = rune::client;
using rc::Acks;
using rc::Key;
using rc::Producer;
using rc::ProducerConfig;
using rc::SendResult;
using rc::TopicHandle;
using rc::producer::ProducerImpl;
using test_support::FrameCapture;

namespace {
constexpr int64_t kNow = 1'000'000'000;
const TopicHandle kTopic{1, 4};

ProducerConfig voidCfg() {
    ProducerConfig c;
    c.acks        = Acks::None;
    c.lingerUs    = 0;
    c.batchBytes  = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}

std::vector<uint8_t> bytes(std::string_view s) { return {s.begin(), s.end()}; }

void fill(std::span<uint8_t> b, std::string_view s) {
    std::memcpy(b.data(), s.data(), std::min(b.size(), s.size()));
}

// Ring mode: drain the producer ring and flush, a fixed number of passes (a
// pass may stop at a batch boundary; 16 always reach the end in these tests).
void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    for (int i = 0; i < 16; ++i) {
        impl.drainRing(kNow, 256);
        impl.flushBatches(kNow, 42, sink);
    }
}
}  // namespace

// ---------------------------------------------------------------------------
// Thread mode (Ring ingress)
// ---------------------------------------------------------------------------

TEST(ProducerClaim, RingClaimCommitDeliversTheRecordWrittenInPlace) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{7});
    rc::Claim c = p.tryClaim(kTopic, key, 5);
    ASSERT_TRUE(c) << static_cast<int>(c.result());
    EXPECT_EQ(c.result(), SendResult::Ok);
    ASSERT_EQ(c.buffer().size(), 5u);
    fill(c.buffer(), "hello");
    EXPECT_TRUE(c.commit());
    EXPECT_FALSE(c) << "a committed claim is settled";
    EXPECT_FALSE(c.commit()) << "settled: a second commit is a no-op";
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 1u);
    EXPECT_EQ(cap.recs[0].value, "hello");
    EXPECT_EQ(cap.recs[0].key, std::string(reinterpret_cast<const char*>(key.bytes().data()), key.size()));
    EXPECT_EQ(cap.recs[0].partition, key.partition(kTopic.partitionCount));
}

TEST(ProducerClaim, RingAbortAndDestructorDeliverNothing) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    {
        rc::Claim a = p.tryClaim(kTopic, Key(uint64_t{1}), 3);
        ASSERT_TRUE(a);
        fill(a.buffer(), "AAA");
        a.abort();
        EXPECT_FALSE(a);
        EXPECT_FALSE(a.commit());
    }
    {
        rc::Claim d = p.tryClaim(kTopic, Key(uint64_t{2}), 3);
        ASSERT_TRUE(d);
        fill(d.buffer(), "DDD");
    }  // the destructor aborts
    ASSERT_EQ(p.trySend(kTopic, Key(uint64_t{3}), bytes("CCC")), SendResult::Ok);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"CCC"});
}

TEST(ProducerClaim, RingClaimIsBackPressuredWhenTheRingIsFullAndLosesNothing) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);
    Producer p(impl);
    std::vector<rc::Claim> held;
    for (int i = 0; i < 10'000; ++i) {
        rc::Claim c = p.tryClaim(kTopic, Key(static_cast<uint64_t>(i)), 64);
        if (!c) {
            EXPECT_EQ(c.result(), SendResult::BackPressured);
            break;
        }
        std::memset(c.buffer().data(), i & 0xFF, 64);
        held.push_back(std::move(c));
    }
    ASSERT_FALSE(held.empty());
    ASSERT_LT(held.size(), 10'000u) << "a 4 KiB ring must fill";
    for (auto& c : held) EXPECT_TRUE(c.commit());
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.recs.size(), held.size());
}

TEST(ProducerClaim, OverCapAndInvalidClaimsAreRefusedNeverRetryable) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);  // maxMsgLength 8 KiB
    Producer p(impl);
    const Key key(uint64_t{1});  // 8 key bytes
    EXPECT_EQ(p.tryClaim(kTopic, key, static_cast<uint32_t>(Producer::kMaxRecordBytes - 8 + 1)).result(),
              SendResult::MessageTooLarge);
    EXPECT_EQ(p.tryClaim(kTopic, key, 9000).result(), SendResult::ValueTooLarge)
        << "fits 16 KiB but never this ring";
    rc::Claim bad = p.tryClaim(TopicHandle{}, key, 4);
    EXPECT_FALSE(bad);
    EXPECT_EQ(bad.result(), SendResult::InvalidTopic);
    EXPECT_TRUE(bad.buffer().empty());
    EXPECT_FALSE(bad.commit());
    EXPECT_EQ(p.tryClaim(kTopic, key, 4, rc::AckToken{rc::kMaxToken + 1}).result(), SendResult::InvalidArgument);
}

TEST(ProducerClaim, ClosedProducerRefusesClaimsWithClosed) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    ASSERT_EQ(p.closeImmediately(), rc::OpResult::Accepted);
    EXPECT_EQ(p.tryClaim(kTopic, Key(uint64_t{1}), 4).result(), SendResult::Closed);
}

TEST(ProducerClaim, AckTokenClaimCompletesWithItsOffsetAndAnAbortedOneNever) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    auto* impl = new ProducerImpl(cfg, 64 * 1024);
    Producer p(impl);
    test_support::AckLedger L;
    L.attach(p);
    rc::Claim a = p.tryClaim(kTopic, Key(uint64_t{1}), 4, rc::AckToken{11});
    ASSERT_TRUE(a);
    fill(a.buffer(), "aaaa");
    rc::Claim b = p.tryClaim(kTopic, Key(uint64_t{1}), 4, rc::AckToken{12});
    ASSERT_TRUE(b);
    b.abort();
    ASSERT_TRUE(a.commit());
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.corrs.size(), 1u);
    impl->onProduceResponse(cap.corrs[0], 500, 0);
    ASSERT_EQ(L.recs.size(), 1u);
    EXPECT_EQ(L.recs[0].token, 11u);
    EXPECT_EQ(L.recs[0].value, 500);
    EXPECT_EQ(L.recs[0].status, rc::OpStatus::Ok);
    EXPECT_EQ(L.times(12), 0) << "an aborted claim never completes";
}

// Spec B copy count, Thread mode: the claim lives in the producer ring; the
// duty side copies it into the Produce frame (copy 1), the transport copies the
// frame (copy 2).
TEST(ProducerClaim, RingClaimIsCopiedOnceIntoTheFrame) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    rc::Claim c = p.tryClaim(kTopic, Key(uint64_t{3}), 8);
    ASSERT_TRUE(c);
    fill(c.buffer(), "RINGBYTE");
    FrameCapture cap;
    cap.watch = c.buffer().data();
    ASSERT_TRUE(c.commit());
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.values(), std::vector<std::string>{"RINGBYTE"});
    EXPECT_FALSE(cap.watchInFrame) << "Thread mode: ring -> frame -> Aeron (2 internal copies)";
}

// Spec B ordering: a claim never overtakes a Tier-2 batch queued before it on
// the same partition (same rule as a send).
TEST(ProducerClaim, ClaimNeverOvertakesAQueuedTier2Batch) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{5});
    ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    auto t2 = p.batch(kTopic, key);
    t2.add(bytes("X"));
    t2.add(bytes("Y"));
    ASSERT_EQ(t2.send(), rc::OpResult::Accepted);
    rc::Claim c = p.tryClaim(kTopic, key, 1);
    ASSERT_TRUE(c);
    fill(c.buffer(), "C");
    ASSERT_TRUE(c.commit());
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "X", "Y", "C"}));
}

TEST(ProducerClaimAllocRegression, RingClaimSteadyStateAllocatesNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    Producer p(impl);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        rc::Claim c = p.tryClaim(kTopic, Key(k), 64);
        if (c) {
            std::memset(c.buffer().data(), 0x5A, 64);
            (void)c.commit();
        }
        pump(*impl, sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);  // warm-up: builders, frames, ring
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "tryClaim + commit + drain/flush must not allocate";
}
```

In `rune-client-cpp/CMakeLists.txt`, add `tests/producer/ProducerClaimTest.cpp` to the `add_executable(rune_invoker_tests …)` list (after `tests/producer/ProducerCloseTest.cpp`) and `src/producer/Claim.cpp` to `add_library(rune_client_producer STATIC …)` (after `src/producer/Producer.cpp`).

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j8 --target rune_invoker_tests`
Expected: compile error `no member named 'Claim' in namespace 'rune::client'` / `no member named 'tryClaim' in 'rune::client::Producer'`.

- [ ] **Step 3: Implement**

`Producer.h` — add `#include <variant>` to the includes, then insert directly after the closing `};` of `enum class SendResult`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b Part B — zero-copy produce: Claim (one record) / BatchClaim (n
// records appended as ONE RecordBatch).
//
// The caller writes the record straight into the producer's memory:
//   auto c = p.tryClaim(topic, Key(id), PositionEncoder::kEncodedLength);
//   if (c) { PositionEncoder(c.buffer()).zone(4).x(118.5); c.commit(); }
// Copies left (spec §B): Thread mode 2 (producer ring -> Produce frame ->
// Aeron); Invoker mode 1 (the claim IS the frame's bytes; frame -> Aeron).
//
// RAII + move-only: commit() publishes, abort() discards, the destructor aborts
// an open claim.  A refused claim is false; result() says why (BackPressured:
// retry later; MessageTooLarge / ValueTooLarge / InvalidTopic / InvalidArgument
// / Closed / WrongThread / QueueFull / ClaimHeld exactly as for trySend).  The
// claimed bytes are NOT zeroed (a generated XEncoder zeroes its block).
//
// A held claim blocks its producer: Thread mode — the duty side cannot read
// past it, so never hold one across blocking work and never call a blocking
// send() on the same producer from the thread holding it; Invoker mode — until
// it is settled every other send / claim on the producer returns ClaimHeld and
// its partition is not flushed, and it must be settled on the owner thread.
// A claim carrying Completions credit is settled on that queue's owner thread.
// A claim must not outlive its Producer.  Zero allocation.
// ---------------------------------------------------------------------------
namespace detail {
/// The state of an OPEN claim — exactly what settling it needs.
struct ClaimOpen {
    producer::ProducerImpl* impl  = nullptr;
    int64_t                 token = 0;        ///< Ring: the M2O claim index; Direct: the claim generation
    Completions*            cq    = nullptr;  ///< per-record ack credit (tryClaim(.., cq, token)); nullptr: none
    Completions*            bq    = nullptr;  ///< batch-ack credit (batchAcksTo); nullptr: none
    uint32_t                units = 0;        ///< batch-ack units reserved on bq
};
/// Settles a claim and its Completions credit (a friend of Completions).
struct ClaimCredit {
    /// Publish; true = accepted (its completions follow), false = abandoned
    /// (nothing follows: every reservation is returned).
    static bool commit(const ClaimOpen& o) noexcept;
    /// Discard; every reservation is returned.
    static void abort(const ClaimOpen& o) noexcept;
};
}  // namespace detail

class Claim {
public:
    Claim() noexcept = default;  ///< refused: InvalidTopic
    Claim(Claim&& o) noexcept;
    Claim& operator=(Claim&& o) noexcept;  ///< aborts this claim first if it is open
    Claim(const Claim&) = delete;
    Claim& operator=(const Claim&) = delete;
    ~Claim();  ///< aborts an open claim

    /// True while open (granted, neither committed nor aborted).
    [[nodiscard]] explicit operator bool() const noexcept { return std::holds_alternative<Open>(state_); }
    /// Ok if the claim was granted; otherwise why it was refused.
    [[nodiscard]] SendResult result() const noexcept;
    /// The record's value bytes (exactly the claimed length); empty unless open.
    [[nodiscard]] std::span<uint8_t> buffer() const noexcept;
    /// Publish the record.  True: accepted, exactly like an Ok send (its ack /
    /// BatchAck follows).  False: nothing was open, or the claim was abandoned
    /// under it (Invoker: its partition was failed / the producer closed;
    /// Thread: Client close released it) — nothing is sent, nothing completes.
    bool commit() noexcept;
    /// Discard the record: nothing is sent, nothing completes.  No-op unless open.
    void abort() noexcept;

private:
    friend class Producer;
    struct Refused {
        SendResult why = SendResult::InvalidTopic;
    };
    struct Open {
        detail::ClaimOpen core;
        uint8_t*          value = nullptr;
        uint32_t          len   = 0;
    };
    struct Settled {};
    explicit Claim(SendResult why) noexcept : state_(Refused{why}) {}
    Claim(const detail::ClaimOpen& core, uint8_t* value, uint32_t len) noexcept
        : state_(Open{core, value, len}) {}
    std::variant<Refused, Open, Settled> state_{Refused{}};
};
```

In `class Producer`, after the `void send(..., AckToken token);` declaration, add:

```cpp
    // ---- Phase 2b Part B: zero-copy produce (ZERO allocation) -----------------

    /// Claim one record of `valueLen` value bytes (key + valueLen <=
    /// kMaxRecordBytes), routed by `key` exactly like trySend(topic, key, ..):
    /// write it through claim.buffer() (e.g. a generated XEncoder), then
    /// commit().  Non-blocking: a refused claim is false and claim.result()
    /// says why.  Thread rules as trySend (Invoker: owner thread only).  See
    /// Claim for the rules while it is held.
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen) noexcept;
    /// tryClaim with a polled completion (trySend(.., cq, token)'s rules): the
    /// credit is reserved at claim time and returned if the claim is aborted.
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, Completions& cq,
                                 Token token) noexcept;
    /// tryClaim whose completion goes to the onAck() handler.
    [[nodiscard]] Claim tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, AckToken token) noexcept;
```

In the `private:` section of `Producer`, add:

```cpp
    /// 2b-B: the one claim body; `part` computed once by the caller, the
    /// per-record ack credit (cq) already reserved by the caller.
    [[nodiscard]] Claim claimPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t valueLen,
                                   uint64_t ackHandle, Completions* cq) noexcept;
```

`Completions.h` — directly after `namespace rune::client {` add:

```cpp
namespace detail {
struct ClaimCredit;  // Phase 2b Part B (producer/Producer.h)
}  // namespace detail
```

and next to `friend class BatchBuilder;` add:

```cpp
    friend struct detail::ClaimCredit;  // 2b-B: claim credit (Claim / BatchClaim)
```

`ProducerImpl.h` — in the public "App-thread API" section, directly after the `trySubmit(...)` declaration, add:

```cpp
    // ---- Phase 2b Part B: zero-copy claims (Producer::tryClaim / claimBatch) ----

    /// What a granted claim hands to Claim / BatchClaim.
    struct ClaimGrant {
        SendResult      result  = SendResult::BackPressured;
        int64_t         token   = 0;        ///< Ring: the M2O claim index; Direct: the claim generation
        uint8_t*        base    = nullptr;  ///< value bytes of record 0
        const uint32_t* offsets = nullptr;  ///< Direct batch: record i at base + offsets[i]; nullptr: stride
    };
    /// Reserve one record of `valueLen` value bytes: Ring — a SEND_RECORD slot
    /// in the producer ring with its header written; the caller writes the
    /// value and commitClaim()s.  ZERO allocation.  Results as trySubmit.
    [[nodiscard]] ClaimGrant claimRecord(int32_t topicId, int32_t partition, const Key& key,
                                         uint32_t valueLen, uint64_t ackHandle) noexcept;
    /// Publish a granted claim.  False: it was released under the caller
    /// (Ring: Client close's drain unblock) — nothing was accepted.
    [[nodiscard]] bool commitClaim(int64_t token) noexcept;
    /// Discard a granted claim (Ring: the slot becomes padding).
    void abortClaim(int64_t token) noexcept;
```

`ProducerImpl.cpp` — insert directly before `ProducerImpl::~ProducerImpl() {`:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b Part B: zero-copy claims
// ---------------------------------------------------------------------------

namespace {
// The SEND_RECORD / CLAIM_BATCH common header [0..33] (topicId, partition,
// keyLen, inline key zero-padded to 16, flags, ackHandle).
inline void writeClaimHeader(uint8_t* d, int32_t topicId, int32_t partition, const Key& key,
                             uint64_t ackHandle) noexcept {
    writeLE32(d, kRecTopicIdOff, topicId);
    writeLE32(d, kRecPartOff, partition);
    d[kRecKeyLenOff] = static_cast<uint8_t>(key.size());
    std::memset(d + kRecKeyBytesOff, 0, Key::kMaxInline);
    if (key.size() > 0) std::memcpy(d + kRecKeyBytesOff, key.bytes().data(), key.size());
    d[kRecFlagsOff] = ackHandle != 0 ? kFlagWantAck : uint8_t{0};
    writeLE64(d, kRecCorrIdOff, static_cast<int64_t>(ackHandle));
}
}  // namespace

ProducerImpl::ClaimGrant ProducerImpl::claimRecord(int32_t topicId, int32_t partition, const Key& key,
                                                   uint32_t valueLen, uint64_t ackHandle) noexcept {
    ClaimGrant g;
    // Backlog B6: the per-record cap, one O(1) compare (key + value <= 16 KiB).
    if (key.size() + valueLen > rune::client::Producer::kMaxRecordBytes) {
        g.result = SendResult::MessageTooLarge;
        return g;
    }
    // fix1 minor 11 parity: a record this ring can NEVER hold is not back-pressure.
    const std::size_t totalLen = static_cast<std::size_t>(kRecHeaderSize) + valueLen;
    if (totalLen > static_cast<std::size_t>(ring_.maxMsgLength())) {
        g.result = SendResult::ValueTooLarge;
        return g;
    }
    aeron::util::index_t idx = -1;
    try {
        idx = ring_.tryClaim(kMsgSendRecord, totalLen);
    } catch (...) {
        g.result = SendResult::BackPressured;  // unreachable: type and length are pre-checked
        return g;
    }
    if (idx < 0) {
        // F8c C1(a): the closed bit lives in the tail word the claim CASes.
        g.result = ring_.closed() ? SendResult::Closed : SendResult::BackPressured;
        return g;
    }
    uint8_t* dst = ring_.base() + idx;
    writeClaimHeader(dst, topicId, partition, key, ackHandle);
    writeLE32u(dst, kRecValLenOff, valueLen);
    g.result = SendResult::Ok;
    g.token  = idx;
    g.base   = dst + kRecValBytesOff;
    return g;
}

bool ProducerImpl::commitClaim(int64_t token) noexcept {
    try {
        ring_.commit(static_cast<aeron::util::index_t>(token));
        return true;
    } catch (...) {
        // Released by Client close's drain unblock (closeFinal): the slot is
        // padding now and is never read — nothing was accepted.
        return false;
    }
}

void ProducerImpl::abortClaim(int64_t token) noexcept {
    try {
        ring_.abort(static_cast<aeron::util::index_t>(token));
    } catch (...) {
        // Already released by the close-drain unblock: nothing left to discard.
    }
}
```

Create `rune-client-cpp/src/producer/Claim.cpp`:

```cpp
// src/producer/Claim.cpp — Phase 2b Part B: Claim (zero-copy produce) and the
// claim credit settlement shared with BatchClaim.
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <utility>

namespace rune::client {

namespace detail {

namespace {
void giveBack(const ClaimOpen& o) noexcept {
    if (o.cq != nullptr) o.cq->unreserve(producer::kAckRecordBytes);
    if (o.bq != nullptr) {
        for (uint32_t i = 0; i < o.units; ++i) o.bq->unreserve(producer::kAckRecordBytes);
    }
}
}  // namespace

bool ClaimCredit::commit(const ClaimOpen& o) noexcept {
    if (!o.impl->commitClaim(o.token)) {
        giveBack(o);  // abandoned: no completion will ever arrive
        return false;
    }
    if (o.cq != nullptr) ++o.cq->producerIssued_;
    if (o.bq != nullptr) o.bq->producerIssued_ += o.units;
    return true;
}

void ClaimCredit::abort(const ClaimOpen& o) noexcept {
    o.impl->abortClaim(o.token);
    giveBack(o);
}

}  // namespace detail

Claim::Claim(Claim&& o) noexcept : state_(std::exchange(o.state_, Settled{})) {}

Claim& Claim::operator=(Claim&& o) noexcept {
    if (this != &o) {
        abort();
        state_ = std::exchange(o.state_, Settled{});
    }
    return *this;
}

Claim::~Claim() { abort(); }

SendResult Claim::result() const noexcept {
    if (const auto* r = std::get_if<Refused>(&state_)) return r->why;
    return SendResult::Ok;
}

std::span<uint8_t> Claim::buffer() const noexcept {
    if (const auto* o = std::get_if<Open>(&state_)) return {o->value, o->len};
    return {};
}

bool Claim::commit() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return false;
    const detail::ClaimOpen core = o->core;
    state_ = Settled{};
    return detail::ClaimCredit::commit(core);
}

void Claim::abort() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return;
    const detail::ClaimOpen core = o->core;
    state_ = Settled{};
    detail::ClaimCredit::abort(core);
}

}  // namespace rune::client
```

`Producer.cpp` — insert directly before the `// Tier-2: explicit atomic batch` section banner:

```cpp
// ---------------------------------------------------------------------------
// Phase 2b Part B: zero-copy produce
// ---------------------------------------------------------------------------

namespace {
// The batch-ack credit of a claim: `units` reservations of ONE unit each — the
// units a Tier-1 BatchAck returns per record.  All or none.
[[nodiscard]] SendResult reserveBatchUnits(Completions& bq, uint32_t units) noexcept {
    if (!bq.valid()) return SendResult::InvalidArgument;
    if (!bq.onOwnerThread()) return SendResult::WrongThread;
    for (uint32_t i = 0; i < units; ++i) {
        const OpResult r = bq.reserve(producer::kAckRecordBytes);
        if (r != OpResult::Accepted) {
            for (uint32_t j = 0; j < i; ++j) bq.unreserve(producer::kAckRecordBytes);
            return mapReserve(r);
        }
    }
    return SendResult::Ok;
}
}  // namespace

Claim Producer::claimPart_(TopicHandle topic, int32_t part, const Key& key, uint32_t valueLen,
                           uint64_t ackHandle, Completions* cq) noexcept {
    const producer::ProducerImpl::ClaimGrant g =
        impl_->claimRecord(topic.topicId, part, key, valueLen, ackHandle);
    if (g.result != SendResult::Ok) {
        if (cq != nullptr) cq->unreserve(producer::kAckRecordBytes);  // nothing will ever complete
        return Claim(g.result);
    }
    detail::ClaimOpen core{impl_, g.token, cq, nullptr, 0};
    if (Completions* bq = impl_->batchAckCq(); bq != nullptr) [[unlikely]] {
        const SendResult u = reserveBatchUnits(*bq, 1);
        if (u != SendResult::Ok) {
            detail::ClaimCredit::abort(core);  // discards the record, returns cq's credit
            return Claim(u);
        }
        core.bq    = bq;
        core.units = 1;
    }
    return Claim(core, g.base, valueLen);
}

Claim Producer::tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen) noexcept {
    if (!impl_ || !topic.valid()) return Claim(SendResult::InvalidTopic);
    return claimPart_(topic, key.partition(topic.partitionCount), key, valueLen, 0, nullptr);
}

Claim Producer::tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, Completions& cq,
                         Token token) noexcept {
    if (!impl_ || !topic.valid()) return Claim(SendResult::InvalidTopic);
    if (token > kMaxToken || cq.client_ == nullptr || cq.client_ != impl_->hooks().ctx || cq.ring_ == nullptr) {
        return Claim(SendResult::InvalidArgument);
    }
    if (!cq.onOwnerThread()) return Claim(SendResult::WrongThread);
    const OpResult rr = cq.reserve(producer::kAckRecordBytes);
    if (rr != OpResult::Accepted) return Claim(mapReserve(rr));
    return claimPart_(topic, key.partition(topic.partitionCount), key, valueLen,
                      producer::ackhandle::forCq(cq.cqRef_, token), &cq);
}

Claim Producer::tryClaim(TopicHandle topic, const Key& key, uint32_t valueLen, AckToken token) noexcept {
    if (!impl_ || !topic.valid()) return Claim(SendResult::InvalidTopic);
    if (token.token > kMaxToken) return Claim(SendResult::InvalidArgument);
    return claimPart_(topic, key.partition(topic.partitionCount), key, valueLen,
                      producer::ackhandle::forHandler(token.token), nullptr);
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j8 --target rune_invoker_tests && ctest --test-dir build -R 'ProducerClaim' --output-on-failure`
Expected: the 9 Ring tests + `ProducerClaimAllocRegression.RingClaimSteadyStateAllocatesNothingRegression` PASS.

- [ ] **Step 5: Regression gate + full suite, then commit**

```bash
cmake --build build -j8
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add include/rune/client/producer/Producer.h src/producer/Claim.cpp src/producer/Producer.cpp \
        include/rune/client/Completions.h include/rune/client/producer/ProducerImpl.h \
        src/producer/ProducerImpl.cpp tests/support/FrameCapture.h tests/producer/ProducerClaimTest.cpp \
        CMakeLists.txt
git commit -m "feat(producer): Producer::tryClaim + Claim — zero-copy Thread-mode produce (2b-B)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: client — `BatchClaim` + Thread-mode `claimBatch` (`kMsgClaimBatch = 4`)

**Files:**
- Modify: `rune-client-cpp/include/rune/client/producer/Producer.h` (`BatchClaim` after `Claim`; `claimBatch` after the `tryClaim` overloads)
- Modify: `rune-client-cpp/src/producer/Claim.cpp` (append `BatchClaim`)
- Modify: `rune-client-cpp/src/producer/Producer.cpp` (append `claimBatch` to the 2b-B section)
- Modify: `rune-client-cpp/include/rune/client/producer/ProducerImpl.h:73-76` (message ids), the claim API, the private helpers
- Modify: `rune-client-cpp/src/producer/ProducerImpl.cpp` — anonymous-namespace helper after `recordBodyContrib`; `drainRing` (`:1295-1381`); `appendRecord_` (`:1384-1539`, replaced); `discardRing_` (`:2910-2942`); claim section (Task 4)
- Test: `rune-client-cpp/tests/producer/ProducerClaimTest.cpp` (append)

**Interfaces:**
- Consumes: Task 3 `ProduceFrameBuilder::reserveExtra`; Task 4 `detail::ClaimOpen`, `detail::ClaimCredit`, `ProducerImpl::ClaimGrant`, `reserveBatchUnits` (Producer.cpp), `FrameCapture`.
- Produces:
  - `class BatchClaim` — move-only RAII: `explicit operator bool`, `SendResult result()`, `std::span<uint8_t> record(uint32_t i) const noexcept` (empty for `i >= count()` or unless open), `uint32_t count() const noexcept`, `uint32_t recordLength() const noexcept`, `bool commit() noexcept`, `void abort() noexcept`, destructor aborts. States `Refused` / `Open{core, base, offsets, n, recLen}` / `Settled`.
  - `Producer::claimBatch(TopicHandle, const Key&, uint32_t n, uint32_t recLen) noexcept -> BatchClaim`.
  - Ring message `kMsgClaimBatch = 4`, layout `[0..33]` as SEND_RECORD (flags 0, ackHandle 0), `[34..37] n` u32 LE (`kBatchCountOff`), `[38..41] recLen` u32 LE (`kBatchRecLenOff`), `[42..] n × recLen` contiguous (`kBatchRecsOff`, `kBatchHeaderSize = 42`).
  - `ProducerImpl::claimBatch(int32_t topicId, int32_t partition, const Key&, uint32_t n, uint32_t recLen) noexcept -> ClaimGrant`.
  - Private (Task 6 relies on these names): `void ensureBuilder_(PartitionState&)`; `ControlledAction admitRecord_(PartitionState&, uint8_t keyLen, uint32_t valueLen, uint8_t flags, uint64_t ackHandle)`; `ControlledAction admitBatch_(PartitionState&, uint8_t keyLen, uint32_t n, uint32_t recLen)`; `void appendBatch_(PartitionState&, const uint8_t* keyBytes, uint8_t keyLen, const uint8_t* recs, uint32_t n, uint32_t recLen, int64_t nowNs)`; `void noteAppended_(PartitionState&, int32_t firstIndex, int32_t count, uint8_t flags, uint64_t ackHandle, int64_t nowNs)`; `SendResult checkBatchClaim_(const Key&, uint32_t n, uint32_t recLen) const noexcept`; file-local `int64_t claimBatchWireBound(std::size_t keyLen, uint32_t n, uint32_t recLen)`.

- [ ] **Step 1: Write the failing tests** — append to `tests/producer/ProducerClaimTest.cpp`:

```cpp
// ---------------------------------------------------------------------------
// claimBatch — Thread mode (Ring ingress)
// ---------------------------------------------------------------------------

TEST(ProducerClaim, ClaimBatchLandsAsOneRecordBatchWithContiguousOffsets) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{8});
    rc::BatchClaim b = p.claimBatch(kTopic, key, 3, 2);
    ASSERT_TRUE(b) << static_cast<int>(b.result());
    ASSERT_EQ(b.count(), 3u);
    ASSERT_EQ(b.recordLength(), 2u);
    fill(b.record(0), "B1");
    fill(b.record(1), "B2");
    fill(b.record(2), "B3");
    EXPECT_TRUE(b.record(3).empty()) << "out of range";
    ASSERT_TRUE(b.commit());
    EXPECT_FALSE(b);
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 3u);
    EXPECT_EQ(cap.frames, 1) << "one RecordBatch";
    for (int i = 0; i < 3; ++i) {
        EXPECT_EQ(cap.recs[i].frame, 0);
        EXPECT_EQ(cap.recs[i].offset, i) << "contiguous offsets";
        EXPECT_EQ(cap.recs[i].partition, key.partition(kTopic.partitionCount));
    }
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"B1", "B2", "B3"}));
}

TEST(ProducerClaim, ClaimBatchAbortAndDestructorDeliverNone) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    {
        rc::BatchClaim a = p.claimBatch(kTopic, Key(uint64_t{1}), 4, 4);
        ASSERT_TRUE(a);
        a.abort();
        EXPECT_TRUE(a.record(0).empty());
    }
    {
        rc::BatchClaim d = p.claimBatch(kTopic, Key(uint64_t{1}), 4, 4);
        ASSERT_TRUE(d);
    }  // the destructor aborts
    ASSERT_EQ(p.trySend(kTopic, Key(uint64_t{1}), bytes("Z")), SendResult::Ok);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), std::vector<std::string>{"Z"});
}

// All n records land in ONE batch: they join an open batch only when they fit
// it whole; otherwise that batch is flushed first and the claim starts a new one.
TEST(ProducerClaim, ClaimBatchJoinsAnOpenBatchOnlyWhenItFitsWhole) {
    {
        ProducerConfig cfg = voidCfg();
        cfg.lingerUs = 60'000'000;  // only a flush ends a batch
        auto* impl = new ProducerImpl(cfg, 64 * 1024);
        Producer p(impl);
        const Key key(uint64_t{2});
        ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
        rc::BatchClaim b = p.claimBatch(kTopic, key, 3, 2);
        ASSERT_TRUE(b);
        fill(b.record(0), "B1");
        fill(b.record(1), "B2");
        fill(b.record(2), "B3");
        ASSERT_TRUE(b.commit());
        ASSERT_EQ(p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
        p.flush();
        FrameCapture cap;
        pump(*impl, cap.fn());
        EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B1", "B2", "B3", "C"}));
        EXPECT_EQ(cap.frames, 1);
        for (int i = 0; i < 5; ++i) EXPECT_EQ(cap.recs[i].offset, i);
    }
    {
        ProducerConfig cfg = voidCfg();
        cfg.lingerUs   = 60'000'000;
        cfg.batchBytes = 256;  // A (~75 wire bytes) + 4 x 40-byte records do not fit
        auto* impl = new ProducerImpl(cfg, 64 * 1024);
        Producer p(impl);
        const Key key(uint64_t{2});
        ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
        rc::BatchClaim b = p.claimBatch(kTopic, key, 4, 40);
        ASSERT_TRUE(b) << static_cast<int>(b.result());
        for (uint32_t i = 0; i < 4; ++i) std::memset(b.record(i).data(), 'a' + static_cast<int>(i), 40);
        ASSERT_TRUE(b.commit());
        p.flush();
        FrameCapture cap;
        pump(*impl, cap.fn());
        ASSERT_EQ(cap.recs.size(), 5u);
        EXPECT_EQ(cap.recs[0].value, "A");
        EXPECT_NE(cap.recs[1].frame, cap.recs[0].frame) << "A was flushed first";
        for (int i = 1; i < 5; ++i) {
            EXPECT_EQ(cap.recs[i].frame, cap.recs[1].frame) << "the 4 claimed records share one batch";
            EXPECT_EQ(cap.recs[i].offset, i - 1);
            EXPECT_EQ(cap.recs[i].value, std::string(40, static_cast<char>('a' + i - 1)));
        }
    }
}

TEST(ProducerClaim, ClaimBatchNeverOvertakesAQueuedTier2Batch) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    const Key key(uint64_t{5});
    ASSERT_EQ(p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    auto t2 = p.batch(kTopic, key);
    t2.add(bytes("X"));
    t2.add(bytes("Y"));
    ASSERT_EQ(t2.send(), rc::OpResult::Accepted);
    rc::BatchClaim b = p.claimBatch(kTopic, key, 2, 2);
    ASSERT_TRUE(b);
    fill(b.record(0), "B1");
    fill(b.record(1), "B2");
    ASSERT_TRUE(b.commit());
    ASSERT_EQ(p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "X", "Y", "B1", "B2", "C"}));
}

TEST(ProducerClaim, ClaimBatchLimitsAreRefusedNeverRetryable) {
    {
        ProducerConfig cfg = voidCfg();
        cfg.maxFrameBytes = 4096;
        auto* impl = new ProducerImpl(cfg, 64 * 1024);
        Producer p(impl);
        const Key key(uint64_t{1});  // 8 key bytes
        EXPECT_EQ(p.claimBatch(kTopic, key, 0, 4).result(), SendResult::InvalidArgument);
        EXPECT_EQ(p.claimBatch(kTopic, key, 2, static_cast<uint32_t>(Producer::kMaxRecordBytes - 8 + 1)).result(),
                  SendResult::MessageTooLarge)
            << "per-record cap";
        EXPECT_EQ(p.claimBatch(kTopic, key, 100, 100).result(), SendResult::MessageTooLarge)
            << "~12 KiB batch > maxFrameBytes 4096";
        EXPECT_EQ(p.claimBatch(TopicHandle{}, key, 1, 1).result(), SendResult::InvalidTopic);
    }
    {
        ProducerConfig cfg = voidCfg();
        cfg.maxFrameBytes = 0;                                  // no frame cap
        auto* impl = new ProducerImpl(cfg, 64 * 1024);          // ring maxMsgLength 8 KiB
        Producer p(impl);
        EXPECT_EQ(p.claimBatch(kTopic, Key(uint64_t{1}), 100, 100).result(), SendResult::MessageTooLarge)
            << "42 + n x recLen > the ring's maxMsgLength";
    }
}

TEST(ProducerClaim, ClaimBatchDiscardedByCloseReportsOneBatchAckForAllItsRecords) {
    auto* impl = new ProducerImpl(voidCfg(), 64 * 1024);
    Producer p(impl);
    test_support::AckLedger L;
    L.attachBatches(*impl);
    rc::BatchClaim b = p.claimBatch(kTopic, Key(uint64_t{1}), 3, 2);
    ASSERT_TRUE(b);
    ASSERT_TRUE(b.commit());
    ASSERT_EQ(p.closeImmediately(), rc::OpResult::Accepted);  // the batch is still in the ring
    ASSERT_EQ(L.batchAcks.size(), 1u);
    EXPECT_EQ(L.batchAcks[0].recordCount, 3);
    EXPECT_EQ(L.batchAcks[0].baseSequence, -1);
    EXPECT_EQ(L.batchAcks[0].errorCode, rc::BatchAck::kClientFailure);
}

TEST(ProducerClaimAllocRegression, RingClaimBatchSteadyStateAllocatesNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    Producer p(impl);
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        rc::BatchClaim b = p.claimBatch(kTopic, Key(k), 4, 32);
        if (b) {
            for (uint32_t i = 0; i < 4; ++i) std::memset(b.record(i).data(), 0x5A, 32);
            (void)b.commit();
        }
        pump(*impl, sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "claimBatch + commit + drain/flush must not allocate";
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j8 --target rune_invoker_tests`
Expected: compile error `no member named 'BatchClaim' in namespace 'rune::client'`.

- [ ] **Step 3: Implement**

`Producer.h` — directly after `class Claim { … };` add:

```cpp
/// n records of recLen bytes claimed together and appended as ONE RecordBatch
/// (one partition, contiguous offsets, all or nothing) — Producer::claimBatch.
/// Same RAII / refusal / holding rules as Claim.
class BatchClaim {
public:
    BatchClaim() noexcept = default;  ///< refused: InvalidTopic
    BatchClaim(BatchClaim&& o) noexcept;
    BatchClaim& operator=(BatchClaim&& o) noexcept;  ///< aborts this claim first if it is open
    BatchClaim(const BatchClaim&) = delete;
    BatchClaim& operator=(const BatchClaim&) = delete;
    ~BatchClaim();  ///< aborts an open claim

    [[nodiscard]] explicit operator bool() const noexcept { return std::holds_alternative<Open>(state_); }
    [[nodiscard]] SendResult result() const noexcept;
    /// Record i's value bytes (recordLength() bytes); empty for i >= count() or unless open.
    [[nodiscard]] std::span<uint8_t> record(uint32_t i) const noexcept;
    [[nodiscard]] uint32_t count() const noexcept;         ///< n (0 unless open)
    [[nodiscard]] uint32_t recordLength() const noexcept;  ///< recLen (0 unless open)
    /// Publish all n records as one RecordBatch (one BatchAck of n).  False:
    /// nothing was open, or the claim was abandoned under it (see Claim).
    bool commit() noexcept;
    void abort() noexcept;

private:
    friend class Producer;
    struct Refused {
        SendResult why = SendResult::InvalidTopic;
    };
    struct Open {
        detail::ClaimOpen core;
        uint8_t*          base    = nullptr;  ///< record 0's value bytes (Ring) / the frame (Direct)
        const uint32_t*   offsets = nullptr;  ///< Direct: record i at base + offsets[i]; nullptr: stride recLen
        uint32_t          n       = 0;
        uint32_t          recLen  = 0;
    };
    struct Settled {};
    explicit BatchClaim(SendResult why) noexcept : state_(Refused{why}) {}
    BatchClaim(const detail::ClaimOpen& core, uint8_t* base, const uint32_t* offsets, uint32_t n,
               uint32_t recLen) noexcept
        : state_(Open{core, base, offsets, n, recLen}) {}
    std::variant<Refused, Open, Settled> state_{Refused{}};
};
```

In `class Producer`, after the three `tryClaim` declarations, add:

```cpp
    /// Claim n records of recLen value bytes each for ONE atomic append (one
    /// RecordBatch on key's partition, contiguous offsets, all or nothing):
    /// write record(i) for every i, then commit().  Limits — never retryable:
    /// n >= 1 (InvalidArgument); key + recLen <= kMaxRecordBytes, the whole
    /// batch within ProducerConfig::maxFrameBytes (a conservative bound) and,
    /// Thread mode, 42 + n x recLen <= the producer ring's maxMsgLength
    /// (MessageTooLarge).  Never ahead of a Tier-2 batch queued earlier on the
    /// partition.  With batchAcksTo bound it reserves n credit units (one
    /// BatchAck of n returns them).  ZERO allocation in steady state.
    [[nodiscard]] BatchClaim claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen) noexcept;
```

`Claim.cpp` — append before the closing `}  // namespace rune::client`:

```cpp
BatchClaim::BatchClaim(BatchClaim&& o) noexcept : state_(std::exchange(o.state_, Settled{})) {}

BatchClaim& BatchClaim::operator=(BatchClaim&& o) noexcept {
    if (this != &o) {
        abort();
        state_ = std::exchange(o.state_, Settled{});
    }
    return *this;
}

BatchClaim::~BatchClaim() { abort(); }

SendResult BatchClaim::result() const noexcept {
    if (const auto* r = std::get_if<Refused>(&state_)) return r->why;
    return SendResult::Ok;
}

std::span<uint8_t> BatchClaim::record(uint32_t i) const noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr || i >= o->n) return {};
    uint8_t* p = o->offsets != nullptr ? o->base + o->offsets[i]
                                       : o->base + static_cast<std::size_t>(i) * o->recLen;
    return {p, o->recLen};
}

uint32_t BatchClaim::count() const noexcept {
    const auto* o = std::get_if<Open>(&state_);
    return o != nullptr ? o->n : 0u;
}

uint32_t BatchClaim::recordLength() const noexcept {
    const auto* o = std::get_if<Open>(&state_);
    return o != nullptr ? o->recLen : 0u;
}

bool BatchClaim::commit() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return false;
    const detail::ClaimOpen core = o->core;
    state_ = Settled{};
    return detail::ClaimCredit::commit(core);
}

void BatchClaim::abort() noexcept {
    const auto* o = std::get_if<Open>(&state_);
    if (o == nullptr) return;
    const detail::ClaimOpen core = o->core;
    state_ = Settled{};
    detail::ClaimCredit::abort(core);
}
```

`Producer.cpp` — append to the 2b-B section (after the `AckToken` `tryClaim`):

```cpp
BatchClaim Producer::claimBatch(TopicHandle topic, const Key& key, uint32_t n, uint32_t recLen) noexcept {
    if (!impl_ || !topic.valid()) return BatchClaim(SendResult::InvalidTopic);
    const producer::ProducerImpl::ClaimGrant g =
        impl_->claimBatch(topic.topicId, key.partition(topic.partitionCount), key, n, recLen);
    if (g.result != SendResult::Ok) return BatchClaim(g.result);
    detail::ClaimOpen core{impl_, g.token, nullptr, nullptr, 0};
    if (Completions* bq = impl_->batchAckCq(); bq != nullptr) [[unlikely]] {
        const SendResult u = reserveBatchUnits(*bq, n);  // one unit per record, like n sends
        if (u != SendResult::Ok) {
            detail::ClaimCredit::abort(core);
            return BatchClaim(u);
        }
        core.bq    = bq;
        core.units = n;
    }
    return BatchClaim(core, g.base, g.offsets, n, recLen);
}
```

`ProducerImpl.h` — replace the message-id block (`:72-77`, the comment + `kMsgSendRecord` … `kMsgTier2Batch`) with:

```cpp
inline constexpr int32_t kMsgSendRecord  = 1; // SEND_RECORD (producer-ring local ID)
inline constexpr int32_t kMsgFlush       = 2; // flush trigger (producer-ring local ID)
inline constexpr int32_t kMsgTier2Batch  = 3; // Tier-2 explicit batch token
inline constexpr int32_t kMsgClaimBatch  = 4; // Phase 2b B: claimBatch — n records, ONE RecordBatch

// CLAIM_BATCH layout (kMsgClaimBatch payload): [0..33] exactly as SEND_RECORD
// (topicId, partition, keyLen, inline key, flags = 0, ackHandle = 0), then
//   [34..37] n      : uint32 LE (records)
//   [38..41] recLen : uint32 LE (value bytes per record)
//   [42..]          : n x recLen value bytes, contiguous
inline constexpr int kBatchCountOff   = 34;
inline constexpr int kBatchRecLenOff  = 38;
inline constexpr int kBatchRecsOff    = 42;
inline constexpr int kBatchHeaderSize = 42;
```

In the public 2b-B claim section (Task 4), after `claimRecord`, add:

```cpp
    /// Reserve n records of recLen bytes appended as ONE RecordBatch: Ring — a
    /// CLAIM_BATCH ring message (header written, records contiguous at `base`).
    /// Refusals: InvalidArgument (n == 0), MessageTooLarge (per-record cap,
    /// maxFrameBytes bound, ring maxMsgLength), Closed, BackPressured.
    [[nodiscard]] ClaimGrant claimBatch(int32_t topicId, int32_t partition, const Key& key, uint32_t n,
                                        uint32_t recLen) noexcept;
```

In the `private:` section, directly after the `appendRecord_` declaration, add:

```cpp
    /// Create the partition's builder once (reused across batches).
    void ensureBuilder_(PartitionState& ps);
    /// Every check before ONE record may join `ps`'s builder (Tier-2 queued
    /// ahead, frame cap, batchBytes, batchRecords), the builder created and the
    /// record's ack-ring room reserved.  ABORT = back-pressure (nothing changed).
    rune::client::detail::ControlledAction admitRecord_(PartitionState& ps, uint8_t keyLen, uint32_t valueLen,
                                                        uint8_t flags, uint64_t ackHandle);
    /// Same for a claimBatch of n x recLen: it joins a non-empty builder only
    /// when it fits whole (else flushRequested + ABORT); builder capacity for
    /// all n is reserved so appending them cannot throw (all or none).
    rune::client::detail::ControlledAction admitBatch_(PartitionState& ps, uint8_t keyLen, uint32_t n,
                                                       uint32_t recLen);
    /// Append an admitted CLAIM_BATCH's n records (one timestamp), then account them.
    void appendBatch_(PartitionState& ps, const uint8_t* keyBytes, uint8_t keyLen, const uint8_t* recs,
                      uint32_t n, uint32_t recLen, int64_t nowNs);
    /// Accounting after `count` records starting at offsetDelta `firstIndex`
    /// were written into ps.builder: batch clock, dirty list, the ack handle of
    /// an acked single record (room reserved by admitRecord_), wire size, count.
    void noteAppended_(PartitionState& ps, int32_t firstIndex, int32_t count, uint8_t flags, uint64_t ackHandle,
                       int64_t nowNs);
    /// claimBatch's size rules, shared by both ingress modes.
    [[nodiscard]] SendResult checkBatchClaim_(const Key& key, uint32_t n, uint32_t recLen) const noexcept;
```

`ProducerImpl.cpp` — directly after `recordBodyContrib(...)` (inside the same anonymous namespace), add:

```cpp
// Phase 2b Part B: an upper bound on ONE claimBatch's RecordBatch wire bytes
// (batch overhead + record bodies): every record is charged the widest
// timestamp delta and the batch's last offsetDelta.  Compared against
// maxFrameBytes at claim time and used to reserve builder capacity.
inline int64_t claimBatchWireBound(std::size_t keyLen, uint32_t n, uint32_t recLen) noexcept {
    const int32_t perRecord = recordBodyContrib(static_cast<int32_t>(keyLen), static_cast<int32_t>(recLen),
                                                INT64_MAX >> 1, static_cast<int32_t>(n - 1));
    return static_cast<int64_t>(rune::protocol::batch::kRecordBatchOverhead) +
           static_cast<int64_t>(n) * perRecord;
}
```

In `drainRing`, directly after the `if (msgTypeId == kMsgTier2Batch) { … }` block and before `if (msgTypeId != kMsgSendRecord) return ControlledAction::CONTINUE;`, insert:

```cpp
            if (msgTypeId == kMsgClaimBatch) {
                // Phase 2b B: n records claimed as ONE atomic append — all n go
                // into one builder (one RecordBatch, contiguous offsets) or the
                // message stays in the ring (ABORT) until they can.
                const std::size_t bsz = static_cast<std::size_t>(len);
                if (bsz < static_cast<std::size_t>(kBatchHeaderSize)) return ControlledAction::CONTINUE;
                const uint32_t n      = readLE32u(p, kBatchCountOff);
                const uint32_t recLen = readLE32u(p, kBatchRecLenOff);
                if (n == 0 || bsz < static_cast<std::size_t>(kBatchHeaderSize) +
                                        static_cast<std::size_t>(n) * recLen) {
                    return ControlledAction::CONTINUE;  // malformed: the producer validated it
                }
                PartitionState& ps = getOrCreatePartition(readLE32(p, kRecTopicIdOff), readLE32(p, kRecPartOff));
                const uint8_t keyLen = p[kRecKeyLenOff];
                if (admitBatch_(ps, keyLen, n, recLen) == ControlledAction::ABORT) {
                    lastDrainStop_ = DrainStop::Blocked;
                    return ControlledAction::ABORT;
                }
                appendBatch_(ps, p + kRecKeyBytesOff, keyLen, p + kBatchRecsOff, n, recLen, nowNs);
                return ControlledAction::CONTINUE;
            }
```

Replace the whole of `appendRecord_` (from its banner comment `// Owner/duty thread: append one record to its partition builder` through its closing `}` before `// Agent-thread: encode and offer one batch`) with:

```cpp
// ---------------------------------------------------------------------------
// Owner/duty thread: append one record to its partition builder (Task 14: the
// single append path — the ring drain and Direct trySubmit both land here).
// Phase 2b B: split into admitRecord_ (every check; may throw) and
// noteAppended_ (accounting) so Invoker claims reserve with the same rules.
// ---------------------------------------------------------------------------

void ProducerImpl::ensureBuilder_(PartitionState& ps) {
    if (ps.builder) return;
    // Created ONCE per partition; reused (reset in place) across batches so
    // there is no per-batch heap allocation.
    const int64_t baseWall = wallMs();
    ps.builder = std::make_unique<ProduceFrameBuilder>(0, baseWall);
    ps.builderBaseWallMs = baseWall;
}

rune::client::detail::ControlledAction ProducerImpl::admitRecord_(PartitionState& ps, uint8_t keyLen,
                                                                  uint32_t valueLen, uint8_t flags,
                                                                  uint64_t ackHandle) {
    using rune::client::detail::ControlledAction;
    // Task 14 fix1 I1: never overtake a Tier-2 batch queued earlier on the
    // partition (flushBatches sends Tier-1 first).
    if (!ps.pendingTier2Entries.empty()) return ControlledAction::ABORT;

    // M2: the per-frame cap computed once at construction (cfg_ immutable).
    const bool    frameCapped = frameCapped_;
    const int32_t effCap      = effCap_;
    // Frame-cap path: exact pre-flight of this record's wire contribution
    // (+1 byte slack for wallMs() jitter between here and the append) — no
    // emitted RecordBatch ever exceeds effCap.
    if (frameCapped && ps.builder && ps.builder->recordCount() > 0) {
        const int64_t tsDelta = wallMs() - ps.builderBaseWallMs;
        const int32_t contrib = recordBodyContrib(static_cast<int32_t>(keyLen), static_cast<int32_t>(valueLen),
                                                  tsDelta, ps.builder->recordCount()) + 1;
        if (ps.builderBytes + contrib > effCap) {
            // sizeFull will not trigger below the cap: force the flush.
            if (ps.builderBytes < effCap) ps.flushRequested = true;
            return ControlledAction::ABORT;
        }
    }
    // Normal path: a full builder ABORTs regardless of the window (no flush
    // runs between appends in either mode — F12 / Task 14).
    if (!frameCapped && ps.builderBytes >= effCap) return ControlledAction::ABORT;
    // Task 14 fix1 M2 / F12: batchRecords enforced at append time too.
    if (cfg_.batchRecords > 0 && ps.builder && ps.builder->recordCount() >= cfg_.batchRecords) {
        return ControlledAction::ABORT;
    }
    ensureBuilder_(ps);
    const int32_t index = ps.builder->recordCount();  // this record's offsetDelta
    // Task 14 fix1 M1: everything that can throw happens BEFORE anything is
    // committed — the ack slots are reserved here and written by noteAppended_.
    if ((flags & kFlagWantAck) && ackHandle != 0) {
        const uint64_t need = static_cast<uint64_t>(index - ps.batchAckCount) + 1u;
        while (ps.ackHead - ps.ackTail + need > ps.ackRing.size()) growAckRing(ps);
    }
    // I2: a single record whose own contribution exceeds the frame cap is
    // still emitted (alone, to guarantee progress) — surfaced loudly.
    if (frameCapped && index == 0) {
        const int32_t soloContrib = recordBodyContrib(static_cast<int32_t>(keyLen), static_cast<int32_t>(valueLen),
                                                      /*tsDelta=*/0, /*offsetDelta=*/0);
        if (soloContrib > effCap) {
            std::fprintf(stderr,
                "ProducerImpl: single record wire=%d exceeds maxFrameBytes=%d "
                "(topic=%d part=%d); emitting oversize frame (broker may reject)\n",
                soloContrib, effCap, ps.topicId, ps.partition);
        }
    }
    return ControlledAction::CONTINUE;
}

void ProducerImpl::noteAppended_(PartitionState& ps, int32_t firstIndex, int32_t count, uint8_t flags,
                                 uint64_t ackHandle, int64_t nowNs) {
    // fix2: the batch clock is the EARLIEST record's acceptance time.
    if (firstIndex == 0 || nowNs < ps.batchStartNs) ps.batchStartNs = nowNs;
    markDirty_(ps);  // F12: no-throw (capacity reserved with the partition)
    // Per-record acks at the record's OWN index (spec §4.3); zeros back-filled
    // lazily.  Room was reserved by admitRecord_, so pushAck_ never grows here.
    if ((flags & kFlagWantAck) && ackHandle != 0) {
        if (ps.batchAckCount == 0) ps.batchAckStart = ps.ackHead;
        while (ps.batchAckCount < firstIndex) pushAck_(ps, 0);
        pushAck_(ps, ackHandle);
    }
    // The TRUE encoded wire size (batch header + record bodies).
    ps.builderBytes = static_cast<int32_t>(rune::protocol::batch::kRecordBatchOverhead + ps.builder->bodyBytes());
    recordsSent_ += static_cast<uint64_t>(count);
}

rune::client::detail::ControlledAction ProducerImpl::appendRecord_(
    int32_t topicId, int32_t partition, const uint8_t* keyBytes, uint8_t keyLen,
    const uint8_t* valBytes, uint32_t valueLen, uint8_t flags, uint64_t ackHandle, int64_t nowNs)
{
    using rune::client::detail::ControlledAction;
    PartitionState& ps = getOrCreatePartition(topicId, partition);
    if (admitRecord_(ps, keyLen, valueLen, flags, ackHandle) == ControlledAction::ABORT) {
        return ControlledAction::ABORT;
    }
    const int32_t index = ps.builder->recordCount();
    const std::string_view kv(reinterpret_cast<const char*>(keyBytes), static_cast<std::size_t>(keyLen));
    const std::string_view vv(reinterpret_cast<const char*>(valBytes), static_cast<std::size_t>(valueLen));
    ps.builder->append(kv, vv, wallMs());
    // ---- committed: nothing below throws -----------------------------------
    noteAppended_(ps, index, 1, flags, ackHandle, nowNs);
    return ControlledAction::CONTINUE;
}

rune::client::detail::ControlledAction ProducerImpl::admitBatch_(PartitionState& ps, uint8_t keyLen, uint32_t n,
                                                                 uint32_t recLen) {
    using rune::client::detail::ControlledAction;
    // Same ordering rule as a record: never ahead of a queued Tier-2 batch.
    if (!ps.pendingTier2Entries.empty()) return ControlledAction::ABORT;
    const int64_t body = claimBatchWireBound(keyLen, n, recLen) -
                         static_cast<int64_t>(rune::protocol::batch::kRecordBatchOverhead);
    if (ps.builder && ps.builder->recordCount() > 0) {
        // Join the open batch only when the WHOLE claim fits it; otherwise
        // flush that batch first and start an empty one with the claim.
        const bool overBytes   = static_cast<int64_t>(ps.builderBytes) + body > static_cast<int64_t>(effCap_);
        const bool overRecords = cfg_.batchRecords > 0 &&
                                 static_cast<int64_t>(ps.builder->recordCount()) + n > cfg_.batchRecords;
        if (overBytes || overRecords) {
            ps.flushRequested = true;
            return ControlledAction::ABORT;
        }
    }
    ensureBuilder_(ps);
    ps.builder->reserveExtra(static_cast<std::size_t>(body));  // the n appends cannot reallocate / throw
    return ControlledAction::CONTINUE;
}

void ProducerImpl::appendBatch_(PartitionState& ps, const uint8_t* keyBytes, uint8_t keyLen, const uint8_t* recs,
                                uint32_t n, uint32_t recLen, int64_t nowNs) {
    // Capacity for all n was reserved by admitBatch_: no append below
    // reallocates, so the n records land together.
    const int32_t first = ps.builder->recordCount();
    const std::string_view kv(reinterpret_cast<const char*>(keyBytes), static_cast<std::size_t>(keyLen));
    const int64_t ts = wallMs();
    for (uint32_t i = 0; i < n; ++i) {
        const std::string_view vv(reinterpret_cast<const char*>(recs + static_cast<std::size_t>(i) * recLen), recLen);
        ps.builder->append(kv, vv, ts);
    }
    noteAppended_(ps, first, static_cast<int32_t>(n), 0, 0, nowNs);
}
```

Replace the whole of `discardRing_` (`:2910-2942`) with (the `kMsgClaimBatch` branch is the only change):

```cpp
int ProducerImpl::discardRing_(OpStatus st, int limit) {
    // Records still in the app->agent ring (single consumer: we are it) are
    // completed with `st` and never sent.  Tier-2 entries are freed.  NOTE:
    // the ring head advances only when read() returns, so a handler run here
    // must never re-enter a read of this ring — the only ways (closeImmediately
    // / failClose from a handler) are deferred to the DutyScope end (fix round 1,
    // I1), and discardRing_'s callers hold a DutyScope or are Discarding already.
    return ring_.read(
        [&](int32_t type, aeron::concurrent::AtomicBuffer& buf, aeron::util::index_t off,
            aeron::util::index_t len) {
            const auto* p = reinterpret_cast<const uint8_t*>(buf.buffer() + off);
            if (type == kMsgSendRecord && len >= kRecHeaderSize) {
                const uint64_t h = (p[kRecFlagsOff] & kFlagWantAck)
                                       ? static_cast<uint64_t>(readLE64(p, kRecCorrIdOff)) : 0u;
                const int32_t tid  = readLE32(p, kRecTopicIdOff);
                const int32_t part = readLE32(p, kRecPartOff);
                emitUnsentBatchAck_(tid, part, 1, st, 1);
                completeHandle_(h, OpKind::Send, -1, 0, st, 0);
            } else if (type == kMsgTier2Batch && len >= kT2TokenSize) {
                std::unique_ptr<Tier2Entry> e(
                    reinterpret_cast<Tier2Entry*>(static_cast<uintptr_t>(readLE64(p, 0))));
                if (!e) return;
                const uint64_t h  = e->ackHandle;
                const int32_t  rc = e->recordCount;
                const int32_t  tid = e->topicId, part = e->partition;
                e.reset();
                emitUnsentBatchAck_(tid, part, rc, st, 1);
                completeHandle_(h, OpKind::Tier2Batch, -1, rc, st, 0);
            } else if (type == kMsgClaimBatch && len >= kBatchHeaderSize) {
                // Phase 2b B: a committed claimBatch never sent — ONE BatchAck
                // for its n records (n credit units); claims carry no ack handle.
                const int32_t tid  = readLE32(p, kRecTopicIdOff);
                const int32_t part = readLE32(p, kRecPartOff);
                const int32_t n    = static_cast<int32_t>(readLE32u(p, kBatchCountOff));
                emitUnsentBatchAck_(tid, part, n, st, static_cast<uint32_t>(n));
            }
            // kMsgFlush: nothing to complete.
        },
        limit);
}
```

In the 2b-B claim section (after `claimRecord`), add:

```cpp
SendResult ProducerImpl::checkBatchClaim_(const Key& key, uint32_t n, uint32_t recLen) const noexcept {
    if (n == 0) return SendResult::InvalidArgument;
    if (n > static_cast<uint32_t>(INT32_MAX) || key.size() + recLen > rune::client::Producer::kMaxRecordBytes) {
        return SendResult::MessageTooLarge;
    }
    // A claimBatch is ONE frame: it must fit the hard per-frame wire cap.
    if (cfg_.maxFrameBytes > 0 &&
        claimBatchWireBound(key.size(), n, recLen) > static_cast<int64_t>(cfg_.maxFrameBytes)) {
        return SendResult::MessageTooLarge;
    }
    return SendResult::Ok;
}

ProducerImpl::ClaimGrant ProducerImpl::claimBatch(int32_t topicId, int32_t partition, const Key& key, uint32_t n,
                                                  uint32_t recLen) noexcept {
    ClaimGrant g;
    const SendResult check = checkBatchClaim_(key, n, recLen);
    if (check != SendResult::Ok) {
        g.result = check;
        return g;
    }
    const uint64_t totalLen = static_cast<uint64_t>(kBatchHeaderSize) + static_cast<uint64_t>(n) * recLen;
    if (totalLen > static_cast<uint64_t>(ring_.maxMsgLength())) {
        g.result = SendResult::MessageTooLarge;  // spec B: n x recLen <= the ring's maxMsgLength
        return g;
    }
    aeron::util::index_t idx = -1;
    try {
        idx = ring_.tryClaim(kMsgClaimBatch, static_cast<std::size_t>(totalLen));
    } catch (...) {
        g.result = SendResult::BackPressured;  // unreachable: type and length are pre-checked
        return g;
    }
    if (idx < 0) {
        g.result = ring_.closed() ? SendResult::Closed : SendResult::BackPressured;
        return g;
    }
    uint8_t* dst = ring_.base() + idx;
    writeClaimHeader(dst, topicId, partition, key, 0);
    writeLE32u(dst, kBatchCountOff, n);
    writeLE32u(dst, kBatchRecLenOff, recLen);
    g.result = SendResult::Ok;
    g.token  = idx;
    g.base   = dst + kBatchRecsOff;
    return g;
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j8 --target rune_invoker_tests && ctest --test-dir build -R 'ProducerClaim|ProducerSend|DirectIngress|ProducerAllocRegression|ProducerBatching|ProducerFlushDirty|ProducerAckPath' --output-on-failure`
Expected: all PASS — the new claimBatch tests, and the existing producer suites prove the `appendRecord_` split changed no behaviour.

- [ ] **Step 5: Regression gate + full suite, then commit**

```bash
cmake --build build -j8
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add include/rune/client/producer/Producer.h src/producer/Claim.cpp src/producer/Producer.cpp \
        include/rune/client/producer/ProducerImpl.h src/producer/ProducerImpl.cpp \
        tests/producer/ProducerClaimTest.cpp
git commit -m "feat(producer): claimBatch — n records as ONE RecordBatch via kMsgClaimBatch=4 (2b-B)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: client — Invoker (Direct) claims: the claim writes the Produce frame

**Files:**
- Modify: `rune-client-cpp/include/rune/client/producer/Producer.h` (`enum class SendResult`: append `ClaimHeld`)
- Modify: `rune-client-cpp/include/rune/client/producer/ProducerImpl.h` (`enum class RejectReason`: append `ClaimHeld`; `claimsAbandoned()` next to `deferredApplyFailures()`; private Direct-claim state machine)
- Modify: `rune-client-cpp/src/producer/ProducerImpl.cpp` — `tryAppendDirect_` (`:476-536`), `throwForSendResult` (`:915-941`), `admitRecord_` (Task 5), `encodeAndSend` (top, `:1546`), `failBuilder_` (top, `:2632`), the 2b-B claim section (dispatch + Direct bodies)
- Test: `rune-client-cpp/tests/producer/ProducerClaimTest.cpp` (append)

**Interfaces:**
- Consumes: Task 3 `ProduceFrameBuilder::{reserveRecord, reserveExtra, Mark, mark, rollback, kRecordOverheadMax}`; Task 5 `admitRecord_`, `admitBatch_`, `noteAppended_`, `checkBatchClaim_`.
- Produces: `SendResult::ClaimHeld` (appended last; non-retryable — a blocking `send()` throws `std::logic_error`); `RejectReason::ClaimHeld`; `ProducerImpl::claimsAbandoned() const noexcept -> uint64_t`; Direct dispatch inside `claimRecord` / `claimBatch` / `commitClaim` / `abortClaim`. Direct claim state machine (private): `NoDirectClaim` / `DirectClaimOpen{slot, mark, firstIndex, count, flags, ackHandle, gen}`.

- [ ] **Step 1: Write the failing tests** — append to `tests/producer/ProducerClaimTest.cpp`:

```cpp
// ---------------------------------------------------------------------------
// Invoker mode (Direct ingress): the claim is reserved IN the Produce frame
// ---------------------------------------------------------------------------

namespace {
struct DirectRig {
    ProducerImpl* impl;
    Producer      p;
    explicit DirectRig(ProducerConfig cfg = voidCfg())
        : impl(new ProducerImpl(cfg, 4 * 1024)), p(impl) {
        impl->enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    }
    /// One owner duty pass (Direct: drainRing is a no-op; flush the builders).
    void pass(const ProducerImpl::SendFn& sink) { impl->flushBatches(kNow, 42, sink); }
};
}  // namespace

// Spec B copy count, Invoker mode: the claimed bytes ARE the Produce frame's
// bytes — the only copy left is the transport's (frame -> Aeron).
TEST(ProducerClaimDirect, ClaimWritesStraightIntoTheProduceFrame) {
    DirectRig r;
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{5}), 4);
    ASSERT_TRUE(c) << static_cast<int>(c.result());
    fill(c.buffer(), "WXYZ");
    FrameCapture cap;
    cap.watch = c.buffer().data();
    ASSERT_TRUE(c.commit());
    r.pass(cap.fn());
    ASSERT_EQ(cap.values(), std::vector<std::string>{"WXYZ"});
    EXPECT_TRUE(cap.watchInFrame) << "Invoker: 1 copy (frame -> Aeron)";
}

// Review Focus 3.
TEST(ProducerClaimDirect, OpenClaimBlocksSendsAndIsNeverFlushedHalfWritten) {
    DirectRig r;
    const Key key(uint64_t{1});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    rc::Claim c = r.p.tryClaim(kTopic, key, 1);
    ASSERT_TRUE(c);
    EXPECT_EQ(r.p.trySend(kTopic, Key(uint64_t{2}), bytes("x")), SendResult::ClaimHeld);
    EXPECT_EQ(r.p.tryClaim(kTopic, Key(uint64_t{2}), 1).result(), SendResult::ClaimHeld);
    EXPECT_EQ(r.p.claimBatch(kTopic, Key(uint64_t{2}), 2, 1).result(), SendResult::ClaimHeld);
    EXPECT_THROW(r.p.send(kTopic, Key(uint64_t{2}), bytes("x")), std::logic_error)
        << "a blocking send fails fast — pumping could never settle the caller's own claim";
    FrameCapture cap;
    r.pass(cap.fn());
    EXPECT_EQ(cap.frames, 0) << "a partition with an open claim is not flushed";
    c.buffer()[0] = 'B';
    ASSERT_TRUE(c.commit());
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B", "C"}));
    ASSERT_EQ(cap.recs.size(), 3u);
    for (int i = 0; i < 3; ++i) EXPECT_EQ(cap.recs[i].offset, i);
}

// Review Focus 2.
TEST(ProducerClaimDirect, CloseAbandonsAnOpenClaimAndItsCommitReturnsFalse) {
    DirectRig r;
    test_support::AckLedger L;
    L.attachBatches(*r.impl);
    rc::Claim c = r.p.tryClaim(kTopic, Key(uint64_t{1}), 3);
    ASSERT_TRUE(c);
    fill(c.buffer(), "abc");
    ASSERT_EQ(r.p.closeImmediately(), rc::OpResult::Accepted);
    EXPECT_EQ(r.impl->claimsAbandoned(), 1u);
    EXPECT_FALSE(c.commit()) << "abandoned under the caller: nothing is sent";
    FrameCapture cap;
    r.pass(cap.fn());
    EXPECT_TRUE(cap.recs.empty());
    EXPECT_TRUE(L.batchAcks.empty()) << "an uncommitted claim was never accepted: no BatchAck";
}

TEST(ProducerClaimDirect, ClaimOffTheOwnerThreadIsWrongThread) {
    DirectRig r;
    SendResult one = SendResult::Ok;
    SendResult many = SendResult::Ok;
    std::thread t([&] {
        one  = r.p.tryClaim(kTopic, Key(uint64_t{1}), 4).result();
        many = r.p.claimBatch(kTopic, Key(uint64_t{1}), 2, 4).result();
    });
    t.join();
    EXPECT_EQ(one, SendResult::WrongThread);
    EXPECT_EQ(many, SendResult::WrongThread);
}

TEST(ProducerClaimDirect, ClaimBatchIsOneRecordBatchAndAbortRollsBackExactly) {
    DirectRig r;
    const Key key(uint64_t{1});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);
    {
        rc::BatchClaim a = r.p.claimBatch(kTopic, key, 3, 1);
        ASSERT_TRUE(a);
        a.record(0)[0] = 'x';
        a.abort();
    }
    FrameCapture cap;
    {
        rc::BatchClaim b = r.p.claimBatch(kTopic, key, 2, 2);
        ASSERT_TRUE(b) << static_cast<int>(b.result());
        fill(b.record(0), "B1");
        fill(b.record(1), "B2");
        cap.watch = b.record(1).data();
        ASSERT_TRUE(b.commit());
    }
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("C")), SendResult::Ok);
    r.pass(cap.fn());
    EXPECT_EQ(cap.values(), (std::vector<std::string>{"A", "B1", "B2", "C"}));
    EXPECT_EQ(cap.frames, 1);
    for (int i = 0; i < 4; ++i) EXPECT_EQ(cap.recs[i].offset, i) << "the aborted batch left no gap";
    EXPECT_TRUE(cap.watchInFrame) << "Invoker claimBatch writes the frame in place too";
}

TEST(ProducerClaimDirect, AckedClaimCompletesWithItsOwnOffset) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    test_support::AckLedger L;
    L.attach(r.p);
    const Key key(uint64_t{1});
    ASSERT_EQ(r.p.trySend(kTopic, key, bytes("A")), SendResult::Ok);  // offsetDelta 0, void
    rc::Claim c = r.p.tryClaim(kTopic, key, 1, rc::AckToken{5});      // offsetDelta 1
    ASSERT_TRUE(c);
    c.buffer()[0] = 'B';
    ASSERT_TRUE(c.commit());
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(cap.corrs.size(), 1u);
    r.impl->onProduceResponse(cap.corrs[0], 200, 0);
    ASSERT_EQ(L.recs.size(), 1u);
    EXPECT_EQ(L.recs[0].token, 5u);
    EXPECT_EQ(L.recs[0].value, 201) << "baseOffset + its own index";
}

// Claims are never deferred: inside a callback the producer fires during its
// own duty work, a claim is refused (use trySend() there — it defers).
TEST(ProducerClaimDirect, ClaimFromInsideAProducerCallbackIsBackPressured) {
    ProducerConfig cfg = voidCfg();
    cfg.acks = Acks::Leader;
    DirectRig r(cfg);
    struct Ctx {
        Producer*  p;
        SendResult got   = SendResult::Ok;
        int        calls = 0;
    } ctx{&r.p};
    r.p.onAck(
        [](void* c, rc::Token, int64_t, rc::OpStatus, uint16_t) noexcept {
            auto* x = static_cast<Ctx*>(c);
            ++x->calls;
            x->got = x->p->tryClaim(kTopic, Key(uint64_t{2}), 4).result();
        },
        &ctx);
    ASSERT_EQ(r.p.trySend(kTopic, Key(uint64_t{1}), bytes("a"), rc::AckToken{1}), SendResult::Ok);
    FrameCapture cap;
    r.pass(cap.fn());
    ASSERT_EQ(cap.corrs.size(), 1u);
    r.impl->onProduceResponse(cap.corrs[0], 10, 0);
    ASSERT_EQ(ctx.calls, 1);
    EXPECT_EQ(ctx.got, SendResult::BackPressured);
}

TEST(ProducerClaimAllocRegression, DirectClaimAndClaimBatchSteadyStateAllocateNothingRegression) {
    DirectRig r;
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](uint64_t k) {
        {
            rc::Claim c = r.p.tryClaim(kTopic, Key(k), 64);
            if (c) {
                std::memset(c.buffer().data(), 0x5A, 64);
                (void)c.commit();
            }
        }
        {
            rc::BatchClaim b = r.p.claimBatch(kTopic, Key(k), 4, 32);
            if (b) {
                for (uint32_t i = 0; i < 4; ++i) std::memset(b.record(i).data(), 0x5B, 32);
                (void)b.commit();
            }
        }
        r.pass(sink);
    };
    for (uint64_t k = 0; k < 64; ++k) cycle(k);  // warm-up: builders, frames, claim offsets
    alloc_counter::start();
    for (uint64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "Invoker claims must not allocate in steady state";
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j8 --target rune_invoker_tests`
Expected: compile error `no member named 'ClaimHeld' in 'rune::client::SendResult'`.

- [ ] **Step 3: Implement**

`Producer.h` — in `enum class SendResult`, after `MessageTooLarge,///< …` add (last):

```cpp
    ClaimHeld,      ///< Phase 2b B, Invoker mode: a claim on this producer is still open —
                    ///< commit() or abort() it first.  Never retryable by waiting (the
                    ///< blocking send() throws std::logic_error).
```

`ProducerImpl.h` — in `enum class RejectReason`, after `Unaddressable,` add:

```cpp
    ClaimHeld,          ///< Direct: an open claim owns the append path (Phase 2b B)
```

After `deferredApplyFailures()` add:

```cpp
    /// Phase 2b B (owner thread): Invoker claims abandoned while open — their
    /// partition's builder was failed / the producer closed; commit() returned false.
    [[nodiscard]] uint64_t claimsAbandoned() const noexcept { return claimsAbandoned_; }
```

In the `private:` section, after the `checkBatchClaim_` declaration (Task 5), add:

```cpp
    // ---- Phase 2b B: the Invoker (Direct) claim state machine (owner thread) ---
    //   NoDirectClaim ──claimRecord / claimBatch──> DirectClaimOpen
    //   DirectClaimOpen ──commit──> NoDirectClaim   (records join the batch: noteAppended_)
    //   DirectClaimOpen ──abort / abandon──> NoDirectClaim (builder rolled back to `mark`)
    // While open: every other append to the producer is ClaimHeld (trySubmit) or
    // ABORTed (admitRecord_ — the deferred apply pass), and the partition is not
    // flushed (encodeAndSend); failBuilder_ abandons it.
    struct NoDirectClaim {};
    struct DirectClaimOpen {
        uint32_t                  slot       = 0;  ///< PartitionState::slotIndex
        ProduceFrameBuilder::Mark mark{};          ///< the builder before the reservation
        int32_t                   firstIndex = 0;  ///< offsetDelta of the first reserved record
        int32_t                   count      = 0;  ///< 1 (tryClaim) or n (claimBatch)
        uint8_t                   flags      = 0;  ///< kFlagWantAck for an acked tryClaim
        uint64_t                  ackHandle  = 0;
        uint64_t                  gen        = 0;  ///< the Claim's token
    };
    [[nodiscard]] bool directClaimOn_(const PartitionState& ps) const noexcept {
        const auto* o = std::get_if<DirectClaimOpen>(&directClaim_);
        return o != nullptr && o->slot == ps.slotIndex;
    }
    /// failBuilder_: the open claim on `ps` (if any) is rolled back and dropped.
    void abandonDirectClaimOn_(PartitionState& ps) noexcept;
    /// Owner-thread / closed / held / in-duty / deferred-ahead checks of a claim.
    [[nodiscard]] SendResult directClaimGate_(int32_t topicId, int32_t partition) noexcept;
    [[nodiscard]] ClaimGrant claimRecordDirect_(int32_t topicId, int32_t partition, const Key& key,
                                                uint32_t valueLen, uint64_t ackHandle) noexcept;
    [[nodiscard]] ClaimGrant claimBatchDirect_(int32_t topicId, int32_t partition, const Key& key, uint32_t n,
                                               uint32_t recLen) noexcept;
    [[nodiscard]] bool commitDirectClaim_(uint64_t gen) noexcept;
    void abortDirectClaim_(uint64_t gen) noexcept;
```

and in the members, directly after the line `int64_t     lastNowNs_ = 0;` (the Task 14 ingress block), add:

```cpp
    // ---- Phase 2b B: Invoker claims (owner thread only) -----------------------
    std::variant<NoDirectClaim, DirectClaimOpen> directClaim_{};
    uint64_t              claimGen_        = 0;
    uint64_t              claimsAbandoned_ = 0;
    std::vector<uint32_t> claimOffsets_;  ///< Direct claimBatch value offsets (capacity persists)
```

`ProducerImpl.cpp`:

1. `throwForSendResult` — add before the closing `}` of the switch:

```cpp
    case SendResult::ClaimHeld:
        throw std::logic_error("Producer::send: a claim is open on this invoker-mode producer — "
                               "commit() or abort() it first");
```

2. `tryAppendDirect_` — directly after the `if (directClosed_) { … }` block, insert:

```cpp
    // Phase 2b B: an open claim owns the append path until it is settled (its
    // reserved record sits at the builder tail).  Not retryable by waiting.
    if (std::holds_alternative<DirectClaimOpen>(directClaim_)) [[unlikely]] {
        why = RejectReason::ClaimHeld;
        return SendResult::ClaimHeld;
    }
```

3. `admitRecord_` (Task 5) — make its first statement:

```cpp
    // Phase 2b B: an open Invoker claim owns this partition's builder tail (the
    // deferred apply pass keeps its records until the claim is settled).
    if (directClaimOn_(ps)) return ControlledAction::ABORT;
```

4. `encodeAndSend` — directly after `if (!ps.builder || ps.builder->recordCount() == 0) return false;` insert:

```cpp
    // Phase 2b B: an open Invoker claim's record is not written yet — never
    // seal it; the partition stays dirty and flushes once the claim settles.
    if (directClaimOn_(ps)) return false;
```

5. `failBuilder_` — make its first statement:

```cpp
    abandonDirectClaimOn_(ps);  // Phase 2b B: an open claim is dropped first (never reported)
```

6. 2b-B claim section — make `claimRecord`'s first statement `if (mode_ == IngressMode::Direct) return claimRecordDirect_(topicId, partition, key, valueLen, ackHandle);`, `claimBatch`'s first statement `if (mode_ == IngressMode::Direct) return claimBatchDirect_(topicId, partition, key, n, recLen);`, `commitClaim`'s first statement `if (mode_ == IngressMode::Direct) return commitDirectClaim_(static_cast<uint64_t>(token));`, `abortClaim`'s first statement

```cpp
    if (mode_ == IngressMode::Direct) {
        abortDirectClaim_(static_cast<uint64_t>(token));
        return;
    }
```

and append the Direct bodies to the section:

```cpp
SendResult ProducerImpl::directClaimGate_(int32_t topicId, int32_t partition) noexcept {
    // Foreign thread decided FIRST, before any owner-thread state is read.
    if (std::this_thread::get_id() != dutyThreadId_) return SendResult::WrongThread;
    if (directClosed_) return SendResult::Closed;
    if (std::holds_alternative<DirectClaimOpen>(directClaim_)) return SendResult::ClaimHeld;
    // Claims are never deferred: inside this producer's own duty work (a
    // callback it fired) the builders may be mid-walk — use trySend() there.
    if (inDuty_) return SendResult::BackPressured;
    // This partition's deferred records go first (same rule as a send).
    if (!deferred_.empty() && !settleDeferredFor_(topicId, partition)) return SendResult::BackPressured;
    return SendResult::Ok;
}

ProducerImpl::ClaimGrant ProducerImpl::claimRecordDirect_(int32_t topicId, int32_t partition, const Key& key,
                                                          uint32_t valueLen, uint64_t ackHandle) noexcept {
    using rune::client::detail::ControlledAction;
    ClaimGrant g;
    g.result = directClaimGate_(topicId, partition);
    if (g.result != SendResult::Ok) return g;
    if (key.size() + valueLen > rune::client::Producer::kMaxRecordBytes) {  // B6
        g.result = SendResult::MessageTooLarge;
        return g;
    }
    const auto          kb    = key.bytes();
    const uint8_t       flags = ackHandle != 0 ? kFlagWantAck : uint8_t{0};
    try {
        PartitionState& ps = getOrCreatePartition(topicId, partition);
        if (admitRecord_(ps, static_cast<uint8_t>(kb.size()), valueLen, flags, ackHandle) ==
            ControlledAction::ABORT) {
            g.result = SendResult::BackPressured;
            return g;
        }
        // Room for the whole record first: the reservation cannot reallocate,
        // so data() stays put while the caller writes and nothing below throws.
        ps.builder->reserveExtra(kb.size() + valueLen + ProduceFrameBuilder::kRecordOverheadMax);
        const ProduceFrameBuilder::Mark mark  = ps.builder->mark();
        const int32_t                   index = ps.builder->recordCount();
        const std::size_t off = ps.builder->reserveRecord(
            std::string_view(reinterpret_cast<const char*>(kb.data()), kb.size()), valueLen, wallMs());
        directClaim_ = DirectClaimOpen{ps.slotIndex, mark, index, 1, flags, ackHandle, ++claimGen_};
        g.result = SendResult::Ok;
        g.token  = static_cast<int64_t>(claimGen_);
        g.base   = ps.builder->data() + off;
    } catch (const std::length_error&) {
        g.result = SendResult::InvalidTopic;   // the partition table is full (unaddressable)
    } catch (...) {
        g.result = SendResult::BackPressured;  // bad_alloc growing a builder / the ack ring: nothing reserved
    }
    return g;
}

ProducerImpl::ClaimGrant ProducerImpl::claimBatchDirect_(int32_t topicId, int32_t partition, const Key& key,
                                                         uint32_t n, uint32_t recLen) noexcept {
    using rune::client::detail::ControlledAction;
    ClaimGrant g;
    g.result = directClaimGate_(topicId, partition);
    if (g.result != SendResult::Ok) return g;
    g.result = checkBatchClaim_(key, n, recLen);
    if (g.result != SendResult::Ok) return g;
    const auto             kb = key.bytes();
    const std::string_view kv(reinterpret_cast<const char*>(kb.data()), kb.size());
    try {
        if (claimOffsets_.size() < n) claimOffsets_.resize(n);  // high-water mark: warm-up only
        PartitionState& ps = getOrCreatePartition(topicId, partition);
        if (admitBatch_(ps, static_cast<uint8_t>(kb.size()), n, recLen) == ControlledAction::ABORT) {
            g.result = SendResult::BackPressured;
            return g;
        }
        // admitBatch_ reserved capacity for all n: no reservation below
        // reallocates (data() stays put, nothing throws).
        const ProduceFrameBuilder::Mark mark  = ps.builder->mark();
        const int32_t                   first = ps.builder->recordCount();
        const int64_t                   ts    = wallMs();
        for (uint32_t i = 0; i < n; ++i) {
            claimOffsets_[i] = static_cast<uint32_t>(ps.builder->reserveRecord(kv, recLen, ts));
        }
        directClaim_ = DirectClaimOpen{ps.slotIndex, mark, first, static_cast<int32_t>(n), 0, 0, ++claimGen_};
        g.result  = SendResult::Ok;
        g.token   = static_cast<int64_t>(claimGen_);
        g.base    = ps.builder->data();
        g.offsets = claimOffsets_.data();
    } catch (const std::length_error&) {
        g.result = SendResult::InvalidTopic;
    } catch (...) {
        g.result = SendResult::BackPressured;
    }
    return g;
}

bool ProducerImpl::commitDirectClaim_(uint64_t gen) noexcept {
    const auto* o = std::get_if<DirectClaimOpen>(&directClaim_);
    if (o == nullptr || o->gen != gen) return false;  // abandoned under the caller
    const DirectClaimOpen c = *o;
    directClaim_ = NoDirectClaim{};
    // Ack-ring room was reserved by admitRecord_ and nothing else appended to
    // the partition while the claim was open: noteAppended_ cannot throw here.
    noteAppended_(*partitions_[c.slot], c.firstIndex, c.count, c.flags, c.ackHandle, lastNowNs_);
    RUNE_FR_MARK_PRODUCE(rune_fr::Site::ClientAppSend, static_cast<uint32_t>(c.ackHandle));
    return true;
}

void ProducerImpl::abortDirectClaim_(uint64_t gen) noexcept {
    const auto* o = std::get_if<DirectClaimOpen>(&directClaim_);
    if (o == nullptr || o->gen != gen) return;
    partitions_[o->slot]->builder->rollback(o->mark);
    directClaim_ = NoDirectClaim{};
}

void ProducerImpl::abandonDirectClaimOn_(PartitionState& ps) noexcept {
    const auto* o = std::get_if<DirectClaimOpen>(&directClaim_);
    if (o == nullptr || o->slot != ps.slotIndex) return;
    ps.builder->rollback(o->mark);
    directClaim_ = NoDirectClaim{};
    ++claimsAbandoned_;
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j8 --target rune_invoker_tests && ctest --test-dir build -R 'ProducerClaim|DirectIngress|ProducerSend|ProducerClose|ProducerAckToken' --output-on-failure`
Expected: all PASS.

- [ ] **Step 5: Regression gate + full suite, then commit**

```bash
cmake --build build -j8
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add include/rune/client/producer/Producer.h include/rune/client/producer/ProducerImpl.h \
        src/producer/ProducerImpl.cpp tests/producer/ProducerClaimTest.cpp
git commit -m "feat(producer): Invoker claims reserve the record in the Produce frame (1 copy) (2b-B)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: client — end to end through a real Client (both runner modes)

**Files:**
- Create: `rune-client-cpp/tests/client/ClaimClientTest.cpp`
- Modify: `rune-client-cpp/CMakeLists.txt` (`rune_invoker_tests` sources)

**Interfaces:**
- Consumes: Tasks 4–6 public API (`tryClaim` ×3, `claimBatch`, `Claim`, `BatchClaim`); `Client::producer`, `Client::topic`, `Completions(Client&)`, `Producer::batchAcksTo`, `batchAckOf`; `test_support::InvokerGateway` (`keepValues`, `values`), `test_support::TestCq`.
- Produces: nothing new (proof only).

- [ ] **Step 1: Write the tests** — create `rune-client-cpp/tests/client/ClaimClientTest.cpp`:

```cpp
// tests/client/ClaimClientTest.cpp — Phase 2b Part B end to end: a real Client
// (Invoker and Thread runner modes) over Aeron IPC to the in-process
// InvokerGateway.  Claims, claim batches and sends arrive in submission order;
// an acked claim completes; batch-ack credit is exact for committed AND
// aborted claims.  Drive loops are deadline-bounded (only a failing test
// reaches the deadline); nothing sleeps.
#include "../support/InvokerGateway.h"
#include "../support/TestCompletions.h"

#include <rune/client/Client.h>
#include <rune/client/Completions.h>
#include <rune/client/producer/Producer.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <vector>

namespace rc = rune::client;
using rc::SendResult;

namespace {
std::atomic<int> g_ctr{0};

rc::ClientConfig cfgFor(rc::RunnerMode mode, int32_t in, int32_t eg) {
    rc::ClientConfig c;
    c.gatewayChannel  = "aeron:ipc";
    c.gatewayStreamId = in;
    c.egressChannel   = "aeron:ipc";
    c.egressStreamId  = eg;
    c.dataDir = "/tmp/claim_client_" + std::to_string(::getpid()) + "_" + std::to_string(g_ctr.fetch_add(1));
    c.runnerMode = mode;
    rc::TopicSpec ts;
    ts.name           = "t";
    ts.topicId        = 1;
    ts.partitionCount = 1;
    c.topics.push_back(ts);
    return c;
}

struct Rig {
    bool                         invoker;
    std::unique_ptr<rc::Client>  client;  // constructed first: owns the driver
    test_support::InvokerGateway gw;      // destroyed first
    explicit Rig(const rc::ClientConfig& c)
        : invoker(c.runnerMode == rc::RunnerMode::Invoker),
          client(std::make_unique<rc::Client>(c)),
          gw(c.dataDir + "/aeron", c.gatewayChannel, c.gatewayStreamId, c.egressChannel, c.egressStreamId) {
        gw.keepValues = true;
    }
    template <class P>
    bool drive(P&& pred) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline) {
            if (invoker) client->doWork();  // Thread mode: the runner works by itself
            gw.pump();
            if (pred()) return true;
            if (!invoker) std::this_thread::yield();
        }
        return false;
    }
    void connect() {
        ASSERT_TRUE(drive([&] { return gw.ready(); }));
        test_support::TestCq cq(*client);
        auto f = cq.issue([&](rc::Completions& q, rc::Token t) { return client->connect(q, t); });
        ASSERT_TRUE(drive([&] { return f.ready(); }));
        ASSERT_TRUE(f.ok()) << f.describe();
    }
};

std::vector<uint8_t> bytes(std::string_view s) { return {s.begin(), s.end()}; }

void fill(std::span<uint8_t> b, std::string_view s) {
    std::memcpy(b.data(), s.data(), std::min(b.size(), s.size()));
}

std::vector<std::string> valuesOf(const test_support::InvokerGateway& gw, int32_t partition) {
    std::vector<std::string> out;
    const auto it = gw.values.find(partition);
    if (it == gw.values.end()) return out;
    for (const auto& v : it->second) out.emplace_back(v.begin(), v.end());
    return out;
}
}  // namespace

TEST(ClaimClient, ClaimsBatchesAndSendsArriveInSubmissionOrderInBothModes) {
    int32_t stream = 5810;
    for (const rc::RunnerMode mode : {rc::RunnerMode::Invoker, rc::RunnerMode::Thread}) {
        SCOPED_TRACE(mode == rc::RunnerMode::Invoker ? "Invoker" : "Thread");
        Rig r(cfgFor(mode, stream, stream + 1));
        stream += 2;
        r.connect();
        rc::ProducerConfig pc;
        pc.acks     = rc::Acks::Leader;
        pc.lingerUs = 0;
        auto p = r.client->producer(pc);
        const auto topic = r.client->topic("t");
        const rc::Key key(uint64_t{9});
        ASSERT_EQ(p.trySend(topic, key, bytes("A")), SendResult::Ok);
        {
            rc::BatchClaim b = p.claimBatch(topic, key, 3, 2);
            ASSERT_TRUE(b) << static_cast<int>(b.result());
            fill(b.record(0), "B1");
            fill(b.record(1), "B2");
            fill(b.record(2), "B3");
            ASSERT_TRUE(b.commit());
        }
        {
            rc::Claim c = p.tryClaim(topic, key, 1);
            ASSERT_TRUE(c) << static_cast<int>(c.result());
            fill(c.buffer(), "C");
            ASSERT_TRUE(c.commit());
        }
        rc::Completions cq(*r.client);
        int64_t ackedOffset = -2;
        {
            rc::Claim d = p.tryClaim(topic, key, 1, cq, 77);
            ASSERT_TRUE(d) << static_cast<int>(d.result());
            fill(d.buffer(), "D");
            ASSERT_TRUE(d.commit());
        }
        p.flush();
        ASSERT_TRUE(r.drive([&] {
            cq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
                if (c.token == 77) ackedOffset = c.value;
            });
            return valuesOf(r.gw, 0).size() >= 6 && ackedOffset != -2;
        }));
        EXPECT_EQ(valuesOf(r.gw, 0), (std::vector<std::string>{"A", "B1", "B2", "B3", "C", "D"}));
        EXPECT_GE(ackedOffset, 0) << "the acked claim completes Ok with its offset";
        r.client->close();
    }
}

// Review Focus 5.
TEST(ClaimClient, BatchAckCreditIsReturnedForCommittedAndAbortedClaims) {
    Rig r(cfgFor(rc::RunnerMode::Invoker, 5814, 5815));
    r.connect();
    rc::ProducerConfig pc;
    pc.acks     = rc::Acks::Leader;
    pc.lingerUs = 0;
    auto p = r.client->producer(pc);
    rc::Completions bq(*r.client);
    p.batchAcksTo(bq);
    const auto topic = r.client->topic("t");
    const rc::Key key(uint64_t{4});
    {
        rc::BatchClaim a = p.claimBatch(topic, key, 3, 2);
        ASSERT_TRUE(a);
        EXPECT_EQ(bq.outstanding(), 3u) << "one unit per record, like three sends";
        a.abort();
        EXPECT_EQ(bq.outstanding(), 0u) << "an aborted batch returns every unit";
    }
    {
        rc::Claim c = p.tryClaim(topic, key, 2);
        ASSERT_TRUE(c);
        EXPECT_EQ(bq.outstanding(), 1u);
    }  // the destructor aborts
    EXPECT_EQ(bq.outstanding(), 0u);
    {
        rc::BatchClaim b = p.claimBatch(topic, key, 3, 2);
        ASSERT_TRUE(b);
        fill(b.record(0), "x1");
        fill(b.record(1), "x2");
        fill(b.record(2), "x3");
        ASSERT_TRUE(b.commit());
    }
    EXPECT_EQ(bq.outstanding(), 3u);
    std::vector<rc::BatchAck> acks;
    ASSERT_TRUE(r.drive([&] {
        bq.poll([&](const rc::Completion& c, std::span<const uint8_t>) {
            if (c.op == rc::OpKind::BatchAck) acks.push_back(rc::batchAckOf(c));
        });
        return !acks.empty();
    }));
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0].recordCount, 3);
    EXPECT_EQ(acks[0].errorCode, 0);
    EXPECT_EQ(bq.outstanding(), 0u) << "one BatchAck of 3 returns the 3 units";
    EXPECT_EQ(bq.reservedBytes(), 0u);
    r.client->close();
}
```

Add `tests/client/ClaimClientTest.cpp` to the `rune_invoker_tests` source list (after `tests/client/InvokerClientTest.cpp`). Before choosing the stream ids, confirm they are unused: `grep -rn "581[0-5]" tests/` must print nothing.

- [ ] **Step 2: Run them**

Run: `cmake --build build -j8 --target rune_invoker_tests && ctest --test-dir build -R 'ClaimClient' --output-on-failure`
Expected: both PASS (Tasks 4–6 already implement the behaviour; this task is the real-path proof). If either fails, the defect is in Tasks 4–6: fix it there with a `*Regression*`-named unit test in `ProducerClaimTest.cpp` reproducing it first (CLAUDE.md #6).

- [ ] **Step 3: Regression gate + full suite, then commit**

```bash
cmake --build build -j8
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add tests/client/ClaimClientTest.cpp CMakeLists.txt
git commit -m "test(producer): claims end to end through a real Client in both runner modes (2b-B)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: client — `TypedTopic::claim(keyValue)` → the generated encoder over the claim

**Files:**
- Modify: `rune-client-cpp/include/rune/client/TypedTopic.h` (includes; `TypedClaim<E>` before `class TypedTopic`; `claim` members after `send`)
- Create: `rune-client-cpp/tests/support/GenClaimSchemas.cpp`
- Create: `rune-client-cpp/tests/client/TypedClaimTest.cpp`
- Modify: `rune-client-cpp/CMakeLists.txt` (codegen tool + target after the `rune_ql` block; `rune_invoker_tests` sources / include dir / dependency)

**Interfaces:**
- Consumes (Part C — MUST be merged; verified in Step 0): `TypedTopic(ProducerT producer, TopicHandle topic, uint16_t version)`; members `ProducerT producer_`, `TopicHandle topic_`, `uint16_t version_`; `send(rec)` routes with `Key(rec.key())` (keyless schema: `Key{}`); the generated struct's `key()`. From Task 2: `T::Encoder` (`kEncodedLength`, `kHasKey`, `KeyType`, `key(KeyType)`), from Task 4: `Producer::tryClaim`, `Claim`.
- Produces (namespace `rune::client`):
  - `template <class E> class TypedClaim : private detail::TypedClaimSlot<E>, public E` — the encoder's setters/getters, plus `explicit operator bool() const noexcept`, `SendResult result() const noexcept`, `bool commit() noexcept`, `void abort() noexcept`; non-copyable, non-movable (returned as a prvalue: `auto w = topic.claim(k);`).
  - `TypedTopic<T>::claim(const typename T::Encoder::KeyType& keyValue) -> TypedClaim<typename T::Encoder>` (requires `T::Encoder::kHasKey`); `TypedTopic<T>::claim() -> TypedClaim<typename T::Encoder>` (requires `!T::Encoder::kHasKey`).
  - Build-time generated test schemas `gen/position.gen.h` (`rune::gen::Position`, key `userId` Int64) and `gen/tick_sample.gen.h` (`rune::gen::TickSample`, keyless), target `generate_claim_schema_headers`, include dir `${RUNE_CLAIM_GEN_DIR}`.

- [ ] **Step 0: Verify the Part C interface is on this branch**

Run: `grep -n "TopicHandle *topic_;\|uint16_t *version_\|recordKey_\|sendWithKey_" include/rune/client/TypedTopic.h`
Expected: hits for the member, the version, and C's key routing (`send` → `sendWithKey_(rec, recordKey_(rec))`; updated 2026-10-19 — C landed with these names). Also add to `TypedClaimTest.cpp` (Step 1): `TEST(TypedClaimRegression, OverlongStringKeyThrowsAndClaimsNothingRegression)` — a schema keyed by `string(20)` (add it to `GenClaimSchemas.cpp` as `long_key.gen.h`, `rune::gen::LongKey{code string(20) KEY, v Int64}`): `claim(std::string(17, 'x'))` throws `std::invalid_argument` and the frame capture shows no record and the ring no committed slot; `claim(std::string(16, 'x'))` routes like `send` of the same record. Run: `grep -n "key()" ../rune-codec/src/Generator.cpp` — expected: at least one hit (C's generated `key()`). If any is missing, stop: Part C has not landed (B depends on it). If C landed with different member names, use C's names in Step 3 (the only references are `producer_`, `topic_`, `version_`).

- [ ] **Step 1: Write the failing tests**

Create `rune-client-cpp/tests/support/GenClaimSchemas.cpp`:

```cpp
// tests/support/GenClaimSchemas.cpp — Phase 2b Part B: writes the all-fixed
// schemas the claim tests and the claim bench compile against (build-time
// codegen through rune-codec's generateHeader, like characters.gen.h).
//   position     userId Int64 KEY, zone Int32, x/y/z Float64, flags UInt16,
//                seq UInt32, tick UInt64, alive Bool, kind Enum{0,1,2},
//                tag string(8)            -> blockLength 63, kEncodedLength 67
//   tick_sample  seq UInt64, value Float64 (no key field)
#include <rune/codec/Descriptor.h>
#include <rune/codec/Generator.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {
bool write(const std::string& path, const rune::codec::Descriptor& d) {
    std::ofstream f(path, std::ios::trunc);
    if (!f) {
        std::fprintf(stderr, "rune_gen_claim_schemas: cannot write '%s'\n", path.c_str());
        return false;
    }
    f << rune::codec::generateHeader(d);
    return static_cast<bool>(f);
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: rune_gen_claim_schemas <out-dir>\n");
        return 1;
    }
    using rune::codec::DescriptorBuilder;
    using rune::codec::PrimType;
    const auto position = DescriptorBuilder("position")
                              .add("userId", PrimType::Int64, 1, true)
                              .add("zone", PrimType::Int32)
                              .add("x", PrimType::Float64)
                              .add("y", PrimType::Float64)
                              .add("z", PrimType::Float64)
                              .add("flags", PrimType::UInt16)
                              .add("seq", PrimType::UInt32)
                              .add("tick", PrimType::UInt64)
                              .add("alive", PrimType::Bool)
                              .add("kind", PrimType::Enum, 1, false, {0, 1, 2})
                              .addFixedStr("tag", 8)
                              .build();
    const auto tick = DescriptorBuilder("tick_sample")
                          .add("seq", PrimType::UInt64)
                          .add("value", PrimType::Float64)
                          .build();
    const std::string dir = argv[1];
    return write(dir + "/position.gen.h", position) && write(dir + "/tick_sample.gen.h", tick) ? 0 : 1;
}
```

In `rune-client-cpp/CMakeLists.txt`, directly after the `rune_ql` block (`target_compile_features(rune_ql PUBLIC cxx_std_23)`), add:

```cmake
# ── Phase 2b Part B: build-time generated all-fixed schemas (claim tests + bench) ──
add_executable(rune_gen_claim_schemas tests/support/GenClaimSchemas.cpp)
target_link_libraries(rune_gen_claim_schemas PRIVATE rune_codec)
set(RUNE_CLAIM_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/claim-gen")
file(MAKE_DIRECTORY "${RUNE_CLAIM_GEN_DIR}/gen")
add_custom_command(
    OUTPUT  "${RUNE_CLAIM_GEN_DIR}/gen/position.gen.h" "${RUNE_CLAIM_GEN_DIR}/gen/tick_sample.gen.h"
    COMMAND $<TARGET_FILE:rune_gen_claim_schemas> "${RUNE_CLAIM_GEN_DIR}/gen"
    DEPENDS rune_gen_claim_schemas
    COMMENT "2b-B: generating position.gen.h / tick_sample.gen.h (all-fixed flyweights)"
)
add_custom_target(generate_claim_schema_headers
    DEPENDS "${RUNE_CLAIM_GEN_DIR}/gen/position.gen.h" "${RUNE_CLAIM_GEN_DIR}/gen/tick_sample.gen.h")
```

In the `rune_invoker_tests` block, add `tests/client/TypedClaimTest.cpp` to its sources and, after its `target_include_directories(...)` line, add:

```cmake
        target_include_directories(rune_invoker_tests PRIVATE ${RUNE_CLAIM_GEN_DIR})
        add_dependencies(rune_invoker_tests generate_claim_schema_headers)
```

Create `rune-client-cpp/tests/client/TypedClaimTest.cpp`:

```cpp
// tests/client/TypedClaimTest.cpp — Phase 2b Part B: TypedTopic::claim(key) — the
// generated PositionEncoder over a claimed record, routed exactly like
// TypedTopic::send(rec) (Part C), the bound schema version stamped, refusals
// harmless, zero allocation.  Bare ProducerImpl (Ring ingress), no Aeron.
#include "../support/AllocCounter.h"
#include "../support/FrameCapture.h"
#include "gen/position.gen.h"
#include "gen/tick_sample.gen.h"

#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace rc = rune::client;
using rc::Key;
using rc::Producer;
using rc::ProducerConfig;
using rc::SendResult;
using rc::TopicHandle;
using rc::TypedTopic;
using rc::producer::ProducerImpl;
using rune::gen::Position;
using rune::gen::PositionDecoder;
using rune::gen::PositionEncoder;
using rune::gen::TickSample;
using test_support::FrameCapture;

static_assert(std::is_same_v<Position::Encoder, PositionEncoder>);
static_assert(PositionEncoder::kEncodedLength == 67u);
static_assert(!rune::gen::TickSampleEncoder::kHasKey);

namespace {
constexpr int64_t kNow = 1'000'000'000;

ProducerConfig voidCfg() {
    ProducerConfig c;
    c.acks        = rc::Acks::None;
    c.lingerUs    = 0;
    c.batchBytes  = 16 * 1024;
    c.maxInFlight = 8;
    return c;
}

void pump(ProducerImpl& impl, const ProducerImpl::SendFn& sink) {
    for (int i = 0; i < 16; ++i) {
        impl.drainRing(kNow, 256);
        impl.flushBatches(kNow, 42, sink);
    }
}

std::span<const uint8_t> span(const std::string& s) {
    return {reinterpret_cast<const uint8_t*>(s.data()), s.size()};
}
}  // namespace

// Review Focus 1.
TEST(TypedClaim, RoutesLikeTypedSendForEveryKey) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    for (int64_t k = 0; k < 64; ++k) {
        Position rec;
        rec.userId = k;
        rec.zone   = 1;
        topic.send(rec);
        auto w = topic.claim(k);
        ASSERT_TRUE(w) << static_cast<int>(w.result());
        w.zone(2);
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 128u);
    std::map<int64_t, std::vector<test_support::CapturedRecord>> byUser;
    for (const auto& r : cap.recs) byUser[PositionDecoder(span(r.value)).userId()].push_back(r);
    ASSERT_EQ(byUser.size(), 64u);
    for (const auto& [k, rs] : byUser) {
        ASSERT_EQ(rs.size(), 2u) << k;
        EXPECT_EQ(rs[0].partition, rs[1].partition) << "claim(" << k << ") must route like send";
        EXPECT_EQ(rs[0].key, rs[1].key) << "same record key for userId " << k;
        EXPECT_EQ(rs[0].partition, Key(k).partition(8)) << k;
        EXPECT_EQ(PositionDecoder(span(rs[0].value)).zone(), 1) << "per-key order: send first";
        EXPECT_EQ(PositionDecoder(span(rs[1].value)).zone(), 2);
    }
}

TEST(TypedClaim, StampsTheBoundVersionAndTheKeyFieldAndRoundTripsEveryField) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{7}};
    {
        auto w = topic.claim(int64_t{42});
        ASSERT_TRUE(w);
        w.zone(-3).x(1.5).y(-2.25).z(1e9).flags(0xBEEF).seq(4'000'000'000u).tick(0x0102030405060708ull)
            .alive(true).kind(2).tag("north");
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 1u);
    const std::string& v = cap.recs[0].value;
    ASSERT_EQ(v.size(), PositionEncoder::kEncodedLength);
    EXPECT_EQ(static_cast<uint8_t>(v[2]), 7);
    EXPECT_EQ(static_cast<uint8_t>(v[3]), 0) << "the bound version is stamped at bytes [2..3]";
    Position back;
    back.decode(span(v));
    EXPECT_EQ(back.userId, 42) << "claim(key) writes the key field";
    EXPECT_EQ(back.zone, -3);
    EXPECT_EQ(back.x, 1.5);
    EXPECT_EQ(back.y, -2.25);
    EXPECT_EQ(back.z, 1e9);
    EXPECT_EQ(back.flags, 0xBEEF);
    EXPECT_EQ(back.seq, 4'000'000'000u);
    EXPECT_EQ(back.tick, 0x0102030405060708ull);
    EXPECT_TRUE(back.alive);
    EXPECT_EQ(back.kind, 2);
    EXPECT_EQ(std::string_view(back.tag.data(), 8), std::string_view("north\0\0\0", 8));
    const Key k42(int64_t{42});
    EXPECT_EQ(cap.recs[0].key, std::string(reinterpret_cast<const char*>(k42.bytes().data()), k42.size()));
}

TEST(TypedClaim, ARefusedClaimWritesOnlyItsScratchAndCommitsNothing) {
    auto* impl = new ProducerImpl(voidCfg(), 4 * 1024);  // nobody drains: it fills
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    int accepted = 0;
    bool refused = false;
    for (int64_t i = 0; i < 1000 && !refused; ++i) {
        auto w = topic.claim(i);
        if (!w) {
            refused = true;
            EXPECT_EQ(w.result(), SendResult::BackPressured);
            w.zone(9).x(1.0).tag("scratch");  // safe: the encoder writes its private scratch block
            EXPECT_FALSE(w.commit());
            break;
        }
        w.zone(1);
        ASSERT_TRUE(w.commit());
        ++accepted;
    }
    ASSERT_TRUE(refused) << "a 4 KiB ring must fill";
    ASSERT_GT(accepted, 0);
    FrameCapture cap;
    pump(*impl, cap.fn());
    EXPECT_EQ(cap.recs.size(), static_cast<std::size_t>(accepted));
    for (const auto& r : cap.recs) EXPECT_EQ(PositionDecoder(span(r.value)).zone(), 1);
}

TEST(TypedClaim, AKeylessSchemaClaimsPartitionZeroWithAnEmptyKey) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<TickSample> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    for (uint64_t i = 0; i < 4; ++i) {
        auto w = topic.claim();
        ASSERT_TRUE(w);
        w.seq(i).value(0.5);
        ASSERT_TRUE(w.commit());
    }
    FrameCapture cap;
    pump(*impl, cap.fn());
    ASSERT_EQ(cap.recs.size(), 4u);
    for (const auto& r : cap.recs) {
        EXPECT_EQ(r.partition, 0) << "keyless typed topics keep partition 0 (spec C)";
        EXPECT_TRUE(r.key.empty());
    }
}

TEST(TypedClaimAllocRegression, TypedClaimSteadyStateAllocatesNothingRegression) {
    auto* impl = new ProducerImpl(voidCfg(), 1 << 20);
    TypedTopic<Position> topic{Producer{impl}, TopicHandle{1, 8}, uint16_t{1}};
    const ProducerImpl::SendFn sink = [](int32_t, int32_t, int32_t, const uint8_t*, std::size_t) {
        return true;
    };
    auto cycle = [&](int64_t k) {
        {
            auto w = topic.claim(k);
            if (w) {
                w.zone(1).x(2.0);
                (void)w.commit();
            }
        }
        pump(*impl, sink);
    };
    for (int64_t k = 0; k < 64; ++k) cycle(k);
    alloc_counter::start();
    for (int64_t k = 64; k < 1064; ++k) cycle(k);
    EXPECT_EQ(alloc_counter::stop(), 0L) << "typed claim + commit + drain/flush must not allocate";
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build build -j8 --target rune_invoker_tests`
Expected: compile error `no member named 'claim' in 'rune::client::TypedTopic<rune::gen::Position>'`.

- [ ] **Step 3: Implement** — in `TypedTopic.h`, add `#include <array>` and `#include <type_traits>` to the includes; directly before the `// TypedTopic<T, ProducerT>` banner add:

```cpp
// ---------------------------------------------------------------------------
// TypedClaim<E> — Phase 2b Part B: what TypedTopic::claim() returns: the
// generated encoder E (all-fixed schema) bound to the claimed record bytes,
// plus the claim's commit() / abort().  Chain the setters, then commit():
//   auto w = positions.claim(userId);  w.zone(4).x(118.5);  w.commit();
// A refused claim is false (result(): BackPressured / Closed / ...); its
// setters then write a private scratch block and commit() returns false.
// Neither copyable nor movable (E points into the claim or into the scratch).
// ---------------------------------------------------------------------------
namespace detail {
template <class E>
struct TypedClaimSlot {
    Claim                                  claim;
    std::array<uint8_t, E::kEncodedLength> scratch{};  // a refused claim's encoder writes here
    explicit TypedClaimSlot(Claim&& c) noexcept : claim(std::move(c)) {}
    [[nodiscard]] std::span<uint8_t> target() noexcept {
        return claim ? claim.buffer() : std::span<uint8_t>(scratch.data(), scratch.size());
    }
};
}  // namespace detail

template <class E>
class TypedClaim : private detail::TypedClaimSlot<E>, public E {
    using Slot = detail::TypedClaimSlot<E>;

public:
    /// `boundVersion`: the TypedTopic's bound schema version, stamped over the
    /// encoder's codegen kVersion at bytes [2..3]; `keyValue` (keyed schemas):
    /// written into the key field.
    template <class... K>
    TypedClaim(Claim&& c, uint16_t boundVersion, const K&... keyValue)
        : Slot(std::move(c)), E(Slot::target()) {
        const std::span<uint8_t> t = Slot::target();
        t[2] = static_cast<uint8_t>(boundVersion & 0xffu);
        t[3] = static_cast<uint8_t>((boundVersion >> 8) & 0xffu);
        if constexpr (sizeof...(K) == 1) E::key(keyValue...);
    }
    TypedClaim(const TypedClaim&)            = delete;
    TypedClaim& operator=(const TypedClaim&) = delete;
    TypedClaim(TypedClaim&&)                 = delete;
    TypedClaim& operator=(TypedClaim&&)      = delete;

    [[nodiscard]] explicit operator bool() const noexcept { return static_cast<bool>(Slot::claim); }
    [[nodiscard]] SendResult result() const noexcept { return Slot::claim.result(); }
    /// Publish (see Claim::commit).  Destruction without commit() aborts.
    bool commit() noexcept { return Slot::claim.commit(); }
    void abort() noexcept { Slot::claim.abort(); }
};
```

In `class TypedTopic`, directly after the `send(const T& rec)` member, add:

```cpp
    // ---- Phase 2b Part B: zero-copy typed produce (all-fixed schemas) -------

    /// Claim one record (T::Encoder::kEncodedLength bytes) routed EXACTLY like
    /// send(rec) with rec.key() == keyValue (Part C: Key(rec.key())), with the
    /// bound schema version and the key field already written; set the other
    /// fields on the returned encoder and commit().  ZERO allocation.  The
    /// claim follows Claim's rules (do not hold it across blocking work; it
    /// must not outlive this TypedTopic).
    template <class U = T>
        requires requires { typename U::Encoder; } && U::Encoder::kHasKey
    [[nodiscard]] TypedClaim<typename U::Encoder> claim(const typename U::Encoder::KeyType& keyValue) {
        using E = typename U::Encoder;
        return TypedClaim<E>(producer_.tryClaim(topic_, claimKey_(keyValue), E::kEncodedLength), version_, keyValue);
    }

    /// (Updated 2026-10-19) The SAME key rules as Part C's recordKey_(rec): a trivially-copyable key
    /// must fit Key::kMaxInline (static_assert); a string(N) key arrives as std::string_view and
    /// longer than Key::kMaxInline throws std::invalid_argument BEFORE anything is claimed (never
    /// truncated).  Pinned by TypedClaim.RoutesLikeTypedSendForEveryKey and the new
    /// TypedClaimRegression.OverlongStringKeyThrowsAndClaimsNothingRegression.
    template <class KeyT>
    [[nodiscard]] static Key claimKey_(const KeyT& k) {
        if constexpr (std::is_convertible_v<KeyT, std::string_view>) {
            const std::string_view sv(k);
            if (sv.size() > Key::kMaxInline) [[unlikely]]
                throw std::invalid_argument("TypedTopic::claim: key is " + std::to_string(sv.size()) +
                                            " bytes; a record key holds at most " + std::to_string(Key::kMaxInline));
            return Key(std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(sv.data()), sv.size()));
        } else {
            static_assert(std::is_trivially_copyable_v<KeyT> && sizeof(KeyT) <= Key::kMaxInline,
                          "TypedTopic::claim: the key field cannot be a record key without truncation");
            return Key(k);
        }
    }

    /// Keyless schema: partition 0, empty record key (spec C's documented
    /// behaviour for typed topics without a key field).
    template <class U = T>
        requires requires { typename U::Encoder; } && (!U::Encoder::kHasKey)
    [[nodiscard]] TypedClaim<typename U::Encoder> claim() {
        using E = typename U::Encoder;
        return TypedClaim<E>(producer_.tryClaim(topic_, Key{}, E::kEncodedLength), version_);
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `cmake --build build -j8 --target rune_invoker_tests rune_open_topic_tests && ctest --test-dir build -R 'TypedClaim|OpenTopic|TypedTopic' --output-on-failure`
Expected: all PASS (`rune_open_topic_tests` proves the `TypedTopic<StubT, StubProducer>` seam still compiles — `claim()` is a constrained template, never instantiated for a schema without `Encoder`).

- [ ] **Step 5: Regression gate + full suite, then commit**

```bash
cmake --build build -j8
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add include/rune/client/TypedTopic.h tests/support/GenClaimSchemas.cpp tests/client/TypedClaimTest.cpp \
        CMakeLists.txt
git commit -m "feat(typed): TypedTopic::claim(key) returns the generated encoder over the claim (2b-B)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: client — the four-way benchmark gate + docs

**Files:**
- Create: `rune-client-cpp/bench/ProducerClaimBench.cpp`
- Modify: `rune-client-cpp/CMakeLists.txt` (inside `if(BUILD_BENCHMARKS AND aeron_FOUND)`)
- Modify: `rune-client-cpp/CLAUDE.md` (§2.1 Producer)
- Modify: `rune-client-cpp/include/rune/client/producer/Producer.h` (file header "Usage" block)

**Interfaces:**
- Consumes: Tasks 4–8 (`tryClaim`, `TypedTopic::claim`, `gen/position.gen.h`), `tests/support/AllocCounter.{h,cpp}`, `producer::CountingSink`, `ProducerImpl::enableDirectIngress`.
- Produces: executable `rune_producer_claim_bench`; env `CLAIM_MODE=send|claim|typed_send|typed_claim`, `CLAIM_INGRESS=thread|invoker`, `CLAIM_RECORDS` (default 1'000'000), `CLAIM_PARTITIONS` (default 8); prints `RESULT mode=… ingress=… records=… p50_ns=… p99_ns=… allocs=… frames=…`; exit code 1 if a claim form allocated after warm-up (the spec gate).

- [ ] **Step 1: Write the benchmark** — create `rune-client-cpp/bench/ProducerClaimBench.cpp`:

```cpp
// bench/ProducerClaimBench.cpp — Phase 2b Part B gate: per-record cost and
// allocations of the four produce forms, in-process (no Aeron):
//   send        Producer::trySend(topic, key, span)    — encoded in a caller buffer
//   claim       Producer::tryClaim + PositionEncoder    — encoded in place
//   typed_send  TypedTopic<Position>::send(rec)         — struct -> reused buffer -> send
//   typed_claim TypedTopic<Position>::claim(key)        — encoded in place
// CLAIM_INGRESS=thread  (default): this thread produces, a duty thread drains
//                        the producer ring and flushes (Thread mode);
//               invoker : one thread, Direct ingress, a flush pass every 64
//                        records (Invoker mode; the pass is not timed).
// Per-record latency = the app-side call(s) for one record including any
// back-pressure retry (steady_clock around each record).  Allocations = global
// operator new on THIS thread after a 10'000-record warm-up.  Exit 1 if a claim
// form allocated (spec B: claim paths 0 allocations in steady state).
#include "../tests/support/AllocCounter.h"
#include "gen/position.gen.h"

#include <rune/client/Key.h>
#include <rune/client/TypedTopic.h>
#include <rune/client/producer/CountingSink.h>
#include <rune/client/producer/Producer.h>
#include <rune/client/producer/ProducerImpl.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <vector>

using namespace rune::client;
using rune::client::producer::CountingSink;
using rune::client::producer::ProducerImpl;
using rune::gen::Position;
using rune::gen::PositionEncoder;
using clk = std::chrono::steady_clock;

namespace {
int64_t envI64(const char* n, int64_t d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::atoll(v) : d;
}
std::string envStr(const char* n, const char* d) {
    const char* v = std::getenv(n);
    return (v != nullptr && *v != '\0') ? std::string(v) : std::string(d);
}
int64_t nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(clk::now().time_since_epoch()).count();
}
enum class Mode { Send, Claim, TypedSend, TypedClaim };
template <class Enc>
void encodeInto(Enc& e, int64_t i) {
    e.zone(static_cast<int32_t>(i & 7)).x(1.5).y(2.5).z(-3.5).seq(static_cast<uint32_t>(i))
        .tick(static_cast<uint64_t>(i)).alive(true).kind(1).tag("bench");
}
}  // namespace

int main() {
    const std::string modeName = envStr("CLAIM_MODE", "claim");
    const Mode mode = modeName == "send"       ? Mode::Send
                    : modeName == "typed_send" ? Mode::TypedSend
                    : modeName == "typed_claim" ? Mode::TypedClaim
                                               : Mode::Claim;
    const bool    invoker    = envStr("CLAIM_INGRESS", "thread") == "invoker";
    const int64_t N          = envI64("CLAIM_RECORDS", 1'000'000);
    const int32_t partitions = static_cast<int32_t>(envI64("CLAIM_PARTITIONS", 8));
    constexpr int64_t kWarm  = 10'000;
    if (N <= kWarm) {
        std::fprintf(stderr, "CLAIM_RECORDS must exceed the %lld-record warm-up\n", static_cast<long long>(kWarm));
        return 2;
    }

    ProducerConfig cfg;
    cfg.acks         = Acks::None;
    cfg.lingerUs     = 0;
    cfg.batchRecords = 64;
    cfg.maxInFlight  = 32;
    auto* raw = new ProducerImpl(cfg, 32 * 1024 * 1024);  // the default producer ring
    ProducerImpl& impl = *raw;
    if (invoker) impl.enableDirectIngress({std::this_thread::get_id(), nullptr, nullptr});
    const TopicHandle topic{0, partitions};
    std::optional<Producer>             plain;
    std::optional<TypedTopic<Position>> typed;
    if (mode == Mode::TypedSend || mode == Mode::TypedClaim) {
        typed.emplace(Producer{raw}, topic, uint16_t{1});
    } else {
        plain.emplace(raw);
    }

    uint64_t frames = 0, bytes = 0;
    const CountingSink sink{&frames, &bytes};
    std::atomic<bool> stop{false};
    std::thread duty;
    if (!invoker) {
        duty = std::thread([&] {
            while (!stop.load(std::memory_order_acquire)) {
                const int64_t t = nowNs();
                impl.publishHeartbeat(t);  // a blocking typed send() waits on this, never times out
                impl.drainRing(t, 256);
                impl.flushBatches(t, 1, sink);
            }
            for (int i = 0; i < 4096; ++i) {  // drain what is left
                const int64_t t = nowNs();
                impl.drainRing(t, 256);
                impl.flushBatches(t, 1, sink);
            }
        });
    }
    auto relieve = [&] {  // back-pressured: Invoker flushes itself, Thread mode yields to the duty thread
        if (invoker) impl.flushBatches(nowNs(), 1, sink);
        else std::this_thread::yield();
    };

    std::vector<int64_t> lat(static_cast<std::size_t>(N));
    std::vector<uint8_t> scratch(PositionEncoder::kEncodedLength);  // "send": the caller's own buffer
    long allocs = 0;
    for (int64_t i = 0; i < N; ++i) {
        if (i == kWarm) alloc_counter::startThisThread();
        const Key key(i);
        const int64_t t0 = nowNs();
        switch (mode) {
        case Mode::Send: {
            PositionEncoder e(scratch);
            e.userId(i);
            encodeInto(e, i);
            while (plain->trySend(topic, key, scratch) != SendResult::Ok) relieve();
            break;
        }
        case Mode::Claim:
            for (;;) {
                Claim c = plain->tryClaim(topic, key, PositionEncoder::kEncodedLength);
                if (c) {
                    PositionEncoder e(c.buffer());
                    e.userId(i);
                    encodeInto(e, i);
                    (void)c.commit();
                    break;
                }
                relieve();
            }
            break;
        case Mode::TypedSend: {
            Position rec;
            rec.userId = i;
            rec.zone   = static_cast<int32_t>(i & 7);
            rec.x = 1.5; rec.y = 2.5; rec.z = -3.5;
            rec.seq   = static_cast<uint32_t>(i);
            rec.tick  = static_cast<uint64_t>(i);
            rec.alive = true;
            rec.kind  = 1;
            std::memcpy(rec.tag.data(), "bench", 5);
            typed->send(rec);  // void send: Thread mode waits on the duty thread
            break;
        }
        case Mode::TypedClaim:
            for (;;) {
                auto w = typed->claim(i);
                if (w) {
                    encodeInto(w, i);
                    (void)w.commit();
                    break;
                }
                relieve();
            }
            break;
        }
        lat[static_cast<std::size_t>(i)] = nowNs() - t0;
        if (invoker && (i & 63) == 63) impl.flushBatches(nowNs(), 1, sink);  // end of an owner pass (untimed)
    }
    allocs = alloc_counter::stopThisThread();
    if (invoker) {
        impl.flushBatches(nowNs(), 1, sink);
    } else {
        stop.store(true, std::memory_order_release);
        duty.join();
    }

    std::vector<int64_t> steady(lat.begin() + kWarm, lat.end());
    auto pct = [&](double q) {
        const auto k = static_cast<std::size_t>(q * static_cast<double>(steady.size() - 1));
        std::nth_element(steady.begin(), steady.begin() + static_cast<std::ptrdiff_t>(k), steady.end());
        return steady[k];
    };
    const int64_t p50 = pct(0.50);
    const int64_t p99 = pct(0.99);
    std::printf("RESULT mode=%s ingress=%s records=%lld p50_ns=%lld p99_ns=%lld allocs=%ld frames=%llu\n",
                modeName.c_str(), invoker ? "invoker" : "thread", static_cast<long long>(N),
                static_cast<long long>(p50), static_cast<long long>(p99), allocs,
                static_cast<unsigned long long>(frames));
    const bool claimForm = mode == Mode::Claim || mode == Mode::TypedClaim;
    if (claimForm && allocs != 0) {
        std::fprintf(stderr, "GATE FAIL: %s allocated %ld times in steady state (spec B: 0)\n",
                     modeName.c_str(), allocs);
        return 1;
    }
    return 0;
}
```

Note on Invoker `typed_send`: it uses the void `send()`, which in Direct mode without a pump hook throws when back-pressured; the untimed flush every 64 records (`batchRecords = 64`) keeps every builder below its cap, so it never back-pressures.

In `rune-client-cpp/CMakeLists.txt`, inside `if(BUILD_BENCHMARKS AND aeron_FOUND)` after the `rune_producer_encode_bench` block, add:

```cmake
    # Phase 2b Part B — zero-copy produce gate: send vs tryClaim vs typed send vs
    # typed claim (p50/p99 + steady-state allocations; exit 1 if a claim form allocates)
    add_executable(rune_producer_claim_bench bench/ProducerClaimBench.cpp tests/support/AllocCounter.cpp)
    target_include_directories(rune_producer_claim_bench PRIVATE ${RUNE_PROTOCOL_INCLUDE} ${RUNE_CLAIM_GEN_DIR})
    target_link_libraries(rune_producer_claim_bench PRIVATE rune_client_api rune_client_producer)
    add_dependencies(rune_producer_claim_bench generate_claim_schema_headers)
```

- [ ] **Step 2: Build it Release and run the gate (8 runs)**

```bash
conan install . -of build-bench -s build_type=Release -s arch=x86_64 -s compiler.cppstd=gnu17 --build=missing
cmake -S . -B build-bench -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=ON \
      -DCMAKE_TOOLCHAIN_FILE=build-bench/Release/generators/conan_toolchain.cmake \
      -DCMAKE_PREFIX_PATH="$PWD/build-bench/Release/generators"
cmake --build build-bench -j8 --target rune_producer_claim_bench
for ing in thread invoker; do
  for m in send claim typed_send typed_claim; do
    CLAIM_MODE=$m CLAIM_INGRESS=$ing ./build-bench/rune_producer_claim_bench || echo "GATE FAILED: $m/$ing"
  done
done
```

Expected: 8 `RESULT` lines; `allocs=0` on every `claim` / `typed_claim` line and no `GATE FAILED`. Record the eight lines in the commit message body. (Expectation, not a gate: `claim` ≤ `send` and `typed_claim` < `typed_send` per record — the claim forms skip the caller-side buffer and the struct round-trip.)

- [ ] **Step 3: Docs** — in `rune-client-cpp/CLAUDE.md` §2.1 Producer, add a bullet after the F8c C6 bullet:

```markdown
- Phase 2b zero-copy produce: `Producer::tryClaim(topic, key, len)` → `Claim`
  (`buffer()` / `commit()` / `abort()`, RAII, refused = false + `result()`),
  `claimBatch(topic, key, n, recLen)` → `BatchClaim` (`record(i)`; ONE RecordBatch
  via ring message `kMsgClaimBatch = 4`), `TypedTopic::claim(key)` → the generated
  `XEncoder` over the claimed bytes (all-fixed schemas; rune-codec emits
  `XEncoder` / `XDecoder`).  Thread mode claims a producer-ring slot (2 copies left:
  ring → frame → Aeron); Invoker mode reserves the record in the partition's
  Produce frame (1 copy: frame → Aeron) under the `DirectClaimOpen` state — every
  other send on the producer is `ClaimHeld` until it settles, and a failed /
  closed partition abandons it (`commit()` → false).  Zero allocation (gates:
  `ProducerClaimAllocRegression`, `TypedClaimAllocRegression`,
  `rune_producer_claim_bench`).
```

In `Producer.h`'s file-header `// Usage:` block, append:

```cpp
//   auto c = p.tryClaim(topic, Key(id), PositionEncoder::kEncodedLength);  // zero-copy (2b)
//   if (c) { PositionEncoder(c.buffer()).zone(4).x(1.5); c.commit(); }
```

- [ ] **Step 4: Regression gate + full suite, then commit**

```bash
cmake --build build -j8
ctest --test-dir build -R Regression --output-on-failure
ctest --test-dir build --output-on-failure
git add bench/ProducerClaimBench.cpp CMakeLists.txt CLAUDE.md include/rune/client/producer/Producer.h
git commit -m "bench(producer): send vs tryClaim vs typed send vs typed claim — 0-alloc claim gate (2b-B)

<paste the eight RESULT lines here>

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Optional dashboard push (client CLAUDE.md §5): `../rune-broker/scripts/rune-live.sh test "rune-client/claim" passed commit=$(git rev-parse --short HEAD)`.

---

## Finish

- [ ] rune-codec: `ctest --test-dir build -R Regression` then `ctest --test-dir build` → 0 failures; `FlyweightGen.*`, `Flyweight.*`, `Generator.*` green.
- [ ] rune-client-cpp: `ctest --test-dir build -R Regression` (includes `ProducerClaimAllocRegression.*`, `TypedClaimAllocRegression.*`) then `ctest --test-dir build` → 0 failures; `ProducerClaim*`, `ClaimClient.*`, `TypedClaim*` green.
- [ ] Copy-count proofs green: `ProducerClaim.RingClaimIsCopiedOnceIntoTheFrame` (Thread: 2), `ProducerClaimDirect.ClaimWritesStraightIntoTheProduceFrame` (Invoker: 1).
- [ ] `rune_producer_claim_bench`: 8 RESULT lines recorded, `allocs=0` for both claim forms in both ingress modes.
- [ ] Fast-forward the integration lines to `feat/phase2b-b-zero-copy-produce` (rune-codec `feat/schema-evolution-durable-tags`, rune-client-cpp `feature/materialized-views`) only on the controller's instruction. Nothing pushed.

## Spec questions

1. **`tryClaim` / `claimBatch` return type.** The spec says `std::optional<Claim>` (nullopt = BackPressured), but the published docs call `c.buffer()` / `c.commit()` directly on the result, and an `optional` cannot say *why* a claim was refused (MessageTooLarge / WrongThread / Closed / QueueFull are all required by the spec). This plan returns `Claim` / `BatchClaim` themselves with `explicit operator bool` (false = refused) and `result()`; `if (auto c = p.tryClaim(...))` works either way. Confirm.
2. **`commit()` returns `bool`** (spec: `void`) — false when the claim was abandoned under the caller (Invoker close / failed partition; Thread-mode Client-close unblock). Source-compatible with the docs (`c.commit();`). Confirm.
3. **Docs claim length bug.** The docs claim `PositionEncoder::kBlockLength` bytes, but the encoder writes `[blockLength][version]` + block = `kEncodedLength` (spec: `4 + kBlockLength`). The plan's encoder throws `std::length_error` on a short buffer (Review Focus 4). The site example should say `kEncodedLength` — or should `XEncoder::kBlockLength` mean the encoded length (diverging from the struct's `kBlockLength`)?
4. **Composite key in the docs.** The docs show `pos.claim(userId, characterId)`; composite keys are Phase 3. This plan implements `claim(keyValue)` (single key field) and `claim()` (keyless schema). The docs example will not compile until Phase 3.
5. **`claimBatch` size cap.** Beyond the spec's `n × recLen ≤ maxMsgLength`, the plan refuses (`MessageTooLarge`) a batch whose conservative wire bound exceeds `ProducerConfig::maxFrameBytes`, because a claimBatch is ONE frame and `maxFrameBytes` is the client's hard per-frame cap. A batch may exceed `batchBytes` (it is flushed alone). Confirm.
6. **Invoker rules the spec does not state:** (a) a claim from inside a producer callback (duty work) is refused `BackPressured` — claims are not deferred (use `trySend`, which defers); (b) while an Invoker claim is open every other send / claim on that producer returns the new appended `SendResult::ClaimHeld` (the void `send()` throws `std::logic_error`), and its partition is not flushed — a Tier-2 `BatchBuilder::send()` is still accepted (it queues behind the claim's record, so submission order holds); (c) a failure / close of that partition abandons the claim. Thread mode: holding a claim and calling a blocking `send()` on the same producer from the same thread can wait forever (documented, not detected — detecting it would need cross-thread state).
7. **Acked `claimBatch`.** The spec lists ack overloads only for `tryClaim`; `claimBatch` has none (it reports through `batchAcksTo` / `onBatchAck`, n units). Add `claimBatch(..., Completions&, Token)` / `AckToken` forms?
8. **Enum fields and zeroed blocks.** Encoders zero the block, so an unset `Enum` field encodes 0 — rejected by the broker's Validator unless 0 is whitelisted (same as today's struct default `= {}`). Should the encoder initialise enum fields to their first whitelisted value instead?
9. **Name collisions.** A non-key field literally named `key` suppresses the flyweights (its setter would collide with the key helpers; Part C's struct `key()` has the same collision). Fields named `commit` / `abort` / `result` are hidden by `TypedClaim`'s members (reachable via `static_cast<E&>(w)`). Reject such names in Rune QL instead?
10. **Real-broker e2e.** Spec B's gates are tests + benchmarks; this plan's end-to-end proof is a real `Client` over Aeron IPC to the in-process `InvokerGateway` (Task 7), not the dockerized broker. Is a docker e2e probe (e.g. a typed claim against a live `rune>`-defined all-fixed topic) required for 2b?
11. **Part C interface assumptions** (Task 8 Step 0): `TypedTopic(ProducerT, TopicHandle, uint16_t version)`, members `producer_` / `topic_` / `version_`, `send` keyed by `Key(rec.key())`, and `key()`'s return type equal to the field's member type (`std::array<char, N>` for `string(N)`), so `Key(keyValue)` in `claim` picks the same `Key` constructor as C's `send`.

### Spec gaps found 2026-10-19

- **G1 — Typed-claim key length** (spec §B is silent; Part C made `send` refuse a >16-byte key): resolved by BD1.
