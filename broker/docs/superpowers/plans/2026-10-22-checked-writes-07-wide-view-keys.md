# Checked Writes Part 07 — Wide View Keys, Point and Prefix Reads (Part D) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** View keys of up to 128 bytes — composite keys `key (a, b, …)` and key expressions `key lower(col) as name` — and composite `group by` keys for aggregates (R07c) — stored in a `ViewTable` whose index slot stays 13 bytes, with a seeded hash, an owner map for views keyed on anything but the source wire key, a prefix index over the first n−1 key columns, checkpoint v3, the full plan v2 format (C14), the codec's pinned Unicode simple-lowercase table, `GetViewRowK` (81) and `ScanViewRows` (82/83), the client read API with a typed key builder, and the key index widened to 16-byte keys (SD8).

**Architecture:** The codec gains three shared pieces that the broker fold, the client key builder and (Part 08) QL literals all call: `UnicodeLower.h` (a generated two-stage table pinned to Unicode 16.0.0), `ViewKeyEncoding.h` (the order-preserving key encoding of §7.3), and `ViewPlanAuthored.h` (the authored plan, moved from the broker and extended to v2). The compiler resolves a plan's key components and classifies it as `LegacyU64` (one integer column that is the source key: today's `uint64_t` table, v1/v2 checkpoints, byte-identical) or `Keyed` (everything else: `ViewKey` tables, an owner map when the key is not the source key, a prefix index when the key has two or more columns, checkpoint v3). Reads reuse the existing GetViewRow pipeline end to end (`viewRowGate_`, READ-on-VIEW, leader-only serving, the read lease, the fold barrier, restart readiness), extended with a `minOffset` barrier and a one-step page scan.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark (`broker/benchmark/ViewTableBench.cpp`, `ViewFoldBench.cpp`, `GetViewRowServeBench.cpp`), CMake/Ninja, SBE 1.40.2 for the internal ring, real Aeron, in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`).

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md` — §7 (Part D), §9 (Reads), §8.1 (the `lower()` mechanics, SD7), §14 (Table and Fold budgets), §15 (the cells owned by 07). Contract: `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md` (C9.1, C14, C15 reads, C16 ids 33/34, SD7, SD8, index row 07). Committed predecessors: plans 01 (checkpoint marker, AD5/AD5a), 02 (`RecordSpan::key`, `TopicWriteSpec`, `Utf8.h`, the view-source refusals), 03 (versions, codes 56–58, `OpStatus::BrokerTooOld`), 04 (`ViewKey.h` with `fromU64` / `asU64`, R07d), 05 (the key index) and 06 (`rune::protocol::partitionFor`). Parts 04, 05 and 06 are used only through the overview contract (C9–C13) and their contract additions. Dependency order (R-ORD): 05 → 06 → 07. Executors read the spec sections, the overview and this file.

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-07-wide-view-keys`, cut from the latest `main` once Part **06** has merged (`git fetch && git switch -c feature/checked-writes-07-wide-view-keys origin/main`). 07 depends on 06 (R-ORD); the only 06 name this plan uses is `rune::protocol::partitionFor` (C13). Merge `main` in again before review.
- **Preconditions** (each must hit before Task 1; a miss means a predecessor shipped differently — stop and tell the controller):
  - `grep -n "struct ViewKey" broker/libs/view/include/rune/view/ViewKey.h` (04, C9.1)
  - `grep -n "prepareProjection" broker/libs/view/include/rune/view/ViewFold.h` (04, C9.3)
  - `grep -n "kViewCheckpointMagicV2Marked = 0x534B4332u" broker/libs/view/include/rune/view/ViewCheckpoint.h` (01, AD5)
  - `grep -n "ViewUniqueInvalid *= 58" protocol/include/rune/protocol/client/Protocol.h` (03)
  - `grep -n "BrokerTooOld" client-cpp/include/rune/client/Completion.h` (03)
  - `grep -rn "key.index needs a key of at most 8 bytes" broker/libs/node` (05, SD8; one hit)
  - `grep -n "VerdictViewKind::KeyIndex" broker/libs/node/src/VerdictSource.cpp` (05)
  - `grep -n "fromU64\|asU64" broker/libs/view/include/rune/view/ViewKey.h` (04, R07d; ≥ 2 hits)
  - `grep -n "partitionFor(" protocol/include/rune/protocol/PartitionHash.h` (06, C13; ≥ 1 hit)
- **Builds** (already configured; if one is missing, stop and tell the controller): `build/root` (Ninja, Debug, `BUILD_TESTING=ON`; `cmake --build build/root -j14 --target <t>`, `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`), `build/sub-codec` (`rune_codec_tests`, `rune_catalog_tests`, CatalogGolden), `build/sub-client-cpp`, `build/rel` (Release, `BUILD_BENCHMARKS=ON`, binary `build/rel/broker/rune_broker_benchmark`), `build/asan`, `build/tsan`, `build/client-tsan`.
- **Codec sources are listed three times.** A new `codec/src/*.cpp` goes into `codec/CMakeLists.txt` (`rune_codec`, `:6-15`), `broker/CMakeLists.txt` (its `rune_codec` copy, `:144-157`) and `client-cpp/CMakeLists.txt` (its `if(NOT TARGET rune_codec)` copy, `:194-207`).
- **SBE.** The internal-ring flyweights under `protocol/include/rune_protocol_internal_sbe/` are generated and committed. Regenerate with a local `sbe-all-1.40.2.jar` given through `$SBE_JAR`. Never commit the jar.
- **VM.** This part has no docker or VM e2e of its own (Part 05 owns `broker/docker/checked-writes-e2e.sh`). Its real e2e runs in `rune_client_broker_tests` on the default shard runtime (Task 17). Stage (c) of the regression script and borderline benches run on the shared VM (`ssh root@$RUNE_VM_HOST`, every command under `flock /root/vm.lock`, in your own directory), never on local Mac docker. Prepare the clean box once for benches and restore it afterwards (`broker/bench/BENCHMARKING.md`).
- **Commits.** `git add <paths>` only (never `.superpowers/`, build logs or `-A`). Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create`, `conan export` or `conan upload`. Nothing in this plan pushes.

## Global Constraints

Copied verbatim from the overview (Plan 00). Every task implicitly includes this section.

**Error codes** (`protocol/include/rune/protocol/client/Protocol.h:105`, `ErrorCode`, append-only):
- `ConstraintViolation = 51`: the record broke a rule when it was folded. Constraint id `viewObjectId << 16 | ordinal`, ordinal 0 = `unique`. Fold, per record, definite.
- `KeyExists = 52`: `insert` on a key that exists. Fold, per record, definite.
- `KeyNotFound = 53`: `update` or delete-existing on a key that's absent. Fold, per record, definite.
- `AutoIncrementExhausted = 54`: the counter has reached `autoincrement.max`. The batch isn't stored. Write, definite.
- `TxnTooLarge = 55`: a transaction wrote more than `txn.judged.records.max` records to one voting partition. The transaction aborts. Vote, definite.
- `ViewKeyExpressionInvalid = 56`, `ViewConstraintInvalid = 57`, `ViewUniqueInvalid = 58`: compile errors (ruling SD3).
- Reused codes: `NotLeader` (2, retriable) while the autoincrement reservation is being found; `SchemaValidationFailed` (24) for validation; `InvalidRequest` (39) for forbidden writes; `NotAuthorized` (45) for an explicit id without `IMPORT`.
- "Definite" for 51–53 means *never visible*. 51–53 reach only `View` writers on client version 12.

**Versions.**
- `kClientVersionCheckedWrites = 12` and `kClientVersion = 12`.
- `kBrokerVersionCheckedWrites = 7` and `kBrokerVersion = 7`. Today `kClientVersion` is 11 and `kBrokerVersion` is 6 (`Protocol.h:327`, `Protocol.h:360`).
- Feature level `MetadataFeature::kCheckedWrites = 4` and `kLatest = kCheckedWrites` (`broker/libs/replication/include/rune/replication/ReplicationMessages.h:470-472`; today `kLatest = kBoundProducerIds = 3`).
- Plan v2, checkpoint v3 and catalog `formatVer` 2 follow the existing frozen-format rules: old formats still decode.

**Values.**
- `verdict.interval` = **100 ms**.
- `txn.judged.records.max` = **4096**.
- `autoincrement.reserve` = **4096**.
- Id cap **2^53 − 1** (`autoincrement.max` may be lower).
- View key width K ≤ **128 bytes**.
- Topic key field ≤ `Key::kMaxInline` = **16 bytes** (`client-cpp/include/rune/client/Key.h:35`).
- The `autoincrement` side window holds **32** batches = `kIdempotentMaxInFlightBatches` (`Protocol.h:16`).
- Partition `p` assigns `n·P + p` for n = 1, 2, …. Id 0 is never assigned.
- Index slot **13 bytes** (`tag u8 | word u64 | rid u32`). Plan key functions: 0 none, 1 `lower`, 2 reserved `casefold`.
- `lower(string(N))` width = the largest lowered width at the pinned Unicode version: ⌊3N/2⌋ at Unicode 16.0.0, packed 0x00100000 (R07a).
- Wire `acks` byte: bit 0 = `All`, bit 1 = `View`. Other bits are refused with `InvalidRequest`.
- Intent bits: record `attributes` bits 0–1. 0 = upsert/delete, 1 = insert, 2 = update/delete-existing. 3 and the other bits are refused with `InvalidRequest`.
- `partitions` defaults to 1. Strings are UTF-8, and `lower()` is Unicode simple lowercasing.

**Budgets** (hard gates, spec §14):
- Write hook: autoincrement assignment, including the scratch copy, < **1 µs p50** per batch of up to 4 KiB (`ProduceValidateBench`).
- New typed-topic validation: ≤ **1 µs p50** for a one-record batch.
- Judging: ≤ **1 µs per record** for one unique view plus one count check (`ViewFoldBench`).
- `ViewFoldBench` with derived keys plus the owner map: ≤ **10 µs** per event batch.
- No ABAB n=3 regression on `ProducePathBenchmark` and `WorkerProduceLatencyBench` for untyped topics and for already-validated typed topics.
- `ViewTableBench`: no change for K ≤ 8.
- Repo latency gates stay: view update < 10 µs, point read < 50 µs, QL ≤ 2–5 µs.

**Repo rules** (root `CLAUDE.md` §1–§4):
- **Hot path.** The produce, consume, fold and query paths are zero-alloc, take no lock, use no atomics as coordination and no `thread_local`, make no virtual call and use no `std::function`. Hot paths use concrete types plus a branch, or templates. Control-plane work (installs, DDL, recovery scans) runs as cold worker ops and may allocate.
- **States.** Rich-state variants, never scattered booleans.
- **Clocks.** One injected clock. Tests advance it and call `doWork`, with a hard iteration cap and never a wall-clock wait.
- **Regression tests.** Every fix ships a `*Regression*` test. Each task runs the affected-area tests.
- **Checkpoints** run the full `-R Regression -LE latency` suite, plus ASan+UBSan and TSan for the broker and the client, plus CatalogGolden.
- **Interaction matrix.** Every feature is tested across its matrix (§ Shared test infrastructure). Each plan has an "Interaction matrix" section and a "Review Focus" section, with 1–3 phases of small tasks.
- **Real e2e.** Nothing is done until its real e2e path is green on the **default** config (shard worker mode).
- **No weakened tests.** A weakened assertion is recorded as a deviation. Urgent ones are fixed now and the rest are filed as GitHub issues with type, severity and area labels.

## Rulings this part depends on (restated, binding)

- **SD7.** Derived keys and QL literals need the lowering table before Part E, so **Part D owns the codec `lower` and the Unicode pin** (§8.1 mechanics). Part E owns §8.2, §8.3 and the §8.7 wiring.
- **SD8.** Part B built the key index for wire keys ≤ 8 bytes and refuses `key.index` on a wider key with `InvalidRequest`, "key.index needs a key of at most 8 bytes". **Lifting it is this part's deliverable** (Task 16).
- **SD3.** Compile errors get **56, 57, 58**. Part 08 removes the message-search mapping (`AdminWorker.cpp:1241-1244`); this part's new errors never go through it (P7-14).
- **SD11.** The client gates `GetViewRowK` and `ScanViewRows` on `ConnectResponse.brokerVersion >= kBrokerVersionCheckedWrites`; below that it fails locally with `OpStatus::BrokerTooOld`.
- **SD13 / C16.** This part keeps `scanRows` (stat id 33) and `scanPages` (34) in `ViewFoldCounters`; Part 08 serves them.
- **C9.1 / R07d (reworded).** `ViewKey` (Part 04's `ViewKey.h`) is `{ std::array<uint8_t, kMaxViewKeyBytes> bytes; uint8_t len; fromU64; asU64; span }`. Part 04 defines `fromU64` and `asU64`, and Parts 05 and 06 already use them. This part owns and extends `ViewKey` (`fromBytes`, `operator==`, wide keys) and never renames those two or changes their meaning. **This part also adds `ViewTable::put/get/erase(const ViewKey&)` and the > 8 byte storage.**
- **R07a.** A lowered `string(N)` component is ⌊3N/2⌋ bytes wide at Unicode 16.0.0 (packed 0x00100000), the table-computed maximum (P7-2).
- **R05c (amended).** The key index is never checkpointed; each leadership rebuilds it from the local log (Task 16).
- **R10b.** Every view over a keyless typed topic is refused with `ViewKeyInvalid` (35), "view source needs a key", by Part 02's view-source check, which runs before the compile (Task 12).
- **R-X3.** Checkpoint v3 accepts any table id below the record's table count (Part 09 adds tables 6 and 7). A test pins 8 tables (Task 7).
- **C14.** This part owns the format of every plan v2 field; 08 authors it, 09 evaluates constraints. The authored plan moves to `codec/include/rune/codec/ViewPlanAuthored.h` + `codec/src/ViewPlanAuthored.cpp`, namespace `rune::view`.
- **R01 (replaces SD15; plan 01 AD5a).** Fixed code writes marked checkpoint magics `0x534B4331` / `0x534B4332`. **Checkpoint v3 must not use `0x534B433x`, and every v3 record counts as marked** (`checkpointMarkOf` returns `CheckpointMark::FoldSkipped` for v3).
- **R02a.** `RecordSpan::key` and the bounded `forEachRecordSpan` are Part 02's; the fold reads keys only through them.

## Part rulings (P7; binding for executors, recorded so a reviewer can overturn any)

- **P7-1 — The Unicode pin.** The pinned version is **Unicode 16.0.0**, packed `kUnicodeVersion = (16 << 16) | (0 << 8) | 0 = 0x00100000`.
  - The source is the UCD 16.0.0 release file `UnicodeData.txt` (the Unicode Consortium's public `Public/16.0.0/ucd/UnicodeData.txt`), committed as `codec/data/UnicodeData-16.0.0.txt`: 2,175,362 bytes, 40,116 lines, SHA-256 `ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f`. Its license goes beside it as `codec/data/LICENSE-UNICODE.txt` (the Unicode License v3 text from the same release).
  - Facts of that file that the tests pin (computed from it while writing this plan): **1,460** code points carry a `Simple_Lowercase_Mapping` (field 13): 26 ASCII (A–Z) and **1,434** non-ASCII. By UTF-8 length (source → target): 1→1 ×26, 2→1 ×1 (U+0130 İ → U+0069), 2→2 ×440, **2→3 ×2** (U+023A Ⱥ → U+2C65 ⱥ, U+023E Ⱦ → U+2C66 ⱦ), 3→1 ×1 (U+212A KELVIN SIGN → U+006B), 3→2 ×23 (among them U+1E9E ẞ → U+00DF ß, U+2126 Ω → U+03C9), 3→3 ×685, 4→4 ×282. No mapping target has a mapping of its own. The highest mapped source is U+1E921.
  - The table is **generated** from that file by `codec/tools/gen_unicode_lower.cpp` and the output `codec/src/UnicodeLowerTable.inc` is **committed**, like the SBE flyweights. The spec says "generated at build time"; generating inside three separate CMake trees (codec, broker copy, client copy) would ship the 2 MB data file with every client build. The table is still machine-made, never hand-written, and a cross-check test re-derives every code point from the data file (Task 1). *Flagged to the controller.*
- **P7-2 — The lowered width.** `maxLoweredWidth(N)` is computed from the table, as the spec asks ("the builder recomputes it from the table"): a knapsack over the largest lowered UTF-8 length per source length (`{1: 1, 2: 3, 3: 3, 4: 4}` at 16.0.0). At 16.0.0 it equals **⌊3N/2⌋**: 1→1, 2→3, 3→4, 4→6, 5→7, 16→**24**, 64→96, 85→127, 128→192. The spec's "⌈3N/2⌉" is the same for even N and one byte larger for odd N; the table value is authoritative and smaller, never too small. Accepted by R07a.
- **P7-3 — Two key modes.** `rune::view::ViewKeyMode keyModeOf(const CompiledViewPlan&) noexcept`:
  - **`LegacyU64`**: an aggregate view grouped by exactly one plain `Int64`/`UInt64` column (today's fast path, R07c), or a projection whose key is exactly one plain component on the **source key field**, of an integer type (`Int32`, `Int64`, `UInt16`, `UInt32`, `UInt64`). The table is today's `uint64_t` table (the value `decodeWireKey` produces, `ViewFold.cpp:73-110`), checkpoints stay v1 (projection) / v2 (aggregate), byte for byte. Every view that exists today is `LegacyU64`, and Part 04's three-way byte-identical fold test keeps covering these aggregates unchanged.
  - **`Keyed`**: everything else — composite keys, `lower()`, a `string(N)` source key, a single column that is not the source key, and every aggregate whose group key is not one 8-byte integer (a composite group, an enum, a narrow integer, `string(N)` or `lower(string(N))`, P7-8). `ViewKey` tables of width K = Σ component widths, checkpoint v3.
  - `[[nodiscard]] bool ownerMappedOf(const CompiledViewPlan&) noexcept`: `Keyed`, a projection, and not "exactly one plain component on the source key field" (aggregates never have one: their contributors table already maps the source key to its group). Only these views have an owner map.
  - `[[nodiscard]] uint16_t prefixWidthOf(const CompiledViewPlan&) noexcept`: the encoded width of the first n−1 components when n ≥ 2, else 0. Only these views have a prefix index.
- **P7-4 — The seeded hash.** Wide keys (K > 8) and prefixes hash with the seeded wyhash v4 the repo already ships (`rune::storage::detail::wyhash64`, `SkimpyOffsetMap.h:88-123`), moved unchanged into `broker/libs/storage/include/rune/storage/WyHash.h` (`SkimpyOffsetMap.h` includes it; `SkimpyOffsetMapTest` pins it). Narrow tables keep today's exact-key index and its `mix()`, now fed `key ^ seed` (one XOR), so player-chosen narrow keys cannot target a home slot either. Seed 0 reproduces today's tables bit for bit (tests, benches, the `ViewTableBench` K ≤ 8 gate). Every `WorkerViewSet` draws its own seed from `std::random_device` at construction (cold) unless `NodeConfig::viewHashSeedForTest != 0`. The seed never leaves the process. A test hook `ViewTableKeys::hashMaskForTest` (AND-ed into every hash; 0 = every key collides) forces collisions (§15 Reads, "a forced hash collision").
- **P7-5 — Row layout (spec §18, the builder's choice).** A `Keyed` table row in the arena is `[header][payload]`. The header holds the K key bytes when K > 8 or the table has a prefix index, then `[prev u32][next u32]` when it has a prefix index. The payload is exactly the view's `RowLayout` row, so every reader (`ViewRowResponse`, scans, checkpoints) sees the same bytes as today. `put`/`get` return the payload pointer. The header key is the identity; the payload's key columns are written from the same decoded values in the same `applyProjection` call (a test pins that they agree, Task 9).
- **P7-6 — The owner map** is a `ViewTable` keyed by the source **wire key** (width = the topic's `keyWidth`, ≤ 16) whose payload is the K-byte view key (not a rid: rids are not stable across a replay). It is checkpointed with the view rows in the same v3 record (table 1), so a restart restores it exactly. The prefix index is rebuilt by the replay's `put`s. Both choices are the builder's (§18).
- **P7-7 — Plan versions.** `kAuthoredVersion` and `kCompiledVersion` become 2. The encoders write the **lowest version that can carry the plan**: v1 bytes, unchanged, whenever no v2 field is used (`needsV2`, Task 3/4). So an existing-shaped plan keeps its exact normalized bytes and catalog fingerprint, and a mixed-version cluster never sees a v2 blob it cannot decode. A compiled plan's `viewObjectId` alone does not force v2 (a v1 compiled blob decodes it as 0; only constrained views, which are v2, need it). Both decoders become strict: trailing bytes are refused, as the client's decoder already does (`ViewPlanAuthor.h`, "TRAILING bytes").
- **P7-8 — Composite group keys (ruling R07c).** An aggregate's group key is the view's key spec, encoded as a `ViewKey` with the same §7.3 encoding as `key (a, b)` (the RegressorsPath `device_history` view is `key (userId, deviceFp) … group by userId, deviceFp`).
  - **The rule.** The authored plan carries `groupBy` (v2, a list of `KeyComponent`s). An aggregate with a key spec must have `groupBy` equal to it, component by component (`column` and `keyFunction`; `outName` is the key's); otherwise `ViewKeyInvalid` (35), "group by (<list>) must equal the key (<list>)". A v1-shaped aggregate (no key spec, no `groupBy`) keeps today's meaning: the group-by field is `sources[0].keyField`.
  - **Columns.** Any key-eligible type: integers, `Enum` (4 bytes, unsigned big-endian), `Bool`, `string(N)`, `lower(string(N))`. The source key must be an integer of at most 8 bytes (contributors are keyed by it, `AggState.h:3-4`); otherwise `ViewKeyExpressionInvalid` (56), "aggregate views need an integer source key of at most 8 bytes".
  - **Fast path kept.** One plain `Int64`/`UInt64` group column is `LegacyU64`: today's code, layout and v2 checkpoint, byte for byte. Every other aggregate is `Keyed`.
  - **How a keyed aggregate works.** The aggregate machinery (`AggFold.cpp`: contributor links, accumulators, Min/Max value blocks, `blockIdCandidate`) keeps its `uint64_t` group key, which becomes a dense **group id** interned per instance: a `groupIds` table (`ViewKey` K bytes → `[gid u64]`), and the group row appends the group's K key bytes, so `emit` and `dropGroup` reach the view row (keyed by the `ViewKey`, with a prefix index when n ≥ 2). Group ids come from a per-instance counter, are never reused within an incarnation, and are internal: two folds of the same log may number groups differently but always produce identical view rows. A group's id and its `groupIds` entry are created in apply (never in prepare) and dropped with the group.
  - **Prefix reads** on a composite group-by view (`scanViewAndWait(device_history, prefix(userId))`) are served by the same `scanPage` as projections.
- **P7-9 — `unique` and constraints before Part 09.** The format and the compile are complete here (constraints compile structurally: names resolve to out-slots, the tree is well formed). The AdminWorker refuses `unique` with `ViewUniqueInvalid`, "unique views are not available yet", and any constraint with `ViewConstraintInvalid`, "check constraints are not available yet"; Part 09 lifts both. At the engine level (tests that install plans directly), a record whose view key is already owned by a different source key is not applied and is counted in `FoldStats::keyConflicts`; Part 09 replaces this with the §8.3 rule.
- **P7-10 — Read routing.**
  - `GetViewRow` (72) keeps serving `LegacyU64` views with K = 8 (every view that exists today). On any other view the worker answers `ViewRowResponse::kInvalidRequest` (5), never a wrong row: the gateway's 8-byte canonicalization (`ClientGateway.cpp:9293-9300`) cannot route narrow keys (the "Bug B" note, `ViewTypes.h:32-50`).
  - `GetViewRowK` (81) carries a `partition` field. On a sink with one partition it must be −1 or 0 (served from 0). On a sink with more partitions (possible only for a source-keyed `LegacyU64` or `string(N)` view, P7-3 and §7.4) it must be in `[0, P)`, and the client computes it with the producer's own routing (`ViewKeyBuilder::partition()`), so the read and the write that made the row route identically. −1 there is `kInvalidRequest`. The gateway never needs the key's type. *Flagged to the controller as a wire extension of §9.*
  - The view-key encoding (§7.3, big-endian) is never hashed for routing.
- **P7-11 — Scan pages.** At most `ScanViewRows::kMaxRowsPerPage = 256` rows and `ScanViewRowsResponse::kMaxPageBytes = 61,440` bytes of rows per page (`limit` 0 or above 256 means 256). One page is served in one worker step. The resume token is the K-byte key of the last row served; an empty next token means the scan is complete. A token whose key is gone (or no longer has the requested prefix) answers `kCursorInvalidated` (8). New rows are linked at the head of their prefix list, so a page boundary never skips a row that stayed in place; a row erased and re-inserted between pages may be missed or repeated (no snapshot, §9). A single row larger than the page budget answers `kRowTooLarge` (7).
- **P7-12 — `minOffset`.** Both reads take `minOffset` (−1 = none). The worker's fold-barrier target becomes `max(target, minOffset + 1)`, taken at arrival and checked on every serve (also when the table already folded to the HW), with the existing hold limit (`kHeldViewReadMaxNs` = 100 ms) and `NotReady` on expiry.
- **P7-13 — A plan pinned to an unknown Unicode version.** The AdminWorker refuses it at DDL time (`ViewKeyExpressionInvalid`). A worker that receives one anyway (an upgrade, a hand-made catalog) does not install it: `spdlog::critical("VIEW-UNICODE-UNSUPPORTED view={} unicode={}.{}.{} — this broker has 16.0.0; the view is not installed")`, `WorkerViewSet::installRefusals()` + 1; its reads answer `kUnknownView` (the existing no-template path).
- **P7-14 — Compile error kinds.** `CompileError` appends `CompileErrorKind kind = CompileErrorKind::Generic`. The AdminWorker maps a non-Generic kind straight to its code and only Generic errors reach the old message search (which Part 08 removes): `FieldUnresolved` → 34, `KeyInvalid` → 35, `NotCoPartitioned` → 36, `KeyExpressionInvalid` → 56, `ConstraintInvalid` → 57, `UniqueInvalid` → 58.
- **P7-15 — Derived key columns.** A `lower()` component's output column is a `FixedStr` of the component width, and its `CompiledField` carries `srcFieldId = kDerivedKeyField (0xFFFE)`: the fold writes it from the encoded key bytes, never from the raw source field. Implicit key columns (plain components not in the select list) become ordinary `Replace` output columns. Output order: the key columns in component order, then the remaining select items in authored order (today's "key first" generalized).
- **P7-16 — A record without a key.** If a key component is absent in the writer's version (a field added after the record was written), the record cannot be placed: on an owner-mapped view the source key's current row is erased (the record replaced it), nothing is inserted, and `FoldStats::skippedNoKey` counts it — the same shape as aggregates' `skippedNoGroup`.
- **P7-17 — Numbers.** `WorkerOp::Type::GetViewRowK = 29` (SBE `GET_VIEW_ROW_K = 29`), `WorkerOp::Type::ScanViewRows = 30` (`SCAN_VIEW_ROWS = 30`), `TailKind VIEW_READ = 13`, `WorkerReturn::Type::ViewScanResult = 9` (`ReturnType VIEW_SCAN_RESULT = 9`), `ViewRowResponse::kCursorInvalidated = 8`. Checked against the code and the other plans: op types 26/27 are 02's, 28 is 11's; `TailKind` 12 is 02's; `ReturnType` 8 is 11's; statuses stop at 7 (+45).

## Contract additions (this part adds them; the overview stays the authority for everything else)

1. `broker/libs/view/include/rune/view/ViewKey.h` (R07d, reworded): 07 appends `static ViewKey fromBytes(std::span<const uint8_t>) noexcept` (precondition `size ≤ 128`) and `bool operator==(const ViewKey&) const noexcept` (compares `len` and the first `len` bytes). `fromU64(uint64_t)` (`len = 8`, the little-endian bytes of `v`) and `asU64()` (precondition `len ≤ 8`; the little-endian load of the first `len` bytes, zero-extended; `fromU64(v).asU64() == v`) are Part 04's: 07 verifies they exist as specified and does not redefine them. No other conversion name exists (no `packed()`). `rune::view::kMaxViewKeyBytes` (04, `std::size_t`) becomes an alias of the codec's value, `inline constexpr std::size_t kMaxViewKeyBytes = rune::codec::kMaxViewKeyBytes;`, so one constant (128) exists (Task 5).
2. `broker/libs/view/include/rune/view/ViewTable.h`: `struct ViewTableKeys { uint8_t keyWidth = 8; uint8_t prefixWidth = 0; uint64_t hashSeed = 0; uint64_t hashMaskForTest = ~uint64_t{0}; };`, a trailing `ViewTableKeys keys = {}` on the constructor, `put/get/erase(const ViewKey&)`, `drainDirtyKeyed`, `scanPrefix`, `enum class PrefixScan : uint8_t { End, Stopped, CursorGone }`, `keyOfRow`, `keyWidth()`, `prefixWidth()`.
3. `broker/libs/view/include/rune/view/ViewPlan.h`: `CompiledKeyComponent`, `CompiledPredicateNode`, `CompiledConstraint`; `CompiledViewPlan` appends `keyComponents`, `keyWidth`, `unique`, `constraints`, `unicodeVersion`, `viewObjectId`; `ViewKeyMode`, `keyModeOf`, `ownerMappedOf`, `prefixWidthOf`, `needsV2(const CompiledViewPlan&)`; an aggregate's group key is its `keyComponents` (no separate compiled group-by list). `ViewTypes.h`: `kDerivedKeyField = 0xFFFE`. `PlanCompiler.h`: `CompileErrorKind`, `CompileError::kind`.
4. `ViewCheckpoint.h`: `kViewCheckpointMagicV3 = 0x564B4333u` ("3CKV"), `KeyedDirtyEntry`, `KeyedTableDelta`, `encodeKeyedDelta`, `applyKeyedDelta`, `replayKeyed`, `kCheckpointV3HeaderBytes = 13`, `kCheckpointV3TableHeaderBytes = 10`, table ids `kKeyedTableView = 0`, `kKeyedTableOwners = 1`, `kKeyedTableContributors = 2`, `kKeyedTableGroups = 3`, `kKeyedTableBlocks = 4`, `kKeyedTableGroupIds = 5`. `checkpointMarkOf` (01) returns `FoldSkipped` for v3.
5. `FoldStats` appends `skippedNoKey`, `keyConflicts`, `ownerMoves`; `ViewFoldCounters` appends the same three plus `scanRows`, `scanPages` (C16 ids 33, 34). `ProjectionFold` (04, C9.3) appends `const KeyedFoldSpec* keyed = nullptr; ViewTable* owners = nullptr;`. `struct KeyedFoldSpec` + `buildKeyedFoldSpec` + `deriveViewKey` in `broker/libs/view/include/rune/view/KeyedFold.h`.
6. Codec: `codec/include/rune/codec/UnicodeLower.h`, `codec/include/rune/codec/ViewKeyEncoding.h`, `codec/include/rune/codec/ViewPlanAuthored.h` (with `KeyComponent`, `AuthoredConstraint`, `PredicateNode`, `PredicateOp`, `ViewPlan::groupBy`, `needsV2(const ViewPlan&)`, `keyDisplayName`), `decodeKeyComponent` and `Enum` as a key type in `ViewKeyEncoding.h`, exact signatures in Tasks 1–3.
6a. Keyed aggregates (Task 11): `AggLayout` appends `bool keyed = false; uint32_t groupKeyOff = 0; uint16_t groupKeyWidth = 0;`; `AggInstance` appends `std::optional<rune::view::ViewTable> groupIds;` and `uint64_t nextGroupId = 1;`; `AggregateFold` (04, C9.3) appends `const KeyedFoldSpec* keyed = nullptr; ViewTable* groupIds = nullptr; uint64_t* nextGroupId = nullptr;`; checkpoint v3 table ids `kKeyedTableContributors = 2`, `kKeyedTableGroups = 3`, `kKeyedTableBlocks = 4`, `kKeyedTableGroupIds = 5`.
7. Wire: `GetViewRowK` (81), `ScanViewRows` (82), `ScanViewRowsResponse` (83) in `protocol/include/rune/protocol/client/DataMessages.h`; `ViewRowResponse::kCursorInvalidated = 8`; `ScanViewRows::kMaxRowsPerPage = 256`, `ScanViewRowsResponse::kMaxPageBytes = 61440`. Internal ring: P7-17. `WorkerOp` appends `std::span<const uint8_t> viewReadTail;` (non-owning, read only by `enqueueOp` in the same call).
8. `WorkerViewSet`: constructor `WorkerViewSet(ViewSnapshotChannel&, std::string viewDataDir, uint64_t hashSeed = 0)`, `getRowK`, `scanPage`, `RowStatus::BadKey` (appended), `installRefusals()`, `hashSeedForTest()`, `setHashMaskForTest(uint64_t)`. `NodeConfig::viewHashSeedForTest` (`uint64_t`, 0 = random). Test seams on `ClientGateway`: `applyGetViewRowKForTest`, `applyScanViewRowsForTest`.
9. Client: `OpKind::GetViewRowK`, `OpKind::ScanViewRows` (after `Seek`, `Completion.h:51`); `Client::getViewRowK`, `Client::scanView`, `getViewRowAndWait(int32_t, std::span<const uint8_t>, ViewReadOptions, timeout)`, `scanViewAndWait`, `ViewReadOptions`, `ViewScanOptions`, `ViewScanResult`, `ViewRowResult::kCursorInvalidated = 8`; `client-cpp/include/rune/client/view/ViewKeyBuilder.h`; `decodeScanPayload` in `ViewRowCodec.h`.
10. Matrix ids: `Op` 60–68 and `Entity` 60–68 (the overview reserves 60–79).

## Review Focus

These are the five inputs the spec implies but no requirement line names, most likely to bite first. Each is pinned by a test in the owning task.

1. **A rename inside one batch that renames back** (`Foo` → `Bar` → `Foo` for the same source key), and a rename to the key the record already owns. The owner map and the table must end with exactly one row, under the last name, and no orphaned row. Tests: `CheckedWritesKeyedFoldRegression.RenameAndBackInOneBatchLeavesOneRowRegression`, `…RenameToTheOwnKeyIsAnUpdateNotAMoveRegression` (Task 9).
2. **The old `GetViewRow` (72) aimed at a new view** (composite, `lower()`, or a narrow single key), and a `GetViewRowK` with a key of the wrong length or without a partition on a multi-partition view. The answer is `kInvalidRequest`, never a wrong row or a wrong `AbsentKey`. Tests: `CheckedWritesViewReadRegression.GetViewRowOnAKeyedViewIsInvalidRequestRegression`, `…WrongKeyLengthIsInvalidRequestRegression`, `CheckedWritesViewReadGatewayRegression.MultiPartitionSinkNeedsThePartitionRegression` (Task 14).
3. **A `string(N)` name at its byte limit that grows when lowered** (sixteen bytes of `Ⱥ`, fifteen bytes ending in a 2-byte character, a value with bytes after its first NUL). The lowered component never writes past its width, two inputs that differ only after a NUL map to the same key (a false reject, never a false accept), and every pinned width holds. Tests: `CheckedWritesUnicodeLower.SixteenBytesOfGrowersFillTwentyFourExactly`, `…StopsAtTheFirstNul`, `CheckedWritesViewKeyEncodingRegression.LoweredComponentNeverExceedsItsWidthRegression` (Tasks 1, 2).
4. **A crash right after a rename was checkpointed** (erase of the old row, insert of the new one and the owner update in one fold step). Recovery must give one row under the new key and an owner map pointing at it, equal to a fresh refold. Test: `CheckedWritesKeyedRecoveryRegression.RenameJustBeforeRestartRecoversOneRowRegression` (Task 10).
5. **A scan whose resume token row was erased and re-inserted, or whose prefix list emptied, between pages.** The next page must terminate (no loop), answer `kCursorInvalidated` only when the token key is truly gone, and never serve a row from another prefix. Tests: `CheckedWritesViewTablePrefixRegression.ReinsertedTokenRowResumesWithoutLoopingRegression`, `…EmptiedPrefixListAnswersCursorGoneRegression` (Task 6), `CheckedWritesViewReadRegression.ScanNeverCrossesIntoAnotherPrefixRegression` (Task 14).

---

## Interaction matrix

This part owns the overview's §15 cells listed for **07**: every "Reads" bullet except the four owned by 02 and 09 (point and prefix read right after an acked write; a scan across pages with an erase between them, `kCursorInvalidated`; a scan of an empty prefix; a forced hash collision using a fixed seed through a test hook), every `lower()` bullet (SD7), and "rename with an owner-map move". The pre-existing-duplicates bullet is Part 09's; `partitions` omitted, binding and the 16-byte key refusal are Part 02's.

### A. The shared matrix (`rune_checked_writes_matrix_tests`, `expect07`, Task 17)

Rows are added to Part 04's `CheckedWritesMatrixExpectations.h` with this part's reserved ids. Every cell drives the real worker through `CheckedWritesRig.h` (`createView(planV2)` is added by this part) and asserts:
- (a) the view table equals `CheckedWritesModel::refoldKeyedView(plan, log)` row for row (key bytes and payload bytes);
- (b) on owner-mapped views, the owner map equals the model's `{wire key → view key}`;
- (c) a point read (`GetViewRowK` through the worker) of every model key returns the model row, and a full scan (all pages, `limit 2`) of every model prefix returns exactly the model's rows for it, as a set;
- (d) after the timing's failover or restart, (a)–(c) hold on the serving leader.

| Axis | Values (ids) |
|---|---|
| `Op` | `ViewUpsertNew` 60, `ViewUpdateNonKey` 61, `ViewRename` 62 (changes the derived/leading key), `ViewMoveGroup` 63 (changes the leading composite column), `ViewDelete` 64, `ViewDeleteThenReuse` 65 (delete, then a new source key takes the same view key), `ViewDuplicateRetry` 66, `ViewPointRead` 67, `ViewPrefixScan` 68 |
| `Entity` (the key shape) | `KeyU16` 60, `KeyU32` 61, `KeyU64` 62, `KeyI64` 63 (single-column source-keyed `LegacyU64`, one view per type); `KeyStr16` 64 (`string(16)` source key, `Keyed`, no owner map, K = 16); `KeyComposite` 65 (`(userId u64, characterId u64)`, K = 16, prefix 8); `KeyCompositeWide` 66 (`(provider u16, subject string(64), userId u64)`, K = 74, prefix 66); `KeyLower` 67 (`(lower(name string(16)) as nameKey, characterId u64)`, K = 32, prefix 24); `KeyMaxWidth` 68 (`(lower(tag string(80)) as tagKey, characterId u64)`, K = 120 + 8 = **128**, prefix 120) |
| `Timing` (the overview's fixed list) | writes: `Single`, `BackToBack`, `SelfCollidingBatch`, `FailoverHwFold`, `RestartDuringRebuild`, `CreateOnNonEmpty`, `ReplaceDuringWrites`, `DropDuringWrites`; `ViewDuplicateRetry`: `RetryBeforeJudge`, `RetryAfterJudge`, `FailoverHwFold` (on a source without verdict views "judge" means "fold"); reads: `Single`, `BackToBack`, `FailoverHwFold`, `RestartDuringRebuild`, `CreateOnNonEmpty` |

Not applicable (top-down): `ViewRename` only on `KeyCompositeWide`, `KeyLower`, `KeyMaxWidth`; `ViewMoveGroup` only on `KeyComposite`, `KeyCompositeWide`; `ViewPrefixScan` only on the four composite shapes; every other op on all nine. Count: write ops (4 × 9 + 3 + 2 = 41) × 8 = 328, `ViewDuplicateRetry` 9 × 3 = 27, `ViewPointRead` 9 × 5 = 45, `ViewPrefixScan` 4 × 5 = 20 → **420 cells**, in the projection block.

**The group-by block (R07c).** `CheckedWritesMatrix07Agg/Cell.GroupByViewEqualsTheModel/<n>` reuses the same ids: the `Entity` names the key shape, now as the group key — `KeyComposite` 65 = group `(userId u64, deviceId u64)`, `KeyLower` 67 = group `(lower(deviceFp string(16)) as fpKey, userId u64)` — over the `device` source of Task 4. A local parameter `rule ∈ {count(*), sum(bytes), min(lastSeen), max(lastSeen), last(platform)}` picks the one aggregate column under test (not an id). Ops: `ViewUpsertNew` (insert), `ViewUpdateNonKey` (update inside the group), `ViewMoveGroup` (update into another group), `ViewDelete` (tombstone), each × {`Single`, `BackToBack`, `RestartDuringRebuild`, `FailoverHwFold`}; and `ViewPrefixScan` × {`Single`, `RestartDuringRebuild`, `FailoverHwFold`}. Each cell asserts (a), (c) and (d) above against `CheckedWritesModel::refoldKeyedAggregate`. Count: 4 × 2 × 4 × 5 = 160 + 1 × 2 × 3 × 5 = 30 → **190 cells**.

The two blocks are **610 cells**, pinned in `CheckedWritesMatrixCountRegression.CellCountsArePinned` (`expect07` row: `EXPECT_EQ(applicable07, 610u)`, the group-by block 190 of them).

### B. The §15 bullets and where they are tested

| §15 bullet | Test (task) |
|---|---|
| a point read and a prefix read right after an acked write | `CheckedWritesBrokerRegression.CompositeViewPointAndPrefixReadsRightAfterTheAckShardRegression` (17), matrix `ViewPointRead`/`ViewPrefixScan` × `Single` |
| a scan across pages with an erase between them | `CheckedWritesViewReadRegression.ErasedTokenRowAnswersCursorInvalidatedRegression` (14), `CheckedWritesBrokerRegression.ScanAcrossPagesThenCursorInvalidatedShardRegression` (17) |
| a scan of an empty prefix | `CheckedWritesViewTablePrefix.EmptyPrefixScansNothing` (6), `CheckedWritesViewRead.EmptyPrefixPageIsOkWithNoRowsAndNoToken` (14) |
| a forced hash collision, fixed seed through a test hook | `CheckedWritesViewTableRegression.FullCollisionsStayCorrectAcrossGrowthRegression` (5), `CheckedWritesViewTablePrefixRegression.CollidingPrefixesNeverMixRegression` (6), `CheckedWritesViewSetRegression.FixedSeedHookReachesEveryTableRegression` (10) |
| `lower()`: reference table cross-checked against `UnicodeData.txt` | `CheckedWritesUnicodeLower.EveryCodePointMatchesUnicodeData16` (1) |
| `lower()`: growing characters at the maximum width | `CheckedWritesUnicodeLower.SixteenBytesOfGrowersFillTwentyFourExactly` (1), matrix `KeyMaxWidth` |
| `lower()`: shrinking characters (İ → i) | `CheckedWritesUnicodeLower.DottedCapitalIShrinksToAsciiI` (1) |
| `lower('Foo')` from the client key builder equals the broker's key | `CheckedWritesViewKeyBuilder.LowerMatchesTheBrokerFoldKey` (15), `CheckedWritesBrokerRegression.LowerKeyReadWithTheKeyBuilderShardRegression` (17). The QL leg calls the same `rune::codec::lowerUtf8` and is Part 08's test. |
| `lower()`: a plan pinned to an unknown Unicode version is refused | `CheckedWritesPlanCompile.UnknownUnicodeVersionIsRefused` (4), `CheckedWritesAdminViewDdl.UnknownUnicodeVersionIsViewKeyExpressionInvalid` (12), `CheckedWritesViewSetRegression.UnsupportedUnicodePlanIsNotInstalledRegression` (10) |
| rename with an owner-map move | `CheckedWritesKeyedFold.RenameMovesTheRowAndTheOwner` (9), matrix `ViewRename` |

### C. Model checks

- `CheckedWritesViewTableModelRegression.RandomOpsMatchAMapModel` (Task 5/6, `rune_view_tests`): seeds 1–200; K ∈ {2, 6, 8, 16, 66, 128}; prefix on/off; `hashMaskForTest` ∈ {~0, 0xFF, 0}; 20,000 random put/get/erase ops each across at least two index growths. After every op the table agrees with a `std::map<std::vector<uint8_t>, row>` model on `size()`, `get()` of a touched key and, every 1,000 ops, every key; every prefix's `scanPrefix` set equals the model's.
- `CheckedWritesKeyedFoldModelRegression.RandomLogsFoldToTheFreshRefold` (Task 9, `rune_view_tests`): seeds 1–300 over the five `Keyed` matrix shapes; random upserts, renames, moves, deletes and re-uses across 1–20 batches folded with random pass bounds; the table and owner map equal a fresh refold of the whole log into empty tables, and a v3 checkpoint replay of the dirty stream equals both.
- `CheckedWritesKeyedAggModelRegression.RandomLogsFoldToTheFreshRefold` (Task 11, `rune_view_tests`): seeds 1–300 over four group shapes and the eight aggregate rules, with inserts, group moves and tombstones; every view row equals a fresh refold, and a v3 replay equals both.
- **Stress.** The overview assigns no stress invariant to 07. `CheckedWritesModel.h` gains `refoldKeyedView`, so Part 04's `ViewEqualsFreshRefold` checks keyed views wherever the stress driver installs one (Part 09's scenario installs `character_name` and `account_characters`).

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `codec/data/UnicodeData-16.0.0.txt`, `codec/data/LICENSE-UNICODE.txt` | C | The pinned UCD source and its license (P7-1) |
| `codec/tools/gen_unicode_lower.cpp` | C | Generates `UnicodeLowerTable.inc` from the data file |
| `codec/src/UnicodeLowerTable.inc` | C (generated, committed) | The two-stage table and the per-length maxima |
| `codec/include/rune/codec/UnicodeLower.h`, `codec/src/UnicodeLower.cpp` | C | `kUnicodeVersion`, `lowerCodePoint`, `lowerUtf8`, `maxLoweredWidth` |
| `codec/include/rune/codec/ViewKeyEncoding.h` | C | §7.3 encoding of one key component (header-only) |
| `codec/include/rune/codec/ViewPlanAuthored.h`, `codec/src/ViewPlanAuthored.cpp` | C | The authored plan v1/v2 (moved from `broker/libs/view/src/ViewPlan.cpp:242-319`) |
| `codec/CMakeLists.txt`, `broker/CMakeLists.txt:144-157`, `client-cpp/CMakeLists.txt:194-207` | M | The two new codec sources, three times; the generator target and the test data path (codec only) |
| `codec/tests/CheckedWritesUnicodeLowerTest.cpp`, `CheckedWritesViewKeyEncodingTest.cpp`, `CheckedWritesViewPlanAuthoredTest.cpp` | C | Codec tests (`rune_codec_tests`, `build/sub-codec`) |
| `broker/libs/storage/include/rune/storage/WyHash.h` | C | `wyhash64`, moved unchanged from `SkimpyOffsetMap.h:54-125` |
| `broker/libs/view/include/rune/view/{ViewPlan.h,ViewTypes.h}`, `src/ViewPlan.cpp` | M | Compiled plan v2; includes the codec authored plan |
| `broker/libs/view/{include/rune/view/PlanCompiler.h,src/PlanCompiler.cpp}` | M | Key components, key modes, identity rule, constraints (structural) |
| `broker/libs/view/include/rune/view/ViewKey.h` (04's) | M | `fromBytes`, `operator==`, the `kMaxViewKeyBytes` alias (Task 5) |
| `broker/libs/view/include/rune/view/ViewKeyHash.h` | C | `viewKeyHash(span, seed)` over `WyHash.h` |
| `broker/libs/view/{include/rune/view/ViewTable.h,src/ViewTable.cpp}` | M | `ViewKey` overloads, wide slots, prefix index, keyed dirty drain |
| `broker/libs/view/{include/rune/view/ViewCheckpoint.h,src/ViewCheckpoint.cpp}` | M | Checkpoint v3 |
| `broker/libs/view/include/rune/view/KeyedFold.h`, `src/KeyedFold.cpp` | C | `KeyedFoldSpec`, `deriveViewKey` |
| `broker/libs/view/{include/rune/view/AggState.h,src/AggState.cpp,include/rune/view/AggFold.h,src/AggFold.cpp}` | M | Composite group keys: group ids, `groupIds`, keyed `emit`/`dropGroup` (R07c) |
| `broker/libs/view/{include/rune/view/ViewFold.h,src/ViewFold.cpp}` | M | `prepareProjection`/`applyProjection` keyed mode (04's functions) |
| `broker/libs/view/CMakeLists.txt` | M | New sources and tests |
| `broker/libs/view/tests/view/CheckedWrites{PlanV2,ViewTable,ViewTablePrefix,ViewCheckpointV3,KeyedFold,KeyedAgg}Test.cpp` | C | `rune_view_tests` |
| `broker/libs/node/{include/rune/node/WorkerViewSet.h,src/WorkerViewSet.cpp}`, `src/ViewSnapshotIoAgent.cpp:56-67` | M | Keyed instances, v3 checkpoints and recovery, `getRowK`, `scanPage`, seed, install refusal |
| `broker/libs/node/include/rune/node/{BrokerNode.h,NodeConfig.h}`, `src/BrokerNode.cpp:1937` | M | Per-worker seed, `viewHashSeedForTest` |
| `protocol/include/rune/protocol/client/{Protocol.h,DataMessages.h,Dispatch.h}`, `protocol/conformance/{gen_vectors.cpp,manifest.md,vectors/*}` | M | 81–83, status 8, vectors |
| `protocol/schema/rune-internal-ring.xml`, `protocol/include/rune_protocol_internal_sbe/*`, `broker/libs/node/include/rune/node/{WorkerOp.h,WorkerReturnCodec.h,Worker.h}` | M | Ops 29/30, tail 13, return 9, the serve path |
| `broker/libs/gateway/{include/rune/gateway/ClientGateway.h,src/ClientGateway.cpp,include/rune/gateway/RequestRejection.h}` | M | Handlers, routing, return encoding, rejections |
| `broker/libs/node/src/AdminWorker.cpp`, `include/rune/node/AdminWorker.h` | M | Plan v2 DDL gates, error kinds, `viewObjectId`, SD8 |
| `broker/libs/node/src/VerdictSource.cpp` (05) | M | The key index on `ViewKey` (SD8) |
| `broker/libs/node/tests/node/CheckedWrites{ViewSet,KeyedRecovery,ViewRead,ViewReadGateway,AdminViewDdl,KeyIndexWide}Test.cpp` | C | `rune_checked_writes_tests` |
| `broker/libs/node/tests/support/{CheckedWritesRig.h,CheckedWritesMatrixExpectations.h,CheckedWritesModel.h}`, `tests/node/CheckedWritesMatrixTest.cpp` | M | `createView(planV2)`, `expect07`, `refoldKeyedView` |
| `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp`, `tests/node/CheckedWritesAllocRegressionTest.cpp` | M | Real e2e, allocation gate |
| `client-cpp/include/rune/client/{Client.h,Completion.h,Completions.h}`, `src/Client.cpp`, `include/rune/client/view/{ViewPlanAuthor.h,ViewRowCodec.h,ViewKeyBuilder.h(C)}`, `src/view/ViewKeyBuilder.cpp` (C) | M/C | Read API, key builder, aliases |
| `client-cpp/tests/client/CheckedWritesViewKeyBuilderTest.cpp` | C | `rune_schema_codec_smoke_tests` |
| `broker/benchmark/{ViewTableBench.cpp,ViewFoldBench.cpp,GetViewRowServeBench.cpp}` | M | `BM_ViewTableWideKey`, `BM_ViewTablePrefixScan`, `BM_ViewFoldDerivedKey`, `BM_GetViewRowKServe`, `BM_ScanViewRowsServe` |
| `docs/api/{wire-protocol.md,client-cpp.md,codec.md}` | M | 81–83 and status 8; wide keys, the key builder, scans, `minOffset`; `lower`, key encoding, plan v2 |

---
## Phase 1 — The codec: `lower()`, the key encoding, the authored plan v2

### Task 1: The pinned Unicode simple-lowercase table and `lowerUtf8`

**Files:**
- Create: `codec/data/UnicodeData-16.0.0.txt` (the UCD 16.0.0 release file, byte for byte), `codec/data/LICENSE-UNICODE.txt`
- Create: `codec/tools/gen_unicode_lower.cpp`, `codec/src/UnicodeLowerTable.inc` (its output), `codec/include/rune/codec/UnicodeLower.h`, `codec/src/UnicodeLower.cpp`
- Modify: `codec/CMakeLists.txt` (`src/UnicodeLower.cpp` in `rune_codec` `:6-15`; `add_executable(gen_unicode_lower tools/gen_unicode_lower.cpp)` beside the other helpers `:44-75`, linking nothing; `tests/CheckedWritesUnicodeLowerTest.cpp` in `rune_codec_tests` `:197`; `target_compile_definitions(rune_codec_tests PRIVATE RUNE_UNICODE_DATA="${CMAKE_CURRENT_SOURCE_DIR}/data/UnicodeData-16.0.0.txt")`), `broker/CMakeLists.txt:144-157`, `client-cpp/CMakeLists.txt:194-207` (`UnicodeLower.cpp` in both copies)
- Test: `codec/tests/CheckedWritesUnicodeLowerTest.cpp`

**Interfaces:**
- Produces (`namespace rune::codec`, header `UnicodeLower.h`):
  ```cpp
  inline constexpr uint32_t kUnicodeVersion = (16u << 16) | (0u << 8) | 0u;   // 0x00100000 = 16.0.0 (P7-1)
  inline constexpr std::size_t kSimpleLowercaseMappings = 1460;
  [[nodiscard]] constexpr uint32_t packUnicodeVersion(uint8_t major, uint8_t minor, uint8_t update) noexcept;
  [[nodiscard]] constexpr bool unicodeVersionSupported(uint32_t packed) noexcept;   // packed == kUnicodeVersion
  [[nodiscard]] char32_t lowerCodePoint(char32_t cp) noexcept;                     // Simple_Lowercase_Mapping, else cp
  struct LowerResult { bool ok = false; uint32_t len = 0; };                       // ok=false: invalid UTF-8, or out too small
  // Lowers the UTF-8 in `in` up to its first 0x00 byte (string(N) padding) into `out`, then zero-fills out to its end.
  [[nodiscard]] LowerResult lowerUtf8(std::span<const uint8_t> in, std::span<uint8_t> out) noexcept;
  [[nodiscard]] uint32_t maxLoweredWidth(uint32_t n) noexcept;                     // P7-2, from the table
  ```
- The table (generated): `kLowerStage1[0x110000 >> 7]` (`uint16_t`, 0 = the identity block) and `kLowerStage2[blocks * 128]` (`int32_t` deltas, `lowerCodePoint(cp) = cp + kLowerStage2[kLowerStage1[cp >> 7] * 128 + (cp & 127)]`), plus `kMaxLoweredLenBySrcLen[5] = {0, 1, 3, 3, 4}`. Its first lines are a comment naming `Unicode 16.0.0`, the data file name, its SHA-256 and `mappings: 1460`.
- `lowerUtf8`: bytes below 0x80 take the inline A–Z → a–z path; a lead byte ≥ 0x80 decodes one scalar with the well-formedness rules of `Utf8.h` (02), maps it through the table and re-encodes it. No allocation, no lookup beyond the two table reads.

- [ ] **Step 1: Place the data and verify the pin.** Copy the release file to `codec/data/UnicodeData-16.0.0.txt`. Run `shasum -a 256 codec/data/UnicodeData-16.0.0.txt && wc -lc codec/data/UnicodeData-16.0.0.txt`. Expected: `ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f`, `40116` lines, `2175362` bytes. Anything else is the wrong file: stop.
- [ ] **Step 2: Write the failing tests** (`CheckedWritesUnicodeLowerTest.cpp`):
  - `CheckedWritesUnicodeLower.VersionIsPinnedTo16_0_0`: `kUnicodeVersion == 0x00100000u`, `packUnicodeVersion(16, 0, 0) == kUnicodeVersion`, `unicodeVersionSupported(kUnicodeVersion)`, `!unicodeVersionSupported(packUnicodeVersion(15, 1, 0))`, `!unicodeVersionSupported(packUnicodeVersion(17, 0, 0))`, `!unicodeVersionSupported(0)`.
  - `CheckedWritesUnicodeLower.EveryCodePointMatchesUnicodeData16`: parse `RUNE_UNICODE_DATA` (field 0 = code point, field 13 = mapping; expand the `<…, First>` / `<…, Last>` ranges as unmapped). For every `cp` in `0..0x10FFFF`: `lowerCodePoint(cp) == (mapping ? mapping : cp)`. Count the mappings: `== kSimpleLowercaseMappings` (1460), ASCII 26, non-ASCII 1434.
  - `CheckedWritesUnicodeLower.LengthTransitionsArePinned`: over the data file, the (source UTF-8 length → target length) histogram is exactly `{1→1: 26, 2→1: 1, 2→2: 440, 2→3: 2, 3→1: 1, 3→2: 23, 3→3: 685, 4→4: 282}`; the 2→3 pair is exactly {U+023A → U+2C65, U+023E → U+2C66}.
  - `CheckedWritesUnicodeLower.AsciiFastPath`: `"FoO"` → `"foo"` + zero padding in an 8-byte `out`, `len == 3`; `"[A-Za-z0-9]"` round trip of all 62 name characters (`ALLOWED_NAME_CHARS`): upper → lower, the rest unchanged.
  - `CheckedWritesUnicodeLower.SixteenBytesOfGrowersFillTwentyFourExactly`: input = 8 × `C8 BA` (Ⱥ, 16 bytes), `out` 24 bytes → `ok`, `len == 24`, `out` = 8 × `E2 B1 A5` (ⱥ). The same input into a 23-byte `out` → `!ok`.
  - `CheckedWritesUnicodeLower.DottedCapitalIShrinksToAsciiI`: `C4 B0` (İ) → `69` then zeros, `len == 1`. `E1 BA 9E` (ẞ) → `C3 9F` (ß). `E2 84 AA` (KELVIN SIGN) → `6B`. Turkish `I` (`49`) → `i` (`69`), no locale rule.
  - `CheckedWritesUnicodeLower.StopsAtTheFirstNul`: `"Ab\0Cd"` and `"Ab\0Zz"` lower to identical 8-byte outputs `"ab"` + zeros (Review Focus 3).
  - `CheckedWritesUnicodeLower.InvalidUtf8IsNotOk`: an overlong `C0 80`, a surrogate `ED A0 80`, `F4 90 80 80` (> U+10FFFF), a truncated `E2 B1`, a stray `80` → `!ok` each.
  - `CheckedWritesUnicodeLower.NoNormalization`: `C3 A9` (é) and `65 CC 81` (e + U+0301) give different outputs.
  - `CheckedWritesUnicodeLower.MaxLoweredWidthIsPinned`: `maxLoweredWidth` of 1, 2, 3, 4, 5, 16, 64, 85, 128 = 1, 3, 4, 6, 7, **24**, 96, 127, 192; for every n in 1..128 it equals `3 * n / 2` (P7-2) and a brute-force knapsack over `kMaxLoweredLenBySrcLen`.
- [ ] **Step 3: Run to verify they fail.** `cmake --build build/sub-codec -j14 --target rune_codec_tests`. Expected: `rune/codec/UnicodeLower.h: No such file or directory`.
- [ ] **Step 4: Write the generator and generate the table.** `gen_unicode_lower <UnicodeData.txt> <out.inc>`: fails (exit 1, a message) unless the file has 40,116 lines and exactly 1,460 mappings; computes the per-source-length maxima; writes the two stages with the identity block at index 0 and identical blocks shared. Run `cmake --build build/sub-codec --target gen_unicode_lower && build/sub-codec/gen_unicode_lower codec/data/UnicodeData-16.0.0.txt codec/src/UnicodeLowerTable.inc`. Expected: exit 0; the header comment names 16.0.0, the SHA-256 and 1460.
- [ ] **Step 5: Implement** `UnicodeLower.h` / `UnicodeLower.cpp` (which `#include "UnicodeLowerTable.inc"`) per the Interfaces.
- [ ] **Step 6: Run.** `ctest --test-dir build/sub-codec -R 'CheckedWritesUnicodeLower' --output-on-failure`. Expected: PASS (10 tests). Then the broker and the client still build: `cmake --build build/root -j14 --target rune_codec && cmake --build build/sub-client-cpp -j14 --target rune_codec`.
- [ ] **Step 7: Commit.**
```bash
git add codec/data/UnicodeData-16.0.0.txt codec/data/LICENSE-UNICODE.txt codec/tools/gen_unicode_lower.cpp \
        codec/src/UnicodeLowerTable.inc codec/include/rune/codec/UnicodeLower.h codec/src/UnicodeLower.cpp \
        codec/CMakeLists.txt broker/CMakeLists.txt client-cpp/CMakeLists.txt codec/tests/CheckedWritesUnicodeLowerTest.cpp
git commit -m "feat(codec): Unicode 16.0.0 simple lowercasing - generated two-stage table, ASCII fast path, maxLoweredWidth" \
           -m "UnicodeData-16.0.0.txt sha256 ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f, 1460 mappings." \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The view-key encoding (§7.3)

**Files:**
- Create: `codec/include/rune/codec/ViewKeyEncoding.h` (header-only)
- Modify: `codec/CMakeLists.txt` (test file in `rune_codec_tests`)
- Test: `codec/tests/CheckedWritesViewKeyEncodingTest.cpp`

**Interfaces:**
- Consumes: `lowerUtf8`, `maxLoweredWidth`, `kUnicodeVersion` (Task 1); `rune::codec::FieldValue` (`Record.h:10-18`), `PrimType` (`PrimType.h:5`).
- Produces (`namespace rune::codec`):
  ```cpp
  inline constexpr uint8_t kKeyFunctionNone = 0, kKeyFunctionLower = 1, kKeyFunctionCasefold = 2;   // 2 reserved
  inline constexpr uint32_t kMaxViewKeyBytes = 128;
  [[nodiscard]] constexpr bool isViewKeyType(PrimType t) noexcept;   // Int32 Int64 UInt16 UInt32 UInt64 Enum Bool FixedStr
  // 0 = not a valid component (wrong type, lower on a non-FixedStr, casefold or an unknown function).
  [[nodiscard]] uint32_t viewKeyComponentWidth(PrimType t, uint16_t fixedLen, uint8_t keyFunction) noexcept;
  // Writes exactly `out.size()` (= the component width) bytes; false if v is absent, the wrong kind, or invalid UTF-8.
  [[nodiscard]] bool encodeKeyComponent(PrimType t, uint8_t keyFunction, const FieldValue& v, std::span<uint8_t> out) noexcept;
  // The inverse, for writing a group key's columns into an aggregate's output row: integers, Enum and Bool decode to their
  // value; FixedStr and lower(FixedStr) return a FixedStr view of `in` (a lowered column holds the lowered bytes).
  [[nodiscard]] FieldValue decodeKeyComponent(PrimType t, uint8_t keyFunction, std::span<const uint8_t> in) noexcept;
  ```
- Encoding (§7.3, exact): unsigned → big-endian at the type's width; signed → big-endian with the sign bit flipped (`Int32`: `u32(v) ^ 0x8000'0000`; `Int64`: `u64(v) ^ 0x8000'0000'0000'0000`); `Enum` → its 4-byte value, unsigned big-endian; `Bool` → one byte 0/1; `FixedStr` → the `fixedLen` bytes, zero-padded; `lower(FixedStr)` → `lowerUtf8` into `maxLoweredWidth(fixedLen)` bytes.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesViewKeyEncoding.UnsignedIsBigEndian`: `UInt16 0x0102` → `01 02`; `UInt32 1` → `00 00 00 01`; `UInt64 0x0102030405060708` → `01 … 08`.
  - `CheckedWritesViewKeyEncoding.SignedFlipsTheSignBit`: `Int64 -1` → `7F FF FF FF FF FF FF FF`, `Int64 0` → `80 00 … 00`, `Int32 INT32_MIN` → `00 00 00 00`.
  - `CheckedWritesViewKeyEncoding.OrderIsPreserved`: for 10,000 random pairs per integer type, `a < b` ⇔ `memcmp(enc(a), enc(b)) < 0`.
  - `CheckedWritesViewKeyEncoding.BoolAndFixedStr`: `true` → `01`; `FixedStr(8) "ab"` → `61 62 00 00 00 00 00 00`.
  - `CheckedWritesViewKeyEncoding.Widths`: `viewKeyComponentWidth`: `UInt16` 2, `Int32` 4, `UInt64` 8, `Bool` 1, `FixedStr 64` 64, `lower(FixedStr 16)` **24**, `lower(FixedStr 64)` 96, `lower(UInt64)` 0, `casefold(FixedStr 16)` 0, function 3 → 0, `Enum` 4, `Float64`/`String`/`Bytes` 0. The spec's `(provider uint16, subject string(64))` is 2 + 64 = **66**.
  - `CheckedWritesViewKeyEncoding.LowerMatchesLowerUtf8`: `encodeKeyComponent(FixedStr, Lower, "FoO\0…", out24)` == `lowerUtf8` of the same bytes into 24.
  - `CheckedWritesViewKeyEncodingRegression.LoweredComponentNeverExceedsItsWidthRegression` (Review Focus 3): for N in 1..85 and 2,000 random valid inputs of ≤ N bytes drawn from {ASCII, Ⱥ, Ⱦ, İ, é, U+1E921, U+2C62}, `encodeKeyComponent` into exactly `maxLoweredWidth(N)` bytes returns true, and no byte past the span is touched (the span sits inside a guard-filled 256-byte buffer).
  - `CheckedWritesViewKeyEncoding.DecodeInvertsEncode`: for every key type (`Enum` included) and 1,000 random values, `decodeKeyComponent(t, fn, encode(v)) == v`; a lowered component decodes to its lowered bytes.
  - `CheckedWritesViewKeyEncoding.AbsentOrWrongKindIsFalse`: `present = false` → false; a `FixedStr` value into a `UInt64` component → false.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-codec -j14 --target rune_codec_tests`. Expected: `ViewKeyEncoding.h: No such file or directory`.
- [ ] **Step 3: Implement** the header per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-codec -R 'CheckedWritesViewKeyEncoding' --output-on-failure`. Expected: PASS (9 tests).
- [ ] **Step 5: Commit.**
```bash
git add codec/include/rune/codec/ViewKeyEncoding.h codec/tests/CheckedWritesViewKeyEncodingTest.cpp codec/CMakeLists.txt
git commit -m "feat(codec): order-preserving view-key component encoding (big-endian, sign flip, string(N), lower)" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The authored plan moves to the codec and gains every v2 field (C14)

**Files:**
- Create: `codec/include/rune/codec/ViewPlanAuthored.h`, `codec/src/ViewPlanAuthored.cpp` (namespace `rune::view`)
- Modify: `codec/CMakeLists.txt`, `broker/CMakeLists.txt:144-157`, `client-cpp/CMakeLists.txt:194-207` (the new source, three times)
- Modify: `broker/libs/view/include/rune/view/ViewTypes.h:9` (`MergeRule` moves out; the header includes `ViewPlanAuthored.h`), `broker/libs/view/include/rune/view/ViewPlan.h:13-48` (`ViewSizing`, `AuthoredField`, `SourceSpec`, `ViewPlan` and the two authored functions move out; the header includes `ViewPlanAuthored.h`), `broker/libs/view/src/ViewPlan.cpp:242-319` (the authored half moves out; the shared LE helpers `:21-142` are duplicated into the codec file's anonymous namespace)
- Modify: `client-cpp/include/rune/client/view/ViewPlanAuthor.h` (its types and functions become aliases: `using MergeRule = rune::view::MergeRule;` … `using rune::view::serializeAuthored;`, `using rune::view::deserializeAuthored;`, `inline constexpr uint8_t kAuthoredViewPlanVersion = 1;` stays as "the v1 byte")
- Test: `codec/tests/CheckedWritesViewPlanAuthoredTest.cpp`; the existing `broker/libs/view/tests/view/ViewPlanTest.cpp`, `PlanSourcesTest.cpp` and `client-cpp/tests/client/ViewPlanAuthorTest.cpp` must pass unchanged

**Interfaces:**
- Produces (`ViewPlanAuthored.h`, namespace `rune::view`):
  ```cpp
  enum class MergeRule : uint8_t { Replace, Count, Sum, Avg, Min, Max, Last, CountAll, LastNonNull };   // moved, values unchanged
  struct ViewSizing { uint32_t chunkBytes = 32u << 20; uint32_t capacity = 0; uint64_t maxRows = 0; };   // moved
  struct AuthoredField { std::string name; rune::codec::PrimType type; uint16_t sourceIdx; std::string srcField; MergeRule rule; };
  struct SourceSpec { int32_t topicId; std::string keyField; };
  struct KeyComponent { std::string column; uint8_t keyFunction = 0; std::string outName; };            // C14
  enum class PredicateOp : uint8_t { Column = 1, Const = 2, Eq = 3, Ne = 4, Lt = 5, Le = 6, Gt = 7, Ge = 8, And = 9, Or = 10, Not = 11 };
  struct PredicateNode { PredicateOp op; uint16_t a = 0, b = 0; std::string column; rune::codec::PrimType constType{};
                         std::array<uint8_t, 16> constBytes{}; };                                              // C14; flat, root last
  struct AuthoredConstraint { std::string name; std::vector<PredicateNode> predicate; };
  struct ViewPlan { std::string viewName; std::string key; std::vector<SourceSpec> sources; std::vector<AuthoredField> output;
                    uint8_t persistence = 0; ViewSizing sizing;
                    std::vector<KeyComponent> keyComponents; bool unique = false;                               // v2
                    std::vector<AuthoredConstraint> constraints; uint32_t unicodeVersion = 0;                   // v2
                    std::vector<KeyComponent> groupBy; };                                                       // v2 (R07c, P7-8)
  inline constexpr uint8_t kAuthoredVersionV1 = 1, kAuthoredVersion = 2;
  [[nodiscard]] bool needsV2(const ViewPlan& p) noexcept;
  [[nodiscard]] std::string keyDisplayName(const ViewPlan& p);   // "a,b" of the outNames; `key` for a v1-shaped plan
  std::vector<uint8_t> serializeAuthored(const ViewPlan& plan);
  std::optional<ViewPlan> deserializeAuthored(std::span<const uint8_t> buf);
  ```
- **`needsV2`** is true iff `unique`, or `!constraints.empty()`, or `unicodeVersion != 0`, or `!groupBy.empty()`, or `keyComponents` is non-empty and is **not** exactly one component with `keyFunction == 0`, `outName == column` and (`key.empty()` or `key == column`).
- **Layout.** v1 is unchanged (`ViewPlan.cpp:242-250`). v2 = the v1 fields with version byte 2, then `keyComponents: u32 count × {column str, keyFunction u8, outName str}`, `unique u8` (0 or 1), `constraints: u32 count × {name str, nodes: u32 count × {op u8, a u16, b u16, column str, constType u8, constBytes 16 B}}`, `unicodeVersion u32`, `groupBy: u32 count × {column str, keyFunction u8, outName str}`.
- **`serializeAuthored`** writes v1 when `!needsV2(plan)` (a single plain component then rides as `key = column`), else v2. **`deserializeAuthored`** accepts versions 1 and 2; a v1 blob yields empty v2 fields. It refuses (nullopt) a truncated blob, an unknown version, an out-of-range `PrimType`/`MergeRule`/`PredicateOp`, `unique` > 1, and trailing bytes (P7-7). Counts are untrusted: nothing is reserved from them.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesViewPlanAuthoredTest.cpp`):
  - `CheckedWritesViewPlanAuthored.V1ShapedPlanSerializesToTheV1Bytes`: the plan of `ViewPlanTest.cpp:60-75` → bytes identical to the pre-move `serializeAuthored` (a golden array captured in the test from the current code before Step 3), first byte `0x01`. With `keyComponents = {{"id", 0, "id"}}` added → the same bytes.
  - `CheckedWritesViewPlanAuthored.V2RoundTripsEveryField`: `keyComponents {{"userId",0,"userId"},{"characterId",0,"characterId"}}`, `unique = true`, two constraints (`characters <= 12`: nodes `[Column "characters", Const UInt64 12, Le(a=0,b=1)]`; `not (balance < 0)`: 4 nodes), `unicodeVersion = kUnicodeVersion` → first byte `0x02`, and `deserializeAuthored(serializeAuthored(p)) == p` field by field; re-serializing gives the same bytes.
  - `CheckedWritesViewPlanAuthored.GroupByRoundTrips`: `device_history` (`keyComponents` and `groupBy` both `(userId, deviceFp)`, rules `count(*)` and `max(lastSeen)`) → byte 0 is `0x02` and it round-trips equal.
  - `CheckedWritesViewPlanAuthored.LowerComponentForcesV2`: `{{"name", 1, "nameKey"}}` → byte 0 is `0x02`.
  - `CheckedWritesViewPlanAuthored.EveryTruncationIsRefused`: for the v2 plan above, every prefix `[0, len)` → nullopt.
  - `CheckedWritesViewPlanAuthoredRegression.TrailingBytesAreRefusedRegression`: a v1 and a v2 blob with one extra byte → nullopt.
  - `CheckedWritesViewPlanAuthored.BadEnumsAreRefused`: `PredicateOp` 0 and 12, `unique` 2, `constType` 11, `keyFunction` stays a raw byte (any value decodes; the compiler refuses it).
  - `CheckedWritesViewPlanAuthored.KeyDisplayName`: `"userId,characterId"`, `"nameKey"`, and `"id"` for a v1-shaped plan.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-codec -j14 --target rune_codec_tests`. Expected: `ViewPlanAuthored.h: No such file or directory`.
- [ ] **Step 3: Move and extend.** Move the authored types and functions; extend them per the Interfaces; turn the client header into aliases. `PlanSources.cpp` and `PlanCompiler.cpp` keep compiling through `ViewPlan.h`.
- [ ] **Step 4: Run.**
  - `ctest --test-dir build/sub-codec -R 'CheckedWritesViewPlanAuthored' --output-on-failure` → PASS (8).
  - `cmake --build build/root -j14 --target rune_view_tests && ctest --test-dir build/root -R 'ViewPlan|PlanSources|PlanCompiler' --output-on-failure` → PASS unchanged.
  - `cmake --build build/sub-client-cpp -j14 --target rune_schema_codec_smoke_tests && ctest --test-dir build/sub-client-cpp -R 'ViewPlanAuthor' --output-on-failure` → PASS unchanged.
  - `ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure` → PASS (the normalized plan bytes of existing-shaped plans did not move).
- [ ] **Step 5: Commit.**
```bash
git add codec/include/rune/codec/ViewPlanAuthored.h codec/src/ViewPlanAuthored.cpp codec/CMakeLists.txt broker/CMakeLists.txt \
        client-cpp/CMakeLists.txt broker/libs/view/include/rune/view/ViewTypes.h broker/libs/view/include/rune/view/ViewPlan.h \
        broker/libs/view/src/ViewPlan.cpp client-cpp/include/rune/client/view/ViewPlanAuthor.h \
        codec/tests/CheckedWritesViewPlanAuthoredTest.cpp
git commit -m "refactor(codec): the authored view plan lives in the codec (broker and client share it); plan v2 fields, v1 bytes kept" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (end of Phase 1).** `ctest --test-dir build/sub-codec --output-on-failure` (all codec, catalog and security suites, CatalogGolden included); `ctest --test-dir build/root -R 'View|Plan' -j14 --output-on-failure`; `ctest --test-dir build/sub-client-cpp -R 'ViewPlanAuthor|SchemaCodec' --output-on-failure`. All PASS.

---

## Phase 2 — The view engine: compile, store, checkpoint, fold

### Task 4: Compiled plan v2 and the compiler (key components, key modes, the identity rule)

**Files:**
- Modify: `broker/libs/view/include/rune/view/ViewPlan.h:50-71` (`CompiledField`, `CompiledViewPlan`), `broker/libs/view/src/ViewPlan.cpp:14-15` (versions), `:146-240` (compiled encode/decode)
- Modify: `broker/libs/view/include/rune/view/ViewTypes.h` (`kDerivedKeyField`), `broker/libs/view/include/rune/view/PlanCompiler.h:16-18` (`CompileError`), `broker/libs/view/src/PlanCompiler.cpp:243-361` (steps 2–5)
- Test: `broker/libs/view/tests/view/CheckedWritesPlanV2Test.cpp` (add to `rune_view_tests`, `broker/libs/view/CMakeLists.txt:21-27`)

**Interfaces:**
- Consumes: `ViewPlan` v2 (Task 3), `viewKeyComponentWidth`, `isViewKeyType`, `kKeyFunction*` (Task 2), `maxLoweredWidth`, `unicodeVersionSupported` (Task 1).
- Produces:
  ```cpp
  inline constexpr uint16_t kDerivedKeyField = 0xFFFE;   // ViewTypes.h, P7-15
  struct CompiledKeyComponent { uint16_t srcFieldId; uint8_t keyFunction; uint16_t width; uint16_t outSlot; rune::codec::PrimType type; };
  struct CompiledPredicateNode { PredicateOp op; uint16_t a = 0, b = 0; uint16_t outSlot = 0; rune::codec::PrimType constType{};
                                 std::array<uint8_t, 16> constBytes{}; };
  struct CompiledConstraint { uint16_t ordinal; std::string name; std::vector<CompiledPredicateNode> predicate; };   // ordinals 1..n
  // CompiledViewPlan appends:
  std::vector<CompiledKeyComponent> keyComponents; uint16_t keyWidth = 8; bool unique = false;
  std::vector<CompiledConstraint> constraints; uint32_t unicodeVersion = 0; uint64_t viewObjectId = 0;
  enum class ViewKeyMode : uint8_t { LegacyU64, Keyed };
  [[nodiscard]] ViewKeyMode keyModeOf(const CompiledViewPlan&) noexcept;       // P7-3
  [[nodiscard]] bool ownerMappedOf(const CompiledViewPlan&) noexcept;          // P7-3
  [[nodiscard]] uint16_t prefixWidthOf(const CompiledViewPlan&) noexcept;      // P7-3
  [[nodiscard]] bool needsV2(const CompiledViewPlan&) noexcept;
  enum class CompileErrorKind : uint8_t { Generic, FieldUnresolved, KeyInvalid, KeyExpressionInvalid, ConstraintInvalid,
                                          UniqueInvalid, NotCoPartitioned };   // P7-14
  struct CompileError { std::string message; CompileErrorKind kind = CompileErrorKind::Generic; };
  ```
- **Compiled layout.** v1 unchanged. v2 = the v1 fields with version byte 2, then `keyComponents: u32 count × {srcFieldId u16, keyFunction u8, width u16, outSlot u16, type u8}`, `keyWidth u16`, `unique u8`, `constraints: u32 count × {ordinal u16, name str, nodes: u32 count × {op u8, a u16, b u16, outSlot u16, constType u8, constBytes 16 B}}`, `unicodeVersion u32`, `viewObjectId u64`. A v1 blob decodes with one synthesized component `{keyFieldId[0], 0, fixedSize(output[0].type), 0, output[0].type}` and `keyWidth` = that width, so every consumer reads `keyComponents`. `needsV2(compiled)` is the compiled twin of Task 3's rule (the synthesized component does not count); the encoder writes v1 when it is false (P7-7). Strict decode: trailing bytes refused.
- **`compileView`** (single source, as today), in this order. Each refusal names its kind (code in brackets) and exact message prefix `view '<name>': `:
  1. Components = `authored.keyComponents`, or `{{authored.key, 0, authored.key}}` when empty.
  2. Per component: the column resolves in the source (else `FieldUnresolved` [34], "key column '<c>' is not a field of the source"); `viewKeyComponentWidth(type, fixedLen, fn) > 0` (else `KeyExpressionInvalid` [56]: "key column '<c>' must be an integer, enum, bool or string(N)", "lower() needs a string(N) column; '<c>' is <Type>", "casefold() is reserved", "unknown key function <n>"); outNames unique across components and the select list (else 56, "duplicate key column '<n>'").
  3. `K = Σ widths ≤ 128` (else 56, "view key is <K> bytes; the limit is 128").
  4. `unicodeVersion` is non-zero iff some component uses `lower` (else 56, "unicodeVersion is set but no key component uses lower()") and, when non-zero, `unicodeVersionSupported` (else 56, "lower() is pinned to Unicode <M>.<m>.<u>; this broker has 16.0.0").
  5. A plain component also in the select list must be `Replace` of the same source field (else 56, "key column '<c>' must project source field '<c>' unchanged"); otherwise it is added as an implicit `Replace` column. A `lower` component adds a `FixedStr(width)` column named `outName` with `srcFieldId = kDerivedKeyField`.
  6. **Aggregates** (any non-Replace rule; P7-8). A v1-shaped aggregate (no key spec, no `groupBy`) keeps the existing checks (`PlanCompiler.cpp:300-324`). For every aggregate `keyFieldId[0]` is the first group column, as today. Otherwise: `groupBy` equals the key components in order, by `column` and `keyFunction` (else `KeyInvalid` [35], "group by (<groupBy list>) must equal the key (<key list>)", an empty `groupBy` included); the source key field is an integer of at most 8 bytes (else 56, "aggregate views need an integer source key of at most 8 bytes"); every non-key column is an aggregate (the existing rule, now over all key columns); `unique` refused (`UniqueInvalid` [58], "unique needs a projection view").
  7. **Identity rule (§7.2):** a projection that is not `unique` must have a plain component on the source key field (else 56, "key does not identify the source record; declare the view unique or include <sourceKey>", with the source key field's name).
  8. Output order per P7-15; `CompiledKeyComponent::outSlot` points at each key column.
  9. Constraints: names unique and non-empty, every `Column` node resolves to an output column (→ its `outSlot`), every child index is below its node, the root is last, `And`/`Or` have two children, `Not` one, comparisons a `Column` and a `Const` whose `constType` equals the column's type (else `ConstraintInvalid` [57], "constraint '<n>': <why>"). Ordinals are 1..n in declaration order. (Evaluation and the SD5 limits are Part 09's.)
  10. `plan.keyWidth = K`; `plan.unique`, `plan.unicodeVersion` copied; `viewObjectId` stays 0 (the AdminWorker sets it, Task 12).
- The `isEightByteKeyType` check (`PlanCompiler.cpp:269-282`) is replaced by step 2 and the key modes (P7-10): it is no longer called.
- **`keyModeOf`** = `LegacyU64` iff (an aggregate with exactly one plain `Int64`/`UInt64` component) or (a projection with exactly one component, `keyFunction == 0`, on `keyFieldId[0]`, of an integer type); else `Keyed`. `ownerMappedOf` is false for every aggregate.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesPlanV2Test.cpp`; the source is `character {characterId u64 key, userId u64, name string(16), tag string(80), provider u16, subject string(64), gender u16, lastPlayed i64}` built with `DescriptorBuilder`):
  - `CheckedWritesPlanCompile.LegacyShapeStaysV1`: v1 `key characterId as select *` → `keyModeOf == LegacyU64`, `!ownerMappedOf`, `prefixWidthOf == 0`, `keyWidth == 8`, `serialize` byte 0 `0x01`, bytes equal to the pre-change compiled bytes (golden captured before Step 3).
  - `CheckedWritesPlanCompile.CompositeAccountCharacters`: components `(userId, characterId)`, select `name, gender, lastPlayed` → `Keyed`, `ownerMappedOf` true (the key is not the source key alone), `keyWidth == 16`, `prefixWidthOf == 8`, output slots 0 `userId` and 1 `characterId` (implicit `Replace`), then `name, gender, lastPlayed`.
  - `CheckedWritesPlanCompile.LowerKeyColumnIsDerived`: `(lower(name) as nameKey, characterId)`, `unicodeVersion = kUnicodeVersion` → slot 0 is `FixedStr(24)` named `nameKey` with `srcFieldId == kDerivedKeyField`, `keyComponents[0].width == 24`, `keyWidth == 32`, `prefixWidthOf == 24`.
  - `CheckedWritesPlanCompile.MaximumWidthIs128`: `(lower(tag) as tagKey, characterId)` → `keyWidth == 128` compiles; adding `gender` as a third component → `KeyExpressionInvalid`, message ends "view key is 130 bytes; the limit is 128".
  - `CheckedWritesPlanCompile.IdentityRule`: `(userId)` non-unique projection → 56, message contains "key does not identify the source record; declare the view unique or include characterId"; the same with `unique = true` compiles (`ownerMappedOf` true).
  - `CheckedWritesPlanCompile.StringSourceKeyIsKeyedWithoutOwnerMap`: a topic keyed by `name string(16)` with `key name` → `Keyed`, `!ownerMappedOf`, `keyWidth == 16`.
  - `CheckedWritesPlanCompile.NarrowSourceKeyIsLegacy`: a topic keyed by `id u16` with `key id` → `LegacyU64`, `keyWidth == 2` (no more "8-byte" refusal).
  - `CheckedWritesPlanCompile.UnknownUnicodeVersionIsRefused`: `lower` with `unicodeVersion = packUnicodeVersion(17,0,0)` → 56 "pinned to Unicode 17.0.0"; `unicodeVersion = 0` with `lower` → 56; non-zero without `lower` → 56.
  - `CheckedWritesPlanCompile.KeyTypeRefusals`: `lower(gender)` → 56 "lower() needs a string(N) column; 'gender' is UInt16"; a `Float64` column → 56; `keyFunction 2` → "casefold() is reserved"; `3` → "unknown key function 3"; an unknown column → `FieldUnresolved`.
  - The aggregate cases use a second source `device {eventId u64 key, userId u64, deviceFp string(32), platform enum, lastSeen i64, bytes u64}`:
  - `CheckedWritesPlanCompile.SingleEightByteGroupKeyStaysLegacy`: `count(*) group by userId` (v1 shape, and the same as a v2 plan with one plain `userId` component and `groupBy (userId)`) → `LegacyU64`; the compiled bytes equal the pre-change golden (v1).
  - `CheckedWritesPlanCompile.CompositeGroupByIsKeyed` (R07c, `device_history`): `key (userId, deviceFp)`, `groupBy (userId, deviceFp)`, `count(*) as uses, max(lastSeen) as lastSeen` → `Keyed`, `keyWidth == 40`, `prefixWidthOf == 8`, `!ownerMappedOf`, output slots `userId, deviceFp, uses, lastSeen`. With `lower(deviceFp) as fpKey` in both lists → `keyWidth == 8 + 48 = 56`, slot 1 `FixedStr(48)` with `kDerivedKeyField`. A single `platform` (enum) group → `Keyed`, `keyWidth == 4`; a single `u16` group → `Keyed`, `keyWidth == 2`.
  - `CheckedWritesPlanCompile.GroupByMustEqualTheKey`: `groupBy (deviceFp, userId)` against `key (userId, deviceFp)` → `KeyInvalid` (35), detail "group by (deviceFp, userId) must equal the key (userId, deviceFp)"; an empty `groupBy` with that key → 35; `groupBy (userId, lower(deviceFp))` against `key (userId, deviceFp)` → 35.
  - `CheckedWritesPlanCompile.KeyedAggregateNeedsAnIntegerSourceKey`: the same view over a topic keyed by `string(16)` → 56 "aggregate views need an integer source key of at most 8 bytes"; `unique` on an aggregate → `UniqueInvalid`.
  - `CheckedWritesPlanCompile.ConstraintsCompileStructurally`: `check (lastPlayed >= 0)` resolves `outSlot` of `lastPlayed`, ordinal 1; a second constraint gets ordinal 2; a dangling child index, a `Const` type mismatch, a duplicate name and an unknown column → `ConstraintInvalid` each.
  - `CheckedWritesPlanV2.CompiledRoundTripAndStrictness`: a v2 compiled plan with every field (two components, a constraint, `unicodeVersion`, `viewObjectId = 77`) round-trips equal; every truncation and one trailing byte → nullopt; a v1 blob decodes with the synthesized component.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `no member named 'keyComponents' in 'rune::view::CompiledViewPlan'`.
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesPlan|PlanCompiler|ViewPlan|AggState|ViewFold|AggFold' --output-on-failure`. Expected: PASS. The existing `PlanCompilerTest` case that asserted the narrow-key refusal (grep `isEightByteKeyType` / "8-byte Phase 1 key type" in `PlanCompilerTest.cpp`) is re-pinned to the new behaviour and recorded as a deviation in the commit body (the restriction is lifted by design, P7-10).
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/view/include/rune/view/ViewPlan.h broker/libs/view/src/ViewPlan.cpp broker/libs/view/include/rune/view/ViewTypes.h \
        broker/libs/view/include/rune/view/PlanCompiler.h broker/libs/view/src/PlanCompiler.cpp \
        broker/libs/view/tests/view/CheckedWritesPlanV2Test.cpp broker/libs/view/tests/view/PlanCompilerTest.cpp broker/libs/view/CMakeLists.txt
git commit -m "feat(view): compiled plan v2 - key components, key modes, identity rule, 128-byte keys, Unicode pin, structural constraints" \
           -m "Deviation: PlanCompilerTest's narrow-key refusal re-pinned (lifted by design, P7-10)." \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `ViewTable` takes `ViewKey`s: wide slots, the seeded hash, keyed dirty drain

**Files:**
- Create: `broker/libs/storage/include/rune/storage/WyHash.h` (`rune::storage::detail::wyhash64` and its helpers, moved byte for byte from `SkimpyOffsetMap.h:54-125`; `SkimpyOffsetMap.h` includes it)
- Create: `broker/libs/view/include/rune/view/ViewKeyHash.h`: `[[nodiscard]] inline uint64_t viewKeyHash(std::span<const uint8_t> bytes, uint64_t seed) noexcept` = `wyhash64(bytes.data(), bytes.size(), seed)`
- Modify: `broker/libs/view/include/rune/view/ViewKey.h` (contract addition 1: `fromBytes`, `operator==`, `kMaxViewKeyBytes` aliased to `rune::codec::kMaxViewKeyBytes`; 04's `fromU64` / `asU64` untouched)
- Modify: `broker/libs/view/include/rune/view/ViewTable.h:82-343`, `broker/libs/view/src/ViewTable.cpp`
- Test: `broker/libs/view/tests/view/CheckedWritesViewTableTest.cpp` (add to `rune_view_tests`); `ViewTableTest.cpp` and `SkimpyOffsetMapTest` pass unchanged

**Interfaces:**
- Consumes: Part 04's `ViewKey::fromU64` / `asU64` (R07d).
- Produces: contract addition 1 (`ViewKey::fromBytes`, `operator==`, the `kMaxViewKeyBytes` alias), and (`ViewTable`, contract addition 2):
  ```cpp
  struct ViewTableKeys { uint8_t keyWidth = 8; uint8_t prefixWidth = 0; uint64_t hashSeed = 0; uint64_t hashMaskForTest = ~uint64_t{0}; };
  explicit ViewTable(RowLayout layout, size_t initialIndexSlots = 4096, size_t chunkBytes = 32u << 20, ViewTableKeys keys = {});
  uint8_t* put(const ViewKey& key);                       // key.len == keyWidth (asserted)
  const uint8_t* get(const ViewKey& key) const noexcept;
  bool erase(const ViewKey& key) noexcept;
  // emit(uint32_t rid, const uint8_t* payload, bool deleted, std::span<const uint8_t> key) - key is keyWidth bytes; same order rule as drainDirty
  template <class Fn> void drainDirtyKeyed(Fn&& emit);
  [[nodiscard]] std::span<const uint8_t> keyOfRow(const uint8_t* payload) const noexcept;   // tables that store the key (P7-5)
  [[nodiscard]] uint8_t keyWidth() const noexcept;
  [[nodiscard]] uint8_t prefixWidth() const noexcept;
  ```
- **Narrow** (`keyWidth ≤ 8`): the slot word is `key.asU64()`, compared exactly, as today; home and tag come from `mix(word ^ hashSeed) & hashMaskForTest`. The `uint64_t` API stays and is the same code path. With `hashSeed = 0` and the default mask the index is bit-for-bit today's.
- **Wide** (`keyWidth > 8`): the slot word is `h = viewKeyHash(key, hashSeed) & hashMaskForTest`; tag `fullTag(h)`, home `h & mask`. A probe matches tag, then word, then `memcmp` of the row header's key. `backshiftPrimary` (`ViewTable.cpp:124-155`), `migrateStep` (`:226-283`) and `startGrow` take the home from the stored word and never touch the arena (§7.5). The `uint64_t` API asserts `keyWidth ≤ 8 && prefixWidth == 0` (a prefix table must write its header key, so it takes `ViewKey`s only).
- **Row** (P7-5): the slab row is `headerLen + layout.rowLen`; `headerLen = (keyWidth > 8 || prefixWidth > 0 ? keyWidth : 0) + (prefixWidth > 0 ? 8 : 0)`; `rowsPerChunk_` uses the full stride (`ViewTable.cpp:45-51`). `put` writes the header key on insert and returns the payload. A deleted wide key is copied into a reserved `deletedKeyBytes_` (K bytes per `deleted_` entry), so `drainDirtyKeyed` never reads a reused row.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesViewTable.ViewKeyNames` (R07d): `ViewKey::fromU64(v).asU64() == v` for 0, 1, 2^63 and `UINT64_MAX` (04's names, unchanged); `fromBytes` of 6 bytes has `len == 6` and `asU64()` = their little-endian value; `fromBytes(a) == fromBytes(a)`, and two keys equal except in their length are not equal; `static_assert(rune::view::kMaxViewKeyBytes == rune::codec::kMaxViewKeyBytes)`.
  - `CheckedWritesViewTable.NarrowViewKeyIsTheU64Path`: `put(ViewKey::fromU64(42))` then `get(42)` (u64 API) returns the same pointer; `erase(ViewKey::fromU64(42))` → `get(42) == nullptr`.
  - `CheckedWritesViewTableRegression.SeedZeroIsBitIdenticalRegression`: the same 10,000-op random sequence on a default-constructed table and on `ViewTableKeys{.keyWidth = 8, .hashSeed = 0}` gives identical `size`, `chunkCount`, rid of every key and the same `drainDirty` stream.
  - `CheckedWritesViewTable.WideKeysOfEveryWidth`: K ∈ {9, 16, 24, 66, 74, 120, 128}: 50,000 distinct random keys put / get / erase (half), across at least two grows (`initialIndexSlots = 64`); every survivor's payload pointer is stable since its put; `keyOfRow(get(k))` equals `k`.
  - `CheckedWritesViewTableRegression.FullCollisionsStayCorrectAcrossGrowthRegression` (§15 forced collision): `hashMaskForTest = 0`, `hashSeed = 7`, K = 24, 4,000 keys that share their first 23 bytes: every get/erase is correct throughout the migration window, and `size()` is exact.
  - `CheckedWritesViewTableRegression.WideBackshiftNeverReadsTheArenaRegression`: K = 16, `initialIndexSlots = 64`, 5,000 keys (so growth and migration run), then 1,000 erases. A test-only counter `arenaKeyReadsForTest()` (incremented on every header-key `memcmp`) grows by exactly one per erase (the lookup of the erased key itself), although backshift and `migrateStep` moved hundreds of entries; every remaining key is then found.
  - `CheckedWritesViewTable.DrainDirtyKeyedEmitsKBytes`: K = 16: put A, put B, erase A, put A again → deletes first (A's 16 bytes), then upserts A and B with their 16-byte keys; a narrow K = 6 table emits 6-byte keys (the first 6 little-endian bytes of the slot word).
  - `CheckedWritesViewTable.SeedChangesPlacementNotResults`: two K = 16 tables with seeds 1 and 2 fed the same keys hold different slot orders (a test-only `slotOrderForTest()`) and the same contents.
  - `CheckedWritesViewTableModelRegression.RandomOpsMatchAMapModel` (Interaction matrix C; prefix off here, Task 6 adds the prefix leg).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `no matching constructor` / `no member named 'drainDirtyKeyed'`.
- [ ] **Step 3: Implement** `WyHash.h` (a pure move), `ViewKeyHash.h` and the `ViewTable` changes per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesViewTable|ViewTable|SkimpyOffsetMap|WorkerViewSet|ViewCheckpoint' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/storage/include/rune/storage/WyHash.h broker/libs/storage/include/rune/storage/SkimpyOffsetMap.h \
        broker/libs/view/include/rune/view/ViewKey.h \
        broker/libs/view/include/rune/view/ViewKeyHash.h broker/libs/view/include/rune/view/ViewTable.h broker/libs/view/src/ViewTable.cpp \
        broker/libs/view/tests/view/CheckedWritesViewTableTest.cpp broker/libs/view/CMakeLists.txt
git commit -m "feat(view): ViewTable keys up to 128 bytes - 13-byte slots with a cached seeded hash, key in the row, keyed dirty drain" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The prefix index

**Files:**
- Modify: `broker/libs/view/include/rune/view/ViewTable.h`, `broker/libs/view/src/ViewTable.cpp`
- Test: `broker/libs/view/tests/view/CheckedWritesViewTablePrefixTest.cpp` (add to `rune_view_tests`)

**Interfaces:**
- Consumes: Task 5's header layout and seeded hash.
- Produces:
  ```cpp
  enum class PrefixScan : uint8_t { End, Stopped, CursorGone };
  // visit(std::span<const uint8_t> key, const uint8_t* payload) -> bool; false stops BEFORE consuming that row.
  // resumeAfter empty: from the head of the prefix's list.  Otherwise: from the row after that key; CursorGone if the key is
  // absent or does not start with `prefix`.
  template <class Fn> PrefixScan scanPrefix(std::span<const uint8_t> prefix, std::span<const uint8_t> resumeAfter,
                                            Fn&& visit) const noexcept;
  ```
- **Structure (§7.5, P7-11):** a second open-addressing index from `viewKeyHash(prefix, hashSeed) & hashMaskForTest` to the **head rid**, same 13-byte slot and the same incremental-growth machinery as the main index (extract the arrays, `maybePrezero`, `startGrow` and `migrateStep` into a private `SlotIndex` used twice, constants unchanged, so no op ever rehashes either index whole). A prefix match is confirmed against the head row's header key. Rows with one prefix are a doubly linked list through the header's `prev`/`next` rids (`kNoRid = 0xFFFF'FFFF`). A new key is linked at the head; an existing key updated in place keeps its place; an erase unlinks in O(1) and removes the prefix slot when its list empties.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesViewTablePrefix.ScanReturnsExactlyThePrefixRows`: K = 16, prefix 8: 12 rows under prefix A, 5 under B, 300 under C → `scanPrefix(A)` visits exactly A's 12 (as a set), `End`.
  - `CheckedWritesViewTablePrefix.EmptyPrefixScansNothing` (§15): an unknown prefix → `End`, no visit.
  - `CheckedWritesViewTablePrefix.ResumeAfterAKeyContinues`: C's 300 rows scanned with `visit` returning false after 100 → `Stopped`; resuming after the 100th key three times visits the other 200 exactly once each, then `End`.
  - `CheckedWritesViewTablePrefix.ErasesAndInsertsUnlinkAndLink`: erase the head, the tail and a middle row of A; insert two new A rows → the scan set is the model's after every step.
  - `CheckedWritesViewTablePrefixRegression.ReinsertedTokenRowResumesWithoutLoopingRegression` (Review Focus 5): token = A's 3rd row; erase and re-insert it → resuming after it terminates (`End` within ≤ 12 visits) and never revisits a row twice in that page.
  - `CheckedWritesViewTablePrefixRegression.EmptiedPrefixListAnswersCursorGoneRegression` (Review Focus 5): token = A's last row; erase every A row → resume → `CursorGone`; a token whose key exists under prefix B but is passed with prefix A → `CursorGone`.
  - `CheckedWritesViewTablePrefixRegression.CollidingPrefixesNeverMixRegression` (§15): `hashMaskForTest = 0` (every prefix collides): 64 prefixes × 8 rows → each scan returns only its prefix's rows, across a prefix-index growth (`initialIndexSlots = 16`).
  - `CheckedWritesViewTablePrefix.NarrowCompositeTableHasAPrefixIndex`: K = 6 (`u16, u32`), prefix 2 → scans work; the main index still compares the `asU64()` word.
  - The prefix leg of `CheckedWritesViewTableModelRegression.RandomOpsMatchAMapModel` (Interaction matrix C).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `no member named 'scanPrefix'`.
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesViewTable|ViewTable' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/view/include/rune/view/ViewTable.h broker/libs/view/src/ViewTable.cpp \
        broker/libs/view/tests/view/CheckedWritesViewTablePrefixTest.cpp broker/libs/view/CMakeLists.txt
git commit -m "feat(view): prefix index over the first n-1 key columns - head rid per prefix hash, O(1) linked rows, resumable scans" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Checkpoint v3

**Files:**
- Modify: `broker/libs/view/include/rune/view/ViewCheckpoint.h` (after `:116-137`), `broker/libs/view/src/ViewCheckpoint.cpp`
- Modify: `broker/libs/node/src/WorkerViewSet.cpp:175-226` (`splitViewCheckpointRecords` learns v3), `broker/libs/node/src/ViewSnapshotIoAgent.cpp:56-67` (`peekDeltaOffset` accepts v3)
- Test: `broker/libs/view/tests/view/CheckedWritesViewCheckpointV3Test.cpp` (add to `rune_view_tests`), `broker/libs/node/tests/node/ViewSnapshotIoAgentTest.cpp` (one case), `broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp` (create; add to `rune_checked_writes_tests`)

**Interfaces:**
- Consumes: `ViewTable::put/erase(const ViewKey&)`, `drainDirtyKeyed` (Task 5); 01's `CheckpointMark`, `AppliedDelta::mark`, `checkpointMarkOf`.
- Produces:
  ```cpp
  inline constexpr uint32_t kViewCheckpointMagicV3 = 0x564B4333u;   // "3CKV" (R01: never 0x534B433x)
  inline constexpr std::size_t kCheckpointV3HeaderBytes = 4 + 8 + 1;          // magic, O, tableCount
  inline constexpr std::size_t kCheckpointV3TableHeaderBytes = 1 + 1 + 4 + 4; // tableId, keyLen, rowLen, count
  [[nodiscard]] constexpr std::size_t checkpointV3EntryHeaderBytes(uint8_t keyLen) noexcept { return 4 + 1 + keyLen; }
  inline constexpr uint8_t kKeyedTableView = 0, kKeyedTableOwners = 1;   // projections
  inline constexpr uint8_t kKeyedTableContributors = 2, kKeyedTableGroups = 3, kKeyedTableBlocks = 4, kKeyedTableGroupIds = 5;   // keyed aggregates (Task 11)
  struct KeyedDirtyEntry { uint32_t rid; bool deleted; std::span<const uint8_t> key; const uint8_t* row; };
  struct KeyedTableDelta { uint8_t tableId; uint8_t keyLen; uint32_t rowLen; std::span<const KeyedDirtyEntry> entries; };
  size_t encodeKeyedDelta(std::span<const KeyedTableDelta> tables, int64_t O, std::vector<uint8_t>& out);
  std::optional<AppliedDelta> applyKeyedDelta(std::span<ViewTable* const> tables, std::span<const uint32_t> rowLens,
                                              std::span<const uint8_t> keyLens, std::span<const uint8_t> record);
  int64_t replayKeyed(std::span<ViewTable* const> tables, std::span<const uint32_t> rowLens,
                      std::span<const uint8_t> keyLens, std::span<const std::span<const uint8_t>> recordsInOrder);
  ```
- **Wire:** `[magic3 u32][O i64][tableCount u8]` then per table `[tableId u8][keyLen u8][rowLen u32][count u32]` and `count × {rid u32, flags u8 (0 upsert, 1 delete), key[keyLen], [rowLen bytes iff upsert]}`, then `[crc32c u32]` over every preceding byte. O at byte 4 (the IO agent peek). The row bytes are the payload only (P7-5); prefix links are rebuilt by the replay's `put`s. `applyKeyedDelta` validates everything (bounds per entry against the real buffer, ids `< tables.size()` and each at most once — any id below the table count is accepted, whatever the caller's table count up to the `tableCount u8` limit (R-X3: Part 09 adds tables 6 and 7) — `rowLen`/`keyLen` equal to the table's, CRC) before mutating any table, exactly like `applyMultiDelta` (`ViewCheckpoint.cpp:159-221`), and returns `mark = CheckpointMark::FoldSkipped` (R01).

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesViewCheckpointV3.RoundTripTwoTables`: a K = 32 view table (prefix 24) with 1,000 rows and an owner table (key 8, payload 32) → `encodeKeyedDelta` of a base, then a delta with 100 erases and 50 re-puts → `replayKeyed` into fresh tables gives identical contents (every key, every payload byte), identical `scanPrefix` sets, returned O = the delta's O.
  - `CheckedWritesViewCheckpointV3.SixTablesRoundTrip`: a record with tables 0 (key 40), 2, 3, 4 (key 8) and 5 (key 40) replays into six fresh tables identically.
  - `CheckedWritesViewCheckpointV3Regression.EightTablesRoundTripRegression` (R-X3): a record with tables 0–7 (tables 6 and 7 with key 24 and payload 8, the shape Part 09's unique-claim tables use) replays into eight fresh tables identically; the same record given seven tables → nullopt and no table touched; an id 8 in a record replayed into eight tables → nullopt.
  - `CheckedWritesViewCheckpointV3.MagicAndOffsetPlacement`: bytes 0..3 = `33 43 4B 56`, bytes 4..11 = O little-endian; `checkpointMarkOf(record) == CheckpointMark::FoldSkipped`.
  - `CheckedWritesViewCheckpointV3Regression.CorruptOrTruncatedNeverMutatesRegression`: every truncation, a flipped CRC byte, a bogus `count = 0xFFFFFFFF`, a duplicated table id, a wrong `keyLen` and a wrong `rowLen` → nullopt and both tables untouched.
  - `CheckedWritesViewCheckpointV3.DeleteThenReinsertInOneIntervalSurvives`: the `ViewTableDirtyTrackingRegression.DeleteThenReinsertSameKeyWithinIntervalReplaysCorrectly` scenario on K = 16 through `drainDirtyKeyed` → the key is present after replay.
  - `CheckedWritesViewSet.SplitterAcceptsV3` (`CheckedWritesViewSetTest.cpp`): a file of `[v1][v3][v3, truncated]` → `splitViewCheckpointRecords` returns the first two only.
  - `ViewSnapshotIoAgentTest`: `CheckedWritesIoAgent.V3RecordIsAppendedAndAcked`: a v3 frame on the deltas ring is appended and its O reported on `done`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests rune_checked_writes_tests rune_node_tests`. Expected: `kViewCheckpointMagicV3 was not declared`.
- [ ] **Step 3: Implement** per the Interfaces; `checkpointMarkOf` (01) gains the v3 case.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesViewCheckpointV3|CheckedWritesViewSet|CheckedWritesIoAgent|ViewCheckpoint|ViewSnapshotIoAgent|CheckedWritesAbortedFold' -j14 --output-on-failure`. Expected: PASS (01's marker tests unchanged).
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/view/include/rune/view/ViewCheckpoint.h broker/libs/view/src/ViewCheckpoint.cpp broker/libs/node/src/WorkerViewSet.cpp \
        broker/libs/node/src/ViewSnapshotIoAgent.cpp broker/libs/view/tests/view/CheckedWritesViewCheckpointV3Test.cpp \
        broker/libs/node/tests/node/ViewSnapshotIoAgentTest.cpp broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp \
        broker/libs/view/CMakeLists.txt broker/libs/node/CMakeLists.txt
git commit -m "feat(view): checkpoint v3 - K-byte keys per table (view rows + owner map), always marked, v1/v2 still load" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: `KeyedFoldSpec` and `deriveViewKey`

**Files:**
- Create: `broker/libs/view/include/rune/view/KeyedFold.h`, `broker/libs/view/src/KeyedFold.cpp` (add to `rune_view`, `broker/libs/view/CMakeLists.txt:1-10`)
- Test: `broker/libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp` (create; add to `rune_view_tests`)

**Interfaces:**
- Consumes: compiled plan v2 (Task 4), `encodeKeyComponent` (Task 2), `rune::codec::readField` (`Record.h`), `ViewKey` (C9.1).
- Produces:
  ```cpp
  struct KeyedFoldSpec {
      struct Component { uint16_t srcFieldId; rune::codec::PrimType type; uint8_t keyFunction; uint16_t width; uint16_t offset; uint16_t outSlot; };
      std::vector<Component> components;   // built once at install (cold)
      uint16_t keyWidth = 0;
      uint16_t prefixWidth = 0;
      uint8_t  wireKeyWidth = 0;           // the source key field's width (owner-table key)
      bool     sourceKeyed = false;        // exactly one plain component on the source key field
      bool     ownerMapped = false;
  };
  [[nodiscard]] std::optional<KeyedFoldSpec> buildKeyedFoldSpec(const CompiledViewPlan& plan, const rune::codec::Descriptor& evolvingSrc);
  enum class DeriveKey : uint8_t { Ok, Absent, Malformed };
  [[nodiscard]] DeriveKey deriveViewKey(const KeyedFoldSpec& spec, const rune::codec::Descriptor& writer,
                                        std::span<const uint8_t> value, ViewKey& out) noexcept;
  ```
- `buildKeyedFoldSpec` returns nullopt for a `LegacyU64` plan or a field missing from `evolvingSrc` (cold, logged by the caller). `deriveViewKey` reads each component's field from the writer's version and encodes it at `offset`; `Absent` when any component field is absent in that version (P7-16), `Malformed` when `encodeKeyComponent` fails (defensive: Part 02 validated the UTF-8). For `sourceKeyed` string keys the fold uses the wire key itself (Task 9).

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesKeyedFold.DerivesTheCompositeKey`: `(userId=42, characterId=7)` → 16 bytes `00…2A 00…07`.
  - `CheckedWritesKeyedFold.DerivesTheLoweredKey`: `(lower(name)="FoO", characterId=7)` → `"foo"` + 21 zeros + `00…07`.
  - `CheckedWritesKeyedFold.AbsentComponentIsAbsent`: a writer version without `userId` → `Absent`.
  - `CheckedWritesKeyedFold.SpecOffsetsAndFlags`: the four `Keyed` matrix shapes give the offsets `{0, 8}`, `{0, 2, 66}`, `{0, 24}`, `{0, 120}`, the prefix widths 8, 66, 24, 120, `ownerMapped` true; the `string(16)` source-keyed view gives `sourceKeyed` true, `ownerMapped` false.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `KeyedFold.h: No such file or directory`.
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesKeyedFold' --output-on-failure`. Expected: PASS (4).
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/view/include/rune/view/KeyedFold.h broker/libs/view/src/KeyedFold.cpp \
        broker/libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp broker/libs/view/CMakeLists.txt
git commit -m "feat(view): KeyedFoldSpec and deriveViewKey - one view key from the writer's version, no allocation" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: The keyed projection fold: derived and composite keys, the owner map, moves

**Files:**
- Modify: `broker/libs/view/include/rune/view/ViewFold.h` (`FoldStats` `:50-72`; 04's `ProjectionFold`, `prepareProjection`, `applyProjection`), `broker/libs/view/src/ViewFold.cpp`
- Test: `broker/libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp`

**Interfaces:**
- Consumes: Tasks 5, 6, 8; C9.3 (`SourceRecord`, `Prospect`, `prepareProjection`, `applyProjection`, `ProjectionFold`); `RecordSpan::key` (R02a).
- Produces: `FoldStats` appends `uint64_t skippedNoKey = 0; uint64_t keyConflicts = 0; uint64_t ownerMoves = 0;`. `ProjectionFold` appends `const KeyedFoldSpec* keyed = nullptr; ViewTable* owners = nullptr;` (null for `LegacyU64`: the 04 path, unchanged).
- **The keyed rules** (prepare computes, apply writes; both no-alloc; the view keys touched go into the `Prospect`):
  - **Tombstone.** `sourceKeyed`: view key = the wire key bytes → `Erase` if present, else `None`. `ownerMapped`: `owners.get(wire)` → `Erase(that view key)` and drop the owner entry; absent → `None`.
  - **Live record.** `deriveViewKey` → `Absent`: on an owner-mapped view `Erase` of the source key's current row (if any) and `skippedNoKey` + 1 (P7-16); `Malformed`: `skippedMalformed` + 1, `None`. `Ok` → new key N:
    - `ownerMapped`, current owner key O ≠ N → **Move** (erase O's row, put N, `owners[wire] = N`), `ownerMoves` + 1;
    - O == N, or no O → **Upsert** N (`owners[wire] = N` when owner-mapped);
    - before either: if N's row exists and `owners[wire] != N` (another source key owns it) → `None`, `keyConflicts` + 1 (P7-9; unreachable through DDL until Part 09).
  - **Apply** writes every output slot from the writer's value, except `kDerivedKeyField` slots, which get the component's encoded bytes from the key (P7-15).

- [ ] **Step 1: Write the failing tests** (`CheckedWritesKeyedFoldTest.cpp`, folding real `RecordBatchBuilder` batches through `foldBatch` with the four `Keyed` shapes):
  - `CheckedWritesKeyedFold.CompositeRowsAndPrefix`: three characters of user 42 and one of 43 → 4 rows; `scanPrefix(42)` → 3; `get((42, c2))` payload has `userId 42`, `characterId c2`, `name` as sent.
  - `CheckedWritesKeyedFold.RenameMovesTheRowAndTheOwner` (§15 "rename with an owner-map move"): `(lower(name), characterId)`: upsert c7 "Foo", then c7 "Bar" → the "foo" row is gone, the "bar" row exists, `owners[c7] == ("bar", c7)`, `ownerMoves == 1`, the dirty stream has one delete and two upserts (view + owner).
  - `CheckedWritesKeyedFold.MoveGroupMovesTheRow`: `(userId, characterId)`: c7 from user 42 to 43 → `scanPrefix(42)` lacks c7, `scanPrefix(43)` has it.
  - `CheckedWritesKeyedFold.TombstoneErasesThroughTheOwnerMap`: delete c7 (empty value) → its row and owner entry are gone; a second tombstone → `None`.
  - `CheckedWritesKeyedFold.DeleteThenReuseTheName`: c7 "Foo" deleted, then c9 "Foo" → one row `("foo", c9)`.
  - `CheckedWritesKeyedFoldRegression.RenameAndBackInOneBatchLeavesOneRowRegression` (Review Focus 1): one batch c7 "Foo", "Bar", "Foo" → exactly one row `("foo", c7)`, `owners[c7] == ("foo", c7)`, `table.size() == 1`.
  - `CheckedWritesKeyedFoldRegression.RenameToTheOwnKeyIsAnUpdateNotAMoveRegression` (Review Focus 1): c7 "Foo" then c7 "FOO" (same lowered key) with another `gender` → `ownerMoves == 0`, one row, the new `gender`.
  - `CheckedWritesKeyedFold.DerivedColumnHoldsTheLoweredBytes`: the `nameKey` slot of `("ⱥⱥ", …)` written from "ȺȺ" equals the key's first 24 bytes; `readSlot` of it gives a `FixedStr` of those bytes; the payload key columns equal `keyOfRow` decoded (P7-5).
  - `CheckedWritesKeyedFold.AbsentKeyComponentErasesAndCounts`: c7 written under v2 (with `userId`), then under v1 (without) → c7's row is erased, `skippedNoKey == 1`.
  - `CheckedWritesKeyedFold.StringSourceKeyedViewUsesTheWireKey`: a `string(16)`-keyed topic: upsert "alice", tombstone "alice" → erased with no owner map.
  - `CheckedWritesKeyedFold.UniqueConflictIsSkippedAndCounted` (P7-9): a unique `lower(name)` plan installed directly: c7 "Foo", then c9 "foo" → c9 not applied, `keyConflicts == 1`, the row still belongs to c7.
  - `CheckedWritesKeyedFoldRegression.LegacyPlansFoldExactlyAsBeforeRegression`: the existing `ViewFoldTest` scenarios through `prepareProjection`/`applyProjection` with `keyed == nullptr` give byte-identical tables and stats (04's invariant still holds).
  - `CheckedWritesKeyedFoldModelRegression.RandomLogsFoldToTheFreshRefold` (Interaction matrix C).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests`. Expected: `no member named 'keyed' in 'rune::view::ProjectionFold'`.
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesKeyedFold|ViewFold|AggFold|CheckedWritesAbortedFold' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/view/include/rune/view/ViewFold.h broker/libs/view/src/ViewFold.cpp broker/libs/view/tests/view/CheckedWritesKeyedFoldTest.cpp
git commit -m "feat(view): keyed projection fold - composite and lower() keys, owner map with rename/move retraction, no allocation" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint B (end of Phase 2).** `ctest --test-dir build/root -R 'View|Plan|Agg|CheckedWrites' -j14 --output-on-failure`; `ctest --test-dir build/asan -R 'CheckedWritesView|CheckedWritesKeyed|CheckedWritesPlan|ViewTable|ViewCheckpoint' --output-on-failure` (after `cmake --build build/asan -j14 --target rune_view_tests`). All PASS. Start the Task 18 baseline worktree build now (lean-bench rule: start benches early).

---
## Phase 3 — The broker, the wire and the client

### Task 10: Keyed instances in `WorkerViewSet`: owner tables, seeds, v3 checkpoints and recovery, the Unicode refusal

**Files:**
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h` (`ViewFoldCounters` `:130-139`, `ViewInstance` `:191-263`, constructor `:269`, `getRow` `:438-451`, `driveView_` `:599-702`, `discardDirty_` `:706-714`, the private members `:723-754`), `broker/libs/node/src/WorkerViewSet.cpp` (`replayInstance` `:76-129`, `logRecovered` `:135-141`, `maxRecordsPerPassFor` `:148-170`, `drainInstalls` `:279-336`, `getOrCreate_` `:338-422`, `getRow` `:613-648`, `buildAndPushCheckpoint_` `:650-728`)
- Modify: `broker/libs/node/include/rune/node/NodeConfig.h` (`uint64_t viewHashSeedForTest = 0;`), `broker/libs/node/src/BrokerNode.cpp:1937` (pass a seed: `cfg.viewHashSeedForTest != 0 ? cfg.viewHashSeedForTest + workerIndex : (uint64_t{rd()} << 32) | rd()` with one `std::random_device rd;` per boot)
- Test: `broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp` (Task 7 created it), `broker/libs/node/tests/node/CheckedWritesKeyedRecoveryTest.cpp` (create; `rune_checked_writes_tests`). Helpers as in `WorkerViewSetTest.cpp:100-170`.

**Interfaces:**
- Consumes: Tasks 4–9; 01's `FoldSkip`, `sourceLogStart`, `CheckpointStream` (unchanged semantics: v3 records are marked, so AD5a never discards them).
- Produces:
  ```cpp
  WorkerViewSet(ViewSnapshotChannel& channel, std::string viewDataDir, uint64_t hashSeed = 0);
  [[nodiscard]] uint64_t hashSeedForTest() const noexcept;
  [[nodiscard]] uint64_t installRefusals() const noexcept;                       // P7-13
  void setHashMaskForTest(uint64_t mask) noexcept;   // test seam: tables created afterwards AND their hashes with it (P7-4)
  enum class RowStatus : uint8_t { Ok, NotFound, NotReady, BadKey };             // BadKey appended
  // LegacyU64 views: key = the view-key encoding of the one column, decoded to the table's u64; Keyed: key.size() == keyWidth.
  const uint8_t* getRowK(int32_t viewTopicId, int32_t partition, std::span<const uint8_t> key,
                         const rune::view::RowLayout*& outLayout, RowStatus* outStatus = nullptr) const;
  struct ScanPage { uint16_t status = 0; uint16_t rows = 0; uint32_t bytes = 0; uint8_t nextTokenLen = 0;
                    std::array<uint8_t, rune::view::kMaxViewKeyBytes> nextToken{}; };
  // Writes rows as [len u32][payload] into `out`, stops at min(limit or 256, 256) rows or ScanViewRowsResponse::kMaxPageBytes; status uses
  // ViewRowResponse codes (Ok, NotReady, UnknownView, InvalidRequest, RowTooLarge, CursorInvalidated).
  ScanPage scanPage(int32_t viewTopicId, int32_t partition, std::span<const uint8_t> prefix,
                    std::span<const uint8_t> resumeToken, uint16_t limit, std::span<uint8_t> out);
  ```
  `getRow` (u64) answers `BadKey` for a view that is not `LegacyU64` with `keyWidth == 8` (P7-10). `ViewFoldCounters` appends `skippedNoKey`, `keyConflicts`, `ownerMoves` (summed from `FoldStats` in `driveView_`), `scanRows`, `scanPages` (raised by `scanPage`).
- **Keyed instance.** `ViewInstance` appends `std::optional<rune::view::KeyedFoldSpec> keyed;` and `std::optional<rune::view::ViewTable> owners;`. `getOrCreate_` builds them for a `Keyed` projection (keyed aggregates: Task 11): `table` with `ViewTableKeys{keyWidth, prefixWidth, hashSeed_}`, `owners` (when owner-mapped) with `ViewTableKeys{wireKeyWidth, 0, hashSeed_}` and a `RowLayout` whose `rowLen` is `keyWidth`. Every table of every instance (aggregate aux tables included) takes `hashSeed_`. A `LegacyU64` instance is built exactly as today.
- **Checkpoints.** A `Keyed` persisted instance encodes one v3 record per pass (`kKeyedTableView`, then `kKeyedTableOwners` when present) from `drainDirtyKeyed`, with key bytes copied into a reused `keyScratch_` beside `rowScratch_` (`WorkerViewSet.h:750-752`). `maxRecordsPerPassFor` for `Keyed`: `fixed = kDeltaHeaderBytes + kCheckpointCrcBytes + kCheckpointV3HeaderBytes + nTables × kCheckpointV3TableHeaderBytes`; per record (a move): one delete `5 + K`, one upsert `5 + K + rowLen`, and when owner-mapped one owner upsert `5 + W + K`. `replayInstance` and `logRecovered` take a third shape: `format=v3`. `splitViewCheckpointRecords` already knows v3 (Task 7).
- **Install refusal (P7-13).** `drainInstalls` refuses a plan with `unicodeVersion != 0 && !unicodeVersionSupported(unicodeVersion)` before `installView`.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesViewSet.KeyedInstanceFoldsAndServes`: install `account_characters` (Task 4 plan); `driveFolds` over three batches → `getRowK(view, 0, key(42, c2))` returns the row, `RowStatus::Ok`; `getRow(view, 0, 42)` → `nullptr`, `BadKey`.
  - `CheckedWritesViewSet.LegacyViewServesBothWays`: a `key characterId` (u64) view: `getRow(42)` and `getRowK(encode(42))` return the same pointer; a narrow `u16` view: `getRowK` works, `getRow` → `BadKey`.
  - `CheckedWritesViewSet.ScanPageFillsAndResumes`: 300 rows under user 42 → `scanPage(limit 100)` gives 100 rows, `nextTokenLen == 16`; two more pages, the last with `nextTokenLen == 0`; `counters.scanRows == 300`, `scanPages == 3`.
  - `CheckedWritesViewSet.ScanPageRespectsTheByteBudget`: a view whose payload is 4,000 bytes (a wide plan with many `FixedStr(255)` columns) → a page holds `61440 / 4004 = 15` rows.
  - `CheckedWritesViewSet.ScanStatuses`: wrong prefix length → `kInvalidRequest`; a `LegacyU64` view → `kInvalidRequest`; a token of the wrong length → `kInvalidRequest`; an erased token key → `kCursorInvalidated` (8); not Ready → `kNotReady`; unknown view → `kUnknownView`.
  - `CheckedWritesViewSetRegression.FixedSeedHookReachesEveryTableRegression` (§15 forced collision): `WorkerViewSet(ch, dir, 7)` → `hashSeedForTest() == 7`, and the instance's view and owner tables report seed 7 (a test accessor on `ViewInstance`).
  - `CheckedWritesViewSetRegression.UnsupportedUnicodePlanIsNotInstalledRegression` (§15 / P7-13): an install frame whose compiled plan carries `unicodeVersion = packUnicodeVersion(17,0,0)` → `installRefusals() == 1`, `viewCount() == 0`, `getRowK` → `nullptr` with no layout.
  - `CheckedWritesViewSet.MaxRecordsPerPassBoundsAV3Delta`: for K = 128 and `rowLen` 300 on a 64 KiB delta ring, `maxRecordsPerPass ≥ 1`, and the worst-case pass (every record a move) encodes a delta that fits one ring message.
  - `CheckedWritesKeyedRecovery.V3CheckpointRestoresRowsOwnersAndPrefixes` (`CheckedWritesKeyedRecoveryTest.cpp`): fold 1,000 records with renames and deletes, push every delta, write them to the instance's file as the IO agent would, then `installViewForRecovery` on a fresh set → table, owner map and every `scanPage` equal the original's; the log line `VIEW-RECOVERED view=<id> partition=0 O=<o> records=<n> format=v3`.
  - `CheckedWritesKeyedRecoveryRegression.RenameJustBeforeRestartRecoversOneRowRegression` (Review Focus 4): the last pass renames c7 "Foo" → "Bar"; recover → exactly one row `("bar", c7)`, `owners[c7] == ("bar", c7)`, equal to a fresh refold.
  - `CheckedWritesKeyedRecoveryRegression.V3IsNeverDiscardedByTheAbortedRuleRegression` (R01): an aborted range below the checkpoint's O and `sourceLogStart = 0` → the v3 checkpoint is kept (`checkpointsDiscarded == 0`).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `no member named 'getRowK'`.
- [ ] **Step 3: Implement** per the Interfaces, plus the `BrokerNode` seed and `NodeConfig::viewHashSeedForTest` (env `RUNE_VIEW_HASH_SEED_FOR_TEST`, parsed like the other test knobs in `NodeConfig.cpp`).
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesViewSet|CheckedWritesKeyedRecovery|WorkerViewSet|ViewRecovery|ViewReadiness|ViewSink|CheckedWritesAbortedFold' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp broker/libs/node/include/rune/node/NodeConfig.h \
        broker/libs/node/src/NodeConfig.cpp broker/libs/node/src/BrokerNode.cpp broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp \
        broker/libs/node/tests/node/CheckedWritesKeyedRecoveryTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "feat(views): keyed view instances - owner tables, per-worker hash seed, v3 checkpoints and recovery, getRowK, scanPage" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Composite group keys for aggregate views (R07c)

**Files:**
- Modify: `broker/libs/view/include/rune/view/AggState.h` (`AggLayout` `:114-142`, `AggDirtyBound` `:164-170`), `broker/libs/view/src/AggState.cpp` (`buildAggLayout`, `aggDirtyBoundPerRecord`), `broker/libs/view/include/rune/view/AggFold.h`, `broker/libs/view/src/AggFold.cpp` (the group key derivation `:600-615`, `emit` `:438-445`, `dropGroup` `:432-436`; 04's `prepareAggregate`/`applyAggregate`)
- Modify: `broker/libs/node/include/rune/node/WorkerViewSet.h` (`AggInstance` `:112-124`), `broker/libs/node/src/WorkerViewSet.cpp` (`getOrCreate_`, `replayInstance`, `maxRecordsPerPassFor`, `buildAndPushCheckpoint_`, `scanPage`, `getRowK`)
- Test: `broker/libs/view/tests/view/CheckedWritesKeyedAggTest.cpp` (create; `rune_view_tests`), `broker/libs/node/tests/node/CheckedWritesKeyedRecoveryTest.cpp`, `broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp`

**Interfaces:**
- Consumes: Tasks 4–10 (`keyModeOf`, `KeyedFoldSpec`/`deriveViewKey`, `ViewTable` with `ViewKey`s and the prefix index, checkpoint v3 with table ids 2–5, `decodeKeyComponent`); C9.3 (`AggregateFold`, `prepareAggregate`, `applyAggregate`).
- Produces (contract addition 6a): `AggLayout::keyed`, `groupKeyOff` (the group row appends the K key bytes after its existing fields), `groupKeyWidth`; `AggInstance::groupIds` (`ViewTableKeys{K, 0, seed}`, payload `[gid u64]`) and `nextGroupId`; `AggregateFold::keyed`, `groupIds`, `nextGroupId`. `buildKeyedFoldSpec` (Task 8) accepts aggregate plans (its `ownerMapped` is false).
- **The keyed rules** (P7-8; null `keyed` = the `LegacyU64` path, untouched):
  - **Group of a live record:** `deriveViewKey` of the group components → `Absent` is today's `skippedNoGroup` path (retract, add nothing); `Ok` → G. `groupIds.get(G)` gives the gid; absent means a new group, whose gid is `(*nextGroupId)++`, assigned in apply together with the `groupIds` entry and the group row's key bytes. Everything downstream (contributor row `[groupKey u64]`, links, accumulators, value blocks, `blockIdCandidate(gid)`) uses the gid exactly as it uses today's group value.
  - **`emit(gid)`:** the view row is `view.put(ViewKey from the group row's key bytes)`; the key columns are written with `decodeKeyComponent` (a `kDerivedKeyField` column gets the lowered bytes); the aggregate columns as today.
  - **`dropGroup(gid)`:** erase the view row by its key, the group row and the `groupIds` entry.
  - **Tombstones and moves** find the old group by the contributor row's gid, as today.
  - **Checkpoint v3** of a keyed aggregate: tables 0 (view, key K), 2 (contributors, key 8), 3 (groups, key 8), 4 (blocks, key 8, only with Min/Max) and 5 (`groupIds`, key K), in one record per pass, so all of them are restored at the same O. After a replay `nextGroupId` = 1 + the largest gid in `groupIds` (one cold scan).
  - **`maxRecordsPerPassFor`:** `aggDirtyBoundPerRecord` gains `groupIdRows` (2: a group created and a group dropped by one move); `perRecord` sums every table with `checkpointV3EntryHeaderBytes(keyLen)` instead of `kCheckpointEntryHeaderBytes`.
  - **Reads:** `getRowK` and `scanPage` serve a keyed aggregate's view table like a projection's (prefix reads when n ≥ 2).

- [ ] **Step 1: Write the failing tests** (`CheckedWritesKeyedAggTest.cpp`; the `device` source of Task 4; `device_history` = `key (userId, deviceFp) group by (userId, deviceFp)` with `count(*) as uses, sum(bytes) as total, min(lastSeen) as firstSeen, max(lastSeen) as lastSeen, last(platform) as platform`):
  - `CheckedWritesKeyedAgg.TwoColumnGroupsAggregate`: events (u1, "fpA") ×3, (u1, "fpB") ×1, (u2, "fpA") ×2 → three view rows; `(u1, "fpA")` has `uses 3`, the right `total`, `firstSeen`, `lastSeen` and `platform`; `scanPrefix(u1)` returns exactly the two u1 rows.
  - `CheckedWritesKeyedAgg.UpdateMovesAContributorBetweenGroups`: an event re-written from (u1, "fpA") to (u1, "fpB") → `uses` 2 and 2, the Min/Max values recomputed (the value-block rescan path).
  - `CheckedWritesKeyedAgg.TombstoneDropsTheLastContributorsGroup`: delete the only (u2, "fpZ") event → its view row, group row and `groupIds` entry are gone; re-creating it yields a row equal to a fresh one (a new gid).
  - `CheckedWritesKeyedAgg.LowerGroupKey`: `(userId, lower(deviceFp) as fpKey)`: "FpA" and "fpa" fold into one group; the `fpKey` column holds `"fpa"` + padding.
  - `CheckedWritesKeyedAgg.EnumAndNarrowGroupKeys`: `count(*) group by platform` (enum) and `group by` a `u16` column fold and read by their 4- and 2-byte keys.
  - `CheckedWritesKeyedAggRegression.GroupIdsAreInternalRegression`: the same log folded in two different batchings (one batch; one record per batch with random pass bounds) → identical view tables, although the gids differ.
  - `CheckedWritesKeyedAggRegression.SingleEightByteGroupIsByteIdenticalRegression` (R07c fast path): `count(*), sum(bytes) group by userId` folds through the `LegacyU64` path (`AggregateFold::keyed == nullptr`) and its tables and v2 checkpoint bytes equal those of the pre-change build (golden captured before Step 3); Part 04's three-way byte-identical test passes unchanged.
  - `CheckedWritesKeyedAggModelRegression.RandomLogsFoldToTheFreshRefold`: seeds **1–300**; the shapes `(u64, u64)`, `(u64, string(32))`, `(u64, lower(string(16)))`, `(enum)`; the rules `count(*)`, `count(col)`, `sum`, `avg`, `min`, `max`, `last`, `lastNonNull`; random inserts, group-moving updates and tombstones across 1–20 batches with random pass bounds → every view row equals a fresh refold of the whole log, and a v3 replay of the dirty stream equals both.
  - `CheckedWritesViewSet.KeyedAggregateServesPrefixReads` (`CheckedWritesViewSetTest.cpp`): `device_history` installed; 12 devices of user 42 → `scanPage(prefix 42)` returns 12 rows in one page; `getRowK((42, "fpA"))` returns its row.
  - `CheckedWritesKeyedRecoveryRegression.KeyedAggregateRestartEqualsRefoldRegression` (`CheckedWritesKeyedRecoveryTest.cpp`): fold 2,000 events with moves and tombstones, recover from the v3 file → the view, contributors, groups, blocks and `groupIds` tables equal the originals; 500 more events after the restart give the same view as a fresh refold of all 2,500 (so `nextGroupId` never collides).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_view_tests rune_checked_writes_tests`. Expected: `no member named 'groupIds' in 'rune::view::AggregateFold'`.
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesKeyedAgg|CheckedWritesViewSet|CheckedWritesKeyedRecovery|AggFold|AggRetractable|AggState|WorkerViewSet|CheckedWritesAbortedFold' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/view/include/rune/view/AggState.h broker/libs/view/src/AggState.cpp broker/libs/view/include/rune/view/AggFold.h \
        broker/libs/view/src/AggFold.cpp broker/libs/node/include/rune/node/WorkerViewSet.h broker/libs/node/src/WorkerViewSet.cpp \
        broker/libs/view/tests/view/CheckedWritesKeyedAggTest.cpp broker/libs/node/tests/node/CheckedWritesKeyedRecoveryTest.cpp \
        broker/libs/node/tests/node/CheckedWritesViewSetTest.cpp broker/libs/view/CMakeLists.txt
git commit -m "feat(views): composite group by - ViewKey group keys interned to dense ids, v3 checkpoints, prefix reads; single-int groups unchanged" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: The AdminWorker accepts plan v2 (gates, error kinds, `viewObjectId`, the partition rule)

**Files:**
- Modify: `broker/libs/node/src/AdminWorker.cpp` (`viewDef_` `:1211-1265`, `buildCreateView_` `:1267-1283`, `buildAlterView_` `:1285-1300`), `broker/libs/node/include/rune/node/AdminWorker.h` (the `viewDef_` declaration gains `uint64_t viewObjectId`)
- Test: `broker/libs/node/tests/node/CheckedWritesAdminViewDdlTest.cpp` (create; `rune_checked_writes_tests`), modeled on the CreateView cases of `broker/libs/node/tests/integration/CreateViewRegressionTest.cpp`

**Interfaces:**
- Consumes: `needsV2(ViewPlan)` (Task 3), `CompileErrorKind` and `ownerMappedOf`/`keyModeOf` (Task 4); 02's `clusterFeatureLevel_` and `MetadataFeature::kCheckedWrites`.
- Produces, in `viewDef_`, in this order (each refusal `Rejected(reject(code, message))`):
  0. Part 02's view-source refusals (02 P6) run first and are unchanged: an untyped source → 33, "view source must be a typed topic"; a keyless typed source → `ViewKeyInvalid` (35), "view source needs a key" (R10b). So a projection over a keyless typed topic never reaches the compile and never answers 56;
  1. the authored blob decodes (else 34, as today, `:1217`);
  2. `needsV2(*authored) && clusterFeatureLevel_ < MetadataFeature::kCheckedWrites` → `InvalidRequest` (39), "plan v2 needs the checked-writes feature level";
  3. `authored->unique` → `ViewUniqueInvalid` (58), "unique views are not available yet"; non-empty `constraints` → `ViewConstraintInvalid` (57), "check constraints are not available yet" (P7-9; Part 09 removes both lines);
  4. compile; a non-Generic `CompileError::kind` maps per P7-14 with the compiler's message as the detail; Generic keeps the message search (`:1241-1244`) until Part 08;
  5. `ownerMappedOf(plan)` (every composite, derived or non-source-key view) on a source with more than one partition → `ViewNotCoPartitioned` (36), "a view keyed on anything but the source key needs a source with one partition" (§7.4; the aggregate rule `:1248` stays);
  6. `compiled->plan.viewObjectId = viewObjectId` (`s.nextObjectId()` for create, `v->objectId` for alter), before `rv::serialize`.
- `ViewDef.keyField` is whatever the client sent (the client sends `keyDisplayName`, Task 15).

- [ ] **Step 1: Write the failing tests** (a real `AdminWorker` with the in-memory catalog, as `CreateViewRegressionTest.cpp` sets it up; the level raised to 4 with `setClusterFeatureLevel(4)` unless stated):
  - `CheckedWritesAdminViewDdl.CompositeViewIsCreatedWithItsObjectId`: `account_characters` over a 1-partition `character` → OK; the event's `compiledPlanBlob` decodes to `keyComponents.size() == 2`, `viewObjectId ==` the event's `objectId`.
  - `CheckedWritesAdminViewDdl.LegacyPlanBytesAreUnchanged`: a v1 plan → the stored `normalizedPlanBlob` and `compiledPlanBlob` are byte-identical to a pre-change capture (golden in the test).
  - `CheckedWritesAdminViewDdl.BelowTheLevelPlanV2IsInvalidRequest`: level 3 → 39 with the message; the v1 plan still creates.
  - `CheckedWritesAdminViewDdl.UniqueAndCheckAreNotAvailableYet`: `unique` → 58, a constraint → 57, messages as above.
  - `CheckedWritesAdminViewDdl.CompileKindsMapToTheirCodes`: K = 130 → 56; identity rule → 56; `lower(gender)` → 56; an unknown key column → 34; a `groupBy` that differs from the key → 35 with the compiler's detail; a keyed aggregate over a `string(16)`-keyed source → 56.
  - `CheckedWritesAdminViewDdl.UnknownUnicodeVersionIsViewKeyExpressionInvalid` (§15): `unicodeVersion` 17.0.0 → 56, detail "lower() is pinned to Unicode 17.0.0; this broker has 16.0.0".
  - `CheckedWritesAdminViewDdl.KeyedViewsNeedOnePartition`: `account_characters` over a 3-partition source → 36; a source-keyed `u16` view over the same source → OK (P7-10).
  - `CheckedWritesAdminViewDdlRegression.KeylessSourceIsRefusedBeforeTheCompileRegression` (R10b): a composite `key (a, b)` projection and a single-column projection over a keyless typed topic, each as a v2 plan at level 4 → 35 "view source needs a key", never 56 (the identity rule) or 34.
  - `CheckedWritesAdminViewDdl.AlterViewKeepsTheObjectId`: `alter view account_characters` → the new compiled plan's `viewObjectId` equals the original object id.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: assertion failures (the v2 plan is refused with 34/35 by the old path).
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesAdminViewDdl|CreateView|AdminWorker|Catalog' -j14 --output-on-failure` and `ctest --test-dir build/sub-codec -R CatalogGolden --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/src/AdminWorker.cpp broker/libs/node/include/rune/node/AdminWorker.h \
        broker/libs/node/tests/node/CheckedWritesAdminViewDdlTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "feat(admin): CreateView/AlterView accept plan v2 - level gate, 56/57/58 by kind, viewObjectId, one-partition rule for keyed views" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: The wire: `GetViewRowK` (81), `ScanViewRows` (82), `ScanViewRowsResponse` (83)

**Files:**
- Modify: `protocol/include/rune/protocol/client/Protocol.h:89-102` (`GetViewRowK = 81`, `ScanViewRows = 82`, `ScanViewRowsResponse = 83`), `protocol/include/rune/protocol/client/DataMessages.h` (after `ViewRowResponse`, `:795-879`; `Status` gains `kCursorInvalidated = 8`), `protocol/include/rune/protocol/client/Dispatch.h:53-71` (`isKnownTemplate` covers 81–83; the range comment)
- Modify: `protocol/conformance/gen_vectors.cpp`, `protocol/conformance/manifest.md`, new `protocol/conformance/vectors/{get_view_row_k,scan_view_rows,scan_view_rows_response}.bin`
- Test: `protocol/tests/protocol/client/CheckedWritesViewReadMessagesTest.cpp` (create; add to `rune_protocol_tests`, `protocol/CMakeLists.txt:22`)

**Interfaces:**
- Produces (`namespace rune::protocol::client`; little-endian; var fields use the existing `putVarBytes32` / `getVarBytes32`):
  ```
  GetViewRowK (81)  C→B   BODY_FIXED = 24
    [24..27] viewTopicId i32   [28..31] partition i32 (-1 = the sink's only partition)
    [32..39] requestedFingerprint u64 (0 = latest)   [40..47] minOffset i64 (-1 = none)
    var: key (u32 length 1..128, then the key bytes)                       total = 52 + keyLen
  ScanViewRows (82)  C→B   BODY_FIXED = 24
    [24..27] viewTopicId i32   [28..29] limit u16 (0 = 256)   [30..31] reserved u16 = 0
    [32..39] requestedFingerprint u64   [40..47] minOffset i64
    var: prefix (u32 length 1..127)   var: resumeToken (u32 length 0 = from the start, else 1..128)
  ScanViewRowsResponse (83)  B→C   BODY_FIXED = 4
    [24..25] status u16 (ViewRowResponse::Status)   [26..27] rowCount u16
    var: nextToken (u32 length; 0 = the scan is complete)
    var: rows (u32 length; rowCount × {[len u32][len bytes]})
  ```
  Structs `GetViewRowK`, `ScanViewRows`, `ScanViewRowsResponse` with `TEMPLATE_ID`, `BODY_FIXED`, `encodedSize()`, `encode(uint8_t*)`, `static std::optional<T> decode(const uint8_t*, std::size_t)`; the response decode is zero-copy (`nextToken`, `rowsBytes` spans) and offers `template <class Fn> bool forEachRow(Fn&&) const noexcept` (false on a malformed rows region). Constants: `ScanViewRows::kMaxRowsPerPage = 256`, `ScanViewRowsResponse::kMaxPageBytes = 61440`, `static_assert(MessageHeader::ENCODED_SIZE + 4 + 4 + 128 + 4 + kMaxPageBytes <= 68 * 1024)`. Decoders refuse a key of length 0 or > 128, a prefix of length 0 or > 127, a token > 128, a non-zero reserved field, and trailing bytes.
- Every one of 81/82 is answered by a v12 client only; nothing older ever sends them.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesViewReadMessages.GetViewRowKRoundTrip`: keys of 1, 16, 74 and 128 bytes; every field; `encodedSize() == 52 + keyLen`.
  - `CheckedWritesViewReadMessages.ScanRequestRoundTrip` and `…ScanResponseRoundTrip` (3 rows of 32 bytes, a 16-byte token; and an empty page with no token).
  - `CheckedWritesViewReadMessagesRegression.BoundsAreRefusedRegression`: key 0 / 129, prefix 0 / 128, token 129, reserved ≠ 0, every truncation, one trailing byte → nullopt; a rows region whose last row length overruns → `forEachRow` false.
  - `CheckedWritesViewReadMessages.KnownTemplates`: `isKnownTemplate(81)`, `(82)`, `(83)` true; `(84)` stays as Part 08 leaves it (false until then).
  - `CheckedWritesViewReadMessages.ConformanceVectors`: the three `.bin` files decode to the manifest's field values and re-encode byte for byte.
  - `CheckedWritesViewReadMessages.CursorInvalidatedIsEight`: `ViewRowResponse::kCursorInvalidated == 8`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: `no member named 'GetViewRowK' in 'TemplateId'`.
- [ ] **Step 3: Implement** the messages; regenerate the vectors with `gen_vectors` (as the manifest's "Regenerate" note says) and add their manifest entries.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesViewReadMessages|Conformance|DataMessages|Dispatch' --output-on-failure`; the client's conformance suite `ctest --test-dir build/sub-client-cpp -R Conformance --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add protocol/include/rune/protocol/client/Protocol.h protocol/include/rune/protocol/client/DataMessages.h \
        protocol/include/rune/protocol/client/Dispatch.h protocol/conformance/gen_vectors.cpp protocol/conformance/manifest.md \
        protocol/conformance/vectors/get_view_row_k.bin protocol/conformance/vectors/scan_view_rows.bin \
        protocol/conformance/vectors/scan_view_rows_response.bin protocol/tests/protocol/client/CheckedWritesViewReadMessagesTest.cpp \
        protocol/CMakeLists.txt
git commit -m "feat(protocol): GetViewRowK 81, ScanViewRows 82, ScanViewRowsResponse 83, status CursorInvalidated 8, vectors" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: Serving the reads: worker ops, held reads with `minOffset`, the gateway

**Files:**
- Modify: `protocol/schema/rune-internal-ring.xml` (`OpType` `GET_VIEW_ROW_K = 29`, `SCAN_VIEW_ROWS = 30` after `:113`; `TailKind VIEW_READ = 13` after `:142`; `ReturnType VIEW_SCAN_RESULT = 9` after `:176`) and the regenerated `protocol/include/rune_protocol_internal_sbe/*`
- Modify: `broker/libs/node/include/rune/node/WorkerOp.h` (`Type` `:66-158`: `GetViewRowK = 29`, `ScanViewRows = 30`; `std::span<const uint8_t> viewReadTail;`; `WorkerReturn::Type::ViewScanResult = 9` after `:537`), `broker/libs/node/include/rune/node/WorkerReturnCodec.h` (the new return type; the `ProtocolRegression.WorkerReturnLayoutUnchanged` pin stays green)
- Modify: `broker/libs/node/include/rune/node/Worker.h`: the `enqueueOp` tail switch (`:1246-1265`, `VIEW_READ` from `viewReadTail`); the op dispatch (`:2568-2569`); `executeGetViewRowWire_` (`:3986-4007`); `ViewReadReq` (`:4010-4019`); `serveViewRead_` (`:4036-4139`); `serveHeldViewReads_` (`:4143-4158`); a scan sibling of `enqueueViewRowResult_` (`:4169-4189`); counters beside `getViewRows_` (`:5857`)
- Modify: `broker/libs/gateway/include/rune/gateway/ClientGateway.h` (`:1858` declarations, `:1299` test hooks), `broker/libs/gateway/src/ClientGateway.cpp` (the touch list `:1278`, the dispatch `:1384-1386`, the return encoder beside `ViewRowResult` `:6850-6889`, the handlers beside `handleGetViewRow` `:9259-9337`), `broker/libs/gateway/include/rune/gateway/RequestRejection.h:165-169`
- Test: `broker/libs/node/tests/node/CheckedWritesViewReadTest.cpp`, `broker/libs/node/tests/node/CheckedWritesViewReadGatewayTest.cpp` (create; `rune_checked_writes_tests`; the gateway cases modeled on `ViewSinkRegressionTest.cpp`'s `applyGetViewRowForTest` use)

**Interfaces:**
- Consumes: `getRowK`, `scanPage` (Task 10); the messages (Task 13).
- Produces:
  - **The tail** `VIEW_READ`: `[keyOrPrefixLen u8][bytes][tokenLen u8][token bytes]` (point reads carry `tokenLen = 0`). Scalars: `topicId = viewTopicId`, `partition`, `sessionId`, `correlationId`, `hwBound = requestedFingerprint`, `nextOffset = minOffset`, `recordCount = limit`. `viewReadTail` is non-owning and read only by `enqueueOp` within the gateway's call (`WorkerPool::enqueueOp` encodes synchronously, `WorkerPool.h:499-530`), so the gateway allocates nothing.
  - **`ViewReadReq`** appends `enum class Kind : uint8_t { PointU64, PointKey, Scan } kind = Kind::PointU64; std::array<uint8_t, 128> key{}; uint8_t keyLen = 0; std::array<uint8_t, 128> token{}; uint8_t tokenLen = 0; uint16_t limit = 0; int64_t minOffset = -1;` (fixed arrays: a held read never allocates; `kMaxHeldViewReads` stays 256).
  - **`serveViewRead_`** keeps checks 1–2d as they are, then: the barrier target becomes `max(target, minOffset + 1)` whenever `minOffset ≥ 0`, also when `foldCursor ≥ hw` (P7-12); step 3 switches on `kind` — `PointU64` calls `getRow` (`BadKey` → `kInvalidRequest`), `PointKey` calls `getRowK` (`BadKey` → `kInvalidRequest`), `Scan` calls `scanPage` into a worker-owned `std::array<uint8_t, ScanViewRowsResponse::kMaxPageBytes> scanScratch_` and returns `ViewScanResult` with `errorCode = status`, `recordCount = rows`, the chunk = `[nextTokenLen u32][token][rowsLen u32][rows]` gathered into the return ring like `enqueueViewRowResult_` (back-pressure fallback copies, as today).
  - **Gateway.** `handleGetViewRowK` and `handleScanViewRows` follow `handleGetViewRow` step for step (decode → `kInvalidRequest`; unknown session; READ on the view's topic id; `viewRowGate_`; `viewSourceLedElsewhere_` redirect; route by the source topic; `kBusy` on a full ring), plus:
    - a session below `kClientVersionCheckedWrites` → `kInvalidRequest`;
    - routing: sink partitions = 1 → partition 0 (`GetViewRowK.partition` must be −1 or 0); more → `GetViewRowK.partition` in `[0, P)`, else `kInvalidRequest`; a scan always partition 0 (keyed views have one partition, Task 12);
    - `ViewScanResult` → `ScanViewRowsResponse` on the session egress (the chunk is the two var fields verbatim; a frame over `kScratchSize` → `kRowTooLarge`, as `:6874-6884`); `kViewRowNotLeaderInternal` → the route refusal, as `:6861-6866`.
  - `RequestRejection.h`: `GetViewRowK` answers `ViewRowResponse`, `ScanViewRows` answers `ScanViewRowsResponse` (status only).
  - Counters: `getViewRowKs_`, `scanViewRows_` beside `getViewRows_`.

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesViewReadTest.cpp` (a real `Worker` + `WorkerViewSet` + disk log, the `GetViewRowRegressionTest.cpp` rig):
    - `CheckedWritesViewRead.PointKeyReadServesTheRow`: `account_characters`; 3 committed batches → a `GetViewRowK` op for `(42, c2)` returns `ViewRowResult` OK with the row bytes.
    - `CheckedWritesViewRead.ScanServesOnePageInOneStep`: 12 characters of user 42 → one `ScanViewRows` op (limit 0) → one `ViewScanResult`, 12 rows, no token, in the same `doWork` pass.
    - `CheckedWritesViewRead.EmptyPrefixPageIsOkWithNoRowsAndNoToken` (§15).
    - `CheckedWritesViewReadRegression.ErasedTokenRowAnswersCursorInvalidatedRegression` (§15): page 1 (limit 5) of 12 → token = row 5's key; tombstone that character; page 2 → `kCursorInvalidated`.
    - `CheckedWritesViewReadRegression.ScanNeverCrossesIntoAnotherPrefixRegression` (Review Focus 5): users 42 and 43 with 12 characters each, the view set's `setHashMaskForTest(0)` called before the view is installed (every key and prefix collides) → every page of 42 holds only 42's rows.
    - `CheckedWritesViewReadRegression.MinOffsetHoldsTheReadUntilFoldedRegression` (P7-12): append at offset 9 uncommitted; a `GetViewRowK` with `minOffset 9` is held (`heldViewReadsForTest() == 1`); commit and fold → served OK with the row; a second read with `minOffset 50` (never written) → `kNotReady` once the pass clock passes 100 ms, never earlier.
    - `CheckedWritesViewReadRegression.GetViewRowOnAKeyedViewIsInvalidRequestRegression` (Review Focus 2): 72 on `account_characters`, on a `lower` view and on a narrow `u16` view → `kInvalidRequest` (5); 72 on a u64 legacy view → OK as today.
    - `CheckedWritesViewReadRegression.WrongKeyLengthIsInvalidRequestRegression` (Review Focus 2): `GetViewRowK` with 15 bytes on K = 16 → 5.
    - `CheckedWritesViewReadRegression.HeldScanAllocatesNothingRegression`: 256 held scans under `rune::testing::AllocWindow` → 0 allocations; the 257th → `kNotReady` at once.
  - `CheckedWritesViewReadGatewayTest.cpp` (a real `ClientGateway` + `WorkerPool`, `applyGetViewRowKForTest` / `applyScanViewRowsForTest`):
    - `CheckedWritesViewReadGateway.RoutesToTheSourceOwner`: a 1-partition sink → the op lands on the source owner's worker with partition 0.
    - `CheckedWritesViewReadGatewayRegression.MultiPartitionSinkNeedsThePartitionRegression` (Review Focus 2): a 4-partition source-keyed `u16` view: `partition 2` → routed to (source, 2); `partition -1` and `4` → `kInvalidRequest`; a 1-partition sink with `partition 1` → `kInvalidRequest`.
    - `CheckedWritesViewReadGateway.OldSessionIsInvalidRequest`: a session announced with client version 11 → 81 and 82 answered `kInvalidRequest`.
    - `CheckedWritesViewReadGateway.NotAuthorizedLikeGetViewRow`: no READ on the view → `kNotAuthorized` (45) for both.
    - `CheckedWritesViewReadGateway.ScanResultEncodesTheFrame`: a `ViewScanResult` return → one `ScanViewRowsResponse` whose `forEachRow` yields the rows and whose token matches.
    - `CheckedWritesViewReadGateway.GatewayAllocatesNothingPerRead`: 1,000 `GetViewRowK` and 1,000 scans under `AllocWindow` → 0 (P7: zero-alloc query path).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `no member named 'GetViewRowK' in 'rune::node::WorkerOp::Type'`.
- [ ] **Step 3: Implement.** Regenerate the SBE flyweights (`java -jar "$SBE_JAR" …` as the header of `rune-internal-ring.xml` documents); then the worker and gateway changes per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesViewRead|GetViewRow|ViewSink|ViewReadiness|ProtocolRegression|WorkerReturn|RequestRejection' -j14 --output-on-failure`. Expected: PASS (`WorkerReturnLayoutUnchanged` unchanged).
- [ ] **Step 5: Commit.**
```bash
git add protocol/schema/rune-internal-ring.xml protocol/include/rune_protocol_internal_sbe broker/libs/node/include/rune/node/WorkerOp.h \
        broker/libs/node/include/rune/node/WorkerReturnCodec.h broker/libs/node/include/rune/node/Worker.h \
        broker/libs/gateway/include/rune/gateway/ClientGateway.h broker/libs/gateway/src/ClientGateway.cpp \
        broker/libs/gateway/include/rune/gateway/RequestRejection.h broker/libs/node/tests/node/CheckedWritesViewReadTest.cpp \
        broker/libs/node/tests/node/CheckedWritesViewReadGatewayTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "feat(views): serve GetViewRowK and ScanViewRows through the GetViewRow pipeline - minOffset barrier, one-step pages, no allocation" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: The client: read API, `ViewKeyBuilder`, the SD11 gate

**Files:**
- Modify: `client-cpp/include/rune/client/Completion.h:51` (`GetViewRowK`, `ScanViewRows` after `Seek`), `client-cpp/include/rune/client/Completions.h:155-160` (both reserve `cfg_.maxResultBytes`), `client-cpp/include/rune/client/Client.h` (`ViewRowResult` `:195-215` gains `kCursorInvalidated = 8`; the new types and methods beside `getViewRow` `:820` and `getViewRowAndWait` `:941`), `client-cpp/src/Client.cpp` (beside `getViewRow` `:1579-1591`, `failDdl_` `:1898-1906` maps routing codes for both new ops like `GetViewRow`, `getViewRowAndWait` `:2829-2845`, the `ViewRowResponse` dispatch `:4334-4356` and a new `ScanViewRowsResponse` case)
- Create: `client-cpp/include/rune/client/view/ViewKeyBuilder.h`, `client-cpp/src/view/ViewKeyBuilder.cpp` (add to `rune_client` in `client-cpp/CMakeLists.txt`; it links `rune_codec` and `rune_catalog`)
- Modify: `client-cpp/include/rune/client/view/ViewRowCodec.h` (`decodeScanPayload`)
- Test: `client-cpp/tests/client/CheckedWritesViewKeyBuilderTest.cpp` (add to `rune_schema_codec_smoke_tests`, `client-cpp/CMakeLists.txt:1073-1081`, and link `rune_catalog` there)

**Interfaces:**
- Consumes: `encodeKeyComponent`, `lowerUtf8` (Tasks 1–2), the authored plan (Task 3), the messages (Task 13); `rune::protocol::partitionFor` (C13, Part 06, merged before this part per R-ORD).
- Produces:
  ```cpp
  // Client.h
  struct ViewReadOptions { uint64_t requestedFingerprint = 0; int64_t minOffset = -1; int32_t partition = -1; };
  struct ViewScanOptions { std::vector<uint8_t> resumeToken; uint64_t requestedFingerprint = 0; int64_t minOffset = -1; };
  struct ViewScanResult { uint16_t status = 0; std::vector<std::vector<uint8_t>> rows; std::vector<uint8_t> nextToken;
                          [[nodiscard]] bool complete() const noexcept { return status == ViewRowResult::kOk && nextToken.empty(); } };
  OpResult getViewRowK(int32_t viewTopicId, std::span<const uint8_t> key, const ViewReadOptions& opts, CompletionTarget, Token);
  OpResult scanView(int32_t viewTopicId, std::span<const uint8_t> prefix, uint16_t limit, const ViewScanOptions& opts, CompletionTarget, Token);
  ViewRowResult  getViewRowAndWait(int32_t viewTopicId, std::span<const uint8_t> key, const ViewReadOptions& opts = {},
                                   std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
  ViewScanResult scanViewAndWait(int32_t viewTopicId, std::span<const uint8_t> prefix, uint16_t limit,
                                 const ViewScanOptions& opts = {}, std::chrono::nanoseconds timeout = kDefaultWaitTimeout);
  // ViewKeyBuilder.h (namespace rune::client::view)
  class ViewKeyBuilder {
  public:
      struct Component { std::string column; std::string outName; rune::codec::PrimType type; uint16_t srcWidth; uint16_t width; uint8_t keyFunction; };
      static std::expected<ViewKeyBuilder, std::string> fromPlan(const rune::view::ViewPlan& authored, const rune::codec::Descriptor& sourceDescriptor,
                                                                 int32_t sourcePartitions = 1, rune::protocol::Partitioner p = {});
      static std::expected<ViewKeyBuilder, std::string> fromCatalog(const rune::catalog::CatalogState& s, std::string_view viewName);
      ViewKeyBuilder& setInt(std::string_view column, int64_t v);         // any integer component; range-checked in key()
      ViewKeyBuilder& setUInt(std::string_view column, uint64_t v);
      ViewKeyBuilder& setBool(std::string_view column, bool v);
      ViewKeyBuilder& setString(std::string_view column, std::string_view utf8);   // string(N); lower() applied here
      [[nodiscard]] std::expected<std::vector<uint8_t>, std::string> key() const;      // every component set
      [[nodiscard]] std::expected<std::vector<uint8_t>, std::string> prefix() const;   // the first n-1 set, the last not
      [[nodiscard]] int32_t partition() const noexcept;   // -1 for a 1-partition source; else the producer's routing of the wire key
      [[nodiscard]] uint16_t keyWidth() const noexcept;
      [[nodiscard]] std::span<const Component> components() const noexcept;
      void clear() noexcept;
  };
  // ViewRowCodec.h
  [[nodiscard]] bool decodeScanPayload(std::span<const uint8_t> payload, ViewScanResult& out);
  ```
- `set*` accepts the source column name or the component's `outName`. `key()` refuses (with a message) an unset component, a value out of the column's range, a string longer than N bytes or invalid UTF-8. `fromCatalog` decodes the view's `normalizedPlanBlob` and its first source's `descriptorBlob`, partition count and `keyFeatures.partitioner` from the state. `partition()` encodes the single source-keyed component as the topic's wire key (little-endian at its width, or the N string bytes; §4) and routes it with `partitionFor`.
- **SD11:** both ops fail locally with `OpStatus::BrokerTooOld` when the seed's `brokerVersion` is known and below 7 (P3-4's source). The polled forms complete `OpKind::GetViewRowK` with `payload` = the row and `OpKind::ScanViewRows` with `payload` = the response's two var fields verbatim (`decodeScanPayload`); a result over the reservation is `ResultTooLarge`, as today. `scanViewAndWait` uses a 64 KiB reservation (a full page's payload is ≤ 4 + 128 + 4 + 61,440 = 61,576 bytes).
- `rune_client` passes `keyDisplayName(plan)` as `keyField` in `createView`/`alterView` when the plan is v2 (Task 12).

- [ ] **Step 1: Write the failing tests** (`CheckedWritesViewKeyBuilderTest.cpp`):
  - `CheckedWritesViewKeyBuilder.CompositeKeyAndPrefix`: `account_characters`: `setUInt("userId", 42).setUInt("characterId", 7)` → `key()` = 16 bytes `00…2A 00…07`; with only `userId` set → `prefix()` = 8 bytes, `key()` error "characterId is not set".
  - `CheckedWritesViewKeyBuilder.LowerMatchesTheBrokerFoldKey` (§15): `(lower(name) as nameKey, characterId)` with `setString("name", "FoO").setUInt("characterId", 7)` → exactly the 32-byte golden `66 6F 6F` + 21 × `00` + `00 00 00 00 00 00 00 07`, the same golden `CheckedWritesKeyedFold.DerivesTheLoweredKey` (Task 8) pins for the broker's `deriveViewKey`; `setString("nameKey", "FOO")` gives it too. Both sides call `rune::codec::encodeKeyComponent`; Task 17's e2e reads the broker's row with the builder's key.
  - `CheckedWritesViewKeyBuilder.RangeAndLengthAreChecked`: `setInt("provider", -1)` on `u16` → error; `setUInt` 70000 → error; a 17-byte name for `string(16)` → error; invalid UTF-8 → error.
  - `CheckedWritesViewKeyBuilder.PartitionFollowsTheProducer`: a 4-partition `u16`-keyed source: `partition()` of id 513 equals `rune::protocol::partitionFor(LE(513, 2 bytes), 4, Partitioner::Fnv1a)`, the function `TypedTopic` and the producer route with since Part 06; a 4-partition `Modulo` (autoincrement) `u64`-keyed source: `partition()` of id 4097 equals `partitionFor(LE8(4097), 4, Modulo) == 1`; a 1-partition source → −1.
  - `CheckedWritesViewKeyBuilder.FromCatalogFindsTheView`: a `CatalogState` built with `cat::testing` helpers holding `character` and `account_characters` → a builder with 2 components; an unknown name → error.
  - `CheckedWritesViewKeyBuilder.DecodeScanPayload`: a payload of 3 rows + a token → 3 rows and the token; a truncated payload → false.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_schema_codec_smoke_tests`. Expected: `ViewKeyBuilder.h: No such file or directory`.
- [ ] **Step 3: Implement** the builder, the decode helper and the `Client` methods per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesViewKeyBuilder|ViewPlanAuthor|Conformance' --output-on-failure` and `cmake --build build/root -j14 --target rune_client_broker_tests` (the embedded client compiles). Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add client-cpp/include/rune/client/Completion.h client-cpp/include/rune/client/Completions.h client-cpp/include/rune/client/Client.h \
        client-cpp/src/Client.cpp client-cpp/include/rune/client/view/ViewKeyBuilder.h client-cpp/src/view/ViewKeyBuilder.cpp \
        client-cpp/include/rune/client/view/ViewRowCodec.h client-cpp/tests/client/CheckedWritesViewKeyBuilderTest.cpp client-cpp/CMakeLists.txt
git commit -m "feat(client): getViewRowAndWait by key bytes, scanViewAndWait, ViewKeyBuilder (lower, composite, producer routing), BrokerTooOld gate" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 16: The key index takes 16-byte keys (SD8)

**Files:**
- Modify: `broker/libs/node/src/VerdictSource.cpp` (05's `VerdictViewKind::KeyIndex` member: its `ViewTable` and probes), the AdminWorker refusal (locate: `grep -rn "key.index needs a key of at most 8 bytes" broker/libs/node`)
- Test: `broker/libs/node/tests/node/CheckedWritesKeyIndexWideTest.cpp` (create; `rune_checked_writes_tests`), using the rig's `createKeyIndex()` (05); one failover case in `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp`

**Interfaces:**
- Consumes: `ViewTable(…, ViewTableKeys{.keyWidth = spec.keyWidth, .hashSeed = seed})`, `ViewKey::fromBytes` (Task 5); `TopicWriteSpec::keyWidth` (02); the worker's `WorkerViewSet::hashSeedForTest()` value as the key index seed.
- Produces: the key index keyed by the **wire key bytes** at the topic's key width (2–16 bytes); `fromU64` disappears from its probes. The key index is not checkpointed (R05c); each leadership rebuilds it from the local log with wire-key `ViewKey`s. The DDL line refusing keys wider than 8 bytes is deleted; the refusal test 05 wrote for it is inverted (recorded in the commit body).

- [ ] **Step 1: Write the failing tests:**
  - `CheckedWritesKeyIndexWide.SixteenByteStringKeys`: `tag {name string(16), n u64} key name with (key.index = on)` at level 4 → DDL OK; `insert "alice"` → OK; `insert "alice"` again → 52 `KeyExists`; `update "bob"` → 53; `removeExisting "alice"` → OK, then `update "alice"` → 53.
  - `CheckedWritesKeyIndexWide.EveryKeyWidth`: `u16`, `u32`, `u64`, `i64`, `string(9)`, `string(16)` key topics with a key index: insert/insert/update/removeExisting give OK/52/OK/OK.
  - `CheckedWritesKeyIndexWide.KeysDifferingInTheLastByteAreDistinct`: two 16-byte names equal except byte 15 → both inserts OK.
  - `CheckedWritesBrokerRegression.WideKeyIndexSurvivesFailoverThreeNodesRegression` (in `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp`, `rune_client_broker_tests`): `ThreeNodes`, a `string(16)` key index; 100 inserts; crash the leader; on the new leader re-inserting any of the 100 → 52, a new name → OK.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesKeyIndexWide|WideKeyIndexSurvivesFailover' --output-on-failure`. Expected: FAIL with DDL `InvalidRequest` "key.index needs a key of at most 8 bytes".
- [ ] **Step 3: Implement** per the Interfaces.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesKeyIndex|CheckedWritesIntent|CheckedWritesMatrix' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/src/VerdictSource.cpp broker/libs/node/src/AdminWorker.cpp broker/libs/node/tests/node/CheckedWritesKeyIndexWideTest.cpp \
        broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp broker/libs/node/CMakeLists.txt
git commit -m "feat(views): the key index takes wire keys up to 16 bytes (SD8 lifted)" \
           -m "Deviation: Part 05's 8-byte key.index refusal test inverted (the refusal is gone by design)." \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 17: The matrix and the real e2e

**Files:**
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (`createView(const rune::view::ViewPlan& planV2)` through the real AdminWorker path; `getViewRowK(view, key, minOffset)`, `scanAll(view, prefix, limit)` through worker ops), `broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h` (`Op` 60–68, `Entity` 60–68, `expect07` with the not-applicable table), `broker/libs/node/tests/support/CheckedWritesModel.h` (`refoldKeyedView(const CompiledViewPlan&, log) -> {rows, owners}`), `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp` (the `CheckedWritesMatrix07` instantiation and the pin row)
- Modify: `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` (`rune_client_broker_tests`, `broker/libs/node/CMakeLists.txt:1731`)

**Interfaces:**
- Consumes: everything above; 04's rig, model and `CheckedWritesMatrixCountRegression`. No new production code.
- Every wait is bounded (`waitFor(…, 10s)`, rig pass caps).

- [ ] **Step 1: Write the matrix** (Interaction matrix A): `CheckedWritesMatrix07/Cell.KeyedViewEqualsTheModel/<n>` over the 420 projection cells and `CheckedWritesMatrix07Agg/Cell.GroupByViewEqualsTheModel/<n>` over the 190 group-by cells, asserting (a)–(d); `CheckedWritesModel.h` gains `refoldKeyedAggregate`; `expect07`'s pin `EXPECT_EQ(applicable07, 610u)` in `CheckedWritesMatrixCountRegression.CellCountsArePinned`.
- [ ] **Step 2: Run it.** `cmake --build build/root -j14 --target rune_checked_writes_matrix_tests && ctest --test-dir build/root -R 'CheckedWritesMatrix07|CheckedWritesMatrix07Agg|CheckedWritesMatrixCount' -j14 --output-on-failure`. Expected: PASS (610 + 1).
- [ ] **Step 3: Write the real e2e tests** (`OneBroker(tag, Runtime::Shard)` unless stated; `character {characterId u64, userId u64, name string(16), gender u16, lastPlayed i64} key characterId partitions 1`; views created with `createViewAndWait` from v2 plans built in the test; writes `All | View`):
  - `CheckedWritesBrokerRegression.CompositeViewPointAndPrefixReadsRightAfterTheAckShardRegression` (§15): `account_characters`; three characters of user 42 and one of 43; right after each ack, `getViewRowAndWait(view, builder.key())` is OK with the sent `name`, and `scanViewAndWait(view, builder.prefix(42), 0)` returns exactly the acked rows of 42 (`complete()`).
  - `CheckedWritesBrokerRegression.LowerKeyReadWithTheKeyBuilderShardRegression` (§15): `(lower(name) as nameKey, characterId)`; write `name "FoO"` → `ViewKeyBuilder::fromCatalog(state, view).setString("name", "foo").setUInt("characterId", c)` reads the row; its `nameKey` column is `"foo"`.
  - `CheckedWritesBrokerRegression.RenameMovesTheRowShardRegression`: rename c7 → the old key reads `kAbsentKey`, the new one OK.
  - `CheckedWritesBrokerRegression.ScanAcrossPagesThenCursorInvalidatedShardRegression` (§15): 300 characters of user 42; pages of 100 → 100, 100, 100 rows then `complete()`, the union = all 300; a second scan deletes the page-1 token row before page 2 → `kCursorInvalidated` (8); restarting the scan with an empty token completes with 299 rows.
  - `CheckedWritesBrokerRegression.MinOffsetReadSeesTheWriteShardRegression`: a write acked with `All` (no `View`) returns offset o; `getViewRowAndWait(view, key, {.minOffset = o})` returns the row on the first attempt.
  - `CheckedWritesBrokerRegression.WideViewSurvivesRestartShardRegression`: 1,000 characters with renames; `restart()` → every row and scan identical; the broker log has `VIEW-RECOVERED view=<id> partition=0` with `format=v3`.
  - `CheckedWritesBrokerRegression.WideViewReadsAfterLeaderCrashThreeNodesRegression`: `ThreeNodes::start("cw07")`, `replicas 3`; 200 characters; `crash(leader)`; every point read and the user scans succeed on the new leader within the client's redirect budget.
  - `CheckedWritesBrokerRegression.MultiPartitionNarrowKeyViewReadsWithThePartitionShardRegression`: a 4-partition `u16`-keyed source and its source-keyed view; 64 records; every `getViewRowAndWait(view, key, {.partition = builder.partition()})` is OK; 72 on that view → `kInvalidRequest`.
  - `CheckedWritesBrokerRegression.OldGetViewRowOnALegacyViewUnchangedShardRegression`: a `key characterId` view; `getViewRowAndWait(view, 42, fp)` (72) and `getViewRowAndWait(view, builder.key(), {})` (81) return the same bytes.
  - `CheckedWritesBrokerRegression.DeviceHistoryGroupByPrefixReadShardRegression` (R07c): `device {eventId u64, userId u64, deviceFp string(32), lastSeen i64} key eventId partitions 1`, the view `device_history key (userId, deviceFp) as select count(*) as uses, max(lastSeen) as lastSeen from device group by userId, deviceFp`; 3 devices of user 42 with 5, 2, 1 events and 1 of user 43 → `scanViewAndWait(view, prefix(42), 0)` returns 3 rows with `uses` 5, 2, 1; after `restart()` the same.
  - `CheckedWritesBrokerRegression.KeyIndexOnASixteenByteKeyShardRegression` (SD8 end to end): a `string(16)` topic with `key.index = on`; `insert` twice → the second `WriteResult::code() == 52`.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesBrokerRegression' --output-on-failure`. Expected: PASS. Then `ctest --test-dir build/root -R 'rune_client_broker|ClientDdlBroker|ClientFailoverBroker' -j6 --output-on-failure`.
- [ ] **Step 5: Commit.**
```bash
git add broker/libs/node/tests/support/CheckedWritesRig.h broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h \
        broker/libs/node/tests/support/CheckedWritesModel.h broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp \
        broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp
git commit -m "test(views): the 07 matrix (610 cells) and real-broker e2e - composite, lower(), pages, restart, failover, routing" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 18: The allocation gate, the benches and the docs

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp` (03's binary)
- Modify: `broker/benchmark/ViewTableBench.cpp` (after `:262-266`), `broker/benchmark/ViewFoldBench.cpp` (after `:578-581`), `broker/benchmark/GetViewRowServeBench.cpp` (after `:220`)
- Modify: `docs/api/wire-protocol.md` (frame catalogue rows after `:102-103`; statuses `:121-122` gain 8; a section "Wide view keys and scans (client version 12)"), `docs/api/client-cpp.md` ("Materialized views", `:454-497`: key builder, point and prefix reads, pages and `kCursorInvalidated`, `minOffset`, `partition`), `docs/api/codec.md` (a section "View keys": `lower()` at Unicode 16.0.0 with its width table, the §7.3 encoding, the authored plan v2 and the version rule P7-7)

**Interfaces:**
- Consumes: Tasks 10–17.

- [ ] **Step 1: Write the alloc gate.** `CheckedWritesAllocRegression.KeyedFoldAndViewReadsAllocateNothingRegression`: the 03 rig with `account_characters` and a `lower` view, both pre-sized (`capacity 8192`); warm up 2,000 records with renames and 500 reads; then under `rune::testing::AllocWindow w;` 10,000 more records (upserts, renames, moves, deletes), 2,000 `GetViewRowK`, 1,000 scans (pages of 12) and 100 held reads → `EXPECT_EQ(w.count(), 0u)`.
- [ ] **Step 2: Run the gate.** `cmake --build build/root -j14 --target rune_checked_writes_alloc_regression_tests && ctest --test-dir build/root -R CheckedWritesAllocRegression --output-on-failure`. Expected: PASS.
- [ ] **Step 3: Write the benches** (`benchmark::RegisterBenchmark`, the files' existing fixtures):
  - `BM_ViewTableWideKey/{16,24,66,128}`: the `BM_ViewTableChurn` loop with K-byte keys (seeded), reporting `ns_per_op` for put, get and erase.
  - `BM_ViewTablePrefixScan/{12,256}`: one prefix of 12 or 256 rows among 100,000 rows; one full `scanPrefix`, reporting `ns_per_scan`.
  - `BM_ViewFoldDerivedKey/{lower,composite}`: `BM_ViewFoldReplace`'s event batch through `account_characters` and `(lower(name), characterId)` with 10 % renames, reporting `us_per_batch`.
  - `BM_ViewFoldGroupBy2/{count,minmax}`: `BM_ViewFoldAggregate`'s event batch through `device_history` (`count(*)`; `min` + `max`), reporting `us_per_batch`.
  - `BM_GetViewRowKServe/{16,128}` and `BM_ScanViewRowsServe/12`: `BM_GetViewRowServe`'s worker loop with the new ops.
- [ ] **Step 4: Run the benches** (baseline = this part's base commit on `main`, ABAB n=3, one session, same box; one prepare/restore):
  - `git worktree add "$TMPDIR/rune-cw07-base" <base>`, configured Release `BUILD_BENCHMARKS=ON` like `build/rel` (started at Checkpoint B).
  - ABAB: `rune_broker_benchmark --benchmark_filter='BM_ViewTableChurn|BM_ViewFoldReplace|BM_ViewFoldAggregate|BM_GetViewRowServe|BM_ViewTableWideKey|BM_ViewTablePrefixScan|BM_ViewFoldDerivedKey|BM_ViewFoldGroupBy2|BM_GetViewRowKServe|BM_ScanViewRowsServe' --benchmark_repetitions=3`.
  - **Gates:**
    - `BM_ViewTableChurn/*` (K ≤ 8), `BM_ViewFoldReplace`, `BM_ViewFoldAggregate`, `BM_GetViewRowServe`: no regression against the baseline (same-session ABAB medians; a shift beyond BENCHMARKING.md's noise is investigated, never waived). This covers the one-XOR seeded `mix` (P7-4).
    - `BM_ViewFoldDerivedKey/*` and `BM_ViewFoldGroupBy2/*`: ≤ **10 µs** per event batch (§14).
    - `BM_ViewFoldAggregate` (single-int groups) is the R07c fast-path gate: no ABAB regression.
    - `BM_GetViewRowKServe/*`: well inside the **50 µs** point-read gate; `BM_ScanViewRowsServe/12` reported.
    - `BM_ViewTableWideKey/*` and `BM_ViewTablePrefixScan/*`: reported (new cases, §14 "adds wide-key and prefix-scan cases").
    - Borderline numbers on the dev Mac are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh` under the VM lock on `$RUNE_VM_HOST`, then `broker/bench/vm-bench-restore.sh`).
  - Record the medians in the commit body.
- [ ] **Step 5: Write the docs** per the Files list (the site's views section is the separate repo's and is not touched).
- [ ] **Step 6: Commit.**
```bash
git add broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp broker/benchmark/ViewTableBench.cpp broker/benchmark/ViewFoldBench.cpp \
        broker/benchmark/GetViewRowServeBench.cpp docs/api/wire-protocol.md docs/api/client-cpp.md docs/api/codec.md
git commit -m "bench(views): wide keys, prefix scans, derived-key fold, keyed reads; zero-allocation gate; docs for wide view keys" \
           -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c); stage (c) runs on the VM under its lock.
  - The client `Regression` suite on `build/sub-client-cpp`; the codec, catalog and security suites on `build/sub-codec`, CatalogGolden included.
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) with `-R Regression -LE latency`.
  - `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure --repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 18 benches (already recorded).
  - This part has no docker or VM e2e of its own. Its real e2e is Task 17 on the default shard runtime.

## Finish

- [ ] Checkpoint C green.
- [ ] Whole-branch review by the most capable model; the implementer resolves each comment.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune` (one batch, one summary at the end):
  1. "Aggregate views over a source whose key is not an integer of at most 8 bytes (P7-8)" — `enhancement`, `sev:low`, `area:views`.
  2. "Generate the Unicode lowercase table at build time instead of committing it (P7-1)" — `enhancement`, `sev:low`, `area:codec` (only if the owner wants the literal reading).
  3. Every recorded deviation that is not urgent.
- [ ] Hand over to Part 08 per the overview's acceptance list for 07: plan v2 with every field, every key form, both reads, the client key builder, the codec `lower` with its pinned version, and the key index on 16-byte keys. Part 08 adds the QL leg of "`lower('Foo')` from QL equals the broker's key" with `rune::codec::lowerUtf8`.
- [ ] Do not push or merge without the owner's go-ahead (overview § Sequencing).

---

## Self-review (done while writing)

- **Spec coverage.**
  - §10.1 `group by <cols>` (R07c): Tasks 3, 4, 11, 17 (the group-by matrix block, the `device_history` e2e).
  - §7.1 key forms: Task 4 (components, `lower` on `string(N)` only, fixed-width types, K ≤ 128; 66 bytes for `(provider, subject)` pinned in Task 2).
  - §7.2 identity rule and its exact message: Task 4, Task 12.
  - §7.3 encoding: Task 2. "Two encodings, kept separate": the wire key is never re-encoded (Task 9 uses it raw for owner keys and `string(N)` identity), the view key is never hashed for routing (P7-10).
  - §7.4 one partition, reads to partition 0: Task 12 (the DDL rule), Task 14 (routing).
  - §7.5 13-byte slot, raw key for K ≤ 8, cached hash for K > 8, growth and backshift without the arena, seeded hash per worker, key bytes in the row, prefix index with `prev`/`next` and O(1) insert/erase, unordered scan, the owner map with erase-then-insert on rename, checkpoint v3 with recomputed hashes and v1/v2 still loading: Tasks 5, 6, 7, 9, 10.
  - §8.1 mechanics (SD7): Task 1 (simple lowercasing from `UnicodeData.txt`, locale-free, ASCII fast path, static table, no allocation, the pin, the width, zero padding, no normalization), Task 4 / 10 / 12 (the pin enforced at compile, DDL and install), Task 15 (the client uses the codec's table). `casefold` is reserved (function 2 refused, Task 2/4).
  - §9: Task 13 (81–83, `minOffset`, statuses), Task 14 (the shared pipeline, bounds, one-step pages, `kRowTooLarge`, `kCursorInvalidated`), Task 15 (the client API, `ResultTooLarge`, the key builder, new `OpKind`s). "A user's ≤ 12 characters fit in one page": `ScanServesOnePageInOneStep`.
  - §10.2 plan v2 fields (C14): Tasks 3 and 4; the error codes 56–58: Tasks 4 and 12.
  - §12: plan v2 and checkpoint v3 follow the frozen-format rule (P7-7, Task 7); the DDL level gate: Task 12; client version 12 for 81/82: Tasks 14, 15.
  - §13 per view: `scanRows`, `scanPages` (Task 10).
  - §14 Table and Fold budgets: Task 18.
  - §15: the Interaction matrix section (A, B, C).
  - SD8: Task 16. Overview acceptance for 07: Tasks 4, 9, 14, 15, 1, 16.
- **Step scan.** Every test step names its tests and assertions with the spec's values (128, 66, 24, 13 bytes, 16.0.0, 1460, ⌊3N/2⌋, 256 rows, 61,440 bytes, codes 34/36/39/56/57/58, statuses 5/7/8/45). Every code step names its file, signature and placement. No body is transcribed; the layouts (plan v2, checkpoint v3, wire, tail) and the keyed fold rules are spelled out because the tests alone do not fix them.
- **Type consistency.** `ViewTableKeys`, `PrefixScan`, `drainDirtyKeyed`, `keyOfRow` (Task 5/6) → Tasks 7, 9, 10, 16. `CompiledKeyComponent`, `keyModeOf`, `ownerMappedOf`, `prefixWidthOf`, `needsV2`, `CompileErrorKind` (Task 4) → Tasks 8, 10, 12. `KeyedFoldSpec`, `deriveViewKey`, `DeriveKey` (Task 8) → Tasks 9, 10, 11, 15. `AggLayout::keyed`, `AggInstance::groupIds`, `AggregateFold::keyed` (Task 11) → Task 17. `ViewKey::fromU64`/`asU64` (Part 04's, R07d) → Tasks 5, 9, 16; `ViewKey::fromBytes`/`operator==`, the `kMaxViewKeyBytes` alias (Task 5) → Tasks 7, 9, 16. `partitionFor` (06) → Task 15. Checkpoint v3 table ids 0–5 (this part) and any id below the table count (R-X3) → Part 09's 6 and 7. `KeyedDirtyEntry`, `KeyedTableDelta`, `encodeKeyedDelta`, `applyKeyedDelta`, `replayKeyed` (Task 7) → Task 10. `getRowK`, `scanPage`, `ScanPage`, `RowStatus::BadKey` (Task 10) → Task 14. `GetViewRowK`, `ScanViewRows`, `ScanViewRowsResponse`, `kCursorInvalidated` (Task 13) → Tasks 14, 15. `ViewReadOptions`, `ViewScanOptions`, `ViewScanResult`, `ViewKeyBuilder` (Task 15) → Task 17.
- **Review Focus.** Each of the five lines has its named tests in its owning task (Tasks 1, 2, 6, 9, 10, 14).
- **Proportion.** The plan fixes decisions, layouts, signatures, placements and assertions; table internals, the fold bodies, the serve code and the rig are left to the implementer within the stated rules.
