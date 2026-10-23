# Checked Writes — Part 06: Broker-Assigned Keys (`autoincrement`) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A typed, compacted topic whose `uint64` / `int64` / `uint32` key field is declared `autoincrement` gets small, dense ids from the partition leader. Partition `p` of `P` hands out `n·P + p` for n = 1, 2, …, with no extra hop and no reuse, ever, even after a delete, a failover, a truncation or a compaction. A key-0 `insert` gets the next id. An `IMPORT`-granted principal writes an explicit id and moves the counter past it. A retry inside the dedupe window returns the same ids. The client reads them through `insertAndWait(rec).key()`.

**Architecture:**
- **Routing.** The topic's catalog features carry `Partitioner::Modulo`. One function, `rune::protocol::partitionFor`, routes for the client, the gateway's `TopicRegistry` and the view-read path.
- **Write path.** Part 03's `CheckedWriteHook` gains the C-rules in `check()`, side-effect free. `stage()` appends a `kReservation` control record through the same `log` when the counter passes the reservation, copies the batch into a per-worker scratch, patches the ids into each record's key and the value's key field, and recomputes the CRC. `onAppended()` records the batch's base id in a per-partition side window keyed by `(producerId, baseOffset)`, so a duplicate gets the original `assignedBase`.
- **Recovery.** The `RcState` leadership scan (Part 04's verdict hooks) feeds reservations and data batches. Each new leadership resumes at `R + 1` and rebuilds the side window from the log. The cleaner keeps the latest reservation and every record of each live producer's last 32 batches.
- **Client.** It keeps key-0 creates in their own batches, routes them sticky, maps `assignedBase + i·P` onto each record's completion, and reports `StoredIdUnknown` when the stored batch's ids can't be returned.

**Tech Stack:** C++23, GoogleTest (`TEST_P` matrices, seeded model checks), Google Benchmark (`build/rel/broker/rune_broker_benchmark`), CMake/Ninja, real Aeron (the embedded invoker driver over `aeron:ipc`) for the worker rig, real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`, `AuthBroker`), and Docker Compose on the shared VM.

**Spec:** `broker/docs/superpowers/specs/2026-10-22-rune-checked-writes-and-view-constraints-design.md`. This part implements:
- §6 (Part C) in full;
- the §3 write-path step 3;
- the §11.1 code 54 and the reused codes 2 and 45;
- the §11.2 key-0 rows;
- §12 (`autoincrement` DDL, `assignedBase`);
- §13 (the autoincrement stats);
- §14 (the write-hook budget);
- the §15 "Autoincrement" block, plus "a batch mixing key-0 creates with other records is refused" and "a retry after failover and compaction still returns its ids".

The binding contract is `broker/docs/superpowers/plans/2026-10-22-checked-writes-00-overview.md`: C1, C5, C6, C7, C8, C10, C11, C13, C15, C16 (ids 8–10), C17, SD4, SD9, SD10, SD11 and the shared test infrastructure. The controller rulings R02e, R04e, R04g, R05a, R05d, R05f, R07d and R10a override the overview where they differ. This part consumes plans 01, 02, 03, 04, 05 and 07 (only `ViewKey::fromU64` / `asU64`) through their Interfaces and Contract additions.

---

## Execution context

- **Repo and paths.** Every path is relative to the monorepo root, and every command runs from it.
- **Branch.** `feature/checked-writes-06-autoincrement`. Cut it from the latest `main` (`git fetch` first) **after Part 05 has merged**, and merge `main` in again before review.
- **Preconditions.** Run these before Task 1. Each must match; otherwise stop and tell the controller.
  - `grep -n "enum class Partitioner" protocol/include/rune/protocol/CatalogTypes.h` (1 hit, Part 05)
  - `grep -n "kFormatV2 *= 2" codec/include/rune/catalog/CatalogEvent.h` (1 hit, Part 05)
  - `grep -n "carryKeyFeatures" codec/src/catalog/ApplyV2.h` (≥ 1 hit, Part 05)
  - `grep -n "autoincrement is not supported by this broker yet" broker/libs/node/src/AdminWorker.cpp` (1 hit: 05's P5-6 rule 4, removed here)
  - `grep -n "TopicKind kind" client-cpp/include/rune/client/TopicHandle.h` (1 hit, Part 05)
  - `grep -n "KeyIndexActivating" broker/libs/node/include/rune/node/CheckedWriteHook.h` (≥ 1 hit, Part 05)
  - `grep -n "struct RcVerdictHooks" broker/libs/node/include/rune/node/RcState.h` (1 hit, Part 04)
  - `grep -n "kReservation = 16" broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (1 hit, Part 04)
  - `grep -n "keepsReservation" broker/libs/storage/include/rune/storage/VerdictCleanState.h` (1 hit, Part 04)
  - `grep -n "VerdictUnknown = 14" client-cpp/include/rune/client/Completion.h` (1 hit, Part 04)
  - `grep -n "createKeyIndex" broker/libs/node/tests/support/CheckedWritesRig.h` (≥ 1 hit, Part 05)
  - `grep -n "fromU64" broker/libs/view/include/rune/view/ViewKey.h` (≥ 1 hit)
- **Builds.** These are already configured. If one is missing, stop and tell the controller.
  - `build/root`: Ninja, Debug, `BUILD_TESTING=ON`. Build with `cmake --build build/root -j14 --target <t>`. Test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - Also `build/sub-client-cpp` (the client suites), `build/sub-codec` (`rune_codec_tests`, `rune_catalog_tests`, `rune_security_tests`, CatalogGolden), `build/sub-ql` (`rune_ql_tests`), `build/rel` (Release, `BUILD_BENCHMARKS=ON`), `build/asan`, `build/tsan` and `build/client-tsan`.
- **Line numbers** cite `main` before Parts 01–05. Those parts edit `Worker.h`, `CheckedWriteHook.h`, `ProducePath.cpp`, `ProducerImpl.cpp`, `Client.cpp` and `AdminWorker.cpp`, so anchor on the named function or member, not on the number.
- **Docker e2e and VM benches** run only on the shared Linux VM (`$RUNE_VM_HOST`), under `flock -w 7200 /root/vm.lock`, never on local Mac docker. For benches, prepare the clean box once and restore it afterwards (`broker/bench/BENCHMARKING.md`). This plan writes the e2e leg (Task 14). Running it on the VM is part of Checkpoint C.
- **Commits.** Use `git add <paths>` only, never `-A`, `.superpowers/` or build logs. Every message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Never run `conan create`, `conan export` or `conan upload`. Nothing in this plan pushes.

## Global Constraints

Copied verbatim from the overview (Plan 00), restricted to what this part touches. Every task implicitly includes this section.

- **Error codes** (`protocol/include/rune/protocol/client/Protocol.h:105`, `ErrorCode`, append-only):
  - `AutoIncrementExhausted = 54`: the counter has reached `autoincrement.max`. The batch isn't stored. Write, definite.
  - `ConstraintViolation = 51`, `KeyExists = 52`, `KeyNotFound = 53`: fold, per record, definite. "Definite" for 51–53 means *never visible*. 51–53 reach only `View` writers on client version 12.
  - Reused codes: `NotLeader` (2, retriable) while the autoincrement reservation is being found; `SchemaValidationFailed` (24) for validation; `InvalidRequest` (39) for forbidden writes; `NotAuthorized` (45) for an explicit id without `IMPORT`.
- **Versions.** `kClientVersionCheckedWrites = 12`, `kClientVersion = 12`, `kBrokerVersionCheckedWrites = 7`, `kBrokerVersion = 7`, `MetadataFeature::kCheckedWrites = 4`. Plan v2, checkpoint v3 and catalog `formatVer` 2 follow the existing frozen-format rules: old formats still decode.
- **Values.**
  - `autoincrement.reserve` = **4096**.
  - Id cap **2^53 − 1** (`autoincrement.max` may be lower).
  - The `autoincrement` side window holds **32** batches = `kIdempotentMaxInFlightBatches` (`Protocol.h:16`).
  - Partition `p` assigns `n·P + p` for n = 1, 2, …. Id 0 is never assigned.
  - Topic key field ≤ `Key::kMaxInline` = **16 bytes**.
  - Wire `acks` byte: bit 0 = `All`, bit 1 = `View`.
  - Intent bits: record `attributes` bits 0–1. 0 = upsert/delete, 1 = insert, 2 = update/delete-existing.
  - `partitions` defaults to 1.
- **Budgets** (hard gates, spec §14):
  - Write hook: autoincrement assignment, including the scratch copy, < **1 µs p50** per batch of up to 4 KiB (`ProduceValidateBench`).
  - No ABAB n=3 regression on `ProducePathBenchmark` and `WorkerProduceLatencyBench` for untyped topics and for already-validated typed topics.
- **Repo rules** (root `CLAUDE.md` §1–§4):
  - **Hot path.** The produce, consume, fold and query paths are zero-alloc, take no lock, use no atomics as coordination and no `thread_local`, make no virtual call and use no `std::function`. Hot paths use concrete types plus a branch, or templates. Control-plane work (installs, DDL, recovery scans) runs as cold worker ops and may allocate.
  - **States.** Rich-state variants, never scattered booleans.
  - **Clocks.** One injected clock. Tests advance it and call `doWork`, with a hard iteration cap and never a wall-clock wait.
  - **Regression tests.** Every fix ships a `*Regression*` test. Each task runs the affected-area tests.
  - **Checkpoints** run the full `-R Regression -LE latency` suite, plus ASan+UBSan and TSan for the broker and the client, plus CatalogGolden.
  - **Interaction matrix.** Every feature is tested across its matrix. Each plan has an "Interaction matrix" section and a "Review Focus" section, with 1–3 phases of small tasks.
  - **Real e2e.** Nothing is done until its real e2e path is green on the **default** config (shard worker mode).
  - **No weakened tests.** A weakened assertion is recorded as a deviation. Urgent ones are fixed now and the rest are filed as GitHub issues with type, severity and area labels.
- **Names.** Every new test suite name starts with `CheckedWrites`. This part uses the prefixes `CheckedWritesAutoInc*` and `CheckedWritesImport*`. Every fix test carries `Regression` in its suite or name.

## Rulings this part depends on (restated; binding)

- **SD9 / R10a / R02e.** `autoincrement` on a transient topic is refused through Part 02's `featureRefusal(DbFeature::AutoIncrement, shape)` (`broker/libs/node/include/rune/node/TopicFeatureRules.h`). Its messages are, in rule order:
  - untyped: "autoincrement needs a typed topic";
  - keyless: "autoincrement needs a key";
  - transient: "autoincrement needs a persisted topic".
- **SD10.** In legacy worker mode, a key-0 create is answered `InvalidRequest`. It carries intent 1, so 03's rule 2 (`LegacyMode`) refuses it.
- **SD11.** The client gates key-0 creates on `ConnectResponse.brokerVersion >= kBrokerVersionCheckedWrites`. Below that it fails locally with `OpStatus::BrokerTooOld`. A key-0 create may go without `View`, so 03's `View` gate doesn't cover it; this part adds the seal-time gate (P6-12). A v7 broker below level 4 answers it `InvalidRequest` (03's rule 3).
- **R04e / R04g.** An in-window idempotent duplicate is answered with the original's verdicts, and **this part adds the original `assignedBase`** to that replay. A duplicate outside the window gets `DuplicateSequence` (21). The client maps it to one `OpStatus`, which for a key-0 batch is `StoredIdUnknown = 13` (P6-10).
- **R05a.** Local refusals are `SendResult::WriteNotAllowed`, synchronous, with no completion.
- **R05d.** `ApplyV2` (catalog `formatVer` 2) stays open until the first release. This part appends its autoincrement rules to it and leaves 05's v2 golden byte-identical.
- **R05f.** `TopicHandle` appends `rune::protocol::Partitioner partitioner = Partitioner::Fnv1a;` **after** 05's `kind`.
- **R07d.** `ViewKey::fromU64(v).asU64() == v`. This part does not touch the key index's word mapping.
- **P5-4 (05).** An intent sent before the key index's change record is in the log gets a retriable `NotLeader` (`KeyIndexActivating`). That includes key-0 creates.
- **P5-1 (05).** The key index is rebuilt from the local log at every activation. Assigned ids are folded into it like any insert.
- **C8 rule 8 (03).** The key-0 rules are in place: `MixedKeyZeroBatch`, `KeyZeroCreateNeedsIdempotentAll` and `KeyZeroCreateNeedsView`. This part appends 8a–8f after them (P6-4).

## Part rulings (P6; binding for executors, recorded so the reviewer can overturn any)

- **P6-1 — Topic features.**
  - **Normalization.** `autoIncrement = true` makes the AdminWorker store `keyIndex = true` and `partitioner = Modulo` (§6.2 "implies"), whatever the request carried.
  - **Partitioner without autoincrement.** `Modulo` is refused without `autoIncrement` (P6-5 rule 10).
  - **Effective cap.** The cap is `effectiveAutoIncMax(type, max)` = `min(max, 2^32 − 1)` for `uint32` and `max` otherwise. A `uint32` request that carries the default max stores `2^32 − 1`.
  - **Immutability.** The features never change after create (05's `carryKeyFeatures`), and the partition count can't be altered (no alter exists). So the key field, its type, `autoincrement` and P are immutable (§6.2).
- **P6-2 — `partitionFor`.**
  - **Fnv1a** is exactly `partitionForKey(key, P)`.
  - **Modulo.** For `key.size()` in 1–8 it is the little-endian, zero-extended `uint64` of the key mod P. An empty key, a key wider than 8 bytes, or `P ≤ 1` gives 0.
  - `int64` ids are positive, so their LE bytes equal the `uint64` form.
- **P6-3 — `assignedBase` of a stored batch.** On an autoincrement partition, `assignedBase` is the first record's key read as `uint64` when that record's intent is `Insert`, and 0 otherwise.
  - For a key-0 batch it is the base id. For an `IMPORT` batch it is the first imported id. Either way the fresh answer and the answer rebuilt from the log are equal.
  - The client uses it only for key-0 batches (P6-10).
- **P6-4 — Write rules on an autoincrement topic** (§6.6, §11.2). These are appended to C8 inside `evaluateRequirements`, after 03's rule 8 and before rule 9, in this order:
  - **8a.** A live record with intent 0 (a plain upsert) → `InvalidRequest`, `UpsertOnAutoIncrement`. A tombstone with intent 0 (`remove`) is allowed.
  - **8b.** An explicit-key `insert` without `facts.importGranted` → **`NotAuthorized` (45)**, `ImportNotGranted`.
  - **8c.** An explicit-key `insert` whose key is out of range → `InvalidRequest`, `ImportOutOfRange`. Out of range means:
    - `id % P != partition`, or
    - `id > effectiveMax` (this covers negative `int64` ids, which read as huge `uint64`).
  - **8d.** A key-0 create, or an explicit `insert`, while `autoInc` is not `Ready` → **`NotLeader`** (retriable), `AutoIncActivating`.
  - **8e.** A key-0 batch whose last `n` exceeds `nMax = (effectiveMax − p) / P` (0 when `effectiveMax < p`) → **`AutoIncrementExhausted` (54)**, `AutoIncExhausted`. Nothing is appended and the counter is unchanged.
  - **8f.** A key-0 batch from a producer the side window can't hold → **`NotLeader`**, `AutoIncWindowFull`. That producer is unknown and the window is at capacity. The cold maintenance grows the window (P6-8).
  - `update`, `removeExisting` and `remove` on an autoincrement topic need nothing beyond 03/05's rules.
- **P6-5 — DDL rules** (`AdminWorker::buildDefine_`). They run when the request has `autoIncrement` or a non-default `partitioner`, `autoIncrementMax` or `autoIncrementReserve`. They replace 05's P5-6 rule 4 and run in this order. Each failure is `InvalidRequest` (39) with this exact detail:
  1. `clusterFeatureLevel_ < kCheckedWrites` → "autoincrement needs every broker at feature level 4".
  2. `featureRefusal(DbFeature::AutoIncrement, shape)` → its message (R02e).
  3. An adoption (the name is an existing untyped topic) → "autoincrement needs a new topic".
  4. A key field type other than `UInt64`, `Int64` or `UInt32` → "autoincrement needs a uint64, int64 or uint32 key".
  5. `config.cleanupPolicy != 1` → "autoincrement needs cleanup.policy = compact".
  6. `config.retentionMs != -1 || config.retentionBytes != -1` → "autoincrement topics keep every record: retention.ms and retention.bytes must be -1".
  7. `autoIncrementReserve` outside `[1, kAutoIncrementReserveMax = 1'048'576]` → "autoincrement.reserve must be between 1 and 1048576".
  8. The normalized max outside `[2·P − 1, cap]` → "autoincrement.max must be between <2P−1> and <cap>". The cap is `2^53 − 1`, or `2^32 − 1` for `uint32`, and the numbers are printed in decimal.
  9. Non-default `autoIncrementMax` or `autoIncrementReserve` without `autoIncrement` → "autoincrement.max and autoincrement.reserve need autoincrement".
  10. `partitioner == Modulo` without `autoIncrement` → "the modulo partitioner needs autoincrement".

  After these rules the request is normalized (P6-1), and 05's `key.index` rules 1–3 run on the result.

  `buildAlterConfig_` refuses a change on an autoincrement topic that leaves `retentionMs` or `retentionBytes` other than −1, with rule 6's detail.

  There is no delete-records API in the broker. With `cleanup.policy = compact` no retention runs (`NodeRuntimeAgent.h:830`, `LogCleanerAgent.h:402`), so the log start never moves. `topicSpecFromCatalog` leaves `TopicSpec::logStartOffset = 0` (a static-config knob that catalog topics never set).
- **P6-6 — `ApplyV2` rules** (replayed, so a hand-made catalog can't bypass the DDL). Each failure is `Precondition`.
  - `autoIncrement` needs all of:
    - a schema with a key field of type `UInt64`, `Int64` or `UInt32`;
    - `Persistence::Disk`;
    - `keyIndex`;
    - `partitioner == Modulo`;
    - `cleanupPolicy == 1`;
    - `retentionMs == retentionBytes == -1`;
    - the reserve and max ranges of P6-5;
    - a `CreateTopic` (never a `CreateSchema` adoption).
  - `Modulo` needs `autoIncrement`.
  - An `AlterTopicConfig` on an autoincrement topic must leave retention at −1.
- **P6-7 — Counter, reservation and activation** (`AutoIncState`, three states).
  - **The states.**
    - `Inactive`: a follower, or never activated.
    - `Scanning{reservedThroughN, maxAssignedN}`: the leadership scan is feeding.
    - `Ready{nextN, reservedThroughN}`.
  - **Activation.** On every `RcState` leadership scan, `begin` moves the state to `Scanning`, and every `kReservation` record seen raises `reservedThroughN` (`R`). The scan's `end` moves it to `Ready{nextN = R + 1, R}`, and the burned gap `R − maxAssignedN` (when positive) is added to `idsBurned`. A role change to follower moves it to `Inactive`. A new topic starts at `Ready{1, 0}` once its first scan ends.
  - **Planning a batch** (pure, `plan()`):
    - Key-0 creates of k records: `firstN = nextN` and `lastN = nextN + k − 1`. The batch is exhausted when `lastN > nMax`.
    - Imports: `importMaxN = max(id / P)` over the batch, and `newNextN = max(nextN, importMaxN + 1)`.
    - **Reserve** when the highest n the batch takes is above R. The new reservation runs through `min(nMax, max(highestN, R + reserve))`. For an import this is "through `id div P`" (§6.6) extended to a full block, so a back-fill in rising id order does not append a reservation per batch.
  - **Order in `stage()`:** append the reservation (if any), set R, then assign and patch, then set `nextN`. An append that fails after `stage()` only burns ids.
  - **Id of n:** `n·P + p`. Batch record i gets `base + i·P` (§6.3).
- **P6-8 — The side window** (`AssignedBaseWindow`).
  - **Layout.** An open-addressed table of producer slots `{producerId, head, count, 32 × {baseOffset i64, assignedBase u64}}`, reserved at install at `kAssignedBaseProducersInitial = 1024` slots. It holds 32 entries per producer, in step with `kRetryWindowSize`.
  - **Fill.** `record` runs in `onAppended` for every batch with `assignedBase != 0` and `producerId >= 0`, transactional or not.
  - **Lookup.** `find(pid, baseOffset)` answers 0 for unknown.
  - **Growth.** A cold maintenance step, `Worker::maintainAutoIncWindows_()`, runs once per 1,024 passes and whenever the load passes 75 %. It doubles the table, up to `kAssignedBaseProducersMax = 1'048'576`.
  - **Rebuild.** The window is cleared at the scan's `begin` and refilled from every idempotent data batch the scan sees. `recordScanned` may grow, since the scan is a cold recovery path.
  - **Expiry.** It follows the cleaner's producer expiry: an expired producer's batches stop being kept (P6-9), and its slot is cleared at the next scan that does not see them.
- **P6-9 — The cleaner on an autoincrement partition.**
  - **Reservations.** Every `kReservation` record except the latest one below the scan's end is removable through `markerAction`'s horizon path. The latest one is always kept (`keepsReservation`).
  - **Protected batches.** The pre-pass (`scanTxnState`) records, per producer, the last 32 idempotent data batches. Every record of a protected batch is copied unchanged by both rewrite engines (§6.6), including superseded and rejected records. Rejected ones stay invisible: their offsets stay in the index, and the marker that lists them is not removable while they exist (04's rule).
  - **Expiry.** A protected batch of a producer that the cleaner's existing expiry (`ProducerTails`, 7 days by default) has expired is no longer protected.
  - **Pinned tombstones** (added to §6.6). A tombstone whose key appears in a protected batch is never dropped by `delete.retention.ms` while that batch is protected. Without this, a deleted id's insert, kept in a protected batch, would come back once its tombstone expired: the class of bug ProducerTails' fix round 1 C2 removed. *Flagged.*
  - **Depth.** The depth is `PartitionConfig::producerTailDepth`: 32 on autoincrement topics, 1 (today's metadata-only rule) elsewhere.
- **P6-10 — Client results for a key-0 batch.**
  - **Accepted.** An accepted record i completes `Ok` with `value = baseOffset + i`, `aux = assignedBase + i·P`, and `WriteResult::key()` = that id.
  - **Rejected.** A record rejected at fold (51) completes `BrokerError` with `aux = 0`. Its id is burned.
  - **`StoredIdUnknown`.** The whole batch completes `StoredIdUnknown` (13, `errorCode` 21 or 0) when any of these holds:
    - the answer is `DuplicateSequence`;
    - it is `OK` with `baseOffset < 0` (a known-old duplicate);
    - it is `OK` with `assignedBase == 0` (a side-window miss, counted `autoIncWindowMisses` on the broker).

    For key-0 batches this replaces 04's `VerdictUnknown`, which stays for every other batch.
- **P6-11 — Client batching and routing.**
  - **What counts as a key-0 create.** A record is one when the handle's `partitioner == Modulo`, the intent is `Insert`, and the key is `keyLen` zero bytes.
  - **Homogeneous batches.** The producer never puts a key-0 create in a partition batch that holds other records, or the reverse: a kind change seals the open batch first (ring flag `kFlagKeyZero = 0x08`).
  - **Sticky routing.** Key-0 creates route to `sticky_[topicId]`, which starts at 0 and advances (mod P) after every `kKeyZeroStickyRun = 64` key-0 records on that topic. Every other record on a Modulo handle routes with `partitionFor`.
- **P6-12 — Client local refusals on a Modulo handle.** Each is `SendResult::WriteNotAllowed` (R05a), and the blocking forms throw `std::invalid_argument` naming the rule. They amend 05's P5-9:
  - A key-0 create from a non-idempotent producer → "autoincrement creates need an idempotent producer".
  - A key-0 create without `All` → "autoincrement creates need acks All".
  - A live `Upsert` → "an autoincrement topic takes no plain upserts".
  - **Exempt from 05's refusals.** A key-0 create is exempt from "intents need acks including View" (§11.2 row 3) and from "intents inside a transaction need Part G" (§11.2 last sentence).
  - **Version gate.** A batch that holds key-0 creates and is sealed toward a seed `brokerVersion` in 1–6 completes `BrokerTooOld` and is not sent (SD11).
  - **Not checked locally.** Whether the source is constrained. The broker's `KeyZeroCreateNeedsView` answers it, and the docs tell writers to use `All | View`.
- **P6-13 — The key index still judges assigned ids** (deviation from §6.4's "skips the key-index existence check").
  - Assigned ids are folded as inserts and judged like any insert. The probe costs one hash lookup and is always absent by construction.
  - It doubles as a reuse detector. A 52 on a key-0 record would mean a reused id: it is counted `autoIncAssignedKeyExists`, logged `critical`, and asserted 0 by every test and the stress run.
  - *Flagged.*
- **P6-14 — Import rights.**
  - **The grant.** `IMPORT` (`SecurityOperation::Import = 7`) is a TOPIC operation. `validGrant` allows it on TOPIC patterns only, and nothing implies it: not ADMIN, not CLUSTER grants. Like every grant it implies DESCRIBE (D15).
  - **Dev mode.** An allow-all session (dev mode, `NotEnforcing` workers) has it.
  - **Level gate.** A `grant import` or `revoke import` is refused by `SecurityAdmin` below cluster level 4 with `InvalidRequest`, "the IMPORT grant needs every broker at feature level 4". An older broker can't decode op 7, so letting it through would halt its security apply.
- **P6-15 — No client-version rule for key-0 creates.** A v11 session can't build one with a Rune client. A hand-built frame is stored, and its id is dropped by `encodeFor` below v12. That is the same as any v12 field.

## Contract additions (none redefines a contract type)

1. **`protocol/include/rune/protocol/PartitionHash.h`** (C13): `[[nodiscard]] inline constexpr int32_t partitionFor(std::span<const uint8_t> key, int32_t partitions, Partitioner p) noexcept;` (P6-2). The header includes `rune/protocol/CatalogTypes.h`, or moves `Partitioner` into a tiny `rune/protocol/Partitioner.h` that `CatalogTypes.h` includes, whichever needs no cycle.
2. **Client:**
   - `TopicHandle` appends `rune::protocol::Partitioner partitioner = rune::protocol::Partitioner::Fnv1a;` after `kind` (R05f);
   - `TopicSchemaResult` (`Client.h:156`) appends `bool hasKeyFeatures = false; rune::protocol::TopicKeyFeatures keyFeatures{};` (05's deferred exposure);
   - `OpStatus::StoredIdUnknown = 13` (C6);
   - ring flag `kFlagKeyZero = 0x08` (`ProducerImpl.h`);
   - `InFlightBatch` (`InFlightRing.h:77`) appends `uint32_t idStride = 0;` (0 = not a key-0 batch, else P);
   - `ClientAgent::ProduceAck` appends `uint64_t assignedBase = 0;`;
   - `ProducerHub::onProduceResponse` / `ProducerImpl::onProduceResponse` gain a trailing `uint64_t assignedBase = 0` after 04's `verdicts, hasVerdicts`;
   - `[[nodiscard]] constexpr bool isKeyZeroCreate(const TopicHandle&, const Key&, WriteIntent) noexcept` (`Producer.h`);
   - `inline constexpr int32_t kKeyZeroStickyRun = 64;`.
3. **Broker routing tables:**
   - `rune::node::TopicSpec` (`NodeConfig.h:66`) appends `rune::protocol::Partitioner partitioner = rune::protocol::Partitioner::Fnv1a; bool autoIncrement = false;`;
   - `CatalogTopicAppliedMsg` (`AdminChannels.h:1273`) appends `rune::protocol::TopicKeyFeatures keyFeatures{};`, and its in-process wire appends `[flags u8: bit0 autoIncrement][partitioner u8]`. `catalogTopicAppliedWireLen` adds 2 (a view's message carries its source's);
   - `rune::replication::PartitionConfig` (`PartitionReplica.h:174`) appends `uint8_t producerTailDepth = 1;`, with `[[nodiscard]] uint8_t PartitionReplica::producerTailDepth() const noexcept`.
4. **`WriteState.h` (C7):**
   - `TopicWriteSpecWire` appends `bool autoIncrement` (flags bit 2), `rune::protocol::Partitioner partitioner`, `uint64_t autoIncrementMax`, `uint32_t autoIncrementReserve` and `int32_t partitionCount`. `kTopicWriteSpecWireLen` goes from 9 to **26**: the 9 bytes, then `[partitioner u8][autoIncrementMax u64][autoIncrementReserve u32][partitionCount i32]`.
   - C7's `TopicWriteSpec` and `PartitionWriteState` members verbatim.
5. **`broker/libs/node/include/rune/node/AutoInc.h`** (new, included by `WriteState.h`):
   ```cpp
   inline constexpr uint32_t    kAutoIncrementReserveMax      = 1u << 20;
   inline constexpr std::size_t kAssignedBaseSlots            = 32;          // == rune::storage::kRetryWindowSize
   inline constexpr std::size_t kAssignedBaseProducersInitial = 1024;
   inline constexpr std::size_t kAssignedBaseProducersMax     = 1u << 20;
   [[nodiscard]] constexpr uint64_t effectiveAutoIncMax(rune::codec::PrimType keyType, uint64_t max) noexcept;
   [[nodiscard]] constexpr uint64_t autoIncId(uint64_t n, int32_t partitions, int32_t partition) noexcept;   // n*P + p
   struct AutoIncPlan { uint64_t firstN = 0; uint32_t count = 0; uint64_t newNextN = 0;
                        bool reserve = false; uint64_t reserveThroughN = 0; bool exhausted = false; };
   struct AutoIncInactive {}; struct AutoIncScanning { uint64_t reservedThroughN = 0; uint64_t maxAssignedN = 0; };
   struct AutoIncReady { uint64_t nextN = 1; uint64_t reservedThroughN = 0; };
   class AutoIncState {
   public:
       void configure(int32_t partition, int32_t partitions, uint64_t effectiveMax, uint32_t reserve) noexcept;   // cold
       void beginScan() noexcept;  void onReservation(uint64_t reservedThroughN) noexcept;
       void onAssignedThrough(uint64_t n) noexcept;  [[nodiscard]] uint64_t finishScan() noexcept;   // returns the burned gap
       void deactivate() noexcept;
       [[nodiscard]] bool ready() const noexcept;
       [[nodiscard]] AutoIncPlan plan(uint32_t keyZeroCount, uint64_t importMaxN /*0 = none*/) const noexcept;
       void commitReservation(uint64_t reservedThroughN) noexcept;  void commitNext(uint64_t newNextN) noexcept;
       [[nodiscard]] uint64_t nextN() const noexcept;  [[nodiscard]] uint64_t reservedThroughN() const noexcept;
       [[nodiscard]] uint64_t nMax() const noexcept;   [[nodiscard]] int32_t partition() const noexcept;
       [[nodiscard]] int32_t partitions() const noexcept;
   private: std::variant<AutoIncInactive, AutoIncScanning, AutoIncReady> st_; /* + configuration */
   };
   class AssignedBaseWindow {
   public:
       void reserve(std::size_t producers);  void clear() noexcept;
       [[nodiscard]] bool canAccept(int64_t producerId) const noexcept;
       void record(int64_t producerId, int64_t baseOffset, uint64_t assignedBase) noexcept;   // precondition canAccept
       void recordScanned(int64_t producerId, int64_t baseOffset, uint64_t assignedBase);     // cold; may grow
       [[nodiscard]] uint64_t find(int64_t producerId, int64_t baseOffset) const noexcept;   // 0 = unknown
       [[nodiscard]] std::size_t producers() const noexcept;  [[nodiscard]] std::size_t capacity() const noexcept;
       void grow(std::size_t producers);   // cold
   };
   struct AutoIncStats { uint64_t idsBurned = 0, reservationsAppended = 0, keyZeroBatches = 0, importsMovedCounter = 0,
                         windowMisses = 0, windowFullRefusals = 0, assignedKeyExists = 0; };
   ```
6. **`ControlRecordLayout.h` (C10, 06's builder):** `[[nodiscard]] std::size_t reservationBatchBytes() noexcept;`, `std::size_t buildReservation(std::span<uint8_t> out, uint64_t reservedThroughN) noexcept;` (0 = out too small) and `[[nodiscard]] std::optional<uint64_t> parseReservation(const uint8_t* batch, std::size_t len) noexcept;`.
7. **`CheckedWriteHook.h` (03's seam):**
   - `WriteFacts` appends `int64_t producerId = -1;`;
   - `TopicHookFacts` appends `bool autoIncReady = false;`;
   - `BatchIntents` appends `bool plainUpsert = false, explicitInsert = false, importBadKey = false, autoIncExhausted = false, autoIncWindowFull = false; uint32_t keyZeroCount = 0; uint64_t importMaxN = 0;`;
   - `HookRefusal` gains `UpsertOnAutoIncrement, ImportNotGranted, ImportOutOfRange, AutoIncActivating, AutoIncExhausted, AutoIncWindowFull` before `Count_`;
   - rules 8a–8f (P6-4);
   - 03's sweep invariant (iv) becomes: "every non-OK code is `InvalidRequest`, except `KeyIndexActivating` / `AutoIncActivating` / `AutoIncWindowFull` → `NotLeader`, `ImportNotGranted` → `NotAuthorized`, and `AutoIncExhausted` → `AutoIncrementExhausted`";
   - invariant (i) gains the exception 8a.
8. **`RcVerdictHooks` (04's contract addition 8)** appends `void (*dataBatch)(void* ctx, const uint8_t* batch, std::size_t len, int64_t baseOffset) noexcept = nullptr;` (every non-control data batch with `producerId >= 0`, called from `feedProducerState_`) and `void (*end)(void* ctx, int64_t target) noexcept = nullptr;` (called on the scan's `Ready` edge).
9. **Cleaner:**
   - `broker/libs/storage/include/rune/storage/ProducerBatchWindow.h` (new, cleaner thread, cold): `class ProducerBatchWindow { void setDepth(uint8_t); void noteBatch(int64_t pid, int64_t base, int64_t last, int64_t maxTimestampMs, std::span<const uint64_t> keys); void finish(int64_t nowMs, int64_t expirationMs); [[nodiscard]] bool protects(int64_t offset) const noexcept; [[nodiscard]] bool pinsTombstone(uint64_t key) const noexcept; void clear() noexcept; };`;
   - `scanTxnState` gains a trailing `ProducerBatchWindow* batches = nullptr`;
   - `void LogCleaner::setProducerTailDepth(uint8_t) noexcept;` and the test seam `[[nodiscard]] const ProducerBatchWindow* LogCleaner::producerBatchWindowForTest() const noexcept;`;
   - `VerdictCleanState::onReservation` and `keepsReservation` get their meaning (P6-9).
10. **Security:**
    - `SecurityOperation::Import = 7`, with `validSecurityOperation` extended;
    - `validGrant` allows `Import` on TOPIC;
    - the op names "IMPORT" (`SecurityEvent.cpp:270`, `SecurityStatementText.cpp:31`, `Authorizer.cpp:19`);
    - QL `import` in `parseOperation` (`Parser.cpp:928-936`);
    - `WritePermTable` appends `std::vector<uint64_t> importBits;`, and `[[nodiscard]] inline bool allowsImport(const WriteGrantSlot&, int32_t topicId) noexcept` (`grantAll` → true);
    - `kPermFlagImport = 0x80` marks a Table or Patch item of the import bitmap;
    - `PermissionShipper::shipImportTable(WorkerPool&, uint32_t tableId, std::span<const uint64_t>)` and `patchImportTable(…)`;
    - the gateway `PermissionTable` appends `importBits_` and `[[nodiscard]] std::span<const uint64_t> importBits() const noexcept`;
    - `kOpFlagImportGranted = 0x10` on the produce op's flag byte (`WorkerOp.h:657`).
11. **`ProduceOutcome::assignedBase`** (C5) is filled through 03's `assignedBase()` hook member. `WorkerReturn::assignedBase` and `AckEntry::assignedBase` carry it to `ProduceResponse.assignedBase`.
12. **Stats (C16):** id 8 `autoIncrementCounter` = the next id this partition assigns (`nextN·P + p`); id 9 `autoIncrementReservedThrough` = `R·P + p` (0 when R = 0); id 10 `autoIncrementIdsBurned`. Before Part 08: `[[nodiscard]] const AutoIncStats* Worker::autoIncStatsForTest(int32_t topicId, int32_t partition) const noexcept` `[[nodiscard]] std::optional<AutoIncReady> Worker::autoIncStateForTest(…) const` and `[[nodiscard]] WriteFacts Worker::lastWriteFactsForTest() const noexcept`.
13. **Test support:**
    - `RigOptions::partition = 0` (the rig's replica index);
    - `struct AutoIncShape { int32_t partitions = 1; rune::codec::PrimType keyType = UInt64; uint32_t reserve = 4096; uint64_t max = kAutoIncrementMaxDefault; };` and `void CheckedWritesRig::createAutoIncTopic(AutoIncShape = {});` (topic `{id <keyType> key autoincrement, v int64, tag uint8}`, compact);
    - `Ack` appends `uint64_t assignedBase = 0;`;
    - `RigOptions::importGranted = true`;
    - `CheckedWritesModel::addAutoIncRule(AutoIncShape, int32_t partition)`;
    - matrix `Op` 40–45 and `Entity` 40–46, `expect06`, pin row 06;
    - the stress invariant `NoIdTwiceNoReuse`.

## Contract gaps (flagged to the controller)

- **Untyped-topic message.** The brief says autoincrement on a keyless *or untyped* topic is refused with "autoincrement needs a key". Part 02's binding table (R02e) gives untyped topics "autoincrement needs a typed topic". This plan follows 02.
- **§6.6 "never removes records from the last 32 batches".** Kept literally, this would bring a deleted id back once its tombstone expired. P6-9 pins those tombstones while the batch is protected.
- **§6.4 "skips the key-index existence check".** Not skipped (P6-13). The probe is kept as a reuse detector.
- **No delete-records API exists.** The §15 cell "delete-records on an autoincrement topic" has nothing to refuse. A follow-up issue requires a future delete-records to refuse autoincrement topics.
- **§6.6 import reservation "through id div P"** is extended to a full block (P6-7).
- **`assignedBase` on `IMPORT` batches** is the first imported id (P6-3), so a rebuilt window and a fresh answer always agree.
- **QL grammar.** The `autoincrement` field modifier and `with (autoincrement.reserve / .max)` are Part 08's. This part exercises them through `TopicOptions::keyFeatures`. Part 08 owns the QL cell `topic … { id uint64 autoincrement … } key id with (cleanup.policy = compact);` with no `partitions`, which must give 1 partition.
- **Orphaned rows.** An `insertAndWait` that times out with its outcome unknown makes the producer bump its epoch. A stored key-0 batch then keeps ids its writer never learned. That is today's unknown-outcome semantics, documented in `client-cpp.md` ("look the row up through a unique view").

## Review Focus

These are the five inputs the spec implies but no requirement names, most likely to bite first. Each is pinned by a test in the owning task.

1. **A key-0 `insertAndWait` whose ack is lost, then the leader fails over (memory lost), then the retry.** The retry must get the original ids from the window rebuilt from the log, never new ids and never `StoredIdUnknown` inside the window.
   - Test: `CheckedWritesAutoIncRegression.RetryAfterFailoverReturnsTheIdsFromTheRebuiltWindowRegression` (Task 9).
2. **A back-fill that imports ids above the reservation, then failover, then creates.** No id is reused, and the counter is above max(imported id).
   - Tests: `CheckedWritesAutoIncRegression.ImportAboveTheReservationThenFailoverNeverReusesRegression` (Task 9) and `CheckedWritesBrokerRegression.RegressorsPathBackFillThenInsertsNeverCollideThreeNodesRegression` (Task 13).
3. **The highest id deleted, its insert and tombstone compacted away, then restart.** The next create gets an id above the deleted one (the latest reservation survives), and the deleted row never comes back (P6-9).
   - Test: `CheckedWritesAutoIncCleanerRegression.HighestIdDeletedAndCompactedIsNeverReusedNorResurrectedRegression` (Task 10).
4. **Raw producers sending autoincrement shapes a typed client never would.** This covers a key-0 key with a non-zero value key field, a plain upsert, an import on the wrong partition, a negative `int64` import, a mixed key-0 batch and a key-0 create with `acks=leader`. Each is refused definitely, with nothing appended and the counter unchanged.
   - Test: `CheckedWritesAutoIncRegression.RawAutoIncShapesAreRefusedAndTheCounterHoldsRegression` (Task 9).
5. **A P = 4 topic written through the gateway route (`partitionKnown = 0`) and the typed client.** Every id lands on `id % 4`. A later `update`, `removeExisting`, `GetViewRow` and keyed subscribe all route to it.
   - Test: `CheckedWritesBrokerRegression.ModuloRoutingIsTheSameOnEveryPathShardRegression` (Task 13).

---

## Interaction matrix

### A. The shared worker-level matrix (`rune_checked_writes_matrix_tests`, `CheckedWritesMatrix/Cell.*`, `cellsFor(Part::P06)`, Task 11)

Built on 04's `CheckedWritesRig` with `createAutoIncTopic(shape)`. Every op is sent by an idempotent v12 producer with `All | View` (wire 3), import-granted, unless the timing says otherwise.

**Ops (Part 06 range 40–59):**

| Id | Op | Record(s) |
|---|---|---|
| 40 | `AutoCreate1` | one key-0 create, tag 1 |
| 41 | `AutoCreate8` | 8 key-0 creates in one batch, tags `1,9,1,9,1,9,1,9` |
| 42 | `ImportBelow` | `insert` of the id most recently assigned on the partition (explicit) |
| 43 | `ImportAbove` | `insert` of the id with `n = R + 100` on the partition |
| 44 | `UpdateAssigned` | `update` of an id assigned (and acked) in setup, tag 9 |
| 45 | `RemoveExistingAssigned` | `removeExisting` of an id assigned in setup |

**Entities (Part 06 range 40–59):**

| Id | Entity | Shape and setup |
|---|---|---|
| 40 | `P1U64` | P = 1, `uint64`; 3 creates acked |
| 41 | `P4U64` | P = 4, the rig owns partition 2 (`RigOptions{.partition = 2}`), `uint64`; 3 creates |
| 42 | `P1U32` | P = 1, `uint32`; 3 creates |
| 43 | `P2I64` | P = 2, partition 1, `int64`; 3 creates |
| 44 | `AtReservationEdge` | P = 1, `reserve = 8`; 6 creates (next n = 7, R = 8) |
| 45 | `NearMax` | P = 1, `max = 10`; 7 creates (next n = 8) |
| 46 | `Constrained` | P = 1 plus `TestReject{TagEquals{9}}` objectId 7; 3 creates with tag 1 |

**Timings:** every value of the shared `Timing` enum (21). Their rig meaning is 04's and 03's. In this part:
- `Racing`: two producers send the op in the same pass.
- `CreateOnNonEmpty` / `ReplaceDuringWrites` / `DropDuringWrites`: the op is sent as two batches. Between them a `TestReject{TagEquals{9}}` member is created (CreateOnNonEmpty), replaced by `TagEquals{1}`, or dropped.
- `TxnCommitBehind` / `TxnAbortBehind`: a third producer's open transaction of 2 key-0 creates sits before the op, and commits or aborts after the op's ack.

**Not applicable** (top-down; the first rule that matches names the reason):
1. `Timing ∈ {ReplaceDuringWrites, DropDuringWrites} × Entity ≠ Constrained`: "no constraint member to replace or drop" (6 × 6 × 2 = 72 cells).
2. `Timing ∈ {TxnCommitBehind, TxnAbortBehind} × Constrained`: "transactions are refused on a constrained source before Part G" (6 × 1 × 2 = 12 cells).

**Cell count:** 6 × 7 × 21 = 882. **798** are applicable and **84** skipped, pinned by `CheckedWritesMatrixCountRegression.CellCountsArePinned` (row 06).

**`expect06(cell)`, top-down:**
1. `NearMax × AutoCreate8` → every answer is `AutoIncrementExhausted` (54), the LEO is unchanged and `nextN` stays 8. `NearMax × ImportAbove` → `InvalidRequest` (`ImportOutOfRange`: id 110 > max 10).
2. `FailoverAppendHw`, `TruncateTail` → the first ack is `NotLeader`. The re-sent batch is acked with ids from the surviving reservation (the cut ids were never acked, so they may be handed out again).
3. `LeaderLossParked` → `NotLeader`. `AckDeadline` → `Internal`, and once the fold resumes the log holds the batch's ids.
4. `RetryAfterWindow` → for ops 40–41 the broker answers `DuplicateSequence` (21) with `assignedBase = 0` (`View`). For ops 42–45 it follows 04/05.
5. `RetryBeforeJudge` / `RetryAfterJudge` → the duplicate's ack is byte-identical to the original's, `assignedBase` included, and the batch is stored once.
6. `Constrained × AutoCreate8` → the trailer is `{(1,51,0x00070001), (3,…), (5,…), (7,…)}` and `idsBurned` rises by 4. A later `AutoCreate1` gets `base + 8`.
7. `Racing` → the two batches' ids are disjoint and contiguous: the second base is the first batch's last id + P.
8. `ImportBelow` → 52 with `constraintId 0`, and the counter is unchanged. `ImportAbove` → `Ok`. A `kReservation` record with `reservedThroughN ≥ R + 100` sits right below the import, and `nextN = R + 101`.
9. `UpdateAssigned × Constrained` → 51. Every other `UpdateAssigned` / `RemoveExistingAssigned` → `Ok`.
10. Every other cell → one `Ok` ack whose `assignedBase` and trailer match the model.

**Every applicable cell asserts**, after `driveUntilJudged(leo())`, then `compact()`, then `restart()` and a second `driveUntilJudged`:
- (a) the `rejected` entries equal the model's verdicts;
- (b) for every `OK` ack of a key-0 batch: `assignedBase` equals the first key of the batch at the ack's `baseOffset` in the log, record i's key is `assignedBase + i·P`, and each value's key field equals its key;
- (c) **NoIdTwiceNoReuse:** no id appears in two acks (across every failover and truncation of the cell), and no id appears in two committed key-0 records;
- (d) **ReservationCoversEveryId:** every committed id's n is ≤ the `reservedThroughN` of a `kReservation` record at a lower offset;
- (e) after `restart()`, `autoIncStateForTest()->nextN` is greater than every committed id's n, and `reservedThroughN ≥ nextN − 1`;
- (f) after `compact()`, the latest reservation is still in the log;
- (g) `windowMisses == 0`, `assignedKeyExists == 0`, `rejected.mismatches() == 0`.

### B. The hook requirement sweep (`CheckedWritesRequirementTable.SweepInvariants`, Task 8)

- **Shapes.** 05's 7 shapes plus "explicit insert with a bad key" (8).
- **New bits.** `importGranted`, `autoIncReady`, `autoIncExhausted` and `autoIncWindowFull`, each 0/1.
- **Size.** 172,032 × 8 / 7 × 16 = **3,145,728 cells**, with invariants (i) and (iv) amended (contract addition 7) and 06's pinned rows.

### C. Named cells outside the matrix

| §15 / overview cell | Test (task) |
|---|---|
| an explicit id without `IMPORT` | `CheckedWritesAutoIncHook.ExplicitInsertWithoutImportIsNotAuthorized` (8), `CheckedWritesBrokerRegression.ImportNeedsTheGrantShardRegression` (13) |
| exhaustion at the maximum | matrix `NearMax`, `CheckedWritesAutoIncState.ExhaustionIsExactAtTheCap` (7) |
| failover just before / just after a reservation | `CheckedWritesAutoIncRegression.FailoverAroundAReservationResumesAboveItRegression` (9) |
| truncation of a reservation with its data | matrix `TruncateTail × AtReservationEdge`, `CheckedWritesAutoIncRegression.TruncatedReservationTakesItsDataWithItRegression` (9) |
| compaction after the highest id is deleted | `CheckedWritesAutoIncCleanerRegression.HighestIdDeletedAndCompactedIsNeverReusedNorResurrectedRegression` (10) |
| a create rejected at fold: its id is burned | matrix `Constrained × AutoCreate8` |
| an `IMPORT` id above the reservation, then failover | `CheckedWritesAutoIncRegression.ImportAboveTheReservationThenFailoverNeverReusesRegression` (9) |
| a retry after failover from the rebuilt side window | `CheckedWritesAutoIncRegression.RetryAfterFailoverReturnsTheIdsFromTheRebuiltWindowRegression` (9) |
| a retry after failover and compaction still returns its ids | `CheckedWritesAutoIncCleanerRegression.RetryAfterFailoverAndCompactionStillReturnsItsIdsRegression` (10) |
| a batch mixing key-0 creates with other records | 03's row 22 + `CheckedWritesAutoIncRegression.RawAutoIncShapesAreRefusedAndTheCounterHoldsRegression` (9) |
| DDL refused for `delete` retention and log-start moves | `CheckedWritesAutoIncDdl.Refusals` (4), `CheckedWritesBrokerRegression.AutoIncDdlRefusalsShardRegression` (13) |
| autoincrement on a transient, keyless or untyped topic | `CheckedWritesAutoIncDdl.Refusals` (4) |
| legacy mode refuses key-0 creates (SD10) | `CheckedWritesBrokerRegression.LegacyRuntimeRefusesKeyZeroCreatesRegression` (13) |
| a v6 broker / level 3 (SD11) | `CheckedWritesAutoIncClient.KeyZeroToAnOldBrokerIsBrokerTooOld` (5), `CheckedWritesBrokerRegression.KeyZeroCreatesToAnOlderClusterShardRegression` (13) |
| RegressorsPath: userId / characterId, back-fill with ids kept | `CheckedWritesBrokerRegression.RegressorsPathBackFillThenInsertsNeverCollideThreeNodesRegression` (13), e2e leg (14) |

### D. Model check and cluster stress (Task 12)

- **Model check.** `CheckedWritesAutoIncModelRegression.SeededRigMatchesTheModel` runs seeds 1–100 × 250 steps on the rig, at P ∈ {1, 3}, owned partition `seed % P`. Actions, one per step from the seed's `std::mt19937_64`:
  - key-0 batches of 1–8 records from 2 producers;
  - imports below and above the counter;
  - `update` / `removeExisting` / `remove` of known ids;
  - a `TestReject{TagEquals{9}}` member installed or dropped;
  - an idempotent re-send before and after the judge, and after 33 more batches;
  - `failover()`, `demotePromote()`, `restart()`, `truncateTo(hw)`, `compact()` with `deleteRetentionMs = 0`;
  - an open transaction of key-0 creates, committed or aborted (when unconstrained).

  After every step it checks 04's and 05's invariants plus this part's:
  - `NoIdTwiceNoReuse`;
  - `IdsFollowTheStride` (key-0 keys are `base + i·P`, with `base % P == p`);
  - `ReservationCoversEveryId`;
  - `AckedIdsMatchTheLog`;
  - `CounterAboveEveryId` (after each activation).

  `CheckedWritesAutoIncModelRegression.SameSeedSameTrace` re-runs seeds 1–8 twice. A failure prints `seed=<n> step=<k> action=<name>`.
- **Cluster stress.** `CheckedWritesStressRegression.AutoIncSeededClusterHoldsTheInvariants` runs seeds 1–8 × 300 steps on 04's driver (`ThreeNodes`, default shard mode).
  - **Topic.** `ai {id uint64 key autoincrement, n int64}`, P = 2, replicas 3, compact, `deleteRetentionMs = 0`.
  - **Clients.** Three typed clients send `insertAndWait` / `removeExistingAndWait` / `updateAndWait`, and a raw import client sends ids above and below the counter. 04's faults apply.
  - **`NoIdTwiceNoReuse`**, registered through `registerInvariant`:
    1. no id is in two `ok()` `WriteResult`s;
    2. no acked-then-removed id is ever acked again;
    3. on every node, once it leads a partition, `nextN·P + p` is above every id acked on that partition;
    4. every key-0 result was definite (`StoredIdUnknown == 0`, `VerdictUnknown == 0`) and every node has `windowMisses == 0` and `assignedKeyExists == 0`.

### E. Docker e2e (VM, Task 14)

An `AUTOINC` leg in `broker/docker/checked-writes-e2e.sh`. Its legs are listed in Task 14.

---

## File structure

| File | C/M | Responsibility |
|---|---|---|
| `protocol/include/rune/protocol/PartitionHash.h`, `protocol/tests/protocol/PartitionHashTest.cpp` | M | `partitionFor` |
| `broker/libs/node/include/rune/node/NodeConfig.h:66-120` | M | `TopicSpec::partitioner`, `autoIncrement` |
| `broker/libs/node/include/rune/node/AdminChannels.h:1273-1345` | M | `CatalogTopicAppliedMsg` features |
| `broker/libs/node/include/rune/node/BrokerNode.h:1084-1093`, `broker/libs/node/src/BrokerNode.cpp:1365-1400,1565-1580,3910-3925,5560-5600` | M | Post the features, `topicSpecFromCatalog`, `producerTailDepth`, the view-sink copy |
| `broker/libs/gateway/src/TopicRegistry.cpp:339-346` | M | `partitionForKey` through `partitionFor` |
| `broker/libs/replication/include/rune/replication/PartitionReplica.h:174,986` | M | `PartitionConfig::producerTailDepth` |
| `protocol/include/rune/protocol/SecurityTypes.h`, `codec/src/security/{SecurityEvent.cpp,SecurityState.cpp,SecurityStatementText.cpp}`, `codec/tests/security/SecurityPermissionsTest.cpp`, `codec/tests/security/SecurityEventTest.cpp` | M | The `IMPORT` operation |
| `ql/src/Parser.cpp:928-936`, `ql/tests/SecurityStatementsTest.cpp` | M | `grant import` |
| `broker/libs/gateway/{include/rune/gateway/PermissionTable.h,src/PermissionTable.cpp,src/Authorizer.cpp}`, `broker/libs/gateway/src/ClientGateway.cpp:3505-3530,8200-8275` | M | Import bits, shipping, the op flag |
| `broker/libs/node/include/rune/node/{WorkerWriteGrants.h,PermissionShipper.h,WorkerOp.h:650-660,SecurityAdmin.h}`, `broker/libs/node/src/SecurityAdmin.cpp:380-395`, `broker/libs/node/tests/node/WorkerWriteGrantsTest.cpp` | M | Worker import bits, `kPermFlagImport`, the level gate |
| `broker/libs/node/src/AdminWorker.cpp` (`buildDefine_` `:1090-1172`, `buildAlterConfig_` `:1331-1347`), `codec/src/catalog/ApplyV2.cpp`, `codec/include/rune/catalog/testing/GoldenScript.h`, `codec/tools/make_catalog_golden.cpp`, `codec/tests/catalog/{golden/v2/autoinc.rcat (C),CatalogGoldenPins.h,CatalogGoldenTest.cpp,CheckedWritesCatalogV2Test.cpp}` | M/C | DDL and apply rules, the v2 autoincrement golden |
| `broker/libs/node/include/rune/node/WriteState.h` | M | Spec wire, spec members, state members |
| `broker/libs/node/include/rune/node/AutoInc.h` (C), `broker/libs/node/tests/node/CheckedWritesAutoIncStateTest.cpp` (C) | C | Counter, plan, window, stats |
| `broker/libs/storage/include/rune/storage/ControlRecordLayout.h`, `broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp` | M | The reservation builder and parser |
| `broker/libs/node/{include/rune/node/CheckedWriteHook.h,src/CheckedWriteHook.cpp}`, `broker/libs/node/tests/node/CheckedWritesHookTest.cpp`, `broker/libs/node/tests/node/CheckedWritesAutoIncHookTest.cpp` (C) | M/C | C-rules, stage, onAppended |
| `broker/libs/node/include/rune/node/RcState.h:600-720` | M | `dataBatch` / `end` hooks |
| `broker/libs/node/include/rune/node/Worker.h` (`produceCore_`, `handleWorkerIngressFrame_` `:8016-8085`, `executeProduceWire_` `:3944-3961`, `emitProduceAck_` `:2132-2148`, `rebuildRcSlots_` `:6364`, `executeBecomeFollowerWire_` `:4703`, the role and verdict hooks), `broker/libs/node/src/VerdictSource.cpp` (burn and reuse-detector counts) | M | Worker wiring |
| `broker/libs/storage/include/rune/storage/ProducerBatchWindow.h` (C), `broker/libs/storage/{include/rune/storage/LogCleaner.h,src/LogCleaner.cpp,include/rune/storage/VerdictCleanState.h}`, `broker/libs/node/include/rune/node/LogCleanerAgent.h:401-460`, `broker/libs/storage/tests/storage/CheckedWritesAutoIncCleanerTest.cpp` (C) | M/C | Cleaner retention |
| `client-cpp/include/rune/client/{TopicHandle.h,Client.h,Completion.h,WriteResult.h,TypedTopic.h}`, `client-cpp/include/rune/client/producer/{Producer.h,ProducerImpl.h,InFlightRing.h,ProducerHub.h}`, `client-cpp/src/producer/{Producer.cpp,ProducerImpl.cpp,ProducerHub.cpp}`, `client-cpp/src/shard/ClientShard.cpp`, `client-cpp/include/rune/client/shard/ClientShard.h:102,340`, `client-cpp/include/rune/client/metadata/PartitionRouter.h:86`, `client-cpp/src/Client.cpp` (`getTopicSchemaAndWait` decode, `:4954`), `client-cpp/include/rune/client/agent/ClientAgent.h`, `client-cpp/src/agent/ClientAgent.cpp` | M | Routing, sticky, batching, refusals, ids, `StoredIdUnknown` |
| `client-cpp/tests/producer/CheckedWritesAutoIncClientTest.cpp` (C), `client-cpp/tests/client/CheckedWritesAutoIncTypedTest.cpp` (C), `client-cpp/CMakeLists.txt` | C/M | Client suites |
| `broker/libs/node/tests/support/{CheckedWritesRig.h,CheckedWritesModel.h,CheckedWritesMatrixExpectations.h,CheckedWritesStressDriver.h}` | M | Rig, model, matrix rows |
| `broker/libs/node/tests/node/{CheckedWritesAutoIncWorkerTest.cpp (C),CheckedWritesMatrixTest.cpp,CheckedWritesModelTest.cpp,CheckedWritesAllocRegressionTest.cpp}`, `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp` | C/M | Worker tests, matrix, model, stress, alloc gate |
| `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp`, `broker/libs/node/tests/client/support/AutoIncRecords.h` (C) | M/C | Real-broker slice |
| `broker/docker/checked-writes-e2e.sh`, `client-cpp/examples/checked_writes_probe.cpp`, `client-cpp/tests/support/GenClaimSchemas.cpp` | M | The e2e leg |
| `broker/benchmark/ProduceValidateBench.cpp` | M | `BM_CheckedWriteHook/autoinc_*` |
| `broker/libs/node/CMakeLists.txt`, `broker/libs/storage/CMakeLists.txt` | M | New test sources |
| `docs/api/{ql.md,wire-protocol.md,client-cpp.md,admin-cli.md}`, `broker/docs/{CATALOG.md,SECURITY.md}`, `docs/operations.md`, `docs/testing.md`, `client-cpp/CLAUDE.md` | M | Docs this part ships |

---

## Phase 1 — Routing, the grant, the DDL and the client

### Task 1: `partitionFor` and the broker's routing tables

**Files:**
- Modify: `protocol/include/rune/protocol/PartitionHash.h` (append `partitionFor`), `broker/libs/gateway/src/TopicRegistry.cpp:339-346`
- Modify: `broker/libs/node/include/rune/node/NodeConfig.h:66` (`TopicSpec`), `broker/libs/node/include/rune/node/AdminChannels.h:1273-1345` (`CatalogTopicAppliedMsg`, `catalogTopicAppliedWireLen`, `writeCatalogTopicApplied`, `decodeCatalogTopicApplied`), `broker/libs/node/include/rune/node/BrokerNode.h:1084-1093` (`postCatalogTopicApplied_` gains `const rune::protocol::TopicKeyFeatures&`; every caller passes the topic's, or for a view its source topic's, `schema->keyFeatures`)
- Modify: `broker/libs/node/src/BrokerNode.cpp:5560-5600` (`topicSpecFromCatalog` copies `partitioner` and `autoIncrement`), `:1365-1400` and `:1565-1580` (`pcfg.producerTailDepth = spec->autoIncrement ? 32 : 1`), `broker/libs/replication/include/rune/replication/PartitionReplica.h:174,986`
- Test: `protocol/tests/protocol/PartitionHashTest.cpp`, `broker/libs/gateway/tests/gateway/TopicRegistryTest.cpp`, `broker/libs/node/tests/integration/SchemaGatewayTest.cpp:676-680`, `broker/libs/node/tests/node/CatalogEffectsTest.cpp`

**Interfaces:**
- Consumes: C13's `Partitioner` and `TopicKeyFeatures` (05).
- Produces: contract additions 1 and 3. `TopicRegistry::partitionForKey(topicId, key)` returns `partitionFor(key, spec->partitionCount, spec->partitioner)`. That serves every gateway caller (`ClientGateway.cpp:3607`, `:4892`, `:9302`) unchanged, and a view sink spec carries its source's partitioner.

- [ ] **Step 1: Write the failing tests.**
  - `PartitionHashTest.cpp`:
    - `CheckedWritesAutoIncRouting.ModuloIsTheLittleEndianIdModP`:
      - `partitionFor(LE8(4097), 4, Modulo) == 1`;
      - `LE4(7)` with P 3 → 1;
      - `LE8(0)` → 0;
      - an empty key → 0;
      - a 9-byte key → 0;
      - P 1 → 0;
      - for 10,000 random `n` and `p < P ≤ 64`, `partitionFor(LE8(n·P + p), P, Modulo) == p`.
    - `CheckedWritesAutoIncRoutingRegression.Fnv1aIsUnchangedRegression`: 10,000 random keys of 0–16 bytes → `partitionFor(k, P, Fnv1a) == partitionForKey(k, P)`.
    - `static_assert(partitionFor(…, Modulo) …)` on one constant key (constexpr).
  - `TopicRegistryTest.cpp`, `CheckedWritesAutoIncRouting.RegistryRoutesByTheTopicsPartitioner`: a topic spec with `partitioner = Modulo`, P 4 → `partitionForKey(id, LE8(10)) == 2`. The same id on a `Fnv1a` spec → `partitionForKey` of the shared function.
  - `SchemaGatewayTest.cpp`: the round trip at `:676` now carries `keyFeatures{.autoIncrement = true, .partitioner = Modulo}`. One extra byte still decodes false.
  - `CatalogEffectsTest.cpp`, `CheckedWritesAutoIncRouting.CatalogEffectCarriesThePartitioner`: an applied topic with Modulo → `TopicRegistry::topic(id)->partitioner == Modulo`, `autoIncrement`, and the replica's `producerTailDepth() == 32`. A view over it → its sink spec has Modulo. A plain topic → depth 1.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_protocol_tests rune_gateway_topic_registry_tests`. Expected: `no member named 'partitionFor' in namespace 'rune::protocol'`.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_protocol_tests rune_gateway_topic_registry_tests rune_catalog_effects_tests rune_schema_gateway_tests && ctest --test-dir build/root -R 'PartitionHash|CheckedWritesAutoIncRouting|TopicRegistry|SchemaGateway|CatalogEffects' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add protocol/include/rune/protocol/PartitionHash.h protocol/tests/protocol/PartitionHashTest.cpp \
          broker/libs/gateway/src/TopicRegistry.cpp broker/libs/gateway/tests/gateway/TopicRegistryTest.cpp \
          broker/libs/node/include/rune/node/NodeConfig.h broker/libs/node/include/rune/node/AdminChannels.h \
          broker/libs/node/include/rune/node/BrokerNode.h broker/libs/node/src/BrokerNode.cpp \
          broker/libs/replication/include/rune/replication/PartitionReplica.h \
          broker/libs/node/tests/integration/SchemaGatewayTest.cpp broker/libs/node/tests/node/CatalogEffectsTest.cpp
  git commit -m "feat: the modulo partitioner (partitionFor) on every broker routing table; producerTailDepth per partition" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 2: The `IMPORT` operation in the protocol, the codec and QL

**Files:**
- Modify: `protocol/include/rune/protocol/SecurityTypes.h:10-15`, `codec/src/security/SecurityEvent.cpp:19,270`, `codec/src/security/SecurityState.cpp:111-131` (`validGrant`: `Import` on TOPIC only), `codec/src/security/SecurityStatementText.cpp:31`
- Modify: `ql/src/Parser.cpp:29` (grammar comment), `:928-936` (`{"import", SecOp::Import}`; the error text lists `'import'`)
- Test: `codec/tests/security/SecurityPermissionsTest.cpp`, `codec/tests/security/SecurityEventTest.cpp`, `ql/tests/SecurityStatementsTest.cpp`

**Interfaces:**
- Produces: contract addition 10 (codec, protocol, QL half). `opsOn` needs no change: `opBit(Import) == 0x80` fits its `uint8_t`, and D15/D16 apply as written (CLUSTER grants never carry `Import`).

- [ ] **Step 1: Write the failing tests.**
  - `SecurityPermissionsTest.cpp`:
    - `CheckedWritesImportGrant.ValidOnTopicsOnly`: `validGrant({Import, {Topic, Exact, "user"}})`, and the same with `Prefix`, are true. On `View`, `Group`, `TransactionalId` and `Cluster` → false.
    - `CheckedWritesImportGrant.OpsOnCarriesBitSevenAndImpliesDescribe`: `opsOn({{Import, Topic "user"}}, Topic, "user") == (0x80 | opBit(Describe))`.
    - `CheckedWritesImportGrant.NothingImpliesImport`: grants `{Write, Alter, Create, Drop} on Topic "user"` plus `{Admin, Create, Alter} on Cluster` → `(opsOn(…, Topic, "user") & 0x80) == 0`.
  - `SecurityEventTest.cpp`:
    - `CheckedWritesImportGrant.EventRoundTrips`: a `GrantOps` with op 7 encodes and decodes equal. Op 8 → `BadEnum`.
    - `CheckedWritesImportGrantRegression.ExistingGoldenLogsReplayUnchangedRegression`: the security golden digests (`SecurityGoldenPins.h`) are unchanged.
  - `SecurityStatementsTest.cpp`:
    - `CheckedWritesImportGrant.ParsesGrantAndRevokeImport`: `"grant import on topic 'user' to user backfill;"` and `"revoke import on topic prefix 'world.' from role r;"` → op `Import`, the right patterns.
    - The `"GRANT FLY …"` row's expected text now lists `'import'`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-codec -j14 --target rune_security_tests && cmake --build build/sub-ql -j14 --target rune_ql_tests`. Expected: `no member named 'Import' in 'rune::protocol::SecurityOperation'`.
- [ ] **Step 3: Implement** as specified. `"IMPORT"` is the name in all three name tables.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-codec -R 'CheckedWritesImport|Security' --output-on-failure` and `ctest --test-dir build/sub-ql -R 'SecurityStatements|CheckedWritesImport' --output-on-failure`. Expected: PASS. The security golden tests pass unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add protocol/include/rune/protocol/SecurityTypes.h codec/src/security codec/tests/security ql/src/Parser.cpp \
          ql/tests/SecurityStatementsTest.cpp
  git commit -m "feat(security): the IMPORT topic operation (C17) - protocol enum, grant rules, names, QL grant/revoke import" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 3: `IMPORT` on the broker: gateway table, shipping, worker bits, the op flag, `WriteFacts`

**Files:**
- Modify: `broker/libs/gateway/include/rune/gateway/PermissionTable.h:34-57`, `broker/libs/gateway/src/PermissionTable.cpp:8-46` (`importBits_` mirrored beside `writeBits_` in `setOps_` / `onObjectDropped`), `broker/libs/gateway/src/Authorizer.cpp:19`
- Modify: `broker/libs/gateway/src/ClientGateway.cpp`:
  - `:8200-8275`: every `shipTable` / `patchTable` is followed by `shipImportTable` / `patchImportTable` **only when** the table's import bitmap has any bit (most tables have none);
  - the gateway-routed produce: set `kOpFlagImportGranted` when `perms.allows(topicId, 0x80)` or the table is allow-all.
- Modify: `broker/libs/node/include/rune/node/WorkerWriteGrants.h` (`importBits`, `allowsImport`, `kPermFlagImport = 0x80`, `PermissionStager` routes `kPermFlagImport` chunks to `installImportGrants`), `broker/libs/node/include/rune/node/PermissionShipper.h` (the two methods; a new item `Kind::ImportTable` / `Kind::ImportPatch` shipped with `kPermFlagImport`), `broker/libs/node/include/rune/node/SessionAckSink.h` (`installImportGrants`, `patchImportTable` beside `installWriteGrants` / `patchWriteTable`), `broker/libs/node/include/rune/node/WorkerOp.h:657` (`kOpFlagImportGranted = 0x10`)
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - the permission op dispatch (`:2744-2757`);
  - `handleWorkerIngressFrame_` (`:8016-8085`): `f.importGranted = !enforcing || allowsImport(*grants, msg.topicId)`;
  - `executeProduceWire_` (`:3944-3961`): from `kOpFlagImportGranted`;
  - `ProduceFields` appends `bool importGranted = false;`;
  - `produceCore_` builds `WriteFacts{…, .importGranted = f.importGranted, .producerId = f.producerId}`.
- Modify: `broker/libs/node/include/rune/node/SecurityAdmin.h`, `broker/libs/node/src/SecurityAdmin.cpp:380-395` (P6-14 level gate; `SecurityAdmin` reads the level through a `const int16_t*` to `AdminWorker::clusterFeatureLevel_`, set at construction)
- Test: `broker/libs/node/tests/node/WorkerWriteGrantsTest.cpp`, `broker/libs/gateway/tests/PermissionTableTest.cpp`, `broker/libs/gateway/tests/gateway/CatalogGatewayTest.cpp`, `broker/libs/node/tests/node/SecurityWritePathTest.cpp`, `broker/libs/node/tests/node/CheckedWritesAutoIncWorkerTest.cpp` (create; add to `rune_checked_writes_verdict_tests`)

**Interfaces:**
- Consumes: Task 2; 03's `WriteFacts::importGranted`.
- Produces: contract addition 10 (broker half) and `WriteFacts::producerId` (contract addition 7). On every produce path the worker knows whether the session may `IMPORT` the topic.

- [ ] **Step 1: Write the failing tests.**
  - `WorkerWriteGrantsTest.cpp`:
    - `CheckedWritesImportGrant.StagerInstallsTheImportBitmapSeparately`: a table's write chunks, then its import chunk (`kPermFlagTable | kPermFlagImport | kPermFlagLast`) → `allowsWrite(slot, 5) && allowsImport(slot, 5)`, and `!allowsImport(slot, 6)`.
    - `CheckedWritesImportGrant.TableWithoutImportItemDeniesImport`: `allowsImport == false` for every id.
    - `CheckedWritesImportGrant.GrantAllAllowsImport`: `allowAllWriteTable()` → true.
    - `CheckedWritesImportGrant.ImportPatchFlipsOneBit`.
    - `CheckedWritesImportGrantRegression.WriteTablesShipUnchangedRegression`: a table with no import bits ships exactly the item sequence it shipped before this task (the `PermissionShipper` queue compared item by item).
  - `PermissionTableTest.cpp`, `CheckedWritesImportGrant.TableMirrorsImportBits`: a grant set `{Write, Import} on 'user'` compiled over an index holding `user` (id 3) and `char` (id 4) → `importBits()` has bit 3 only; `onObjectDropped(3)` clears it; an allow-all table answers `allows(3, 0x80)`.
  - `CatalogGatewayTest.cpp`, `CheckedWritesImportGrant.GatewaySetsTheOpFlagOnlyWithTheGrant`: a session with `{Write, Import}` on `user` → the routed op has `kOpFlagImportGranted`. With `{Write}` only → it doesn't.
  - `CheckedWritesAutoIncWorkerTest.cpp`, `CheckedWritesImportGrant.ShardIngressFactsCarryTheGrant`: an `Enforcing` rig worker with a bound table granting `Write` on topic 1 and `Import` on topic 2 → the hook sees `importGranted` false on 1 and true on 2 (`Worker::lastWriteFactsForTest()`, a test seam beside 03's `hookChecksForTest`).
  - `SecurityWritePathTest.cpp` (the existing `SecurityAdmin` write-path rig), `CheckedWritesImportGrant.GrantImportBelowLevelFourIsRefused`: level 3 → `InvalidRequest`, "the IMPORT grant needs every broker at feature level 4", nothing appended. Level 4 → committed.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_worker_write_grants_tests rune_gateway_permission_table_tests rune_gateway_catalog_tests rune_checked_writes_verdict_tests rune_node_tests`. Expected: `no member named 'importBits'`.
- [ ] **Step 3: Implement** as specified. The hot-path cost is one bit load beside the existing write-bit load on the shard ingress, and one flag test on the op path.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesImport|WorkerWriteGrants|PermissionShipper|PermissionTable|CatalogGateway|SecurityWritePath|AuthAlloc' -j14 --output-on-failure`. Expected: PASS. `AuthAllocRegression` stays at zero allocations.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/gateway broker/libs/node/include/rune/node/WorkerWriteGrants.h \
          broker/libs/node/include/rune/node/PermissionShipper.h broker/libs/node/include/rune/node/SessionAckSink.h \
          broker/libs/node/include/rune/node/WorkerOp.h broker/libs/node/include/rune/node/Worker.h \
          broker/libs/node/include/rune/node/SecurityAdmin.h broker/libs/node/src/SecurityAdmin.cpp \
          broker/libs/node/tests/node/WorkerWriteGrantsTest.cpp broker/libs/node/tests/node/SecurityWritePathTest.cpp \
          broker/libs/node/tests/node/CheckedWritesAutoIncWorkerTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "feat(auth): IMPORT bits from the gateway table to every worker (kPermFlagImport, kOpFlagImportGranted) into WriteFacts; level-4 gate" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 4: `autoincrement` DDL, the `ApplyV2` rules, the write-spec fields and the v2 autoincrement golden

**Files:**
- Modify: `broker/libs/node/src/AdminWorker.cpp`:
  - `buildDefine_` (`:1090-1172`): P6-5 for the adoption and create branches, normalization, 05's P5-6 rule 4 deleted;
  - `buildAlterConfig_` (`:1331-1347`): rule 6 on an autoincrement topic, evaluated on the resulting config through `cat::applyConfigChanges`;
  - `respondGet` passes the stored features (05 wired them).
- Modify: `codec/src/catalog/ApplyV2.cpp` (P6-6, after 05's rules), `codec/include/rune/catalog/testing/GoldenScript.h` (`goldenScriptV2AutoInc()`), `codec/tools/make_catalog_golden.cpp` (`--v2-autoinc`), `codec/tests/catalog/CatalogGoldenPins.h` (`kAutoIncV2Digests`, "FROZEN"), `codec/tests/catalog/CatalogGoldenTest.cpp`
- Create: `codec/tests/catalog/golden/v2/autoinc.rcat`
- Modify: `broker/libs/node/include/rune/node/WriteState.h` (`TopicWriteSpecWire` fields, `encode` / `decode`, `writeSpecWireOf`, `TopicWriteSpec` members set in `setFacts`; `autoIncrementMax` is stored effective)
- Test: `codec/tests/catalog/CheckedWritesCatalogV2Test.cpp`, `broker/libs/node/tests/node/CheckedWritesKeyIndexDdlTest.cpp` (05's refusal rows for autoincrement removed; a deviation note), `broker/libs/node/tests/node/CheckedWritesAutoIncDdlTest.cpp` (create; in `rune_checked_writes_tests`, driving `CatalogDdlRig`), `broker/libs/node/tests/node/CheckedWritesWriteStateTest.cpp`

**Interfaces:**
- Consumes: 02's `featureRefusal`, `topicShapeOf`, `isTopicKeyType`; 05's `TopicKeyFeatures` codec, `ApplyV2`, `writeSpecWireOf`, `TopicWriteSpecWire`; Task 1's catalog effect.
- Produces:
  - every worker's `TopicWriteSpec` for an autoincrement topic has `autoIncrement`, `partitioner = Modulo`, `partitionCount = P`, the effective max and the reserve, delivered by 02's `WriteSpecUpdate` before the topic serves (02 P7);
  - `goldenScriptV2AutoInc()` has 5 events: `CreateTopic user {userId uint64 key, provider uint16, subject string(64), createdAt int64}` autoincrement; `CreateTopic character {characterId uint64 key, userId uint64, name string(16), createdAt int64}` autoincrement with `reserve 128`; `AlterSchema` ADD on `character`; `AlterTopicConfig` `delete.retention.ms` on `user`; `CreateTopic ai32 {id uint32 key}` autoincrement P 3.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesAutoIncDdlTest.cpp` (each case after `becomeServingLeader()`, at level 4 unless named):
    - `CheckedWritesAutoIncDdl.RegressorsPathTopicsAreAccepted`:
      - `user` as in the golden, P 1, `cleanup.policy = compact`, `{.autoIncrement = true}` → code 0;
      - the committed event is `formatVer 2` with `keyIndex`, `autoIncrement`, `Modulo`, max `2^53 − 1` and reserve 4096;
      - `writeSpecWireOf(*t)` is 26 bytes and decodes to `{autoIncrement, Modulo, 9007199254740991, 4096, partitionCount 1}`;
      - the same for `character`;
      - `getSchema("user", 0)` → `hasKeyFeatures`, Modulo, and `partitionCount == 1`.
    - `CheckedWritesAutoIncDdl.Uint32DefaultMaxIsTheTypeMax`: `{id uint32 key}` → stored max `4294967295`.
    - `CheckedWritesAutoIncDdl.Refusals` (`TEST_P`, each → 39 and the exact P6-5 detail):
      - level 3;
      - untyped → "autoincrement needs a typed topic";
      - typed keyless → "autoincrement needs a key";
      - transient → "autoincrement needs a persisted topic";
      - an adoption of an untyped disk topic → "autoincrement needs a new topic";
      - key `uint16`, `int32` and `string(8)` → "autoincrement needs a uint64, int64 or uint32 key";
      - `cleanup.policy` 0 and 2 → "autoincrement needs cleanup.policy = compact";
      - `retention.ms = 1000` and `retention.bytes = 1` → the retention detail;
      - reserve 0 and 1,048,577 → the reserve detail;
      - P 4 with max 6 → "autoincrement.max must be between 7 and 9007199254740991";
      - `uint32` with max `2^32` → "… between 1 and 4294967295";
      - Modulo without autoincrement → "the modulo partitioner needs autoincrement";
      - reserve 8 without autoincrement → "autoincrement.max and autoincrement.reserve need autoincrement".
    - `CheckedWritesAutoIncDdl.MaxAtTheLowerBoundIsAccepted`: P 4, max 7 → code 0.
    - `CheckedWritesAutoIncDdl.RequestedPartitionerAndKeyIndexAreNormalized`: `{.autoIncrement = true, .keyIndex = false, .partitioner = Fnv1a}` → stored `keyIndex`, Modulo.
    - `CheckedWritesAutoIncDdlRegression.AlterConfigCannotAddRetentionRegression`: `AlterTopicConfig retention.ms = 60000` on `user` → 39, the retention detail. `delete.retention.ms = 0` → code 0.
    - `CheckedWritesAutoIncDdlRegression.AlterSchemaKeepsAutoIncrementRegression`: ALTER ADD on `character` → features unchanged in the catalog and in `writeSpecWireOf`.
    - `CheckedWritesAutoIncDdl.CatalogTopicSpecHasNoLogStartMove`: `topicSpecFromCatalog(applied user)` → `logStartOffset == 0`, `retentionMs == retentionBytes == -1`, `cleanupPolicy == Compact`.
  - `CheckedWritesCatalogV2Test.cpp`:
    - `CheckedWritesCatalogV2.AutoIncrementApplyRules` (`TEST_P` over P6-6, each → `Precondition`): a hand-built v2 `CreateTopic` with `autoIncrement` and `keyIndex = false`, with `Fnv1a`, with policy 0, with retention 1, with a `uint16` key, a transient one, a `CreateSchema` with `autoIncrement`, and a `Modulo` without `autoIncrement`.
    - `CheckedWritesCatalogV2.AutoIncrementTopicAppliesAndFingerprints`: the golden `user` event applies, and its fingerprint differs from the same topic without features.
  - `CatalogGoldenTest.cpp`: `CatalogGoldenRegression.V2AutoIncLogReplaysToPinnedFingerprints`, `…TheV2AutoIncScriptStillProducesTheCheckedInBytes`, `…TheV2AutoIncLogDecodesToTheScriptEventByEvent`. 05's three v2 tests and the three v1 tests stay unchanged and green.
  - `CheckedWritesWriteStateTest.cpp`, `CheckedWritesWriteState.AutoIncFieldsRoundTrip`: 26 bytes, flags bit 2, the fields equal. Partitioner 2 → `nullopt`. A 25-byte tail → `nullopt`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests && cmake --build build/sub-codec -j14 --target rune_catalog_tests`. Expected: FAIL. Today's text "autoincrement is not supported by this broker yet" is answered for every autoincrement request.
- [ ] **Step 3: Implement** as specified. Generate the golden once: `cmake --build build/sub-codec --target make_catalog_golden && build/sub-codec/make_catalog_golden --v2-autoinc codec/tests/catalog/golden/v2/autoinc.rcat`. Paste the printed array as `kAutoIncV2Digests`.
- [ ] **Step 4: Run.**
  - `ctest --test-dir build/root -R 'CheckedWritesAutoIncDdl|CheckedWritesKeyIndexDdl|CheckedWritesDdl|CheckedWritesWriteState|AdminWorker|DdlOccupant|DdlStress' -j14 --output-on-failure`
  - `ctest --test-dir build/sub-codec -R 'CheckedWritesCatalogV2|CatalogGolden|CatalogApply|CatalogEvent|CatalogFingerprint' --output-on-failure`
  - Expected: PASS. 05's v2 golden passes byte-identical (R05d).
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/src/AdminWorker.cpp broker/libs/node/include/rune/node/WriteState.h codec/src/catalog/ApplyV2.cpp \
          codec/include/rune/catalog/testing/GoldenScript.h codec/tools/make_catalog_golden.cpp codec/tests/catalog \
          broker/libs/node/tests/node/CheckedWritesAutoIncDdlTest.cpp broker/libs/node/tests/node/CheckedWritesKeyIndexDdlTest.cpp \
          broker/libs/node/tests/node/CheckedWritesWriteStateTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "feat(admin): autoincrement DDL and ApplyV2 rules (compact-only, no retention, key types, ranges), normalization, write-spec fields, v2 autoinc golden" \
             -m "Deviation (05 Task 5): its 'autoincrement is not supported by this broker yet' rows are replaced by this part's rules." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 5: The client surface: routing, features, sticky key-0, homogeneous batches, local refusals, the version gate

**Files:**
- Modify: `client-cpp/include/rune/client/TopicHandle.h` (`partitioner` after `kind`), `client-cpp/include/rune/client/Client.h:156-165` (`TopicSchemaResult`), `:1060-1105` (`openTopicAndWait` builds `TopicHandle{r.topicId, typedPartitionCount(r.partitionCount), TopicKind::Typed, r.keyFeatures.partitioner}`), `client-cpp/src/Client.cpp` (`getTopicSchemaAndWait` copies the 05 block; `:4954` routes with `partitionFor`)
- Modify: `client-cpp/include/rune/client/producer/Producer.h` (`isKeyZeroCreate`, `kKeyZeroStickyRun`), `client-cpp/src/producer/Producer.cpp:150-260,340-400` (every `key.partition(topic.partitionCount)` becomes the routing rule of P6-11; P6-12 refusals beside 05's, amending them), `client-cpp/include/rune/client/shard/ClientShard.h:102,340`, `client-cpp/src/shard/ClientShard.cpp:62`, `client-cpp/include/rune/client/metadata/PartitionRouter.h:86` (the topic's partitioner is registered with its count)
- Modify: `client-cpp/include/rune/client/producer/ProducerImpl.h` (`kFlagKeyZero`; `PartitionState` appends `uint8_t openKind = 0; uint32_t keyZeroStride = 0;`), `client-cpp/src/producer/ProducerImpl.cpp` (`admitRecord_` `:1547-1580`: a kind change sets `flushRequested` and ABORTs; `encodeAndSend`: a key-0 batch stamps `InFlightBatch::idStride`, and its seal gate fails it `BrokerTooOld` when the seed's `brokerVersion` is in 1–6), `client-cpp/include/rune/client/producer/InFlightRing.h:77`
- Create: `client-cpp/tests/producer/CheckedWritesAutoIncClientTest.cpp` (in `rune_invoker_tests`)

**Interfaces:**
- Consumes: Task 1's `partitionFor`; 05's `WriteIntent`, `TopicKind`, local refusal machinery and the define options; 03's seal gate and `setBrokerVersion`.
- Produces: contract addition 2, except the ack-side members (Task 6). `isKeyZeroCreate(h, k, i)` is `h.partitioner == Modulo && i == WriteIntent::Insert && !k.empty() && every byte of k == 0`.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesAutoIncClientTest.cpp`; rigs as in 05's `CheckedWritesIntentsTest.cpp`, `support/FrameCapture.h`; an idempotent `All` producer with `brokerVersion 7` unless named).
  - `CheckedWritesAutoIncClient.ModuloHandleRoutesExplicitIdsByModulo`: `TopicHandle{.topicId = 3, .partitionCount = 4, .kind = Typed, .partitioner = Modulo}`, `send(Key(uint64_t{4097}), v, Update)` → captured on partition 1.
  - `CheckedWritesAutoIncClient.KeyZeroCreatesAreSticky`: 130 key-0 creates on P 4 → records 0–63 on partition 0, 64–127 on 1, 128–129 on 2.
  - `CheckedWritesAutoIncClient.KeyZeroBatchesAreHomogeneous`: on P 1, `[create, create, update 5, create]` with one flush at the end → three frames `[2 creates]`, `[update]`, `[1 create]`. Every frame's records are all key-0 or all not, and the key-0 frames' `InFlightBatch::idStride == 1`.
  - `CheckedWritesAutoIncClient.LocalRefusals`: each case is `WriteNotAllowed`, with no frame and no completion, and the blocking forms throw with the P6-12 text:
    - a non-idempotent producer creating;
    - an `acks=leader` producer creating;
    - a live `Upsert` on a Modulo handle.
  - `CheckedWritesAutoIncClient.KeyZeroCreateNeedsNoViewAndWorksInATransaction`: an `All` producer (no `View`) creating → `Ok` and a frame. A transactional `All` producer creating → `Ok` (the 05 refusals don't apply).
  - `CheckedWritesAutoIncClient.KeyZeroToAnOldBrokerIsBrokerTooOld`: `setBrokerVersion(6)` → no frame, and one `OpStatus::BrokerTooOld` completion per record.
  - `CheckedWritesAutoIncClientRegression.FnvHandlesRouteAsBeforeRegression`: 1,000 random sends on a default handle route as `key.partition(P)` did.
  - `CheckedWritesAutoIncClient.OpenTopicCarriesThePartitioner`: a fake schema response with the 05 block (Modulo) → `TypedTopic::handle().partitioner == Modulo`. Without a block → `Fnv1a`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_invoker_tests`. Expected: `no member named 'partitioner' in 'rune::client::TopicHandle'`.
- [ ] **Step 3: Implement** as specified. The sticky counter lives on the `Producer` front-end (app thread), beside the handle check. A non-Modulo handle pays one predicted branch.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesAutoIncClient|CheckedWritesIntents|ProducerSend|ProducerBatching|ProducerAlloc|ClientPerWorkerRouting|Shard|KeyRouting|TypedTopic' -j14 --output-on-failure`. Expected: PASS, with `ProducerAllocRegressionTest` at zero allocations.
- [ ] **Step 5: Commit.**
  ```bash
  git add client-cpp/include/rune/client/TopicHandle.h client-cpp/include/rune/client/Client.h client-cpp/src/Client.cpp \
          client-cpp/include/rune/client/producer client-cpp/src/producer client-cpp/include/rune/client/shard \
          client-cpp/src/shard client-cpp/include/rune/client/metadata/PartitionRouter.h \
          client-cpp/tests/producer/CheckedWritesAutoIncClientTest.cpp client-cpp/CMakeLists.txt
  git commit -m "feat(client): modulo routing, sticky homogeneous key-0 batches, autoincrement local refusals and the SD11 gate" \
             -m "Deviation (05 P5-9): key-0 creates are exempt from 'intents need View' and the transaction refusal (spec 11.2)." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 6: Client acks: `assignedBase`, the id of every record, `StoredIdUnknown`, `WriteResult::key()`

**Files:**
- Modify: `client-cpp/include/rune/client/Completion.h:54` (`StoredIdUnknown = 13`, doc "stored; its id is unknown (the retry fell outside the dedupe window)"), `client-cpp/include/rune/client/agent/ClientAgent.h` (`ProduceAck::assignedBase`), `client-cpp/src/agent/ClientAgent.cpp` (copy it from 03's decode), `client-cpp/src/Client.cpp` (the ack dispatch), `client-cpp/include/rune/client/producer/ProducerHub.h`, `client-cpp/src/producer/ProducerHub.cpp`, `client-cpp/include/rune/client/producer/ProducerImpl.h`, `client-cpp/src/producer/ProducerImpl.cpp` (`onProduceResponse` `:2350-2390`, `completeSlice_` `:2761-2782`: P6-10)
- Modify: `client-cpp/include/rune/client/TypedTopic.h` (no API change: `insert` / `insertAndWait` already send intent 1; a `rec.key() == 0` on a Modulo handle is a key-0 create through Task 5)
- Create: `client-cpp/tests/client/CheckedWritesAutoIncTypedTest.cpp` (in `rune_client_tests`). Extend `CheckedWritesAutoIncClientTest.cpp`.

**Interfaces:**
- Consumes: 03's `ProduceResponse::assignedBase` decode, 04's per-record override and `VerdictUnknown` mapping, 05's `WriteResult::fromCompletion` and `assignedKeyOf`.
- Produces: P6-10. `Completion::aux` is the assigned id for a `Send` of a key-0 batch, and 0 otherwise. `WriteResult::key()` returns it (C6).

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesAutoIncClientTest.cpp` (a fake session answering v12 frames):
    - `CheckedWritesAutoIncClient.EachRecordGetsBasePlusIStride`: P 4, 5 key-0 creates in one frame, ack `{OK, base 100, assignedBase 9}` → completions `value 100..104`, `aux 9, 13, 17, 21, 25`.
    - `CheckedWritesAutoIncClient.RejectedCreateHasNoId`: the trailer `{(1, 51, 0x00070001)}` → record 1 is `BrokerError 51` with `aux 0`, the others `Ok` with their ids.
    - `CheckedWritesAutoIncClientRegression.LostAckRetryCompletesOnceWithTheOriginalIdsRegression`: the first ack is dropped and the retry's ack carries the same `assignedBase` → each record completes exactly once with its id.
    - `CheckedWritesAutoIncClient.StoredIdUnknownCases` (`TEST_P`): each of these → every record `StoredIdUnknown`, `errorCode` 21 or 0 as received, never `Ok`, `VerdictUnknown` or `BrokerError`:
      - `DuplicateSequence` on a `View` producer;
      - `OK, baseOffset −1` on an `All` producer;
      - `OK, assignedBase 0`.
    - `CheckedWritesAutoIncClientRegression.NonKeyZeroBatchesKeepTheirMappingRegression`: an update batch answered `DuplicateSequence` → `VerdictUnknown` (04). `OK −1` on an `All` producer → `Ok −1`. `aux` is 0 throughout.
  - `CheckedWritesAutoIncTypedTest.cpp` (the stub `ProducerT` of 05's typed tests):
    - `CheckedWritesAutoIncTyped.InsertAndWaitKeyIsTheAssignedId`: a scripted completion `{Ok, value 7, aux 42}` → `insertAndWait(rec{.id = 0}).key() == 42`, `offset() == 7`.
    - `CheckedWritesAutoIncTyped.StoredIdUnknownSurfacesAsStatus`: `{StoredIdUnknown}` → `!ok()`, `status() == StoredIdUnknown`, `key() == 0`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/sub-client-cpp -j14 --target rune_invoker_tests rune_client_tests`. Expected: `no member named 'StoredIdUnknown' in 'rune::client::OpStatus'`.
- [ ] **Step 3: Implement** as specified. The id loop adds one multiply per record on key-0 batches only. Other batches take today's path with one `idStride == 0` test.
- [ ] **Step 4: Run.** `ctest --test-dir build/sub-client-cpp -R 'CheckedWritesAutoInc|ProducerVerdicts|ProducerAck|ProducerEos|TypedTopic|CheckedWritesTypedTopic' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add client-cpp/include/rune/client/Completion.h client-cpp/include/rune/client/agent/ClientAgent.h \
          client-cpp/src/agent/ClientAgent.cpp client-cpp/src/Client.cpp client-cpp/include/rune/client/producer \
          client-cpp/src/producer client-cpp/include/rune/client/TypedTopic.h \
          client-cpp/tests/producer/CheckedWritesAutoIncClientTest.cpp client-cpp/tests/client/CheckedWritesAutoIncTypedTest.cpp \
          client-cpp/CMakeLists.txt
  git commit -m "feat(client): assigned ids per record (base + i*P), StoredIdUnknown (13), insertAndWait(rec).key()" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint A (controller).**
  - `cmake --build build/root -j14 && ctest --test-dir build/root -R Regression -LE latency -j14 --output-on-failure`
  - `ctest --test-dir build/sub-client-cpp -R Regression -LE latency -j14 --output-on-failure`
  - `cmake --build build/sub-codec -j14 && ctest --test-dir build/sub-codec -R 'Regression|CatalogGolden|CheckedWrites|Security' --output-on-failure`. CatalogGolden must show the v1, the 05 v2 and the 06 v2 autoincrement tests passing.
  - `ctest --test-dir build/sub-ql -R 'Security|CheckedWrites' --output-on-failure`
  - `build/asan` (`ASAN_OPTIONS=detect_leaks=0` on macOS x86_64 under Rosetta, then `leaks(1)` on `rune_catalog_tests`), `build/tsan` and `build/client-tsan` with `-R Regression -LE latency`.
  - Expected: 100 % pass and no sanitizer report. A failure is fixed with a `*Regression*` test, and then only the failed and affected tests are rerun.

---

## Phase 2 — The broker: counter, hook, worker, cleaner

### Task 7: `AutoInc.h` (counter, plan, side window, stats) and the reservation record

**Files:**
- Create: `broker/libs/node/include/rune/node/AutoInc.h` (header-only; included by `WriteState.h`, which appends C7's `TopicWriteSpec` and `PartitionWriteState` members)
- Modify: `broker/libs/storage/include/rune/storage/ControlRecordLayout.h` (contract addition 6), `broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp`
- Create: `broker/libs/node/tests/node/CheckedWritesAutoIncStateTest.cpp` (in `rune_checked_writes_tests`)

**Interfaces:**
- Consumes: C10's batch form and `recordType`.
- Produces: contract additions 5 and 6, with P6-7 and P6-8 as their behavior.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesControlRecordTest.cpp`:
    - `CheckedWritesControlRecord.ReservationRoundTrips`: `buildReservation(out, 8192) == reservationBatchBytes()` → `recordType == 16`, `parseReservation == 8192`, CRC valid, `producerId −1`.
    - A 1-byte-short `out` → 0.
    - A verdict marker given to `parseReservation` → `nullopt`.
  - `CheckedWritesAutoIncStateTest.cpp`:
    - `CheckedWritesAutoIncState.FreshTopicStartsAtOne`: `configure(0, 1, 2^53−1, 4096)`, `beginScan()`, `finishScan() == 0` → `nextN 1`, `R 0`. `plan(1, 0)` → `{firstN 1, count 1, newNextN 2, reserve, reserveThroughN 4096}`.
    - `CheckedWritesAutoIncState.ResumesAboveTheLatestReservation`: scan sees `onReservation(4096)`, `onReservation(8192)`, `onAssignedThrough(5000)` → `nextN 8193`, `R 8192`, `finishScan() == 3192` (the burned gap).
    - `CheckedWritesAutoIncState.CrossingTheReservationReservesAFullBlock`: `R 8`, `nextN 7`, reserve 8, `plan(8, 0)` → `{7, 8, 15, reserve, 16}`.
    - `CheckedWritesAutoIncState.IdsFollowTheStride`: P 4, p 2 → `autoIncId(1, 4, 2) == 6`, `autoIncId(2, 4, 2) == 10`. P 1 gives 1, 2, 3.
    - `CheckedWritesAutoIncState.ExhaustionIsExactAtTheCap`:
      - P 1, max 10, `nextN 8` → `plan(3, 0).exhausted == false` (ids 8–10) and `plan(4, 0).exhausted`;
      - P 4, p 3, max 10 → `nMax == 1` (id 7);
      - max 2, p 3 → `nMax == 0`, every plan exhausted;
      - the reservation never exceeds `nMax`.
    - `CheckedWritesAutoIncState.ImportMovesTheCounterPastIt`: P 2, p 1, `nextN 5`, `R 4096`:
      - `plan(0, 50)` → `newNextN 51`, no reserve;
      - `plan(0, 5000)` → `newNextN 5001`, `reserveThroughN = max(5000, 4096 + 4096) = 8192`;
      - `plan(0, 3)` → `newNextN 5`.
    - `CheckedWritesAutoIncState.NotReadyOutsideReady`: `Inactive` and `Scanning` → `ready() == false`. `deactivate()` from `Ready` → `Inactive`.
    - `CheckedWritesAutoIncWindow.RecordAndFind`: pid 7 at offsets 0, 10, …, 330 (34 batches) → `find(7, 330)` is its base, `find(7, 0) == 0` (pushed out after 32), `find(8, 10) == 0`.
    - `CheckedWritesAutoIncWindow.CanAcceptAtCapacity`: capacity 4 → four pids accepted, a fifth `canAccept == false`, a known pid still `true`. `grow(8)` → the fifth is accepted and every entry is found.
    - `CheckedWritesAutoIncWindow.RecordAllocatesNothing`: after `reserve(1024)`, 100,000 `record` calls over 512 pids under `rune::testing::AllocWindow` → 0.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests rune_storage_tests`. Expected: `rune/node/AutoInc.h` not found.
- [ ] **Step 3: Implement** as specified. `plan()` is pure. The reserve rule is the one P6-7 states:
  ```
  highestN = max(count > 0 ? nextN + count - 1 : 0, importMaxN)
  exhausted = count > 0 && nextN + count - 1 > nMax
  reserve   = !exhausted && highestN > R
  reserveThroughN = min(nMax, max(highestN, R + reserveStep))
  newNextN  = max(nextN + count, importMaxN + 1)
  ```
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesAutoIncState|CheckedWritesAutoIncWindow|CheckedWritesControlRecord' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/AutoInc.h broker/libs/node/include/rune/node/WriteState.h \
          broker/libs/storage/include/rune/storage/ControlRecordLayout.h \
          broker/libs/storage/tests/storage/CheckedWritesControlRecordTest.cpp \
          broker/libs/node/tests/node/CheckedWritesAutoIncStateTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): AutoIncState (inactive/scanning/ready, plan, reservation blocks), the assigned-base side window, the reservation control record" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 8: The hook: C-rules in `check()`, reservation + copy + patch in `stage()`, the window in `onAppended()`

**Files:**
- Modify: `broker/libs/node/include/rune/node/CheckedWriteHook.h`, `broker/libs/node/src/CheckedWriteHook.cpp` (contract addition 7; `topicHookFactsOf` sets `autoIncrement = spec->autoIncrement` and `autoIncReady = autoInc.ready()`)
- Modify: `broker/libs/node/tests/node/CheckedWritesHookTest.cpp` (pinned rows, sweep)
- Create: `broker/libs/node/tests/node/CheckedWritesAutoIncHookTest.cpp` (in `rune_checked_writes_tests`)

**Interfaces:**
- Consumes: Task 7; 02's `SpecVersion::keyValueOffset` and `descriptorFor`; 03's `RecordSpan::{attributes, key, index}`, `HookStaged`, `assignedBase()`; C10's `buildReservation`.
- Produces:
  - **`check()`** fills the new `BatchIntents` fields in its one walk:
    - `plainUpsert`: a live record with intent 0 on an autoincrement spec;
    - `explicitInsert`: intent 1 with a non-zero key;
    - `importBadKey` (P6-4 8c);
    - `keyZeroCount`, `importMaxN`;
    - `autoIncExhausted` from `state.autoInc.plan(keyZeroCount, importMaxN).exhausted` when ready;
    - `autoIncWindowFull` from `!state.assignedBases.canAccept(facts.producerId)` on a key-0 batch.

    It keeps the plan in the hook object for `stage()`. Nothing is mutated.
  - **`stage(log, batch, len)`** runs only when `keyZeroCount > 0` or `plan.reserve`. Otherwise it returns `{OK, batch, len}`. In order:
    1. If `plan.reserve`: `buildReservation(resv_, plan.reserveThroughN)` into the hook's fixed `std::array<uint8_t, kReservationScratch>` member (sized by a `static_assert` against `reservationBatchBytes()`), then `log.appendNoSyncRestamp(resv_.data(), n, 0, off)`. An exception → `{Internal}`, nothing else. Then `autoInc.commitReservation(…)` and `++reservationsAppended`.
    2. If `keyZeroCount > 0`: `std::memcpy` into the scratch (`len ≤ scratch.size()`, else `{Internal}` with a rate-limited error: unreachable, because the scratch is `maxIntakeBatchBytes_`).
    3. For each record i: write `autoIncId(firstN + i, P, p)` little-endian at `keyWidth` (4 or 8) over the record key (offset `key.data() - batch`) and over the value's key field (the record's value offset + `descriptorFor(version)`'s `keyValueOffset`).
    4. `crc32c(scratch + kAttributesOffset, len − kAttributesOffset)` into the CRC field.
    5. `autoInc.commitNext(plan.newNextN)`, then return `{OK, scratch.data(), len}`.

    An import-only batch (`importMaxN > 0`) commits `newNextN` with no copy. `++importsMovedCounter` when it moved the counter.
  - **`onAppended(base, last)`:** `assignedBase_ = first record's intent == Insert ? u64(firstKey) : 0` (P6-3; for a key-0 batch, `autoIncId(firstN, P, p)`). If `assignedBase_ != 0 && facts.producerId >= 0`, then `assignedBases.record(pid, base, assignedBase_)`. On a key-0 batch, `++keyZeroBatches`.
  - **`assignedBase()`** returns `assignedBase_` (03's fourth member). `NoWriteHook` stays 0.
  - A non-autoincrement spec takes none of these branches beyond one `[[unlikely]] if (spec.autoIncrement)` per walk.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesHookTest.cpp`, the pinned rows. Each is `{BatchIntents, WriteFacts, TopicHookFacts} → {code, why}`, on a topic with `keyIndex, autoIncrement, autoIncReady` unless noted:

    | # | Shape | Facts | Topic | Expected |
    |---|---|---|---|---|
    | 65 | plain upsert, acks 1 | idempotent, level | autoInc | `InvalidRequest` / `UpsertOnAutoIncrement` |
    | 66 | plain tombstone, acks 1 | idempotent, level | autoInc | `OK` |
    | 67 | explicit insert, acks 3 | idempotent, level, `!importGranted` | autoInc | `NotAuthorized` / `ImportNotGranted` |
    | 68 | explicit insert, acks 3 | idempotent, level, import | autoInc | `OK` |
    | 69 | explicit insert bad key, acks 3 | idempotent, level, import | autoInc | `InvalidRequest` / `ImportOutOfRange` |
    | 70 | key-0 creates, acks 1 | idempotent, level | autoInc, `!autoIncReady` | `NotLeader` / `AutoIncActivating` |
    | 71 | key-0 creates, acks 1, exhausted | idempotent, level | autoInc | `AutoIncrementExhausted` / `AutoIncExhausted` |
    | 72 | key-0 creates, acks 1, window full | idempotent, level | autoInc | `NotLeader` / `AutoIncWindowFull` |
    | 73 | key-0 creates, acks 1, exhausted | **not** idempotent | autoInc | `KeyZeroCreateNeedsIdempotentAll` (definite rules first) |
    | 74 | explicit insert, acks 1 | idempotent, level, import | autoInc | `ExplicitKeyNeedsIdempotentAndView` (05 rule 5 first) |
    | 75 | update, acks 3 | idempotent, level | autoInc | `OK` |
    | 76 | key-0 creates, acks 1 | legacy | autoInc | `LegacyMode` (SD10) |

    `CheckedWritesRequirementTable.SweepInvariants` covers Interaction matrix B (3,145,728 cells) with invariants (i) and (iv) amended. The amendment is a deviation of 03's text, recorded in the commit body.
  - `CheckedWritesAutoIncHookTest.cpp`. The `PartitionWriteState` is built with 02's spec builder for `{id uint64 key, v int64, tag uint8}` with autoincrement, P 1, `autoInc` driven to `Ready{1, 0}`, and a 64 KiB scratch, over an `InMemoryLog`-backed test log (03's hook tests):
    - `CheckedWritesAutoIncHook.KeyZeroBatchIsPatchedInPlaceOfTheCopy`: 3 creates →
      - the log holds a type-16 record `reservedThroughN 4096` at offset 0, then the data batch at 1;
      - the stored keys are `LE8(1), LE8(2), LE8(3)`, and each value's `id` field equals its key;
      - the CRC is valid (`RecordBatchReader::ensureValid`), `assignedBase() == 1`;
      - the caller's input buffer is byte-identical to before (never mutated).
    - `CheckedWritesAutoIncHook.SecondBatchReservesNothing`: then 2 creates → no new type-16 record, keys 4, 5.
    - `CheckedWritesAutoIncHook.Uint32KeysPatchFourBytes`: a `uint32` spec → 4-byte keys `LE4(1)`, `LE4(2)`.
    - `CheckedWritesAutoIncHook.StrideAndPartition`: P 4, p 2 → keys 6, 10, 14.
    - `CheckedWritesAutoIncHook.ExhaustedBatchAppendsNothing`: max 2, 3 creates → `check()` returns 54, the LEO and `nextN` are unchanged, and no reservation is appended.
    - `CheckedWritesAutoIncHook.ExplicitInsertWithoutImportIsNotAuthorized`: → 45, LEO unchanged.
    - `CheckedWritesAutoIncHook.ImportAboveTheReservationAppendsItFirst`: `R 4096`, an import of id 5000 (P 1) → a type-16 record `≥ 5000` right below the data. `nextN 5001`, `assignedBase() == 5000`.
    - `CheckedWritesAutoIncHook.ImportBadKeys`: P 2, p 0, id 3 → `ImportOutOfRange`. An `int64` id −5 → `ImportOutOfRange`. Id `2^53` → `ImportOutOfRange`.
    - `CheckedWritesAutoIncHook.KeyZeroWithANonZeroValueKeyIsSchemaValidationFailed`: key `LE8(0)`, value id 7 → 24 (02's placeholder rule).
    - `CheckedWritesAutoIncHook.WindowRecordsTheBase`: pid 11 → `assignedBases.find(11, base) == 1`.
    - `CheckedWritesAutoIncHook.ReservationAppendFailureIsInternalAndAssignsNothing`: the test log throws on its next append → `{Internal}`, `nextN` unchanged.
    - `CheckedWritesAutoIncHookRegression.NonAutoIncSpecIsUntouchedRegression`: a 02 spec without autoincrement → `stage()` returns the input pointer, and `assignedBase() == 0`.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_tests`. Expected: `no member named 'plainUpsert' in 'rune::node::BatchIntents'`.
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesAutoIncHook|CheckedWritesHook|CheckedWritesRequirementTable|ProducePath' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/CheckedWriteHook.h broker/libs/node/src/CheckedWriteHook.cpp \
          broker/libs/node/tests/node/CheckedWritesHookTest.cpp broker/libs/node/tests/node/CheckedWritesAutoIncHookTest.cpp \
          broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): autoincrement in the write hook - C-rules 8a-8f, reservation then scratch copy, id patch and CRC, assigned-base window" \
             -m "Deviation (03 Task 6): sweep invariants (i) and (iv) gain the autoincrement exceptions (NotLeader, NotAuthorized, 54)." \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 9: Worker wiring: install, activation and rebuild, duplicates, the ack, stats and the rig

**Files:**
- Modify: `broker/libs/node/include/rune/node/RcState.h:600-720` (`dataBatch` from `feedProducerState_` for data batches with `pid >= 0`, both scans; `end` on the `Ready` edge)
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `rebuildRcSlots_` (`:6364`) and the `WriteSpecUpdate` apply: an autoincrement spec configures `autoInc` (partition, P, effective max, reserve), reserves `assignedBases` at 1,024, and sizes the worker's `std::vector<uint8_t> autoIncScratch_` to `maxIntakeBatchBytes_` once (cold);
  - the verdict hooks of 04: `begin` → `autoInc.beginScan()` and `assignedBases.clear()`; `batch` (type 16) → `onReservation`; `dataBatch` → P6-3 on the batch's first record → `recordScanned` and `onAssignedThrough(lastN)`; `end` → `idsBurned += finishScan()`;
  - `executeBecomeFollowerWire_` (`:4703`) → `autoInc.deactivate()`;
  - `produceCore_`: the hook takes `autoIncScratch_` when `spec->autoIncrement`; `ret.assignedBase = outcome.assignedBase`;
  - the duplicate branch (an in-window duplicate on an autoincrement partition whose first record's key is all-zero): `ret.assignedBase = assignedBases.find(pid, outcome.baseOffset)`, and a 0 counts `windowMisses`. This applies to both the `All` park and 04's `View` replay park;
  - `emitProduceAck_` (`:2132-2148`): `a.assignedBase = ret.assignedBase`;
  - `maintainAutoIncWindows_()` (P6-8);
  - `autoIncStatsForTest`, `autoIncStateForTest`, `lastWriteFactsForTest`.
- Modify: `broker/libs/node/src/VerdictSource.cpp`:
  - in `judge`, on a reject (code ≠ 52) of an intent-1 record on an autoincrement partition → `++autoIncStats.idsBurned`;
  - a 52 on a record of a batch whose first key is that batch's `assignedBase` → `++assignedKeyExists` and `spdlog::critical("AUTOINC-ID-REUSE topic={} partition={} id={}")` (P6-13).
- Modify: `broker/libs/node/tests/support/CheckedWritesRig.h` (contract addition 13; `appendBatch` with `Rec{.id = 0, .intent = 1}` builds key-0 records; `Ack::assignedBase` decoded)
- Modify: `broker/libs/node/tests/node/CheckedWritesAutoIncWorkerTest.cpp`

**Interfaces:**
- Consumes: Tasks 3, 4, 7 and 8; 04's rig (`driveUntilJudged`, `failover`, `demotePromote`, `restart`, `truncateTo`, `compact`, `ack(corr)`, `holdJudge`), 04's verdict hooks and `PendingAcksView::replay`; 05's `createKeyIndex` path (an autoincrement topic is a key-index topic).
- Produces: `createAutoIncTopic(AutoIncShape)` builds the topic, sends the `WriteSpecUpdate` and drives the first scan to `Ready`.

- [ ] **Step 1: Write the failing tests** (`CheckedWritesAutoIncWorkerTest.cpp`; the rig, `createAutoIncTopic()`, an idempotent `All | View` v12 producer unless named).
  - `CheckedWritesAutoIncWorker.DenseIdsFromOne`:
    - 100 creates in batches of 1–8 → acked ids are exactly 1..100 in order;
    - the log holds one type-16 record (`4096`) before the first data batch;
    - `autoIncStatsForTest(1, 0)` gives counter id 101, reserved-through 4096 and burned 0.
  - `CheckedWritesAutoIncWorker.IdsAreNeverReusedAfterADelete`: creates 1–10, `removeExisting 10`, a create → 11.
  - `CheckedWritesAutoIncWorker.AllOnlyCreateOnAKeyIndexOnlyTopicIsAcked`: `acks 1` (All, no View) → acked at the HW with `assignedBase` and no trailer (`hasVerdicts` false).
  - `CheckedWritesAutoIncWorker.ActivationAnswersNotLeaderUntilTheScanEnds`: `restart()` with `holdJudge(true)` and the scan held mid-way (`RcState` budget 1 batch per pass) → a create is `NotLeader` (`AutoIncActivating` count 1). After the scan → accepted with an id above every earlier id.
  - `CheckedWritesAutoIncRegression.FailoverAroundAReservationResumesAboveItRegression`:
    - **(a) just before.** Reserve 8 and 8 creates (R 8, next 9), then `failover()` → the next create gets 9 and appends `R 16`.
    - **(b) just after.** From R 8 / next 9, a 1-create batch crosses: it appends `R 16` and takes id 9, both committed. Then `failover()` → the counter resumes at `R + 1 = 17`, so the next create gets 17, and `idsBurned` rose by 7 (16 − 9).
    - **(c)** The same as (b) but with `failover()` after the reservation committed and before its data's HW: the data is cut, and the next id is 17.
  - `CheckedWritesAutoIncRegression.TruncatedReservationTakesItsDataWithItRegression`: R 8, next 9, a crossing batch of 4 creates (`R → 16`, ids 9–12), `truncateTo(hw)` before the follower fetch (both cut) → the resent batch gets 9–12 again (never acked before), and the log has exactly one `R 16`.
  - `CheckedWritesAutoIncRegression.ImportAboveTheReservationThenFailoverNeverReusesRegression` (Review Focus 2): creates 1–3, import 5000 (P 1) → `R ≥ 5000`. `failover()` → creates get 5001+, never ≤ 5000. `demotePromote()` → the same.
  - `CheckedWritesAutoIncRegression.LostAckRetryReturnsTheSameIdsRegression` (R04e): 4 creates with the ack dropped, then the retry (same pid, epoch, seq), run three ways:
    - before the judge reaches the batch (parked; `View`);
    - after it (from the index);
    - with an `All` producer.

    Each time: the same `baseOffset` and `assignedBase`, a byte-identical frame, the LEO unchanged and `verdictReplays` +1 (for the `View` runs). 33 batches later the retry → `DuplicateSequence` with `assignedBase 0`.
  - `CheckedWritesAutoIncRegression.RetryAfterFailoverReturnsTheIdsFromTheRebuiltWindowRegression` (Review Focus 1):
    - 4 creates, ack dropped, `failover()` (fresh memory, rebuilt from the log), then the retry → the original ids, `windowMisses == 0`;
    - the same with 31 later batches of the producer in between → still found;
    - after `restart()` → found.
  - `CheckedWritesAutoIncRegression.RawAutoIncShapesAreRefusedAndTheCounterHoldsRegression` (Review Focus 4). Each raw batch below is refused with nothing appended, and `nextN` and R are unchanged after each:
    - key 0 with value id 7 → 24;
    - a plain upsert of id 3 → 39;
    - an import of id 3 on P 2 / p 0 → 39;
    - an `int64` import −1 → 39;
    - `[create, update 1]` → 39 (`MixedKeyZeroBatch`);
    - a create with `acks 0` → 39;
    - a create from a non-idempotent producer → 39.
  - `CheckedWritesAutoIncWorker.IdOfARejectedCreateIsBurned`: a `TestReject{TagEquals{9}}` member, a 3-create batch with tags 1, 9, 1 → the trailer `{(1, 51, …)}`, `idsBurned 1`, and the next create's id is `base + 3`.
  - `CheckedWritesAutoIncWorker.KeyZeroOnAConstrainedSourceNeedsView`: with the member, `acks 1` → 39 (`KeyZeroCreateNeedsView`).
  - `CheckedWritesAutoIncWorker.AbortedTransactionBurnsItsIds`: a transactional producer creates 2 then aborts → ids burned, the next plain create gets the id after them, and the key index has neither.
  - `CheckedWritesAutoIncWorker.WindowFullIsRetriableUntilItGrows`: the window reserved at 4 (rig option) and 5 producers → the fifth gets `NotLeader` (`AutoIncWindowFull`). After `maintainAutoIncWindows_()` (forced by the rig) → accepted.
  - `CheckedWritesAutoIncWorker.GatewayRoutedCreatesUseTheOpScratch`: `GatewayRouted` (03's rig route) with `kOpFlagImportGranted` → creates acked with ids. An import without the flag → 45.
  - `CheckedWritesAutoIncWorker.KeyIndexNeverRejectsAnAssignedId`: 2,000 creates → `assignedKeyExists == 0`. The mutation check (do it, then revert): skip `commitNext` in `stage()` → the second batch reuses ids and `assignedKeyExists > 0` is red.
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_checked_writes_verdict_tests && ctest --test-dir build/root -R 'CheckedWritesAutoIncWorker|CheckedWritesAutoIncRegression' --output-on-failure`. Expected: FAIL (no activation; `assignedBase` 0 on the ack).
- [ ] **Step 3: Implement** as specified.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesAutoInc|CheckedWritesKeyIndex|CheckedWritesVerdict|CheckedWritesHook|CheckedWritesViewAck|RcState|ShardWorkerAck|WorkerViewSet' -j14 --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/node/include/rune/node/RcState.h broker/libs/node/include/rune/node/Worker.h \
          broker/libs/node/src/VerdictSource.cpp broker/libs/node/tests/support/CheckedWritesRig.h \
          broker/libs/node/tests/node/CheckedWritesAutoIncWorkerTest.cpp broker/libs/node/CMakeLists.txt
  git commit -m "feat(node): autoincrement on the worker - activation from the leadership scan, window rebuild, assignedBase on fresh and replayed acks, burn and reuse counters" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 10: The cleaner: the latest reservation, protected producer batches, pinned tombstones

**Files:**
- Create: `broker/libs/storage/include/rune/storage/ProducerBatchWindow.h`, `broker/libs/storage/tests/storage/CheckedWritesAutoIncCleanerTest.cpp` (in `rune_storage_tests`, `broker/libs/storage/CMakeLists.txt:22`)
- Modify: `broker/libs/storage/src/LogCleaner.cpp`:
  - `scanTxnState` (`:216-251`) feeds `ProducerBatchWindow::noteBatch` for idempotent data batches (keys read as LE `uint64`), and `VerdictCleanState::onReservation` for type 16;
  - both rewrite engines (`rewriteSegmentImpl` `:435`, `mergeGroupWithMap` `:1386`) copy a protected batch verbatim before any record rule;
  - the tombstone-expiry drop skips `pinsTombstone(key)`;
  - `markerAction` keeps type 16 iff `keepsReservation`.
- Modify: `broker/libs/storage/include/rune/storage/LogCleaner.h:79-110` (`setProducerTailDepth`), `broker/libs/storage/include/rune/storage/VerdictCleanState.h` (`onReservation` records the highest reservation offset seen; `keepsReservation(o)` is `o == thatOffset`), `broker/libs/node/include/rune/node/LogCleanerAgent.h:401-460` (`cleaner.setProducerTailDepth(replica->producerTailDepth())` before each pass)
- Modify: `broker/libs/node/tests/node/CheckedWritesAutoIncWorkerTest.cpp` (rig cells)

**Interfaces:**
- Consumes: Task 1's `producerTailDepth`; 04's `VerdictCleanState`, `scanTxnState` signature, `markerAction` path; the existing `ProducerTails` expiry (`kDefaultExpirationMs`).
- Produces: contract addition 9 and P6-9. A depth-1 partition (every non-autoincrement topic) gets no `ProducerBatchWindow` (`nullptr`), so its cleaning is byte-identical to before.

- [ ] **Step 1: Write the failing tests.**
  - `CheckedWritesAutoIncCleanerTest.cpp` (a real `Log` with small segments, as 04's cleaner tests):
    - `CheckedWritesAutoIncCleaner.OnlyTheLatestReservationSurvives`: reservations at 0 (4096), 20 (8192) and 40 (12288), with data between them → after a full clean (with the horizon passed), only offset 40's type-16 record remains.
    - `CheckedWritesAutoIncCleaner.LastThirtyTwoBatchesOfAProducerAreKeptWhole`: producer 7 writes 40 one-record batches for ids 1–40, then `update`s each id → after the clean:
      - the batches at the last 32 insert offsets of producer 7 are present byte for byte;
      - the 8 oldest inserts are gone (superseded);
      - every update is present.
    - `CheckedWritesAutoIncCleaner.ExpiredProducerIsNotProtected`: the same with `nowMs` past the expiry → the superseded inserts are gone.
    - `CheckedWritesAutoIncCleaner.PinnedTombstoneOutlivesDeleteRetention`: a protected insert of id 5, then a tombstone of 5, with `deleteRetentionMs = 0` and the horizon past → the tombstone is kept. Once 32 later batches unprotect the insert → both are gone after the next pass.
    - `CheckedWritesAutoIncCleaner.RejectedRecordInAProtectedBatchIsKept`: a marker rejecting offset 9 inside a protected batch → offset 9 is present and the marker is not removable.
    - `CheckedWritesAutoIncCleanerRegression.DepthOneTakesTodaysPathRegression`: a depth-1 cleaner never builds a `ProducerBatchWindow` (`scanTxnState` receives `nullptr`, asserted through `LogCleaner::producerBatchWindowForTest() == nullptr`), and the logs of the cases above cleaned at depth 1 lose the superseded inserts exactly as `ProducerTails` dictates today. Every pre-existing `LogCleaner*` / `Compaction*` test stays green unchanged.
  - `CheckedWritesAutoIncWorkerTest.cpp` (rig, `compact()`):
    - `CheckedWritesAutoIncCleanerRegression.HighestIdDeletedAndCompactedIsNeverReusedNorResurrectedRegression` (Review Focus 3):
      1. creates 1–50 from producer A, then 40 batches of other work by A (so id 50's batch leaves A's last 32);
      2. `removeExisting 50`, with `deleteRetentionMs = 0`;
      3. `compact()` until neither id 50's insert nor its tombstone is in the log;
      4. `restart()`;
      5. a create → 51 or higher (from the kept reservation), a raw subscription from 0 never sees id 50 live, and a fresh projection refold has no row 50.
    - `CheckedWritesAutoIncCleanerRegression.RetryAfterFailoverAndCompactionStillReturnsItsIdsRegression` (§15): 4 creates with the ack dropped, then 10 updates of those ids, `compact()`, `failover()`, and the retry → the original ids (the protected batch kept its first key).
- [ ] **Step 2: Run to verify they fail.** `cmake --build build/root -j14 --target rune_storage_tests rune_checked_writes_verdict_tests && ctest --test-dir build/root -R 'CheckedWritesAutoIncCleaner' --output-on-failure`. Expected: FAIL (the superseded inserts are dropped).
- [ ] **Step 3: Implement** as specified. `ProducerBatchWindow` is cleaner-thread only and may allocate (the cleaner is the sanctioned heap user, `ProducerTails.h:31`). It reuses `ProducerTails`' expiry rule and producer cap. Past the cap it protects every batch of an unrecorded producer, the conservative side.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'CheckedWritesAutoIncCleaner|CheckedWritesVerdictCleaner|LogCleaner|Compaction|CleanerOffset|SkimpyOffsetMap|ProducerTails' -j14 --output-on-failure` and `ctest --test-dir build/root -R rune_compaction_correctness --output-on-failure`. Expected: PASS, with every pre-existing cleaner test unchanged.
- [ ] **Step 5: Commit.**
  ```bash
  git add broker/libs/storage/include/rune/storage/ProducerBatchWindow.h broker/libs/storage/include/rune/storage/LogCleaner.h \
          broker/libs/storage/src/LogCleaner.cpp broker/libs/storage/include/rune/storage/VerdictCleanState.h \
          broker/libs/node/include/rune/node/LogCleanerAgent.h broker/libs/storage/tests/storage/CheckedWritesAutoIncCleanerTest.cpp \
          broker/libs/storage/CMakeLists.txt \
          broker/libs/node/tests/node/CheckedWritesAutoIncWorkerTest.cpp
  git commit -m "storage: autoincrement retention in the cleaner - latest reservation kept, live producers' last 32 batches kept whole, their keys' tombstones pinned" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint B (controller).** The Checkpoint A commands, plus `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure` under `--repeat until-fail:3`.

---

## Phase 3 — Matrix, stress, the real broker, the docker e2e, benches and docs

### Task 11: The shared matrix rows for 06

**Files:**
- Modify: `broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h`:
  - `Op` gains `AutoCreate1 = 40, AutoCreate8 = 41, ImportBelow = 42, ImportAbove = 43, UpdateAssigned = 44, RemoveExistingAssigned = 45`;
  - `Entity` gains `P1U64 = 40, P4U64 = 41, P1U32 = 42, P2I64 = 43, AtReservationEdge = 44, NearMax = 45, Constrained = 46`;
  - `expect06`; `cellsFor(Part::P06)`.
- Modify: `broker/libs/node/tests/support/CheckedWritesModel.h` (`addAutoIncRule`: the key index rule of 05 plus the five predicates of Interaction matrix D), `broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp` (the P06 instantiation, the pin row)

**Interfaces:**
- Consumes: Tasks 9 and 10; 04's harness; 05's model rule.

- [ ] **Step 1: Write the matrix.**
  - `INSTANTIATE_TEST_SUITE_P(Cell06, CheckedWritesMatrix, ValuesIn(cellsFor(Part::P06)))` runs the shared `TEST_P(CheckedWritesMatrix, Holds)`. It dispatches on the op range to 06's driver, which builds the entity's rig (`RigOptions{.partition = …}` + `createAutoIncTopic(shape)` + setup), runs the op under the timing, and asserts (a)–(g) of Interaction matrix A.
  - The pin row: `EXPECT_EQ(applicable(cellsFor(Part::P06)), 798u); EXPECT_EQ(skipped(cellsFor(Part::P06)), 84u);`.
  - `CheckedWritesModel.AutoIncPredicatesCatchHandMadeViolations`: a hand-built committed log with an id twice → `NoIdTwiceNoReuse` false; an id above the latest reservation → `ReservationCoversEveryId` false; a key-0 batch with keys 1, 3 at P 1 → `IdsFollowTheStride` false.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_checked_writes_matrix_tests && ctest --test-dir build/root -R 'CheckedWritesMatrix|CheckedWritesModel\.' -j14 --output-on-failure`. Expected: the P06 cells give 798 pass and 84 skip, and the 03, 04 and 05 rows are unchanged.
- [ ] **Step 3: Mutation checks** (do each, then revert):
  - resume at `maxAssignedN + 1` instead of `R + 1` → (c) is red on `FailoverHwFold × AtReservationEdge × AutoCreate8`;
  - skip the reservation for imports → (d) is red on every `ImportAbove` cell after `failover`;
  - answer duplicates with `assignedBase = 0` → (b)/(5) is red on every `RetryAfterJudge` create cell;
  - drop `keepsReservation` → (f) is red on every cell after `compact()`.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/support/CheckedWritesMatrixExpectations.h broker/libs/node/tests/support/CheckedWritesModel.h \
          broker/libs/node/tests/node/CheckedWritesMatrixTest.cpp
  git commit -m "test(node): checked-writes matrix rows for autoincrement (06: 798 cells) and the id predicates" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 12: The autoincrement model check and the cluster stress

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesModelTest.cpp` (06 seeds), `broker/libs/node/tests/support/CheckedWritesStressDriver.h` (an autoincrement topic option, the typed and import clients' actions, `NoIdTwiceNoReuse`), `broker/libs/node/tests/integration/CheckedWritesStressTest.cpp`
- Create: `broker/libs/node/tests/client/support/AutoIncRecords.h`. It holds hand-written `AiUser {userId uint64 key, provider uint16, subject string(64), createdAt int64}`, `AiCharacter {characterId uint64 key, userId uint64, name string(16), createdAt int64}` and `Ai {id uint64 key, n int64}`, following the `ClientAuthClusterRegressionTest.cpp:64` contract, shared with Task 13.

**Interfaces:**
- Consumes: Tasks 9–11; 04's `registerInvariant` and `onOwnerThreadForTest`; `Worker::autoIncStateForTest` on each node's owner thread.

- [ ] **Step 1: Write the model check.** `CheckedWritesAutoIncModelRegression.SeededRigMatchesTheModel` and `…SameSeedSameTrace`, with the actions and invariants of Interaction matrix D. A failure prints `SCOPED_TRACE("seed=" … " step=" … " action=" …)`.
- [ ] **Step 2: Write the cluster stress.** `CheckedWritesStressRegression.AutoIncSeededClusterHoldsTheInvariants`, per Interaction matrix D. Every wait is a bounded poll (10 s), and the deadline is the failure.
- [ ] **Step 3: Run.**
  - `ctest --test-dir build/root -R 'CheckedWritesAutoIncModelRegression' -j14 --output-on-failure`. Expected: PASS.
  - `ctest --test-dir build/root -R 'CheckedWritesStressRegression' --output-on-failure`. Expected: PASS, including 04's and 05's runs.
  - Mutation check (do it, then revert): skip `assignedBases.clear()` at the scan's `begin` and record from the live path only → `NoIdTwiceNoReuse` stays green but `AckedIdsMatchTheLog` (`windowMisses`) is red after `failover()`.
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesModelTest.cpp broker/libs/node/tests/support/CheckedWritesStressDriver.h \
          broker/libs/node/tests/integration/CheckedWritesStressTest.cpp broker/libs/node/tests/client/support/AutoIncRecords.h
  git commit -m "test(node): autoincrement model check on the rig and the ThreeNodes stress (NoIdTwiceNoReuse)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 13: The real-broker slice (default shard runtime)

**Files:**
- Modify: `broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp` (in `rune_client_broker_tests`)

**Interfaces:**
- Consumes: everything above; `AutoIncRecords.h`; `OneBroker(tag, Runtime::Shard, tweak)`, `ThreeNodes`, `AuthBroker(tag, {.shard = true})` (`tests/support/AuthBroker.h:76`); 05's typed `defineTopicAndWait` with `TopicOptions{.config = {.cleanupPolicy = 1}, .keyFeatures = {.autoIncrement = true}}`; `openTopicAndWait<T>`.
- **Test setup.** Producers are idempotent `All | View` unless named. Every wait is `waitFor(…, 10s)`.

- [ ] **Step 1: Write the tests.**
  - `CheckedWritesBrokerRegression.AutoIncEndToEndShardRegression`:
    - `user` (P 1);
    - 100 × `insertAndWait({.provider = 1, .subject = …})` → `key()` is 1..100 in order, all `ok()`;
    - `removeExistingAndWait(50)` → ok, then `insertAndWait` → 101;
    - a raw subscription from 0 sees `op() == Insert` for 100 records with keys 1..100, then the delete;
    - an `All`-only producer's `insertAndWait` → `key() == 102`.
  - `CheckedWritesBrokerRegression.ModuloRoutingIsTheSameOnEveryPathShardRegression` (Review Focus 5):
    - `ai` with P 4;
    - 400 typed inserts → each `key() % 4` equals the partition a per-partition subscription received it on, and each partition's ids rise by 4;
    - then for 40 of the ids:
      - `updateAndWait` → ok;
      - `removeExistingAndWait` of half → ok;
      - a projection view `ai_by_id key id` with `getViewRowAndWait(id)` → found for the live ones;
      - a keyed subscribe (`Client.cpp:4954`) on the id's key → its partition;
    - a raw produce with `partitionKnown = 0` of an explicit `update` → routed by the gateway to `id % 4`.
  - `CheckedWritesBrokerRegression.ImportNeedsTheGrantShardRegression`:
    - setup: `AuthBroker("ai-import", {.shard = true})`; user `app` with `write on topic 'user'`, user `backfill` with `write` and `import on topic 'user'`;
    - `app`: an explicit-id `insertAndWait({.userId = 500})` → `code() == 45`, LEO unchanged;
    - `backfill`: the same → ok, `key() == 500`;
    - `app`: `insertAndWait` key-0 → `key() == 501`;
    - `backfill` again on the gateway-routed path (`rune::client::ClientConfig::produceRoute = Gateway`, the 03 rig knob) → 45 / ok exactly as above;
    - `grant import` against a level-3 broker (`testSupportedFeatureLevel = 3`) → `InvalidRequest`, the P6-14 text.
  - `CheckedWritesBrokerRegression.RegressorsPathBackFillThenInsertsNeverCollideThreeNodesRegression` (Review Focus 2, the RegressorsPath migration):
    1. `ThreeNodes::start("cw06")`, with `user` and `character` (P 1, replicas 3, compact, autoincrement).
    2. **Back-fill.** An import-granted client (dev mode) imports 3,000 users with ids from the fixture `{1, 2, 3, 5, 8, 13, …}` (first 3,000 values of `i + i/7·3`, max 3,856), and 6,000 characters with ids preserved, referencing those users. Every `insertAndWait` is ok with `key() ==` the imported id.
    3. `crash(leader)` and a re-election.
    4. **Live inserts.** 500 users and 1,000 characters through key-0 `insertAndWait`. None is 52, and every `key()` is above the back-fill's max for its topic.
    5. `autoIncStatsForTest` on the new leader: the counter is above the max.
    6. A second `crash(leader)` → another 100 inserts, all above every earlier id.
    7. No id is ever answered twice across all of the run.
  - `CheckedWritesBrokerRegression.AutoIncDdlRefusalsShardRegression`: the P6-5 refusals through the real client, each with its exact `DefineTopicResult::detail`:
    - untyped, keyless, transient;
    - `uint16` key;
    - `cleanup.policy` delete and compact,delete;
    - `retention.ms`;
    - an adoption;
    - plus `alterTopicConfigAndWait(retention.bytes = 1)`.
  - `CheckedWritesBrokerRegression.LegacyRuntimeRefusesKeyZeroCreatesRegression` (SD10): `OneBroker(…, Runtime::Legacy)` → `insertAndWait` key-0 → `code() == 39`, and `removeAndWait(1)` → ok (a plain tombstone).
  - `CheckedWritesBrokerRegression.KeyZeroCreatesToAnOlderClusterShardRegression` (SD11):
    - a broker with `testSupportedFeatureLevel = 3` → the autoincrement DDL gets 39 "autoincrement needs every broker at feature level 4";
    - a key-0 create raw-sent to a level-3 broker's existing typed topic → 39;
    - a client whose seed reports `brokerVersion 6` → `status() == BrokerTooOld`, and nothing is sent.
  - `CheckedWritesBrokerRegression.LostAckRetryAcrossFailoverReturnsTheSameIdsThreeNodesRegression`: the client's first ack is dropped (`ClientConfig::dropProduceAcksForTest = 1`), the leader crashes before the retry, and the retry on the new leader → `key()` equals the id the old leader assigned (read back through the view), with `StoredIdUnknown` never seen.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_broker_tests && ctest --test-dir build/root -R 'CheckedWritesBrokerRegression' --output-on-failure`, then `--repeat until-fail:3 -j4`. Expected: PASS every round. A failure here is a defect in Tasks 1–12: fix it there with its own `Regression` test, never by relaxing the cell.
- [ ] **Step 3: Commit.**
  ```bash
  git add broker/libs/node/tests/client/CheckedWritesBrokerRegressionTest.cpp
  git commit -m "test(e2e): autoincrement on real brokers - dense ids, modulo routing on every path, IMPORT, RegressorsPath back-fill, DDL refusals, legacy and older clusters, retry across failover" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 14: The docker e2e `AUTOINC` leg (VM only; written here, run at Checkpoint C)

**Files:**
- Modify: `broker/docker/checked-writes-e2e.sh` (the leg after 05's REFUSALS), `client-cpp/examples/checked_writes_probe.cpp` (modes `ai-setup`, `ai-backfill --ids <a>-<b>`, `ai-create --worker <i> --count <n>`, `ai-verify`), `client-cpp/tests/support/GenClaimSchemas.cpp` (an `ai_user` schema, `{userId uint64 key, provider uint16, subject string(64), createdAt int64}`), `client-cpp/CMakeLists.txt` (`ai_user.gen.h` in `RUNE_CLAIM_GEN_HEADERS`), `docs/testing.md` (the row text)

**Interfaces:**
- The probe writes one result line per op (`op key code`) to the shared directory. It logs in with `docker/lib/auth.sh`'s e2e users. The script grants the back-fill user `import on topic 'ai_user'` through `rune ql`. It takes no host, IP or credential from the source: everything comes from the environment the script sets.

- [ ] **Step 1: Write the leg.** House style: `set -euo pipefail`, bounded waits, the existing trap teardown, and every failure names its step.
  1. **AI-SETUP:** `ai_user`, P 1, replicas 3, `cleanup.policy = compact`, autoincrement. The `backfill` user gets `write` + `import`, and the `app` user `write`.
  2. **AI-BACKFILL:** `backfill` imports ids 1–5,000 in steps of 2 (2,500 rows). All ok.
  3. **AI-RACE:** 4 `app` probes run `insertAndWait` key-0 × 5,000 each. About 30 % of the way through, `docker kill` the partition leader and restart it once progress resumes. **Check:**
     - every returned key is unique and above 4,999;
     - there is no `StoredIdUnknown`, `VerdictUnknown` or `Timeout`;
     - every key > 4,999 that a subscription from 0 sees is one some probe was returned.
     - Bounded at 600 s.
  4. **AI-DELETE-TOP:** remove the highest returned id, then `docker restart` all three nodes, then one more create → its key is above the removed one.
  5. **AI-REFUSALS:**
     - `app` importing id 2 → code 45;
     - an `ai_bad` topic with `cleanup.policy = delete` → the P6-5 message;
     - a `grant import on view …` → refused.
  6. Print `AUTOINC: OK` before the script's final `CHECKED-WRITES-E2E: OK`.
- [ ] **Step 2: Build check locally (no docker).** `cmake --build build/sub-client-cpp -j14 --target checked_writes_probe`. Expected: it builds. Then `bash -n broker/docker/checked-writes-e2e.sh` and `shellcheck broker/docker/checked-writes-e2e.sh` (if installed) report nothing.
- [ ] **Step 3: Docs.** Extend the `docs/testing.md` row of `checked-writes-e2e.sh` with: "autoincrement: an `IMPORT` back-fill with ids kept, then 4 racing creators through a leader kill (unique ids above the back-fill, none unknown), the highest id deleted then a full restart (never reused), the `IMPORT` and retention refusals; `AUTOINC` race 600 s".
- [ ] **Step 4: Commit.**
  ```bash
  git add broker/docker/checked-writes-e2e.sh client-cpp/examples/checked_writes_probe.cpp \
          client-cpp/tests/support/GenClaimSchemas.cpp client-cpp/CMakeLists.txt docs/testing.md
  git commit -m "test(docker): checked-writes e2e AUTOINC leg - IMPORT back-fill, racing creators through a leader kill, delete-top restart, refusals (VM only)" \
             -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

### Task 15: The alloc gate, the benches and the docs

**Files:**
- Modify: `broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp`, `broker/benchmark/ProduceValidateBench.cpp` (beside 02/03's `BM_CheckedWriteHook/*`)
- Modify: `docs/api/ql.md`, `docs/api/wire-protocol.md`, `docs/api/client-cpp.md`, `docs/api/admin-cli.md`, `broker/docs/CATALOG.md`, `broker/docs/SECURITY.md`, `docs/operations.md`, `client-cpp/CLAUDE.md` (§2.1 Producer)

- [ ] **Step 1: Write the alloc gate.** `CheckedWritesAllocRegression.SteadyStateAutoIncCreatesAllocateNothingRegression`, on the rig with `createAutoIncTopic({.reserve = 64})`, the plain projection and a raw subscription:
  - warm up with 2,000 batches from 8 producers: key-0 creates of 1 and 16 records, 20 % imports above the counter, 20 % updates, `acks` 3. That makes reservations every few batches and fills the window for all 8 producers.
  - then, under `rune::testing::AllocWindow w;`, 5,000 more batches of the same mix, including reservations, patching and side-window records;
  - `EXPECT_EQ(w.count(), 0u)`.
- [ ] **Step 2: Write the bench.** `BM_CheckedWriteHook/autoinc_1` (1 record, ~60 B), `/autoinc_16` and `/autoinc_4k` (64 records, ≤ 4096 B). Each runs `check()` + `stage()` + `onAppended()` on a 02 spec over a `NullLog` whose append returns at once. One batch in 64 crosses a reservation (reserve 64 × k).
  - It reports `hook_p50_ns`, `hook_p99_ns` and `steady_alloc`.
  - The gate is `hook_p50_ns(autoinc_4k) < 1000` (§14, the copy included) and `steady_alloc == 0`.
  - The stderr line is `[CheckedWriteHook autoinc gate] p50=<n> ns (budget 1000) => PASS|FAIL`.
- [ ] **Step 3: Run the gate.** `cmake --build build/root -j14 --target rune_checked_writes_alloc_regression_tests && ctest --test-dir build/root -R CheckedWritesAllocRegression --output-on-failure`. Expected: PASS.
- [ ] **Step 4: Run the benches.** The baseline is this part's base commit on `main`: ABAB n=3, one session, the same box, one prepare/restore.
  - `git worktree add "$TMPDIR/rune-cw06-base" <base>`, configured Release `BUILD_BENCHMARKS=ON` like `build/rel`.
  - ABAB: `rune_broker_benchmark --benchmark_filter='BM_ProducePath_|BM_WorkerProduce_Majority|BM_WorkerProduceToPushRead_Disk|BM_CheckedWriteHook/' --benchmark_repetitions=3`, then the tip alone for `BM_CheckedWriteHook/autoinc_*`.
  - **Gates:**
    - no regression on the ABAB set for untyped and already-validated typed topics (06 adds one `spec.autoIncrement` test to the hook and one `idStride` test to the client ack, and nothing to topics without autoincrement);
    - the autoincrement gate above.
  - Borderline numbers are confirmed on the prepared VM (`broker/bench/vm-bench-prepare.sh` under `flock -w 7200 /root/vm.lock` on `$RUNE_VM_HOST`, then `vm-bench-restore.sh`).
  - Record the medians in the commit body.
- [ ] **Step 5: Write the docs.**
  - **`ql.md`** (semantics; the modifier's grammar line is Part 08's, so this adds the meaning under the topic section):
    - "`autoincrement` on a `uint64`, `int64` or `uint32` key field";
    - "implies `key.index` and modulo routing";
    - "needs `cleanup.policy = compact` and no retention";
    - "ids `n·P + p`, dense, never reused, gaps on failover, rejection and abort";
    - `autoincrement.reserve` (4096) and `autoincrement.max` (2^53 − 1);
    - `grant import on topic <pattern> to …`.
  - **`wire-protocol.md`:**
    - `ProduceResponse.assignedBase` (P6-3, v12);
    - the `kReservation` control record (type 16, `[reservedThroughN u64]`), skipped by every reader;
    - the `DefineTopic` / `TopicSchemaResponse` autoincrement fields' meaning;
    - code 54;
    - modulo routing (P6-2);
    - "a batch with key-0 creates holds only key-0 creates".
  - **`client-cpp.md`:**
    - `insertAndWait(rec).key()`, and `Completion::aux` / `assignedKeyOf`;
    - `StoredIdUnknown`;
    - the sticky routing and homogeneous batches;
    - the local refusals (P6-12);
    - "key-0 creates need an idempotent producer with `All`; use `All | View` where a constraint can reject the create";
    - the orphaned-row note ("an `insertAndWait` that times out may have created the row: look it up through a unique view");
    - `TopicSchemaResult::keyFeatures`.
  - **`admin-cli.md`:** `rune ql "grant import on topic 'user' to user backfill"` and the back-fill recipe (import with ids kept, then live inserts).
  - **`CATALOG.md`:** the autoincrement `ApplyV2` rules (P6-6), the normalization (P6-1), the v2 autoincrement golden, and that the features never change.
  - **`SECURITY.md`:** the `IMPORT` operation (a TOPIC op, implied by nothing, level-4 gated), and `NotAuthorized` on an explicit id without it.
  - **`operations.md`**, section "Autoincrement":
    - reservations and failover gaps;
    - the 32-batch side window and its rebuild;
    - the cleaner retention and pinned tombstones (P6-9);
    - the window growth;
    - the stats (C16 ids 8–10) and the alarms `windowMisses`, `assignedKeyExists` and `AUTOINC-ID-REUSE`.
  - **`client-cpp/CLAUDE.md` §2.1:** "key-0 creates ride `kFlagKeyZero`, never share a batch with other records, route sticky; their completions carry the id in `aux`".
- [ ] **Step 6: Commit.**
  ```bash
  git add broker/libs/node/tests/node/CheckedWritesAllocRegressionTest.cpp broker/benchmark/ProduceValidateBench.cpp \
          docs/api/ql.md docs/api/wire-protocol.md docs/api/client-cpp.md docs/api/admin-cli.md broker/docs/CATALOG.md \
          broker/docs/SECURITY.md docs/operations.md client-cpp/CLAUDE.md
  git commit -m "bench/docs: autoincrement hook gate (< 1 us incl. copy), zero-alloc creates, autoincrement / IMPORT / routing docs" \
             -m "<baseline vs tip medians>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
  ```

- [ ] **Checkpoint C (controller, before the PR).**
  - `broker/scripts/run-regression.sh build/root 10`: stages (a), (b) and (c). Stage (c) runs on the VM under its lock.
  - The client `Regression` suite on `build/sub-client-cpp`, CatalogGolden and `rune_security_tests` on `build/sub-codec`, and `rune_ql_tests` on `build/sub-ql`.
  - ASan+UBSan (`build/asan`) and TSan (`build/tsan`, `build/client-tsan`) with `-R Regression -LE latency`, including `rune_checked_writes_matrix_tests` and `rune_checked_writes_stress_tests`.
  - `ctest --test-dir build/root -R CheckedWrites -j14 --output-on-failure` under `--repeat until-fail:3`.
  - The latency gates on an idle host: `ctest --test-dir build/root -R Regression -L latency`.
  - The Task 15 benches, recorded.
  - **The docker e2e on the VM:** `ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash broker/docker/checked-writes-e2e.sh'`, run from the branch's checkout in your own VM directory. Expected: `AUTOINC: OK` and `CHECKED-WRITES-E2E: OK`. Keep the logs with `E2E_LOG_DIR` on a failure.

## Finish

- [ ] Checkpoint C green.
- [ ] Whole-branch review by the most capable model. The reviewer leaves PR comments, and the implementer resolves each one.
- [ ] File the deferred items as labelled GitHub issues on `arcanumruna/rune`, in one batch with one summary at the end:
  1. "A future delete-records must refuse autoincrement topics", `enhancement`, `sev:low`, `area:broker`.
  2. "Patch autoincrement ids in place on the gateway-routed path (the op-ring slot is Rune's)", `enhancement`, `sev:low`, `area:broker` (spec §17 follow-up, if the bench shows the copy matters).
  3. "Key-0 creates through Tier-2 batches and claims", `enhancement`, `sev:low`, `area:client`.
  4. Every recorded deviation that is not urgent.
- [ ] Hand over to Part 07 and Part 08 per the overview's acceptance list for 06:
  - key-0 creates get ids;
  - `assignedBase` and `WriteResult::key()` work;
  - `IMPORT` is enforced;
  - `rune::protocol::partitionFor` exists for 07's `ViewKeyBuilder::partition()`;
  - Part 08 parses `autoincrement`, `autoincrement.reserve` / `.max` and the default of 1 partition into `TopicKeyFeatures`.
- [ ] Do not push or merge without the owner's go-ahead (overview § Sequencing).

---

## Self-review (done while writing)

- **Spec coverage (§6).**
  - **§6.1 design** (stride + durable block on the leader): P6-7, Task 7.
  - **§6.2:**
    - key field `uint64` / `int64` / `uint32`, mandatory key: P6-5 rules 2 and 4 (Task 4);
    - implies `key.index` and modulo: P6-1 (Task 4);
    - immutable, including removal of `autoincrement` by alter: 05's carry, `AlterSchemaKeepsAutoIncrementRegression`.
  - **§6.3:**
    - modulo routing on the client, the gateway and the view-read path: Tasks 1, 5 and 13;
    - `n·P + p` from n = 1: Task 7;
    - the 2^53 − 1 cap and a lower max: P6-1, P6-5 rule 8, `ExhaustionIsExactAtTheCap`;
    - homogeneous key-0 batches: 03 rule 8, Task 5;
    - `base + i·P`: Tasks 6 and 8.
  - **§6.4:**
    - key 0 at the field width in key and value: 02's placeholder, `KeyZeroWithANonZeroValueKeyIsSchemaValidationFailed`;
    - sticky routing: P6-11;
    - the scratch copy, patch and CRC: Task 8;
    - the 1 µs budget: Task 15;
    - the skip of the key index: declined (P6-13, flagged);
    - burned rejected ids: Task 9 and the matrix.
  - **§6.5:**
    - the reservation before the data, same thread: Task 8;
    - replication with no new path: C10;
    - failover resume at R + 1 with `NotLeader` until found: P6-7, Task 9;
    - truncation: Task 9;
    - the cleaner keeps the latest reservation: Task 10;
    - compact-only and no retention: Task 4;
    - readers skip type 16: 04's sweep.
  - **§6.6:**
    - the write table: P6-4, Task 8;
    - `IMPORT` with `KeyExists`, the modulo partition, the counter move and the reservation first: Tasks 3, 8 and 9;
    - plain upsert refused: 8a;
    - 54: 8e;
    - retries with the original base: P6-8, Tasks 8–10;
    - the window rebuild on failover: Task 9;
    - the cleaner keeps the last 32 batches: Task 10;
    - outside the window → `StoredIdUnknown`, asserted unreachable in the stress: P6-10, Task 12;
    - transactions burn ids: Task 9;
    - ids only after commit: `All` required (03 rule 8).
  - **§6.7:** `assignedBase` (C5, Task 9), ids per record (Task 6), `insertAndWait(rec).key()` and the completion (Task 6), dependent writes (docs).
  - **§6.8:** nothing to build (P is immutable). The docs note the rule.
  - **Other sections:**
    - §11.1 54: Global Constraints, Task 8;
    - §11.2 key-0 rows: 03 + Tasks 5 and 8;
    - §12 level-gated DDL and the grant: Tasks 3 and 4;
    - §13 stats: contract addition 12;
    - §14 hook budget: Task 15;
    - §15 "Autoincrement" block and the two extra cells: Interaction matrix A–C;
    - §16.6 deliverables and §16.12 docs (`CATALOG.md`, `SECURITY.md`, `ql.md`, `wire-protocol.md`, `client-cpp.md`, `admin-cli.md`): Task 15.
- **Brief points.**
  - Small, dense, never reused after a delete: Tasks 9, 10 and 13.
  - Key-0 → next id; IMPORT → counter above max(id): Tasks 8, 9 and 13.
  - Transient refused through `featureRefusal`, and keyless "autoincrement needs a key": `CheckedWritesAutoIncDdl.Refusals`. Untyped follows 02, as listed under gaps.
  - Partitions default to 1: the P = 1 default path and `partitionCount == 1` in Task 4; the QL default is handed to 08.
  - Retry in the window returns the same key: Task 9, `LostAckRetry…` and `RetryAfterFailover…`.
  - Legacy refusal: Task 13.
  - The `brokerVersion` gate: Task 5.
  - RegressorsPath back-fill then inserts: Task 13 and the e2e leg.
- **Step scan.**
  - Every test step names its suite and test and its assertions with the spec's values: 4096, 2^53 − 1, 32, `n·P + p`, 54, 45, 26-byte spec wire, 798/84 cells and 3,145,728 sweep cells.
  - Code steps name files, signatures and placement.
  - Bodies appear only where the signature leaves the algorithm open: the plan/reserve rule (Task 7) and the stage order (Task 8).
- **Type consistency.**
  - These are defined once (contract additions) and used with the same spelling in Tasks 5–15: `AutoIncState`, `AutoIncPlan`, `AssignedBaseWindow`, `AutoIncStats`, `autoIncId`, `effectiveAutoIncMax`, `partitionFor`, `kFlagKeyZero`, `idStride`, `isKeyZeroCreate`, `kKeyZeroStickyRun`, `StoredIdUnknown`, `kPermFlagImport`, `kOpFlagImportGranted`, `allowsImport`, `ProducerBatchWindow`, `producerTailDepth`, `createAutoIncTopic(AutoIncShape)`, `autoIncStatsForTest` and `autoIncStateForTest`.
  - C7's member names (`autoInc`, `assignedBases`, `autoIncStats`, `autoIncrement`, `autoIncrementMax`, `autoIncrementReserve`, `partitionCount`, `partitioner`) are verbatim.
  - `TopicHandle::partitioner` follows `kind` (R05f).
- **Review Focus.** Each of the five has its named test in its owning task: 9, 9/13, 10, 9 and 13.
- **Proportion.** The plan fixes decisions, placements, signatures and assertions. The hook body, the worker plumbing, the cleaner rewrite and the client loops are left to the implementer within the stated rules.
