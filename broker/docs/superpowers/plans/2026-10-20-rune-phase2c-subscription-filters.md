# Rune Phase 2c — Broker-side Subscription Filters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A subscriber names the records it wants by typed field values with a generated filter-by-example struct (`E::Filter{ .worldId = worldX, .channel = anyOf(1, 2, 3) }`). The broker's shard worker drops every non-matching record before the network, at a per-record cost that does not grow with the number of filtered subscribers. Unfiltered subscriptions stay byte-identical on the wire and on the push path.

**Architecture:** The filter is a flat, canonical, fixed-width little-endian encoding (spec §5). It is produced by the generated `E::Filter` (members are `rune::Match<T>`) or by the runtime `rune::codec::FilterBuilder`, and it rides a new optional trailing field of `Subscribe` and `JoinGroup` (client version 10). The gateway validates the filter against the topic's per-version `rune::codec::FilterSchema` and admits it against the per-partition caps. It then forwards the bytes to the owning shard worker in the `Subscribe` op's tail, on the same per-worker FIFO that carries the topic's `FilterSchema` op. On the worker, every filtered subscription is a member of a `FilterGroup` (one per owned partition × isolation level). A member is in one of three states. In `InSync`, it is served by the group's single shared scan: one index lookup per record maps the record's index-field value to the candidate members. In `Behind`, it reads its own cursor and evaluates its own predicate. In `VersionStall`, it waits a bounded time for a schema version this worker does not know yet. Batches that match completely are pushed zero-copy. Partly matching batches are rebuilt with their original offsets, base offset and last-offset delta. Quiet filters get the existing v6 position note. Unfiltered subscriptions never enter the filter code.

**Tech Stack:** C++23, GoogleTest (`TEST_P` for the matrix), Google Benchmark with `broker/benchmark/BenchGate.h`, CMake/Ninja (`build/root`, `build/sub-client-cpp`, `build/sub-codec`, `build/rel`, `build/asan`, `build/tsan`), SBE 1.40.2 for the internal ring schema, real Aeron (embedded driver) for worker and client tests, real in-process `BrokerNode`s (`OneBroker`, `ThreeNodes`), and docker compose (3 nodes, authenticated) on the k3s VM.

**Spec:** `broker/docs/superpowers/specs/2026-10-13-rune-subscription-filters-design.md` (approved 2026-10-13). It binds, except where *Spec drift* below records that the code has moved since; there the plan follows the code.

**Builds on:** Phase 2b C (typed topic routing), E/E2 (transient topics, position note v6, lineage note v7, moved-subscription resume), D (console), B (zero-copy claims, all-fixed flyweights), F (unified producer), A (retractable aggregates), catalog history (client v8), authentication (client v9), all merged on `main`.

## Execution context

- **Repo:** the monorepo root (the `rune` checkout). Every path in this plan is relative to it, and every command runs from it.
- **Branch:** cut `feat/phase2c-subscription-filters` from `main` (`71081b68` when this plan was written; use the tip at execution time):
  ```bash
  git switch -c feat/phase2c-subscription-filters main
  ```
  Verify the preconditions before Task 1. Each command must hit:
  - `grep -n "kClientVersionAuth *= 9" protocol/include/rune/protocol/client/Protocol.h` (1 hit)
  - `grep -n "kA2cPeerFrameDrops *= 23" broker/libs/node/include/rune/node/AdminChannels.h` (1 hit)
  - `grep -n "PEER_LINK_KEYS\">22" protocol/schema/rune-internal-ring.xml` (1 hit)
  - `grep -n "int pushOwnedSubscriptions_" broker/libs/node/include/rune/node/Worker.h` (1 hit)
- **Builds (all already configured; if one is missing, stop and tell the controller):**
  - `build/root`: the broker plus protocol, with the client, codec, ql and crypto compiled in (Ninja, Debug, `BUILD_TESTING=ON`). Build with `cmake --build build/root -j14 --target <t>` and test with `ctest --test-dir build/root -R <filter> -j14 --output-on-failure`.
  - `build/sub-client-cpp`: the client's own suites and examples (Ninja, Debug, `BUILD_TESTING=ON`, `BUILD_EXAMPLES=ON`).
  - `build/sub-codec`: the codec's own suites (`rune_codec_tests`, `rune_catalog_tests`; CatalogGolden runs only here).
  - `build/rel`: broker benchmarks (Release, `BUILD_BENCHMARKS=ON`). The binary is `build/rel/broker/rune_broker_benchmark`. `build/bench-rel` is the client's release bench build: do not use it.
  - `build/asan` (ASan+UBSan) and `build/tsan` (TSan): the root tree under sanitizers.
  - `build/client-tsan`: the client's own suites under TSan (`cmake -S client-cpp`, Debug, `BUILD_TESTING=ON`, `-fsanitize=thread`; already configured).
- **Codec sources are listed three times.** A new `codec/src/*.cpp` must go into `codec/CMakeLists.txt` (`rune_codec`), `broker/CMakeLists.txt` (its `rune_codec` copy, near line 143) and `client-cpp/CMakeLists.txt` (its `if(NOT TARGET rune_codec)` copy, near line 197).
- **SBE:** the internal-ring flyweights under `protocol/include/rune_protocol_internal_sbe/` are generated and committed. `protocol/schema/README.md` names `tools/sbe-all-1.40.2.jar`, but the jar was not carried into the monorepo. Use any copy of `sbe-all-1.40.2.jar` (for example the pre-monorepo `rune-protocol/tools/` checkout) through `$SBE_JAR`; never commit the jar.
- **Test gate (CLAUDE.md #7):** each task builds and runs the executables that cover the code it touches, plus every new or changed test (its *Run* steps name them). The FULL `*Regression*` suites run at the checkpoints: **A** after Task 5, **B** after Task 10, **C** after Task 15, **D** after Task 17 (before the final whole-branch review). Each checkpoint also runs CatalogGolden and the sanitizers. A checkpoint failure is bisected over the commits since the last green checkpoint. Every fix gets a test whose suite or name contains `Regression`.
- **Docker e2e and VM benches** run only on the k3s VM (`ssh root@$RUNE_VM_HOST`, under `flock /root/vm.lock`), never on the Mac. Benches run on a prepared box (`broker/bench/vm-bench-prepare.sh`, then `vm-bench-restore.sh` afterwards).
- **Commits:** on this branch only (`git add <paths>`, never `.superpowers/`). Nothing is pushed. Every commit message ends with the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Spec drift (verified against `main` 2026-10-20; the plan follows the code)

- **SD1 — Monorepo.** The spec's repos are now directories: rune-codec is `codec/`, rune-protocol is `protocol/`, rune-broker is `broker/`, rune-client-cpp is `client-cpp/` and rune-ql is `ql/`. There is one branch.
- **SD2 — Client version.** The spec says "v8, the next version (v7 is taken by E2, and authentication may take the one after)". Today v8 is catalog history (`kClientVersionCatalog`) and v9 is authentication (`kClientVersionAuth`). Filters are **`kClientVersionFilter = 10`**. The broker side gets **`kBrokerVersionFilter = 6`** (`kBrokerVersionAuth` is 5).
- **SD3 — No refusal at version negotiation.** The spec says a filter-capable client connected to an older broker "is refused at session setup by the existing version negotiation". No such refusal exists. A broker accepts any higher `ConnectRequest.clientVersion`, and `Subscribe::decode` ignores trailing bytes, so an old broker would silently drop a filter. The client therefore gates on the destination session's `ConnectResponse.brokerVersion`, the same pattern `GroupConsumerHub` uses for `kBrokerVersionExplicitGroupSubscriptions`. Below 6, a filtered subscription is failed with `SubscriptionFailReason::BrokerTooOld` (a group with `GroupFailReason::BrokerTooOld`) and nothing is sent.
- **SD4 — "A v8 client sends a zero length for no filter"** conflicts with "code without a filter behaves exactly as today, byte for byte on the wire". The byte-identity rule wins: an unfiltered `Subscribe` / `JoinGroup` omits the trailing field. The decoder still reads a zero-length field as "no filter".
- **SD5 — Error details.** `SubscribeResponse` (fixed 38 bytes) and `JoinGroupResponse` carry no text, and `encodeRejection` answers in the request's own response type. The §9 details ride a new **v10-only trailing `detail` var field** (`u16` length + UTF-8) on both responses. Older sessions never get it, and their decoders ignore trailing bytes.
- **SD6 — Schema distribution.** The spec relies on "the existing schema-distribution ring". `SchemaFanout` / `WorkerOp::Type::SchemaUpdate` carry validators (one materialized descriptor per version) only for `validate = true` topics (`BrokerNode::postCatalogValidator_` returns early otherwise). A filter needs every version of every typed topic, `validate = false` ones included. New path: the catalog `schemaChanged` effect also posts **`kA2cFilterSchema = 24`** (`[topicId:i32][schemaVersion:u16][evolving blob]`). The control thread installs it in the gateway's `FilterSchemaRegistry`, and the gateway sends a new **`WorkerOp::Type::FilterSchema` (SBE `OpType` 23)** to every worker through its per-worker FIFO (`pendingSubOps_`). That FIFO is the same one that carries `Subscribe` ops, so a worker always holds the topic's schema before it sees a filtered `Subscribe` for it.
- **SD7 — Push points.** Shard mode, the default `NodeConfig::workerMode`, pushes owned subscriptions from `Worker::pushOwnedSubscriptions_` (`broker/libs/node/include/rune/node/Worker.h`). Legacy mode pushes from the gateway (`ClientGateway::pushSubscriptionsPass_`, both inline and through `PushRead` returns). Ruling FD1: filtered subscriptions are served in shard mode, and **refused in legacy mode** with `InvalidRequest` detail "filter needs RUNE_WORKER_MODE=shard".
- **SD8 — One cursor per subscription.** The spec's per-record index lookup assumes one read per partition. The worker reads every subscription from its own cursor (`WorkerSubscription::nextOffset`). This plan adds the `FilterGroup` shared scan for in-sync members, with `Behind` members falling back to per-subscription evaluation (FD4). Without the shared scan, the per-record cost would grow linearly with the number of subscribers.
- **SD9 — Unknown writer version can happen.** It is not impossible as §8 assumes: a `validate = false` typed topic accepts any bytes, and a record of version N+1 can reach a partition leader before that broker's catalog apply. Ruling FD6: a bounded stall (`RUNE_FILTER_VERSION_WAIT_MS`, default 5000) lets the schema arrive. After the wait, the record is excluded, counted and logged, never delivered by mistake and never silently.
- **SD10 — Batch offsets may have holes (the spec's "first task verifies").** Verified: the client walks each record's own `offsetDelta` (`RecordBatchReader::forEachRecord`). The comments in `client-cpp/include/rune/client/codec/DataCodec.h` and `GroupConsumerHub::onRecords` say "compaction can leave holes INSIDE a batch" and compute the batch end as `baseOffset + lastOffsetDelta + 1`. Contiguous offsets are therefore not required. A rebuilt batch is **one batch per source batch**. It keeps the source `baseOffset`, `baseTimestamp`, producer fields and `lastOffsetDelta`, and copies the matching records verbatim. Task 1 pins this.
- **SD11 — Position progress on the client.** `SubscriberHub::onRecords` advances `nextOffset` per *delivered record*, and keeps an empty-batch position note only as a moved-subscription `resumeHint`. `Client.cpp` never routes an empty batch to `GroupConsumerHub`. For **filtered** subscriptions only (unfiltered behaviour is untouched), the client advances its position to a batch's end (`baseOffset + lastOffsetDelta + 1`) and to a position note's offset (Tasks 11, 12).
- **SD12 — Accessor tables.** "Preallocated slab extended through the ring" becomes: per-tag, per-version `FieldSlot` vectors, grown on the cold path when a `FilterSchema` is installed (DDL rate), never on the record path. The index and member tables of a group are reserved at the configured caps when the group is created.
- **SD13 — Rune QL `WHERE`.** `ql/` has no `SELECT` / consume statement, and the console has no tail command (`client-cpp/src/cli/`), so `WHERE` has nothing to attach to. The runtime `FilterBuilder` ships and is tested. Lowering `WHERE` to it is deferred to the native Rune QL query API (a GitHub issue at Finish).
- **SD14 — MMORPG absolute gate.** `broker/bench/BENCHMARKING.md` records that the co-located 3-node docker path on the 4-vCPU box has **never** met p50 50 µs / p99 200 µs, even unfiltered (baseline p50 350–690 µs, p99 10–30 ms), and that 3-node saturation is 112–200k rec/s, below the spec's 1M events/s. Ruling FD12: the 50/200 µs class is gated in-process on the filtered push path (`BM_FilterWorkerPush`). The docker scenario is gated relative to the same load unfiltered, plus the absolute bytes-per-server gate (≤ 1.2× ideal). No gate is relaxed.
- **SD15 — `Record` has no partition.** An `AllPartitions` handler needs it, so `Record::partition` is appended.
- **SD16 — Sanitizers exist now.** `build/asan` and `build/tsan` exist (the Part A plan predates them), so they run at every checkpoint. The client's own suites run under TSan from the existing `build/client-tsan`.

## Global Constraints

- **HARD RULES (CLAUDE.md, all eight), as they bind here:**
  1. **Shared-nothing.** The gateway owns `FilterSchemaRegistry`, `FilterAdmission` and every gateway `Subscription` record on the control thread. Each worker owns its `FilterSchemas`, `FilterGroup`s, members, `FilterScratch` and counters, on its own thread. The client's filter bytes are written by the app thread before the `SubscribeAdd` command and are immutable afterwards (the existing `SubscriptionImpl` rule). There are no locks, no atomics as coordination, no `thread_local` and no `shared_ptr` (its refcount is atomic). `check_no_atomics.py` (ctest `NoAtomicsOrMutexesOutsideAllowlist`) stays green.
  2. **Rings only.** Filter schemas travel admin → control on the admin→control ring (`kA2cFilterSchema`), then control → worker on the worker ingress ring (`WorkerOp::Type::FilterSchema`). Filter bytes travel control → worker in the `Subscribe` op's tail (`TailKind::FILTER`). Client → broker uses the wire. There are no cross-thread callbacks.
  3. **Per-state data.** Member: `std::variant<Behind, InSync, VersionStall>`. Group scan: `std::variant<ScanIdle, Scanning, ScanStall>`. Worker subscription binding: `std::variant<SubUnfiltered, SubFiltered>`. Gateway pending worker op: `std::variant<SubOpSubscribe, SubOpUnsubscribe, SubOpFilterSchema>`. That last one replaces `PendingSubOp`'s `unsubscribe` / `mdc` booleans (fix-on-sight, Task 9). A predicate verdict is an enum, never a set of booleans.
  4. **Zero allocation** on the record path and in steady state: the predicate, the index lookup, the shared scan, the rebuild, the position notes and the client encode (`FilterWriter` writes into a fixed `FilterBytes`). Allocation is allowed only on cold paths: a `FilterSchema` install (DDL rate), group creation (first filtered subscription of a partition), member value storage at `Subscribe`, and the grow-once rebuild scratch (bounded by the egress max message, never shrinks). An allocation gate binary proves it (Task 16).
  5. **Test and benchmark everything:** unit tests, a parameterized worker-level interaction matrix (Task 14), seeded model-checked stress (Task 15), a real-broker slice (Task 13), benches (Task 16) and docker e2e (Task 17).
  6. **A `*Regression*` test on every fix**, named in its task.
  7. **Affected-area tests per task; the FULL `*Regression*` suite plus ASan+UBSan and TSan at Checkpoints A–D.**
  8. **Fix-on-sight, owned by the named task:** `PendingSubOp` booleans become a variant (Task 9, `GatewayFilterRegression.QueuedSubscribeUnsubscribeAndSchemaOpsKeepTheirOrderRegression`). `WorkerSubscription`'s existing `isGroup` / `groupCarrierReceived` / `active` / `cursorFresh` booleans are *not* restructured here: that change is wider than this phase, so it is filed as a GitHub issue at Finish (good-enough scope rule). The new filter state on it is a variant.
- **Spec rules (verbatim values):**
  - Encoding: `filter := [formatVer u8 = 1][termCount u8 (1..8)] term{termCount}`; `term := [tag u16][primType u8][op u8: 1=Eq, 2=In][count u8 (Eq: 1; In: 1..64)][value × count]`; each value is the field's fixed width (FixedStr: its declared length).
  - Limits: at most 8 terms, 64 `In` values, FixedStr ≤ 16 bytes. The largest filter is 2 + 8 × (5 + 64 × 16) = **8,234 bytes** (`kMaxFilterBytes`).
  - Canonical form: terms are sorted by tag, and `In` values are sorted and de-duplicated. Equal filters encode identically.
  - Filterable: fixed-width integers (`Int32`, `Int64`, `UInt16`, `UInt32`, `UInt64`), `Enum`, `Bool` and `FixedStr` ≤ 16. Never `String`, `Bytes` or `Float64`.
  - An empty filter (`E::Filter{}`) is no filter.
  - Index term: the first `Eq` in tag order, else the first `In`. An `In` index term registers the member once per value.
  - A field absent in the record's version means **no match**, counted per subscription, with the first exclusion logged per subscription and version.
  - Position note: a subscription that has skipped records gets one when **50 ms** have passed or **65,536** records have been skipped, whichever comes first (both configurable).
  - Caps: **4,096 filtered subscriptions per partition** and **4,096 distinct index values per partition**, both configurable. Exceeding one answers the `Subscribe` explicitly ("filter capacity") and never allocates.
  - `read_committed`: the filter applies after isolation, and the LSO bound is unchanged.
  - Moved subscriptions re-subscribe with the **same filter bytes** from `position()`.
  - Consumer groups: **one filter per group**, byte-identical, fixed by the first member and changeable only once the group is empty. A different filter is rejected at JoinGroup with `InvalidRequest` (39), detail "group filter mismatch: the group uses filter <hex>".
  - Errors (§9): untyped topic → 39 "filter needs a typed topic"; malformed / unknown tag / type mismatch / var-len / float / limit → 39 with a detail naming the term index; cap reached → 39 "filter capacity". The client never falls back to unfiltered.
- **Append-only values.** Wire and enum values are append-only, and none is reused. The new ones:
  - client versions: `kClientVersionFilter = 10`, `kClientVersion = 10`;
  - broker versions: `kBrokerVersionFilter = 6`, `kBrokerVersion = 6`;
  - `FilterOp { Eq = 1, In = 2 }` and `kFilterFormatVer = 1`;
  - SBE: `OpType::FILTER_SCHEMA = 23`, `TailKind::FILTER = 10`;
  - `WorkerOp::Type::FilterSchema = 23`;
  - admin→control `kA2cFilterSchema = 24`;
  - `SubscriptionFailReason::BrokerTooOld = 9`;
  - `GroupFailReason::FilterMismatch = 6`;
  - `Record::partition`, `SubscriptionStatus::batchBytes` (appended members).
- **Catalog compatibility.** Nothing touches `rune_catalog`: no event layout, fingerprint or golden-log change. `CatalogGoldenRegression.*` must pass unchanged at every checkpoint (`build/sub-codec`, 3 tests). Topic schemas, fingerprints and `GetTopicSchema` are unchanged. The generated `E::Filter` is a nested type and changes no `kFingerprint`.
- **Latency gates (never relaxed):**
  - Unfiltered worker push: `BM_RcWorkerPush/0` and `/1` (`broker/benchmark/RcBench.cpp`) within **+2%** of the pre-2c baseline (ABAB, n = 3, same box and session).
  - `GatewayDataFixture/Produce` and `GatewayDataFixture/SubscribeDelivery` (`broker/benchmark/GatewayDataBench.cpp`) keep their hard **p99 < 100 µs** gates.
  - New, in-process filtered push: `BM_FilterWorkerPush`, produce→push **p50 ≤ 50 µs, p99 ≤ 200 µs**.
  - New: `BM_FilterIndexLookup` **≤ 5 ns/record** for a hit and for a miss, and flat within **10%** across 1, 50 and 500 subscribers. `BM_FilterWorkerPush` per-record broker cost is flat within **10%** across 1, 50 and 500 filtered subscribers.
  - Zero steady-state allocation (`rune_filter_alloc_regression_tests`).
  - Docker scenario: bytes per world server **≤ 1.2×** ideal, and filtered p50 / p99 no worse than unfiltered at the same load (FD12).
- **No secrets, emails or IPs** in the repo: the VM is `$RUNE_VM_HOST`, passwords come from `broker/docker/lib/auth.sh`, and no host paths go into committed scripts.

## Decisions (binding for executors)

- **FD1 — Legacy worker mode refuses filters.** A filtered `Subscribe` to a gateway without per-worker egress (`!perWorkerEgressActive_ || !workerPool_`) is answered `SubscribeResponse{InvalidRequest, detail "filter needs RUNE_WORKER_MODE=shard"}`. Shard mode is the default and the only mode allowed with authentication in a multi-broker cluster (`NodeConfig.cpp`), so the feature mostly works without legacy support. Legacy-mode filters become a GitHub issue at Finish.
- **FD2 — Where things live.**
  - `codec/`: the wire format and the predicate, shared by client and broker, and reusable by a later view-source `Filter` operator (spec §12): `FilterWire.h`, `Filter.h` (`rune::Match`), `FilterSchema.{h,cpp}`, `FilterPredicate.h`, `FilterBuilder.{h,cpp}`.
  - `protocol/`: the batch rebuild (`BatchRebuild.h`), which the client's tests also use.
  - `broker/libs/node/.../filter/`: the index and the group engine.
  - `broker/libs/gateway`: validation and admission.
- **FD3 — Gateway is the single admission point.** The gateway validates (typed topic, parse, schema check) and admits (caps), then answers `SubscribeResponse`. It holds a move-only `FilterAdmission::Ticket` inside its `Subscription` record, which releases the counts on every erase path. The worker reserves its tables at the same caps and can therefore never exceed them. If it ever would (a bug), it counts `filterOverCap` and serves that member `Behind`-only, never allocating on the record path.
- **FD4 — Member state machine.** A new member starts `Behind{}` at the gateway-resolved start (`sub.nextOffset`).
  - **Becoming `InSync`:** a `Behind` member becomes `InSync` when its cursor equals the group's `Scanning` cursor. If the group is `ScanIdle`, it becomes `InSync` when it has read up to the group's bound; the group then goes `Scanning{cursor}`.
  - **Falling `Behind`:** an `InSync` member whose delivery returns `Retry` (egress back-pressure) goes `Behind{}` with its cursor at that batch's base. On `TooLarge`, it gets the existing F19 `ErrorOwed` / `Failed` push state and is parked.
  - **Version stall:** a `Behind` member whose next record has an unknown version goes `VersionStall{version, sinceNs}`. A group scan in the same position goes `ScanStall{cursor, version, sinceNs}`.
  - **Join check:** the join test runs in the member's own step, so a group scan never iterates non-matching members.
- **FD5 — Shared scan per batch.**
  1. Walk the records once (`forEachRecordSpan`).
  2. For each record, read its version and, for each distinct index field of the group, look up its slot, load the key and probe the index. For each `InSync` candidate, evaluate the residual terms and append `{member, recordIndex}` to the match list.
  3. Counting-sort the match list by member.
  4. A member whose match count equals the batch's record count gets the original span, zero-copy. Any other matched member gets a rebuild into the grow-once scratch, sent before the next member's rebuild.
  5. Untouched members cost nothing per batch. Their "skipped" count is computed as `group.scannedRecords - member.coveredAt`.
  6. If the match list would overflow `RUNE_FILTER_MATCH_SCRATCH` (default 65,536), that batch is served by per-member evaluation for every `InSync` member (correct, bounded, and rare).
- **FD6 — Unknown version:** see SD9. `VersionStall` / `ScanStall` last at most `versionWaitNs`. After that the record is excluded and counted (`unknownVersionExcluded`, plus a first log per member and version), and the cursor moves on.
- **FD7 — Null values (tombstones) never match.** A record whose value is null (length −1) has no field values, so under three-valued logic it is excluded and counted as `nullValueExcluded`. A value shorter than 4 bytes, or shorter than a slot's `offset + width`, is `Malformed`: excluded and counted, never read past its end.
- **FD8 — Value compatibility at compile time.** `Match<T>` accepts `U` when `std::same_as<T,U>`, or when both are integral and neither is `bool`. The range is checked at encode time, and an out-of-range value fails explicitly (never truncated). Floats, strings into integer fields, and `bool` against integers do not compile. A FixedStr member accepts `std::array<char,N>` and string literals of length ≤ N (`static_assert`), zero-padded exactly as `fly::storeFixedStr` stores them. A field named `Filter` makes the generator throw (`std::invalid_argument`): it would collide with the nested type, the same rule as a field named `key`.
- **FD9 — Detail transport:** see SD5. The client fails the subscription `RejectedByBroker` with `brokerErrorCode() == 39` and calls `onError("subscribe topic <id> partition <p> refused: InvalidRequest: <detail>")`. A group whose JoinGroupResponse is 39 with a detail starting with `rpc::kGroupFilterMismatchPrefix` ends with `GroupFailReason::FilterMismatch`.
- **FD10 — `AllPartitions`.** `Client::subscribe(TopicHandle, AllPartitions, SubscribeOptions, RecordHandler)` returns a `SubscriptionSet`: one `Subscription` per partition `0..topic.partitionCount-1` (cold allocation at subscribe), with `size()`, `operator[](int32_t)`, `position(int32_t)`, `unsubscribe()`, `active()` (any) and `finished()` (all). Each record carries `Record::partition`.
- **FD11 — Group fetch filters are client-enforced.** A member's per-partition fetch `Subscribe` (bit-61 correlation id) carries `GroupConfig`'s filter bytes. The broker validates it like any filtered `Subscribe`, but cannot tie it to a group, so the one-filter rule is enforced at JoinGroup.
- **FD12 — The scenario gate:** see SD14.
- **FD13 — Bytes per world server** are measured by a new duty-side counter, `SubscriptionStatus::batchBytes` (the sum of delivered `recordBatch` sizes), read with `Subscription::batchBytes()` / `SubscriptionSet::batchBytes()`.

## Review Focus

These are the inputs most likely to hurt a user. Each is pinned by a named test in the task that owns it:

1. **A record whose schema version the push point does not know yet** (a `validate = false` topic, or a catalog apply that lags the produce) must be delivered once its schema arrives, never silently dropped. Excluding it is allowed only after the bounded wait, and then it is counted and logged. Test: `WorkerFilterRegression.RecordOfALateVersionWaitsForItsSchemaThenDeliversRegression` (Task 8).
2. **One back-pressured filtered subscriber among many in-sync ones** must lose and duplicate nothing, must not stall the others, and must rejoin the shared scan. Tests: `FilterGroupRegression.BackPressuredMemberFallsBehindWithoutStallingOthersRegression` (Task 7) and the `BackPressure` timing of the matrix (Task 14).
3. **A truncated or short record value** on a `validate = false` topic must never be read past its end, and is counted as `Malformed`. Test: `FilterPredicateRegression.ShortValueNeverReadsPastItsEndRegression` (Task 3; it runs under ASan at every checkpoint).
4. **Tombstones (null values) on a compacted typed topic** never match and are counted, and do not wedge the cursor. Tests: `FilterPredicateRegression.NullValueNeverMatchesAndIsCountedRegression` (Task 3) and `WorkerFilterRegression.TombstonesAdvanceTheFilteredCursorRegression` (Task 8).
5. **An integer literal outside the field's range** (`.worldId = -1` on a `UInt32`, or an `int64_t` above `UINT32_MAX`) fails the encode explicitly and is never truncated into a different filter. Test: `FilterMatchRegression.OutOfRangeValueFailsEncodeNeverTruncatesRegression` (Task 2).

Also pinned: a short FixedStr literal matches the zero-padded stored field (`FilterPredicateRegression.ShortFixedStrLiteralMatchesTheZeroPaddedFieldRegression`, Task 3), and negative signed values compare by their stored bytes (`FilterPredicate.NegativeInt32EqualsItsStoredBytes`, Task 3).

---

## Interaction matrix

**Single source of truth:** `broker/libs/node/tests/support/FilterMatrixExpectations.h` (Task 14) holds `naRule(Op, Entity, Timing) -> std::optional<NaRule>` and `expect(Op, Entity, Timing) -> Outcome`. The worker matrix (Task 14), the engine stress (Task 15) and the broker slice (Task 13) read it. No test hard-codes an outcome.

The selection axis (key / partition / AllPartitions / group) is resolved before the worker: the gateway resolves the partition, and the client builds one `Subscription` per partition. It is therefore N/A at worker level by rule, and it is covered by the real-broker slice (Task 13) and the e2e (Task 17).

### Operations (5): the filter shape

| Op | Members in the partition |
|---|---|
| `Unfiltered` | one unfiltered subscription (the control) |
| `Eq` | `worldId == 7` |
| `In` | `channel ∈ {1, 3}` |
| `EqAndIn` | `worldId == 7 ∧ channel ∈ {1, 3}` (the index is worldId; channel is residual) |
| `MixedIndexFields` | member A `worldId == 7`, member B `playerId ∈ {k1, k2}` (two index fields in one group), plus one unfiltered member |

### Entities (4): topic persistence × isolation

`DiskRU` · `DiskRC` · `TransientRU` · `TransientRC`. All use the typed `player_event` schema (Task 4). The key-field variants are covered in Task 13.

### Timings (14)

| Timing | What the rig does |
|---|---|
| `SteadyAllMatch` | batches whose every record matches |
| `SteadyPartial` | batches where some records match |
| `SteadyNoMatch` | batches where no record matches |
| `StraddlingResume` | the member starts at an offset in the middle of a batch |
| `QuietRun` | 200,000 non-matching records after one match (note by count), then a 60 ms pause (note by time) |
| `OpenTxn` | a transactional batch whose transaction is still open |
| `AbortedTxn` | a transaction appended, then an abort marker |
| `CommittedTxn` | a transaction appended, then a commit marker |
| `OwnershipMove` | `SetOwnership` removes the partition mid-stream; the rig re-subscribes on a second worker from the model client's position |
| `TransientEviction` | the ring evicts records past the member's cursor |
| `AlterAddField` | FilterSchema v2 adds `guild`; v1 and v2 records interleave |
| `AlterDropFilteredField` | FilterSchema v3 drops `channel`; v3 records follow |
| `VersionArrivesLate` | a v2 record is appended before its FilterSchema op, which arrives 20 ms later; a second leg never sends it (the wait is set to 50 ms) |
| `BackPressure` | one member's egress refuses 3 times in a row, then accepts |

### Not applicable (top-down; the first matching rule names the reason)

| # | Rule | Cells |
|---|---|---|
| NA1 | `Unfiltered` × every timing except `SteadyAllMatch`, `AbortedTxn`, `OwnershipMove`, `BackPressure`. The unfiltered path is untouched; these four pin that it stays byte-identical, and the existing push suites cover the rest. | 40 |
| NA2 | `TransientEviction` × `DiskRU` / `DiskRC`: a disk partition has no ring | 8 |

### Expected outcome per applicable cell (`expect()`, top-down)

| # | Rule | Outcome |
|---|---|---|
| R1 | `Unfiltered` × applicable | `ByteIdentical`: every frame's `recordBatch` equals the log batch byte for byte; no note is sent |
| R2 | filtered × `SteadyAllMatch` | `ZeroCopyOriginal`: every frame is the log batch byte for byte |
| R3 | filtered × `SteadyPartial` | `Rebuilt`: one frame per source batch; valid CRC; same `baseOffset` and `lastOffsetDelta`; exactly the matching records with their original offsets |
| R4 | filtered × `SteadyNoMatch` | `NoteOnly`: no data frame; at least one note, and the last note's offset equals the log end |
| R5 | filtered × `StraddlingResume` | `WholeBatchFromPosition`: the first frame may be the straddling batch; the model client drops records below the start; nothing extra |
| R6 | filtered × `QuietRun` | `NotesByCountAndTime`: a note within each 65,536 skipped records and within 60 ms; offsets increase; the final one equals the log end |
| R7 | filtered × `OpenTxn` | RC: `WithheldUntilCommit` (nothing past the LSO); RU: `Delivered` |
| R8 | filtered × `AbortedTxn` | RC: `NeverDelivered` for the aborted records; RU: `Delivered` (read_uncommitted sees them) |
| R9 | filtered × `CommittedTxn` | `Delivered` (both isolations) |
| R10 | filtered × `OwnershipMove` | `ResumeExact`: after the re-subscribe, exactly the remaining matches; no duplicate and no gap |
| R11 | filtered × `TransientEviction` (transient) | `SkipEvictedCounted`: evicted records are never delivered; the E2 eviction counter increments; later matches are delivered |
| R12 | filtered × `AlterAddField` | `Delivered` across versions; v2 offsets are taken from the v2 slot |
| R13 | filtered × `AlterDropFilteredField` | `In` and `EqAndIn` (filtering on `channel`): `AbsentExcluded`, no v3 record delivered; `In` counts `absentExcluded` equal to the v3 records, `EqAndIn` equal to the v3 records with worldId 7 (its index candidates). `Eq` and `MixedIndexFields` (worldId / playerId, untouched by the drop): `Delivered`, v3 matches included |
| R14 | filtered × `VersionArrivesLate` | `StallThenDeliver`: the late leg delivers the v2 match once the schema arrives; the never leg excludes it after 50 ms and counts `unknownVersionExcluded == 1` |
| R15 | filtered × `BackPressure` | `NoLossNoDupRejoined`: no loss, no duplicate; the member is `InSync` again; the other members got every frame on time |

### Cell counts (pinned by `FilterMatrixCountRegression.CellCountsArePinned`, Task 14)

5 ops × 4 entities × 14 timings = **280 cells**: **232 applicable**, **48 `NotApplicable`** (NA1 40, NA2 8).

Applicable by rule:
- R1: 16 (4 entities × 4 timings).
- R11: 8 (4 filtered ops × 2 transient entities).
- R2–R10 and R12–R15: 16 each (4 filtered ops × 4 entities); 13 rules × 16 = 208.

Total: 16 + 208 + 8 = 232.

### Every cell asserts

1. The outcome from `expect()`.
2. **The exact delivered set.** The model client applies the record offsets, whole-batch straddle semantics and notes. Its set equals the model's matching, isolation-visible records: none extra, none missing, no duplicates, in offset order.
3. **Monotonic position:** the model position (`max(last delivered + 1, batch end, note offset)`) never decreases, and reaches the log end by the end of the cell.
4. **Unfiltered members are unaffected:** where present (`MixedIndexFields`), unfiltered members receive byte-identical log batches.
5. **Zero allocation** inside the measured passes after one warm-up pass of the same cell (`AllocCounter` window).
6. **The counters** (`absentExcluded`, `unknownVersionExcluded`, `notes`, `zeroCopy`, `rebuilt`) match the cell's expectation.

### Coverage by layer

| Layer | Task | Cells |
|---|---|---|
| Worker-level parameterized matrix (`TEST_P` over all 280; N/A cells skip and name their rule). `FilterMatrixRig`: a real `Worker`, a real disk `Log` or `InMemoryLog`, and an embedded-driver IPC egress decoded by the rig's model client | 14 | all 232 applicable |
| Engine model-checked stress (independent `FilterModel`) | 15 | 64 seeds × 600 steps: random members (Eq/In on 1–2 fields), random batches of random versions, ALTER add/drop, late schemas, refusals, unsubscribes |
| Real-broker slice (`OneBroker` / `ThreeNodes`, real client) | 13 | selection {`Key`, `Partition`, `AllPartitions`, `Group`} × filter {`Eq`, `EqAndIn`} × timing {`Steady`, `LeaderMove`, `RestartResubscribe`, `AlterDrop`, `RcAborted`}: 40 cells, plus every row of the error table below |
| Docker e2e (3 nodes, authenticated) | 17 | every selection, leader kill, node restart, ALTER, the error rows, and the MMORPG scenario |

### Error rows (spec §9 and the rulings), each with its own test (Tasks 9, 10, 11, 12, 13)

| # | Condition | Answer |
|---|---|---|
| E1 | filter on an untyped topic (incl. `__catalog`, `__security`) | `SubscribeResponse{39, "filter needs a typed topic"}` |
| E2 | malformed bytes (bad `formatVer`, truncated, term count 0 or > 8, unknown op, bad count, not canonical) | `{39, describe(error)}`: the Task 2 text of its kind, e.g. `"filter term <i>: malformed"`, `"filter: unknown format version"`, `"filter term <i>: not canonical (…)"` |
| E3 | unknown tag | `{39, "filter term <i> (tag <t>): unknown tag"}` |
| E4 | type mismatch | `{39, "filter term <i> (tag <t>): type mismatch"}` |
| E5 | variable-length field | `{39, "filter term <i> (tag <t>): variable-length field"}` |
| E6 | float field | `{39, "filter term <i> (tag <t>): float field"}` |
| E7 | > 8 terms, > 64 values, FixedStr > 16, out-of-range value | client: `std::invalid_argument` at `opts.filter()` and nothing sent; broker (raw frame): `{39, ...term...}` |
| E8 | 4,097th filtered subscription on one partition | `{39, "filter capacity: 4096 filtered subscriptions on topic <t> partition <p> (RUNE_FILTER_MAX_SUBS_PER_PARTITION)"}` |
| E9 | 4,097th distinct index value on one partition | `{39, "filter capacity: 4096 index values on topic <t> partition <p> (RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION)"}` |
| E10 | group member with a different filter | `JoinGroupResponse{39, "group filter mismatch: the group uses filter <hex>"}` → `GroupFailReason::FilterMismatch` |
| E11 | destination broker < 6 | client: `SubscriptionFailReason::BrokerTooOld` / `GroupFailReason::BrokerTooOld`, nothing sent |
| E12 | legacy worker mode | `{39, "filter needs RUNE_WORKER_MODE=shard"}` |
| E13 | session < 10 sends trailing bytes | ignored (no filter), as today |

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `protocol/include/rune/protocol/BatchRebuild.h` (new) | `RecordSpan`, `forEachRecordSpan`, `rebuildBatch` | 1 |
| `protocol/tests/protocol/BatchRebuildTest.cpp` (new), `protocol/CMakeLists.txt` | rebuild pins | 1 |
| `client-cpp/tests/subscriber/SubscriberHubRecordsRegressionTest.cpp`, `client-cpp/tests/group/GroupConsumerTest.cpp` | client decode of holed batches pinned | 1 |
| `codec/include/rune/codec/FilterWire.h` (new) | constants, `FilterOp`, `FilterKey`, `FilterBytes`, `FilterError`, `describe` | 2 |
| `codec/include/rune/codec/Filter.h` (new) | `rune::Match<T>`, `rune::anyOf`, `rune::codec::FilterWriter` | 2 |
| `codec/tests/FilterWireTest.cpp` (new) | encode, canonical form, limits, compile-time checks | 2 |
| `codec/include/rune/codec/FilterSchema.h`, `codec/src/FilterSchema.cpp` (new) | per-tag per-version slots, `parseFilter` | 3 |
| `codec/include/rune/codec/FilterPredicate.h` (new) | `CompiledFilter`, `FilterVerdict`, `recordVersion` | 3 |
| `codec/tests/FilterSchemaTest.cpp`, `codec/tests/FilterPredicateTest.cpp` (new) | schema and predicate tests | 3 |
| `codec/src/Generator.cpp`, `codec/include/rune/codec/Generator.h` | emit `E::Filter` | 4 |
| `codec/include/rune/codec/FilterBuilder.h`, `codec/src/FilterBuilder.cpp` (new) | runtime builder | 4 |
| `codec/tests/FilterGeneratedTest.cpp`, `codec/tests/FilterBuilderTest.cpp` (new), `codec/tests/GeneratorTest.cpp` | generator and builder tests | 4 |
| `client-cpp/examples/filter/PlayerEventSchema.h` (new), `client-cpp/tests/support/GenClaimSchemas.cpp` | the shared `player_event` descriptor; `player_event.gen.h` at build time | 4 |
| `codec/CMakeLists.txt`, `broker/CMakeLists.txt`, `client-cpp/CMakeLists.txt` | new codec sources (three lists) | 3, 4 |
| `protocol/include/rune/protocol/client/Protocol.h` | v10 / broker v6, `clientUnderstandsFilters` | 5 |
| `protocol/include/rune/protocol/client/DataMessages.h` | `Subscribe::filter`, `SubscribeResponse::detail` | 5 |
| `protocol/include/rune/protocol/client/GroupMessages.h` | `JoinGroup::filter`, `JoinGroupResponse::detail`, `kGroupFilterMismatchPrefix` | 5 |
| `protocol/schema/rune-internal-ring.xml`, `protocol/include/rune_protocol_internal_sbe/{OpType,TailKind}.h` | `FILTER_SCHEMA = 23`, `FILTER = 10` (regenerated) | 5 |
| `protocol/tests/protocol/client/{DataMessagesTest,GroupMessagesTest}.cpp` | wire pins | 5 |
| `broker/libs/node/include/rune/node/filter/FilterIndex.h` (new) | value → members index | 6 |
| `broker/libs/node/include/rune/node/filter/FilterGroup.h` (new) | members, states, shared scan, delivery, notes | 7 |
| `broker/libs/node/tests/node/FilterIndexTest.cpp`, `FilterGroupTest.cpp` (new) | engine unit tests | 6, 7 |
| `broker/libs/node/include/rune/node/WorkerOp.h`, `Worker.h` | `FilterSchema` op, filter tail, binding, `pushFilteredGroups_`, sink, counters | 8 |
| `broker/libs/node/tests/node/WorkerFilterPushTest.cpp` (new), `broker/libs/node/CMakeLists.txt` | worker integration | 8 |
| `broker/libs/gateway/include/rune/gateway/FilterAdmission.h`, `src/FilterAdmission.cpp` (new) | `FilterSchemaRegistry`, `FilterAdmission`, `Ticket` | 9 |
| `broker/libs/gateway/include/rune/gateway/ClientGateway.h`, `src/ClientGateway.cpp` | validation, refusal detail, ordered fan-out, `PendingSubOp` variant | 9 |
| `broker/libs/gateway/tests/gateway/FilterSubscribeGatewayTest.cpp` (new), `broker/libs/gateway/CMakeLists.txt` | gateway tests | 9 |
| `broker/libs/node/include/rune/node/AdminChannels.h`, `BrokerNode.h`, `src/BrokerNode.cpp`, `NodeConfig.h`, `src/NodeConfig.cpp` | `kA2cFilterSchema`, schema post, wiring, knobs | 9 |
| `broker/libs/coordination/include/rune/coordination/GroupCoordinator.h`, `src/GroupCoordinator.cpp` | `admitFilter`, `filterBytes` | 10 |
| `broker/libs/node/src/GroupCoordinationService.cpp`, `tests/node/GroupCoordinationServiceTest.cpp` | one filter per group | 10 |
| `client-cpp/include/rune/client/subscriber/Subscription.h`, `src/subscriber/Subscription.cpp`, `include/rune/client/subscriber/SubscriberHub.h`, `src/subscriber/SubscriberHub.cpp`, `include/rune/client/codec/DataCodec.h`, `include/rune/client/Client.h`, `src/Client.cpp` | filter option, encoding, gate, positions, detail, `AllPartitions` | 11, 12 |
| `client-cpp/include/rune/client/group/GroupConsumer.h`, `src/group/GroupConsumerHub.cpp`, `include/rune/client/group/GroupConsumerHub.h` | `GroupConfig::filter`, JoinGroup trailer, fetch filters, notes, `FilterMismatch` | 12 |
| `client-cpp/tests/subscriber/SubscriberFilterTest.cpp`, `client-cpp/tests/client/SubscriptionSetTest.cpp`, `client-cpp/tests/group/GroupFilterTest.cpp` (new), `client-cpp/tests/support/InvokerGateway.h` | client tests and fake | 11, 12 |
| `broker/libs/node/tests/client/FilterBrokerRegressionTest.cpp` (new), `broker/libs/node/CMakeLists.txt` | real-broker slice | 13 |
| `broker/libs/node/tests/support/FilterMatrixExpectations.h`, `tests/node/WorkerFilterMatrixTest.cpp` (new) | matrix | 14 |
| `broker/libs/node/tests/support/FilterModel.h`, `tests/node/FilterStressRegressionTest.cpp` (new) | stress | 15 |
| `broker/benchmark/FilterPushBench.cpp` (new), `broker/CMakeLists.txt`, `broker/libs/node/tests/node/FilterAllocRegressionTest.cpp` (new) | benches and alloc gate | 16 |
| `client-cpp/examples/filter_probe.cpp`, `client-cpp/docker/Dockerfile.filter-probe{,.dockerignore}`, `broker/docker/filter-e2e.sh` (new) | e2e | 17 |
| `docs/api/client-cpp.md`, `docs/api/codec.md`, `docs/api/wire-protocol.md`, `docs/configuration.md`, `broker/docs/release-notes/phase2c-subscription-filters.md` (new) | docs | 5, 9, 17 |

---

### Task 1: Pin the rebuild rule: holed batches keep their offsets (`BatchRebuild.h`)

The spec (§7) makes this the first task: it must establish whether a rebuilt batch may have holes in its offsets. Verified (SD10): it may. This task adds the rebuild primitive and pins three things: the protocol bytes; that the plain client delivers the exact offsets (and, today, a position of the last delivered record + 1); and that the group client moves its position to the source batch's end.

**Files:**
- Create: `protocol/include/rune/protocol/BatchRebuild.h`, `protocol/tests/protocol/BatchRebuildTest.cpp`
- Modify: `protocol/CMakeLists.txt` (add `tests/protocol/BatchRebuildTest.cpp` to `rune_protocol_tests`, after `tests/protocol/RecordBatchTest.cpp`)
- Modify (tests only): `client-cpp/tests/subscriber/SubscriberHubRecordsRegressionTest.cpp`, `client-cpp/tests/group/GroupConsumerTest.cpp`

**Interfaces:**
- Produces:
  ```cpp
  namespace rune::protocol {
  struct RecordSpan {
      std::size_t      pos    = 0;      ///< first byte of the record (its length varint) inside the batch
      std::size_t      len    = 0;      ///< the whole record, length varint included
      std::string_view value;           ///< value bytes; empty when null or empty
      bool             nullValue = false;  ///< valueLen == -1 (a tombstone)
      int64_t          offset = 0;      ///< baseOffset + offsetDelta
  };
  /// Visit every record of ONE batch (whose header and CRC the caller trusts): fn(const RecordSpan&).
  /// Every read is bounded by the batch's totalLength; returns false at the first record that does not fit.
  template <class Fn> bool forEachRecordSpan(const uint8_t* batch, std::size_t len, Fn&& fn) noexcept;
  /// Rebuild `src` keeping the records `keep` lists (ascending indices into `records`, the spans
  /// forEachRecordSpan produced for `src`).  Header fields are copied from `src`, including baseOffset,
  /// partitionLeaderEpoch, attributes, lastOffsetDelta, base/max timestamp and the producer fields.
  /// recordCount = keep.size(); length and CRC-32C are recomputed; kept records are copied verbatim.
  /// Returns the bytes written, or 0 (keep empty, an index out of range, or `out` too small).  No allocation.
  std::size_t rebuildBatch(std::span<const uint8_t> src, std::span<const RecordSpan> records,
                           std::span<const uint32_t> keep, std::span<uint8_t> out) noexcept;
  }
  ```

- [ ] **Step 1: Write the failing protocol tests** (`protocol/tests/protocol/BatchRebuildTest.cpp`):

```cpp
// BatchRebuildTest.cpp — Phase 2c Task 1: a filtered subscription's rebuilt batch keeps the source's offsets.
#include <rune/protocol/BatchRebuild.h>
#include <rune/protocol/RecordBatch.h>

#include <gtest/gtest.h>

#include <array>
#include <string>
#include <vector>

using namespace rune::protocol;

namespace {
std::vector<uint8_t> batch5(int64_t base) {
    RecordBatchBuilder b(base, 1'000'000);
    b.setProducerId(77); b.setProducerEpoch(3); b.setBaseSequence(40);
    for (int i = 0; i < 5; ++i) b.append("k" + std::to_string(i), "v" + std::to_string(i), 1'000'000 + i);
    return b.build();
}
std::vector<RecordSpan> spans(const std::vector<uint8_t>& b) {
    std::vector<RecordSpan> v;
    EXPECT_TRUE(forEachRecordSpan(b.data(), b.size(), [&](const RecordSpan& r) { v.push_back(r); }));
    return v;
}
}  // namespace

TEST(BatchRebuild, SpansCoverEveryRecordInOrder) {
    const auto b = batch5(100);
    const auto s = spans(b);
    ASSERT_EQ(s.size(), 5u);
    for (std::size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(s[i].offset, 100 + static_cast<int64_t>(i));
        EXPECT_EQ(s[i].value, "v" + std::to_string(i));
        EXPECT_FALSE(s[i].nullValue);
        if (i > 0) EXPECT_EQ(s[i].pos, s[i - 1].pos + s[i - 1].len);
    }
    EXPECT_EQ(s.back().pos + s.back().len, b.size());
}

TEST(BatchRebuildRegression, RebuiltBatchKeepsOffsetsBaseAndLastOffsetDeltaRegression) {
    const auto src = batch5(100);
    const auto s = spans(src);
    const std::array<uint32_t, 2> keep{1, 3};
    std::vector<uint8_t> out(src.size());
    const std::size_t n = rebuildBatch(src, s, keep, out);
    ASSERT_GT(n, 0u);
    RecordBatchReader r(out.data(), n);
    ASSERT_TRUE(r.ensureValid()) << "CRC recomputed";
    EXPECT_EQ(r.baseOffset(), 100);
    EXPECT_EQ(r.lastOffsetDelta(), 4) << "the rebuilt batch still spans the whole source range";
    EXPECT_EQ(r.recordCount(), 2);
    EXPECT_EQ(r.totalLength(), static_cast<int32_t>(n));
    std::vector<int64_t> offs;
    std::vector<std::string> vals;
    r.forEachRecord([&](std::string_view, std::string_view v, int64_t o, int64_t) { offs.push_back(o); vals.emplace_back(v); });
    EXPECT_EQ(offs, (std::vector<int64_t>{101, 103}));
    EXPECT_EQ(vals, (std::vector<std::string>{"v1", "v3"}));
    // The producer identity and timestamps are the source's.
    RecordBatchReader sr(src.data(), src.size());
    EXPECT_EQ(r.producerId(), sr.producerId());
    EXPECT_EQ(r.baseTimestamp(), sr.baseTimestamp());
}

TEST(BatchRebuildRegression, RefusesEmptyKeepOutOfRangeAndShortOutputRegression) {
    const auto src = batch5(0);
    const auto s = spans(src);
    std::vector<uint8_t> out(src.size());
    EXPECT_EQ(rebuildBatch(src, s, std::span<const uint32_t>{}, out), 0u);
    const std::array<uint32_t, 1> bad{5};
    EXPECT_EQ(rebuildBatch(src, s, bad, out), 0u);
    const std::array<uint32_t, 1> one{0};
    std::vector<uint8_t> tiny(batch::kRecordBatchOverhead);
    EXPECT_EQ(rebuildBatch(src, s, one, tiny), 0u);
}

TEST(BatchRebuildRegression, ARecordThatDoesNotFitTheBatchStopsTheWalkRegression) {
    auto b = batch5(0);
    // A buffer shorter than the header's totalLength: refused before any record is visited.
    auto shortBuf = b;
    shortBuf.resize(shortBuf.size() - 3);
    int seen = 0;
    EXPECT_FALSE(forEachRecordSpan(shortBuf.data(), shortBuf.size(), [&](const RecordSpan&) { ++seen; }));
    EXPECT_EQ(seen, 0);
    // A header whose length is consistent with a torn last record: the walk stops AT that record.
    b.resize(b.size() - 3);
    const auto l = static_cast<uint32_t>(b.size() - 12);
    for (int i = 0; i < 4; ++i) b[static_cast<std::size_t>(batch::kLengthOffset + i)] = static_cast<uint8_t>(l >> (8 * i));
    EXPECT_FALSE(forEachRecordSpan(b.data(), b.size(), [&](const RecordSpan&) { ++seen; }));
    EXPECT_EQ(seen, 4);
}
```
(`RecordBatchReader::producerId()` / `baseTimestamp()`: use the existing accessor names in `RecordBatch.h`. If one is missing, read the field with `detail::getLE64(d + batch::kProducerIdOffset)` in the test.)

- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: `fatal error: 'rune/protocol/BatchRebuild.h' file not found`.
- [ ] **Step 3: Implement** `protocol/include/rune/protocol/BatchRebuild.h`:

```cpp
#pragma once
// BatchRebuild.h — Phase 2c: rebuild one RecordBatch keeping a subset of its records, offsets unchanged.
// A Kafka-v2 batch may have holes in its offsets (each record carries its own offsetDelta; compaction
// leaves holes too).  A filtered subscription's partly matching batch therefore stays ONE batch: same
// baseOffset / lastOffsetDelta / timestamps / producer fields, matching records copied verbatim.
#include <rune/protocol/Crc32c.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/Varint.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <string_view>

namespace rune::protocol {

struct RecordSpan {
    std::size_t      pos       = 0;
    std::size_t      len       = 0;
    std::string_view value;
    bool             nullValue = false;
    int64_t          offset    = 0;
};

namespace detail {
// Bounded varint read: false when the varint runs past `end`.
inline bool readVarintBounded(const uint8_t* d, std::size_t end, std::size_t& pos, int64_t& out) noexcept {
    uint64_t raw = 0; int shift = 0;
    while (pos < end && shift < 64) {
        const uint8_t b = d[pos++];
        raw |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) { out = static_cast<int64_t>((raw >> 1) ^ (~(raw & 1) + 1)); return true; }   // zigzag
        shift += 7;
    }
    return false;
}
}  // namespace detail

template <class Fn>
bool forEachRecordSpan(const uint8_t* batch, std::size_t len, Fn&& fn) noexcept {
    if (len < static_cast<std::size_t>(batch::kRecordBatchOverhead)) return false;
    RecordBatchReader r(batch, len);
    const std::size_t end = static_cast<std::size_t>(r.totalLength());
    if (end > len || end < static_cast<std::size_t>(batch::kRecordBatchOverhead)) return false;
    const int64_t base = r.baseOffset();
    std::size_t pos = batch::kRecordBatchOverhead;
    for (int32_t i = 0; i < r.recordCount(); ++i) {
        const std::size_t start = pos;
        int64_t bodyLen = 0, tsDelta = 0, offDelta = 0, klen = 0, vlen = 0, hcount = 0;
        if (!detail::readVarintBounded(batch, end, pos, bodyLen) || bodyLen < 0) return false;
        const std::size_t recEnd = pos + static_cast<std::size_t>(bodyLen);
        if (recEnd > end) return false;
        if (pos >= recEnd) return false;
        ++pos;                                                               // attributes
        if (!detail::readVarintBounded(batch, recEnd, pos, tsDelta)) return false;
        if (!detail::readVarintBounded(batch, recEnd, pos, offDelta)) return false;
        if (!detail::readVarintBounded(batch, recEnd, pos, klen)) return false;
        if (klen > 0) { if (pos + static_cast<std::size_t>(klen) > recEnd) return false; pos += static_cast<std::size_t>(klen); }
        if (!detail::readVarintBounded(batch, recEnd, pos, vlen)) return false;
        RecordSpan s;
        s.pos = start; s.len = recEnd - start; s.offset = base + offDelta; s.nullValue = vlen < 0;
        if (vlen > 0) {
            if (pos + static_cast<std::size_t>(vlen) > recEnd) return false;
            s.value = std::string_view(reinterpret_cast<const char*>(batch + pos), static_cast<std::size_t>(vlen));
        }
        (void)hcount;
        fn(static_cast<const RecordSpan&>(s));
        pos = recEnd;
    }
    return true;
}

inline std::size_t rebuildBatch(std::span<const uint8_t> src, std::span<const RecordSpan> records,
                                std::span<const uint32_t> keep, std::span<uint8_t> out) noexcept {
    if (keep.empty() || src.size() < static_cast<std::size_t>(batch::kRecordBatchOverhead)) return 0;
    std::size_t need = batch::kRecordBatchOverhead;
    for (const uint32_t k : keep) {
        if (k >= records.size()) return 0;
        need += records[k].len;
    }
    if (need > out.size()) return 0;
    std::memcpy(out.data(), src.data(), batch::kRecordBatchOverhead);        // header incl. lastOffsetDelta
    std::size_t pos = batch::kRecordBatchOverhead;
    for (const uint32_t k : keep) {
        std::memcpy(out.data() + pos, src.data() + records[k].pos, records[k].len);
        pos += records[k].len;
    }
    const auto put32 = [&](int off, uint32_t v) { for (int i = 0; i < 4; ++i) out[static_cast<std::size_t>(off + i)] = static_cast<uint8_t>(v >> (8 * i)); };
    put32(batch::kLengthOffset, static_cast<uint32_t>(pos - 12));             // bytes after the length field
    put32(batch::kRecordCountOffset, static_cast<uint32_t>(keep.size()));
    put32(batch::kCrcOffset, crc32c(out.data() + batch::kAttributesOffset, pos - batch::kAttributesOffset));
    return pos;
}

}  // namespace rune::protocol
```
(If `Varint.h`'s zigzag decoding differs from the formula above, call its `readVarint` / `readVarlong` semantics instead. The bounded read must decode exactly what `RecordBatchReader::forEachRecord` decodes; `SpansCoverEveryRecordInOrder` proves it.)

- [ ] **Step 4: Write the client pins** (they use the new header to build a holed batch):
  - In `SubscriberHubRecordsRegressionTest.cpp`:
    ```cpp
    // Phase 2c Task 1: a holed (rebuilt) batch is delivered with its records' own offsets.  The plain
    // subscription's position is the last DELIVERED record + 1 (today's per-record rule); Task 11 moves a
    // FILTERED subscription's position to the batch end.
    TEST(SubscriberHubRecordsRegression, AHoledBatchDeliversItsOwnOffsetsRegression) {
        Bound b(kTopic, 12, 102);
        const auto src = batchAt(10, 5);
        std::vector<rune::protocol::RecordSpan> s;
        ASSERT_TRUE(rune::protocol::forEachRecordSpan(src.data(), src.size(), [&](const auto& r) { s.push_back(r); }));
        const std::array<uint32_t, 2> keep{0, 2};
        std::vector<uint8_t> out(src.size());
        const std::size_t n = rune::protocol::rebuildBatch(src, s, keep, out);
        ASSERT_GT(n, 0u);
        b.hub.onRecords(b.view(std::span<const uint8_t>(out.data(), n), 10));
        EXPECT_EQ(b.delivered, (std::vector<int64_t>{10, 12}));
        EXPECT_EQ(b.impl->nextOffset, 13);
        EXPECT_EQ(b.hub.malformedBatches(), 0u);
    }
    ```
  - In `GroupConsumerTest.cpp` (fixture `GroupConsumerHubTest`): `TEST_F(GroupConsumerHubTest, AHoledBatchMovesTheGroupPositionToTheSourceEndRegression)`. Do `bringToStable(*hub_, sink_, kTopicId, kPartition)` and `seedPosition(*hub_, 10)`. Build `src = 5 records at base 10`, rebuild it keeping {0, 2}, and set `rv.recordBatch` to the rebuilt span. Call `hub_->onRecords(rv)`. Expect one callback, records 10 and 12, and `impl_->assignment[tp] == 15` (base 10 + `lastOffsetDelta` 4 + 1).
- [ ] **Step 5: Run.**
  - Root: `cmake --build build/root -j14 --target rune_protocol_tests && ctest --test-dir build/root -R 'BatchRebuild|RecordBatch' --output-on-failure`.
  - Client: `cmake --build build/sub-client-cpp --target rune_subscriber_tests rune_group_tests && ctest --test-dir build/sub-client-cpp -R 'SubscriberHubRecordsRegression|GroupConsumerHubTest' --output-on-failure`.
  - Expected: all pass. The client pins pass at once: they pin current behaviour, which proves the holed-batch rule.
- [ ] **Step 6: Commit.**

```bash
git add protocol/include/rune/protocol/BatchRebuild.h protocol/tests/protocol/BatchRebuildTest.cpp protocol/CMakeLists.txt \
        client-cpp/tests/subscriber/SubscriberHubRecordsRegressionTest.cpp client-cpp/tests/group/GroupConsumerTest.cpp
git commit -m "feat(protocol): rebuildBatch keeps a subset of a batch's records with their offsets; pin holed-batch decode in both client hubs" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Filter wire format, `rune::Match<T>`, `anyOf`, `FilterWriter` (codec, header-only)

**Files:**
- Create: `codec/include/rune/codec/FilterWire.h`, `codec/include/rune/codec/Filter.h`, `codec/tests/FilterWireTest.cpp`
- Modify: `codec/CMakeLists.txt` (add `tests/FilterWireTest.cpp` to `rune_codec_tests`)

**Interfaces:**
- Produces (`FilterWire.h`, namespace `rune::codec`):
  ```cpp
  inline constexpr uint8_t     kFilterFormatVer       = 1;
  inline constexpr std::size_t kMaxFilterTerms        = 8;
  inline constexpr std::size_t kMaxFilterInValues     = 64;
  inline constexpr std::size_t kMaxFilterFixedStrLen  = 16;
  inline constexpr std::size_t kFilterHeaderBytes     = 2;
  inline constexpr std::size_t kFilterTermHeaderBytes = 5;
  inline constexpr std::size_t kMaxFilterBytes =
      kFilterHeaderBytes + kMaxFilterTerms * (kFilterTermHeaderBytes + kMaxFilterInValues * kMaxFilterFixedStrLen);
  static_assert(kMaxFilterBytes == 8234, "spec §5");
  enum class FilterOp : uint8_t { Eq = 1, In = 2 };            // wire values, append-only
  [[nodiscard]] constexpr bool    isFilterableType(PrimType t) noexcept;   // Int32/Int64/UInt16/UInt32/UInt64/Enum/Bool/FixedStr
  [[nodiscard]] inline bool       isFilterable(const FieldDescriptor& f) noexcept;  // + FixedStr fixedLen in [1,16]
  [[nodiscard]] constexpr uint8_t filterValueWidth(PrimType t, uint16_t fixedLen) noexcept;  // 0 = not filterable
  struct FilterKey { uint64_t hi = 0; uint64_t lo = 0; friend constexpr auto operator<=>(const FilterKey&, const FilterKey&) = default; };
  [[nodiscard]] constexpr FilterKey loadFilterKey(const uint8_t* p, uint8_t width) noexcept;  // LE bytes, zero-extended to 16
  struct FilterBytes {
      std::array<uint8_t, kMaxFilterBytes> bytes{};
      uint16_t len = 0;                                        // 0 = no filter
      [[nodiscard]] constexpr std::span<const uint8_t> span() const noexcept { return {bytes.data(), len}; }
      [[nodiscard]] constexpr bool empty() const noexcept { return len == 0; }
      friend constexpr bool operator==(const FilterBytes& a, const FilterBytes& b) noexcept;   // len + prefix
  };
  enum class FilterErrorKind : uint8_t {
      Malformed, BadFormatVersion, TermCount, UnknownOp, ValueCount, NotCanonical,
      TooManyTerms, TooManyValues, ValueOutOfRange, UnknownTag, TypeMismatch,
      VariableLength, FloatField, FixedStrTooLong, UntypedTopic, Capacity, BufferTooSmall, UnknownField };
  struct FilterError { FilterErrorKind kind = FilterErrorKind::Malformed; uint8_t termIndex = 0xFF; uint16_t tag = 0; };
  [[nodiscard]] std::string describe(const FilterError& e);    // cold; the §9 detail texts (inline in the header)
  ```
- Produces (`Filter.h`): `rune::Match<T>`, `rune::AnyOf<U,N>`, `rune::AnyOfSpan<U>`, `rune::anyOf(...)`, `rune::FilterValueFor<T,U>`, and `rune::codec::FilterWriter` with `explicit FilterWriter(std::span<uint8_t> out) noexcept`, `template <class T> constexpr void term(uint16_t tag, PrimType type, uint8_t width, const rune::Match<T>& m) noexcept`, `[[nodiscard]] constexpr std::expected<std::size_t, FilterError> finish() noexcept`.

- [ ] **Step 1: Write the failing tests** (`codec/tests/FilterWireTest.cpp`):

```cpp
// FilterWireTest.cpp — Phase 2c Task 2: the filter encoding (spec §5), Match<T> / anyOf (spec §4).
#include <rune/codec/Filter.h>
#include <rune/codec/FilterWire.h>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>

using namespace rune::codec;
using rune::Match;
using rune::anyOf;

namespace {
template <class T> FilterBytes encodeOne(uint16_t tag, PrimType t, uint8_t w, const Match<T>& m) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(tag, t, w, m);
    auto r = wr.finish();
    EXPECT_TRUE(r.has_value());
    b.len = static_cast<uint16_t>(r.value_or(0));
    return b;
}
}  // namespace

TEST(FilterWire, EqU32IsElevenBytes) {   // spec §5: worldId == x (u32) is 2 + 5 + 4
    const auto b = encodeOne<uint32_t>(1, PrimType::UInt32, 4, Match<uint32_t>(0x01020304u));
    ASSERT_EQ(b.len, 11);
    const std::array<uint8_t, 11> want{1, 1, /*tag*/ 1, 0, /*type*/ static_cast<uint8_t>(PrimType::UInt32),
                                       /*op Eq*/ 1, /*count*/ 1, 0x04, 0x03, 0x02, 0x01};
    EXPECT_TRUE(std::equal(want.begin(), want.end(), b.bytes.begin()));
}

TEST(FilterWire, EmptyFilterIsZeroBytes) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(1, PrimType::UInt32, 4, Match<uint32_t>{});   // unset: no term
    auto r = wr.finish();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(*r, 0u);
}

TEST(FilterWireRegression, EqualFiltersEncodeIdenticallyRegression) {   // spec §5 canonical form
    const auto a = encodeOne<int32_t>(2, PrimType::Int32, 4, Match<int32_t>(anyOf(3, 1, 2, 1)));
    const auto b = encodeOne<int32_t>(2, PrimType::Int32, 4, Match<int32_t>(anyOf(1, 2, 3)));
    EXPECT_EQ(a, b) << "In values sorted and de-duplicated";
    EXPECT_EQ(a.bytes[6], 3) << "count after de-duplication";
}

TEST(FilterWireRegression, TermsMustBeWrittenInAscendingTagOrderRegression) {
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(5, PrimType::Int32, 4, Match<int32_t>(1));
    wr.term(2, PrimType::Int32, 4, Match<int32_t>(1));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::NotCanonical);
    EXPECT_EQ(r.error().termIndex, 1);
}

TEST(FilterWireRegression, NineTermsFailAtEncodeRegression) {   // spec §4 term limit
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    for (uint16_t t = 0; t < 9; ++t) wr.term(t, PrimType::Int32, 4, Match<int32_t>(1));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::TooManyTerms);
}

TEST(FilterWireRegression, RuntimeAnyOfOverSixtyFourFailsAtEncodeRegression) {
    std::array<int32_t, 65> v{};
    for (int i = 0; i < 65; ++i) v[static_cast<std::size_t>(i)] = i;
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(0, PrimType::Int32, 4, Match<int32_t>(anyOf(std::span<const int32_t>(v))));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::TooManyValues);
    EXPECT_EQ(r.error().termIndex, 0);
}

TEST(FilterMatchRegression, OutOfRangeValueFailsEncodeNeverTruncatesRegression) {   // Review Focus #5
    FilterBytes b;
    FilterWriter wr(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr.term(1, PrimType::UInt32, 4, Match<uint32_t>(int64_t{-1}));
    auto r = wr.finish();
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error().kind, FilterErrorKind::ValueOutOfRange);
    FilterWriter wr2(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
    wr2.term(1, PrimType::UInt32, 4, Match<uint32_t>(int64_t{0x1'0000'0000}));
    EXPECT_FALSE(wr2.finish().has_value());
}

TEST(FilterMatch, CompileTimeTypeRules) {   // spec §4: a wrong value type does not compile
    static_assert(std::is_constructible_v<Match<uint32_t>, int>, "integer literals are accepted (range checked at encode)");
    static_assert(!std::is_constructible_v<Match<uint32_t>, double>, "a float is not an integer field value");
    static_assert(!std::is_constructible_v<Match<uint32_t>, const char*>, "a string is not an integer field value");
    static_assert(!std::is_constructible_v<Match<bool>, int>, "bool fields take bool");
    static_assert(!std::is_constructible_v<Match<int32_t>, bool>, "integer fields do not take bool");
    static_assert(std::is_constructible_v<Match<std::array<char, 8>>, const char (&)[3]>, "a short FixedStr literal");
    static_assert(!std::is_constructible_v<Match<std::array<char, 8>>, const char (&)[10]>, "a FixedStr literal longer than N");
    static_assert(std::is_trivially_copyable_v<Match<uint32_t>>, "E::Filter is a plain aggregate (spec §4)");
    SUCCEED();
}

TEST(FilterWire, ConstexprEncodingOfAConstantFilter) {   // spec §4 cost: constexpr encoding possible
    constexpr auto b = [] {
        FilterBytes x;
        FilterWriter wr(std::span<uint8_t>(x.bytes.data(), x.bytes.size()));
        wr.term(1, PrimType::UInt32, 4, Match<uint32_t>(7u));
        x.len = static_cast<uint16_t>(*wr.finish());
        return x;
    }();
    static_assert(b.len == 11);
    SUCCEED();
}

TEST(FilterWire, FixedStrValueIsZeroPaddedToItsWidth) {
    const auto b = encodeOne<std::array<char, 8>>(3, PrimType::FixedStr, 8, Match<std::array<char, 8>>("eu"));
    ASSERT_EQ(b.len, 2 + 5 + 8);
    EXPECT_EQ(b.bytes[7], 'e'); EXPECT_EQ(b.bytes[8], 'u');
    for (int i = 9; i < 15; ++i) EXPECT_EQ(b.bytes[static_cast<std::size_t>(i)], 0);
}

TEST(FilterWire, DescribeNamesTheTermIndex) {
    EXPECT_EQ(describe({FilterErrorKind::TypeMismatch, 2, 7}), "filter term 2 (tag 7): type mismatch");
    EXPECT_EQ(describe({FilterErrorKind::UntypedTopic, 0xFF, 0}), "filter needs a typed topic");
}
```

- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/sub-codec --target rune_codec_tests`. Expected: `'rune/codec/Filter.h' file not found`.
- [ ] **Step 3: Implement `FilterWire.h`.**
  - `filterValueWidth`: `Int32`, `UInt32` and `Enum` are 4; `Int64` and `UInt64` are 8; `UInt16` is 2; `Bool` is 1; `FixedStr` is `fixedLen` when 1 ≤ `fixedLen` ≤ 16; everything else is 0.
  - `loadFilterKey`: byte `i` of `p` (for `i < width`) goes to bit `8*i` of `lo` (`i < 8`) or of `hi` (`i ≥ 8`). The key compares `hi` first, then `lo` (member order), so FixedStr ordering is defined.
  - `describe` texts:
    - `Malformed`: `"filter term <i>: malformed"`, or `"filter: malformed"` when `termIndex == 0xFF`.
    - `BadFormatVersion`: `"filter: unknown format version"`.
    - `TermCount`: `"filter: term count must be 1..8"`.
    - `UnknownOp`: `"filter term <i>: unknown op"`.
    - `ValueCount`: `"filter term <i>: value count must be 1 for Eq and 1..64 for In"`.
    - `NotCanonical`: `"filter term <i>: not canonical (terms by tag, In values sorted and unique)"`.
    - `TooManyTerms`: `"filter: more than 8 terms"`.
    - `TooManyValues`: `"filter term <i>: more than 64 values"`.
    - `ValueOutOfRange`: `"filter term <i> (tag <t>): value out of the field's range"`.
    - `UnknownTag`: `"filter term <i> (tag <t>): unknown tag"`.
    - `TypeMismatch`: `"filter term <i> (tag <t>): type mismatch"`.
    - `VariableLength`: `"filter term <i> (tag <t>): variable-length field"`.
    - `FloatField`: `"filter term <i> (tag <t>): float field"`.
    - `FixedStrTooLong`: `"filter term <i> (tag <t>): string(N) longer than 16"`.
    - `UntypedTopic`: `"filter needs a typed topic"`.
    - `Capacity`: `"filter capacity"` (callers append the knob).
    - `BufferTooSmall`: `"filter: output buffer too small"`.
    - `UnknownField`: `"filter term <i>: unknown field"`.
- [ ] **Step 4: Implement `Filter.h`.**

```cpp
#pragma once
// Filter.h — Phase 2c: the filter-by-example member type (spec §4) and the canonical encoder (spec §5).
#include <rune/codec/FilterWire.h>
#include <rune/codec/PrimType.h>

#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <limits>
#include <span>
#include <type_traits>

namespace rune {

template <class U, std::size_t N> struct AnyOf { std::array<U, N> v; };
template <class U> struct AnyOfSpan { std::span<const U> v; };

template <class... U>
    requires(sizeof...(U) >= 1)
constexpr auto anyOf(U... v) {
    static_assert(sizeof...(U) <= codec::kMaxFilterInValues, "anyOf: at most 64 values (spec §4)");
    using C = std::common_type_t<U...>;
    return AnyOf<C, sizeof...(U)>{{static_cast<C>(v)...}};
}
template <class U> constexpr AnyOfSpan<U> anyOf(std::span<const U> v) noexcept { return {v}; }

template <class T, class U>
concept FilterValueFor = std::same_as<T, U> ||
    (std::integral<T> && std::integral<U> && !std::same_as<T, bool> && !std::same_as<U, bool>);

template <class T> struct IsCharArray : std::false_type {};
template <std::size_t N> struct IsCharArray<std::array<char, N>> : std::true_type {};

/// One member of a generated E::Filter: unset (any value), Eq (one value) or In (up to 64 values, inline).
template <class T>
class Match {
public:
    enum class State : uint8_t { Unset, Eq, In };
    static constexpr std::size_t kMaxValues = codec::kMaxFilterInValues;

    constexpr Match() noexcept = default;
    template <class U> requires FilterValueFor<T, U>
    constexpr Match(U v) noexcept : s_(State::Eq) { push_(v); }
    template <class U, std::size_t N> requires FilterValueFor<T, U>
    constexpr Match(const AnyOf<U, N>& a) noexcept : s_(State::In) { for (const U& x : a.v) push_(x); }
    template <class U> requires FilterValueFor<T, U>
    constexpr Match(AnyOfSpan<U> a) noexcept : s_(State::In) {
        if (a.v.size() > kMaxValues) { overflow_ = true; return; }
        for (const U& x : a.v) push_(x);
    }

    [[nodiscard]] constexpr State       state() const noexcept { return s_; }
    [[nodiscard]] constexpr std::size_t size() const noexcept { return n_; }
    [[nodiscard]] constexpr bool        overflow() const noexcept { return overflow_; }
    [[nodiscard]] constexpr bool        outOfRange() const noexcept { return outOfRange_; }
    [[nodiscard]] constexpr const T&    operator[](std::size_t i) const noexcept { return v_[i]; }

private:
    template <class U> constexpr void push_(U x) noexcept {
        if constexpr (!std::same_as<T, U>) {
            if (!std::in_range<T>(x)) { outOfRange_ = true; return; }
        }
        v_[n_++] = static_cast<T>(x);
    }
    std::array<T, kMaxValues> v_{};
    uint8_t n_ = 0;
    State   s_ = State::Unset;
    bool    overflow_ = false;
    bool    outOfRange_ = false;
};

/// FixedStr members: std::array<char, N> values; literals are zero-padded like fly::storeFixedStr.
template <std::size_t N>
class Match<std::array<char, N>> {
public:
    using V = std::array<char, N>;
    enum class State : uint8_t { Unset, Eq, In };
    static constexpr std::size_t kMaxValues = codec::kMaxFilterInValues;
    constexpr Match() noexcept = default;
    constexpr Match(const V& v) noexcept : s_(State::Eq) { v_[n_++] = v; }
    template <std::size_t M> requires(M - 1 <= N)
    constexpr Match(const char (&lit)[M]) noexcept : s_(State::Eq) { v_[n_++] = pad_(lit, M - 1); }
    template <std::size_t K>
    constexpr Match(const AnyOf<const char*, K>& a) noexcept : s_(State::In) { for (const char* p : a.v) pushC_(p); }
    template <std::size_t K>
    constexpr Match(const AnyOf<V, K>& a) noexcept : s_(State::In) { for (const V& x : a.v) v_[n_++] = x; }
    [[nodiscard]] constexpr State       state() const noexcept { return s_; }
    [[nodiscard]] constexpr std::size_t size() const noexcept { return n_; }
    [[nodiscard]] constexpr bool        overflow() const noexcept { return false; }
    [[nodiscard]] constexpr bool        outOfRange() const noexcept { return outOfRange_; }
    [[nodiscard]] constexpr const V&    operator[](std::size_t i) const noexcept { return v_[i]; }
private:
    static constexpr V pad_(const char* p, std::size_t len) noexcept { V v{}; for (std::size_t i = 0; i < len && i < N; ++i) v[i] = p[i]; return v; }
    constexpr void pushC_(const char* p) noexcept {
        std::size_t len = 0; while (p[len] != '\0') ++len;
        if (len > N) { outOfRange_ = true; return; }
        v_[n_++] = pad_(p, len);
    }
    std::array<V, kMaxValues> v_{};
    uint8_t n_ = 0;
    State   s_ = State::Unset;
    bool    outOfRange_ = false;
};

}  // namespace rune

namespace rune::codec {

/// Straight stores into a caller buffer.  Callers (generated code, FilterBuilder) pass terms in ascending
/// tag order; In values are sorted (FilterKey order) and de-duplicated in place.  No allocation.
class FilterWriter {
public:
    constexpr explicit FilterWriter(std::span<uint8_t> out) noexcept : out_(out), pos_(kFilterHeaderBytes) {}

    template <class T>
    constexpr void term(uint16_t tag, PrimType type, uint8_t width, const rune::Match<T>& m) noexcept {
        if (err_ || m.state() == rune::Match<T>::State::Unset) return;
        const uint8_t idx = terms_;
        if (terms_ == kMaxFilterTerms) { fail_({FilterErrorKind::TooManyTerms, 0xFF, tag}); return; }
        if (terms_ > 0 && tag <= lastTag_) { fail_({FilterErrorKind::NotCanonical, idx, tag}); return; }
        if (m.overflow()) { fail_({FilterErrorKind::TooManyValues, idx, tag}); return; }
        if (m.outOfRange()) { fail_({FilterErrorKind::ValueOutOfRange, idx, tag}); return; }
        const std::size_t head = pos_;
        if (pos_ + kFilterTermHeaderBytes + m.size() * width > out_.size()) { fail_({FilterErrorKind::BufferTooSmall, idx, tag}); return; }
        out_[pos_++] = static_cast<uint8_t>(tag); out_[pos_++] = static_cast<uint8_t>(tag >> 8);
        out_[pos_++] = static_cast<uint8_t>(type);
        out_[pos_++] = static_cast<uint8_t>(m.state() == rune::Match<T>::State::Eq ? FilterOp::Eq : FilterOp::In);
        const std::size_t countAt = pos_++;
        const std::size_t first = pos_;
        uint8_t count = 0;
        for (std::size_t i = 0; i < m.size(); ++i) {
            storeValue_(m[i], width);
            // insertion into the sorted run [first, pos_ - width): de-duplicate.
            if (!insertSorted_(first, count, width)) pos_ -= width; else ++count;
        }
        out_[countAt] = count;
        (void)head;
        lastTag_ = tag;
        ++terms_;
    }

    [[nodiscard]] constexpr std::expected<std::size_t, FilterError> finish() noexcept {
        if (err_) return std::unexpected(*err_);
        if (terms_ == 0) return std::size_t{0};
        out_[0] = kFilterFormatVer;
        out_[1] = terms_;
        return pos_;
    }

private:
    template <class T> constexpr void storeValue_(const T& v, uint8_t width) noexcept {
        if constexpr (rune::IsCharArray<T>::value) {
            for (std::size_t i = 0; i < width; ++i) out_[pos_ + i] = static_cast<uint8_t>(v[i]);
        } else {
            using UT = std::make_unsigned_t<std::conditional_t<std::same_as<T, bool>, uint8_t, T>>;
            const auto u = static_cast<uint64_t>(static_cast<UT>(v));
            for (std::size_t i = 0; i < width; ++i) out_[pos_ + i] = static_cast<uint8_t>(u >> (8 * i));
        }
        pos_ += width;
    }
    // The value just stored at [pos_-width, pos_) moves into sorted position among `count` earlier ones;
    // false when it equals one of them (the caller drops it).
    constexpr bool insertSorted_(std::size_t first, uint8_t count, uint8_t width) noexcept {
        std::size_t at = pos_ - width;
        const FilterKey k = loadFilterKeyFrom_(at, width);
        std::size_t i = count;
        while (i > 0) {
            const FilterKey prev = loadFilterKeyFrom_(first + (i - 1) * width, width);
            if (prev == k) return false;
            if (prev < k) break;
            --i;
        }
        for (std::size_t j = count; j > i; --j)
            for (std::size_t b = 0; b < width; ++b) out_[first + j * width + b] = out_[first + (j - 1) * width + b];
        storeKey_(first + i * width, k, width);
        return true;
    }
    constexpr FilterKey loadFilterKeyFrom_(std::size_t at, uint8_t width) const noexcept { return loadFilterKey(out_.data() + at, width); }
    constexpr void storeKey_(std::size_t at, FilterKey k, uint8_t width) noexcept {
        for (std::size_t b = 0; b < width; ++b)
            out_[at + b] = static_cast<uint8_t>(b < 8 ? (k.lo >> (8 * b)) : (k.hi >> (8 * (b - 8))));
    }
    constexpr void fail_(FilterError e) noexcept { if (!err_) err_ = e; }

    std::span<uint8_t> out_;
    std::size_t pos_ = 0;
    uint8_t terms_ = 0;
    uint16_t lastTag_ = 0;
    std::optional<FilterError> err_;
};

}  // namespace rune::codec
```
(Include `<optional>`. `loadFilterKey` must be `constexpr` (byte loops, no `memcpy`) for `ConstexprEncodingOfAConstantFilter`.)

- [ ] **Step 5: Run.** `cmake --build build/sub-codec --target rune_codec_tests && ctest --test-dir build/sub-codec -R 'FilterWire|FilterMatch' --output-on-failure`. Expected: PASS.
- [ ] **Step 6: Commit.** Run `git add codec/include/rune/codec/FilterWire.h codec/include/rune/codec/Filter.h codec/tests/FilterWireTest.cpp codec/CMakeLists.txt`, then `git commit -m "feat(codec): filter wire format, rune::Match / anyOf, canonical FilterWriter (spec §4-§5)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 3: `FilterSchema` (per-tag, per-version slots), `parseFilter`, `CompiledFilter` (codec)

**Files:**
- Create: `codec/include/rune/codec/FilterSchema.h`, `codec/src/FilterSchema.cpp`, `codec/include/rune/codec/FilterPredicate.h`, `codec/tests/FilterSchemaTest.cpp`, `codec/tests/FilterPredicateTest.cpp`
- Modify: `codec/CMakeLists.txt` (`src/FilterSchema.cpp` in `rune_codec`; both tests in `rune_codec_tests`), `broker/CMakeLists.txt` (its `rune_codec` list, after `AlterOps.cpp`), `client-cpp/CMakeLists.txt` (its `rune_codec` list, after `Evolve.cpp`)

**Interfaces:**
- Consumes: Task 2 (`FilterKey`, `FilterOp`, `FilterError`, `isFilterable`, `filterValueWidth`, `loadFilterKey`); `rune::codec::materializeAsOf` (`Evolve.h`); `rune::codec::Descriptor` / `FieldDescriptor`.
- Produces:
  ```cpp
  namespace rune::codec {
  struct FieldSlot { uint16_t offset = 0; uint8_t width = 0; PrimType type = PrimType::Int32; bool present = false; };
  // offset: byte offset in the record VALUE ([blockLength u16][version u16][fixed block]) — i.e. 4 + block offset.
  class FilterSchema {
  public:
      enum class Ineligible : uint8_t { No, VariableLength, Float, FixedStrTooLong };
      struct Field { uint16_t tag; PrimType type; uint16_t fixedLen; Ineligible why; std::vector<FieldSlot> byVersion; };
      /// Cold.  Every field ever declared in `evolving` (live and retired), sorted by tag; slots for v in 1..currentVersion
      /// from materializeAsOf(evolving, v).  Throws std::invalid_argument on a malformed descriptor.
      static FilterSchema build(const Descriptor& evolving, uint16_t currentVersion);
      /// Cold.  In place: extend with a newer build of the SAME topic (more versions, more tags); existing field indices
      /// stay valid.  false (and *this unchanged) when `newer` is not an extension (a tag's type or fixedLen differs).
      [[nodiscard]] bool extendFrom(const FilterSchema& newer);
      [[nodiscard]] int32_t     indexOfTag(uint16_t tag) const noexcept;   // -1 = unknown
      [[nodiscard]] const Field& field(int32_t idx) const noexcept;
      [[nodiscard]] std::size_t fieldCount() const noexcept;
      [[nodiscard]] uint16_t    maxVersion() const noexcept;
      [[nodiscard]] bool        knowsVersion(uint16_t v) const noexcept { return v >= 1 && v <= maxVersion_; }
      [[nodiscard]] FieldSlot   slot(int32_t idx, uint16_t version) const noexcept {
          const auto& v = fields_[static_cast<std::size_t>(idx)].byVersion;
          return version < v.size() ? v[version] : FieldSlot{};
      }
  private:
      std::vector<Field> fields_;
      uint16_t maxVersion_ = 0;
  };
  struct ParsedTerm { int32_t field = -1; uint16_t tag = 0; FilterOp op = FilterOp::Eq; PrimType type = PrimType::Int32;
                      uint8_t width = 0; uint8_t count = 0; std::array<FilterKey, kMaxFilterInValues> values{}; };
  struct ParsedFilter { std::array<ParsedTerm, kMaxFilterTerms> terms{}; uint8_t termCount = 0; };
  /// Structural + schema validation (spec §6 step 1).  Errors name the term index.  No allocation.
  [[nodiscard]] std::expected<ParsedFilter, FilterError> parseFilter(std::span<const uint8_t> bytes, const FilterSchema& schema) noexcept;
  }
  // FilterPredicate.h
  namespace rune::codec {
  enum class FilterVerdict : uint8_t { Match, NoMatch, Absent, UnknownVersion, Malformed, NullValue };
  [[nodiscard]] inline uint16_t recordVersion(const uint8_t* value) noexcept { return uint16_t(value[2] | (value[3] << 8)); }
  class CompiledFilter {
  public:
      struct Term { int32_t field; FilterOp op; uint8_t width; uint8_t count; uint16_t first; PrimType type; uint16_t tag; };
      static std::expected<CompiledFilter, FilterError> compile(std::span<const uint8_t> bytes, const FilterSchema& schema); // cold
      [[nodiscard]] uint8_t     indexTerm() const noexcept;           // first Eq in tag order, else first In
      [[nodiscard]] uint8_t     termCount() const noexcept;
      [[nodiscard]] const Term& term(uint8_t i) const noexcept;
      [[nodiscard]] std::span<const FilterKey> values(uint8_t i) const noexcept;    // sorted ascending
      /// Every term.  `isNull` = the record's value is null (tombstone).
      [[nodiscard]] FilterVerdict evaluate(std::span<const uint8_t> value, bool isNull, const FilterSchema& s) const noexcept;
      /// Every term except indexTerm() (the shared scan already matched it).  `version` = recordVersion(value).
      [[nodiscard]] FilterVerdict evaluateResidual(std::span<const uint8_t> value, uint16_t version, const FilterSchema& s) const noexcept;
  private:
      std::array<Term, kMaxFilterTerms> terms_{};
      uint8_t n_ = 0, index_ = 0;
      std::vector<FilterKey> values_;   // allocated once at compile (cold)
  };
  }
  ```
- Verdict precedence (documented on `evaluate`), evaluated in this order:
  1. `NullValue`: the value is null (FD7).
  2. `Malformed`: `value.size() < 4`, or a present slot's `offset + width > value.size()`.
  3. `UnknownVersion`: `!s.knowsVersion(version)`.
  4. `Absent`: some term's slot is not `present` in that version, or its slot type differs from the term type (a future retype).
  5. `NoMatch`: some term's value differs (`Eq`) or is not in the sorted list (`In`: a linear scan for count ≤ 8, else binary search).
  6. `Match`.
- `parseFilter` checks, each failure naming `termIndex`:
  - `formatVer == 1`; `termCount` in 1..8.
  - Each term: the op is 1 or 2; count is 1 for `Eq` and 1..64 for `In`.
  - The tag is known (`UnknownTag`) and the field is eligible (`VariableLength` / `FloatField` / `FixedStrTooLong`).
  - The term's `primType` equals the field's type (`TypeMismatch`).
  - The values fit the remaining bytes (`Malformed`).
  - Tags are strictly ascending, and values are strictly ascending by `FilterKey` (`NotCanonical`).
  - The bytes are consumed exactly (`Malformed` on trailing bytes).

- [ ] **Step 1: Write the failing tests.**
  - `FilterSchemaTest.cpp` builds `player_event` v1 with `DescriptorBuilder("player_event")`. Fields and tags: `.addTagged("playerId", 0, UInt64, 1, 0xFFFF, true, {})`, `worldId` 1 `UInt32`, `channel` 2 `Int32`, `zone` 3 `FixedStr(8)` (via `addFixedStrTagged`), `kind` 4 `Enum{1,2,3}`, `alive` 5 `Bool`, `hp` 6 `Int64`, `name` 7 `String`, `pos` 8 `Float64`. It then evolves the descriptor with `applyOps` to v2 (ADD `guild` `UInt32`) and v3 (DROP `channel`). Tests:
    - `FilterSchema.SlotsMatchMaterializedOffsetsPerVersion`: for every field and version, `slot.offset == 4 + materializeAsOf(d, v).find(name)->offset`.
    - `FilterSchema.RetiredFieldIsAbsentInLaterVersions` (`channel` present in v1 and v2, absent in v3).
    - `FilterSchema.NewFieldIsAbsentInEarlierVersions` (`guild` absent in v1).
    - `FilterSchema.VarLenAndFloatFieldsAreIneligible`.
    - `FilterSchema.ExtendFromKeepsFieldIndicesStable`: `indexOfTag(1)` is the same before and after extending a v1 build with the v3 build.
    - `FilterSchemaRegression.ExtendFromRefusesAChangedFieldTypeRegression`.
    - `FilterSchema.ParseRejectsEveryMalformedShape`: a table-driven test over bad `formatVer`, term count 0 and 9, op 3, Eq count 2, In count 0 and 65, a truncated value, trailing bytes, descending tags and duplicate `In` values. Each case asserts its `FilterErrorKind` and `termIndex`.
    - `FilterSchema.ParseRejectsSchemaMismatches`: unknown tag 42; tag 7 (`String`) → `VariableLength`; tag 8 → `FloatField`; tag 1 sent as `Int64` → `TypeMismatch`.
  - `FilterPredicateTest.cpp` encodes records with the generated-style layout. A helper writes `[blockLength][version][block]` with `fly::store*`, at the materialized offsets of the given version. Tests:
    - `FilterPredicate.EqAndInMatchAndMiss`.
    - `FilterPredicate.IndexTermIsFirstEqElseFirstIn` (`EqAndIn` → the Eq term; `In`-only → term 0).
    - `FilterPredicate.ResidualSkipsTheIndexTerm`.
    - `FilterPredicate.NegativeInt32EqualsItsStoredBytes`.
    - `FilterPredicateRegression.ShortFixedStrLiteralMatchesTheZeroPaddedFieldRegression`: a `zone = "eu"` record stored with `storeFixedStr` matches the filter built with `Match<std::array<char,8>>("eu")`.
    - `FilterPredicate.DroppedFieldIsAbsentNotNoMatch` (v3 record, `channel` term → `Absent`).
    - `FilterPredicate.UnknownVersionIsReportedNotGuessed` (a version 9 record → `UnknownVersion`).
    - `FilterPredicateRegression.NullValueNeverMatchesAndIsCountedRegression`: `isNull` → `NullValue` for every filter shape.
    - `FilterPredicateRegression.ShortValueNeverReadsPastItsEndRegression`: Review Focus #3. Values of sizes 0..(slot end − 1) built in a heap buffer exactly that long (so ASan sees any over-read) → `Malformed`; never `Match`.
    - `FilterPredicate.CompileAllocatesOnceEvaluateNever`: `AllocCounter` around 10,000 `evaluate` calls is 0. Use a local `operator new` counter in the test translation unit, guarded by `RUNE_FILTER_PREDICATE_ALLOC_TEST`, or link the codec copy of `AllocCounter`. If the codec has no counter, add a 20-line counting `operator new` to `codec/tests/FilterPredicateTest.cpp` itself.
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/sub-codec --target rune_codec_tests`. Expected: missing headers.
- [ ] **Step 3: Implement `FilterSchema.cpp`.** For `build`:
  1. Collect every field of `evolving`, sort by tag, and classify eligibility (`isFilterable`; `Float64` → `Float`; `String` / `Bytes` → `VariableLength`; FixedStr > 16 → `FixedStrTooLong`).
  2. For each `v` in 1..`currentVersion`: `const Descriptor m = materializeAsOf(evolving, v)`. For each eligible field found by tag in `m` (match `FieldDescriptor::id`), set `byVersion[v] = {uint16_t(4 + f.offset), filterValueWidth(f.type, f.fixedLen), f.type, true}`.
  3. Size each `byVersion` to `currentVersion + 1`.
  For `extendFrom`: for each `newer` field, find the same tag. If it is new, append it. If it exists with a different `type` or `fixedLen`, return false. Otherwise grow `byVersion` and copy the slots for `v > maxVersion_`. Then set `maxVersion_ = newer.maxVersion_`.
- [ ] **Step 4: Implement `FilterPredicate.h`** (inline). The compile step copies each parsed term's values into `values_` (one `reserve` for the total count) and records `first` (the index into `values_`). Evaluate per term:

```cpp
[[nodiscard]] inline FilterVerdict evalTerm_(const CompiledFilter::Term& t, std::span<const FilterKey> vals,
                                             std::span<const uint8_t> value, uint16_t version, const FilterSchema& s) noexcept {
    const FieldSlot sl = s.slot(t.field, version);
    if (!sl.present || sl.type != t.type) return FilterVerdict::Absent;
    if (static_cast<std::size_t>(sl.offset) + sl.width > value.size()) return FilterVerdict::Malformed;
    const FilterKey k = loadFilterKey(value.data() + sl.offset, sl.width);
    if (t.op == FilterOp::Eq) return k == vals[0] ? FilterVerdict::Match : FilterVerdict::NoMatch;
    if (vals.size() <= 8) { for (const FilterKey& v : vals) if (v == k) return FilterVerdict::Match; return FilterVerdict::NoMatch; }
    return std::binary_search(vals.begin(), vals.end(), k) ? FilterVerdict::Match : FilterVerdict::NoMatch;
}
```
`evaluate` applies checks 1–3, then loops over all terms keeping the "worst" verdict in the precedence order above (`Malformed` > `Absent` > `NoMatch` > `Match`). `evaluateResidual` does the same, skipping `index_`.
- [ ] **Step 5: Run.** `cmake --build build/sub-codec --target rune_codec_tests && ctest --test-dir build/sub-codec -R 'FilterSchema|FilterPredicate|FilterWire|Evolve|MixedVersion' --output-on-failure`. Also build the two other copies: `cmake --build build/root -j14 --target rune_codec` and `cmake --build build/sub-client-cpp --target rune_codec`. Expected: PASS; both copies compile.
- [ ] **Step 6: Commit.** Run `git add codec/include/rune/codec/FilterSchema.h codec/src/FilterSchema.cpp codec/include/rune/codec/FilterPredicate.h codec/tests/FilterSchemaTest.cpp codec/tests/FilterPredicateTest.cpp codec/CMakeLists.txt broker/CMakeLists.txt client-cpp/CMakeLists.txt`, then `git commit -m "feat(codec): FilterSchema (per-tag per-version slots), parseFilter, CompiledFilter with three-valued verdicts" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 4: The generator emits `E::Filter`; the runtime `FilterBuilder`; the shared `player_event` schema

**Files:**
- Modify: `codec/src/Generator.cpp` (emit the nested `struct Filter`; add `#include <rune/codec/Filter.h>` and `<expected>` to the generated file header; throw for a field named `Filter`), `codec/include/rune/codec/Generator.h` (document it)
- Create: `codec/include/rune/codec/FilterBuilder.h`, `codec/src/FilterBuilder.cpp`, `codec/tests/FilterGeneratedTest.cpp`, `codec/tests/FilterBuilderTest.cpp`
- Modify: `codec/include/rune/codec/Filter.h` (the `FilterWriter::term(tag, type, width, std::span<const FilterKey>)` overload of Step 4)
- Modify: `codec/CMakeLists.txt` (source + tests; add a build-time generated `player_event.gen.h`, from a descriptor written by `codec/tools/make_player_event_blob.cpp` (new) in the same way `characters.gen.h` is made, for `FilterGeneratedTest`), `broker/CMakeLists.txt` and `client-cpp/CMakeLists.txt` (add `FilterBuilder.cpp` to their `rune_codec` lists), `codec/tests/GeneratorTest.cpp`
- Create: `client-cpp/examples/filter/PlayerEventSchema.h` (one `inline rune::codec::Descriptor playerEventDescriptor()`, the v1 descriptor of Task 3, used by the client tests, the broker slice and the probe)
- Modify: `client-cpp/tests/support/GenClaimSchemas.cpp` (also write `player_event.gen.h` from `playerEventDescriptor()`); `client-cpp/CMakeLists.txt` (append `${RUNE_CLAIM_GEN_DIR}/gen/player_event.gen.h` to `RUNE_CLAIM_GEN_HEADERS`)

**Interfaces:**
- Consumes: Task 2 (`Match`, `FilterWriter`, `FilterBytes`), Task 3 (`FilterSchema`, `parseFilter`).
- Produces, emitted inside every generated record struct:
  ```cpp
  /// Phase 2c filter-by-example (spec §4).  Members follow the schema's field order (designated initializers
  /// must too): playerId (tag 0, uint64), worldId (tag 1, uint32), channel (tag 2, int32), zone (tag 3, string(8)),
  /// kind (tag 4, enum), alive (tag 5, bool), hp (tag 6, int64).  Not filterable: name (string), pos (float64).
  struct Filter {
      ::rune::Match<uint64_t> playerId{};
      ::rune::Match<uint32_t> worldId{};
      ::rune::Match<int32_t> channel{};
      ::rune::Match<std::array<char, 8>> zone{};
      ::rune::Match<int32_t> kind{};
      ::rune::Match<bool> alive{};
      ::rune::Match<int64_t> hp{};
      [[nodiscard]] constexpr std::expected<std::size_t, ::rune::codec::FilterError> encodeTo(std::span<uint8_t> out) const noexcept {
          ::rune::codec::FilterWriter w(out);
          w.term(0, ::rune::codec::PrimType::UInt64, 8, playerId);   // ascending tag order (sorted at generation)
          w.term(1, ::rune::codec::PrimType::UInt32, 4, worldId);
          w.term(2, ::rune::codec::PrimType::Int32, 4, channel);
          w.term(3, ::rune::codec::PrimType::FixedStr, 8, zone);
          w.term(4, ::rune::codec::PrimType::Enum, 4, kind);
          w.term(5, ::rune::codec::PrimType::Bool, 1, alive);
          w.term(6, ::rune::codec::PrimType::Int64, 8, hp);
          return w.finish();
      }
      /// Throws std::invalid_argument(describe(error)) — explicit, before anything is sent.
      [[nodiscard]] ::rune::codec::FilterBytes encode() const {
          ::rune::codec::FilterBytes b;
          auto r = encodeTo(std::span<uint8_t>(b.bytes.data(), b.bytes.size()));
          if (!r) throw std::invalid_argument(::rune::codec::describe(r.error()));
          b.len = static_cast<uint16_t>(*r);
          return b;
      }
  };
  ```
  Retired fields (`untilVersion != 0xFFFF`) and ineligible fields get no member.
- Produces (`FilterBuilder.h`):
  ```cpp
  namespace rune::codec {
  using FilterLiteral = std::variant<int64_t, uint64_t, bool, std::string_view>;
  class FilterBuilder {
  public:
      static FilterBuilder on(const Descriptor& d);           // the topic's current descriptor (names -> tags)
      FilterBuilder& field(std::string_view name);
      FilterBuilder& eq(FilterLiteral v);
      FilterBuilder& in(std::span<const FilterLiteral> vs);
      FilterBuilder& in(std::initializer_list<FilterLiteral> vs);
      [[nodiscard]] std::expected<FilterBytes, FilterError> build() const;   // same bytes as E::Filter
  private:
      const Descriptor* d_ = nullptr;
      struct Pending { uint16_t tag; PrimType type; uint8_t width; FilterOp op; uint8_t count; std::array<FilterKey, kMaxFilterInValues> v; };
      std::array<Pending, kMaxFilterTerms + 1> terms_{};   // one spare slot: the 9th term is reported, not dropped
      uint8_t n_ = 0;
      std::optional<FilterError> err_;
  };
  }
  ```
  - `field()` errors: an unknown name → `UnknownField` (`termIndex` = position); `String` / `Bytes` → `VariableLength`; `Float64` → `FloatField`.
  - Literal checks: an integer literal out of range → `ValueOutOfRange`; `bool` against a non-bool field, a string against an integer field, or an integer against a FixedStr → `TypeMismatch`; a string longer than N → `ValueOutOfRange`.
  - `build()` sorts the terms by tag and the values by `FilterKey`, de-duplicates, and writes the same bytes as `FilterWriter`.

- [ ] **Step 1: Write the failing tests.**
  - `GeneratorTest.cpp`:
    - `Generator.EmitsAFilterMemberPerEligibleFieldInFieldOrder`: string checks for `struct Filter {`, the seven members in order, no `name` or `pos` member, and the order comment.
    - `GeneratorRegression.AFieldNamedFilterIsRejectedRegression`: `generateHeader` throws `std::invalid_argument`.
    - `Generator.RetiredFieldsGetNoFilterMember`.
  - `FilterGeneratedTest.cpp` (compiles against `player_event.gen.h`):

```cpp
#include "player_event.gen.h"
#include <rune/codec/FilterBuilder.h>
#include <rune/codec/FilterSchema.h>
#include <gtest/gtest.h>
using E = rune::gen::PlayerEvent;
using rune::anyOf;

TEST(FilterGenerated, DesignatedInitializersEncode) {
    const auto b = E::Filter{ .worldId = 42u, .channel = anyOf(1, 2, 3) }.encode();
    EXPECT_EQ(b.len, 2 + (5 + 4) + (5 + 3 * 4));
}
TEST(FilterGenerated, EmptyFilterIsNoFilter) {   // spec §4
    EXPECT_TRUE(E::Filter{}.encode().empty());
}
// A requires-expression with an invalid expression is ill-formed outside a template, so every probe is a
// variable template over the filter type.
template <class F> constexpr bool kHasName          = requires(F f) { f.name; };
template <class F> constexpr bool kHasPos           = requires(F f) { f.pos; };
template <class F> constexpr bool kWorldTakesDouble = requires { F{ .worldId = 1.5 }; };
template <class F> constexpr bool kWorldTakesString = requires { F{ .worldId = "7" }; };
template <class F> constexpr bool kZoneTakesLiteral = requires { F{ .zone = "eu" }; };
TEST(FilterGenerated, CompileTimeChecks) {     // spec §4: unknown / ineligible fields and wrong value types do not compile
    static_assert(!kHasName<E::Filter>, "a string field has no member");
    static_assert(!kHasPos<E::Filter>,  "a float field has no member");
    static_assert(!kWorldTakesDouble<E::Filter>, "a double is not a uint32 value");
    static_assert(!kWorldTakesString<E::Filter>, "a string is not a uint32 value");
    static_assert(kZoneTakesLiteral<E::Filter>);
    static_assert(std::is_trivially_copyable_v<E::Filter>);
    SUCCEED();
}
TEST(FilterGeneratedRegression, GeneratedAndBuilderBytesAreIdenticalRegression) {   // spec §4 "same encoding"
    const auto a = E::Filter{ .worldId = 42u, .channel = anyOf(3, 1, 2) }.encode();
    const auto b = rune::codec::FilterBuilder::on(E::descriptor()).field("channel").in({int64_t{1}, int64_t{2}, int64_t{3}})
                       .field("worldId").eq(int64_t{42}).build();
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(a, *b);
}
TEST(FilterGeneratedRegression, GeneratedFilterParsesAgainstItsOwnSchemaRegression) {
    const auto fs = rune::codec::FilterSchema::build(E::descriptor(), E::kVersion);
    const auto b  = E::Filter{ .playerId = 9u, .zone = "eu", .alive = true }.encode();
    EXPECT_TRUE(rune::codec::parseFilter(b.span(), fs).has_value());
}
TEST(FilterGeneratedRegression, NineSetMembersFailAtEncodeRegression) {   // spec §4: >8 terms fails at encode
    // player_event has 7 eligible fields; use a 9-field all-integer schema generated in the same CMake step
    // (nine_ints.gen.h: f0..f8 UInt32) to prove the limit.
    EXPECT_THROW((void)rune::gen::NineInts::Filter{ .f0 = 1u, .f1 = 1u, .f2 = 1u, .f3 = 1u, .f4 = 1u,
                                                    .f5 = 1u, .f6 = 1u, .f7 = 1u, .f8 = 1u }.encode(),
                 std::invalid_argument);
}
```
  (`make_player_event_blob.cpp` also writes `nine_ints.blob`; the CMake step generates both headers.)
  - `FilterBuilderTest.cpp`:
    - `FilterBuilder.UnknownNameIsAnError`
    - `FilterBuilder.FloatAndStringFieldsAreErrors`
    - `FilterBuilderRegression.OutOfRangeLiteralIsAnErrorNotTruncatedRegression` (`uint32` field, literal `int64_t{-1}`)
    - `FilterBuilder.FixedStrLiteralLongerThanTheFieldIsAnError`
    - `FilterBuilder.NineTermsIsAnError`
  (Designator order: GCC rejects out-of-order designators, while Clang accepts them with `-Wreorder-init-list`, which is not SFINAE-visible. The ordering rule is therefore documented in the generated comment and not asserted here. Add `-Werror=reorder-init-list` to `rune_codec_tests` so the codec's own tests never rely on reordering.)
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/sub-codec --target rune_codec_tests`. Expected: `no member named 'Filter' in 'rune::gen::PlayerEvent'`.
- [ ] **Step 3: Implement the emission** in `generateHeader`, after the members and before `key()`:
  1. Collect the eligible live fields in declaration order: `isFilterable(f) && f.untilVersion == 0xFFFF`.
  2. Emit the comment listing them in order, as `name (tag N, type)`. Emit one member per field, typed `::rune::Match<cppType>`, or `::rune::Match<std::array<char, N>>` for FixedStr.
  3. Emit `encodeTo` with the `term(...)` calls sorted by tag ascending (copy the list, `std::sort` by `id`), then `encode()`.
  4. If `d.find("Filter") != nullptr`, throw `std::invalid_argument("generateHeader: schema '<name>' has a field named 'Filter', which collides with the generated filter type — rename the field")`.
- [ ] **Step 4: Implement `FilterBuilder.cpp`.** Literal conversion goes through `std::in_range<T>` for integers. A FixedStr string is zero-padded to N. A `bool` field accepts only `bool`. `build()` writes through a local `FilterWriter`-equivalent loop over sorted `Pending` terms, using the same byte layout (reuse `FilterWriter` by giving it `term(tag, type, width, std::span<const FilterKey>)` as an overload that takes pre-normalized keys; `Match<T>` encoding calls it internally).
- [ ] **Step 5: Add the shared descriptor.** `client-cpp/examples/filter/PlayerEventSchema.h`:

```cpp
#pragma once
// The player_event schema shared by the client tests, the broker slice (FilterBrokerRegressionTest) and
// filter_probe (docker/filter-e2e.sh) — ONE descriptor, so the generated header's kFingerprint matches the
// topic the probe defines.
#include <rune/codec/Descriptor.h>
namespace rune::examples {
inline rune::codec::Descriptor playerEventDescriptor() {
    using rune::codec::DescriptorBuilder; using rune::codec::PrimType;
    return DescriptorBuilder("player_event")
        .addTagged("playerId", 0, PrimType::UInt64, 1, 0xFFFF, /*isKey=*/true, {})
        .addTagged("worldId", 1, PrimType::UInt32, 1, 0xFFFF, false, {})
        .addTagged("channel", 2, PrimType::Int32, 1, 0xFFFF, false, {})
        .addFixedStrTagged("zone", 3, 8, 1, 0xFFFF, false)
        .addTagged("kind", 4, PrimType::Enum, 1, 0xFFFF, false, {1, 2, 3})
        .addTagged("alive", 5, PrimType::Bool, 1, 0xFFFF, false, {})
        .addTagged("hp", 6, PrimType::Int64, 1, 0xFFFF, false, {})
        .addTagged("name", 7, PrimType::String, 1, 0xFFFF, false, {})
        .addTagged("pos", 8, PrimType::Float64, 1, 0xFFFF, false, {})
        .build();
}
}  // namespace rune::examples
```
  `GenClaimSchemas.cpp` includes `"../../examples/filter/PlayerEventSchema.h"` and writes `dir + "/player_event.gen.h"`. `codec/tools/make_player_event_blob.cpp` duplicates the same builder calls (the codec cannot include client files). `FilterGeneratedRegression.GeneratedFilterParsesAgainstItsOwnSchemaRegression` uses the codec copy. `ClientFilterSchemaRegression.SharedDescriptorMatchesTheCodecToolRegression` (in Task 11) asserts that the two fingerprints are equal.
- [ ] **Step 6: Run.** `cmake --build build/sub-codec --target rune_codec_tests && ctest --test-dir build/sub-codec -R 'Filter|Generator|GeneratedRoundTrip|GeneratedKey|Flyweight' --output-on-failure`. Also build `cmake --build build/sub-client-cpp --target generate_claim_schema_headers rune_schema_codec_smoke_tests` (so every existing generated header still compiles with the new nested struct) and `ctest --test-dir build/sub-client-cpp -R 'SchemaCodecSmoke|TypedClaim|ClaimClient' --output-on-failure`. Expected: PASS.
- [ ] **Step 7: Commit.**

```bash
git add codec/src/Generator.cpp codec/include/rune/codec/Generator.h codec/include/rune/codec/Filter.h codec/include/rune/codec/FilterBuilder.h codec/src/FilterBuilder.cpp \
        codec/tests/FilterGeneratedTest.cpp codec/tests/FilterBuilderTest.cpp codec/tests/GeneratorTest.cpp codec/tools/make_player_event_blob.cpp \
        codec/CMakeLists.txt broker/CMakeLists.txt client-cpp/CMakeLists.txt client-cpp/examples/filter/PlayerEventSchema.h client-cpp/tests/support/GenClaimSchemas.cpp
git commit -m "feat(codec): generated E::Filter (filter-by-example, compile-time checked) and the runtime FilterBuilder; shared player_event schema" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Wire — client v10 / broker v6, `Subscribe` + `JoinGroup` filter trailers, response details, SBE `FILTER_SCHEMA` / `FILTER`

**Files:**
- Modify: `protocol/include/rune/protocol/client/Protocol.h` (versions, the version-10 comment block, `clientUnderstandsFilters`)
- Modify: `protocol/include/rune/protocol/client/DataMessages.h` (`Subscribe::filter`, `SubscribeResponse::detail` + `encodedSizeWithDetail`)
- Modify: `protocol/include/rune/protocol/client/GroupMessages.h` (`JoinGroup::filter`, `JoinGroupResponse::detail`, `kGroupFilterMismatchPrefix`)
- Modify: `protocol/schema/rune-internal-ring.xml` (`<validValue name="FILTER_SCHEMA">23</validValue>` in `OpType`, `<validValue name="FILTER">10</validValue>` in `TailKind`, with comments). Regenerate with `"$JAVA" -Dsbe.target.language=cpp -Dsbe.output.dir=include -jar "$SBE_JAR" schema/rune-internal-ring.xml` from `protocol/`. Commit only the changed headers (`OpType.h`, `TailKind.h`, plus any the generator rewrites identically).
- Modify: `protocol/tests/protocol/client/DataMessagesTest.cpp`, `protocol/tests/protocol/client/GroupMessagesTest.cpp`, `docs/api/wire-protocol.md` (frame catalogue rows 18, 19, 32, 33 and a "Filters (client version 10)" section)

**Interfaces:**
- Produces:
  ```cpp
  // Protocol.h
  inline constexpr int32_t kClientVersionFilter = 10;   // subscription filters (Phase 2c)
  inline constexpr int32_t kClientVersion       = 10;
  inline constexpr int32_t kBrokerVersionFilter = 6;
  inline constexpr int32_t kBrokerVersion       = 6;
  [[nodiscard]] constexpr bool clientUnderstandsFilters(int32_t clientVersion) noexcept { return clientVersion >= kClientVersionFilter; }
  // DataMessages.h — Subscribe: [..key][u16 filterLen][filter bytes] — the field is OMITTED when `filter` is empty
  struct Subscribe { /* … */ std::span<const uint8_t> filter; };   // decode: present iff bytes remain after key
  struct SubscribeResponse { /* … */ std::string_view detail; };  // v10 trailing [u16 len][utf-8]; omitted when empty
  // GroupMessages.h
  struct JoinGroup { /* … */ std::span<const uint8_t> filter; };   // after the topic ids; omitted when empty
  struct JoinGroupResponse { /* … */ std::string_view detail; };  // after memberId; omitted when empty
  inline constexpr std::string_view kGroupFilterMismatchPrefix = "group filter mismatch";
  ```
  - Decoding: `Subscribe::decode` reads the trailer only when `avail >= pos + 2`, and the declared length must fit, else `nullopt`. That is safe for older frames, which end exactly at the key.
  - `encodedSize()` adds `2 + filter.size()` only when the filter is non-empty.
  - The response `encode` writes the detail only when it is non-empty. The broker sets it only for v10 sessions.

- [ ] **Step 1: Write the failing tests.**
  - `DataMessagesTest.cpp`:
    - `SubscribeFilter.UnfilteredFrameIsByteIdenticalToV9Regression`: the frame built with an empty filter equals the bytes `encode()` produced before this task (hard-code the 44-byte frame for a fixed message), and `encodedSize()` is 44.
    - `SubscribeFilter.FilterRoundTrips`: an 11-byte filter → `encodedSize() == 44 + 2 + 11`; decode gives the same span.
    - `SubscribeFilter.ZeroLengthTrailerIsNoFilter`: a hand-appended `[0,0]` decodes to an empty filter.
    - `SubscribeFilterRegression.TruncatedTrailerIsUndecodableRegression`: a declared length of 11 with 5 bytes present → `nullopt`, so the gateway answers `InvalidRequest`.
    - `SubscribeFilter.LineageClaimInKeyAndFilterCoexist`: an 8-byte key plus a filter.
    - `SubscribeResponseDetail.DetailRoundTripsAndOldDecoderIgnoresIt`: the 38-byte prefix decodes alone.
  - `GroupMessagesTest.cpp`:
    - `JoinGroupFilter.UnfilteredJoinIsByteIdenticalRegression`
    - `JoinGroupFilter.FilterRoundTrips`
    - `JoinGroupResponseDetail.DetailRoundTrips`
  - Protocol version pins (`DataMessagesTest.cpp`): `ProtocolVersions.FilterIsTenAndBrokerSix`: `static_assert(kClientVersionFilter == kClientVersionAuth + 1)` and `static_assert(kBrokerVersionFilter == kBrokerVersionAuth + 1)`.
  - SBE pin (`broker/libs/node/tests/node/WorkerOwnedIndexRegressionTest.cpp` style, new test in `rune_node_tests`, file `broker/libs/node/tests/node/WorkerOpWireRegressionTest.cpp`): `WorkerOpWireRegression.FilterOpAndTailValuesAreAppendedRegression` asserts `static_cast<int>(sbe::OpType::Value::FILTER_SCHEMA) == 23` and `static_cast<int>(sbe::TailKind::Value::FILTER) == 10`.
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/root -j14 --target rune_protocol_tests`. Expected: `no member named 'filter' in 'Subscribe'`.
- [ ] **Step 3: Implement.** In `Subscribe::encode`, after the key:

```cpp
        if (!filter.empty()) {
            pos += putVarBytes(buf + pos, filter.data(), static_cast<uint16_t>(filter.size()));   // client version 10
        }
        return pos;
```
  In `decode`, after the key:

```cpp
        if (avail >= pos + 2u) {   // client version 10: optional trailing filter
            const uint8_t* fData = nullptr;
            uint16_t       fLen  = 0;
            if (!getVarBytes(buf, avail, pos, fData, fLen)) return std::nullopt;
            msg.filter = std::span<const uint8_t>(fData, fLen);
        }
```
  Mirror this for `SubscribeResponse::detail` (after `subscriptionId`; `ENCODED_SIZE` stays 38; add `std::size_t encodedSizeWithDetail() const noexcept`, which returns `ENCODED_SIZE + (detail.empty() ? 0 : 2 + detail.size())`). Mirror it for `JoinGroup::filter` (after the topic-id array) and `JoinGroupResponse::detail` (after `memberId`). In `Protocol.h`, add the version-10 paragraph to the "Client versions" comment:

  > 10 — Phase 2c subscription filters: `Subscribe` and `JoinGroup` may carry a trailing filter (`[u16 len][bytes]`, canonical format in `rune/codec/FilterWire.h`; omitted = no filter). A broker ≥ 6 serves it and answers a refused filter `SubscribeResponse` / `JoinGroupResponse` `{InvalidRequest, detail}`, the detail in a trailing `[u16 len][utf-8]` field sent only to sessions ≥ 10. A broker below 6 would ignore the trailer, so a client sends a filter only to a session whose `ConnectResponse.brokerVersion >= kBrokerVersionFilter`.

  Add the broker-version 6 line to the broker-version comment.
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_protocol_tests rune_node_tests && ctest --test-dir build/root -R 'SubscribeFilter|SubscribeResponseDetail|JoinGroupFilter|JoinGroupResponseDetail|ProtocolVersions|DataMessages|GroupMessages|Conformance|WorkerOpWireRegression' --output-on-failure`. Then `cmake --build build/sub-client-cpp --target rune_client_tests && ctest --test-dir build/sub-client-cpp -R 'Conformance' --output-on-failure` (the client's conformance vectors still decode). Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add protocol/include/rune/protocol/client/Protocol.h protocol/include/rune/protocol/client/DataMessages.h \
        protocol/include/rune/protocol/client/GroupMessages.h protocol/schema/rune-internal-ring.xml \
        protocol/include/rune_protocol_internal_sbe/ protocol/tests/protocol/client/DataMessagesTest.cpp \
        protocol/tests/protocol/client/GroupMessagesTest.cpp broker/libs/node/tests/node/WorkerOpWireRegressionTest.cpp \
        broker/libs/node/CMakeLists.txt docs/api/wire-protocol.md
git commit -m "feat(protocol): client v10 / broker v6 - optional filter trailers on Subscribe and JoinGroup, refusal details, ring OpType 23 / TailKind 10" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Checkpoint A (controller).**
  - **Run:**
    - `broker/scripts/run-regression.sh build/root 10` (all three stages);
    - `ctest --test-dir build/sub-client-cpp -R Regression -j14 --output-on-failure`;
    - `cmake --build build/sub-codec --target rune_codec_tests rune_catalog_tests && ctest --test-dir build/sub-codec -R 'Regression|CatalogGolden|Filter' --output-on-failure` (CatalogGolden: 3 pass, unchanged);
    - `cmake --build build/asan -j14 && ctest --test-dir build/asan -R Regression -LE latency -j8 --output-on-failure`;
    - the same for `build/tsan`;
    - `cmake --build build/client-tsan -j14 && ctest --test-dir build/client-tsan -R Regression -j8 --output-on-failure`.

---

### Task 6: `FilterIndex`: value → members, open addressing, at the configured cap

**Files:**
- Create: `broker/libs/node/include/rune/node/filter/FilterIndex.h`, `broker/libs/node/tests/node/FilterIndexTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt`. Add a new no-Aeron target `rune_filter_engine_tests` (`tests/node/FilterIndexTest.cpp`; Task 7 adds `FilterGroupTest.cpp`) linking `rune_node rune_codec GTest::gtest_main`, with `gtest_discover_tests(rune_filter_engine_tests PROPERTIES LABELS small)` and `rune_regression_batches(rune_filter_engine_tests 2)`. Place it next to `rune_worker_tests`.

**Interfaces:**
- Consumes: `rune::codec::FilterKey` (Task 2).
- Produces:
  ```cpp
  namespace rune::node::filter {
  inline constexpr uint32_t kNil = UINT32_MAX;
  class FilterIndex {
  public:
      /// Cold: slots = next power of two >= 2 * maxValues; entries reserved to `reserveEntries`.
      void init(uint32_t maxValues, uint32_t reserveEntries);
      /// Cold (Subscribe).  false = `maxValues` distinct values are already registered and k is new (nothing changed).
      [[nodiscard]] bool add(rune::codec::FilterKey k, uint32_t member);
      /// Cold (Unsubscribe).  Removes one (k, member) registration; the last one frees the value (backward-shift delete).
      void remove(rune::codec::FilterKey k, uint32_t member) noexcept;
      /// Hot: fn(uint32_t member) for every member registered under k.  No allocation; one probe sequence.
      template <class Fn> void forEach(rune::codec::FilterKey k, Fn&& fn) const noexcept;
      [[nodiscard]] uint32_t distinctValues() const noexcept { return size_; }
      [[nodiscard]] bool     initialized() const noexcept { return !slots_.empty(); }
  private:
      struct Slot  { rune::codec::FilterKey key{}; uint32_t head = kNil; uint32_t refs = 0; };   // refs 0 = empty slot
      struct Entry { uint32_t member = kNil; uint32_t next = kNil; };
      [[nodiscard]] uint32_t hash_(rune::codec::FilterKey k) const noexcept {
          const uint64_t h = (k.lo * 0x9E3779B97F4A7C15ull) ^ (k.hi * 0xC2B2AE3D27D4EB4Full);
          return static_cast<uint32_t>(h >> 32) & mask_;
      }
      std::vector<Slot>  slots_;
      std::vector<Entry> entries_;   // reserved at init; a free list reuses removed entries
      uint32_t freeEntry_ = kNil, mask_ = 0, size_ = 0, maxValues_ = 0;
  };
  }
  ```

- [ ] **Step 1: Write the failing tests** (`FilterIndexTest.cpp`):
  - `FilterIndex.AddForEachRemove` (two members under one key, one under another; `forEach` order is irrelevant: collect into a sorted vector).
  - `FilterIndex.MissVisitsNothing`.
  - `FilterIndex.FixedStrSizedKeysCollideOnlyWhenEqual`: keys differing only in `hi`.
  - `FilterIndexRegression.CapRefusesTheNextDistinctValueNotAnExistingOneRegression`: with `init(4, 16)`, add 4 distinct values; a 5th distinct value → false and `distinctValues() == 4`; adding a second member to an existing value → true.
  - `FilterIndexRegression.BackwardShiftDeleteKeepsEveryOtherKeyReachableRegression`: 1,000 random keys at `init(1024, 1024)`, remove a random half, and every remaining key still finds its member.
  - `FilterIndex.ModelCheckedAgainstAMultimap`: `TEST_P` over seeds 1..16. 5,000 random add/remove/forEach steps against a `std::multimap<FilterKey, uint32_t>` model, all compared exactly.
  - `FilterIndex.LookupAllocatesNothing`: a counting `operator new` local to this test binary; 100,000 `forEach` calls → 0 allocations. The binary is no-Aeron and owns its `operator new`, so place the counter in `FilterIndexTest.cpp` under `#ifdef RUNE_FILTER_ENGINE_ALLOC_COUNT` and define the macro for this target only.
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/root -j14 --target rune_filter_engine_tests`. Expected: missing header.
- [ ] **Step 3: Implement** with linear probing from `hash_`:
  - `add`: probe until a slot with `refs == 0`, or a slot whose key equals `k`. A new value needs `size_ < maxValues_`; then it takes the slot with `refs = 1`. Otherwise increment `refs`. Then push the entry: pop `freeEntry_`, or `entries_.push_back` (this grows past the reserve only on the cold path), and link it at the head.
  - `remove`: find the slot and unlink the entry onto the free list. If `--refs == 0`, run a backward-shift delete: walk the following slots, and move back any slot whose home lies at or before the hole.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'FilterIndex' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.** Run `git add broker/libs/node/include/rune/node/filter/FilterIndex.h broker/libs/node/tests/node/FilterIndexTest.cpp broker/libs/node/CMakeLists.txt`, then `git commit -m "feat(node): FilterIndex - value to members, open addressing, capped at the configured distinct values" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 7: `FilterGroup`: the member state machine, the shared scan, rebuild delivery and position notes (pure engine)

The engine knows nothing about Aeron or the Worker. It reads through a `FilterSource` and writes through a `FilterSink` (templates, concepts, no virtual calls, no `std::function`), so the unit tests and the stress (Task 15) drive it with fakes, and the Worker (Task 8) instantiates it with its real read and egress.

**Files:**
- Create: `broker/libs/node/include/rune/node/filter/FilterGroup.h`, `broker/libs/node/tests/node/FilterGroupTest.cpp`, `broker/libs/node/tests/support/FilterFakes.h` (`FakeSource`, `FakeSink`, `makePlayerBatch`; shared with Tasks 14 and 15)
- Modify: `broker/libs/node/CMakeLists.txt` (add `tests/node/FilterGroupTest.cpp` to `rune_filter_engine_tests`; include `${CMAKE_CURRENT_SOURCE_DIR}/tests` and `${RUNE_CLIENT_ROOT}/examples`, for `filter/PlayerEventSchema.h`)

**Interfaces:**
- Consumes: Task 1 (`forEachRecordSpan`, `rebuildBatch`, `RecordSpan`), Task 3 (`CompiledFilter`, `FilterSchema`, `FilterVerdict`, `recordVersion`), Task 6 (`FilterIndex`, `kNil`).
- Produces:
  ```cpp
  namespace rune::node::filter {
  enum class SendOutcome : uint8_t { Sent, Retry, TooLarge };
  struct FilterConfig {
      uint32_t maxSubsPerPartition        = 4096;              // RUNE_FILTER_MAX_SUBS_PER_PARTITION
      uint32_t maxIndexValuesPerPartition = 4096;              // RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION
      int64_t  noteIntervalNs             = 50'000'000;        // RUNE_FILTER_NOTE_INTERVAL_MS (50)
      uint64_t noteRecords                = 65'536;            // RUNE_FILTER_NOTE_RECORDS
      int64_t  versionWaitNs              = 5'000'000'000;     // RUNE_FILTER_VERSION_WAIT_MS (5000)
      uint32_t matchScratch               = 65'536;            // RUNE_FILTER_MATCH_SCRATCH
  };
  inline constexpr std::size_t kMaxIndexFields = 8;
  struct FilterCounters {
      uint64_t zeroCopy = 0, rebuilt = 0, notes = 0, absentExcluded = 0, unknownVersionExcluded = 0,
               nullValueExcluded = 0, malformed = 0, overflowBatches = 0, overCap = 0, fellBehind = 0, rejoined = 0;
  };
  struct Behind {};
  struct InSync {};
  struct VersionStall { uint16_t version = 0; int64_t sinceNs = 0; };
  using MemberState = std::variant<Behind, InSync, VersionStall>;
  struct ScanIdle {};
  struct Scanning  { int64_t cursor = 0; };
  struct ScanStall { int64_t cursor = 0; uint16_t version = 0; int64_t sinceNs = 0; };
  using ScanState = std::variant<ScanIdle, Scanning, ScanStall>;

  template <class S> concept FilterSource = requires(S& s, int64_t from, std::size_t cap, int64_t pid, int64_t base) {
      { s.bound() } -> std::same_as<int64_t>;                        // HW (RU) / LSO (RC); < 0 = not servable now
      { s.read(from, cap) } -> std::same_as<std::span<const uint8_t>>;   // whole batches starting at the batch holding `from`
      { s.readCommitted() } -> std::same_as<bool>;
      { s.isAborted(pid, base) } -> std::same_as<bool>;
      { s.skipEvicted(from) } -> std::same_as<int64_t>;               // transient: the ring start when `from` was evicted
  };
  template <class K> concept FilterSink = requires(K& k, uint32_t sub, int64_t base, std::span<const uint8_t> b, int64_t c) {
      { k.sendBatch(sub, base, b) } -> std::same_as<SendOutcome>;
      { k.sendNote(sub, c) } -> std::same_as<SendOutcome>;
      { k.cursor(sub) } -> std::same_as<int64_t&>;                    // the subscription's own cursor (WorkerSubscription::nextOffset)
      { k.deliverable(sub) } -> std::same_as<bool>;                   // active, push state Live, egress present
  };
  struct FilterScratch {                                            // one per worker; sized cold by reserve(cfg)
      struct MatchRef { uint32_t member; uint32_t record; };
      std::vector<rune::protocol::RecordSpan> records;
      std::vector<MatchRef> matches;
      std::vector<uint32_t> counts, starts, order, touched, keep;
      std::vector<uint8_t>  rebuild;                                // grow-once, never shrinks
      void reserve(const FilterConfig& cfg, std::size_t maxRecordsPerRead);
  };
  class FilterGroup {
  public:
      void init(int32_t topicId, int32_t partition, uint8_t isolation, const FilterConfig& cfg);   // cold
      /// Cold.  The member id, or kNil when the member cap is reached (counted overCap, never allocates past it).
      [[nodiscard]] uint32_t add(uint32_t sub, rune::codec::CompiledFilter f, const rune::codec::FilterSchema& schema);
      void remove(uint32_t member) noexcept;
      void moveSub(uint32_t member, uint32_t newSub) noexcept;
      [[nodiscard]] bool empty() const noexcept;
      [[nodiscard]] uint32_t anySub() const noexcept;                // a live member's sub (the Worker builds the source from it)
      /// One push pass.  `budget` is the worker's remaining push-pass byte budget (read + sent bytes are charged).
      template <FilterSource Src, FilterSink Snk>
      int pass(Src& src, Snk& sink, const rune::codec::FilterSchema& schema, FilterScratch& scratch,
               int64_t nowNs, std::size_t& budget);
      /// An InSync member's cursor is the scan cursor (its own cursor field is written back on every transition out).
      [[nodiscard]] int64_t cursorOf(uint32_t member, int64_t ownCursor) const noexcept;
      [[nodiscard]] const ScanState& scanState() const noexcept;
      [[nodiscard]] const MemberState& stateOf(uint32_t member) const noexcept;
      [[nodiscard]] FilterCounters countersOf(uint32_t member) const noexcept;   // folds the group-level absent counts
      [[nodiscard]] const FilterCounters& groupCounters() const noexcept;
  private:
      struct Member {
          uint32_t sub = kNil;
          rune::codec::CompiledFilter filter;
          MemberState state = Behind{};
          uint8_t  indexField = 0;           // fields_[indexField] holds its registrations
          int64_t  coveredAt = 0;            // InSync: scanned_ when the client's position last covered the scan cursor
          uint64_t pendingSkipped = 0;       // Behind: records evaluated and not delivered since the last frame / note
          uint64_t absentAtJoin = 0;         // fields_[indexField].absent when it joined (folded on leave)
          int64_t  lastFrameNs = 0;
          uint16_t loggedVersion = 0;        // unknown-version first log (FD6)
          uint16_t loggedAbsentVersion = 0;  // absent-field first log (spec §8)
          FilterCounters counters;
          bool     live = false;             // slot in use (the free list's own marker, not member state)
      };
      struct IndexField { int32_t schemaField = -1; FilterIndex index; uint32_t members = 0; uint64_t absent = 0; };
      // … join / leave / stepBehind_ / scan_ / deliver_ / notes_ (below)
  };
  }
  ```

- **The pass, exactly** (this is the contract the tests pin):

```cpp
template <FilterSource Src, FilterSink Snk>
int FilterGroup::pass(Src& src, Snk& sink, const rune::codec::FilterSchema& schema, FilterScratch& sc,
                      int64_t nowNs, std::size_t& budget) {
    const int64_t bound = src.bound();
    if (bound < 0) return 0;                                  // not servable this pass (RC not Ready, not owned)
    int work = 0;
    // 1. Catch-up: every live member that is Behind or VersionStall reads ITS cursor and evaluates its own
    //    predicate (O(records) per Behind member — only while it catches up).  A member whose cursor reaches the
    //    scan cursor (Scanning / ScanStall) joins InSync; with ScanIdle it joins once it has read up to `bound`
    //    and the scan starts there.
    for (uint32_t m = 0; m < members_.size() && budget > 0; ++m)
        if (members_[m].live && !std::holds_alternative<InSync>(members_[m].state))
            work += stepBehind_(m, src, sink, schema, sc, nowNs, bound, budget);
    // 2. The shared scan: one read, one index probe per record and index field, residual terms on candidates.
    if (inSync_ > 0 && budget > 0) work += scan_(src, sink, schema, sc, nowNs, bound, budget);
    // 3. Position notes: a sweep at most every noteIntervalNs / 5, or as soon as noteRecords were scanned.
    if (nowNs - lastSweepNs_ >= cfg_.noteIntervalNs / 5 || scanned_ - scannedAtSweep_ >= cfg_.noteRecords)
        work += notes_(sink, nowNs);
    return work;
}
```
  `scan_` per batch (the cursor `c` comes from `Scanning` / `ScanStall`):
  1. Skip control batches. Under RC, skip aborted transactional batches (`src.isAborted(pid, base)`). In both cases set `c = base + lastOffsetDelta + 1` and add the batch's records to `scanned_`.
  2. Collect `sc.records` with `forEachRecordSpan`. For each record `i`:
     - A null value → `groupCounters_.nullValueExcluded++`.
     - A value shorter than 4 bytes → `malformed++`.
     - An unknown version: within `versionWaitNs` of the stall start → `scan_ = ScanStall{batch base, v, since}` and stop the batch; nothing of it is delivered yet, so it is retried as a whole. After the wait → `unknownVersionExcluded++` and a first log per (member, version). That log iterates the InSync members once per version (cold).
     - Otherwise, for each index field `f`: if the slot is not present → `fields_[f].absent++`. If the slot runs past the value's end → `malformed++`. Else probe `fields_[f].index.forEach(loadFilterKey(...), cb)`, where `cb(member)` skips non-InSync members and evaluates `evaluateResidual`:
       - `Match` → append `{member, i}` to `sc.matches`. If `sc.matches` is full, set the batch's overflow flag.
       - `Absent` → that member's `absentExcluded++`.
       - `Malformed` → `malformed++`.
     - **Absent first log (spec §8):** the first `Absent` exclusion per (member, schema version), whether group-level (index field) or residual, logs once at `info`: `"filter: filtered field absent: sub <id> topic <t> partition <p> tag <tag> schema version <v> (records of this version never match)"`, tracked by `loggedAbsentVersion`. The group-level case iterates that field's InSync members once per version (cold). Task 17's `alter` leg greps this line.
  3. Overflow → `overflowBatches++`, and every InSync member is served this batch through `deliverOne_` with its own `evaluate` (per-member path).
  4. Otherwise: counting-sort `sc.matches` by member into `sc.order` (`counts`, `starts`), then deliver per touched member (`deliver_`):
     - `keep.size() == recordCount` → `sink.sendBatch(sub, base, original span)` and `zeroCopy++`.
     - Otherwise, grow `sc.rebuild` to the batch size if needed, then `rebuildBatch` and send it (`rebuilt++`).
     - `Sent` → `coveredAt = scanned_ + recordCount`, `lastFrameNs = now`.
     - `Retry` → leave InSync: `sink.cursor(sub) = base`, `pendingSkipped = scanned_ - coveredAt`, fold the absent counts, `state = Behind{}`, `fellBehind++`.
     - `TooLarge` → the same as `Retry` (the sink parked the subscription with F19's `ErrorOwed`).
     - A touched member that is not `deliverable` → leave InSync at `base` (the same as `Retry`).
  5. `scanned_ += recordCount`, `c = base + lastOffsetDelta + 1`, `scan_ = Scanning{c}`. Untouched members cost nothing here.
  6. If `inSync_` drops to 0 → `scan_ = ScanIdle{}`.

  The read stops at the batch that would exceed `budget` (except the first batch of a fresh pass, as `pushOwnedSubscriptions_`'s F5 rule does). Read bytes and sent bytes are charged to `budget`.

  `notes_` visits each live member. Its skip count is `InSync ? scanned_ - coveredAt : pendingSkipped`. A note is due when `skipped > 0 && (skipped >= noteRecords || now - lastFrameNs >= noteIntervalNs)`. It sends `sink.sendNote(sub, cursorOf(m, sink.cursor(sub)))`. On `Sent`, it resets the skip count (`coveredAt = scanned_` / `pendingSkipped = 0`), sets `lastFrameNs = now` and increments `notes`.

- [ ] **Step 1: Write the failing tests** (`FilterGroupTest.cpp`, using `FilterFakes.h`).
  - **`FakeSource`:** a `std::vector<std::vector<uint8_t>>` of batches with their base offsets. `read(from, cap)` concatenates the whole batches starting at the one holding `from`, up to `cap`. `bound()` is settable; `readCommitted()`; `isAborted()` comes from a set; `skipEvicted()` comes from a settable ring start, and it counts.
  - **`FakeSink`:** records every frame as `{sub, base, bytes, isNote, cursor}`. It refuses the next N sends for a chosen sub (`refuse(sub, n)`), and holds `cursors[sub]` and `deliverable[sub]`.
  - **`makePlayerBatch(base, version, std::vector<PlayerRow>)`:** encodes records with the Task 3 layout for v1, v2 or v3.

  Tests (the record-path ones run inside an allocation window after a warm-up pass):
  - `FilterGroup.NewMemberCatchesUpThenJoinsTheScan`: one member at offset 0 over 3 batches; it ends `InSync` and every match is delivered.
  - `FilterGroup.AllMatchingBatchIsTheOriginalZeroCopy`: frame bytes == source bytes, `zeroCopy == 1`.
  - `FilterGroup.PartlyMatchingBatchIsRebuiltWithOriginalOffsets`.
  - `FilterGroup.NonMatchingBatchSendsNothingThenANote`: `noteIntervalNs = 1 ms` in the config; after a pass at `now + 2 ms`, one note at the log end.
  - `FilterGroup.NoteByCountBeforeTheInterval`: `noteRecords = 100`; 150 non-matching records → a note at once.
  - `FilterGroup.FiveHundredMembersOneMatchEachTouchesOnlyTheMatched`: 500 members `worldId == i`; a batch with worldIds {3, 7}; exactly 2 data frames. Instrument with `FakeSink::calls` and assert `groupCounters().zeroCopy + rebuilt == 2`.
  - `FilterGroup.InIndexTermRegistersEveryValueOnce`: a member `channel ∈ {1, 2, 3}`; a record with channel 2 is delivered once.
  - `FilterGroup.TwoIndexFieldsInOneGroup`: Op `MixedIndexFields`.
  - `FilterGroupRegression.BackPressuredMemberFallsBehindWithoutStallingOthersRegression` (Review Focus #2): members A and B both match every batch. The sink refuses A 3 times. B receives every batch in the same passes as before. A ends with every batch exactly once, in order, and is `InSync` again (`rejoined == 1`).
  - `FilterGroupRegression.UnknownVersionStallsTheScanThenExcludesAfterTheWaitRegression`: a v2 record with the schema at v1. While `now < versionWaitNs`, nothing past it is delivered and `scanState()` is `ScanStall`. After the schema is extended, the next pass delivers it. A second instance never extends; after the wait, `unknownVersionExcluded == 1` and the scan moves on.
  - `FilterGroupRegression.AbsentIndexFieldIsCountedPerMemberRegression`: a v3 batch (`channel` dropped) and an `In` member on `channel` → `countersOf(m).absentExcluded == records`, no delivery.
  - `FilterGroupRegression.ReadCommittedSkipsAbortedAndStopsAtTheLsoRegression`.
  - `FilterGroupRegression.MatchListOverflowServesTheBatchPerMemberRegression`: `matchScratch = 4`, 3 members × 3 matches → `overflowBatches == 1`, same deliveries as without overflow.
  - `FilterGroupRegression.RemovingAMemberUnregistersEveryIndexValueRegression`.
  - `FilterGroupRegression.MemberCapReturnsNilNeverAllocatesRegression`: `maxSubsPerPartition = 2`; the 3rd `add` → `kNil`, `overCap == 1`.
  - `FilterGroupRegression.TombstonesAreExcludedAndTheScanAdvancesRegression`.
  - `FilterGroupRegression.StraddlingStartDeliversTheWholeBatchOnceRegression`: a member at offset 12 inside the batch 10..14 → the rebuilt or whole batch is delivered once, and the model client drops 10 and 11.
  - `FilterGroup.RecordPathAllocatesNothing`: 1,000 passes with 50 members and 5% matches → 0 allocations.
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/root -j14 --target rune_filter_engine_tests`. Expected: missing header.
- [ ] **Step 3: Implement `FilterGroup.h`** exactly as specified above. Put the template bodies in the header and the cold `init` / `add` / `remove` inline. `add`:
  1. Pick the slot from the free list, else the member cap → `kNil`.
  2. Find or create the `IndexField` for `filter.term(filter.indexTerm()).field` (more than `kMaxIndexFields` → `overCap++` and return `kNil`).
  3. `init` the index on its first use, with `(cfg.maxIndexValuesPerPartition, cfg.maxSubsPerPartition)`.
  4. Add every value of the index term. On a `false`, roll back the values already added and return `kNil` with `overCap++`.
  5. `state = Behind{}`, `live = true`.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'FilterGroup|FilterIndex' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.** Run `git add broker/libs/node/include/rune/node/filter/FilterGroup.h broker/libs/node/tests/node/FilterGroupTest.cpp broker/libs/node/tests/support/FilterFakes.h broker/libs/node/CMakeLists.txt`, then `git commit -m "feat(node): FilterGroup engine - Behind/InSync/VersionStall members, shared index scan, zero-copy or rebuilt delivery, position notes" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 8: Worker integration: `FilterSchema` op, the filter tail, member binding, the filtered push pass

**Files:**
- Modify: `broker/libs/node/include/rune/node/WorkerOp.h`:
  - `Type::FilterSchema = 23`, with a comment next to `SchemaUpdate`.
  - `std::span<const uint8_t> filterBytes;` with a comment: Subscribe-only, a view valid for the synchronous `tryEnqueueOp`, `TailKind::FILTER`.
  - `FilterSchema` reuses `schemaUpdateBlob` (the evolving blob, `TailKind::DESCRIPTOR_BLOB`) and `clientVersion` (the current schema version).
- Modify: `broker/libs/node/include/rune/node/Worker.h`:
  - `enqueueOp`'s tail switch: `Subscribe` with a non-empty `filterBytes` → `FILTER`; `FilterSchema` → `DESCRIPTOR_BLOB`.
  - The op dispatch switch (the `case WorkerOp::Type::SchemaUpdate:` anchor near line 2513; the one near line 1146 is `enqueueOp`'s tail switch, above): add `FilterSchema` → `executeFilterSchemaWire_`.
  - `WorkerSubscription` gains the filter binding.
  - `executeSubscribeWire_` binds.
  - `executeUnsubscribeWire_` and `executeEgressReleaseWire_` go through one new `eraseSub_(std::size_t i)`.
  - `pushOwnedSubscriptions_` skips filtered subscriptions after its F19 owed-error block.
  - New `pushFilteredGroups_(nowNs)` is called right after `pushOwnedSubscriptions_` at both call sites (`work += pushOwnedSubscriptions_(nowNs);` near lines 1756 and 1875).
  - New: `FilterSinkImpl`, `FilterSourceImpl`, `setFilterConfig`, `filterCountersForTest()`, `filterGroupForTest(topicId, partition, isolation)`.
- Create: `broker/libs/node/tests/node/WorkerFilterPushTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt` (new Aeron target `rune_worker_filter_tests` from `WorkerFilterPushTest.cpp`, modelled on `rune_worker_push_tests`; `ENVIRONMENT "${RUNE_AERON_TSAN_ENV}"`, `TIMEOUT 180`; include `${RUNE_CLIENT_ROOT}/examples`)

**Interfaces:**
- Consumes: Tasks 3, 5, 6 and 7.
- Produces (Worker, worker thread only):
  ```cpp
  struct SubUnfiltered {};
  struct SubFiltered { uint32_t group = filter::kNil; uint32_t member = filter::kNil; };
  // in WorkerSubscription:
  std::variant<SubUnfiltered, SubFiltered> filter = SubUnfiltered{};
  // Worker:
  void setFilterConfig(const filter::FilterConfig& cfg) noexcept;   // before the runner starts
  [[nodiscard]] filter::FilterCounters filterCountersForTest() const noexcept;   // sum over groups and members
  [[nodiscard]] const filter::FilterGroup* filterGroupForTest(int32_t topicId, int32_t partition, uint8_t isolation) const noexcept;
  ```
- Worker-owned state:
  - `std::unordered_map<int32_t, rune::codec::FilterSchema> filterSchemas_` (installed by `FilterSchema` ops; never erased, since topic ids are never reused and the cost is bounded by the number of typed topics);
  - `std::vector<filter::FilterGroup> filterGroups_` plus a free list;
  - `std::unordered_map<uint64_t, uint32_t> filterGroupByKey_` (key = topicId, partition, isolation; cold lookups at Subscribe);
  - `filter::FilterScratch filterScratch_` (reserved when the first group is created);
  - `filter::FilterConfig filterCfg_`.
- `executeFilterSchemaWire_`:
  1. `deserialize` the tail and `FilterSchema::build(desc, version)`.
  2. If the topic already has a schema, `extendFrom`; if that returns false, replace it and recompile every member of that topic (cold; `remove` then `add` again with the current cursor).
  3. Log `debug`.
  4. A malformed blob → `spdlog::error` with a counter, and the old schema is kept.
- `executeSubscribeWire_` with `tailKind == FILTER`:
  1. `CompiledFilter::compile(tail, filterSchemas_[topicId])`.
  2. A missing schema or a compile error is a defensive branch: the FIFO ordering of Task 9 makes it unreachable. It counts `filterBindFailures_`, sends `ErrorResponse{InvalidRequest, correlationId = subscribeCorrId, "filter schema unavailable on the owning worker"}` once on the subscription's egress (sealed like `sendPushTooLarge_`), and sets `pushState = Failed`.
  3. Otherwise, find or create the group for `(topicId, partition, isolation)` and `add(i, ...)`. A `kNil` result follows the same explicit path, with "filter capacity".
  4. A same-id re-Subscribe removes the old member and adds the new one.
- `eraseSub_(i)`:
  1. If `subs_[i].filter` is `SubFiltered`, `group.remove(member)`; if the group is now empty, return it to the free list and erase its key.
  2. Swap-and-pop.
  3. If the moved element is `SubFiltered`, `group.moveSub(member, i)`.
- `FilterSourceImpl` is built per group per pass from `subs_[group.anySub()]`:
  - `bound()` is `rep->highWatermark()` (or `logPtr->logEndOffset()` with no replica). Under RC it is `rcServe_(rep, s.rc)->lso(bound)`, and `-1` when the RC state is not Ready or the partition is not owned (`s.incarnation != 0 && !rep`).
  - `read()` is `readNoFault_` for disk, or `mem->readView` for transient.
  - `skipEvicted()` is `skipEvictedCursor_(*mem, from, false)` for transient; identity for disk.
  - `isAborted()` is `rc->isAborted`.
- `FilterSinkImpl`:
  - `sendBatch` builds the same `pc::Records` as `pushOwnedSubscriptions_`, on the same per-subscription egress publication and seal context (`findSessionCrypto_`), through `sendRecordsFrame`. `TooLarge` sets `pushState = ErrorOwed`, sets `failedFrameLen` and calls `sendPushTooLarge_`.
  - `sendNote` is a `Records{recordBatch = {}, baseOffset = cursor}` through the same function.
  - `cursor(sub)` is `subs_[sub].nextOffset`.
  - `deliverable(sub)` is `active && pushState == Live && egress pub != null`.
- `pushFilteredGroups_` shares `pushBudgetLeft_` with the unfiltered pass.

- [ ] **Step 1: Write the failing tests** (`WorkerFilterPushTest.cpp`, fixture copied from `WorkerPushFixture`: embedded driver, real `Log`, `WorkerEgress`, dynamic-MDC subscriber, `Collector` that keeps the whole decoded `pc::Records` incl. the batch bytes):
  - `WorkerFilterPush.FilteredSubscriptionReceivesOnlyMatches` (FilterSchema op, then a Subscribe op with the filter tail for `worldId == 7`; 20 batches of mixed worldIds; collected record offsets equal the model's).
  - `WorkerFilterPush.UnfilteredNeighbourIsByteIdentical` (an unfiltered and a filtered subscription on the same partition; every frame of the unfiltered one equals the log batch bytes, and no note reaches it).
  - `WorkerFilterPush.QuietFilterGetsPositionNotes` (via `setFilterConfig({.noteIntervalNs = 2'000'000})`; the last note's `baseOffset == log.logEndOffset()`).
  - `WorkerFilterPushRegression.EraseOfAFilteredSubFixesTheMovedMemberRegression` (3 subscriptions, the middle filtered one removed, the last one filtered and moved into its slot; the moved one keeps receiving exactly its matches).
  - `WorkerFilterPushRegression.EgressReleaseUnbindsFilteredSubscriptionsRegression`.
  - `WorkerFilterPushRegression.ResubscribeSameIdRebindsTheNewFilterRegression`.
  - `WorkerFilterRegression.RecordOfALateVersionWaitsForItsSchemaThenDeliversRegression` (Review Focus #1): the schema at v1; a v2 record (written with the v2 layout) is appended; doWork for 30 ms delivers nothing; the v2 FilterSchema op is enqueued; the next passes deliver it exactly once.
  - `WorkerFilterRegression.TombstonesAdvanceTheFilteredCursorRegression` (Review Focus #4): a compacted typed partition with null-value records interleaved; the filtered subscription gets every match, a note at the end, `nullValueExcluded == tombstones`.
  - `WorkerFilterPushRegression.FilteredSubscriptionUnderReadCommittedSeesOnlyCommittedMatchesRegression` (RC via a real replica, as the `TxnRcRegression` fixture in `WorkerPushTest.cpp`).
  - `WorkerFilterPushRegression.MissingSchemaAnswersInvalidRequestNeverBindsSilentlyRegression` (a Subscribe with a filter tail and no FilterSchema op: one `ErrorResponse{39}` with corr = subscribeCorrId is decoded from the egress; no Records).
  - `WorkerFilterPushRegression.EncryptedSessionSealsFilteredFramesAndNotesRegression` (`WorkerSessionCryptoRegressionTest`'s announce helper; every filtered frame and note is sealed under the session's b2c key; the decoder opens them).
  - `WorkerFilterPush.TransientPartitionFilters` (an `InMemoryLog` replica as in `TransientOwnedPushRegression`).
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/root -j14 --target rune_worker_filter_tests`. Expected: `no member named 'FilterSchema' in 'WorkerOp::Type'`.
- [ ] **Step 3: Implement.** The unfiltered skip, placed right after the `if (s.pushState != WorkerSubscription::PushState::Live) [[unlikely]] { … continue; }` block in `pushOwnedSubscriptions_`:

```cpp
            // Phase 2c: a FILTERED subscription is served by its FilterGroup (pushFilteredGroups_), never here.
            // One predicted branch per subscription per pass; unfiltered frames are unchanged byte for byte.
            if (!std::holds_alternative<SubUnfiltered>(s.filter)) [[unlikely]] continue;
```
  `pushFilteredGroups_`:

```cpp
    int pushFilteredGroups_(int64_t nowNs) {
        if (filterGroupByKey_.empty()) [[likely]] return 0;
        int work = 0;
        for (auto& g : filterGroups_) {
            if (g.empty() || pushBudgetLeft_ == 0) continue;
            WorkerSubscription& any = subs_[g.anySub()];
            const auto fs = filterSchemas_.find(any.topicId);
            if (fs == filterSchemas_.end()) continue;              // unreachable after Task 9's ordering (counted when bound)
            FilterSourceImpl src(*this, any);
            FilterSinkImpl   snk(*this);
            work += g.pass(src, snk, fs->second, filterScratch_, nowNs, pushBudgetLeft_);
        }
        return work;
    }
```
- [ ] **Step 4: Run.** `cmake --build build/root -j14 --target rune_worker_filter_tests rune_worker_push_tests rune_worker_tests rune_node_tests && ctest --test-dir build/root -R 'WorkerFilter|WorkerPush|WorkerLargeBatch|WorkerOversize|TxnRc|TransientOwnedPush|WorkerSessionCrypto|WorkerOpWire|NoAtomicsOrMutexesOutsideAllowlist' --output-on-failure`. Expected: PASS. The existing push suites are unchanged.
- [ ] **Step 5: Commit.** Run `git add broker/libs/node/include/rune/node/WorkerOp.h broker/libs/node/include/rune/node/Worker.h broker/libs/node/tests/node/WorkerFilterPushTest.cpp broker/libs/node/CMakeLists.txt`, then `git commit -m "feat(node): shard worker serves filtered subscriptions through FilterGroups (FilterSchema op, filter tail, one erase path); unfiltered push unchanged" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 9: Gateway: filter validation, admission caps, refusal details, ordered schema fan-out; node wiring and knobs

**Files:**
- Create: `broker/libs/gateway/include/rune/gateway/FilterAdmission.h`, `broker/libs/gateway/src/FilterAdmission.cpp`, `broker/libs/gateway/tests/gateway/FilterSubscribeGatewayTest.cpp`
- Modify: `broker/libs/gateway/CMakeLists.txt`:
  - add `src/FilterAdmission.cpp` to `rune_gateway`;
  - add the new test target `rune_gateway_filter_subscribe_tests`, modelled on `rune_gateway_subscribe_dedupe_tests` (it links `rune_gateway rune_node spdlog::spdlog GTest::gtest_main`, and also `rune_codec`, and includes `${RUNE_CLIENT_ROOT}/examples`).
- Modify: `broker/libs/gateway/include/rune/gateway/ClientGateway.h`, `broker/libs/gateway/src/ClientGateway.cpp`:
  - `Subscription` gains `std::vector<uint8_t> filter` and `FilterAdmission::Ticket filterTicket`.
  - `PendingSubOp` becomes a variant.
  - `handleSubscribe` gains the filter block.
  - `sendResp` takes a detail.
  - `buildSubscribeOp_` sets `filterBytes`.
  - New: `onFilterSchema`, `setFilterLimits`; `onCatalogObjectDropped` also erases the topic's filter schema.
- Modify: `broker/libs/node/include/rune/node/AdminChannels.h`:
  - `kA2cFilterSchema = 24`;
  - `filterSchemaWireLen`, `writeFilterSchema`, `decodeFilterSchema`, `FilterSchemaMsg`;
  - the `static_assert` that a max descriptor fits the ring.
- Modify: `broker/libs/node/include/rune/node/BrokerNode.h`:
  - new `postCatalogFilterSchema_`;
  - `catalogSink_`: `fx.topicApplied` posts the filter schema **before** `postCatalogTopicApplied_` for typed topics, and `fx.schemaChanged` posts it after `postCatalogValidator_`.
- Modify: `broker/libs/node/src/BrokerNode.cpp`:
  - dispatch `kA2cFilterSchema` → `gateway_->onFilterSchema`;
  - in `buildWorkerPool()`, next to `setIngressDrainLimit`, call `worker(i).setFilterConfig(...)` and `gateway_->setFilterLimits(...)`.
- Modify: `broker/libs/node/include/rune/node/NodeConfig.h`, `broker/libs/node/src/NodeConfig.cpp` (six knobs), `broker/libs/node/tests/node/NodeConfigTest.cpp`, `docs/configuration.md` (the six knobs)

**Interfaces:**
- Consumes: Tasks 3, 5 and 8.
- Produces:
  ```cpp
  namespace rune::gateway {
  class FilterSchemaRegistry {      // control thread
  public:
      void install(int32_t topicId, uint16_t version, std::span<const uint8_t> evolvingBlob);   // cold; logs + keeps the old one on a bad blob
      void erase(int32_t topicId) noexcept;
      [[nodiscard]] const rune::codec::FilterSchema* find(int32_t topicId) const noexcept;
  private:
      std::unordered_map<int32_t, rune::codec::FilterSchema> byTopic_;
  };
  class FilterAdmission {           // control thread; spec §13.2 caps per (topic, partition)
  public:
      struct Limits { uint32_t maxSubs = 4096; uint32_t maxValues = 4096; };
      class Ticket {                // move-only; its destructor releases the registration
      public:
          Ticket() = default; Ticket(Ticket&&) noexcept; Ticket& operator=(Ticket&&) noexcept; ~Ticket();
          Ticket(const Ticket&) = delete; Ticket& operator=(const Ticket&) = delete;
          [[nodiscard]] bool held() const noexcept { return owner_ != nullptr; }
      private:
          friend class FilterAdmission;
          FilterAdmission* owner_ = nullptr;
          uint64_t tp_ = 0;
          uint16_t tag_ = 0;
          std::vector<rune::codec::FilterKey> keys_;
      };
      void setLimits(Limits l) noexcept;
      /// Admit one filtered subscription: its index term's tag and values (CompiledFilter::indexTerm()).
      [[nodiscard]] std::expected<Ticket, std::string> admit(int32_t topicId, int32_t partition, uint16_t indexTag,
                                                             std::span<const rune::codec::FilterKey> keys);
      [[nodiscard]] uint32_t subsFor(int32_t topicId, int32_t partition) const noexcept;
      [[nodiscard]] uint32_t valuesFor(int32_t topicId, int32_t partition) const noexcept;
  private:
      void release_(Ticket& t) noexcept;
      struct Counts { uint32_t subs = 0; std::map<std::pair<uint16_t, rune::codec::FilterKey>, uint32_t> values; };
      std::unordered_map<uint64_t, Counts> byTp_;
      Limits limits_{};
  };
  }
  // ClientGateway:
  void onFilterSchema(int32_t topicId, uint16_t version, std::span<const uint8_t> evolvingBlob);   // control thread
  void setFilterLimits(FilterAdmission::Limits l) noexcept;
  // PendingSubOp (fix-on-sight: was {bool unsubscribe; bool mdc; BufferedSubscribe sub;}):
  enum class EgressRoute : uint8_t { Unicast, Mdc };
  struct SubOpSubscribe    { BufferedSubscribe sub; EgressRoute route = EgressRoute::Unicast; };
  struct SubOpUnsubscribe  { int64_t subId = 0; int32_t topicId = 0; int32_t partition = 0; };
  struct SubOpFilterSchema { int32_t topicId = 0; uint16_t version = 0; std::vector<uint8_t> blob; };
  using PendingSubOp = std::variant<SubOpSubscribe, SubOpUnsubscribe, SubOpFilterSchema>;
  void queueSubOp_(std::size_t worker, PendingSubOp p);
  ```
- Ticket identity: the `Ticket` holds a raw `FilterAdmission*`. That is safe because the gateway owns both the admission table and every `Subscription` record, on one thread, and the table outlives the records (declare `filterAdmission_` before `subscriptions_` in `ClientGateway`).
- NodeConfig knobs (env → field, default, bounds; a value out of bounds refuses start with a message naming the knob):
  - `RUNE_FILTER_MAX_SUBS_PER_PARTITION` → `filterMaxSubsPerPartition`: 4096, in [1, 1048576].
  - `RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION` → `filterMaxIndexValuesPerPartition`: 4096, in [1, 1048576].
  - `RUNE_FILTER_NOTE_INTERVAL_MS` → `filterNoteIntervalMs`: 50, in [1, 60000].
  - `RUNE_FILTER_NOTE_RECORDS` → `filterNoteRecords`: 65536, in [1, 2^31-1].
  - `RUNE_FILTER_VERSION_WAIT_MS` → `filterVersionWaitMs`: 5000, in [0, 600000].
  - `RUNE_FILTER_MATCH_SCRATCH` → `filterMatchScratch`: 65536, in [64, 16777216].

- [ ] **Step 1: Write the failing tests.**
  - `FilterSubscribeGatewayTest.cpp` uses `SubscribeDedupeTest.cpp`'s deterministic pattern: no Aeron, the gateway and a one-worker `WorkerPool` driven by hand, and null-egress test sessions observed through `onEgressFrame`. Sessions are injected with `injectReliableTestSessionForTest(now, 10)`, and `setPerWorkerEgressActive(true)`. Its helpers:
    - `filteredSubscribeFrame(sid, corr, partition, filterBytes)`;
    - `decodeSubscribeResponse(frame)` (with the detail);
    - the generated filter bytes from `PlayerEventSchema.h` through `rune::codec::FilterBuilder` (the gateway test does not need the generated header);
    - `ClientGateway::onFilterSchema(topicId, 1, serialize(playerEventDescriptor()))`.

    Tests:
    - `GatewayFilter.ValidFilterIsAdmittedAndForwardedWithItsBytes`: the worker ring's Subscribe op carries `TailKind::FILTER` with the same bytes.
    - `GatewayFilterRegression.FilterSchemaReachesTheWorkerBeforeAnyFilteredSubscribeRegression`: the worker ring is full when `onFilterSchema` runs, so the schema op is queued; a filtered Subscribe arrives; after draining, the worker's op order is FilterSchema, then Subscribe.
    - `GatewayFilterRegression.QueuedSubscribeUnsubscribeAndSchemaOpsKeepTheirOrderRegression` (fix-on-sight variant).
    - `GatewayFilterRegression.UntypedTopicIsRefusedWithDetailRegression` (E1, including the catalog topic id −6).
    - `GatewayFilterRegression.EverySchemaMismatchNamesItsTermRegression`: `TEST_P` over E2–E6, each asserting the code (39) and the exact detail text.
    - `GatewayFilterRegression.SubscriptionCapAnswersFilterCapacityRegression` (E8; `setFilterLimits({.maxSubs = 2})`, the third filtered Subscribe refused; after an Unsubscribe, a new one is admitted).
    - `GatewayFilterRegression.IndexValueCapAnswersFilterCapacityRegression` (E9; `{.maxValues = 3}`; an `In` with 4 values refused; another member reusing an admitted value accepted).
    - `GatewayFilterRegression.TicketsAreReleasedOnEveryRemovalPathRegression`: Unsubscribe, session drop, moved-away retire (`scanMovedAwaySlice_` via a role change) and topic drop each bring `subsFor()` back to 0.
    - `GatewayFilterRegression.LegacyModeRefusesFiltersRegression` (E12; `setPerWorkerEgressActive(false)`).
    - `GatewayFilterRegression.SessionBelowTenSendingTrailingBytesIsUnfilteredRegression` (E13; the same frame from a v9 session → OK, unfiltered, no tail).
    - `GatewayFilterRegression.RefusalDetailIsSentOnlyToVersionTenSessionsRegression`: a v10 session gets a response longer than 38 bytes with the detail; a v9 session with a raw trailer is never refused at all (E13).
    - `GatewayFilter.UnfilteredSubscribeResponseIsByteIdentical` (38 bytes).
    - `GatewayFilter.DuplicateFilteredSubscribeReanswersTheExistingSubscription` (the 5b dedupe path keeps one ticket).
  - `NodeConfigTest.cpp`: `NodeConfig.FilterKnobsDefaultAndParse` and `NodeConfigRegression.FilterKnobOutOfBoundsRefusesStartRegression`.
  - `broker/libs/node/tests/node/SecurityRingCodecTest.cpp` (or the a2c codec's existing test file): `AdminChannelsRegression.FilterSchemaMessageRoundTripsRegression`.
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/root -j14 --target rune_gateway_filter_subscribe_tests rune_node_tests`. Expected: missing header `rune/gateway/FilterAdmission.h`.
- [ ] **Step 3: Implement the filter block** in `handleSubscribe`, placed right before `// 6. Allocate subscriptionId and register subscription.`. The catalog / security branches earlier in the function get the same untyped check first: a session ≥ 10 sending a filter to −6 / −7 → `sendResp(InvalidRequest, …, "filter needs a typed topic")`.

```cpp
    // Phase 2c (spec §6 step 1, §9, FD1, FD3): a filter is read only from a session >= 10 (older sessions'
    // trailing bytes are ignored, as before).  Every refusal is answered with its detail; nothing is registered.
    std::span<const uint8_t> filterBytes;
    rune::codec::CompiledFilter compiled;
    FilterAdmission::Ticket ticket;
    if (pc::clientUnderstandsFilters(session->clientVersion) && !msg.filter.empty()) [[unlikely]] {
        if (!perWorkerEgressActive_ || !workerPool_) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0, "filter needs RUNE_WORKER_MODE=shard");
            return;
        }
        const rune::codec::FilterSchema* fs = filterSchemas_.find(msg.topicId);
        if (fs == nullptr) {
            sendResp(pc::ErrorCode::InvalidRequest, partition, 0, rune::codec::describe({rune::codec::FilterErrorKind::UntypedTopic}));
            return;
        }
        auto c = rune::codec::CompiledFilter::compile(msg.filter, *fs);
        if (!c) { sendResp(pc::ErrorCode::InvalidRequest, partition, 0, rune::codec::describe(c.error())); return; }
        const auto& it = c->term(c->indexTerm());
        auto t = filterAdmission_.admit(msg.topicId, partition, it.tag, c->values(c->indexTerm()));
        if (!t) { sendResp(pc::ErrorCode::InvalidRequest, partition, 0, t.error()); return; }
        filterBytes = msg.filter;
        compiled    = std::move(*c);
        ticket      = std::move(*t);
    }
```
  Then, in step 6:
  - `sub.filter.assign(filterBytes.begin(), filterBytes.end())` and `sub.filterTicket = std::move(ticket)`.
  - `buildSubscribeOp_` sets `op.filterBytes = std::span(it->second.filter)`, looking the record up in `subscriptions_` (the record outlives the synchronous enqueue). `BufferedSubscribe` is unchanged.
  - `sendResp` gains a `std::string_view detail = {}` parameter. It sets `resp.detail` only for a session ≥ 10, and sizes the scratch with `encodedSizeWithDetail()`; the detail is capped at 256 bytes, so it fits `kScratchSize`.

  `onFilterSchema`:
  1. `filterSchemas_.install(...)`.
  2. For each worker `w`: if `pendingSubOps_[w]` is non-empty, or `workerPool_->worker(w).enqueueOp(filterSchemaOp)` returns `Full`, then `queueSubOp_(w, SubOpFilterSchema{...})`.
  3. `TooLarge` → `spdlog::error` naming the size and `RUNE_WORKER_INGRESS_RING_BYTES`, counted. That worker cannot serve filters for the topic, and the gateway refuses its filtered Subscribes with "filter schema too large for the worker ring" (`InvalidRequest`); track the refusal in a per-topic set.
- [ ] **Step 4: Implement the node side.**
  - In `AdminChannels.h`: `struct FilterSchemaMsg { int32_t topicId; uint16_t version; std::vector<uint8_t> blob; }`, `filterSchemaWireLen(n) = 4 + 2 + n`, plus the `write` / `decode` pair, mirroring `writeSchemaUpdate`.
  - `postCatalogFilterSchema_(const rune::catalog::TopicObject& t)`: if `!t.schema`, return. Otherwise `postCatalog_(kA2cFilterSchema, filterSchemaWireLen(t.schema->evolvingBlob.size()), [&](uint8_t* p) { writeFilterSchema(p, t.topicId, t.schema->schemaVersion, t.schema->evolvingBlob); })`.
  - In `BrokerNode.cpp`'s admin→control drain, next to `kA2cCatalogTopicConfig`: `if (tid == kA2cFilterSchema) { FilterSchemaMsg m; if (decodeFilterSchema(p, n, m)) { if (gateway_) gateway_->onFilterSchema(m.topicId, m.version, m.blob); } else if (adminToControlRejects_++ % 1024 == 0) spdlog::error(...); return; }`.
- [ ] **Step 5: Run.** `cmake --build build/root -j14 --target rune_gateway_filter_subscribe_tests rune_gateway_subscribe_dedupe_tests rune_gateway_topic_servability_tests rune_gateway_catalog_tests rune_node_tests && ctest --test-dir build/root -R 'GatewayFilter|SubscribeDedupe|TopicServability|CatalogGateway|NodeConfig|AdminChannels|CatalogApplier|CatalogBoot|NoAtomicsOrMutexesOutsideAllowlist' --output-on-failure`. Expected: PASS.
- [ ] **Step 6: Commit.**

```bash
git add broker/libs/gateway/include/rune/gateway/FilterAdmission.h broker/libs/gateway/src/FilterAdmission.cpp \
        broker/libs/gateway/include/rune/gateway/ClientGateway.h broker/libs/gateway/src/ClientGateway.cpp \
        broker/libs/gateway/tests/gateway/FilterSubscribeGatewayTest.cpp broker/libs/gateway/CMakeLists.txt \
        broker/libs/node/include/rune/node/AdminChannels.h broker/libs/node/include/rune/node/BrokerNode.h broker/libs/node/src/BrokerNode.cpp \
        broker/libs/node/include/rune/node/NodeConfig.h broker/libs/node/src/NodeConfig.cpp broker/libs/node/tests/node/NodeConfigTest.cpp \
        broker/libs/node/tests/node/SecurityRingCodecTest.cpp docs/configuration.md
git commit -m "feat(gateway): validate and admit subscription filters (typed topic, schema, caps), refusal details, filter schemas fanned out in order with Subscribe ops; PendingSubOp is a variant" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Consumer groups: one filter per group at JoinGroup

**Files:**
- Modify: `broker/libs/coordination/include/rune/coordination/GroupCoordinator.h`, `broker/libs/coordination/src/GroupCoordinator.cpp`
- Modify: `broker/libs/node/src/GroupCoordinationService.cpp` (`handleJoinGroup`), `broker/libs/node/tests/node/GroupCoordinationServiceTest.cpp`
- Test: `broker/libs/coordination/tests/coordination/GroupCoordinatorTest.cpp` (target `rune_coordination_tests`)

**Interfaces:**
- Produces:
  ```cpp
  class GroupCoordinator {
  public:
      /// Phase 2c (spec §13.3): an EMPTY group adopts `filter` (any, including none); a non-empty one admits
      /// only byte-identical bytes.  The "unfixed" state is exactly isEmpty() — no extra flag.
      [[nodiscard]] bool admitFilter(std::span<const uint8_t> filter);
      [[nodiscard]] std::span<const uint8_t> filterBytes() const noexcept { return filter_; }
  private:
      std::vector<uint8_t> filter_;     // the group's filter while it has members (stale bytes are overwritten on the next empty-group join)
  };
  ```
- In `handleJoinGroup`, after the `kClientVersionExplicitGroupSubscriptions` refusal and before `gc.join(...)`:

```cpp
    // Phase 2c (spec §13.3): one filter per group, fixed by the first member, changeable once the group is empty.
    const int32_t ver = gateway_ ? gateway_->sessionClientVersion(msg.sessionId) : -1;
    const std::span<const uint8_t> filter = pc::clientUnderstandsFilters(ver) ? msg.filter : std::span<const uint8_t>{};
    if (!gc.admitFilter(filter)) {
        pc::JoinGroupResponse resp;
        resp.sessionId     = msg.sessionId;
        resp.correlationId = msg.correlationId;
        resp.errorCode     = static_cast<int16_t>(rejectionCodeFor(ver));
        resp.generationId  = 0;
        resp.isLeader      = 0;
        resp.memberId      = std::span<const uint8_t>();
        if (pc::clientUnderstandsFilters(ver)) {
            filterDetailScratch_ = groupFilterMismatchDetail(gc.filterBytes());   // cold; reserved member string
            resp.detail = filterDetailScratch_;
        }
        noteFilterMismatch_(groupIdStr, msg.sessionId);   // rate-limited warn: first per (group, session), then powers of two
        return resp.encode(out);
    }
```
  Helpers in `GroupCoordinationService.cpp` (anonymous namespace):

```cpp
// The old-client rule of ClientGateway::answerRejection_: a client below 5 re-joins forever on InvalidRequest.
pc::ErrorCode rejectionCodeFor(int32_t clientVersion) noexcept {
    return pc::clientUnderstandsInvalidRequest(clientVersion) ? pc::ErrorCode::InvalidRequest : pc::ErrorCode::ProtocolVersion;
}
// "group filter mismatch: the group uses filter <hex>" (at most 128 bytes shown, then "... (<n> bytes)"), "none" when empty.
std::string groupFilterMismatchDetail(std::span<const uint8_t> f) {
    std::string s(pc::kGroupFilterMismatchPrefix);
    s += ": the group uses filter ";
    if (f.empty()) { s += "none"; return s; }
    static constexpr char kHex[] = "0123456789abcdef";
    const std::size_t shown = std::min<std::size_t>(f.size(), 128);
    for (std::size_t i = 0; i < shown; ++i) { s += kHex[f[i] >> 4]; s += kHex[f[i] & 0xF]; }
    if (shown < f.size()) s += "... (" + std::to_string(f.size()) + " bytes)";
    return s;
}
```
  `filterDetailScratch_` (a `std::string` member) and `noteFilterMismatch_` (rate-limited by a bounded `std::unordered_map<std::string, uint64_t>` that is cleared at 4,096 entries, as `oldClientRefusals_` is) are new members of `GroupCoordinationService`. Move the existing `coord::GroupCoordinator& gc = coordinator(groupIdStr);` line above this block (it only creates the group).

- [ ] **Step 1: Write the failing tests.**
  - Coordinator unit tests:
    - `GroupCoordinatorFilter.FirstMemberFixesTheFilter`;
    - `GroupCoordinatorFilter.DifferentFilterIsRefusedWhileMembersExist`;
    - `GroupCoordinatorFilter.EmptyGroupAcceptsANewFilter` (join A(f1), leave A, join B(f2) → OK);
    - `GroupCoordinatorFilterRegression.NoFilterIsAFilterValueRegression` (a group fixed with no filter refuses a filtered member, and vice versa).
  - Service tests:
    - `GroupCoordinationServiceRegression.FilterMismatchAnswersInvalidRequestWithDetailRegression` (a v10 session; decode the detail; it starts with `kGroupFilterMismatchPrefix` and holds the group's hex);
    - `GroupCoordinationServiceRegression.OldMemberWithoutFilterIsRefusedFromAFilteredGroupRegression` (a v9 session → `InvalidRequest`, no detail);
    - `GroupCoordinationService.MatchingFilterJoinsAndRebalancesNormally`.
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/root -j14 --target rune_coordination_tests rune_group_coordination_service_tests`. Expected: `no member named 'admitFilter'`.
- [ ] **Step 3: Implement** as above.
- [ ] **Step 4: Run.** `ctest --test-dir build/root -R 'GroupCoordinator|GroupCoordination' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.** Run `git add broker/libs/coordination broker/libs/node/src/GroupCoordinationService.cpp broker/libs/node/tests/node/GroupCoordinationServiceTest.cpp`, then `git commit -m "feat(coordination): one filter per consumer group, fixed by the first member, refused with the group's filter in the detail" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

- [ ] **Checkpoint B (controller):** the same set as Checkpoint A (the broker `run-regression.sh`, the client and codec `*Regression*` suites, CatalogGolden, the ASan / TSan root builds, the client TSan build).

---

### Task 11: Client: `opts.filter(...)`, filtered `Subscribe` frames, the broker-version gate, refusal details, filtered positions

**Files:**
- Modify: `client-cpp/include/rune/client/subscriber/Subscription.h`:
  - `SubscribeOptions::filter` and `filterBytes`;
  - `SubscriptionImpl::filter`;
  - `SubscriptionFailReason::BrokerTooOld = 9`;
  - `Record::partition`;
  - `SubscriptionStatus::batchBytes`, `Subscription::batchBytes()`.
- Modify: `client-cpp/src/subscriber/Subscription.cpp` (`batchBytes()`).
- Modify: `client-cpp/include/rune/client/codec/DataCodec.h`: `encodeSubscribe(..., std::span<const uint8_t> filter = {})`, `kMaxSubscribeFrameBytes`, and `SubscribeResponseView::detail`. `decodeSubscribeResponse` copies `opt->detail`.
- Modify: `client-cpp/include/rune/client/subscriber/SubscriberHub.h`, `client-cpp/src/subscriber/SubscriberHub.cpp`:
  - the four `encodeSubscribe` call sites (lines ~114, ~778, ~861, ~1046) pass `impl.filter.span()`, with `frameBuf[kMaxSubscribeFrameBytes]`;
  - `setBrokerVersionFn(...)` and the gate in `sendSubscribeFrame_`;
  - filtered positions and notes in `onRecords`;
  - `Record::partition` and `batchBytes` accounting;
  - new `failActiveByCorrelation(...)`.
- Modify: `client-cpp/src/Client.cpp`:
  - `subscribeImpl` copies `opts.filterBytes` into the impl;
  - wire `setBrokerVersionFn` to `agent_.sessionBrokerVersion(index)`;
  - the `SubscribeResponse` case calls `cbError_` with the detail;
  - the `ErrorResponse` case handles `InvalidRequest` on an active subscription's correlation id.
- Create: `client-cpp/tests/subscriber/SubscriberFilterTest.cpp`
- Modify: `client-cpp/tests/support/InvokerGateway.h`:
  - `Subscribed::filter` (the received trailer bytes);
  - `subscribeErrorDetail` (sent with `subscribeErrorCode` to v10 sessions);
  - `pushNote(const Subscribed&, int64_t offset)` (an empty-batch Records frame).
- Modify: `client-cpp/CMakeLists.txt`: add `SubscriberFilterTest.cpp` to `rune_invoker_tests`, the Aeron + `InvokerGateway` target, with `add_dependencies(rune_invoker_tests generate_claim_schema_headers)` and `${RUNE_CLAIM_GEN_DIR}/gen` on its include path, so the test sees `player_event.gen.h`.

**Interfaces:**
- Consumes: Task 2 (`codec::FilterBytes`), Task 4 (`E::Filter`, `player_event.gen.h`), Task 5 (`kClientVersionFilter`, `kBrokerVersionFilter`, `Subscribe::filter`, `SubscribeResponse::detail`).
- Produces:
  ```cpp
  struct SubscribeOptions {
      // … existing members …
      /// Phase 2c: the encoded filter (empty = unfiltered; byte-identical Subscribe frame).
      rune::codec::FilterBytes filterBytes;
      /// A generated E::Filter (or anything with `FilterBytes encode() const`).  Throws std::invalid_argument
      /// (the encode error, naming the term) — explicit, before anything is sent.
      template <class F> requires requires(const F& f) { { f.encode() } -> std::same_as<rune::codec::FilterBytes>; }
      SubscribeOptions& filter(const F& f) { filterBytes = f.encode(); return *this; }
      SubscribeOptions& filter(const rune::codec::FilterBytes& b) noexcept { filterBytes = b; return *this; }   // FilterBuilder
  };
  enum class SubscriptionFailReason : int32_t { /* … */ UndeliverableBatch = 8, BrokerTooOld = 9 };
  struct Record { /* offset, timestamp, key, value, then: */ int32_t partition = 0; };
  struct SubscriptionStatus { /* … */ uint64_t batchBytes = 0; };
  // Subscription:
  [[nodiscard]] uint64_t batchBytes() const;   // sum of delivered recordBatch sizes (FD13)
  // SubscriptionImpl (immutable after construction):
  rune::codec::FilterBytes filter;
  [[nodiscard]] bool filtered() const noexcept { return !filter.empty(); }
  // DataCodec.h
  inline constexpr std::size_t kMaxSubscribeFrameBytes = 24 + 18 + 2 + 256 + 2 + rune::codec::kMaxFilterBytes;
  // SubscriberHub
  using BrokerVersionFn = int32_t (*)(void* ctx, uint32_t session) noexcept;   // 0 = unknown (not connected yet)
  void setBrokerVersionFn(BrokerVersionFn fn, void* ctx) noexcept;
  bool failActiveByCorrelation(int64_t correlationId, SubscriptionFailReason why, uint16_t code,
                               int64_t unsubCorr, int64_t sessionId, const SendFn& submitFn);
  ```
- Rules:
  - **Broker-version gate (SD3).** `sendSubscribeFrame_` checks a filtered impl before every send. If `fn(ctx, impl.homeSession)` is ≥ 1 and below `kBrokerVersionFilter`, it calls `finish_` with `BrokerTooOld`, publishes the final status, and sends nothing. A version of 0 means the session is not connected yet, and the frame stays pending, as today.
  - **Filtered positions (SD11):** these apply only when `impl.filtered()`.
    - After each non-skipped batch: `impl.nextOffset = std::max(impl.nextOffset, last + 1)`.
    - On an empty-batch note: `if (rv.baseOffset > impl.nextOffset) impl.nextOffset = rv.baseOffset;`, then publish. The `resumeHint` rule stays for `nextOffset == 0`.
    - Unfiltered impls keep today's code path exactly.
  - **Refusal detail:** `Client.cpp`'s `SubscribeResponse` case calls `cbError_("subscribe topic <t> partition <p> refused: <CodeName> (<code>): <detail>")` when `errorCode != 0` and the detail is non-empty, after the hub has handled the response.
  - **`InvalidRequest` on an active subscription's correlation id** (Task 8's defensive worker answer): `failActiveByCorrelation(corr, RejectedByBroker, 39, ...)` followed by `cbError_`.
  - **`Record::partition`** is `impl.boundPartition >= 0 ? impl.boundPartition : impl.partition`.
  - **`batchBytes`** grows by `rb.size()` per decoded frame (no allocation).

- [ ] **Step 1: Write the failing tests** (`SubscriberFilterTest.cpp`; a real `Client` in Invoker mode over `InvokerGateway`, as in `tests/client/InvokerClientTest.cpp`):
  - `SubscriberFilter.FilterBytesReachTheBrokerVerbatim` (`gw.subscribed[0].filter == E::Filter{.worldId = 7u}.encode().span()`).
  - `SubscriberFilterRegression.UnfilteredSubscribeFrameIsByteIdenticalRegression`: the captured frame of an unfiltered subscribe equals `codec::encodeSubscribe` with no filter, and its length has no trailer.
  - `SubscriberFilterRegression.OldBrokerFailsTheFilteredSubscriptionNothingSentRegression` (E11; `gw.advertisedBrokerVersion = 5`; `finished()`, `failReason() == BrokerTooOld`; `gw.subscribed` empty).
  - `SubscriberFilterRegression.MixedClusterChecksTheDestinationSessionRegression` (`advertisedBrokerVersionAt[1] = 5`; the partition is led by endpoint 1 after a Redirect; the filtered subscription fails `BrokerTooOld` at the re-send, and the unfiltered one on the same partition binds).
  - `SubscriberFilterRegression.RefusedFilterSurfacesTheDetailNeverFallsBackRegression` (E1–E9 class; `gw.subscribeErrorCode = 39`, `subscribeErrorDetail = "filter needs a typed topic"`; `RejectedByBroker`, `brokerErrorCode() == 39`, `onError` text contains the detail; no second Subscribe was sent).
  - `SubscriberFilterRegression.EncodeErrorThrowsBeforeAnythingIsSentRegression` (E7; `opts.filter(E::Filter{.worldId = int64_t{-1}})` throws `std::invalid_argument`).
  - `SubscriberFilterRegression.PositionFollowsNotesOnAQuietFilterRegression` (`gw.pushNote(sub, 5000)` → `position() == 5000`; an older note 4000 → it stays at 5000).
  - `SubscriberFilterRegression.PositionMovesToTheRebuiltBatchEndRegression` (a holed batch base 10, kept {0, 2}, `lastOffsetDelta` 4 → `position() == 15`).
  - `SubscriberFilterRegression.UnfilteredPositionRulesAreUnchangedRegression` (the same frames on an unfiltered subscription → today's values: 13, and the note becomes only a `resumeHint`).
  - `SubscriberFilterRegression.MovedSubscriptionResubscribesWithTheSameFilterBytesRegression` (NotLeader + Redirect → the re-Subscribe on endpoint 1 carries identical filter bytes and `fromOffset == position()`).
  - `SubscriberFilter.RecordCarriesItsPartition`.
  - `SubscriberFilter.BatchBytesCountsDeliveredBatches`.
  - `SubscriberFilterRegression.WorkerInvalidRequestFailsTheBoundSubscriptionRegression` (an `ErrorResponse{39}` on the bound subscription's correlation id → `finished()`, `RejectedByBroker`).
  - `ClientFilterSchemaRegression.SharedDescriptorMatchesTheCodecToolRegression` (the fingerprint of `playerEventDescriptor()` equals `rune::gen::PlayerEvent::kFingerprint`).
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/sub-client-cpp --target rune_invoker_tests`. Expected: `no member named 'filter' in 'SubscribeOptions'`.
- [ ] **Step 3: Implement** per the rules above. In `SubscriberHub::onRecords`, replace the empty-batch branch:

```cpp
    if (rb.empty()) {
        if (isEventLogTopic(impl.topicId)) [[unlikely]] { noteMalformed_(); return; }
        if (impl.filtered()) [[unlikely]] {
            // Phase 2c (SD11): a filtered subscription's POSITION NOTE — every record below it was evaluated and
            // every match below it delivered.  Monotonic.
            if (rv.baseOffset > impl.nextOffset) { impl.nextOffset = rv.baseOffset; publishStatus_(impl); }
            return;
        }
        if (impl.nextOffset == 0 && rv.baseOffset >= 0) impl.resumeHint = rv.baseOffset;
        return;
    }
```
  After the `forEachRecord` visit of a non-skipped batch:

```cpp
        if (impl.filtered() && last + 1 > impl.nextOffset) [[unlikely]] impl.nextOffset = last + 1;   // the rebuilt batch spans its source
```
- [ ] **Step 4: Run.** `cmake --build build/sub-client-cpp --target rune_invoker_tests rune_subscriber_tests rune_client_api_tests rune_redirect_tests rune_subscribe_notleader_regression_tests && ctest --test-dir build/sub-client-cpp -R 'SubscriberFilter|ClientFilterSchema|Subscriber|SubscriberHub|InvokerClient|Redirect|SubscribeNotLeader' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add client-cpp/include/rune/client/subscriber/Subscription.h client-cpp/src/subscriber/Subscription.cpp \
        client-cpp/include/rune/client/codec/DataCodec.h client-cpp/include/rune/client/subscriber/SubscriberHub.h \
        client-cpp/src/subscriber/SubscriberHub.cpp client-cpp/src/Client.cpp client-cpp/tests/subscriber/SubscriberFilterTest.cpp \
        client-cpp/tests/support/InvokerGateway.h client-cpp/CMakeLists.txt
git commit -m "feat(client): SubscribeOptions::filter (filter-by-example), filtered Subscribe frames, broker-version gate, refusal details, filtered positions follow batch ends and notes" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Client: `subscribe(topic, AllPartitions{}, ...)`, `GroupConfig::filter`, group trailers, group position notes

**Files:**
- Modify: `client-cpp/include/rune/client/subscriber/Subscription.h` (`AllPartitions`, `SubscriptionSet`), `client-cpp/src/subscriber/Subscription.cpp`, `client-cpp/include/rune/client/Client.h` (the new overload, documented as spec §3 says, including the partition-wide meaning of `subscribe(topic, key)` and the key + filter idiom), `client-cpp/src/Client.cpp`
- Modify: `client-cpp/include/rune/client/group/GroupConsumer.h`:
  - `GroupConfig::filterBytes` and `filter(...)`;
  - `GroupFailReason::FilterMismatch = 6`;
  - `sendBuf.resize(4096)` becomes `resize(kMaxSubscribeFrameBytes)`, with `ensureBuf` kept.
- Modify: `client-cpp/include/rune/client/group/GroupConsumerHub.h`, `client-cpp/src/group/GroupConsumerHub.cpp`:
  - `sendJoinGroup` sets `msg.filter`;
  - both fetch `encodeSubscribe` calls (lines ~776, ~957) pass the filter;
  - the brokerVersion gates (line ~936, ~1209, ~1937);
  - `onJoinGroupResponse` → `FilterMismatch`;
  - new `onPositionNote(const codec::RecordsView&)`.
- Modify: `client-cpp/src/Client.cpp` (the `Records` case routes empty-batch frames to `groupConsumerHub_.onPositionNote`; the `JoinGroupResponse` detail goes to `cbError_`)
- Create: `client-cpp/tests/client/SubscriptionSetTest.cpp`, `client-cpp/tests/group/GroupFilterTest.cpp`
- Modify: `client-cpp/CMakeLists.txt` (`SubscriptionSetTest.cpp` in `rune_invoker_tests`; `GroupFilterTest.cpp` in `rune_group_tests`)

**Interfaces:**
- Produces:
  ```cpp
  struct AllPartitions {};
  class SubscriptionSet {   // FD10: one Subscription per partition 0..partitionCount-1
  public:
      explicit SubscriptionSet(std::vector<Subscription> subs) noexcept;
      [[nodiscard]] std::size_t   size() const noexcept;
      [[nodiscard]] Subscription& operator[](int32_t partition);
      [[nodiscard]] int64_t       position(int32_t partition) const;
      [[nodiscard]] bool          active() const;      // any
      [[nodiscard]] bool          finished() const;    // all
      [[nodiscard]] uint64_t      batchBytes() const;  // sum
      void unsubscribe();                              // every partition
  private:
      std::vector<Subscription> subs_;
  };
  // Client:
  /// Every partition of the topic (spec §3).  `topic.partitionCount` partitions; the handler is copied per
  /// partition; Record::partition says which.  Throws std::invalid_argument when partitionCount < 1.
  SubscriptionSet subscribe(TopicHandle topic, AllPartitions, SubscribeOptions opts, RecordHandler handler);
  struct GroupConfig {
      // … existing …
      rune::codec::FilterBytes filterBytes;   // Phase 2c: one filter per group (spec §13.3)
      template <class F> requires requires(const F& f) { { f.encode() } -> std::same_as<rune::codec::FilterBytes>; }
      GroupConfig& filter(const F& f) { filterBytes = f.encode(); return *this; }
      GroupConfig& filter(const rune::codec::FilterBytes& b) noexcept { filterBytes = b; return *this; }
  };
  enum class GroupFailReason : int32_t { /* … */ SessionRevoked = 5, FilterMismatch = 6 };
  // GroupConsumerHub:
  void onPositionNote(const codec::RecordsView& rv);   // filtered groups only; identity checks exactly as onRecords
  ```
- Rules:
  - **Filtered group positions.** `onPositionNote` applies only when `impl->cfg.filterBytes` is non-empty. It runs the same instance checks as `onRecords` (`routed_()`, `fs.instanceCorr`, `fs.brokerSid`, epoch). If `pos != kUnseededSentinel && rv.baseOffset > pos`, then `assignment[tp] = rv.baseOffset`. Unfiltered groups ignore notes (today's behaviour).
  - **Old-broker gates.**
    - Seed: a filtered group whose seed `brokerVersion_` is below `kBrokerVersionFilter` → `refuse_(BrokerTooOld)` at the existing v3 check site.
    - Partition leaders: a leader below 6 → that partition is skipped and logged once per connect episode (the existing per-target rule, line ~936, with the bar raised to `kBrokerVersionFilter` when filtered). It is never fetched unfiltered.
  - **`FilterMismatch`.** In `onJoinGroupResponse`: `ec == InvalidRequest && opt->detail.starts_with(rpc::kGroupFilterMismatchPrefix)` → `refuse_(*impl, GroupFailReason::FilterMismatch)`. Otherwise the existing classification applies.

- [ ] **Step 1: Write the failing tests.**
  - `SubscriptionSetTest.cpp` (`InvokerGateway`, 4 partitions):
    - `SubscriptionSet.SubscribesEveryPartitionWithTheFilter` (4 Subscribes, each with the filter trailer and its own partition);
    - `SubscriptionSet.HandlerSeesEachRecordsPartition`;
    - `SubscriptionSet.UnsubscribeEndsEveryPartition` (`finished()` after the pump);
    - `SubscriptionSetRegression.OnePartitionRefusedFinishesOnlyThatOneRegression` (`subscribeErrorCode` scripted for partition 2 only: `[2].failReason() == RejectedByBroker`, the others active);
    - `SubscriptionSetRegression.ZeroPartitionTopicIsRejectedRegression`.
  - `GroupFilterTest.cpp` (no Aeron, the `GroupConsumerHubTest` style):
    - `GroupFilter.JoinGroupCarriesTheFilter` (decode the captured JoinGroup; `filter` equals the config's);
    - `GroupFilterRegression.UnfilteredJoinGroupIsByteIdenticalRegression`;
    - `GroupFilter.FetchSubscribesCarryTheFilter`;
    - `GroupFilterRegression.MismatchEndsTheConsumerWithFilterMismatchRegression` (E10: a scripted JoinGroupResponse{39, detail "group filter mismatch: ..."} → `failReason() == FilterMismatch`, no rejoin storm (exactly one JoinGroup sent));
    - `GroupFilterRegression.OtherInvalidRequestStaysRefusedByBrokerRegression`;
    - `GroupFilterRegression.PositionNoteMovesAFilteredGroupsPositionRegression` (seed 10; note 5000 → `assignment[tp] == 5000`; commit sends 5000);
    - `GroupFilterRegression.PositionNoteIsIgnoredByAnUnfilteredGroupRegression`;
    - `GroupFilterRegression.OldSeedRefusesTheFilteredGroupRegression` (`setBrokerVersion(5)` → `BrokerTooOld`);
    - `GroupFilterRegression.OldPartitionLeaderIsSkippedNeverFetchedUnfilteredRegression`.
- [ ] **Step 2: Run to verify they fail.** Run `cmake --build build/sub-client-cpp --target rune_invoker_tests rune_group_tests`. Expected: compile errors (`AllPartitions`, `GroupConfig::filter`).
- [ ] **Step 3: Implement** per the rules. `Client::subscribe(TopicHandle, AllPartitions, ...)`:

```cpp
SubscriptionSet Client::subscribe(TopicHandle topic, AllPartitions, SubscribeOptions opts, RecordHandler handler) {
    if (topic.partitionCount < 1)
        throw std::invalid_argument("subscribe(AllPartitions): topic has no partitions (partitionCount < 1)");
    std::vector<Subscription> subs;
    subs.reserve(static_cast<std::size_t>(topic.partitionCount));   // cold: once per subscribe
    for (int32_t p = 0; p < topic.partitionCount; ++p) subs.push_back(subscribe(topic, p, opts, handler));
    return SubscriptionSet(std::move(subs));
}
```
- [ ] **Step 4: Run.** `cmake --build build/sub-client-cpp --target rune_invoker_tests rune_group_tests rune_client_api_tests && ctest --test-dir build/sub-client-cpp -R 'SubscriptionSet|GroupFilter|GroupConsumer|GroupRouting|GroupSeek' --output-on-failure`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add client-cpp/include/rune/client/subscriber/Subscription.h client-cpp/src/subscriber/Subscription.cpp client-cpp/include/rune/client/Client.h \
        client-cpp/src/Client.cpp client-cpp/include/rune/client/group/GroupConsumer.h client-cpp/include/rune/client/group/GroupConsumerHub.h \
        client-cpp/src/group/GroupConsumerHub.cpp client-cpp/tests/client/SubscriptionSetTest.cpp client-cpp/tests/group/GroupFilterTest.cpp client-cpp/CMakeLists.txt
git commit -m "feat(client): subscribe(topic, AllPartitions{}) returns a SubscriptionSet; GroupConfig::filter rides JoinGroup and every fetch; filtered groups follow position notes; FilterMismatch" \
           -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Real-broker slice: every selection × filter × timing on `OneBroker` / `ThreeNodes`, every error row

**Files:**
- Create: `broker/libs/node/tests/client/FilterBrokerRegressionTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt`:
  - add the file to `rune_client_failover_broker_tests` (it kills brokers; `RUN_SERIAL`);
  - include `${CMAKE_BINARY_DIR}/rune-client/claim-gen/gen` (for `player_event.gen.h`; the target already depends on `generate_claim_schema_headers`) and `${RUNE_CLIENT_ROOT}/examples`.

**Interfaces:**
- Consumes: the real client (Tasks 11, 12) and broker (Tasks 8–10). Also `OneBroker.h` / `ClusterClient.h` helpers as `EosBrokerRegressionTest.cpp` uses them (`clientConfigFor`, `connectClient`, `awaitController`, `awaitLeaders`, `ThreeNodes::crash`).
- Topics are created by DDL: `defineTopicAndWait("player_event", 4, "playerId", serialize(playerEventDescriptor()), /*validate=*/false, Persistence::Disk, …)`, or `Persistence::Transient` for the transient cells. Never `RUNE_TOPICS`. ALTER uses `alterTopicAndWait` with the codec's `AlterOps` blob (ADD `guild UInt32`, DROP `channel`).
- Reads `broker/libs/node/tests/support/FilterMatrixExpectations.h` (Task 14) for the expected outcome. The slice's timings map onto matrix timings:
  - `Steady` → `SteadyPartial`;
  - `LeaderMove` → `OwnershipMove`;
  - `RestartResubscribe` → `OwnershipMove` (a restart ends the session: the app re-subscribes from `position()`);
  - `AlterDrop` → `AlterDropFilteredField`;
  - `RcAborted` → `AbortedTxn`.

  Task 14 therefore lands its header before this task runs. If you execute strictly in order, write the header's enums and `expect()` as Task 14 Step 1 specifies, as part of this task, and Task 14 only adds the rig.

- [ ] **Step 1: Write the tests.** Each cell produces a deterministic, seeded stream of `player_event` records: worldId in [0, 16), channel in [1, 4], 2,000 records over 4 partitions. It asserts the exact delivered set per subscription against the model, `position()` reaching each partition's end, and no duplicates.
  - `FilterBrokerRegression.Cell/*`: `TEST_P` over selection {`Key`, `Partition`, `AllPartitions`, `Group`} × filter {`Eq`, `EqAndIn`} × timing {`Steady`, `LeaderMove` (`ThreeNodes`, crash the partition leader mid-stream), `RestartResubscribe` (`OneBroker` restart), `AlterDrop`, `RcAborted` (a transactional `Producer` with `transactionalId`, one committed and one aborted transaction, consumer `read_committed`)}. 40 cells.
  - The `Key` selection subscribes `subscribe(topic, Key(uint64_t{k}), opts.filter(E::Filter{ .playerId = k }))` (`EqAndIn` cells: `E::Filter{ .playerId = k, .channel = anyOf(1, 2) }`) and asserts that only key k's records arrive. This is the spec §3 idiom; the plain key subscription delivers the whole partition.
  - Error rows, each its own test:
    - `FilterBrokerRegression.UntypedTopicRefusedRegression` (E1: an untyped topic defined without a descriptor; detail via `onError`);
    - `FilterBrokerRegression.SchemaMismatchRefusedRegression` (E3–E6 via `FilterBuilder` against a hand-edited descriptor);
    - `FilterBrokerRegression.SubscriptionCapRefusedRegression` (E8; the cluster started with `RUNE_FILTER_MAX_SUBS_PER_PARTITION=4`);
    - `FilterBrokerRegression.IndexValueCapRefusedRegression` (E9; `RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION=4`);
    - `FilterBrokerRegression.GroupFilterMismatchRefusedRegression` (E10: two clients, the same group, different filters → the second ends `FilterMismatch`, the first keeps consuming);
    - `FilterBrokerRegression.LegacyModeRefusedRegression` (E12: a `OneBroker` with `RUNE_WORKER_MODE=legacy`);
    - `FilterBrokerRegression.UnfilteredNeighbourSeesEveryRecordRegression` (control).
  - `FilterBrokerRegression.GroupCommitAdvancesOnAQuietFilterRegression`: a group filter matching nothing; `commitSyncAndWait` after the notes; `OffsetFetch` returns the partitions' ends.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_client_failover_broker_tests && ctest --test-dir build/root -R 'FilterBroker' --output-on-failure`. Expected: PASS. A failing cell is a bug: fix it in the owning code with its own `*Regression*` test, never by editing `expect()` without a spec reason in the commit body.
- [ ] **Step 3: Commit.** Run `git add broker/libs/node/tests/client/FilterBrokerRegressionTest.cpp broker/libs/node/CMakeLists.txt`, then `git commit -m "test(filter): real-broker slice - selection x filter x timing (leader crash, restart, ALTER, read_committed) and every refusal row" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 14: The worker-level interaction matrix: 5 ops × 4 entities × 14 timings

**Files:**
- Create: `broker/libs/node/tests/support/FilterMatrixExpectations.h`, `broker/libs/node/tests/node/WorkerFilterMatrixTest.cpp`, `broker/libs/node/tests/support/FilterMatrixRig.h`
- Modify: `broker/libs/node/CMakeLists.txt` (new Aeron target `rune_worker_filter_matrix_tests`, `TIMEOUT 600`, `rune_regression_batches(rune_worker_filter_matrix_tests 4 TIMEOUT 900 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")`)

**Interfaces:**
```cpp
namespace rune::node::filtermatrix {
enum class Op : uint8_t { Unfiltered, Eq, In, EqAndIn, MixedIndexFields };
enum class Entity : uint8_t { DiskRU, DiskRC, TransientRU, TransientRC };
enum class Timing : uint8_t { SteadyAllMatch, SteadyPartial, SteadyNoMatch, StraddlingResume, QuietRun, OpenTxn, AbortedTxn,
                              CommittedTxn, OwnershipMove, TransientEviction, AlterAddField, AlterDropFilteredField,
                              VersionArrivesLate, BackPressure };
enum class NaRule : uint8_t { NA1, NA2 };
enum class Outcome : uint8_t { ByteIdentical, ZeroCopyOriginal, Rebuilt, NoteOnly, WholeBatchFromPosition, NotesByCountAndTime,
                               WithheldUntilCommit, Delivered, NeverDelivered, ResumeExact, SkipEvictedCounted, AbsentExcluded,
                               StallThenDeliver, NoLossNoDupRejoined };
[[nodiscard]] constexpr bool isTransient(Entity e) noexcept { return e == Entity::TransientRU || e == Entity::TransientRC; }
[[nodiscard]] constexpr bool isRc(Entity e) noexcept { return e == Entity::DiskRC || e == Entity::TransientRC; }
[[nodiscard]] constexpr std::optional<NaRule> naRule(Op o, Entity e, Timing t) noexcept {
    if (o == Op::Unfiltered && t != Timing::SteadyAllMatch && t != Timing::AbortedTxn && t != Timing::OwnershipMove &&
        t != Timing::BackPressure) return NaRule::NA1;
    if (t == Timing::TransientEviction && !isTransient(e)) return NaRule::NA2;
    return std::nullopt;
}
[[nodiscard]] constexpr Outcome expect(Op o, Entity e, Timing t) noexcept {
    if (o == Op::Unfiltered) return Outcome::ByteIdentical;                                   // R1
    switch (t) {
    case Timing::SteadyAllMatch:         return Outcome::ZeroCopyOriginal;                    // R2
    case Timing::SteadyPartial:          return Outcome::Rebuilt;                             // R3
    case Timing::SteadyNoMatch:          return Outcome::NoteOnly;                            // R4
    case Timing::StraddlingResume:       return Outcome::WholeBatchFromPosition;              // R5
    case Timing::QuietRun:               return Outcome::NotesByCountAndTime;                 // R6
    case Timing::OpenTxn:                return isRc(e) ? Outcome::WithheldUntilCommit : Outcome::Delivered;   // R7
    case Timing::AbortedTxn:             return isRc(e) ? Outcome::NeverDelivered : Outcome::Delivered;        // R8
    case Timing::CommittedTxn:           return Outcome::Delivered;                           // R9
    case Timing::OwnershipMove:          return Outcome::ResumeExact;                         // R10
    case Timing::TransientEviction:      return Outcome::SkipEvictedCounted;                  // R11
    case Timing::AlterAddField:          return Outcome::Delivered;                           // R12
    case Timing::AlterDropFilteredField: return (o == Op::Eq || o == Op::MixedIndexFields) ? Outcome::Delivered
                                                                                         : Outcome::AbsentExcluded;  // R13
    case Timing::VersionArrivesLate:     return Outcome::StallThenDeliver;                    // R14
    case Timing::BackPressure:           return Outcome::NoLossNoDupRejoined;                 // R15
    }
    return Outcome::Delivered;
}
}  // namespace rune::node::filtermatrix
```
(R13: `Eq` and `MixedIndexFields` filter on `worldId` / `playerId`, which survive the drop. The model still computes the exact set, so the `Delivered` outcome for `Eq` is checked against the model's v3 matches.)

- `FilterMatrixRig`, one per cell:
  - an embedded driver, a `Worker` (`setFilterConfig` with a 2 ms note interval and a 50 ms version wait);
  - a real disk `Log`, or a transient replica (`InMemoryLog`, as `TransientOwnedPushRegression` builds it), and, for RC, a replica with RC state as `TxnRcRegression` builds it;
  - a `WorkerEgress` plus IPC subscriber;
  - a `ModelClient` that applies frames, notes, straddle drops and positions;
  - a `FilterModel`-lite expected set computed from the appended records.
- `BackPressure` refuses through a small-term publication that is not polled for 3 passes, then polled. The `WorkerLargeBatchPushRegression` fixture shows how to provoke back-pressure.

- [ ] **Step 1: Write the tests:**
  - `FilterMatrixCountRegression.CellCountsArePinned`: iterate the enums; 280 total, 232 applicable, NA1 40, NA2 8. Applicable by rule: R1 16, R11 8, every other rule 16.
  - `FilterMatrix/WorkerFilterMatrixTest.Cell/*`: `INSTANTIATE_TEST_SUITE_P` over the cartesian product; `GTEST_SKIP() << "NA: " << rule` for N/A cells. Each applicable cell runs its timing script and asserts the six points of *Every cell asserts*.
- [ ] **Step 2: Run.** `cmake --build build/root -j14 --target rune_worker_filter_matrix_tests && ctest --test-dir build/root -R 'FilterMatrix' -j8 --output-on-failure`. Expected: 232 pass and 48 skipped with their rule. A failing cell is a bug in the owning code: fix it there with a `*Regression*` test.
- [ ] **Step 3: Commit.** Run `git add broker/libs/node/tests/support/FilterMatrixExpectations.h broker/libs/node/tests/support/FilterMatrixRig.h broker/libs/node/tests/node/WorkerFilterMatrixTest.cpp broker/libs/node/CMakeLists.txt`, then `git commit -m "test(filter): worker interaction matrix - 5 ops x 4 entities x 14 timings (232 applicable, 48 N/A by rule)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 15: Seeded, model-checked stress of the engine and the index

**Files:**
- Create: `broker/libs/node/tests/support/FilterModel.h` (independent: it shares no code with `FilterGroup`, `FilterIndex` or `CompiledFilter`. It holds records as structs `{offset, version, worldId?, channel?, playerId, null}`, members as `{filter terms as plain values, cursor}`, and brute-force matching), `broker/libs/node/tests/node/FilterStressRegressionTest.cpp`
- Modify: `broker/libs/node/CMakeLists.txt` (add it to `rune_filter_engine_tests`)

- [ ] **Step 1: Write** `FilterStressRegression.SeededRunsMatchTheModelRegression`: `TEST_P` over seeds 1..64, 600 steps each; `RUNE_FILTER_STRESS_SEEDS` widens the run locally. Each step picks one action:
  - add a member (a random filter of Eq / In on 1–2 of {worldId, channel, playerId}, a random start offset);
  - remove a member;
  - append a batch (1–64 records, a random version among the known ones, 2% null values, 1% short values);
  - ALTER (extend the schema: add `guild` or drop `channel`);
  - append a record of a not-yet-known version (the schema arrives k steps later, or never);
  - refuse the next N sends of a random member (`FakeSink`);
  - advance the clock (by note intervals or version waits);
  - run `pass()`.

  After every `pass()`, every member's delivered prefix must equal the model's. At the end, after draining:
  - every member got exactly the model's set, in order, with no duplicates;
  - every position reached the end;
  - the counters equal the model's (`absent`, `unknownVersion`, `null`, `malformed`);
  - 0 allocations in the measured steps after a warm-up.

  A failure prints the seed and the step log.
- [ ] **Step 2: Run.** `ctest --test-dir build/root -R 'FilterStress' --output-on-failure`. Expected: PASS.
- [ ] **Step 3: Commit.** Run `git add broker/libs/node/tests/support/FilterModel.h broker/libs/node/tests/node/FilterStressRegressionTest.cpp broker/libs/node/CMakeLists.txt`, then `git commit -m "test(filter): seeded model-checked stress of the filter engine (64 seeds x 600 steps)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

- [ ] **Checkpoint C (controller):** the same set as Checkpoint A.

---

### Task 16: Benchmarks and the allocation gate: O(1) in subscribers, the latency class, unfiltered non-regression

**Files:**
- Create: `broker/benchmark/FilterPushBench.cpp`, `broker/libs/node/tests/node/FilterAllocRegressionTest.cpp`
- Modify: `broker/CMakeLists.txt` (add `benchmark/FilterPushBench.cpp` to `rune_broker_benchmark`), `broker/libs/node/CMakeLists.txt` (own binary `rune_filter_alloc_regression_tests`, modelled on `rune_catalog_alloc_regression_tests`: a counting global `operator new`)

**Benches** (in `rune_broker_benchmark`, using `BenchGate.h`; every gate goes through `rune::bench::gateCheck`):
- `BM_FilterIndexLookup/<subs>/<hit>`, for subs ∈ {1, 50, 500} (distinct worldIds) and hit ∈ {0, 1}. It measures the per-record accessor + key load + `FilterIndex::forEach` on a 64 Ki-record batch of `player_event` values, reporting `ns_per_record`.
  - **Gate:** `ns_per_record ≤ 5.0` for each arm.
  - **Flatness gate** (checked in the 500 arm against the 1 arm, recorded in a static set by thread 0): `ns(500) ≤ 1.10 × ns(1)`.
- `BM_FilterRebuild/<pct>`, for pct ∈ {1, 10, 50}. It times `rebuildBatch` on 256-record, 1 KiB-value batches and reports `ns_per_matched_byte` (reported, per spec §11) and `allocs` (gate: 0).
- `BM_FilterWorkerPush/<subs>`, for subs ∈ {1, 50, 500}. It runs a real `Worker` with an embedded-driver IPC egress, one partition, `subs` filtered subscriptions with distinct worldIds, plus 1 unfiltered control. Each iteration appends a 64-record batch (uniform worldIds) and times append → the first matching frame polled on the IPC subscriber. It reports `p50_us`, `p99_us`, `ns_per_record_broker` (worker pass time / records) and `allocs`.
  - **Gates:** `p50_us ≤ 50`, `p99_us ≤ 200`, `allocs == 0`, and `ns_per_record_broker(500) ≤ 1.10 × ns_per_record_broker(1)`.
- Unchanged and gated as they are: `BM_RcWorkerPush/0` and `/1`, and `GatewayDataFixture/Produce` and `GatewayDataFixture/SubscribeDelivery` (p99 < 100 µs).

**Alloc gate:** `FilterAllocRegression.SteadyStateFilteredPushAllocatesNothingRegression`. 50 filtered members and 1 unfiltered member on a worker, 10,000 batches of mixed matches, notes and an ALTER-free steady state, after a 100-batch warm-up: the counting `operator new` sees 0 on the worker thread.

- [ ] **Step 1: Baseline first.** In a scratch worktree of this branch's parent commit (`git worktree add <scratchpad>/rune-base main`; configure Release `BUILD_BENCHMARKS=ON`, as `build/rel` is), run `rune_broker_benchmark --benchmark_filter='BM_RcWorkerPush|GatewayDataFixture'` ABAB against the tip, n = 3, on the same box (lean-bench rule: one prepare / restore, these key benches only). Record the medians.
- [ ] **Step 2: Write** `FilterPushBench.cpp` and `FilterAllocRegressionTest.cpp` as specified.
- [ ] **Step 3: Run.**
  - Alloc gate: `cmake --build build/root -j14 --target rune_filter_alloc_regression_tests && ctest --test-dir build/root -R FilterAllocRegression --output-on-failure`. Expected: PASS.
  - Benches: `cmake --build build/rel -j14 --target rune_broker_benchmark && build/rel/broker/rune_broker_benchmark --benchmark_filter='BM_Filter|BM_RcWorkerPush|GatewayDataFixture' --benchmark_repetitions=3`. Expected: every gate passes. `BM_RcWorkerPush/0` and `/1` are within +2% of the Step 1 baseline (same-session ABAB median).
  - If a number is borderline on the dev Mac, the controller confirms on the prepared VM (`broker/bench/vm-bench-prepare.sh`, run, `vm-bench-restore.sh`) before calling it a regression (BENCHMARKING.md: compare to the same-session A/B).
  - Never relax a gate. A failing gate is profiled and fixed (a hot-path allocation is a CLAUDE.md #4 violation).
- [ ] **Step 4: Commit,** with the baseline and new numbers in the body: run `git add broker/benchmark/FilterPushBench.cpp broker/CMakeLists.txt broker/libs/node/tests/node/FilterAllocRegressionTest.cpp broker/libs/node/CMakeLists.txt`, then `git commit -m "bench(filter): index lookup <= 5 ns flat across 1/50/500 subscribers, rebuild per matched byte, in-process filtered push p50/p99 gate; zero-allocation gate" -m "<numbers>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

---

### Task 17: End to end on 3 authenticated brokers (k3s VM): the probe, the script, the MMORPG scenario, docs, release note

Modelled on `broker/docker/view-engine-e2e.sh`:
- one dedicated probe image built from one example (`client-cpp/docker/Dockerfile.filter-probe`, modelled on `Dockerfile.view-probe`);
- `RUNE_MODE` legs run as `docker run --rm` containers on the compose network, with KEY=VALUE handoff lines between legs;
- bounded waits only, with a `trap` teardown;
- authentication through `broker/docker/lib/auth.sh` (every probe gets `RUNE_USER` / `RUNE_PASSWORD_FILE`).

It differs in one way: it brings up the **3-node** cluster (`docker-compose.yml` + `docker-compose.auth.yml`, compose project `rune-filter-e2e`), as `eos-e2e.sh` does, because the leader-kill and scenario legs need it.

**Files:**
- Create: `client-cpp/examples/filter_probe.cpp`, `client-cpp/docker/Dockerfile.filter-probe`, `client-cpp/docker/Dockerfile.filter-probe.dockerignore` (copied from `Dockerfile.view-probe.dockerignore`; it keeps `tests/` so `GenClaimSchemas.cpp` and the generated `player_event.gen.h` build in the image), `broker/docker/filter-e2e.sh`
- Modify: `client-cpp/CMakeLists.txt` (the `filter_probe` example target: depends on `generate_claim_schema_headers`; includes `${RUNE_CLAIM_GEN_DIR}/gen` and `examples`)
- Modify: `docs/api/client-cpp.md` (a new "Subscription filters" section after "Subscribe": the filter-by-example API, `anyOf`, `AllPartitions`, the key + filter idiom, `GroupConfig::filter`, the refusals, `BrokerTooOld`, `FilterMismatch`, positions and notes), `docs/api/codec.md` (the generated `Filter`, `FilterBuilder`, the `Filter` field-name rule)
- Create: `broker/docs/release-notes/phase2c-subscription-filters.md` (semantics; wire v10 / broker v6; the six knobs; legacy mode refused; upgrade every broker before subscribing with filters, otherwise `BrokerTooOld`; no catalog or fingerprint change)

**`filter_probe` modes** (`RUNE_MODE`; deterministic records from `RUNE_SEED`; the record value embeds the send time, `CLOCK_REALTIME` ns, in `hp`):
- `setup`: define `player_event` (8 partitions, rf 3, key `playerId`, `validate = false`) and `untyped_t` (untyped). Prints `FILTER_TOPIC_ID=…` and `FILTER-PROBE-SETUP: OK`.
- `produce`: N records (`FILTER_E2E_N`, default 40,000) at `FILTER_E2E_RATE` per second. worldId uniform in [0, 200), channel in [1, 4], 1% tombstones. It includes one committed and one aborted transaction of worldId 7, through a `Producer` with `transactionalId`.
- `consume`: the subscriptions:
  - (a) `AllPartitions` + `worldId == 7`, `read_committed`;
  - (b) `AllPartitions` + `worldId == 8 ∧ channel ∈ {1, 2}`;
  - (c) `Key(k)` + `playerId == k`;
  - (d) group `worlds-9` (2 members) + `worldId == 9`, `read_committed`;
  - (e) one unfiltered `Partition(0)` control.

  It verifies each exact set against the model (the same seed), no duplicates, per-partition order, and `position()` at each partition's end. It survives leader moves: subscriptions follow and resume, and a `SessionLost` re-subscribes from `position()`. It prints `FILTER-CONSUME: OK sets=5`.
- `errors`: E1 (`untyped_t`), E3/E4 (`FilterBuilder` against a stale descriptor), E8 (cap 64: 65 filtered subscriptions on partition 0), E10 (a second group member with a different filter). Each refusal must surface its detail. It prints `FILTER-ERRORS: OK`.
- `alter`: `alterTopicAndWait` to ADD `guild` (v2), produce 2,000 v2 records, DROP `channel` (v3), produce 2,000 v3 records. Subscription (b) must receive no v3 record, and its v1/v2 matches must be exact. It prints `FILTER-ALTER: OK`, and the broker log must contain the "filtered field absent" line once for (b) and v3.
- `world`: `FILTER_E2E_WORLDS_PER_PROC` world servers (default 25), each a `SubscriptionSet` (`AllPartitions` + `worldId == w`), or unfiltered with client-side dropping when `FILTER_E2E_UNFILTERED=1`. It runs for `FILTER_E2E_SECONDS`, then prints per world `WORLD w=<w> records=<n> bytes=<batchBytes> ideal_bytes=<n * per-record bytes> p50_us=… p99_us=…`.

**`filter-e2e.sh`:**
1. Build the images.
2. Generate the cluster key; `up` 3 nodes, with `RUNE_FILTER_MAX_SUBS_PER_PARTITION=256` and `RUNE_FILTER_NOTE_INTERVAL_MS=20` through the compose environment.
3. Bootstrap and create the e2e user (topic READ / WRITE / CREATE, `READ GROUP PREFIX ''`, `WRITE TRANSACTIONAL_ID PREFIX ''`).
4. Run `setup`.
5. Start `consume`, then `produce`.
6. At ~30% progress, `docker kill` the leader of partition 3 (read from the probe's metadata line), and restart it after progress resumes.
7. At ~60%, `docker compose restart` one non-leader node.
8. `consume` must exit 0 within 600 s. Then run `errors` and `alter`.
9. Print `FILTER-E2E: OK`.

With `--scenario`, the script runs only the MMORPG scenario instead:
1. On a prepared box (`vm-bench-prepare.sh` before, `vm-bench-restore.sh` in the `trap`), start 8 `world` containers (200 world servers) and one `produce` at `FILTER_E2E_RATE` (default 50,000/s, the highest rate the 4-vCPU box sustains unfiltered; the spec's 1M/s exceeds the box, see SD14).
2. Run ABAB × 3: filtered, then unfiltered.
3. Print the medians and check the gates:
   - every world's `bytes ≤ 1.2 × ideal_bytes`;
   - the filtered p50 and p99 (median over the runs) are no worse than unfiltered beyond the run-to-run band printed with them.
4. Print `FILTER-SCENARIO: OK` or `FAIL <why>`.

- [ ] **Step 1: Write** the probe, the Dockerfile and its ignore file, the script, the docs and the release note.
- [ ] **Step 2: Local static checks.**
  - `bash -n broker/docker/filter-e2e.sh`.
  - `cmake --build build/sub-client-cpp --target filter_probe` (it builds).
  - `grep -nE '([0-9]{1,3}\.){3}[0-9]{1,3}|@[a-z0-9-]+\.[a-z]' broker/docker/filter-e2e.sh client-cpp/examples/filter_probe.cpp` finds nothing beyond `127.0.0.1` / `0.0.0.0` bind literals (no IPs, emails or secrets).
- [ ] **Step 3: Run on the VM (controller-run; never the Mac).** Stage the monorepo and a sibling `flight-recorder`, then run under the lock:

```bash
rsync -a --delete --exclude build/ --exclude .git/ ./ root@$RUNE_VM_HOST:/root/mono/rune/
rsync -a --delete "$FLIGHT_RECORDER_DIR"/ root@$RUNE_VM_HOST:/root/mono/flight-recorder/
ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash /root/mono/rune/broker/docker/filter-e2e.sh'
ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash /root/mono/rune/broker/docker/filter-e2e.sh --scenario'
ssh root@$RUNE_VM_HOST 'flock -w 7200 /root/vm.lock bash -c "cd /root/mono/rune/broker/docker && ./view-engine-e2e.sh && ./eos-e2e.sh"'
```
  Expected:
  - `FILTER-E2E: OK` and `FILTER-SCENARIO: OK` (the gate lines are printed);
  - the view-engine and EOS e2es still pass: unfiltered paths, and their subscribe / group traffic now speaks v10.
- [ ] **Step 4: Run the affected client tests and commit.**
  - Run `ctest --test-dir build/sub-client-cpp -R 'SubscriberFilter|SubscriptionSet|GroupFilter' --output-on-failure`.
  - Run `git add client-cpp/examples/filter_probe.cpp client-cpp/docker/Dockerfile.filter-probe client-cpp/docker/Dockerfile.filter-probe.dockerignore client-cpp/CMakeLists.txt broker/docker/filter-e2e.sh docs/api/client-cpp.md docs/api/codec.md broker/docs/release-notes/phase2c-subscription-filters.md`.
  - Commit with `git commit -m "test(filter): 3-node e2e (every selection, leader kill, restart, ALTER, refusals) and the MMORPG scenario; docs and release note" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.

- [ ] **Checkpoint D (controller):**
  - the same set as Checkpoint A;
  - the VM e2e set green (Task 17 Step 3);
  - the Task 16 benches green;
  - then the final whole-branch review.

---

## Finish

The implementer's part ends with each task's affected-area runs. The controller then does the following:

- [ ] Run Checkpoint D (above) to green.
- [ ] Final whole-branch review (Opus).
- [ ] File the deferred items as GitHub issues on `arcanumruna/rune` (one batch, one summary at the end):
  1. Rune QL `WHERE` lowering onto `FilterBuilder` once a native query or consume statement exists (SD13).
  2. Filters in legacy worker mode (FD1).
  3. `WorkerSubscription`'s booleans (`isGroup`, `groupCarrierReceived`, `active`, `cursorFresh`) as an explicit state variant (fix-on-sight deferred, Global Constraints 8).
  4. A view-source `Filter` operator reusing `CompiledFilter` (spec §12).
- [ ] Hand the branch to the user. Do not merge or push.

## Self-review (done while writing)

- **Spec coverage:**
  - §1 Goal and success: Tasks 7, 8, 16 (O(1) gate), 17 (bytes and latency).
  - §2 Non-goals respected: no ranges, OR or NOT; no var-len or float fields (Task 3 refuses them); no FixedStr > 16; no live filter change (re-subscribe); untyped topics refused (Task 9); no view-source filters.
  - §3:
    - selection forms: Tasks 11, 12 (`AllPartitions`), 13 (key + filter idiom);
    - byte-identical unfiltered frames: Tasks 5, 11, 12;
    - one group filter: Task 10.
  - §4:
    - `E::Filter`, `Match<T>` (inline, up to 64 values), `anyOf`: Tasks 2, 4;
    - compile-time checks: Tasks 2, 4;
    - runtime limits: Task 2;
    - the term limit: Tasks 2, 4;
    - empty filter = no filter: Tasks 2, 4;
    - durable tags: Tasks 3, 4;
    - constexpr encoding: Task 2;
    - the runtime builder: Task 4;
    - QL lowering: deferred (SD13).
  - §5:
    - encoding and sizes: Task 2;
    - Subscribe / JoinGroup fields: Task 5;
    - max frame and `length_error`: Task 11 (`kMaxSubscribeFrameBytes`, with the existing check in `subscribeImpl`);
    - canonical form: Tasks 2, 3;
    - version gating: Tasks 5, 11, 12 (SD2–SD4).
  - §6:
    - validation: Tasks 3, 9;
    - index term: Tasks 3, 7;
    - per-version accessors: Task 3 (SD12);
    - per-partition index: Tasks 6, 7;
    - per-record evaluation: Task 7;
    - absent = no match: Tasks 3, 7;
    - caps and preallocation: Tasks 6, 7, 9.
  - §7:
    - zero-copy, rebuild and nothing: Tasks 1, 7;
    - position notes: Tasks 7, 11, 12;
    - `read_committed`: Tasks 7, 8, 13, 14;
    - moved subscriptions: Tasks 11, 13, 14;
    - groups and committed offsets: Tasks 12, 13.
  - §8:
    - durable tags across ALTER: Tasks 3, 14;
    - dropped field counted and logged: Tasks 7, 14, 17;
    - retype: `FieldSlot::type` (Task 3);
    - unknown version: FD6 (Tasks 7, 8).
  - §9: every row E1–E13, in Tasks 9–13.
  - §10: the interaction matrix (Task 14), the stress (Task 15), the broker slice (Task 13), the e2e (Task 17); the three invariants in *Every cell asserts*.
  - §11: Task 16 (index ≤ 5 ns, flat; rebuild per byte; unfiltered ≤ 2% + allocation gate) and Task 17 (the scenario, FD12).
  - §12: `CompiledFilter` is standalone in `codec/` (FD2).
  - §13: the cadence defaults (FilterConfig), the caps (Task 9), the one group filter (Task 10).
- **Placeholder scan:**
  - No TBD or TODO.
  - Two parenthetical fallbacks name the exact alternative: the `RecordBatchReader` accessor names (Task 1), and the codec `operator new` counter (Task 3). Both are mechanical, not design gaps.
  - Task 13 references Task 14's header and says what to do if executed strictly in order.
- **Type consistency:**
  - `FilterBytes`, `FilterKey`, `FilterError`, `FilterErrorKind`, `describe`, `CompiledFilter::{compile, indexTerm, term, values, evaluate, evaluateResidual}`, `FilterVerdict`, `FilterSchema::{build, extendFrom, slot, knowsVersion}`, `FilterIndex::{init, add, remove, forEach}`, `FilterGroup::{init, add, remove, moveSub, pass, cursorOf}`, `SendOutcome` and `FilterConfig` keep one spelling across Tasks 2–17.
  - The wire names are consistent: `kClientVersionFilter = 10`, `kBrokerVersionFilter = 6`, `OpType::FILTER_SCHEMA = 23`, `TailKind::FILTER = 10`, `kA2cFilterSchema = 24`.
  - The client names are consistent: `SubscribeOptions::filter`, `GroupConfig::filter`, `SubscriptionFailReason::BrokerTooOld = 9`, `GroupFailReason::FilterMismatch = 6`.
- **Review Focus:** all five lines have a named test in their owning task (Tasks 2, 3, 7, 8).
- **Task count:** 17. Each is sized for one implementer session. Tasks 7, 8 and 9 are the largest, because their interfaces are fully specified above.

## Spec questions ruled (binding; recorded so the reviewer can overturn any of them)

1. **Legacy worker mode:** filters are refused there (FD1), and filed as an issue.
2. **The QL `WHERE` clause:** deferred, because `ql/` has no statement to attach it to (SD13). The builder ships.
3. **The docker MMORPG gate:** relative to unfiltered at the same load, plus the absolute bytes gate; the absolute 50/200 µs gate is held in-process (SD14, FD12). The spec's 1M events/s exceeds the box.
4. **Refusal details** travel as a v10-only trailing field on `SubscribeResponse` / `JoinGroupResponse` (SD5, FD9).
5. **Tombstones** (null values) never match, and are counted (FD7).
6. **An unknown writer version** stalls for up to `RUNE_FILTER_VERSION_WAIT_MS` (5 s), then is excluded, counted and logged (SD9, FD6).
7. **`AllPartitions`** returns a `SubscriptionSet` of per-partition `Subscription`s, with `Record::partition` appended (FD10, SD15).
8. **Value types:** integral literals are accepted for integral fields, with the range checked at encode time. Float, string and bool mismatches do not compile (FD8).
9. **FixedStr literals** are zero-padded to the declared width, as `fly::storeFixedStr` stores them (FD8).
10. **A schema field named `Filter`** makes the generator throw (FD8).
11. **Group fetch subscriptions'** filter bytes are not cross-checked by the broker against the group's; the client builds both from `GroupConfig` (FD11).
12. **Retired fields** get no generated member, while `FilterSchema` keeps their tags, so a filter from an older header still validates (and matches nothing in versions where its field is absent).
13. **Caps** are enforced at the gateway (the single registration point), and the worker reserves at the same caps (FD3).
14. **The byte-identity of unfiltered frames** wins over "v8 sends a zero length": the field is omitted (SD4).
15. **A new client against an old broker** is gated by the client on `brokerVersion` (SD3); there is no session refusal.
16. **The bytes-per-world-server metric** comes from a new `batchBytes` counter in the subscription status (FD13).

## Plan-review rulings (2026-10-20, binding; each overrides the task text it names)

- **PR1 (Task 7, pass cost).** `FilterGroup` keeps an intrusive list of the members that are not `InSync` (`Behind`, `VersionStall`). Pass step 1 walks only that list, and InSync members are never visited per pass. Task 7 adds a test that a pass with 500 InSync members and 0 off-sync members touches 0 member slots, through a pass counter. `BM_FilterWorkerPush` (Task 16) keeps its 10% flatness gate across 1/50/500 members.
- **PR2 (Task 8, push budget).** The unfiltered and filtered push passes share one `pushBudgetLeft_`. The worker alternates which pass runs first on each cycle (one `uint8_t` rotor owned by the worker), so neither pass starves under the other's load. Task 8 adds `FilteredPushNotStarvedByUnfilteredLoadRegression`: a saturated unfiltered subscriber plus one filtered one, where the filtered one is served within 2 cycles.
- **PR3 (Tasks 7–8, group identity).** The group key is (topic, partition, isolation, replica `incarnation`). `FilterGroup::add` asserts in debug and refuses in release, counted, if the member's `logPtr` or replica differs from the group's scan source. Task 8 adds a test where a re-bound replica with a new incarnation forms a new group.
- **PR4 (Tasks 7–8, cursor readers).** Every reader of a filtered member's position (the moved note, ownership loss, offset commit) goes through `cursorOf()`, and no reader uses `WorkerSubscription::nextOffset` directly for a filtered member. Task 8 greps this in review.
- **PR5 (Task 9, subscribe racing ALTER).** A tag the gateway's installed FilterSchema doesn't know yet is refused with `UnknownTag` and its detail text. The client retries after its schema refresh. This is intended. Task 9 adds `SubscribeRacingAlterUnknownTagThenAcceptedRegression`: refused before the install, accepted after it.
- **PR6 (Tasks 2 and 7, hard rule 3).** `Match<T>` carries one `enum class State : uint8_t { Ok, Overflow, OutOfRange }` instead of two booleans. `FilterGroup` member slots are free or used through a free-index list, with no `live` boolean.
- **PR7 (Task 16/17, MMORPG gate).** The substitution in SD14/FD12 is a change to the spec's gate and needs the user's confirmation. It isn't a ruling: the absolute 50 µs / 200 µs class is gated in-process, and the docker run is gated relative to unfiltered. Until the user confirms, Task 17 also records the absolute docker p50/p99 at the scenario load, as information and not a gate.
