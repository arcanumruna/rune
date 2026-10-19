# Milestone C-C — Log Compaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Rune key-based log compaction (`cleanup.policy=compact`) — retain the latest record per key — by wiring the already-built-but-dormant Phase-4 engine (`LogCleaner`, `OffsetMap`, tombstones, producer-state preservation, offset-preserving crash-safe segment rewrite) into the duty cycle and re-keying the internal topics so `__consumer_offsets` / `__transaction_state` stay bounded.

**Architecture:** `LogCleaner` runs as a **bounded, leader-only background pass** on the node duty cycle: a dirty-ratio trigger selects a compact-policy log, compacts **sealed (non-active) segments only** (single-writer safe), keeps latest-per-key via `OffsetMap`, preserves original offsets (gaps ok — the read path already tolerates them), preserves the last batch per producerId (so MC-B idempotent dedup survives), and drops tombstones after a delete-retention window. Compaction is per-replica and deterministic ⇒ replicas converge with no cross-node coordination.

**Tech Stack:** C++23, GoogleTest, Google Benchmark, Aeron, SBE-style flat LE codecs (schema 120), pytest + Docker (dr-e2e). Repos: `rune-broker`, `rune-protocol` (only if a record-key wire tweak is needed).

**Spec:** `docs/superpowers/specs/2026-09-16-rune-milestone-c-c-log-compaction-design.md`

> **Reality check from codebase exploration (informs CP-1):** `RecordBatch`/`RecordBatchBuilder::append(key, value, …)` **already encode a record key** (varint-length-prefixed, `RecordBatch.h:92–139`). `LogCleaner` (`storage/LogCleaner.h`, `.cpp` ~422 lines) and `OffsetMap` are **complete and tested** (16 gated integration tests in `libs/coordination/tests/integration/CompactionIntegrationTest.cpp` + storage `LogCleanerTest.cpp`), including tombstones, producer-state preservation, offset preservation, atomic-rename crash-safety. The ONLY reason internal topics can't compact today is that `ReplicatedOffsetStore` writes an **empty record key** with identity in the value (`ReplicatedOffsetStore.cpp:24`). So the spec's "add `append(key,value)` at the replica layer" prerequisite is really **(a) re-key the internal topics through the existing key-capable builder, and (b) plumb a key through the `ReplicatedOffsetStore`/replica callsite** — NOT a deep new encoder. This plan is therefore mostly wiring + config + enabling existing tests. Everything the spec asks for functionally still holds.

## Global Constraints

- Commit to **local `main` only — never push** (unless the user explicitly asks).
- **Never** `conan create` / `export` / `upload` — packages are CI-owned (a PreToolUse hook enforces this).
- Any benchmark **p99 > 100µs on the append hot path = STOP and fix** — compaction is background but MUST NOT perturb append p99/p999 while a pass runs.
- **Single-writer duty-cycle discipline:** compaction runs only on the duty-cycle thread, **leader-only**, **sealed segments only** (never the active segment the leader appends to); bounded work per cycle (byte/segment budget); clock injected (`nowNs`) once per cycle; no locks/blocking on the append path.
- **Per-replica independent + deterministic:** each replica compacts its own log; identical sealed input ⇒ identical output ⇒ replicas converge with no cross-node message. Compaction only rewrites committed, below-HW segments — HW/LEO/replication model untouched.
- **Preserve idempotent-producer state:** compaction must keep the last batch per producerId so MC-B dedup survives (already implemented — verify + test).
- Every task ends green (unit/integration), is committed, and — where it maps to a CP ticket — updates Planka (move card, comment) and pushes its test run to live via `scripts/rune-live.sh test <suite> passed passed=.. failed=.. total=.. commit=..`.
- Build with CLion's bundled cmake + `conan_host_profile` (x86_64 / cppstd=23 / Debug): `export PATH="/Applications/CLion.app/Contents/bin/cmake/mac/aarch64/bin:$PATH"` then build in `rune-broker/build/` (Unix Makefiles, `BUILD_TESTING=ON`; enable `BUILD_COORDINATION_INTEGRATION_TESTING=ON` for the gated compaction tests).
- **Model policy:** Sonnet implements each task; Opus reviews (safety-critical — always Opus). Opus finds an issue → Sonnet fixes (first attempt). Sonnet fails / regresses / introduces a new issue → **Opus takes over that corrective fix**, then re-review.
- **Faithful-Aeron / Kafka-faithful** discipline: mirror existing patterns; do not re-implement `LogCleaner`/`OffsetMap` — wire them. Opus review gates on allocation + latency + correctness.

---

## File Structure

**rune-broker**
- `libs/node/include/rune/node/NodeConfig.h` (`TopicSpec`, lines 48–52) — MODIFY: add `cleanupPolicy`, `retentionMs`, `retentionBytes`, `deleteRetentionMs`, `minCleanableDirtyRatio`.
- `libs/gateway/include/rune/gateway/TopicRegistry.*` — MODIFY: carry per-topic cleanup config from `TopicSpec`.
- `libs/coordination/src/ReplicatedOffsetStore.cpp` (line ~24) — MODIFY: write a **real record key** `(group,topic,partition)` via the key-capable batch builder (mark the topic `cleanup.policy=compact`).
- `libs/coordination/src/ReplicatedTxnStateStore.cpp` (from MC-B TX-3) — MODIFY: write a real record key `transactionalId`.
- `libs/replication/include/rune/replication/PartitionReplica.h` (line 194) — MODIFY only if the offset/txn-state store cannot set a key through the current pre-built-batch `append`; otherwise leave `append` as-is and set the key at batch-build time.
- `libs/node/include/rune/node/NodeRuntimeAgent.h` (`doWork`, lines 104–144) + `src` — MODIFY: slot a bounded leader-only compaction pass after the gateway step (6.5); add `runBoundedCompaction(nowNs)`.
- `libs/storage/LogCleaner.*`, `OffsetMap.*` — REUSE (no re-implementation); confirm signatures.
- `libs/coordination/tests/integration/CompactionIntegrationTest.cpp` — ENABLE (`BUILD_COORDINATION_INTEGRATION_TESTING=ON`) + extend.
- `libs/node/tests/integration/CompactionDutyCycleTest.cpp` — CREATE.
- `benchmark/CoordinationBenchmark.cpp` (`BM_LogCleaner_Compact`, lines 380–445) + `LogBenchmark.cpp` — EXTEND: throughput sweeps + append-during-compaction gate.

**dr-e2e**
- `harness/driver.py` / `dr_client` — MODIFY: a compaction scenario (produce many updates per key → trigger/await compaction → consume latest).
- `tests/test_compaction.py` — CREATE.

---

## CP-1 — Re-key internal topics + per-topic cleanup config

### Task 1: `TopicSpec` cleanup + retention config

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (`TopicSpec`, lines 48–52) + `libs/gateway/TopicRegistry.*`.
- Test: `libs/node/tests/NodeConfigTest.cpp` + `libs/gateway/tests/TopicRegistryTest.cpp`.

**Interfaces:**
- Produces:
  ```cpp
  struct TopicSpec {
    std::string name;
    int32_t     partitionCount = 1;
    int32_t     topicId        = -1;
    enum class CleanupPolicy : uint8_t { Delete = 0, Compact = 1, CompactDelete = 2 };
    CleanupPolicy cleanupPolicy       = CleanupPolicy::Delete;
    int64_t       retentionMs         = -1;        // -1 = unbounded
    int64_t       retentionBytes      = -1;        // -1 = unbounded
    int64_t       deleteRetentionMs   = 86'400'000;// tombstone retention (1 day)
    double        minCleanableDirtyRatio = 0.5;    // dirty-ratio compaction trigger
  };
  ```
  `TopicRegistry` exposes `CleanupPolicy cleanupPolicyOf(topicId)`, `double dirtyRatioThresholdOf(topicId)`, `int64_t deleteRetentionMsOf(topicId)`.

- [ ] **Step 1: Failing tests** — a `TopicSpec` defaults to `Delete`/unbounded; a spec with `Compact` round-trips through `TopicRegistry` (`cleanupPolicyOf(id) == Compact`, thresholds returned); invalid policy string (if parsed from config) rejected.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the `TopicSpec` fields + `TopicRegistry` accessors (thread through wherever `TopicSpec` is consumed at topic creation).
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(node+gateway): [CP-1] per-topic cleanup.policy + retention/dirty-ratio config`.

### Task 2: Re-key `__consumer_offsets` (and `__transaction_state`) records

**Files:**
- Modify: `libs/coordination/src/ReplicatedOffsetStore.cpp` (record build, ~line 24 where `record.key = ""`) — build the record with a **real key** via `RecordBatchBuilder::append(key, value, …)` (`protocol/RecordBatch.h:92`); the key = `groupLen|group|topicLen|topic|partition` (the identity currently in the value). Mark `__consumer_offsets` as `cleanup.policy=compact` at creation.
- Modify (if MC-B landed): `libs/coordination/src/ReplicatedTxnStateStore.cpp` — key = `transactionalId`; mark `__transaction_state` `compact`.
- If `PartitionReplica::append` (line 194, pre-built-batch bytes) cannot carry the key end-to-end, add the minimal key plumbing; otherwise set the key at build time and leave `append` unchanged.
- Test: `libs/coordination/tests/coordination/ReplicatedOffsetStoreTest.cpp` (extend).

**Interfaces:**
- Consumes: `rune::protocol::RecordBatchBuilder::append(std::span<const uint8_t> key, std::span<const uint8_t> value, …)` (`RecordBatch.h:92`); the record-key decode used by `LogCleaner`/`OffsetMap`.
- Produces: `ReplicatedOffsetStore` unchanged public API (`commit`/`fetch`/`fetchAll`/`rebuildFromLog`) but now writes a non-empty record key. `rebuildFromLog` must still fold correctly (last-write-wins per key) whether the key is read from the record key or the value (keep value decode for backward-compat with pre-CP-1 records).

- [ ] **Step 1: Failing test**
```cpp
TEST(ReplicatedOffsetStore, RecordCarriesCompactionKey) {
    auto replica = makeSingleNodeReplica(0,0);
    rune::coordination::ReplicatedOffsetStore store(replica.get());
    ASSERT_TRUE(store.commit("g", {1,0}, 42, "", 1));
    driveHwAdvance(*replica);
    // The last appended record's KEY equals (group,topic,partition), not empty.
    auto keyBytes = lastRecordKey(*replica);        // test helper reading the tail record's key
    EXPECT_FALSE(keyBytes.empty());
    EXPECT_EQ(decodeOffsetKey(keyBytes), (OffsetKey{"g",1,0}));
}
TEST(ReplicatedOffsetStore, RebuildStillLastWriteWinsWithRealKey) {
    auto replica = makeSingleNodeReplica(0,0);
    { rune::coordination::ReplicatedOffsetStore s(replica.get());
      s.commit("g",{1,0},42,"",1); s.commit("g",{1,0},99,"",2); driveHwAdvance(*replica); }
    rune::coordination::ReplicatedOffsetStore s2(replica.get()); s2.rebuildFromLog();
    ASSERT_TRUE(s2.fetch("g",{1,0})); EXPECT_EQ(s2.fetch("g",{1,0})->offset, 99);
}
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the real-key build in `ReplicatedOffsetStore` (and `ReplicatedTxnStateStore` if present); mark internal topics `compact`.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(coordination): [CP-1] re-key __consumer_offsets/__transaction_state for compaction`.
- [ ] **Step 6:** **Planka** CP-1 → Done; **live** push.

---

## CP-2 — Wire `LogCleaner` into the duty cycle (bounded, leader-only)

**Files:**
- Modify: `libs/node/include/rune/node/NodeRuntimeAgent.h` (`doWork` lines 104–144) + `src` — add step 6.5 `runBoundedCompaction(nowNs)`.
- Reuse: `libs/storage/LogCleaner.h` (confirm the exact `clean` signature; the benchmark callsite is `cleaner.clean(log, 0, hw, ts, 0)` → `clean(Log&, Offset startOffset, Offset endOffset, int64_t nowNs, int64_t tombstoneRetentionMs)`), `OffsetMap`.
- Enable: `BUILD_COORDINATION_INTEGRATION_TESTING=ON` (the 16 gated compaction integration tests).
- Test: `libs/node/tests/integration/CompactionDutyCycleTest.cpp`.

**Behavior (`runBoundedCompaction`):** only when `isLeader`; iterate topics with `cleanupPolicyOf(topicId) != Delete`; for each such partition Log, compute dirty ratio; if `> minCleanableDirtyRatio` and there is ≥1 **sealed** segment, run `LogCleaner::clean` over the **sealed** range only (exclude the active segment), bounded to one segment (or a byte budget) per cycle; use `deleteRetentionMsOf(topicId)` as the tombstone-retention arg; sample `nowNs` once. Never touch the active segment. Return work count (>0 if a pass ran) so the duty cycle stays responsive.

- [ ] **Step 1: Failing integration test**
```cpp
TEST(CompactionDutyCycle, CompactsSealedSegmentsLeaderOnlyLatestPerKey) {
    auto node = makeSingleNodeRuntime();                 // leader
    auto* log = node.createCompactTopic("t", /*parts=*/1);// cleanup.policy=compact
    for (int v=0; v<200; ++v) appendKeyed(log, "k1", v); // 200 updates, one key → many dirty
    rollActiveSegment(log);                               // seal the dirty segment
    driveDutyCycles(node, /*cycles=*/50);                // runBoundedCompaction fires
    EXPECT_EQ(latestValueForKey(log, "k1"), 199);        // latest kept
    EXPECT_LT(liveRecordCount(log), 200);                // superseded reclaimed
    EXPECT_TRUE(activeSegmentUntouched(log));            // active never compacted
}
TEST(CompactionDutyCycle, FollowerDoesNotCompactOnLeaderCommand) {
    // a follower node's runBoundedCompaction is a no-op driven by leadership flag,
    // but it DOES compact its own log independently & deterministically (covered in CP-4)
}
```
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `runBoundedCompaction` + the `doWork` step-6.5 wiring; confirm the `LogCleaner::clean` signature against `LogCleaner.h` and adjust the call.
- [ ] **Step 4: Run** → PASS. Enable + run the gated `CompactionIntegrationTest.cpp` suite → all green.
- [ ] **Step 5: Commit** — `feat(node): [CP-2] wire LogCleaner as bounded leader-only duty-cycle compaction pass`.
- [ ] **Step 6:** **Planka** CP-2 → Done; **live** push.

---

## CP-3 — cleanup.policy enforcement + tombstones + delete-retention

**Files:** Modify `runBoundedCompaction` (route by policy) + `libs/storage/Log.cpp` (`deleteOldSegments`, lines 202–214) for `compact,delete`; tests `libs/node/tests/integration/CompactionPolicyTest.cpp`.

**Behavior:** `Delete` → existing retention only (no compaction). `Compact` → compaction only (no size/time delete of compacted keys). `CompactDelete` → compaction pass, then normal retention ages out old segments (compaction runs first). Tombstone (null value) deletes the key; `LogCleaner` retains it for `deleteRetentionMs` then drops it (already implemented — drive the config through). Producer-state batches are preserved even under `CompactDelete` retention (precedence within the pass).

- [ ] **Step 1: Failing tests** — a `Compact` topic never size-deletes a still-live key even past `retentionBytes`; a tombstone deletes the key after `deleteRetentionMs` but is retained before it; `CompactDelete` both compacts and ages out; a `Delete` topic never compacts. Producer-state batch survives a `CompactDelete` pass.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** the policy routing + `deleteRetentionMs` plumbing + `compact,delete` ordering.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `feat(node+storage): [CP-3] cleanup.policy routing + tombstone delete-retention + compact,delete ordering`.
- [ ] **Step 6:** **Planka** CP-3 → Done; **live** push.

---

## CP-4 — Correctness interactions (producer state, replication, recovery)

**Files:** Tests `libs/node/tests/integration/CompactionCorrectnessTest.cpp` + `libs/coordination/tests/integration/CompactionIntegrationTest.cpp` (extend); product fixes only if a test exposes a gap.

**Behavior verified:** (a) idempotent-producer dedup (MC-B `ProducerStateManager`) still rejects a duplicate after the partition was compacted (last batch per producerId preserved); (b) per-replica deterministic compaction — two replicas given the same sealed input produce byte-identical retained records (offsets preserved, no reorder); (c) internal-topic recovery — rebuild `__consumer_offsets` (and `__transaction_state`) from a **compacted** log yields the same committed offsets / txn state as before compaction; (d) HW/LEO unchanged across a compaction pass (compaction only rewrites below-HW sealed segments).

- [ ] **Step 1: Failing tests**
```cpp
TEST(CompactionCorrectness, IdempotentDedupSurvivesCompaction) {
    // pid=9 appends seq 0..99 (interleaved with other keys) → compact → retry seq 50
    // → Duplicate (prior offset), NOT re-appended
}
TEST(CompactionCorrectness, DeterministicAcrossReplicas) {
    // feed identical sealed segments to two LogCleaners → identical retained (offset,key,value) set
}
TEST(CompactionCorrectness, OffsetStoreRebuildFromCompactedLog) {
    // commit many offsets for (g,tp) → compact __consumer_offsets → rebuildFromLog → latest offset intact
}
TEST(CompactionCorrectness, HwUnchangedByCompaction) {
    // record HW, run a compaction pass over sealed segments, HW unchanged
}
```
- [ ] **Step 2: Run** → FAIL (or PASS if already covered — then assert coverage & add missing cases).
- [ ] **Step 3: Fix** any gap the tests expose (producer-state preservation, determinism, recovery). If `LogCleaner` already satisfies all, no product change — the tests lock the guarantees.
- [ ] **Step 4: Run** → PASS.
- [ ] **Step 5: Commit** — `test(node+coordination): [CP-4] compaction correctness (producer-state, determinism, recovery, HW)`.
- [ ] **Step 6:** **Planka** CP-4 → Done; **live** push.

---

## CP-5 — DR-E2E + benchmarks + full-area review

**Files:** Modify `dr-e2e/harness/driver.py` + `dr_client` (compaction scenario); create `dr-e2e/tests/test_compaction.py`; extend `benchmark/CoordinationBenchmark.cpp` (`BM_LogCleaner_Compact`) + `LogBenchmark.cpp`.

- [ ] **Step 1: Add a compaction mode to `dr_client`/`driver.py`** — produce many updates per key, trigger/await a compaction pass, then consume and print `{key, latestValue, count}` for the parser.
- [ ] **Step 2: `test_compaction.py`** (matrix: nodes ∈ {1,3,5}): latest-per-key kept + superseded gone + tombstone deletes the key + **original offsets preserved**, and the compacted state **survives restart AND leader-kill/follower-kill failover**; replicas byte-consistent on retained records; internal-topic recovery (offsets/txn-state rebuilt from compacted logs). Producer-state-through-compaction EOS variant (if MC-B present). Run → PASS.
- [ ] **Step 3: Smoke** subset — single + 3-node bring-up with a compact topic, produce→compact→consume-latest happy path, clean shutdown — as a fast pre-flight; wire into the suite.
- [ ] **Step 4: Benchmarks** (Release): compaction throughput (MB/s, records/s) across dirty-ratio + key-cardinality sweeps; `OffsetMap` build cost vs segment size/key count; per-cycle bounded-work budget honored (no duty-cycle stall); **append p99/p999 unperturbed while a compaction pass runs** (the key gate) with zero-alloc on the append path asserted.
- [ ] **Step 5: Gate check** — any append-path p99 > 100µs while compacting → STOP, root-cause (bounded-work budget / segment size), fix, re-measure < 100µs. Record numbers. Run the DR-E2E set on a **fresh `--no-cache` build**.
- [ ] **Step 6: Full-area Opus review** of all CP changes (config + coordination re-key + node duty-cycle + storage) against: single-writer + sealed-segments-only + leader-only, per-replica determinism, offset preservation (no reorder/renumber), producer-state preservation, tombstone/delete-retention correctness, internal-topic recovery, HW/LEO non-perturbation, append-latency non-regression. Fix findings to **CLEAN** (Sonnet first; escalate any Sonnet-introduced regression to Opus).
- [ ] **Step 7: Commit** — `test(dr-e2e)+bench: [CP-5] compaction DR-E2E + benchmark matrix + full-area review CLEAN`. **Planka** CP-5 → Done; **live** push `dr-e2e/compaction passed` + bench numbers (`kind=benchmark`).

---

## Self-review (author checklist — completed)

- **Spec coverage:** every spec §4 component maps to a task — prerequisite/re-key §4.1 → CP-1; `LogCleaner` wiring §4.2 → CP-2; tombstones + `cleanup.policy` §4.3 → CP-3; correctness interactions §4.4 → CP-4; DR-E2E + benchmarks §4.5/§9 → CP-5. Failover/recovery (§6) covered by CP-4 recovery + CP-5 DR-E2E. Milestone-D seam (§7) — nothing to do (per-replica, no coordination) — noted in CP constraints. Edge cases (§8: active-segment-never-compacted, tombstone-only key, `compact,delete` ordering, no-offset-shift, dirty-ratio thrash guard) covered across CP-2/CP-3/CP-4.
- **Reality correction:** CP-1 narrowed because `RecordBatch` already carries keys and `LogCleaner`/`OffsetMap` are complete + tested; the spec §4.1 framing is corrected in the spec doc (see note at top). Functional scope unchanged.
- **Placeholder scan:** no TBD/TODO; each code step carries real test/impl content or an exact interface signature grounded in explored file:line surfaces. `LogCleaner::clean` signature to be confirmed against `LogCleaner.h` in CP-2 Step 3 (benchmark callsite gives the shape).
- **Type consistency:** `TopicSpec::CleanupPolicy{Delete,Compact,CompactDelete}` + `TopicRegistry::{cleanupPolicyOf,dirtyRatioThresholdOf,deleteRetentionMsOf}` consistent across CP-1/CP-2/CP-3; `runBoundedCompaction(nowNs)` consistent across CP-2/CP-3; `ReplicatedOffsetStore` public API unchanged across CP-1/CP-4.

## Open items to confirm during execution (from spec §11)
- `OffsetMap` sizing/eviction: fixed-capacity + multi-pass vs full-log map — confirm `OffsetMap` behavior in CP-2 and choose the per-pass segment budget accordingly.
- Producer-state preservation mechanism: `LogCleaner` already patches producer fields per record — CP-4 locks it with a test; confirm it matches Phase-1 producer-state snapshot semantics.
- Delete-retention accounting: wall-clock vs offset-lag horizon — CP-3 uses `deleteRetentionMs` (wall-clock via injected `nowNs`), consistent with existing retention.
- `compact,delete` interleave vs compaction-then-delete — CP-3 chooses compaction-then-delete (separate ordering) for v1 simplicity.
