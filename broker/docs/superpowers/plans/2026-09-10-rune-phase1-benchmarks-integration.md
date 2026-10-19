# Rune Phase 1 — Benchmarks + Integration Tests Plan

**Goal:** Comprehensive Google-Benchmark suites for every Phase-1 component (dozens of benchmarks) + an extensive multi-component integration-test suite, following the `world-server` benchmark/integration patterns. Focus: performance visibility + code quality. Autonomous execution — no user review required.

**Pattern (from world-server):**
- Benchmarks: one `<proj>_benchmark` executable, `benchmark/BenchmarkMain.cpp` = `BENCHMARK_MAIN()`, one `*Benchmark.cpp` per component with `static void BM_<Component>_<Case>(benchmark::State&)` + `BENCHMARK(...)`; realistic workloads documented in comments; `benchmark::DoNotOptimize`/`ClobberMemory` to prevent dead-code elimination; `BUILD_BENCHMARKS` option; release-tuned.
- Integration: separate `<proj>_integration_tests` gtest executable, `tests/integration/IntegrationTestBase.h` fixture (unique temp dirs, helpers), per-scenario `*IntegrationTest.cpp`; `BUILD_INTEGRATION_TESTING` option (no Docker needed for Phase 1 — real filesystem only).

**Global constraints:** C++23; cmake ≥3.31; cmake not on PATH (use Conan's cmake); rune-protocol consumed via `${RUNE_PROTOCOL_INCLUDE}`; `aeron/1.41.4`; NEVER run `conan create/export/upload`; every benchmark must actually exercise the code (DoNotOptimize) with realistic MMORPG-shaped small messages; every integration test must assert real behavior.

---

## Part A — rune-protocol benchmarks (`rune_protocol_benchmark`)
`benchmark/` dir + `BUILD_BENCHMARKS` block in rune-protocol/CMakeLists.txt.
- **Crc32cBenchmark.cpp:** BM_Crc32c_OneShot at 16B/64B/256B/1KiB/64KiB/1MiB; BM_Crc32c_Incremental (split into 8 chunks); BM_Crc32c_SmallMessage (48B MMORPG-shaped, throughput in items/s).
- **VarintBenchmark.cpp:** BM_Varint_WriteSmall (0..127), BM_Varint_WriteLarge (INT64 extremes), BM_Varint_ReadSmall, BM_Varint_ReadLarge, BM_Varint_RoundTripStream (100 values).
- **RecordBatchBenchmark.cpp:** BM_Batch_Build_1Record, _10Records, _100Records; BM_Batch_EnsureValid (1/100 records); BM_Batch_ForEach_100Records; BM_Batch_StampLeaderEpoch; BM_Batch_BuildParseRoundTrip_SmallMessage (MMORPG 48B key+value).

## Part B — rune-broker storage benchmarks (`rune_broker_benchmark`, expand existing)
Split existing `benchmark/StorageBenchmark.cpp` into per-component TUs + BenchmarkMain.cpp.
- **Crc/uses via storage**; **MappedFileBenchmark:** BM_MappedFile_SequentialWrite (4KiB pages), BM_MappedFile_Sync.
- **OffsetIndexBenchmark:** BM_OffsetIndex_Append; BM_OffsetIndex_LookupWarm (tail), BM_OffsetIndex_LookupCold (early), across index sizes (1K/100K entries).
- **LogSegmentBenchmark:** BM_LogSegment_AppendBatch (single-record MMORPG msg), BM_LogSegment_ReadAtOffset, BM_LogSegment_Recover (N valid batches).
- **LogBenchmark:** BM_Log_AppendWithRolling (small segments, many rolls), BM_Log_ReadAcrossSegments, BM_Log_Recover_MultiSegment.
- **RetentionBenchmark:** BM_Retention_DeleteOldSegments (many segments).
- **PartitionApiBenchmark:** BM_PartitionApi_Produce (durable single-record), BM_PartitionApi_Consume (N records), BM_PartitionApi_ProduceConsumeRoundTrip, BM_PartitionApi_ProduceThroughput (items/s).
- **LeaderEpochCacheBenchmark:** BM_LeaderEpochCache_Assign, BM_LeaderEpochCache_TruncateLookup (many epochs).
- **ProducerStateBenchmark:** BM_ProducerState_ValidateContiguous, BM_ProducerState_ManyProducers.

## Part C+D — rune-broker integration tests (`rune_storage_integration_tests`)
`tests/integration/IntegrationTestBase.h` (unique temp dir per test, helpers to produce N, consume-all, corrupt bytes, count on-disk segments) + files:
- **EndToEndProduceConsumeIntegrationTest.cpp:** 10k+ records across many segments; consume-all returns exactly N with contiguous offsets 0..N-1 and byte-exact values; consume from many mid offsets; binary/empty/large values.
- **RestartRecoveryIntegrationTest.cpp:** produce→destroy→reopen+recover→verify all N and HW; multiple restart cycles; produce-after-restart continues contiguously; interleaved produce/restart.
- **CrashTornTailIntegrationTest.cpp:** truncate/append-junk to the active segment's log at several positions; recover; assert only complete committed batches survive, no partial/duplicate, earlier segments intact.
- **RetentionIntegrationTest.cpp:** produce lots→deleteOldSegments→surviving offsets still consumable, deleted offsets return empty, totalBytes==on-disk sum, active segment always kept, produce continues after retention.
- **RollingStressIntegrationTest.cpp:** tiny segmentRollBytes, tens of thousands of records, random-offset reads all valid, segment count matches, recover after stress reproduces identical log.
- **MixedWorkloadIntegrationTest.cpp:** interleave produce/consume/retention/recover in a loop; invariants hold throughout (offsets monotonic, no loss of retained data, HW consistent).

## Execution & exit
Per part: implementer subagent → quality review (benchmarks genuinely exercise code + realistic; integration tests assert real behavior) → fix loop → green. Final: build both `*_benchmark` executables and run each once (`--benchmark_min_time=0.01s`) to confirm they run; run the full integration suite; confirm unit suites still green. Autonomous — no user checkpoint.
