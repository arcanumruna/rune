# Rune — arcanum-metrics (Phase 0) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** arcanum-metrics becomes an Aeron-CountersManager-style metrics file:
- single-writer slots (a plain add and a release store, acquire reads);
- an injected clock and no threads of its own;
- allocate and free with a registration id, on one owner thread, returning error codes and allocating nothing;
- a capacity fixed at start, with a refusal to start when it is too small;
- rates and windowed max/avg computed by the reader from totals.

Every Rune metric that is an Aeron counter today (the `RoleCounter.h` type ids 1'000'201..1'000'241) then moves onto
it, keeping its name, type id, label and writer thread. `cnc.dat` keeps only the media driver's own counters.
`rune_role`, the dr-e2e harness, the scripts, the benches, the tests and `docs/operations.md` move with it, and
`rune_role` keeps its output and exit codes. Phase 2 builds the per-namespace blocks on these primitives.

**Architecture:**
- **Library (repo `arcanumruna/arcanum-metrics`).** The headers stay under `include/metrics/`.
  - `MetricsLayout.h`: the file layout (a header, a metadata area of 512-byte records, a value area of 128-byte
    records), mirroring Aeron's `CountersManager`.
  - `MetricsFile.h`: safe create (tmp, size, header, version word last, rename), reader open and capacity sizing.
  - `MetricsWriter.h`: the owner-thread allocator (allocate, free, state word, regId, label) and `SlotWriter`, the
    single-writer handle (add, set) given to the slot's writer thread.
  - `MetricsReader.h`: acquire scans, slot-reuse detection by regId, `RateTracker` and `WindowSampler`.
  - `SystemMetrics.h`: process stats read by `doWork(nowNs)` on the owner's duty cycle.
  - The old `CounterMetric`, `GaugeMetric`, `ThroughputMetric`, `WindowedMaxGauge`, `WindowedAvgGauge` and
    `MetricsRegistry` are removed (D51).
- **Rune.**
  - `RoleCounter.h` becomes `RuneMetrics.h`: a compile-time `RuneMetric` enum with the same type ids and labels.
  - `BrokerNode` owns one `MetricsWriter`. It allocates every node metric at start, before any agent thread starts,
    and hands each `SlotWriter` to its writer thread (control, observer or provisioner) at construction.
  - The lazy `findCounter` resolution goes away (D59).
  - `rune_role` reads the metrics file.
  - The library comes in like the flight recorder: `ARCANUM_METRICS_DIR`, or a FetchContent pinned to a commit, or a
    docker named context `arcanum-metrics` (D60).

**Tech Stack:** C++23 header-only library plus GoogleTest and Google Benchmark in arcanum-metrics; CMake/Ninja;
GitHub Actions in arcanum-metrics (gcc-13 and clang; Debug, ASan+UBSan, TSan); Rune's existing test rigs; docker
compose (3 nodes) on the shared Linux test VM for dr-e2e.

**Spec:** `broker/docs/superpowers/specs/2026-10-24-rune-namespaces-and-truncate-design.md` (approved 2026-10-26).
This plan covers §5.3 (the library changes, the capacity sizing and moving every Rune metric) and §7 Phase 0. The
per-namespace half of §5.3 (`NsSlotTable`, the assign/release ring messages, `RUNE_MAX_NAMESPACES`, regId = nsId,
the mismatch metric) is Phase 2 Task 8, which needs this plan merged (Phase 2 D31). The Self-review maps each spec
section to its task.

**Builds on / order:** nothing; this plan can run before or in parallel with Phase 1. What exists today (line
numbers are hints; use the quoted anchors):

- **arcanum-metrics** (`main`, a single commit, `VERSION.txt` = `1.0.4-snapshot`):
  - `include/metrics/Metrics.h` (~798 lines):
    - `MetricType` (~:33, `COUNTER=1 .. WINDOWED_AVG=6`), `MetricMeta` (~:43), `MetricsHeader` (~:52),
      `ValueSlot` (~:66);
    - `CounterMetric` (~:84, `fetch_add`), `GaugeMetric` (~:107);
    - `ThroughputMetric` (~:156, its own `steady_clock` at ~:284–289);
    - `WindowedMaxGauge` (~:312) and `WindowedAvgGauge` (~:425), each with a `std::mutex` and a ms sample buffer;
    - `MetricsRegistry` (~:525): `create` (~:528) takes a `std::string` path, opens with `O_TRUNC` and retries a
      `flock`; `open` (~:598) takes a shared flock.
  - `include/metrics/SystemMetrics.h` (~167 lines): starts its own `std::thread` (~:49) and reads `/proc/self/fd`,
    `task` and `statm`.
  - `src/dummy.cpp`; CMake project `metrics` with the INTERFACE target `metrics`, the static `metrics_dummy` and the
    export set `metricsTargets`; `conanfile.py`; `build.sh`, `release.sh`, `bump_version.py`, `update-deps.sh`
    (these run conan locally and are never run in this plan); a README of ~540 lines whose exporter API is at
    ~:285.
  - No tests, no benchmarks, no CI.
  - Rule violations: `fetch_add` and CAS as coordination, a mutex, `steady_clock`, `std::string` and exceptions
    on the write path, no free, `O_TRUNC` plus a flock wait loop, and a thread of its own.
- **Rune counters** (`broker/libs/node/include/rune/node/RoleCounter.h`): type ids 1'000'201..1'000'241 (236–239
  reserved); `RoleCode {None=-1, Follower=0, Candidate=1, Leader=2}`; `kRoleKeyLength = 4` (the node id); labels
  like `"rune-logins-ok node=<id>"`.
- **Allocation and writers** (`broker/libs/node/src/BrokerNode.cpp`, `BrokerNode.h`):
  - Allocation: `aeron_->addCounter(typeId, roleKey, kRoleKeyLength, label)` at ~:515–577 and ~:632. The
    `RegId`/`Counter` member pairs sit at ~:2516–2647 and ~:2868–2875 in the header; `storageCounterRegIds_` and
    `storageCounters_` hold 4 each, `provCounterRegIds_` and `provCounters_` hold `kMetricCount = 5` each.
  - Lazy resolution: `findCounter` in try/catch, with `catalogMetricsPending_` / `securityMetricsPending_` checked
    at ~:5086–5087.
  - Writers on the **control thread**: `publishCatalogMetrics_` (~:3284), the security metrics (~:3455), the peer
    links (~:3585), storage (~:3812), and the txn coordinator sinks `publishStuckMarkers_` /
    `publishStuckTxnOffsets_` (set at ~:2525–2526).
  - Writer on the **observer/main 100 ms poll loop**: `publishRole` (~:3243: role, term, out-of-sync).
  - Writer on the **provisioner thread**: `publishProvisionMetric_` (~:3201, 5 metrics).
- **`broker/src/rune_role.cpp`:** reads `cnc.dat` and prints
  `ROLE nodeId=<n> role=<LEADER|FOLLOWER|CANDIDATE|NONE> term=<t>` and
  `STORAGE nodeId=<n> state=<...> heldLeaderRoles=<n> pendingNodes=<n> outOfSync=<n> lostCommittedReports=<n>`.
  Exit codes: 0 when found, 1 when the file is missing, 2 when the role metric is absent.
- **`rune_role` consumers** (their parsing stays valid because the output does not change):
  - `broker/dr-e2e/harness/cluster.py` (~:170, `parse_rune_role_storage` ~:181, `docker exec <c> rune_role`
    ~:405–433), `dockerutil.py`, `invariants.py`;
  - the dr-e2e tests `test_multi_leader_smoke`, `test_dr14_storage`, `test_leader_detection`;
  - `client-cpp/scripts/t15b-gates.sh`, `broker/bench/kafka-compare/cluster.sh` and `replbench.sh`.
- **Tests and rigs that read counters** through `CountersReader`, `RoleCounter.h` or `findCounter` (Task 11 greps for
  the full set; some matches may be comments only):
  - `RoleCounterTest`, `RoleCounterPublishTest`, `AuthLoginTest`, `D9CrossNodeMarkerTest`,
    `PeerSealedClusterTest`, `PeerLinkAgentTest`;
  - `AuthTlsBrokerRegressionTest`, `ClientLoginBrokerRegressionTest`, `ClientSessionRevokedBrokerRegressionTest`,
    `AuthClusterIntegrationTest`, `AuthInteractionMatrixRegressionTest`, `AuthLiveChangeTest`, `SecurityHaltTest`,
    `UdpGsoBindingsTest`, `WorkerSessionCryptoRegressionTest`;
  - `AuthTrio.h`, `AuthBroker.h`, `AuthMatrixRig.h`; `protocol/tests/protocol/RecordBatchTest.cpp`.
- **Unchanged:** `ping_probe.cpp` and `broker/scripts/check_single_driver_pumper.py`, which use `cnc.dat` only to see
  whether a driver exists.
- **Docs that mention counters:** `docs/operations.md` (`rune_role` ~:65–66; the Metrics section ~:86–118, which
  says "Metrics are Aeron counters ... `<data dir>/aeron/cnc.dat`, or `RUNE_AERON_DIR`", with the `RoleCounter.h`
  reference at ~:117), `broker/docs/CATALOG.md`, `broker/docs/SECURITY.md`, the release notes,
  `broker/docker/README.md`, `docs/api/admin-cli.md`.
- **Consumption pattern to mirror:** the flight recorder (`FLIGHT_RECORDER_DIR` plus FetchContent in
  `broker/CMakeLists.txt` ~:39–83; the Dockerfile's `COPY --from=flight-recorder include /flight-recorder/include` and
  `-DFLIGHT_RECORDER_DIR=/flight-recorder`).
- **Rune deps:** aeron 1.41.4, spdlog 1.14.0, gtest 1.16.0, benchmark 1.8.2, openssl 3.3.2. There is no metrics
  dependency yet.

## Execution context

- **Two repositories.**
  - Tasks 1–6 run in `arcanumruna/arcanum-metrics` on the branch `feature/single-writer-slots`, created off its
    latest `main`, and are pushed after every task. The branch merges through a PR reviewed by the most capable model
    (CLAUDE.md §6). Rune then pins the merge commit (Task 7).
  - Tasks 7–13 run in this repo on `claude/tender-wozniak-co3crp`. Before each task, merge `main` in if it moved
    (`git fetch origin main && git merge origin/main`). Push after every task with
    `git push -u origin claude/tender-wozniak-co3crp`.
  - Until the library PR merges, Rune builds against a local checkout with `-DARCANUM_METRICS_DIR=<checkout>`. The
    pin is set to the merge commit before checkpoint C.
- **Never run conan locally:** not `build.sh`, not `release.sh`, not `conan create/export/upload`. CI publishes
  packages. Rune does not consume the conan package in this plan (D60).
- **Builds.**
  - Library: `cmake -S . -B build -G Ninja -DMETRICS_BUILD_TESTS=ON -DMETRICS_BUILD_BENCH=ON`, plus `build-asan`
    (`-DMETRICS_SANITIZE=address,undefined`) and `build-tsan` (`-DMETRICS_SANITIZE=thread`).
  - Rune: see README.md#build and `docs/testing.md`:
    - `build` (Debug, `BUILD_TESTING=ON`, `BUILD_INTEGRATION_TESTING=ON`);
    - `build-asan` and `build-tsan` at the checkpoints;
    - `build-bench` (Release, `BUILD_BENCHMARKS=ON`) for Task 12.
  - Re-run cmake only after a `CMakeLists.txt` edit.
- **Test gate (CLAUDE.md rule 10).**
  - **Per task:** build, then run `ctest --test-dir build -R '<filter>' --output-on-failure -j8` with the filter
    named in the task's Step 2.
  - **Checkpoints:**
    - **A**, after Task 6, in the library: the whole library suite under Debug, ASan+UBSan and TSan, each run with
      `--repeat until-fail:20`, and the bench smoke.
    - **B**, after Task 11, in Rune: `cd broker && scripts/run-regression.sh ../build 8`, then the same suite under
      `build-asan` and `build-tsan` (`-R Regression -LE latency`).
    - **C**, after Task 13: B again on the pinned library commit, plus the dr-e2e run and the ABAB result, before
      the final whole-branch review.
  - A checkpoint failure is bisected over the commits since the last green checkpoint.
- **dr-e2e** (Task 13) runs on the shared Linux test VM from `$RUNE_VM_HOST`. Hold the shared lock for the whole run,
  work only in your own directory, remove only containers and images you created, and check `df` before and after.
  Never run it on local Mac docker. The host's address and login are never written into either repo.
- **Commits** in both repos end with the session's `Co-Authored-By:` and `Claude-Session:` trailers.

## Global Constraints

**HARD RULES (CLAUDE.md §1, checked in every task review):**
1. **One writer per slot, one allocator per file.**
   - Each value record has exactly one writer thread for its whole allocated life. It writes with a plain load of
     its own value, an add, and a release store (`std::atomic_ref<int64_t>::store(v, release)`, the same codegen as
     Aeron's `putLongOrdered`). No `fetch_add`, no CAS, no mutex.
   - Allocate and free run only on the allocator's owner thread (Rune: the thread that owns `BrokerNode` before
     the agents start, then the control agent). A `SlotWriter` handed to another thread is handed over before that
     thread starts, or over an existing Aeron ring (Phase 2). Thread start and the ring both give happens-before.
   - Readers are separate processes or threads. They use acquire loads only and never write the file.
   - Under TSan, every cross-thread word goes through `std::atomic_ref`, with no suppressions.
2. **Zero allocation and no exceptions.**
   - `allocate`, `free`, `SlotWriter::add/set`, `SystemMetrics::doWork` and every reader scan allocate nothing and
     throw nothing.
   - Paths are `const char*` and are copied into a fixed buffer at open. Labels and keys are bounded `std::span`s.
   - Every fallible call returns a `[[nodiscard]]` status enum.
   - A unit test runs each call under an allocation-counting `operator new` and asserts zero allocations (Task 3).
3. **No clock of its own.** Every time-dependent computation takes `nowNs` from the caller. The library never calls
   `steady_clock`, `system_clock` or `clock_gettime`. Tests advance an injected clock and never sleep.
4. **No threads of its own.** `SystemMetrics` runs from its owner's `doWork(nowNs)`.
5. **Fixed capacity.**
   - The slot count is fixed when the file is created.
   - `MetricsFile::create` refuses (`CapacityTooSmall`) when the requested capacity is below
     `requiredSlots(layout)`. Rune refuses to start and logs both numbers.
   - `allocate` past the end returns `Full`. It never grows, overwrites or aborts.
6. **Rich state, not flags.**
   - A slot's state word is one of `Unused = 0`, `Allocated = 1`, `Freed = -1` (Aeron's
     `RECORD_UNUSED/ALLOCATED/RECLAIMED`).
   - A reader's view of a slot is `SlotView = std::variant<Empty, Live{regId, typeId, value}, Reused{oldRegId}>`.
   - `RateTracker` and `WindowSampler` keep one entry per regId. An entry whose regId changed restarts; it is never
     marked with a flag.
7. **Names and semantics kept.** Every Rune metric keeps its type id, label text, key bytes, writer thread and
   meaning. Nothing that parses `rune_role` output changes.
8. **No silent fallback.**
   - A missing metrics file gives `rune_role` exit 1. A file with no role metric gives exit 2.
   - A reader that opens a file being created waits a bounded number of polls for the version word, then fails with
     `NotReady`. It never reads a half-written header.
   - A failed allocation at Rune startup is fatal and logged with the metric's name. A metric is never skipped
     silently.
9. **Regression naming.** Every new test is `*Regression*`. The failing test is written first in every task.
10. **Fix on sight.** Each item below has its own regression test:
    - `O_TRUNC` on a file a reader has mapped (Task 2);
    - the flock wait loop (Task 2);
    - the `fetch_add` and mutex write paths (Task 3);
    - the private `steady_clock` (Task 4);
    - the `SystemMetrics` thread (Task 5);
    - the lazy `findCounter` resolution, which leaves a metric unpublished until the first successful lookup
      (Task 9).

**Versions.**
- arcanum-metrics: `VERSION.txt` becomes `2.0.0`, because the API breaks (D51). The file header's `layoutVersion`
  becomes 2, and a v1 file is refused with `VersionMismatch`.
- Rune: no wire or protocol change. `kClientVersion` stays 14, `kBrokerVersion` 8, `kProtocolVersion` 3.

**Configuration (Rune):**
- `RUNE_METRICS_FILE` (default `${RUNE_DATA_DIR}/metrics/rune-metrics.dat`): read by the broker and `rune_role`
  (D54).
- `RUNE_METRICS_SYSTEM_INTERVAL_MS` (default 1000): the `SystemMetrics` sample period on the control duty cycle.
- The capacity is computed, not configured (D55). Phase 2 adds `RUNE_MAX_NAMESPACES` to the formula.

## Review Focus

1. **Single writer and ordering.** No write path uses `fetch_add`, CAS or a lock. Every value store is a release
   store, and every reader load is an acquire load. TSan is clean with one writer per slot plus a reader and the
   allocator running at once. Tested by `MetricsConcurrencyRegression.WriterReaderAllocatorUnderTsanRegression`
   (Task 6).
2. **Reuse is always detected.** A reader never reports a value under a regId that did not write it. Free plus
   reallocate during a read gives `Reused` or the new `Live`, never the old regId with the new value. Tested by
   `MetricsReaderRegression.ReuseMidReadDetectedRegression` (Task 4).
3. **Allocation is bounded and silent-failure-free.** `Full`, `LabelTooLong`, `KeyTooLong` and `InvalidType` each
   return their status with zero allocation and no partial slot. Tested by `MetricsWriterRegression.ErrorTable/*`
   (Task 3).
4. **File creation is safe.** A reader mapped on the old file never sees it truncated (no SIGBUS), and a reader of
   the new file never sees a half-written header. Tested by
   `MetricsFileRegression.RecreateWhileReaderMappedRegression` and `…ReaderWaitsForVersionWordRegression`
   (Task 2).
5. **Capacity refusal.** Below `requiredSlots`, create fails and Rune refuses to start. Tested by
   `MetricsFileRegression.CapacityTooSmallRefusedRegression` (Task 2) and
   `BrokerMetricsRegression.StartRefusedWhenFileTooSmallRegression` (Task 8).
6. **Every Rune metric moved, same name, same writer.** The table test lists all 37 live type ids (201..241 without
   236–239). Each must be present after start with its exact label and key and the value its writer published.
   `cnc.dat` must hold no Rune type id. Tested by `BrokerMetricsRegression.EveryMetricMovedRegression` and
   `…CncHoldsOnlyDriverCountersRegression` (Tasks 8–9).
7. **`rune_role` is unchanged for its consumers.** Its output is byte-identical and its exit codes are 0, 1 and 2.
   Tested by `RuneRoleRegression.*` (Task 10) and the dr-e2e run (Task 13).
8. **No hot-path regression.** No produce, fetch or query path changes. `SystemMetrics::doWork` is benchmarked on
   the control duty cycle, and an ABAB produce-latency run is within noise. Task 12.

## Decisions

- **D51. A clean 2.0.0 and no compatibility shims.** Rune is the library's only consumer. The old metric classes and
  `MetricsRegistry` are deleted rather than kept beside the new API, because keeping them would keep the rule
  violations reachable. The README documents the migration.
- **D52. The file layout mirrors Aeron `CountersManager`.**
  - The header is 4 KiB: `i32 layoutVersion` (written last with a release store), `i32 pad`, `i64 pid`,
    `i64 startEpochNs` (passed in by the caller), `i32 maxSlots`, `i32 metadataRecordLength = 512`,
    `i32 valueRecordLength = 128`, then the user tag bytes.
  - The metadata area holds `maxSlots × 512 B` records:
    - `i32 state`, `i32 typeId`, `i64 regId`, `i64 ownerTag`;
    - `i32 keyLength` plus a 112-byte key;
    - `i32 labelLength` plus a 380-byte label.
  - The value area holds `maxSlots × 128 B` records: an `i64` value at offset 0, padded to two cache lines against
    false sharing (Aeron's `COUNTER_LENGTH`).
  - Little-endian, with `static_assert`s on every offset.
- **D53. Only totals and levels are written.**
  - The writer side has two kinds: `Counter` (a monotonic total, `add`) and `Gauge` (a level, `set`). The kind is a
    metadata field.
  - The reader computes throughput (Δtotal/Δt), windowed max and windowed average from samples it takes at its own
    period. `RateTracker` and `WindowSampler` keep a fixed ring per slot.
  - So a windowed max is the max of the sampled levels. This is the spec's "computed by the reader". Any writer
    that needs an exact peak keeps its own `Gauge` high-water mark and resets it on its own schedule.
- **D54. One metrics file per node, beside the data.**
  - The path is `RUNE_METRICS_FILE`, default `${RUNE_DATA_DIR}/metrics/rune-metrics.dat`.
  - It is a separate file from `cnc.dat`, so the driver's own counters stay where Aeron's tools expect them.
  - The directory is created with mode 0750 and the file is created with mode 0640.
- **D55. Capacity is computed from a layout.**
  - `requiredSlots = nodeMetrics + maxNamespaces × (perThreadNsMetrics × threads + perNodeNsMetrics) + headroom`,
    where `headroom = 16` covers SystemMetrics and future node metrics.
  - Phase 0 passes `maxNamespaces = 0`. Phase 2 Task 8 sets it from `RUNE_MAX_NAMESPACES` with no library change.
  - Rune creates the file with exactly `requiredSlots`, so a refusal can only come from a build where the formula
    and the enum disagree. The capacity check guards that, and the Phase 2 knob.
- **D56. Safe create without `O_TRUNC` or flock.**
  1. Create `<path>.tmp.<pid>` with `O_CREAT|O_EXCL`.
  2. `ftruncate` it to its size, and zero-fill it implicitly.
  3. Write the header with `layoutVersion = 0`, then store the version with a release store.
  4. `fsync`, then `rename` it over `<path>`.

  A reader that still maps the old inode keeps reading it until it re-opens. The reader re-opens when `pid` or
  `startEpochNs` in the file at the path differs from its own mapping. Liveness of the writer is the reader's
  concern (pid check), as in Aeron's `CncFileDescriptor`.
- **D57. The allocator gives no reuse grace period; regId detects reuse.**
  - `free` sets the state to `Freed` with a release store and pushes the slot onto a fixed free stack. Reuse is
    immediate and LIFO.
  - Each allocation writes a new regId before it sets the state to `Allocated`. The regId comes from the caller
    (Phase 2: nsId) or from the allocator's monotonic counter.
  - A reader reads `state`, `regId`, then `value` (acquire each), and re-reads `regId`. If the two regIds differ,
    the slot is `Reused`.
  - This avoids Aeron's `freeToReuseTimeout`, which is a tuned timing constant (rule 16).
  - The writer zeroes the value record before it publishes `Allocated`, so a new owner never inherits the old total.
- **D58. The `SlotWriter` is a thin value type:** a `std::atomic_ref`-ready `int64_t*` plus nothing else. `add(n)`
  is `store(load(relaxed) + n, release)`. `set(v)` is `store(v, release)`. It is trivially copyable and has no
  virtual calls and no checks on the hot path. A debug-only owner-id assert compares a caller-passed owner tag; it
  never reads `thread_local`.
- **D59. No lazy resolution in Rune.** Every node metric is allocated in `BrokerNode` construction, before any agent
  thread starts, so each writer receives a valid `SlotWriter`. `findCounter`, the try/catch around it and
  `catalogMetricsPending_` / `securityMetricsPending_` are deleted. An allocation failure aborts the start with the
  metric's name.
- **D60. Consumption mirrors the flight recorder.**
  - `ARCANUM_METRICS_DIR` names a local checkout. Otherwise FetchContent pulls `arcanumruna/arcanum-metrics` at a
    pinned commit (never a branch).
  - The Dockerfile takes a named build context `arcanum-metrics` and passes
    `-DARCANUM_METRICS_DIR=/arcanum-metrics`.
  - The conan package is not used, because CI publishes it and nothing here runs conan.
- **D61. The type ids and labels stay numeric-compatible.** `RuneMetric` enumerators equal today's type ids, and the
  label format is unchanged. The 4-byte node-id key is kept in the metadata key, so a tool that matched
  `(typeId, key)` in `cnc.dat` matches the same pair in the metrics file.
- **D62. `SystemMetrics` runs on the control agent.** The control agent's `doWork(nowNs)` calls
  `systemMetrics_.doWork(nowNs)`. That call returns at once until `RUNE_METRICS_SYSTEM_INTERVAL_MS` has passed.
  `/proc` is read with `pread` on fds opened at construction, into fixed stack buffers. Control-plane I/O belongs
  on the control agent, which already does I/O.
- **D63. Library CI in arcanum-metrics** (GitHub Actions):
  - gcc-13 and clang-18 on ubuntu, each building Debug, ASan+UBSan and TSan, and running every test with
    `--repeat until-fail:5`;
  - a Release job that runs the bench smoke.
  - No publish job is added here. Publishing stays the existing CI's or the owner's concern, and a missing publish
    workflow is filed as an issue (`enhancement`, `sev:low`, `area:ci`).
- **D64. The exporter becomes a reader client.** The README's exporter (~:285) is rewritten as
  `metrics::PrometheusText::render(const MetricsReader&, RateTracker&, nowNs, std::span<char>) -> size_t`. It is a
  pure function into a caller buffer, used by the CLI and tests, and it opens no sockets.

## Interaction matrix

Operations × entities × timings. Each cell is a row in a table-driven test (Tasks 3–6) or a Rune rig case (Tasks
8–11).

- **Operations:** create, open (reader), allocate, write (`add`, `set`), read (scan, `RateTracker`,
  `WindowSampler`), free, reallocate (same regId, new regId), recreate the file (restart), `SystemMetrics::doWork`.
- **Entities:**
  - a node metric (control, observer or provisioner writer);
  - a namespace-style block (several slots with one regId, allocated and freed together; Phase 2 uses it);
  - a SystemMetrics slot;
  - the file header.
- **Inputs at the bounds:**
  - capacity 0, 1, `requiredSlots − 1`, `requiredSlots`, and `max` (the int32 cap / file-size cap);
  - a label of 0, 379, 380 and 381 bytes, and a key of 0, 112 and 113 bytes;
  - typeId 0, negative and max;
  - a value near `INT64_MAX` for `add` (wraps two's-complement, documented; the reader's rate handles a wrap as one
    Δ).
- **Timings:**
  - sequential;
  - a reader opening before the version word, between the header and the version, and after;
  - a scan interleaved with an allocate at each of its steps (key/label written, regId written, value zeroed, state
    set);
  - a scan interleaved with a free, and with free then reallocate before the scan's second regId read;
  - writer `add` racing a reader scan (TSan);
  - a recreate while a reader has the old file mapped;
  - a Rune restart between two `rune_role` calls;
  - a leader change while `rune_role` runs (the role metric moves, and the output is consistent per line);
  - provisioner, observer and control writing at once.
- **Rune callers:** `rune_role` in the container, the dr-e2e harness, `t15b-gates.sh`, the kafka-compare scripts, the
  in-process test rigs.

## File Structure

**arcanum-metrics:**
- Create:
  - `include/metrics/MetricsLayout.h`, `include/metrics/MetricsFile.h`, `include/metrics/MetricsWriter.h`,
    `include/metrics/MetricsReader.h`, `include/metrics/PrometheusText.h`;
  - `tests/CMakeLists.txt`, `tests/MetricsLayoutRegressionTest.cpp`, `tests/MetricsFileRegressionTest.cpp`,
    `tests/MetricsWriterRegressionTest.cpp`, `tests/MetricsReaderRegressionTest.cpp`,
    `tests/SystemMetricsRegressionTest.cpp`, `tests/MetricsConcurrencyRegressionTest.cpp`,
    `tests/PrometheusTextRegressionTest.cpp`, `tests/AllocCounter.h`;
  - `bench/CMakeLists.txt`, `bench/MetricsBench.cpp`;
  - `.github/workflows/ci.yml`.
- Rewrite: `include/metrics/SystemMetrics.h`, `README.md`.
- Delete: `include/metrics/Metrics.h`, after its content moves.
- Modify: `CMakeLists.txt` (the test, bench and sanitize options; keep the target and export names), `VERSION.txt`
  (2.0.0), `conanfile.py` (the version only).

**Rune:**
- Create:
  - `broker/libs/node/include/rune/node/RuneMetrics.h`;
  - `broker/libs/node/tests/BrokerMetricsRegressionTest.cpp`;
  - `broker/tests/RuneRoleRegressionTest.cpp`;
  - `broker/libs/node/tests/MetricsTestReader.h`, the shared reader helper for rigs.
- Delete: `broker/libs/node/include/rune/node/RoleCounter.h`, whose content moves.
- Modify:
  - `broker/CMakeLists.txt`, `broker/libs/node/CMakeLists.txt`, `broker/docker/Dockerfile` and its compose/build
    scripts;
  - `broker/libs/node/src/BrokerNode.cpp`, `BrokerNode.h`, `broker/src/rune_role.cpp`;
  - the tests and rigs listed above;
  - `docs/operations.md`, `broker/docs/CATALOG.md`, `broker/docs/SECURITY.md`, `broker/docker/README.md`,
    `docs/api/admin-cli.md`, the release notes.

## Tasks

### Phase A — the library (arcanum-metrics, branch `feature/single-writer-slots`)

### Task 1: scaffolding, layout and CI

Spec §5.3 (layout); D52, D63.

**Files:** `CMakeLists.txt`, `tests/CMakeLists.txt`, `bench/CMakeLists.txt`, `include/metrics/MetricsLayout.h`,
`tests/MetricsLayoutRegressionTest.cpp`, `tests/AllocCounter.h`, `.github/workflows/ci.yml`.

**Interfaces:**
```cpp
namespace metrics {
inline constexpr int32_t kLayoutVersion = 2;
inline constexpr int32_t kHeaderLength = 4096;
inline constexpr int32_t kMetadataRecordLength = 512;
inline constexpr int32_t kValueRecordLength = 128;
inline constexpr int32_t kMaxKeyLength = 112;
inline constexpr int32_t kMaxLabelLength = 380;
enum class SlotState : int32_t { Unused = 0, Allocated = 1, Freed = -1 };
enum class MetricKind : int32_t { Counter = 1, Gauge = 2 };
struct FileLayout {                     // computed, never stored separately
    int32_t maxSlots;
    [[nodiscard]] constexpr int64_t metadataOffset() const noexcept { return kHeaderLength; }
    [[nodiscard]] constexpr int64_t valuesOffset() const noexcept;
    [[nodiscard]] constexpr int64_t fileLength() const noexcept;
};
struct CapacityPlan { int32_t nodeMetrics, maxNamespaces, perThreadNsMetrics, threads, perNodeNsMetrics, headroom; };
[[nodiscard]] constexpr int64_t requiredSlots(const CapacityPlan&) noexcept;   // int64: overflow is checked by caller
}
```

- [ ] **Step 1: failing tests:**
  - `MetricsLayoutRegression.OffsetsTable`: every field offset in the header, metadata and value records matches
    D52, and every value record starts on a 64-byte boundary.
  - `MetricsLayoutRegression.RequiredSlotsTable`: plans with zeros, one namespace, 1024 namespaces × 3 threads, and
    a plan whose product passes `INT32_MAX` (the result is > `INT32_MAX`; the caller refuses it).
  - `MetricsLayoutRegression.FileLengthTable`: `maxSlots` 0, 1, 1024 → the exact byte lengths.
  - `AllocCounter.h`: replaceable global `operator new/delete` that count calls inside a scope; a self-test shows
    that a `std::string` of 64 bytes counts one allocation.
  - Mutation: a value record length of 64 → `OffsetsTable` fails.
- [ ] **Step 2:** implement.
  - CMake options `METRICS_BUILD_TESTS`, `METRICS_BUILD_BENCH`, `METRICS_SANITIZE`, with gtest and benchmark
    through FetchContent at the same versions as Rune (1.16.0, 1.8.2).
  - Keep the INTERFACE target `metrics` and `metricsTargets`.
  - The CI workflow as in D63.
  - Run `ctest --test-dir build -R 'MetricsLayoutRegression|AllocCounter' --output-on-failure -j8`.
- [ ] **Step 3:** commit `layout: Aeron-style metadata and value records; tests, bench and CI scaffolding` and push.

### Task 2: the file — safe create, capacity refusal, reader open

Spec §5.3 (fixed capacity, refuse to start); D55, D56. Fixes on sight: `O_TRUNC` and the flock loop.

**Files:** `include/metrics/MetricsFile.h`, `tests/MetricsFileRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace metrics {
enum class FileStatus : int32_t { Ok, PathTooLong, CapacityTooSmall, CapacityTooLarge, IoError, NotReady,
                                  VersionMismatch, BadLength };
struct CreateParams { const char* path; int32_t maxSlots; int64_t requiredSlots; int64_t pid; int64_t startEpochNs;
                      std::span<const std::byte> userTag; };
class MetricsFile {                       // owns the mapping; movable, not copyable
public:
    [[nodiscard]] static FileStatus create(const CreateParams&, MetricsFile& out) noexcept;
    [[nodiscard]] static FileStatus open(const char* path, int32_t maxVersionPolls, MetricsFile& out) noexcept;
    [[nodiscard]] bool sameInstanceAs(const char* path) const noexcept;   // pid + startEpochNs at path
    [[nodiscard]] FileLayout layout() const noexcept;
    [[nodiscard]] std::byte* base() const noexcept;
    ~MetricsFile();                      // munmap + close; never unlinks
};
}
```

- [ ] **Step 1: failing tests:**
  - `MetricsFileRegression.CreateWritesHeaderRegression`: every header field reads back, and `layoutVersion` is 2.
  - `MetricsFileRegression.CapacityTooSmallRefusedRegression`: `maxSlots = requiredSlots − 1` →
    `CapacityTooSmall`, with no file at the path and no tmp file left behind; `maxSlots = requiredSlots` → `Ok`;
    `requiredSlots > INT32_MAX` → `CapacityTooLarge`.
  - `MetricsFileRegression.RecreateWhileReaderMappedRegression`: a reader maps file 1; the writer creates file 2 at
    the same path; the reader reads every byte of its mapping (no SIGBUS) and still sees file 1's values;
    `sameInstanceAs(path)` is false; a re-open sees file 2. Today's `O_TRUNC` gives SIGBUS on the old mapping, so
    this test runs the old `MetricsRegistry::create` in a forked child and expects the signal (the repro). The
    fork part is deleted with `Metrics.h` in Task 6.
  - `MetricsFileRegression.ReaderWaitsForVersionWordRegression`: a header with `layoutVersion = 0` → `open` returns
    `NotReady` after exactly `maxVersionPolls` polls; after the version is stored, `Ok`.
  - `MetricsFileRegression.V1FileRefusedRegression`: a file with version 1 → `VersionMismatch`.
  - `MetricsFileRegression.ShortFileRefusedRegression`: a file shorter than its header's `fileLength()` →
    `BadLength`.
  - `MetricsFileRegression.PathBoundsTable`: path lengths 0, `PATH_MAX − 6` (room for `.tmp.<pid>` is checked) and
    `PATH_MAX` → `Ok` or `PathTooLong`, with no allocation (AllocCounter).
  - `MetricsFileRegression.NoFlockRegression`: a second process holding an exclusive flock on the path does not
    block `create` or `open` (both return within one call).
  - Mutation: drop the `O_EXCL` tmp and `rename`, and write in place → `RecreateWhileReaderMappedRegression` fails.
- [ ] **Step 2:** implement D56. Run `ctest --test-dir build -R 'MetricsFileRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `file: tmp+rename create, version word last, capacity refusal, no flock` and push.

### Task 3: the writer — allocate, free, SlotWriter

Spec §5.3 (single-writer slots, allocate/free with a regId, no-throw, no allocation, labels); D57, D58. Fix on
sight: the `fetch_add` and mutex write paths.

**Files:** `include/metrics/MetricsWriter.h`, `tests/MetricsWriterRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace metrics {
enum class AllocStatus : int32_t { Ok, Full, KeyTooLong, LabelTooLong, InvalidType, InvalidRegId };
enum class FreeStatus : int32_t { Ok, NotAllocated, OutOfRange };
class SlotWriter {                         // trivially copyable; one writer thread
public:
    void add(int64_t n) const noexcept;    // store(load(relaxed) + n, release)
    void set(int64_t v) const noexcept;    // store(v, release)
    [[nodiscard]] int64_t get() const noexcept;   // owner's own read (relaxed)
    [[nodiscard]] int32_t slot() const noexcept;
};
struct AllocResult { AllocStatus status; SlotWriter writer; int64_t regId; };
class MetricsWriter {                      // owner thread only; holds a fixed free stack sized maxSlots
public:
    explicit MetricsWriter(MetricsFile& file, std::span<int32_t> freeStackStorage) noexcept;
    [[nodiscard]] AllocResult allocate(int32_t typeId, MetricKind kind, int64_t regId /* -1 = next */,
                                       std::span<const std::byte> key, std::string_view label) noexcept;
    [[nodiscard]] FreeStatus free(int32_t slot) noexcept;
    [[nodiscard]] int32_t allocatedCount() const noexcept;
};
}
```
Allocate order: pop a free slot, or take the next `Unused` one, or return `Full`. Then write the key, label, typeId
and kind, zero the value (release), write the regId (release), and set the state to `Allocated` (release). Free sets
the state to `Freed` (release) and pushes the slot. A label is written `"<label>"` as given. Rune and Phase 2 pass
their own text (`ns="acme",ns_id="42"` in Phase 2).

- [ ] **Step 1: failing tests:**
  - `MetricsWriterRegression.ErrorTable/*` (table-driven): capacity 0/1/full; key 0/112/113; label 0/380/381;
    typeId 0, −1 and `INT32_MAX`; regId −1 (auto), 0 and a duplicate (allowed: Phase 2 reuses one regId across a
    block). Each row checks the status, `allocatedCount`, that no metadata byte changed on failure, and zero
    allocations.
  - `MetricsWriterRegression.FreeTable/*`: free an allocated, an already freed and an unused slot, and −1 and
    `maxSlots`.
  - `MetricsWriterRegression.ReuseLifoAndZeroedRegression`: allocate A, `add(5)`, free A, allocate B → same slot,
    value 0, new regId, label B.
  - `MetricsWriterRegression.AutoRegIdMonotonicRegression`: auto regIds strictly increase across allocate/free
    cycles and never repeat.
  - `MetricsWriterRegression.AddSetSemanticsRegression`: `add` accumulates; `set` replaces; `add` across
    `INT64_MAX` wraps (two's-complement, documented).
  - `MetricsWriterRegression.ZeroAllocAllOpsRegression`: allocate, free, add and set under AllocCounter → 0.
  - `MetricsWriterRegression.NoAtomicRmwRegression`: a compile-time check (`static_assert` on a traits probe) plus
    a source grep test in CMake (`file(STRINGS …)` fails the configure if `fetch_add`, `compare_exchange` or `mutex`
    appears in `include/metrics/`). This is the guard against the old write paths.
  - Mutation: set the state to `Allocated` before zeroing the value → `ReuseLifoAndZeroedRegression` fails on a
    reader probe that reads between the steps (the test's step hook).
- [ ] **Step 2:** implement, with `std::atomic_ref` on every cross-thread word. Run
  `ctest --test-dir build -R 'MetricsWriterRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `writer: single-writer slots, owner-thread allocate/free with regId, status codes` and push.

### Task 4: the reader — scans, reuse detection, rates and windows

Spec §5.3 (readers compute rates and windowed max/avg from totals, injected `nowNs`); D53, D57. Fix on sight: the
private `steady_clock`.

**Files:** `include/metrics/MetricsReader.h`, `tests/MetricsReaderRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace metrics {
struct Empty {};
struct Live { int32_t slot; int32_t typeId; MetricKind kind; int64_t regId; int64_t value;
              std::span<const std::byte> key; std::string_view label; };
struct Reused { int32_t slot; int64_t oldRegId; };
using SlotView = std::variant<Empty, Live, Reused>;
class MetricsReader {
public:
    explicit MetricsReader(const MetricsFile&) noexcept;
    [[nodiscard]] SlotView read(int32_t slot) const noexcept;  // state, regId, value, regId again (acquire each)
    template <class F> void forEachLive(F&& f) const noexcept; // skips Empty; retries a Reused slot once
    [[nodiscard]] int32_t findFirst(int32_t typeId, std::span<const std::byte> key) const noexcept; // -1 if none
};
template <int32_t Capacity, int32_t Window>
class RateTracker {      // fixed arrays; one entry per slot; restarts an entry when its regId changes
public:
    void sample(const MetricsReader&, int64_t nowNs) noexcept;
    [[nodiscard]] std::optional<double> perSecond(int32_t slot) const noexcept;  // Δvalue / Δt over the last sample
};
template <int32_t Capacity, int32_t Window>
class WindowSampler {    // levels sampled at the caller's period; max and avg over the last Window samples
public:
    void sample(const MetricsReader&, int64_t nowNs) noexcept;
    [[nodiscard]] std::optional<int64_t> max(int32_t slot) const noexcept;
    [[nodiscard]] std::optional<double> avg(int32_t slot) const noexcept;
};
}
```

- [ ] **Step 1: failing tests:**
  - `MetricsReaderRegression.ReadStatesTable`: Unused → Empty; Allocated → Live with the exact fields; Freed →
    Empty.
  - `MetricsReaderRegression.ReuseMidReadDetectedRegression`: a step hook between the reader's first regId read and
    its value read runs free + allocate on the writer; the result is `Reused{oldRegId}`, never `Live` with the old
    regId and the new value. Then the same with the hook between value and second regId.
  - `MetricsReaderRegression.FindFirstByTypeAndKeyRegression`: two slots with the same typeId and different keys;
    a freed match is skipped.
  - `RateTrackerRegression.RateFromTotalsRegression`: totals 0, 1000 and 3000 at injected 0, 1 s and 2 s → rates
    of 1000 and 2000/s; no rate after only one sample.
  - `RateTrackerRegression.RestartOnReuseRegression`: free and reallocate between samples → no rate for that slot
    on the next sample (never a negative or huge Δ), then a normal rate again.
  - `RateTrackerRegression.WrapIsOneDeltaRegression`: a total crossing `INT64_MAX` gives the correct positive
    rate.
  - `WindowSamplerRegression.MaxAvgTable`: levels over a window of 4 samples, including after the window rolls,
    after a reuse, and with a non-uniform injected clock.
  - `MetricsReaderRegression.ZeroAllocScanRegression`: `forEachLive`, `sample`, `max` and `avg` under
    AllocCounter → 0.
  - Mutation: drop the second regId read → `ReuseMidReadDetectedRegression` fails.
- [ ] **Step 2:** implement. Run
  `ctest --test-dir build -R 'MetricsReaderRegression|RateTrackerRegression|WindowSamplerRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `reader: acquire scans, regId reuse detection, reader-side rates and windows` and push.

### Task 5: SystemMetrics on the owner's duty cycle

Spec §5.3 (injected clock, no own threads); D62. Fix on sight: the `SystemMetrics` thread.

**Files:** `include/metrics/SystemMetrics.h` (rewrite), `tests/SystemMetricsRegressionTest.cpp`.

**Interfaces:**
```cpp
namespace metrics {
struct SystemMetricsParams { int64_t intervalNs; const char* procRoot; /* "/proc/self", injectable for tests */ };
class SystemMetrics {
public:
    enum class Status : int32_t { Ok, OpenFailed, AllocFailed };
    [[nodiscard]] Status init(MetricsWriter&, const SystemMetricsParams&, std::span<const std::byte> key) noexcept;
    int doWork(int64_t nowNs) noexcept;     // returns work count; 0 until intervalNs has passed
    // gauges: open fds, threads, rss bytes, vsize bytes; written with SlotWriter::set
};
}
```
The fds for `statm` and `status` are opened in `init` and read with `pread` into a fixed 4 KiB buffer. The fd count
comes from `getdents64` on an fd opened in `init`, into a fixed buffer. There is no `std::thread`, no `opendir` and
no allocation.

- [ ] **Step 1: failing tests:**
  - `SystemMetricsRegression.NoThreadRegression`: `init` and 100 `doWork` calls leave the process thread count
    unchanged (read from `/proc/self/status`).
  - `SystemMetricsRegression.IntervalOnInjectedClockRegression`: with `intervalNs = 1 s`, `doWork` at 0, 0.5 s,
    1 s and 1.5 s → work at 0 and 1 s only.
  - `SystemMetricsRegression.FakeProcTable`: a temp `procRoot` with crafted `statm`/`status` files → the exact
    gauge values; a truncated file → the gauges keep their last value and a `readErrors` counter increments.
  - `SystemMetricsRegression.ZeroAllocDoWorkRegression`: AllocCounter → 0.
  - Mutation: compute the interval with `steady_clock` instead of `nowNs` → `IntervalOnInjectedClockRegression`
    fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'SystemMetricsRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `system: SystemMetrics driven by doWork(nowNs), preopened fds, no thread` and push.

### Task 6: concurrency stress, exporter, bench, README, 2.0.0

Spec §5.3; D51, D64.

**Files:** `tests/MetricsConcurrencyRegressionTest.cpp`, `include/metrics/PrometheusText.h`,
`tests/PrometheusTextRegressionTest.cpp`, `bench/MetricsBench.cpp`, `README.md`, `VERSION.txt`, `conanfile.py`.
Delete `include/metrics/Metrics.h`, and the fork repro in Task 2's test.

- [ ] **Step 1: failing tests:**
  - `MetricsConcurrencyRegression.WriterReaderAllocatorUnderTsanRegression`:
    - Threads: 4 writer threads each own 8 slots and `add(1)` in a bounded loop of 1e6. The allocator thread frees
      and reallocates one extra block of 8 slots 1e4 times with new regIds. A reader thread scans continuously.
    - Assertions:
      - each writer's final totals equal its iteration counts;
      - every `Live` value seen for a regId is monotonic non-decreasing for that regId;
      - no `Live` value is ever seen under a regId with a value from another regId (the block writes
        `regId × 1e9 + k`);
      - TSan is clean.
    - Seeded; `--repeat until-fail:20`; run once under `-j` load.
  - `PrometheusTextRegression.RenderTable`: counters, gauges and rates render to the exact expected text; a buffer
    too small returns the needed length and writes nothing partial; zero allocations.
  - Mutation: replace the release store with a relaxed store in `SlotWriter::add`, and the acquire with relaxed in
    the reader. TSan does not flag atomic_ref relaxed, so the mutation for this test is the non-atomic plain store
    path behind `METRICS_TEST_PLAIN_STORE`. TSan then reports, and the test fails.
- [ ] **Step 2:**
  - `bench/MetricsBench.cpp`: `add` and `set` (expect ~1 ns, a single store), `read`, `forEachLive` over 4096 slots,
    and `RateTracker::sample` over 4096. The results go into the README.
  - Rewrite the README:
    - the model (Aeron counters), the layout, writer and reader;
    - Rune's usage and the capacity formula;
    - migration from 1.x;
    - no conan steps beyond "CI publishes".
  - Set `VERSION.txt` and the `conanfile.py` version to 2.0.0, and delete `Metrics.h`.
  - Run the whole suite under Debug, ASan+UBSan and TSan.
- [ ] **Step 3:**
  - Commit `2.0.0: concurrency stress, Prometheus text renderer, bench, README; remove the 1.x API` and push.
  - Open the PR `feature/single-writer-slots` → `main` in arcanum-metrics. It is reviewed by the most capable
    model, which reads `docs/adversarial-review.md` from the Rune repo and applies the concurrency, bounds,
    resources and crash-and-restart classes.
  - File the publish-workflow issue (D63).

**Checkpoint A** (library suite × Debug/ASan+UBSan/TSan, `--repeat until-fail:20`, the bench smoke). Then the
library review and merge.

### Phase B — Rune

### Task 7: build and image consumption

D60.

**Files:** `broker/CMakeLists.txt` (beside the flight-recorder block, ~:39–83), `broker/libs/node/CMakeLists.txt`,
`broker/docker/Dockerfile`, the compose and build scripts that pass `--build-context flight-recorder=…` (grep for
`flight-recorder` under `broker/docker`, `broker/dr-e2e` and `client-cpp/scripts`), `README.md#build`.

- [ ] **Step 1: failing tests:**
  - `BrokerMetricsRegression.LibraryVersionRegression` (in the new `BrokerMetricsRegressionTest.cpp`):
    `static_assert(metrics::kLayoutVersion == 2)` and a runtime check that a created file reads back version 2.
    It fails to compile until the dependency exists.
  - A CMake configure test: configuring with `-DARCANUM_METRICS_DIR=/nonexistent` fails with a clear message.
- [ ] **Step 2:** implement.
  - `ARCANUM_METRICS_DIR`, or FetchContent at the pinned commit (`GIT_TAG <sha>`, `GIT_SHALLOW` off, as for the
    flight recorder). Link `metrics` into `rune_node` and `rune_role`.
  - Dockerfile: `COPY --from=arcanum-metrics include /arcanum-metrics/include` and
    `-DARCANUM_METRICS_DIR=/arcanum-metrics`. Every image build script passes the named context.
  - Run `ctest --test-dir build -R 'BrokerMetricsRegression' --output-on-failure -j8`, and build the image once
    locally only to check that it builds (the image build is not e2e).
- [ ] **Step 3:** commit `build: consume arcanum-metrics like the flight recorder` and push.

### Task 8: RuneMetrics.h, the file at start, control-thread metrics

Spec §5.3 (every Rune metric moves; refuse to start); D54, D55, D59, D61.

**Files:** create `RuneMetrics.h`; modify `BrokerNode.h` (~:2516–2647, ~:2868–2875), `BrokerNode.cpp` (allocation
~:515–577, ~:632; writers ~:3284, ~:3455, ~:3585, ~:3812, ~:2525–2526); `broker/libs/node/tests/MetricsTestReader.h`.
Delete `RoleCounter.h` once Task 9 is done; until then it includes `RuneMetrics.h`.

**Interfaces:**
```cpp
namespace rune::node {
enum class RuneMetric : int32_t {          // values == today's type ids; 236–239 stay reserved
    Role = 1'000'201, /* … every id to … */ /* last */ = 1'000'241 };
struct RuneMetricDef { RuneMetric id; metrics::MetricKind kind; const char* labelPrefix; MetricOwner owner; };
enum class MetricOwner : uint8_t { Control, Observer, Provisioner };
inline constexpr std::array<RuneMetricDef, 37> kRuneMetrics = { /* one row per live id, label prefixes as today */ };
inline constexpr int32_t kNodeMetricCount = static_cast<int32_t>(kRuneMetrics.size());
enum class RoleCode : int64_t { None = -1, Follower = 0, Candidate = 1, Leader = 2 };   // unchanged
inline constexpr int32_t kRoleKeyLength = 4;                                            // unchanged
// BrokerNode: metrics::MetricsFile metricsFile_; metrics::MetricsWriter metricsWriter_;
//             std::array<metrics::SlotWriter, kNodeMetricCount> slots_;   // indexed by kRuneMetrics row
//             metrics::SystemMetrics systemMetrics_;
}
```
Start order in `BrokerNode` construction:
1. Compute `requiredSlots({kNodeMetricCount, 0, 0, 0, 0, 16})`.
2. `MetricsFile::create`. Any status other than `Ok` → log `metrics file <path>: <status> (need <n> slots, have <m>)`
   and fail the start.
3. Allocate every `kRuneMetrics` row with key = node id (4 bytes LE) and label = `"<prefix> node=<id>"`. A failure →
   log the metric's name and fail the start.
4. `systemMetrics_.init`.
5. Start the agents.

The control-thread writers replace `Counter::setOrdered/addOrdered` with `slots_[i].set/add`.

- [ ] **Step 1: failing tests** (in `BrokerMetricsRegressionTest.cpp`, with the in-process BrokerNode rig):
  - `BrokerMetricsRegression.EveryMetricMovedRegression` (table over `kRuneMetrics`): after start, each id is
    `Live` in the metrics file with the exact label and key. A control-owned metric shows the value its writer
    published after a deterministic action:
    - create a topic → the catalog topic count;
    - a login ok/fail → the security counters;
    - a peer link up → the peer-link metrics;
    - a storage state → the storage metrics;
    - a stuck marker in the txn coordinator rig → the stuck metrics.
    Observer and provisioner rows are added in Task 9.
  - `BrokerMetricsRegression.CncHoldsOnlyDriverCountersRegression`: scan `cnc.dat` with Aeron's `CountersReader`:
    no type id in [1'000'201, 1'000'241].
  - `BrokerMetricsRegression.StartRefusedWhenFileTooSmallRegression`: a test seam forces `maxSlots =
    requiredSlots − 1` → the start fails with the logged message, and no agent thread starts.
  - `BrokerMetricsRegression.StartRefusedOnAllocFailureRegression`: a test seam makes the 10th allocation return
    `Full` → the start fails and names that metric.
  - `BrokerMetricsRegression.RestartRecreatesFileRegression`: stop, start → a new `startEpochNs`; values restart from
    the node's recovered state (role, catalog counts), and a reader on the old mapping detects the change with
    `sameInstanceAs`.
  - Mutation: skip one row in the allocation loop → `EveryMetricMovedRegression` fails on that row.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'BrokerMetricsRegression|RoleCounter' --output-on-failure -j8`.
- [ ] **Step 3:** commit `node: allocate every Rune metric in the metrics file at start; control-thread writers` and
  push.

### Task 9: observer and provisioner writers; delete the lazy resolution

Spec §5.3; D59. Fix on sight: the lazy `findCounter` resolution.

**Files:** `BrokerNode.cpp` (`publishRole` ~:3243, `publishProvisionMetric_` ~:3201, the lazy resolve ~:5086–5087),
`BrokerNode.h` (delete the `RegId`/`Counter` pairs, `storageCounterRegIds_`, `provCounterRegIds_`,
`catalogMetricsPending_`, `securityMetricsPending_`); delete `RoleCounter.h`.

The observer and the provisioner receive their `SlotWriter`s, copied from `slots_`, in their constructors before their
threads start. Nothing else touches those slots. A debug owner-tag assert (D58) records which owner writes each row.

- [ ] **Step 1: failing tests:**
  - `BrokerMetricsRegression.EveryMetricMovedRegression` gains the observer rows (role, term, out-of-sync after an
    election in the rig) and the provisioner rows (the 5 metrics after one provision step).
  - `BrokerMetricsRegression.PublishedFromFirstCycleRegression`: the catalog and security metrics hold their values
    after the first control `doWork` that changes them, with no lookup retry. Today they stay unpublished until
    `findCounter` succeeds; this test is written against the counter path and fails there.
  - `BrokerMetricsRegression.OwnerTableRegression`: each `kRuneMetrics` row is written only by its declared
    `MetricOwner`, checked by the debug owner-tag assert across a full rig run (election, provision, logins,
    topic DDL).
  - `BrokerMetricsRegression.ThreeWritersUnderTsanRegression`: the rig runs with control, observer and provisioner
    writing and a reader scanning; TSan is clean (the `build-tsan` run at checkpoint B).
  - Mutation: give the provisioner the observer's role slot → `OwnerTableRegression` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'BrokerMetricsRegression|RoleCounterPublish|Provision' --output-on-failure -j8`.
- [ ] **Step 3:** commit `node: observer and provisioner write their own slots; remove the lazy counter lookup` and push.

### Task 10: rune_role reads the metrics file

D54, D61; Review Focus 7.

**Files:** `broker/src/rune_role.cpp`, `broker/tests/RuneRoleRegressionTest.cpp`.

`rune_role` resolves the path from `RUNE_METRICS_FILE`, else from `${RUNE_DATA_DIR}/metrics/rune-metrics.dat`, else
from the same default the broker uses. It opens with `maxVersionPolls = 100`, finds the role, term and storage
metrics by `(typeId, nodeKey)`, and prints the same two lines. Exit codes:
- 0 when found;
- 1 when the file is missing or unreadable (`NotReady` included, with the status on stderr);
- 2 when the role metric is absent.

- [ ] **Step 1: failing tests:**
  - `RuneRoleRegression.GoldenOutputTable`: role None/Follower/Candidate/Leader × storage states → stdout
    byte-identical to golden strings captured from today's binary against `cnc.dat` before the migration. Capture
    the goldens in this step from the current tree.
  - `RuneRoleRegression.ExitCodesTable`: no file → 1; a file with the version word 0 → 1 with `NotReady`; a file
    without the role metric → 2; a full file → 0.
  - `RuneRoleRegression.ParsersStillAgreeRegression`: run the golden output through `parse_rune_role_storage` from
    `broker/dr-e2e/harness/cluster.py` (invoked as a Python subprocess) → the same dict as before.
  - Mutation: print `term` before `role` → `GoldenOutputTable` fails.
- [ ] **Step 2:** implement. Run `ctest --test-dir build -R 'RuneRoleRegression' --output-on-failure -j8`.
- [ ] **Step 3:** commit `rune_role: read the metrics file, same output and exit codes` and push.

### Task 11: tests and rigs read the metrics file

Spec §5.3 ("every reading tool moves with it").

**Files:** every file from `git grep -lE 'RoleCounter|CountersReader|findCounter|addCounter' -- broker client-cpp protocol`
that reads a Rune counter. The starting list is in "What exists today". Comment-only matches get their comments
updated. `MetricsTestReader.h` gives the rigs one helper:
`std::optional<int64_t> readRuneMetric(const BrokerNode&, RuneMetric)` (allocation-free, via `MetricsReader`).

- [ ] **Step 1:** before editing, run the listed tests on the pre-Task-8 commit and save the pass list. These are the
  baseline. Point each test at `MetricsTestReader.h`, with no assertion weakened (CLAUDE.md §3). A deviation is
  recorded in the commit message and either fixed now or filed.
- [ ] **Step 2:** run `ctest --test-dir build -R 'RoleCounter|AuthLogin|D9CrossNodeMarker|PeerSealedCluster|PeerLinkAgent|AuthTls|ClientLogin|ClientSessionRevoked|AuthCluster|AuthInteractionMatrix|AuthLiveChange|SecurityHalt|UdpGso|WorkerSessionCrypto|RecordBatch' --output-on-failure -j8`.
  The pass list must equal the baseline. Then run
  `git grep -nE 'addCounter|findCounter' -- broker client-cpp protocol`: the only matches left are driver counters
  in `ping_probe.cpp` and the pumper check.
- [ ] **Step 3:** commit `tests: read Rune metrics through the metrics file` and push.

**Checkpoint B** (Rune full regression × Debug/ASan+UBSan/TSan).

### Task 12: hot path — SystemMetrics cost and the ABAB run

Rule 6, 12; Review Focus 8.

**Files:** `broker/bench/` (a `ControlDutyCycleBench` case beside the existing micro benches), `BENCHMARKING.md`.

- [ ] **Step 1:**
  - The micro bench: the control agent's `doWork` with and without `systemMetrics_.doWork(nowNs)`, on and off its
    interval. Expect less than 50 ns on an off-interval cycle, and an on-interval cycle (4 `pread`s) well under the
    control loop's budget.
  - Record both in BENCHMARKING.md.
- [ ] **Step 2:**
  - Prepare the clean box once per BENCHMARKING.md.
  - Run the single-node produce p99 bench interleaved ABAB against the pre-Task-7 tip, n ≥ 5 per side (n ≥ 15 if
    a p99 shift under 5% is to be claimed).
  - Expect no change, because no produce, fetch or query path changed. Any regression is bisected, with its code
    path named, and fixed.
  - Restore the box.
- [ ] **Step 3:** commit `bench: SystemMetrics duty-cycle cost; ABAB produce p99 unchanged` and push.

### Task 13: docs, dr-e2e and the pin

Spec §5.3 (`docs/operations.md`); rule 11.

**Files:**
- `docs/operations.md`:
  - the `rune_role` lines (~:65–66), which now name the metrics file;
  - the Metrics section (~:86–118), rewritten:
    - where the file lives, and `RUNE_METRICS_FILE`;
    - that `cnc.dat` holds only driver counters;
    - the metric table (id, label, kind, owner, meaning) generated from `kRuneMetrics`;
    - that rates and windows are computed by readers;
    - the reference to `RuneMetrics.h`.
- `broker/docs/CATALOG.md`, `broker/docs/SECURITY.md`, `broker/docker/README.md` (the named context),
  `docs/api/admin-cli.md`, the release notes.
- `broker/CMakeLists.txt`: the pin set to the arcanum-metrics merge commit.

- [ ] **Step 1:** a doc test, `BrokerMetricsRegression.OperationsDocTableMatchesRegression`: parse the metric table
  in `docs/operations.md` and compare it with `kRuneMetrics` (ids, labels, kinds). It fails until the doc is
  rewritten.
- [ ] **Step 2:** dr-e2e on the shared VM (`$RUNE_VM_HOST`), on the default config:
  1. Hold the shared lock for the whole run.
  2. Work only in your own directory.
  3. Check `df` before and after, and clean up only your own containers.
  4. Run `test_multi_leader_smoke`, `test_dr14_storage` and `test_leader_detection`. All three must pass through
     `docker exec <c> rune_role`.
  5. Also run `client-cpp/scripts/t15b-gates.sh` against the 3-node compose. All gates must be green.
- [ ] **Step 3:** commit `docs: metrics live in the arcanum-metrics file; pin the library` and push.

**Checkpoint C**, then the final whole-branch review: the most capable model, exhaustive and adversarial, against
the Review Focus list and the matrix, and covering the arcanum-metrics diff and the Rune diff together.

## Spec gaps (decided here, recorded for review)

- **Where the file lives (D54):** the spec says the metrics leave `cnc.dat` but not where they go. One file per node
  under the data dir, overridable by env.
- **Reuse without a grace period (D57):** Aeron waits `freeToReuseTimeout` before reusing a slot. That is a timing
  constant, which rule 16 forbids. The regId double-read makes immediate reuse safe for readers.
- **Windowed max from samples (D53):** the spec says the reader computes windowed values from totals. A max cannot
  come from a total, so it comes from sampled levels. A writer that needs an exact peak keeps its own high-water
  gauge.
- **No compatibility layer (D51):** the 1.x API breaks every hard rule, and Rune is its only consumer.
- **Capacity in Phase 0 (D55):** the formula and the refusal are built here with `maxNamespaces = 0`, and Phase 2
  Task 8 supplies the namespace count, so the library does not change twice.
- **Where SystemMetrics runs (D62):** the spec says only "no own thread". The control agent already does I/O.

## Self-review (spec section → task)

| Spec | Task |
|---|---|
| §5.3 single-writer slots, release/acquire, atomic_ref under TSan, no RMW/mutex | 3, 6 |
| §5.3 readers compute rates and windows from totals | 4 |
| §5.3 injected `nowNs` | 4, 5 |
| §5.3 slot block = state word + regId, never reused regId | 3, 4 |
| §5.3 allocate/free on one thread, error codes, no allocation, labels | 3 |
| §5.3 fixed capacity, file sizing, refuse to start | 1, 2, 8 |
| §5.3 compile-time metric enum, ownership table | 8, 9 |
| §5.3 every Rune metric moved; `cnc.dat` keeps driver counters only | 8, 9, 11 |
| §5.3 every reading tool moves (`rune_role`, harness, scripts, tests) | 10, 11, 13 |
| §5.3 `docs/operations.md` | 13 |
| §5.3 per-namespace blocks (NsSlotTable, regId = nsId) | Phase 2 Task 8 (uses Tasks 1–4) |
| §7 Phase 0 | 1–13 |
| §8 matrix (Phase 0 rows), §9 review focus | Interaction matrix; Review Focus 1–8 |

## Execution handoff

Run the plan with superpowers:subagent-driven-development:
- one implementer per task (a mid-tier model) with the exact recipe;
- a task review after each task;
- checkpoints A–C as listed;
- the library PR reviewed and merged between checkpoint A and Task 7's pin.

Implementers never spawn subagents; at most 3 agents run at once. Root-cause diagnosis of any red test stays with the
controller (CLAUDE.md rule 14).
