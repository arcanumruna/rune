# Milestone E — E5b: Multi-core knobs (DEDICATED Aeron, affinity, consensus split) + 500k/<1ms validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the shared-nothing runtime the multi-core knobs it needs to convert per-worker ingress (plan E5a) into ≥500k rec/s and <1 ms batched latency, then validate against Kafka on a right-sized box.

**Architecture:** Three independent config-driven changes on top of E5a: (1) make the Aeron media-driver threading mode a knob so `DEDICATED` splits conductor/sender/receiver onto their own threads when reserved cores exist; (2) finish busy-spin + CPU affinity by adding an `AffinityConfig` that reserves cores for the control plane / driver / cleaner and pins them (worker self-pin already landed in E4); (3) split **consensus/replication** onto its own `ControlAgent` thread (it already communicates via its IngressRing), leaving the gateway+coordinators fused and lock-free. Then a benchmark phase runs Rune vs a warmed, 8-partition, tuned Kafka on a ≥16-core box.

**Tech Stack:** C++23, Aeron C media driver, agrona idle strategies + AgentRunner, `pthread_setaffinity_np` (Linux), spdlog, gtest/ctest; benchmark harness on Hetzner (dockerized Kafka `apache/kafka:3.8.0`, `kafka-producer-perf-test`).

**Spec:** `docs/superpowers/specs/2026-09-23-rune-thread-per-core-runtime-design.md` (§ "Throughput / ingress scale-out (E5)", items 2–4; § Testing; Principle #5 / `AffinityConfig` / `ControlAgent`).

**Depends on:** plan `2026-09-24-rune-milestone-e-e5a-ingress-scaleout.md` (per-worker ingress must be landed first — these knobs have little effect while the funnel exists).

## Global Constraints

- **Shared-nothing law:** no shared mutable state / locks / atomics / `thread_local` on hot paths; cross-thread only via SPSC rings. `[[rune-shared-nothing-law]]`
- **No virtual calls on hot paths.** `[[rune-no-virtual-calls-hot-path]]`
- **Aeron is the reference** for threading modes + idle strategies. `[[aeron-is-the-bible]]`
- **Default idle strategy stays `Backoff`; default threading mode stays `SHARED`** — busy-spin and `DEDICATED` are opt-in and only chosen when reserved/isolated cores are available. Small/embedded boxes must be unaffected by defaults.
- **Parity gate:** all Milestone-D + E5a tests pass unchanged after each phase. `[[rune-review-until-clean]]`
- **Latency floor:** native `<100µs` p99 gate must hold on the right-sized box; per-record latency must not regress. `[[benchmark-latency-gate]]`
- **Benchmark rigor:** warm up before measuring; matched partition count + tuned config for both systems; prove every claim (no assumptions). `[[rune-ull-engineering-standard]]`
- **Never push; local `main` only.** Docker images tagged with git SHA, never `:latest`. `[[docker-image-tag-policy]]`
- **Hetzner box** `root@$RUNE_BENCH_VM_HOST` is user-authorized for benchmarking this session; do **not** touch verdaccio/artifactory containers or change `net.core.rmem_max`.

## Acceptance Gates (this plan)

- **G-B1:** `DEDICATED` threading mode is selectable via config; `SHARED` remains the default; both pass the full test suite.
- **G-B2:** On a ≥16-core box, control/driver/cleaner threads are pinned to reserved cores and workers busy-spin on isolated cores; on the 4-core box the defaults keep current behavior (no oversubscription regressions).
- **G-B3:** Consensus/replication runs on its own `ControlAgent` thread; TSan-clean; no shared-state introduced (gateway+coordinators stay fused).
- **G-B4 (headline):** On a ≥16-core box with per-worker ingress + `DEDICATED` + busy-spin + pinned cores, Rune reaches **≥500k rec/s** batched **and** **<1 ms** batched publish latency (size-triggered batching, near-zero linger), measured against a warmed, 8-partition, tuned Kafka baseline. Throughput scales near-linearly with worker count.

---

## File Structure

- `libs/node/include/rune/node/NodeConfig.h` + `src/NodeConfig.cpp` — `aeronThreadingMode` enum + env; `driverIdleStrategy` knob; `AffinityConfig` fields.
- `libs/node/src/BrokerNode.cpp` — select threading mode + driver idle; pin control/driver/cleaner threads; construct + start the `ControlAgent`.
- `libs/node/include/rune/node/ControlAgent.h` (**new**) — consensus/replication duty loop split out of `NodeRuntimeAgent`.
- `libs/node/include/rune/node/NodeRuntimeAgent.h` — drop the consensus step; keep gateway + coordinators + metadata + transport.
- `libs/node/include/rune/node/AffinityConfig.h` (**new**) — core-reservation plan (control/driver/cleaner/worker-base).
- `libs/node/tests/**` — config + threading + affinity + control-split tests.
- `benchmark/` + Hetzner harness — Rune vs Kafka on the big box.

---

## Phase E5.2 — Aeron `DEDICATED` threading-mode knob

### Task E5.2.1: Config-select threading mode + driver idle strategy

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h`, `src/NodeConfig.cpp`, `libs/node/src/BrokerNode.cpp:1468-1480`
- Test: `libs/node/tests/NodeConfigTest.cpp`, `libs/node/tests/AeronDriverModeTest.cpp` (new)

**Interfaces:**
- Produces: `enum class AeronThreadingMode : uint8_t { Shared = 0, Dedicated = 1 };` field `aeronThreadingMode = Shared` (env `RUNE_AERON_THREADING_MODE`, case-insensitive `shared`/`dedicated`, unknown→`Shared`); `driverIdleStrategy` string (env `RUNE_AERON_DRIVER_IDLE`, default `"sleep-ns"`). Mirror the `WorkerIdleStrategy` parse pattern (`NodeConfig.cpp:628-646`).
- Consumes: `aeron_driver_context_set_threading_mode`, `aeron_driver_context_set_*_idle_strategy`.

- [ ] **Step 1: Failing test** — env selects `Dedicated`; default is `Shared`.
```cpp
TEST(NodeConfig, ParsesAeronThreadingMode) {
    EXPECT_EQ(NodeConfig::fromEnv(emptyEnv()).aeronThreadingMode,
              NodeConfig::AeronThreadingMode::Shared);
    EXPECT_EQ(NodeConfig::fromEnv(fakeEnv({{"RUNE_AERON_THREADING_MODE","dedicated"}}))
                  .aeronThreadingMode, NodeConfig::AeronThreadingMode::Dedicated);
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R NodeConfig -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — add enum+field+idle string+env parse; in `BrokerNode.cpp` replace the hard-coded `AERON_THREADING_MODE_SHARED` with a switch on `cfg.aeronThreadingMode`; when `Dedicated`, set conductor/sender/receiver idle strategies from `driverIdleStrategy` (per Aeron C API) instead of the single shared idle.
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "NodeConfig|AeronDriverMode|node_tests" -V`. Expected: PASS. Add an integration smoke that boots a broker in `Dedicated` mode and round-trips a produce/consume.
- [ ] **Step 5: Commit** — `git commit -m "feat(node): Aeron threading-mode knob (SHARED default / DEDICATED) + driver idle strategy (E5.2)"` + trailer.

---

## Phase E5.3 — Busy-spin + CPU affinity completion

### Task E5.3.1: `AffinityConfig` — reserve cores for control/driver/cleaner; pin them

**Files:**
- Create: `libs/node/include/rune/node/AffinityConfig.h`
- Modify: `libs/node/include/rune/node/NodeConfig.h`, `src/NodeConfig.cpp`, `libs/node/src/BrokerNode.cpp` (control/cleaner AgentRunner start; driver thread start `:1501-1507`)
- Test: `libs/node/tests/AffinityConfigTest.cpp` (new)

**Interfaces:**
- Produces: `struct AffinityConfig { int controlCore = -1; int driverCore = -1; int cleanerCore = -1; int workerCoreBase = -1; };` derived from env (`RUNE_CONTROL_CORE`, `RUNE_DRIVER_CORE`, `RUNE_CLEANER_CORE`, existing `RUNE_WORKER_CORE_BASE`). `-1` everywhere = disabled (current behavior). Reuse the existing `pinCurrentThreadToCore_` helper (`Worker.h:949`) hoisted to a shared header so the control/cleaner/driver threads can self-pin on entry, exactly like the worker E4 self-pin (`Worker.h:300-314`).
- Consumes: `AgentRunner` run loop (`AgentRunner.h:94`), the driver thread lambda (`BrokerNode.cpp:1501`).

- [ ] **Step 1: Failing test** — `AffinityConfig::fromEnv` maps env→cores; `-1` when unset; a self-pin helper is callable and no-ops on macOS.
```cpp
TEST(AffinityConfig, ParsesReservedCores) {
    auto a = AffinityConfig::fromEnv(fakeEnv({{"RUNE_CONTROL_CORE","0"},
                                              {"RUNE_DRIVER_CORE","1"},
                                              {"RUNE_CLEANER_CORE","2"},
                                              {"RUNE_WORKER_CORE_BASE","3"}}));
    EXPECT_EQ(a.controlCore, 0); EXPECT_EQ(a.driverCore, 1);
    EXPECT_EQ(a.cleanerCore, 2); EXPECT_EQ(a.workerCoreBase, 3);
    EXPECT_EQ(AffinityConfig::fromEnv(emptyEnv()).controlCore, -1);
}
```
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R AffinityConfig -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — hoist `pinCurrentThreadToCore_` to `libs/node/include/rune/node/ThreadAffinity.h`; add one-shot self-pin (guarded by a `pinned_` flag) at the top of the control AgentRunner's agent `doWork`, the cleaner agent's `doWork`, and inside the driver thread lambda before its loop. All gated on `>= 0` (disabled by default). Add a `controlIdleStrategy`/busy-spin option for the control thread mirroring `WorkerIdleStrategy` (opt-in only).
- [ ] **Step 4: Run tests** — `ctest --test-dir build-fast -R "AffinityConfig|node_tests|worker" -V`. Expected: PASS (defaults unchanged → all D/E5a tests green).
- [ ] **Step 5: Commit** — `git commit -m "feat(node): AffinityConfig — pin control/driver/cleaner to reserved cores; opt-in control busy-spin (E5.3)"` + trailer.

---

## Phase E5.4 — Split consensus/replication onto its own `ControlAgent`

> Corrected from the spec's phrasing: coordinators are called **inline** by the gateway and are lock-free by design (`GroupCoordinationService`/`TransactionCoordinationService` doc: "Must be called from ONE thread only"). Moving *them* off-thread would need synchronization (breaks shared-nothing). Instead we split **consensus/replication** — which already interacts through its IngressRing — onto its own thread.

### Task E5.4.1: Extract consensus step into `ControlAgent`; run it on its own AgentRunner

**Files:**
- Create: `libs/node/include/rune/node/ControlAgent.h`
- Modify: `libs/node/include/rune/node/NodeRuntimeAgent.h:128` (drop step 1 consensus + step 2 bridge; keep metadata/group/txn/transport/replicas/gateway), `libs/node/src/BrokerNode.cpp:1131-1133` (construct + start a second AgentRunner)
- Test: `libs/node/tests/ControlAgentSplitTest.cpp` (new) + TSan run

**Interfaces:**
- Produces: `class ControlAgent { int doWork(int64_t nowNs); }` running `consensus_.doWork` + the metadata bridge role-flip (the two steps that read/advance election state). It communicates with the rest exactly as today — via the consensus IngressRing and the already-committed metadata records — so no new shared state crosses threads. `NodeRuntimeAgent` reads leadership via the existing `election_` snapshot (already a lock-free read of committed state).
- Consumes: `AgentRunner`, `BackoffIdleStrategy` (default), optional pin to `AffinityConfig::controlCore`.

> **HARD GATE (from E5a G0 failover retro-review + TSan):** the G0 failover fix (`979b6ab`) added `MetadataService::reassignDeadPartitionLeaders` which calls `Election::eligibleNodeIds()` — a read that walks Election's mutable `clusterMembers_`/`leaderMember_`. It is race-free ONLY because consensus + failover run on the one thread today. **Splitting consensus off in THIS task turns that read into a data race + shared-nothing violation.** E5.4 MUST NOT ship until: the ControlAgent (Election owner) computes the live-node set on its own thread and publishes it via an SPSC ring / versioned immutable snapshot, and the failover reassignment reads ONLY that snapshot — never walking `clusterMembers_` cross-thread. Add a TSan test that runs a failover under the split-thread model with zero race reports. Full analysis: `.superpowers/sdd/2026-09-24-rune-milestone-e-e5a-ingress-scaleout/review-failover-979b6ab.md`.

- [ ] **Step 1: Failing test** — consensus advances (leader elected, entries committed) while `NodeRuntimeAgent` is *not* ticking consensus, proving the split; and a produce still commits end-to-end with the two agents running concurrently.
- [ ] **Step 2: Run to confirm fail** — `ctest --test-dir build-fast -R ControlAgentSplit -V`. Expected: FAIL.
- [ ] **Step 3: Implement** — move steps 1–2 of `NodeRuntimeAgent::doWork` into `ControlAgent::doWork`; construct a second `AgentRunner` in `BrokerNode` driving `ControlAgent` (its own `idleStrategy_`, optional core pin); verify `NodeRuntimeAgent`'s reads of `election_->leaderMemberId()`/`leadershipTermId()` remain lock-free reads of committed/atomic-published state (audit for any read-modify-write — if found, publish via a seqlock/SPSC snapshot, not a lock).
- [ ] **Step 4: Run tests + TSan** — `ctest --test-dir build-fast -R "ControlAgentSplit|integration|node_tests" -V`, then a TSan build of consensus + produce + migration tests must be race-clean (this is the race proof for the new thread boundary).
- [ ] **Step 5: Commit** — `git commit -m "feat(node): ControlAgent — split consensus/replication off the gateway thread (E5.4); TSan-clean"` + trailer.

---

## Phase E5.5 — Benchmark + 500k/<1ms validation (deferred big box)

### Task E5.5.1: Throughput-vs-worker-count scaling curve (regression-of-record)

**Files:**
- Modify/create: `benchmark/produce_scaling_bench.*`, harness scripts.

- [ ] **Step 1** — On the ≥16-core box, run the batched produce bench at worker counts {1,2,4,8,12}, pinned + busy-spin + `DEDICATED`, size-triggered batching (linger≈0, `batchBytes`=64–256 KiB). Record rec/s + p50/p99 per point.
- [ ] **Step 2** — Assert near-linear scaling up to core budget (the E5 regression-of-record: single-worker ≈ pre-E5, multi-worker scales). Fail the gate if scaling is flat (indicates a residual funnel).
- [ ] **Step 3** — Post results to the live dashboard + ledger; commit the curve. `[[rune-session-live-protocol]]`

### Task E5.5.2: Warmed, matched Rune-vs-Kafka comparison

- [ ] **Step 1** — Bring up dockerized Kafka (`apache/kafka:3.8.0`, KRaft, 8 partitions, `/dev/shm` logs, tuned heap/retention, SHA-tagged image) on the big box; **warm it** (JIT) before measuring. `[[docker-image-tag-policy]]`
- [ ] **Step 2** — Run both systems at matched 8-partition, matched record size, matched batch config; measure steady-state rec/s + p50/p99 for acks=1 and acks=all.
- [ ] **Step 3 — Gate G-B4** — Confirm Rune ≥500k rec/s **and** <1 ms batched p50 (report p99), and that Rune's per-record latency win is preserved. If either target misses, capture flamegraphs, root-cause via systematic-debugging, and loop (do **not** accept an unexplained miss). `[[rune-ull-engineering-standard]]`
- [ ] **Step 4** — Write the honest verdict (numbers, config, warmup) to the spec's testing section + live dashboard; commit.

---

## Phase E5.6 — Dockerized DR / E2E under the sharded runtime

> Runs ONLY after E5a's in-process integration suite (E5.1.6) is green — reproduce any bug in the fast in-process harness first, never debug it first in Docker. `[[feedback_integration_before_e2e]]` `[[rune-dr-e2e-epic]]`

### Task E5.6.1: Multi-node DR/E2E matrix under per-worker ingress + DEDICATED + affinity

**Files:**
- Test/harness: `tests/e2e/**` (py-testinfra-style dockerized matrix, SHA-tagged images `[[docker-image-tag-policy]]`)

- [ ] **Step 1** — Bring up a multi-node Rune cluster in Docker running the E5 config (per-worker ingress, `DEDICATED`, pinned busy-spin workers), multi-client producers/consumers.
- [ ] **Step 2 — Correctness under fault** — inject: leader kill (exercises the G0 failover reassignment end-to-end across real nodes), network partition + `netem` latency/loss, worker-core saturation, slow-consumer backpressure. Assert: no committed-data loss, no dropped/duplicated produces across reassignment, **end-to-end backpressure reaches producers with zero ack loss (G-A6) across the real network**, exactly-once where configured.
- [ ] **Step 3 — Migration under load** — trigger shard reassignment while producing at load; assert each partition's ingress stream follows ownership with no gap/dup (the real-network analogue of E5.1.5).
- [ ] **Step 4 — Sustained soak** — run the matrix under sustained load for a soak window; assert no leaks, no latency cliff, no race (TSan-instrumented image for a subset).
- [ ] **Step 5** — Post results to the live dashboard + ledger; commit the harness. `[[rune-session-live-protocol]]`

**Exit:** the sharded E5 runtime survives the full DR/E2E fault matrix with the same durability + backpressure + exactly-once guarantees proven in-process, on a real multi-node network.

---

## Self-Review

- **Spec coverage:** item 2 (DEDICATED) → E5.2; item 3 (busy-spin+pinning) → E5.3 (worker half already landed in E4; this adds control/driver/cleaner + `AffinityConfig`); item 4 (split consensus) → E5.4 (corrected direction); Testing section (scaling curve, DEDICATED/busy-spin on ≥16-core, `<100µs`/≥Kafka) → E5.5. ✅
- **Placeholder scan:** config/threading/affinity/control tasks have concrete signatures, env names, assertions, and commands. The benchmark phase is procedure + explicit numeric gates (its "code" is harness scripts + assertions), not placeholder prose. ✅
- **Type consistency:** `AeronThreadingMode`, `AffinityConfig` fields, `ControlAgent::doWork` names are used consistently across config/broker/tests; `pinCurrentThreadToCore_` reused from E4 (`Worker.h:949`) via the hoisted `ThreadAffinity.h`. ✅
- **Ordering + dependency:** E5.2 and E5.3 are independent knobs; E5.4 is independent of both; all three depend on E5a (per-worker ingress) being landed, stated in the header. E5.5 validates the whole stack on the big box (the deferred hardware).
