# Self-Contained Worker Shards (Broker) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn each broker worker into a self-contained shard (own embedded Aeron driver in invoker mode, own Aeron client, own ports, one pinned busy-spinning thread that appends, replicates and acks) so a record crosses no thread hop inside a shard, while keeping today's behaviour bit-for-bit when the new flags are off.

**Architecture:** `RUNE_WORKER_MODE=shard` gives worker `w` an `EmbeddedDriver` in `AERON_THREADING_MODE_INVOKER` under `<aeronDir>/w<w>` plus an Aeron client with `useConductorAgentInvoker(true)`; a `ShardAgent` drives driver → conductor → `Worker::doWork` (shard order) on the pinned core with `BusySpinIdleStrategy`. Per-worker client ingress, replication plane (#4) and egress bind to the worker's own client on explicit per-worker ports; the worker encodes ProduceResponses itself on per-session exclusive publications announced by the runtime over the existing SPSC ring. `RUNE_RUNTIME_PLACEMENT=fused` runs the runtime duty cycle (consensus, metadata, gateway control) inside worker 0's loop with its own invoker driver. Replication traffic fixes (progress-based no-dup-fetch gate, unicast fast-acks), de-virtualised replica transport and zero-copy push/fetch are permanent and apply in every mode.

**Tech Stack:** C++23, Aeron C media driver + C++ client 1.41.4 (conan), SBE (rune-protocol, sbe-all 1.40.2), gtest/ctest, Ninja (`build-fast`), io_uring (`UringFlushBackend`) / `PortableFlushBackend`, rune-fr flight recorder, dockerized DR harness (`dr-e2e`, pytest), `bench/kafka-compare` VM harness.

**Spec:** docs/superpowers/specs/2026-09-30-self-contained-worker-shards-design.md

## Global Constraints

- **Where things run (user, 2026-09-30):** the dockerized DR matrix (every DR e2e run) and **every benchmark** run on **k3s-node-01** (`root@$RUNE_VM_HOST`, repos under `/root/rune-e5`), never on the Mac. Local runs are for unit/integration tests only.
- **Mandatory VM cleanup before EVERY benchmark and EVERY DR run:** `bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh` (created in Task 16 Step 0). It kills all Rune/Kafka/bench/client processes and Rune DR containers, removes stale `/tmp/frout*`, waits for load1 < 1, refuses (exit 1) if any stray process remains, root disk < 3 GiB free, or `/dev/shm` < 1 GiB free. A run that did not start from a passing `vm-clean.sh` is not a valid measurement. Never kill processes with a `pkill -f` pattern that can match the calling ssh command line.
- k3s on k3s-node-01 stays stopped while benchmarking/DR (it is a repurposed UAT box); restart it with `systemctl start k3s` when the plan is finished.
- Pass criterion (verbatim, spec §1): on the 4-core k3s-node-01 box, 3 brokers + 1 client co-located, 1 KB records, 3-node acks=all with the **replicated, no-fsync-before-ack** guarantee (Rune `tier=majority`, `durability=all-in-sync`), an **open-loop producer offering 100,000 rec/s for 60 s achieves ≥ 100,000 rec/s and p99 end-to-end ≤ 100 µs** (every record measured, coordinated-omission-safe).
- Out of scope (spec §1): fsync-before-ack (Strict tier) latency; more than one shard worker per node on the 4-core box; consumer/read path.
- §1a.1 **Shared-nothing.** Every piece of mutable state is owned by exactly one thread. No shared mutable state, no locks/mutex/condvar, no atomics as cross-thread coordination (atomics only inside ring-buffer primitives), no `thread_local` on hot paths.
- §1a.2 **Ring buffers are the only cross-thread channel.** Aeron/Agrona rings (OneToOne where single-producer, ManyToOne only where genuinely multi-producer) — never shared reads or cross-thread callbacks.
- §1a.3 **Aeron is the bible.** Mirror Aeron's patterns exactly: single-writer agents with `doWork(nowNs)` under an `IdleStrategy`, invoker modes, `tryClaim`/`BufferClaim` zero-copy, flat SBE messages with reused encoders, correct release/acquire ordering, one injected clock sampled once per cycle. Busy-spin only on a dedicated pinned core; an agent with no work never burns a shared core.
- §1a.4 **Rich-state state machines.** Each state holds exactly the variables it needs (e.g. `Resolving(pubRegId, deadlineNs)`, `Ready(pub)`); every edge case is one explicit state with one code path — no scattered booleans. Reference: `FollowerSync`.
- §1a.5 **Zero allocation, no virtual calls on hot paths.** No heap allocation, `std::function`, node-based containers (`std::map`/`unordered_map`/`std::string`) or vtable dispatch on the per-record/per-batch/per-duty-cycle path. Polymorphism via concrete types + a construction-time branch, templates, or tagged unions.
- §1a.6 **Cache-friendly, compact data structures;** every optimisation proven by a before/after benchmark with no correctness or p99 regression — no speculative optimisation.
- §1a.7 **Test everything; regression test on every fix** (failing repro test first, marked `*Regression*`, regression suite run first). "Done" means the real end-to-end path is green (real client ↔ real broker over real transport) **in the default configuration as well as flag-on**; shortcut/in-process tests alone never count.
- §1a.8 **Tests never spin to pass time:** advance an injected clock to the next deadline; every drive loop has a hard iteration cap and fails instead of hanging.
- §1a.9 **Time-independent liveness:** control-plane convergence never depends on a tuned timeout out-racing jitter.
- §1a.10 **Benchmark gate:** any p99 > 100 µs is a stop-and-fix, not a result. Perf claims are proven with interleaved A/B on a verified-idle box, citing the exact code path; measurements are validated (same binary, complete captures, run succeeded, feature actually active) before any explanation is drawn from them.
- §1a.11 **Never hardware to mask software:** a bad number on this box is a software bug to root-cause and fix.
- §1a.12 **Fix on sight:** code found violating these while doing the work is fixed as part of the work, not noted.
- Flags and defaults (spec §3.1–§3.3, §7): `RUNE_WORKER_MODE=legacy|shard` (default `legacy`); `RUNE_RUNTIME_PLACEMENT=thread|fused` (default `thread`); `RUNE_WORKER_INGRESS_BASE_PORT` (no default; required in shard mode); `RUNE_WORKER_REPL_BASE_PORT` (no default; required in shard mode); `RUNE_PEERS` entries gain optional per-peer `replBasePort` (and, this plan, `ingressBasePort`); existing `RUNE_WORKER_EGRESS_CONTROL_BASE_PORT`, `RUNE_WORKER_CORE_BASE` unchanged. Shard mode with missing/invalid per-worker ports, missing peer `replBasePort`, or asymmetric worker counts → **hard startup refusal** (no silent fallback). Defaults unchanged; the default configuration must pass every test after every task.
- Stream ids (spec §3.3): client→worker w on `workerIngressBaseStreamId + w` (unchanged); replication worker w on `replBaseStreamId + w` (#4); egress unchanged.
- rune-protocol (spec §3.3, FIXED): `MetadataResponse` per-partition entry gains the additive field **`ingressEndpoint`** — string `"host:port"`; **empty = use the node endpoint**. Older clients ignore it.
- Driver ownership (spec §3.6): a driver is driven by exactly one thread; every "add, then sleep until found" becomes "add, then pump **this shard's own** driver + conductor until found or deadline"; no loop waits for another component on the same thread.
- Replication fixes (spec §3.5): no-dup-fetch is per follower and progress-based, re-serve after `kFetchResendNs` (20 ms); tracker is a fixed-size flat array indexed by replica-set slot; fast-acks are unicast to the leader.
- Commit trailer on every commit: `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`. Never `conan create/export/upload`. Docker images tagged with the git SHA, never `latest`.

## Review Focus

1. **Leader demoted while acks=all produces are parked on the worker** (client keeps streaming to the old leader's worker endpoint after a failover). A reasonable person expects a prompt retriable `NotLeader` ack for every parked batch (never a 30 s `Internal` timeout, never a false OK). → Task 13 test `ShardWorkerAckRegression.DemotedLeaderFiresNotLeaderPromptly`.
2. **Client disconnects / session reaped while its acks are backlogged on a worker.** Expected: the worker frees the session slot, drops that session's queued acks, and client intake on that worker resumes — a dead session must never pause the shard forever. → Task 13 test `SessionAckSinkRegression.CloseWhileBackloggedClearsAndResumesIntake`.
3. **Client egress endpoint never connects (NOT_CONNECTED forever) or the client vanishes without Disconnect.** Expected: after a bounded stall window that session's acks are dropped and the session is `Failed`; other clients' intake resumes (no head-of-line block of the whole shard). → Task 13 test `SessionAckSinkRegression.StalledSessionDroppedAfterStallWindow`.
4. **Co-located peers on the same host with overlapping per-worker port ranges** (the bench runs 3 brokers on 127.0.0.1). Expected: startup refuses with a message naming both colliding ports instead of a cryptic Aeron bind error or silent cross-talk. → Task 6 test `ShardConfigRegression.CoLocatedPeerRangeOverlapRefused`.
5. **Restart after SIGKILL leaves a stale `<aeronDir>/w<w>` (cnc.dat with a dead heartbeat) on tmpfs, or a second broker points at the same dir.** Expected: a stale dir is wiped and the shard driver starts; an ACTIVE driver on the dir is refused loudly. → Task 7 tests `EmbeddedDriverRegression.StaleInactiveDirIsReclaimed` and `EmbeddedDriverRegression.ActiveDirIsRefused`.

---

## File Structure

**rune-fr (sibling)**
- `include/rune_fr/Sites.h` — Task 1 commits the #4 instrumentation sites; Task 9 adds `ShardPass`.

**rune-protocol (sibling)**
- `include/rune/protocol/client/MetadataMessages.h` — Task 2: `PartitionEntry::ingressEndpoint` + additive trailer encode/decode.
- `tests/protocol/client/MetadataMessagesTest.cpp` — Task 2 round-trip / compatibility / torn-trailer tests.
- `schema/rune-internal-ring.xml` + regenerated `include/rune_protocol_internal_sbe/*.h` — Task 12: `SESSION_ANNOUNCE`, `SESSION_CLOSE`, `EGRESS_CHANNEL`, `egressStreamId`, `PRODUCE_NOTIFY`.
- `tests/protocol/internal/RingSbeTest.cpp` — Task 12 round-trip tests.

**rune-broker — new files**
- `libs/replication/include/rune/replication/FollowerSlots.h` — Task 3: flat per-follower leader bookkeeping + pure `decideFetchServe`.
- `libs/replication/include/rune/replication/WorkerReplicationTransport.h` — Task 4: per-worker transport moved here from node (namespace `rune::replication`), `final`, `kind()`.
- `libs/replication/include/rune/replication/BatchPrefix.h` — Task 5: `wholeBatchPrefix` (single copy of the whole-batch/cap/ceiling walk).
- `libs/storage/include/rune/storage/LogReadView.h` — Task 5: `LogReadView` moved out of `LogSegment.h` so `InMemoryLog` can return it.
- `libs/aeronutil/include/rune/aeronutil/AeronPump.h` + `libs/aeronutil/CMakeLists.txt` — Task 8: header-only INTERFACE lib; "pump own driver + conductor until found".
- `libs/node/include/rune/node/EmbeddedDriver.h`, `libs/node/src/EmbeddedDriver.cpp` — Task 7: one embedded driver, Threaded or Invoker, single pumping thread.
- `libs/node/include/rune/node/ShardAgent.h` — Task 9 (fused hook in Task 11): the shard duty cycle agent.
- `libs/node/include/rune/node/SessionAckSink.h` — Task 13: worker-side per-session ack publication table, `PubState` and backlog state machines.
- `libs/node/include/rune/node/SessionAnnouncer.h` — Task 13: gateway-side announce-before-accept state machine.
- `libs/node/tests/support/ShardTestClient.h` — Task 9: raw wire client (own driver) used by shard integration tests.
- Tests: `libs/replication/tests/replication/FollowerSlotsTest.cpp`, `NoDupFetchGateTest.cpp`, `BatchPrefixTest.cpp`, `ZeroAllocReplicaTest.cpp`; `libs/node/tests/node/ReplicaTransportKindTest.cpp`, `EmbeddedDriverTest.cpp`, `AeronPumpInvokerTest.cpp`, `ShardAgentTest.cpp`, `SessionAckSinkTest.cpp`, `SessionAnnouncerTest.cpp`, `ShardWorkerAckTest.cpp`; `libs/node/tests/integration/ShardNonBlockingTest.cpp`, `ShardModeTest.cpp`, `ShardFailoverTest.cpp`; `libs/storage/tests/storage/InMemoryLogReadViewTest.cpp`.
- `docker/docker-compose.shard.yml` — Task 15: shard-mode compose override for the DR matrix.

**rune-broker — modified files**
- `bench/kafka-compare/cluster.sh` — Task 1 (aeron dir comment), Task 15 (shard ranges, `RUNE_PEERS`, pinning).
- `bench/kafka-compare/replbench.sh` — Task 15 (shard on/off switch, idle-box gate).
- `CMakeLists.txt` — Task 8 (`add_subdirectory(libs/aeronutil)`).
- `libs/node/include/rune/node/NodeConfig.h`, `libs/node/src/NodeConfig.cpp`, `libs/node/tests/node/NodeConfigTest.cpp` — Task 1 (aeronDir), Task 6 (shard config).
- `libs/node/include/rune/node/WorkerPool.h` — Task 1 (hooks out), Task 9 (`startShardRunners`).
- `libs/node/include/rune/node/Worker.h` — Tasks 1, 9, 10, 13.
- `libs/node/include/rune/node/WorkerReplicationTransport.h` — Task 1 (FR spans), Task 3 (unicast), Task 4 (becomes forwarding header).
- `libs/node/include/rune/node/WorkerIngress.h`, `WorkerEgress.h`, `UdpMessageSink.h` — Task 8 (pumped resolve).
- `libs/node/include/rune/node/WorkerOp.h` — Task 13 (`SessionAnnounce`, `SessionClose`, `ProduceNotify`).
- `libs/node/include/rune/node/BrokerNode.h`, `libs/node/src/BrokerNode.cpp` — Tasks 1, 6, 7, 9, 10, 11, 13, 14.
- `libs/node/CMakeLists.txt` — new test targets + `src/EmbeddedDriver.cpp`.
- `libs/replication/include/rune/replication/PartitionReplica.h`, `libs/replication/src/PartitionReplica.cpp` — Tasks 1, 3, 4, 5, 10.
- `libs/replication/CMakeLists.txt` — Tasks 3, 4, 5 (sources, aeron link, test targets).
- `libs/storage/include/rune/storage/LogSegment.h`, `InMemoryLog.h`, `Log.h`, `libs/storage/src/Log.cpp`, `libs/storage/CMakeLists.txt` — Task 5 (`readView`), Task 10 (`syncCallsForTest`).
- `libs/consensus/include/rune/consensus/AeronIngress.h`, `libs/replication/include/rune/replication/AeronPartitionTransport.h` + `src/AeronPartitionTransport.cpp`, `libs/gateway/include/rune/gateway/ClientGateway.h` + `src/ClientGateway.cpp` — Task 11 (pumped startup resolve, dead blocking `resolvePublication` removed), Task 13 (announce, notify, PSM authority), Task 14 (ingressEndpoint).
- `libs/gateway/tests/gateway/MetadataResponseTest.cpp` — Task 14.
- `libs/replication/tests/replication/PartitionReplicaTest.cpp` — Task 3 (fast-ack unicast).
- `libs/node/tests/node/WorkerReplicationTransportTest.cpp` — Tasks 3, 5 (fake registry gains `hasPeer`, `sendGatherToPeer`).
- `dr-e2e/harness/cluster.py` — Task 15 (`RUNE_DR_COMPOSE_OVERRIDES`).

---

### Task 1: Commit the completed 2026-09-30 work; remove every experiment hook

Review of the working tree (`git status` / `git diff` on `perf/replication-instrumentation`, rune-fr `main`):

| Change in tree | Decision |
|---|---|
| `NodeConfig::aeronDir` + `effectiveAeronDir()`, `RUNE_AERON_DIR` parse, 3 `BrokerNode.cpp` sites, 2 `NodeConfigRegression` tests | **Ship** (real fix, tested). |
| `cluster.sh` `_rune_node_aeron` → `$WORK/rune-c$1/a` | **Ship**; also fix the now-false comment above it. |
| rune-fr `Sites.h` `ReplWorker*` / `Worker*Stage` sites; spans in `Worker.h`, `WorkerReplicationTransport.h`; `StorageFlush` span in `PartitionReplica.cpp` | **Ship** (compiled out unless `ENABLE_FLIGHT_RECORDER`; runtime-disabled by default). |
| `makeBackoffIdle` env hooks `RUNE_IDLE_MAX_YIELDS` / `RUNE_IDLE_MIN_PARK_US` (`WorkerPool.h`, used at `BrokerNode.cpp:1763`) | **Remove the env reads.** Keep `makeBackoffIdle(maxParkUs)` as a dedup helper with today's fixed shape (10, 20, 1 µs, maxPark). Shard mode replaces parking with busy-spin on a pinned core (Task 9); the experiment answered its question and is not a product knob. |
| `RUNE_FOLLOWER_FLUSH_MS` (`PartitionReplica.cpp` doWork) | **Remove**; restore `static constexpr int64_t kFollowerFlushIntervalNs = 20'000'000LL`. The blocking flush itself is removed from the loop in Task 10. |
| `RUNE_EXP_NO_DUP_FETCH`, `lastPushNs_`, `FetchProgress` + `fetchProgress_` `unordered_map` (`PartitionReplica.h/.cpp`) | **Remove.** Replaced in Task 3 by the permanent, always-on, tested per-follower progress gate over a flat slot array (no env gate, no map, no global recency). |

**Files:**
- Modify (rune-fr): `~/Projects/rune-fr/include/rune_fr/Sites.h` (already edited; commit only)
- Modify: `bench/kafka-compare/cluster.sh:13-18`
- Modify: `libs/node/include/rune/node/WorkerPool.h:43-69` (`makeBackoffIdle`)
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h:815-822`
- Modify: `libs/replication/src/PartitionReplica.cpp:662-683` (onFetchRequest experiment block), `:1511-1524` (flush interval), `:1606-1610` (push timestamp)
- Test: `libs/node/tests/node/NodeConfigTest.cpp:1680-1699` (existing `NodeConfigRegression.AeronDir*`)

**Interfaces:**
- Consumes: nothing new.
- Produces: `std::string NodeConfig::aeronDir`; `std::string NodeConfig::effectiveAeronDir() const` (used by Tasks 7, 9, 11); `std::unique_ptr<rune::consensus::BackoffIdleStrategy> makeBackoffIdle(int64_t maxParkUs)` (fixed shape).

- [ ] **Step 1: Write the failing check (no experiment hook may ship)**

```bash
cd ~/Projects/rune-broker
git grep -nE 'RUNE_EXP_NO_DUP_FETCH|RUNE_FOLLOWER_FLUSH_MS|RUNE_IDLE_MAX_YIELDS|RUNE_IDLE_MIN_PARK_US|lastPushNs_|fetchProgress_' -- libs src bench
```

- [ ] **Step 2: Run it to verify it fails**

Run: the command above.
Expected: FAIL — matches in `WorkerPool.h`, `PartitionReplica.h`, `PartitionReplica.cpp`.

- [ ] **Step 3: Remove the hooks**

`libs/node/include/rune/node/WorkerPool.h` — replace the whole experiment block (the comment + function added above `class WorkerPool`) with:

```cpp
// Backoff idle shared by the worker + runtime loops.  Fixed shape (10 spins,
// 20 yields, 1 µs → maxParkUs park) — identical to the two former inline
// constructions; this helper only de-duplicates them.
inline std::unique_ptr<rune::consensus::BackoffIdleStrategy>
makeBackoffIdle(int64_t maxParkUs) {
    return std::make_unique<rune::consensus::BackoffIdleStrategy>(
        10, 20, std::chrono::microseconds{1},
        std::chrono::microseconds{maxParkUs});
}
```

and delete the `#include <cstdlib>` line added with it (keep `<algorithm>` only if another use remains in the file: `grep -n 'std::max\|std::min\|std::find' libs/node/include/rune/node/WorkerPool.h`; delete it if the grep is empty).

`libs/replication/include/rune/replication/PartitionReplica.h` — delete these lines (after `Offset lastPushedOffset_ = 0;`):

```cpp
    int64_t lastPushNs_ = 0;   // EXPERIMENT (RUNE_EXP_NO_DUP_FETCH): time of last data push
    // EXPERIMENT (RUNE_EXP_NO_DUP_FETCH): per-follower fetch-progress tracker —
    // the offset the follower is currently asking for + when it first asked for it.
    struct FetchProgress { Offset offset = -1; int64_t sinceNs = 0; };
    std::unordered_map<NodeId, FetchProgress> fetchProgress_;
```

`libs/replication/src/PartitionReplica.cpp` — in `onFetchRequest` delete the block from `// EXPERIMENT (RUNE_EXP_NO_DUP_FETCH=1)` through the closing `}` of `if (kNoDupFetch && ...)`, and restore the read guard:

```cpp
    // Read records from the log.
    std::vector<uint8_t> logBytes;
    if (req.maxBytes > 0 && req.fetchOffset < logEndOffset_()) {
```

In `doWork` replace the lambda-initialised interval (and its 3-line EXPERIMENT comment) with:

```cpp
    static constexpr int64_t kFollowerFlushIntervalNs = 20'000'000LL; // 20 ms
```

(keep the `RUNE_FR_SPAN_STORAGE(rune_fr::Site::StorageFlush, ...)` span). In the leader push block restore:

```cpp
            if (pushCeiling > lastPushedOffset_) {
                pushToFollowers(lastPushedOffset_, pushCeiling);
                ++work;
            }
```

`bench/kafka-compare/cluster.sh` — replace the two stale comment lines above `CLUSTER_PERSISTENCE=` with:

```bash
# The broker honours RUNE_AERON_DIR (set to $WORK/rune-c$i/a below, on tmpfs):
# Aeron's CnC + term buffers must NOT live on the disk-backed data dir, and
# rune_role reads the role counter from that same dir.
```

- [ ] **Step 4: Run the check + the regression suite + the default build**

Run:
```bash
cd ~/Projects/rune-broker
git grep -nE 'RUNE_EXP_NO_DUP_FETCH|RUNE_FOLLOWER_FLUSH_MS|RUNE_IDLE_MAX_YIELDS|RUNE_IDLE_MIN_PARK_US|lastPushNs_|fetchProgress_' -- libs src bench; echo "exit=$?"
cmake --build build-fast
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast -E 'udp|UDP|Udp' --output-on-failure -j4
```
Expected: grep prints nothing and `exit=1`; build succeeds; every `*Regression*` test passes (including `NodeConfigRegression.AeronDirDefaultsUnderDataDir` and `NodeConfigRegression.AeronDirHonorsRuneAeronDirEnv`); the non-UDP suite passes.

- [ ] **Step 5: Commit (rune-fr first, then the broker in two commits)**

```bash
cd ~/Projects/rune-fr
git add include/rune_fr/Sites.h
git commit -m "feat(sites): per-worker replication transport + worker doWork sub-stage sites

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"

cd ~/Projects/rune-broker
git add libs/node/include/rune/node/NodeConfig.h libs/node/src/NodeConfig.cpp \
        libs/node/src/BrokerNode.cpp libs/node/tests/node/NodeConfigTest.cpp \
        bench/kafka-compare/cluster.sh
git commit -m "fix(node): honour RUNE_AERON_DIR (Aeron CnC + term buffers on tmpfs) + harness dir

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"

git add libs/node/include/rune/node/Worker.h libs/node/include/rune/node/WorkerPool.h \
        libs/node/include/rune/node/WorkerReplicationTransport.h \
        libs/replication/include/rune/replication/PartitionReplica.h \
        libs/replication/src/PartitionReplica.cpp
git commit -m "perf(fr): #4 per-worker replication + worker sub-stage spans; drop experiment env hooks

RUNE_IDLE_MAX_YIELDS/RUNE_IDLE_MIN_PARK_US, RUNE_FOLLOWER_FLUSH_MS and
RUNE_EXP_NO_DUP_FETCH (+ lastPushNs_/fetchProgress_ map) removed; the no-dup
gate returns as permanent tested code (flat slot array) in the next change.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: rune-protocol — additive `MetadataResponse` per-partition `ingressEndpoint`

`MetadataResponse` is a hand-written SBE-style flat codec (`MetadataMessages.h`), not generated from an XML schema, so "regenerate codec" = edit the codec by hand; no generator run is involved. Entries are a fixed 28-byte stride, so the field cannot go inside the entry without breaking older decoders. It is added as a **trailer after `partitions[]`**: `uint16 endpointCount` (== partitionCount) followed by one var-string per partition. Older decoders stop after `partitions[]` and ignore trailing bytes (their decode never checks for leftover bytes). The trailer is written **only when at least one endpoint is non-empty**, so default-config frames stay byte-identical. `kProtocolVersion` is unchanged.

**Files:**
- Modify: `~/Projects/rune-protocol/include/rune/protocol/client/MetadataMessages.h` (`PartitionEntry` ~line 160; `MetadataResponse::encode/encodedSize/decode`)
- Test: `~/Projects/rune-protocol/tests/protocol/client/MetadataMessagesTest.cpp` (append)

**Interfaces:**
- Produces: `std::string_view rune::protocol::client::PartitionEntry::ingressEndpoint` (`"host:port"`, empty = use node endpoint; on decode a view into the frame buffer); `bool MetadataResponse::hasIngressEndpoints() const noexcept`. Consumed by Task 14 (broker) and the rune-client-cpp plan.

- [ ] **Step 1: Write the failing tests** (append to `MetadataMessagesTest.cpp`)

```cpp
// ===========================================================================
// 2026-09-30 self-contained worker shards: per-partition ingressEndpoint trailer.
// ===========================================================================
namespace {
std::vector<uint8_t> encodeResp(const std::vector<rpc::BrokerEntry>& brokers,
                                const std::vector<rpc::PartitionEntry>& parts) {
    rpc::MetadataResponse resp;
    resp.sessionId = 5; resp.correlationId = 6;
    resp.brokers    = std::span<const rpc::BrokerEntry>(brokers.data(), brokers.size());
    resp.partitions = std::span<const rpc::PartitionEntry>(parts.data(), parts.size());
    std::vector<uint8_t> buf(resp.encodedSize());
    EXPECT_EQ(resp.encode(buf.data()), buf.size());
    return buf;
}
} // namespace

TEST(MetadataMessagesIngressEndpoint, RoundTripsPerPartitionEndpoints) {
    rpc::BrokerEntry b; b.nodeId = 1; b.port = 40200; b.host = "10.0.0.1";
    rpc::PartitionEntry p0; p0.topicId = 3; p0.partition = 0; p0.leaderId = 1; p0.ingressEndpoint = "10.0.0.1:40500";
    rpc::PartitionEntry p1; p1.topicId = 3; p1.partition = 1; p1.leaderId = 1; p1.ingressEndpoint = "10.0.0.1:40501";
    auto buf = encodeResp({b}, {p0, p1});
    auto dec = rpc::MetadataResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(dec.has_value());
    ASSERT_EQ(dec->partitions.size(), 2u);
    EXPECT_EQ(dec->partitions[0].ingressEndpoint, "10.0.0.1:40500");
    EXPECT_EQ(dec->partitions[1].ingressEndpoint, "10.0.0.1:40501");
    EXPECT_EQ(dec->partitions[1].partition, 1);
}

TEST(MetadataMessagesIngressEndpoint, EmptyEndpointsAreByteIdenticalToLegacyLayout) {
    rpc::BrokerEntry b; b.nodeId = 1; b.port = 40200; b.host = "h";
    rpc::PartitionEntry p; p.topicId = 1; p.leaderId = 1;
    auto buf = encodeResp({b}, {p, p});
    // header + brokerCount + (nodeId+port+hostLen+1) + partitionCount + 2*28, no trailer.
    EXPECT_EQ(buf.size(), rpc::MessageHeader::ENCODED_SIZE + 2u + (4u + 4u + 2u + 1u) + 2u + 2u * 28u);
    auto dec = rpc::MetadataResponse::decode(buf.data(), buf.size());
    ASSERT_TRUE(dec.has_value());
    EXPECT_TRUE(dec->partitions[0].ingressEndpoint.empty());
}

TEST(MetadataMessagesIngressEndpoint, OlderDecoderIgnoresTrailer) {
    rpc::PartitionEntry p; p.topicId = 9; p.partition = 2; p.leaderId = 4; p.ingressEndpoint = "h:1";
    auto buf = encodeResp({}, {p});
    // A pre-trailer decoder consumes exactly header + 2 + 2 + 28 bytes; decoding that
    // prefix alone must succeed and yield identical fixed fields.
    const std::size_t legacyLen = rpc::MessageHeader::ENCODED_SIZE + 2u + 2u + 28u;
    ASSERT_GT(buf.size(), legacyLen);
    auto dec = rpc::MetadataResponse::decode(buf.data(), legacyLen);
    ASSERT_TRUE(dec.has_value());
    EXPECT_EQ(dec->partitions[0].topicId, 9);
    EXPECT_EQ(dec->partitions[0].partition, 2);
    EXPECT_EQ(dec->partitions[0].leaderId, 4);
    EXPECT_TRUE(dec->partitions[0].ingressEndpoint.empty());
}

TEST(MetadataMessagesIngressEndpointRegression, TornTrailerRejected) {
    rpc::PartitionEntry p; p.ingressEndpoint = "host:40500";
    auto buf = encodeResp({}, {p});
    for (std::size_t cut = 1; cut <= 12; ++cut) {
        EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(), buf.size() - cut).has_value())
            << "cut=" << cut;
    }
}

TEST(MetadataMessagesIngressEndpointRegression, TrailerCountMismatchRejected) {
    rpc::PartitionEntry p; p.ingressEndpoint = "host:1";
    auto buf = encodeResp({}, {p});
    const std::size_t trailerPos = rpc::MessageHeader::ENCODED_SIZE + 2u + 2u + 28u;
    buf[trailerPos] = 2;   // claims 2 endpoints for 1 partition
    EXPECT_FALSE(rpc::MetadataResponse::decode(buf.data(), buf.size()).has_value());
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-protocol/build --target rune_protocol_tests && ~/Projects/rune-protocol/build/rune_protocol_tests --gtest_filter='MetadataMessagesIngressEndpoint*'`
Expected: FAIL to compile — `'ingressEndpoint' is not a member of 'rune::protocol::client::PartitionEntry'`.

- [ ] **Step 3: Implement**

In `PartitionEntry` add after `egressStreamId`:

```cpp
    /// 2026-09-30 (self-contained worker shards): "host:port" of the owning worker's
    /// client-ingress endpoint on the leader.  EMPTY = use the node endpoint
    /// (BrokerEntry host + port).  Carried in an additive trailer after partitions[];
    /// on decode a zero-copy view into the frame buffer.
    std::string_view ingressEndpoint;
```

In `MetadataResponse` add:

```cpp
    [[nodiscard]] bool hasIngressEndpoints() const noexcept {
        for (const auto& p : partitions) if (!p.ingressEndpoint.empty()) return true;
        return false;
    }
```

At the end of `encode()` (after the partition loop, before `return pos;`):

```cpp
        // Additive trailer: uint16 count (== partitions.size()) + var-str per entry.
        // Written only when some endpoint is non-empty → default frames unchanged.
        if (hasIngressEndpoints()) {
            detail::putLE16(buf, static_cast<int>(pos), static_cast<uint16_t>(partitions.size()));
            pos += 2;
            for (const auto& p : partitions) pos += putVarStr(buf + pos, p.ingressEndpoint);
        }
```

In `encodedSize()` before `return sz;`:

```cpp
        if (hasIngressEndpoints()) {
            sz += 2u;
            for (const auto& p : partitions) sz += 2u + p.ingressEndpoint.size();
        }
```

In `decode()` replace the final `return result;` with:

```cpp
        // Optional trailer (brokers that advertise per-worker ingress endpoints).
        if (pos == avail) return result;                 // absent: older broker / default config
        if (pos + 2u > avail) return std::nullopt;       // torn trailer header
        const uint16_t epCount = detail::getLE16(buf, static_cast<int>(pos));
        pos += 2;
        if (epCount != partCount) return std::nullopt;   // must parallel partitions[]
        for (uint16_t i = 0; i < epCount; ++i) {
            if (!getVarStr(buf, avail, pos, result.partitions[i].ingressEndpoint))
                return std::nullopt;
        }
        return result;
```

Update the frame-layout comment block at the top of the file with `[after partitions] optional trailer: uint16 endpointCount; endpointCount × (uint16 len + host:port bytes)`.

- [ ] **Step 4: Run to verify they pass (and nothing else broke)**

Run: `cmake --build ~/Projects/rune-protocol/build --target rune_protocol_tests && ~/Projects/rune-protocol/build/rune_protocol_tests`
Expected: PASS — all tests, including the pre-existing `MetadataMessages.ResponseTruncationReject`.
Then: `cmake --build ~/Projects/rune-broker/build-fast --target rune_gateway_metadata_response_tests rune_metadata_request_tests && ~/Projects/rune-broker/build-fast/libs/gateway/rune_gateway_metadata_response_tests && ~/Projects/rune-broker/build-fast/libs/gateway/rune_metadata_request_tests`
Expected: PASS (broker compiles against the sibling header unchanged).

- [ ] **Step 5: Commit**

```bash
cd ~/Projects/rune-protocol
git add include/rune/protocol/client/MetadataMessages.h tests/protocol/client/MetadataMessagesTest.cpp
git commit -m "feat(protocol): additive MetadataResponse per-partition ingressEndpoint trailer

Empty = use node endpoint; trailer written only when non-empty so default frames
are byte-identical; older decoders ignore it.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 3: Replication traffic fixes — per-follower progress-based no-dup-fetch gate (flat slots) + unicast fast-acks

Spec §3.5. The leader's per-follower state (the existing fetch-conflation record `servedFetch_` — an `unordered_map` on the per-batch path, fixed on sight — plus the new progress tracker) moves into one fixed-size flat array indexed by the follower's slot in the partition's replica set. Rule: while a follower's fetch offset keeps advancing and the requested offset is below what **this leadership pushed to that follower**, answer ack-only (HW, no data); once it has asked for the same offset for ≥ `kFetchResendNs` (20 ms), serve data (lost-push recovery). Coverage is tracked per follower (`pushedThrough`), not globally, and reset on every role change — this is stricter than the rejected env experiment (which used the global `lastPushedOffset_`, stale across leaderships). The gate is permanent and always on when `pushOnAppend` (the default), so the default configuration is tested with it. Fast-acks (and every follower FetchRequest) are unicast to the known leader.

**Files:**
- Create: `libs/replication/include/rune/replication/FollowerSlots.h`
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h` — `PartitionTransport` (~line 149: add `sendFetchRequestTo`), fields ~line 897-909 (`ServedFetch`/`servedFetch_`/`kFetchResendNs`), private helper decl
- Modify: `libs/replication/src/PartitionReplica.cpp` — `initReplica` (~128), `becomeLeader` (`servedFetch_.clear()` ~295), `becomeFollower` (~385), `onFetchRequest` self-demote (~545) + conflation block (~652), `pushToFollowers` loop (~2213), the four `transport_->sendFetchRequest(` sites
- Modify: `libs/node/include/rune/node/WorkerReplicationTransport.h` — `AeronPubRegistry::hasPeer`, `sendFetchRequestTo` override
- Modify: `libs/node/tests/node/WorkerReplicationTransportTest.cpp` — `SharedFakePubRegistry::hasPeer` + 2 tests
- Create: `libs/replication/tests/replication/FollowerSlotsTest.cpp`, `libs/replication/tests/replication/NoDupFetchGateTest.cpp`
- Modify: `libs/replication/CMakeLists.txt` (add both files to `rune_replication_tests`)

**Interfaces:**
- Produces (namespace `rune::replication`): `inline constexpr int64_t kFetchResendNs = 20'000'000;` `struct FollowerSlot`, `class FollowerSlots { static constexpr std::size_t kMaxReplicas = 16; bool init(const std::vector<NodeId>&) noexcept; FollowerSlot* find(NodeId) noexcept; void resetLeadership() noexcept; std::size_t size() const noexcept; }`, `enum class FetchServe : uint8_t { Conflate, AckOnly, Data };` `FetchServe decideFetchServe(FollowerSlot&, Offset fetchOffset, int32_t maxBytes, Offset curLeo, bool pushOnAppend, int64_t nowNs, int64_t resendNs) noexcept`; `virtual void PartitionTransport::sendFetchRequestTo(NodeId leader, const FetchRequestMsg&)` (default: `sendFetchRequest(req)`); PubRegistry concept gains `bool hasPeer(NodeId) const noexcept`.
- `PartitionReplica` constructor now throws `std::invalid_argument` when the replica set exceeds `FollowerSlots::kMaxReplicas`.

- [ ] **Step 1: Write the failing tests**

`libs/replication/tests/replication/FollowerSlotsTest.cpp`:

```cpp
// FollowerSlotsTest.cpp — pure unit tests for the flat per-follower leader
// bookkeeping (fetch conflation + progress-based no-dup-fetch gate, spec §3.5).
#include <gtest/gtest.h>
#include <rune/replication/FollowerSlots.h>

using namespace rune::replication;

namespace {
constexpr int64_t kMs = 1'000'000;
}

TEST(FollowerSlots, InitAssignsSlotsInReplicaOrderAndFindsById) {
    FollowerSlots s;
    ASSERT_TRUE(s.init({1, 2, 3}));
    EXPECT_EQ(s.size(), 3u);
    ASSERT_NE(s.find(2), nullptr);
    EXPECT_EQ(s.find(2)->id, 2);
    EXPECT_EQ(s.find(9), nullptr);
}

TEST(FollowerSlotsRegression, InitRefusesMoreThanCapacity) {
    FollowerSlots s;
    std::vector<NodeId> big;
    for (NodeId i = 0; i < static_cast<NodeId>(FollowerSlots::kMaxReplicas) + 1; ++i) big.push_back(i);
    EXPECT_FALSE(s.init(big));
}

TEST(FollowerSlots, IdenticalRequestInsideWindowIsConflated) {
    FollowerSlot f; f.id = 2;
    EXPECT_EQ(decideFetchServe(f, 0, 1024, 10, true, 1 * kMs, kFetchResendNs), FetchServe::Data);
    EXPECT_EQ(decideFetchServe(f, 0, 1024, 10, true, 5 * kMs, kFetchResendNs), FetchServe::Conflate);
    EXPECT_EQ(decideFetchServe(f, 0, 1024, 11, true, 6 * kMs, kFetchResendNs), FetchServe::Data)
        << "leader LEO changed → not identical";
}

TEST(FollowerSlots, CoveredAndAdvancingIsAckOnly) {
    FollowerSlot f; f.id = 2; f.pushedThrough = 100;
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs, kFetchResendNs), FetchServe::AckOnly);
    EXPECT_EQ(decideFetchServe(f, 20, 1024, 100, true, 2 * kMs, kFetchResendNs), FetchServe::AckOnly);
}

TEST(FollowerSlots, StuckForResendWindowIsServedData) {
    FollowerSlot f; f.id = 2; f.pushedThrough = 100;
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs, kFetchResendNs), FetchServe::AckOnly);
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs + kFetchResendNs, kFetchResendNs),
              FetchServe::Data);
}

TEST(FollowerSlots, NotPushedOrPushDisabledIsServedData) {
    FollowerSlot f; f.id = 2;                       // pushedThrough = 0
    EXPECT_EQ(decideFetchServe(f, 10, 1024, 100, true, 1 * kMs, kFetchResendNs), FetchServe::Data);
    FollowerSlot g; g.id = 3; g.pushedThrough = 100;
    EXPECT_EQ(decideFetchServe(g, 10, 1024, 100, /*pushOnAppend=*/false, 1 * kMs, kFetchResendNs),
              FetchServe::Data);
}

TEST(FollowerSlots, PureFastAckIsAckOnly) {
    FollowerSlot f; f.id = 2;
    EXPECT_EQ(decideFetchServe(f, 10, 0, 100, true, 1 * kMs, kFetchResendNs), FetchServe::AckOnly);
}

TEST(FollowerSlots, ResetLeadershipClearsCoverageKeepsIds) {
    FollowerSlots s;
    ASSERT_TRUE(s.init({1, 2}));
    s.find(2)->pushedThrough = 50;
    s.find(2)->askOffset = 7;
    s.resetLeadership();
    EXPECT_EQ(s.find(2)->pushedThrough, 0);
    EXPECT_EQ(s.find(2)->askOffset, -1);
    EXPECT_EQ(s.find(2)->id, 2);
}
```

`libs/replication/tests/replication/NoDupFetchGateTest.cpp`:

```cpp
// NoDupFetchGateTest.cpp — DR-style tests of the leader's no-dup-fetch gate and
// the follower's unicast fast-ack (spec §3.5).  Injected clock only: every time is
// an explicit nowNs argument; no sleeps, no spinning.
#include <gtest/gtest.h>

#include <rune/replication/PartitionReplica.h>
#include <rune/replication/FollowerSlots.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <unistd.h>

using namespace rune::replication;
namespace fs = std::filesystem;

namespace {

constexpr NodeId  kLeader = 1, kF2 = 2, kF3 = 3;
constexpr int64_t kMs = 1'000'000;

struct RecordingTransport final : PartitionTransport {
    struct Resp { NodeId to; FetchResponseMsg msg; std::vector<uint8_t> records; };
    struct Req  { NodeId to; FetchRequestMsg msg; bool unicast; };
    std::vector<Resp> responses;
    std::vector<Req>  requests;

    void sendFetchRequest(const FetchRequestMsg& r) override { requests.push_back({-1, r, false}); }
    void sendFetchRequestTo(NodeId leader, const FetchRequestMsg& r) override {
        requests.push_back({leader, r, true});
    }
    void sendFetchResponse(NodeId to, const FetchResponseMsg& m) override {
        Resp x{to, m, std::vector<uint8_t>(m.records.begin(), m.records.end())};
        x.msg.records = {};
        responses.push_back(std::move(x));
    }
    // Responses addressed to `to` since the last call; clears ALL recorded responses.
    std::vector<Resp> takeFor(NodeId to) {
        std::vector<Resp> out;
        for (auto& r : responses) if (r.to == to) out.push_back(r);
        responses.clear();
        return out;
    }
};

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static int seq = 0;
        path = fs::temp_directory_path() /
               (std::string("rune_nodup_") + tag + "_" + std::to_string(::getpid()) + "_" +
                std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

PartitionConfig cfgFor(NodeId self) {
    PartitionConfig c;
    c.partitionId = 0; c.selfId = self; c.minIsr = 1;
    c.replicaLagMaxMs = 60'000; c.replicaLagMaxOffset = 1'000'000;
    c.fetchIntervalNs = 1 * kMs; c.isrCheckIntervalNs = 1'000 * kMs;
    c.maxFetchBytes = 1 << 20; c.pushOnAppend = true;
    return c;
}

FetchRequestMsg fetch(NodeId follower, Offset off, int32_t maxBytes = 1 << 20, Epoch epoch = 1) {
    return FetchRequestMsg{epoch, 0, follower, off, maxBytes, 0};
}

// Leader with 10 single-record batches, pushed once (at t=1ms) to every ISR follower.
struct LeaderFixture {
    TmpDir dir{"leader"};
    rune::storage::Log log{dir.path.string(), 64LL * 1024 * 1024};
    RecordingTransport t;
    std::unique_ptr<PartitionReplica> leader;

    explicit LeaderFixture(std::vector<NodeId> isr = {kLeader, kF2, kF3}) {
        log.recover();
        leader = std::make_unique<PartitionReplica>(
            cfgFor(kLeader), Role::Leader, /*epoch=*/1,
            std::vector<NodeId>{kLeader, kF2, kF3}, std::move(isr), &log, &t);
        const std::vector<uint8_t> payload(64, 0xAB);
        for (int i = 0; i < 10; ++i) leader->append(std::span<const uint8_t>(payload));
        leader->doWork(1 * kMs);   // coalesced push [0,10) to the ISR followers
        EXPECT_EQ(t.takeFor(kF2).size(), 1u) << "precondition: exactly one push to F2";
    }
};

} // namespace

TEST(NoDupFetchGateRegression, AdvancingFollowerGetsAckOnly) {
    LeaderFixture f;
    ASSERT_EQ(f.leader->onFetchRequest(fetch(kF2, 4), 2 * kMs), kErrNone);
    auto r1 = f.t.takeFor(kF2);
    ASSERT_EQ(r1.size(), 1u);
    EXPECT_TRUE(r1[0].records.empty()) << "pushed range must not be re-sent while F2 advances";
    f.leader->onFetchRequest(fetch(kF2, 8), 3 * kMs);
    auto r2 = f.t.takeFor(kF2);
    ASSERT_EQ(r2.size(), 1u);
    EXPECT_TRUE(r2[0].records.empty());
}

TEST(NoDupFetchGateRegression, StuckFollowerIsServedDataAfterResendWindow) {
    LeaderFixture f;
    f.leader->onFetchRequest(fetch(kF2, 4), 2 * kMs);
    ASSERT_TRUE(f.t.takeFor(kF2).at(0).records.empty());
    f.leader->onFetchRequest(fetch(kF2, 4), 2 * kMs + kFetchResendNs);
    auto r = f.t.takeFor(kF2);
    ASSERT_EQ(r.size(), 1u);
    ASSERT_FALSE(r[0].records.empty()) << "stuck >= kFetchResendNs must be served data";
    rune::protocol::RecordBatchReader rd(r[0].records.data(), r[0].records.size());
    EXPECT_LE(rd.baseOffset(), 4);
    EXPECT_GE(rd.baseOffset() + rd.lastOffsetDelta(), 4);
}

TEST(NoDupFetchGateRegression, LostPushRecoveredWithoutDuplicateFlood) {
    LeaderFixture f;   // the fixture's push to F3 is "lost": F3 never applied it
    f.leader->onFetchRequest(fetch(kF3, 0), 2 * kMs);
    auto a = f.t.takeFor(kF3);
    ASSERT_EQ(a.size(), 1u);
    EXPECT_TRUE(a[0].records.empty());
    f.leader->onFetchRequest(fetch(kF3, 0), 7 * kMs);            // identical, inside window
    EXPECT_TRUE(f.t.takeFor(kF3).empty()) << "identical request inside the window is conflated";
    f.leader->onFetchRequest(fetch(kF3, 0), 2 * kMs + kFetchResendNs);
    auto c = f.t.takeFor(kF3);
    ASSERT_EQ(c.size(), 1u);
    EXPECT_FALSE(c[0].records.empty()) << "lost push re-served after the window";
}

TEST(NoDupFetchGateRegression, LeaderChangeResetsPushCoverage) {
    LeaderFixture f;
    f.leader->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/kF2);
    f.leader->becomeLeader(/*newEpoch=*/3, {kLeader, kF2, kF3});
    f.t.responses.clear();
    f.leader->onFetchRequest(fetch(kF2, 4, 1 << 20, /*epoch=*/3), 3 * kMs);
    auto r = f.t.takeFor(kF2);
    ASSERT_EQ(r.size(), 1u);
    EXPECT_FALSE(r[0].records.empty())
        << "nothing was pushed in THIS leadership — data must be served at once";
}

TEST(NoDupFetchGate, FollowerOutsideIsrWasNeverPushedAndIsServedImmediately) {
    LeaderFixture f({kLeader, kF2});   // F3 not in ISR → never pushed
    f.leader->onFetchRequest(fetch(kF3, 0), 2 * kMs);
    auto r = f.t.takeFor(kF3);
    ASSERT_EQ(r.size(), 1u);
    EXPECT_FALSE(r[0].records.empty());
}

TEST(NoDupFetchGate, PureFastAckNeverCarriesData) {
    LeaderFixture f;
    f.leader->onFetchRequest(fetch(kF2, 0, /*maxBytes=*/0), 2 * kMs + 5 * kFetchResendNs);
    auto r = f.t.takeFor(kF2);
    ASSERT_EQ(r.size(), 1u);
    EXPECT_TRUE(r[0].records.empty());
}

TEST(FollowerSlotsRegression, ReplicaSetAboveCapacityRefusedAtConstruction) {
    TmpDir d{"cap"};
    rune::storage::Log log{d.path.string(), 64LL * 1024 * 1024};
    log.recover();
    RecordingTransport t;
    std::vector<NodeId> big;
    for (NodeId i = 0; i < static_cast<NodeId>(FollowerSlots::kMaxReplicas) + 1; ++i) big.push_back(i);
    PartitionConfig c = cfgFor(0);
    EXPECT_THROW(PartitionReplica(c, Role::Follower, 1, big, big, &log, &t), std::invalid_argument);
}

TEST(FastAckUnicastRegression, FollowerFastAckTargetsItsLeader) {
    TmpDir d{"follower"};
    rune::storage::Log log{d.path.string(), 64LL * 1024 * 1024};
    log.recover();
    RecordingTransport t;
    PartitionReplica follower(cfgFor(kF2), Role::Follower, 1,
                              {kLeader, kF2, kF3}, {kLeader, kF2, kF3}, &log, &t);
    follower.becomeFollower(/*leaderEpoch=*/1, /*leaderId=*/kLeader);
    rune::protocol::RecordBatchBuilder b(0, 1000);
    b.append("k", "v", 1000);
    const std::vector<uint8_t> batch = b.build();
    FetchResponseMsg resp{1, 0, 0, 0, kErrNone, 0, std::span<const uint8_t>(batch)};
    follower.applyReplicated(resp);
    ASSERT_FALSE(t.requests.empty()) << "apply of new data must emit a fast-ack";
    const auto& ack = t.requests.back();
    EXPECT_TRUE(ack.unicast) << "fast-ack must be unicast to the leader, not broadcast";
    EXPECT_EQ(ack.to, kLeader);
    EXPECT_EQ(ack.msg.maxBytes, 0);
}
```

Append to `libs/node/tests/node/WorkerReplicationTransportTest.cpp` (and add to `SharedFakePubRegistry`, after `allResolved()`):

```cpp
    [[nodiscard]] bool hasPeer(NodeId peer) const noexcept
    {
        return std::find(state_->peers.begin(), state_->peers.end(), peer) != state_->peers.end();
    }
```

```cpp
TEST(WorkerReplicationTransportRegression, SendFetchRequestToUnicastsToLeaderOnly)
{
    auto h = makeTestPair(/*workerId=*/0, {2, 3});
    FetchRequestMsg req{4, 1, 7, 100, 0, 3};
    h.transport->sendFetchRequestTo(/*leader=*/2, req);
    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_TRUE(h.pubs.offersTo(3).empty()) << "fast-ack must not be broadcast";
    EXPECT_EQ(h.pubs.offersTo(2)[0], encodeExpectedFetchRequest(req));
}

TEST(WorkerReplicationTransport, SendFetchRequestToUnknownLeaderFallsBackToBroadcast)
{
    auto h = makeTestPair(/*workerId=*/0, {2, 3});
    FetchRequestMsg req{4, 1, 7, 100, 0, 3};
    h.transport->sendFetchRequestTo(/*leader=*/9, req);
    EXPECT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_EQ(h.pubs.offersTo(3).size(), 1u);
}
```

Add both new replication test files to the `add_executable(rune_replication_tests ...)` list in `libs/replication/CMakeLists.txt`:

```cmake
        tests/replication/FollowerSlotsTest.cpp
        tests/replication/NoDupFetchGateTest.cpp
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_replication_tests rune_worker_replication_transport_tests`
Expected: FAIL to compile — `rune/replication/FollowerSlots.h: No such file`, and `'sendFetchRequestTo' marked 'override' but does not override`.

- [ ] **Step 3: Implement**

Create `libs/replication/include/rune/replication/FollowerSlots.h`:

```cpp
#pragma once
// FollowerSlots.h — leader-side per-follower fetch bookkeeping (spec §3.5).
//
// One fixed-size flat array indexed by the follower's slot in the partition's
// replica set (no map, no allocation).  Holds, per follower:
//   * FETCH CONFLATION (formerly the servedFetch_ unordered_map): the last
//     (fetchOffset, leader LEO, time) served — an IDENTICAL request inside
//     kFetchResendNs gets no response.
//   * NO-DUP-FETCH progress tracker: the offset the follower is asking for and
//     since when, plus the exclusive end of data pushed to THIS follower in THIS
//     leadership.  While the follower advances inside the pushed range the leader
//     answers ack-only; after kFetchResendNs stuck it serves data (lost push).
// Single-writer: the thread that owns the replica.  Reset on every role change.
#include <rune/replication/Types.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace rune::replication {

inline constexpr int64_t kFetchResendNs = 20'000'000;  // 20 ms lost-push re-serve window

struct FollowerSlot {
    NodeId  id                = -1;
    Offset  servedFetchOffset = -1;
    Offset  servedLeo         = -1;
    int64_t servedTimeNs      = 0;
    Offset  askOffset         = -1;
    int64_t askSinceNs        = 0;
    Offset  pushedThrough     = 0;

    void resetLeadership() noexcept {
        servedFetchOffset = -1; servedLeo = -1; servedTimeNs = 0;
        askOffset = -1; askSinceNs = 0; pushedThrough = 0;
    }
};

enum class FetchServe : uint8_t { Conflate, AckOnly, Data };

class FollowerSlots {
public:
    static constexpr std::size_t kMaxReplicas = 16;

    /// Assign slots in replica-set order.  false (nothing assigned) when the set
    /// exceeds kMaxReplicas — the caller refuses construction.
    [[nodiscard]] bool init(const std::vector<NodeId>& replicas) noexcept {
        if (replicas.size() > kMaxReplicas) return false;
        n_ = replicas.size();
        for (std::size_t i = 0; i < n_; ++i) {
            slots_[i] = FollowerSlot{};
            slots_[i].id = replicas[i];
        }
        return true;
    }
    [[nodiscard]] FollowerSlot* find(NodeId id) noexcept {
        for (std::size_t i = 0; i < n_; ++i) if (slots_[i].id == id) return &slots_[i];
        return nullptr;
    }
    void resetLeadership() noexcept {
        for (std::size_t i = 0; i < n_; ++i) slots_[i].resetLeadership();
    }
    [[nodiscard]] std::size_t size() const noexcept { return n_; }

private:
    std::array<FollowerSlot, kMaxReplicas> slots_{};
    std::size_t n_ = 0;
};

/// Pure decision for one FetchRequest; updates the slot's conflation + progress state.
[[nodiscard]] inline FetchServe decideFetchServe(FollowerSlot& s, Offset fetchOffset,
                                                 int32_t maxBytes, Offset curLeo,
                                                 bool pushOnAppend, int64_t nowNs,
                                                 int64_t resendNs) noexcept {
    if (s.servedTimeNs != 0 && s.servedFetchOffset == fetchOffset &&
        s.servedLeo == curLeo && (nowNs - s.servedTimeNs) < resendNs) {
        return FetchServe::Conflate;
    }
    s.servedFetchOffset = fetchOffset;
    s.servedLeo         = curLeo;
    s.servedTimeNs      = nowNs;
    if (maxBytes <= 0) return FetchServe::AckOnly;              // pure fast-ack: HW only
    if (fetchOffset != s.askOffset) { s.askOffset = fetchOffset; s.askSinceNs = nowNs; }
    if (pushOnAppend && fetchOffset < s.pushedThrough && (nowNs - s.askSinceNs) < resendNs) {
        return FetchServe::AckOnly;                             // covered by a push, still advancing
    }
    return FetchServe::Data;                                    // never pushed, or stuck (lost push)
}

} // namespace rune::replication
```

`PartitionReplica.h`:
- add `#include <rune/replication/FollowerSlots.h>` next to `#include <rune/replication/FollowerSync.h>`;
- in `class PartitionTransport`, after `sendFetchRequest`:

```cpp
    /// (Follower → its KNOWN leader) spec §3.5.2: unicast.  Default keeps the old
    /// behaviour for transports that cannot address a peer (tests, legacy ring).
    virtual void sendFetchRequestTo(NodeId /*leader*/, const FetchRequestMsg& req) {
        sendFetchRequest(req);
    }
```

- replace the `struct ServedFetch ...; std::unordered_map<NodeId, ServedFetch> servedFetch_; static constexpr int64_t kFetchResendNs = 20'000'000;` lines (keep the FETCH CONFLATION comment above them) with:

```cpp
    // Per-follower conflation + no-dup-fetch state (spec §3.5) — flat, slot-indexed.
    FollowerSlots followerSlots_;
```

- add to the private helpers: `void sendFetchToLeader_(const FetchRequestMsg& req);`

`PartitionReplica.cpp`:
- add `#include <stdexcept>` and `#include <string>`;
- in `initReplica`, right after `configuredReplicas_ = replicas;`:

```cpp
    if (!followerSlots_.init(configuredReplicas_)) {
        throw std::invalid_argument(
            "PartitionReplica: replica set size " + std::to_string(configuredReplicas_.size()) +
            " exceeds FollowerSlots::kMaxReplicas=" + std::to_string(FollowerSlots::kMaxReplicas));
    }
```

- in `becomeLeader` replace `servedFetch_.clear();   // fetch-conflation state is per-leadership` with `followerSlots_.resetLeadership();   // conflation + push coverage are per-leadership`;
- in `becomeFollower` after `reconcileTarget_ = -1;` add `followerSlots_.resetLeadership();`;
- in `onFetchRequest`'s self-demote branch after `followerSync_.reset();` add `followerSlots_.resetLeadership();`;
- in `onFetchRequest` replace the whole `{ const Offset curLeo = ...; auto& sf = servedFetch_[...]; ... }` conflation block and the read guard below it with:

```cpp
    // FETCH CONFLATION + NO-DUP-FETCH (spec §3.5) — FollowerSlots.h.  ISR liveness
    // and HW were updated above, so answering ack-only or nothing is safe.
    FollowerSlot* const slot = followerSlots_.find(req.followerId);
    const FetchServe serve = slot
        ? decideFetchServe(*slot, req.fetchOffset, req.maxBytes, logEndOffset_(),
                           cfg_.pushOnAppend, nowNs, kFetchResendNs)
        : (req.maxBytes > 0 ? FetchServe::Data : FetchServe::AckOnly);
    if (serve == FetchServe::Conflate) return kErrNone;

    // Read records from the log (Data only; AckOnly answers the HW with no records).
    std::vector<uint8_t> logBytes;
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_()) {
```

- in `pushToFollowers` replace the send loop with:

```cpp
    for (NodeId followerId : isrList) {
        if (followerId == cfg_.selfId) continue;
        transport_->sendFetchResponse(followerId, resp);
        if (FollowerSlot* s = followerSlots_.find(followerId)) {
            if (pushedThrough > s->pushedThrough) s->pushedThrough = pushedThrough;
        }
    }
```

- add the helper (next to `nextFetch`) and replace every `transport_->sendFetchRequest(` in the file with `sendFetchToLeader_(` (4 sites: fast-ack in `applyReplicated`, `applyOffsetsForLeaderEpochResponse`, the epoch-probe liveness fetch and the FollowerSync fetch in `doWork`):

```cpp
// Spec §3.5.2: the follower knows its leader — unicast.  Before the leader is
// known (leaderId_ < 0) fall back to the transport's broadcast.
void PartitionReplica::sendFetchToLeader_(const FetchRequestMsg& req)
{
    if (leaderId_ >= 0 && leaderId_ != cfg_.selfId) transport_->sendFetchRequestTo(leaderId_, req);
    else                                            transport_->sendFetchRequest(req);
}
```

`WorkerReplicationTransport.h`: in `AeronPubRegistry` add

```cpp
    [[nodiscard]] bool hasPeer(rune::replication::NodeId peer) const noexcept
    {
        return pubs_.find(peer) != pubs_.end();
    }
```

and in `WorkerReplicationTransportImpl` after `sendFetchRequest`:

```cpp
    // -----------------------------------------------------------------------
    // sendFetchRequestTo — spec §3.5.2: UNICAST to the replica's known leader.
    // A leader that is not (yet) a registered peer falls back to the broadcast.
    // -----------------------------------------------------------------------
    void sendFetchRequestTo(rune::replication::NodeId leader,
                            const rune::replication::FetchRequestMsg& req) override
    {
        RUNE_FR_SPAN_REPLICATION(rune_fr::Site::ReplWorkerFetchReqSend,
                                 static_cast<uint32_t>(req.partition));
        constexpr std::size_t kSize = rune::replication::FetchRequestMsg::TOTAL_SIZE;
        alignas(8) uint8_t claimBuf[kSize];
        rune::replication::FetchRequestMsg::encode(
            claimBuf, 0, req.leaderEpoch, req.partition, req.followerId,
            req.fetchOffset, req.maxBytes, req.topicId);
        if (pubReg_.hasPeer(leader)) pubReg_.sendToPeer(leader, claimBuf, kSize);
        else                         pubReg_.broadcastToPeers(claimBuf, kSize);
    }
```

- [ ] **Step 4: Run to verify they pass (regression suite first, then everything)**

Run:
```bash
cd ~/Projects/rune-broker
cmake --build build-fast
ctest --test-dir build-fast -R Regression --output-on-failure
./build-fast/libs/replication/rune_replication_tests --gtest_filter='FollowerSlots*:NoDupFetchGate*:FastAckUnicast*'
./build-fast/libs/node/rune_worker_replication_transport_tests
ctest --test-dir build-fast -E 'udp|UDP|Udp' --output-on-failure -j4
ctest --test-dir build-fast -R 'PerWorkerReplication|GatewayFailover|RebalanceFailover|MultiLeader' --output-on-failure
```
Expected: PASS everywhere (the UDP/failover integration suites prove the gate does not stall real replication in the default configuration).

- [ ] **Step 5: Commit**

```bash
git add libs/replication/include/rune/replication/FollowerSlots.h \
        libs/replication/include/rune/replication/PartitionReplica.h \
        libs/replication/src/PartitionReplica.cpp libs/replication/CMakeLists.txt \
        libs/replication/tests/replication/FollowerSlotsTest.cpp \
        libs/replication/tests/replication/NoDupFetchGateTest.cpp \
        libs/node/include/rune/node/WorkerReplicationTransport.h \
        libs/node/tests/node/WorkerReplicationTransportTest.cpp
git commit -m "fix(replication): per-follower progress-based no-dup-fetch gate + unicast fast-acks

Flat slot-indexed FollowerSlots replaces the servedFetch_ map; pushed data is
answered ack-only while the follower advances and re-served after 20 ms stuck.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: De-virtualise the replica transport on the hot path

Spec §3.5a.1. `PartitionReplica` calls `transport_->sendFetchResponse/sendFetchRequestTo/...` through the virtual `PartitionTransport` on every push/ack. The per-worker transport moves into `rune_replication` (header-only, `final`), and the replica classifies its transport **once at construction** (`kind()`), then dispatches through `onTransport(f)`: a perfectly-predicted branch to a direct, inlinable call on the concrete `final` per-worker class. Resolution of a spec ambiguity: the *legacy* `AeronPartitionTransport` lives in `rune_replication_aeron`, which links `rune_replication` (and Worker/Consensus headers); making it a concrete call target would create a library cycle. It is the control-thread path (not on any shard per-record path) and stays behind the `Generic` arm (virtual) together with test fakes. `rune_replication` links `aeron::aeron` when found and compiles the concrete arm under `RUNE_REPLICATION_HAS_AERON`.

**Files:**
- Create: `libs/replication/include/rune/replication/WorkerReplicationTransport.h` (content moved from `libs/node/include/rune/node/WorkerReplicationTransport.h`, namespace `rune::node` → `rune::replication`)
- Modify: `libs/node/include/rune/node/WorkerReplicationTransport.h` (becomes a forwarding header)
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h` (`TransportKind`, `kind()`, forward decls, members)
- Modify: `libs/replication/src/PartitionReplica.cpp` (include, `onTransport`, classification, every `transport_->` call)
- Modify: `libs/replication/CMakeLists.txt` (aeron link + define)
- Create: `libs/node/tests/node/ReplicaTransportKindTest.cpp`; Modify: `libs/node/CMakeLists.txt` (add it to `rune_worker_replication_transport_tests`)

**Interfaces:**
- Consumes: Task 3 `sendFetchRequestTo`, `hasPeer`.
- Produces: `enum class rune::replication::TransportKind : uint8_t { Generic = 0, PerWorker = 1 };` `virtual TransportKind PartitionTransport::kind() const noexcept` (default `Generic`); `rune::replication::AeronPubRegistry`, `rune::replication::WorkerReplicationTransportImpl<PubRegistry>` (final), `rune::replication::WorkerReplicationTransport` (re-exported into `rune::node` by using-declarations); `TransportKind PartitionReplica::transportKindForTest() const noexcept`.

- [ ] **Step 1: Write the failing test** — `libs/node/tests/node/ReplicaTransportKindTest.cpp`

```cpp
// ReplicaTransportKindTest.cpp — the replica classifies its transport ONCE at
// construction; the production per-worker transport takes the concrete
// (de-virtualised) arm, everything else the generic arm (spec §3.5a.1).
#include <rune/node/WorkerReplicationTransport.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/InMemoryLog.h>

#include <gtest/gtest.h>
#include <span>
#include <type_traits>
#include <vector>

using namespace rune::replication;

static_assert(std::is_final_v<rune::node::WorkerReplicationTransport>,
              "the per-worker transport must be final so calls through it are direct");

namespace {
// Minimal PubRegistry satisfying the concept (Aeron-free).
class MinimalFakeRegistry {
public:
    [[nodiscard]] bool    allResolved() const noexcept { return true; }
    [[nodiscard]] int32_t maxMessageLength() const noexcept { return 1 << 20; }
    [[nodiscard]] bool    hasPeer(NodeId) const noexcept { return true; }
    int64_t sendToPeer(NodeId, const uint8_t*, std::size_t) noexcept { ++sends; return 1; }
    void    broadcastToPeers(const uint8_t*, std::size_t) noexcept { ++sends; }
    int sends = 0;
};

PartitionConfig cfg1() { PartitionConfig c; c.selfId = 1; return c; }
} // namespace

TEST(ReplicaTransportKind, ProductionPerWorkerTransportSelectsConcreteArm) {
    rune::node::WorkerReplicationTransport t(nullptr, 0, "", 0, {}, 0);   // null aeron: inert
    EXPECT_EQ(t.kind(), TransportKind::PerWorker);
    rune::storage::InMemoryLog log(1 << 20, 1024, rune::storage::FullPolicy::DropOldest);
    PartitionReplica r(cfg1(), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    EXPECT_EQ(r.transportKindForTest(), TransportKind::PerWorker);
}

TEST(ReplicaTransportKind, FakeRegistryTransportStaysGeneric) {
    rune::node::WorkerReplicationTransportImpl<MinimalFakeRegistry> t(0, MinimalFakeRegistry{});
    EXPECT_EQ(t.kind(), TransportKind::Generic);
    rune::storage::InMemoryLog log(1 << 20, 1024, rune::storage::FullPolicy::DropOldest);
    PartitionReplica r(cfg1(), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    EXPECT_EQ(r.transportKindForTest(), TransportKind::Generic);
}

TEST(ReplicaTransportKind, InMemoryTransportIsGeneric) {
    InMemoryTransport t;
    rune::storage::InMemoryLog log(1 << 20, 1024, rune::storage::FullPolicy::DropOldest);
    PartitionReplica r(cfg1(), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    EXPECT_EQ(r.transportKindForTest(), TransportKind::Generic);
}
```

Register it: in `libs/node/CMakeLists.txt` change `add_executable(rune_worker_replication_transport_tests tests/node/WorkerReplicationTransportTest.cpp)` to

```cmake
        add_executable(rune_worker_replication_transport_tests
            tests/node/WorkerReplicationTransportTest.cpp
            tests/node/ReplicaTransportKindTest.cpp)
```

- [ ] **Step 2: Run to verify it fails**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_worker_replication_transport_tests`
Expected: FAIL to compile — `'TransportKind' is not a member of 'rune::replication'`.

- [ ] **Step 3: Implement**

1. Move the transport:

```bash
cd ~/Projects/rune-broker
git mv libs/node/include/rune/node/WorkerReplicationTransport.h \
       libs/replication/include/rune/replication/WorkerReplicationTransport.h
sed -i '' -e 's/^namespace rune::node {/namespace rune::replication {/' \
          -e 's|^} // namespace rune::node|} // namespace rune::replication|' \
    libs/replication/include/rune/replication/WorkerReplicationTransport.h
```

In the moved file add `#include <type_traits>` and, inside `WorkerReplicationTransportImpl` (public, after `resolved()`):

```cpp
    // Construction-time classification (spec §3.5a.1): the production registry is
    // the replica's concrete, de-virtualised arm; any test registry stays Generic.
    [[nodiscard]] rune::replication::TransportKind kind() const noexcept override
    {
        return std::is_same_v<PubRegistry, AeronPubRegistry>
                   ? rune::replication::TransportKind::PerWorker
                   : rune::replication::TransportKind::Generic;
    }
```

2. Replace `libs/node/include/rune/node/WorkerReplicationTransport.h` with:

```cpp
#pragma once
// Forwarding header: the per-worker replication transport lives in rune_replication
// so PartitionReplica can call it concretely (de-virtualised, spec §3.5a.1).
#include <rune/replication/WorkerReplicationTransport.h>

namespace rune::node {
using rune::replication::AeronPubRegistry;
using rune::replication::WorkerReplicationTransportImpl;
using rune::replication::WorkerReplicationTransport;
} // namespace rune::node
```

3. `PartitionReplica.h`: before `class PartitionTransport` add

```cpp
enum class TransportKind : uint8_t {
    Generic   = 0,  ///< virtual dispatch (legacy control-thread transport, test fakes)
    PerWorker = 1,  ///< concrete final WorkerReplicationTransport — direct calls
};
```

inside `PartitionTransport` (public):

```cpp
    [[nodiscard]] virtual TransportKind kind() const noexcept { return TransportKind::Generic; }
```

after `class PartitionTransport { ... };`:

```cpp
class AeronPubRegistry;
template <typename PubRegistry> class WorkerReplicationTransportImpl;
```

in `class PartitionReplica` public section: `[[nodiscard]] TransportKind transportKindForTest() const noexcept { return tkind_; }` and next to `PartitionTransport* transport_;`:

```cpp
    using PerWorkerTransport = WorkerReplicationTransportImpl<AeronPubRegistry>;
    TransportKind       tkind_     = TransportKind::Generic;  ///< fixed at construction
    PerWorkerTransport* perWorker_ = nullptr;                 ///< non-null iff tkind_ == PerWorker
    /// Apply f to the concrete transport: one predicted branch, direct call on the
    /// PerWorker arm.  Defined in PartitionReplica.cpp (its only user).
    template <class F> void onTransport(F&& f);
```

4. `PartitionReplica.cpp`: after the existing includes add

```cpp
#if RUNE_REPLICATION_HAS_AERON
#include <rune/replication/WorkerReplicationTransport.h>
#endif
```

and at namespace scope (before the constructors):

```cpp
template <class F>
[[gnu::always_inline]] inline void PartitionReplica::onTransport(F&& f)
{
#if RUNE_REPLICATION_HAS_AERON
    if (tkind_ == TransportKind::PerWorker) { f(*perWorker_); return; }
#endif
    f(*transport_);
}
```

At the top of `initReplica`:

```cpp
    // Construction-time transport classification (never re-evaluated on the hot path).
    tkind_ = transport_->kind();
#if RUNE_REPLICATION_HAS_AERON
    if (tkind_ == TransportKind::PerWorker) perWorker_ = static_cast<PerWorkerTransport*>(transport_);
#else
    tkind_ = TransportKind::Generic;
#endif
```

Then rewrite every remaining `transport_->NAME(ARGS);` as `onTransport([&](auto& t) { t.NAME(ARGS); });` — e.g.

```cpp
        onTransport([&](auto& t) { t.sendFetchResponse(req.followerId, resp); });
```

and `sendFetchToLeader_` becomes:

```cpp
void PartitionReplica::sendFetchToLeader_(const FetchRequestMsg& req)
{
    if (leaderId_ >= 0 && leaderId_ != cfg_.selfId)
        onTransport([&](auto& t) { t.sendFetchRequestTo(leaderId_, req); });
    else
        onTransport([&](auto& t) { t.sendFetchRequest(req); });
}
```

Verify: `grep -n 'transport_->' libs/replication/src/PartitionReplica.cpp` lists only `initReplica` (the `kind()` call) and `onTransport`.

5. `libs/replication/CMakeLists.txt`, after `target_link_libraries(rune_replication PUBLIC rune_storage)`:

```cmake
# Task 4 (de-virtualised replica transport): the per-worker transport is
# header-only Aeron code the replica calls concretely.
if(aeron_FOUND)
    target_link_libraries(rune_replication PUBLIC aeron::aeron spdlog::spdlog)
    target_compile_definitions(rune_replication PUBLIC RUNE_REPLICATION_HAS_AERON=1)
endif()
```

- [ ] **Step 4: Run to verify it passes (plus the flag-on real-Aeron per-worker path)**

Run:
```bash
cmake --build build-fast
ctest --test-dir build-fast -R Regression --output-on-failure
./build-fast/libs/node/rune_worker_replication_transport_tests
ctest --test-dir build-fast -E 'udp|UDP|Udp' --output-on-failure -j4
ctest --test-dir build-fast -R 'PerWorkerReplication' --output-on-failure
```
Expected: PASS (the `PerWorkerReplication.*` UDP suite exercises the concrete PerWorker arm over real Aeron).

- [ ] **Step 5: Commit**

```bash
git add -A libs/replication/include/rune/replication/WorkerReplicationTransport.h \
          libs/node/include/rune/node/WorkerReplicationTransport.h \
          libs/replication/include/rune/replication/PartitionReplica.h \
          libs/replication/src/PartitionReplica.cpp libs/replication/CMakeLists.txt \
          libs/node/tests/node/ReplicaTransportKindTest.cpp libs/node/CMakeLists.txt
git commit -m "perf(replication): de-virtualise replica transport (construction-time tag, final per-worker arm)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Zero-copy push/fetch — `readView` + gather send straight into the publication

Spec §3.5a.2. `pushToFollowers` and `onFetchRequest` build `std::vector<uint8_t> logBytes = L.read(...)` per call (allocation + copy). They now take a zero-copy `LogReadView` (disk: `Log::readView`, `Log.h:90`; memory: new `InMemoryLog::readView`), trim it with one shared whole-batch walk, and the per-worker transport encodes only the 46-byte SBE prefix on the stack and copies the record bytes **once**, log → term buffer (`tryClaim` ≤ `maxPayloadLength`, 2-buffer gather `offer` above it). The per-peer publication map becomes a flat array (fix on sight: `unordered_map::find` per send). The reconcile path (`onReconcileRequest`, failover-only, cold) keeps `L.read`.

**Files:**
- Create: `libs/storage/include/rune/storage/LogReadView.h`; Modify: `libs/storage/include/rune/storage/LogSegment.h:20-26` (struct moves out, include added)
- Modify: `libs/storage/include/rune/storage/InMemoryLog.h` (add `readView`)
- Create: `libs/replication/include/rune/replication/BatchPrefix.h`
- Modify: `libs/replication/src/PartitionReplica.cpp` (`onFetchRequest` read block, `pushToFollowers` read block)
- Modify: `libs/replication/include/rune/replication/WorkerReplicationTransport.h` (`AeronPubRegistry` flat + `sendGatherToPeer`; `sendFetchResponseImpl_`; drop `scratchBuf_`)
- Modify: `libs/node/tests/node/WorkerReplicationTransportTest.cpp`, `libs/node/tests/node/ReplicaTransportKindTest.cpp` (fakes gain `sendGatherToPeer`)
- Create: `libs/storage/tests/storage/InMemoryLogReadViewTest.cpp`, `libs/replication/tests/replication/BatchPrefixTest.cpp`, `libs/replication/tests/replication/ZeroAllocReplicaTest.cpp`
- Modify: `libs/storage/CMakeLists.txt` (add the view test to `rune_in_memory_log_tests`), `libs/replication/CMakeLists.txt` (BatchPrefixTest → `rune_replication_tests`; new `rune_replication_zero_alloc_tests`)

**Interfaces:**
- Consumes: Task 3 `FollowerSlots`, Task 4 `onTransport`.
- Produces: `rune::storage::LogReadView` (unchanged layout, new header); `LogReadView InMemoryLog::readView(int64_t startOffset, int32_t maxBytes)`; `struct rune::replication::BatchPrefix { std::size_t bytes; Offset endOffset; }` + `BatchPrefix wholeBatchPrefix(const uint8_t*, std::size_t len, std::size_t cap, Offset ceiling) noexcept`; PubRegistry concept gains `int64_t sendGatherToPeer(NodeId, const uint8_t* prefix, std::size_t prefixLen, std::span<const uint8_t> body) noexcept`.

- [ ] **Step 1: Write the failing tests**

`libs/storage/tests/storage/InMemoryLogReadViewTest.cpp`:

```cpp
#include <gtest/gtest.h>
#include <rune/storage/InMemoryLog.h>
#include <rune/protocol/RecordBatch.h>
#include <vector>

using rune::storage::InMemoryLog;
using rune::storage::FullPolicy;

namespace {
std::vector<uint8_t> batchAt(int64_t base) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    b.append("k", "v", 1000);
    return b.build();
}
}

TEST(InMemoryLogReadView, ContiguousBatchesAreOneZeroCopyView) {
    InMemoryLog log(1 << 20, 1024, FullPolicy::DropOldest);
    const auto b0 = batchAt(0), b1 = batchAt(1), b2 = batchAt(2);
    log.appendNoSync(b0.data(), b0.size(), 0);
    log.appendNoSync(b1.data(), b1.size(), 1);
    log.appendNoSync(b2.data(), b2.size(), 2);
    const auto v = log.readView(0, 1 << 20);
    ASSERT_FALSE(v.empty());
    EXPECT_EQ(v.len, b0.size() + b1.size() + b2.size());
    EXPECT_EQ(v.baseOffset, 0);
    EXPECT_EQ(v.nextOffset, 3);
    EXPECT_EQ(0, std::memcmp(v.data, b0.data(), b0.size()));
}

TEST(InMemoryLogReadView, CapLimitsToWholeBatchesButFirstAlwaysIncluded) {
    InMemoryLog log(1 << 20, 1024, FullPolicy::DropOldest);
    const auto b0 = batchAt(0), b1 = batchAt(1);
    log.appendNoSync(b0.data(), b0.size(), 0);
    log.appendNoSync(b1.data(), b1.size(), 1);
    const auto v = log.readView(0, static_cast<int32_t>(b0.size() + 1));
    EXPECT_EQ(v.len, b0.size());
    EXPECT_EQ(v.nextOffset, 1);
    const auto tiny = log.readView(0, 1);
    EXPECT_EQ(tiny.len, b0.size()) << "first batch always included (progress guarantee)";
}

TEST(InMemoryLogReadViewRegression, ViewStopsAtRingWrap) {
    const auto b = batchAt(0);
    InMemoryLog log(3 * b.size() + b.size() / 2, 1024, FullPolicy::DropOldest);
    for (int64_t i = 0; i < 4; ++i) {           // the 4th batch wraps to ring offset 0
        const auto x = batchAt(i);
        log.appendNoSync(x.data(), x.size(), i);
    }
    const auto v2 = log.readView(2, 1 << 20);
    EXPECT_EQ(v2.len, b.size()) << "batch 3 is not adjacent to batch 2 after the wrap";
    EXPECT_EQ(v2.nextOffset, 3);
    const auto v3 = log.readView(3, 1 << 20);
    EXPECT_EQ(v3.baseOffset, 3);
    EXPECT_EQ(v3.len, b.size());
}
```

`libs/replication/tests/replication/BatchPrefixTest.cpp`:

```cpp
#include <gtest/gtest.h>
#include <rune/replication/BatchPrefix.h>
#include <rune/protocol/RecordBatch.h>
#include <vector>

using namespace rune::replication;

namespace {
std::vector<uint8_t> threeBatches(std::size_t& one) {
    std::vector<uint8_t> all;
    for (int64_t base = 0; base < 3; ++base) {
        rune::protocol::RecordBatchBuilder b(base, 1000);
        b.append("k", "v", 1000);
        const auto x = b.build();
        one = x.size();
        all.insert(all.end(), x.begin(), x.end());
    }
    return all;
}
}

TEST(BatchPrefix, WholeBatchesUnderCap) {
    std::size_t one = 0;
    const auto all = threeBatches(one);
    const auto p = wholeBatchPrefix(all.data(), all.size(), 2 * one + 1, 1LL << 62);
    EXPECT_EQ(p.bytes, 2 * one);
    EXPECT_EQ(p.endOffset, 2);
}

TEST(BatchPrefix, CeilingStopsBeforeBatchAtOrAboveIt) {
    std::size_t one = 0;
    const auto all = threeBatches(one);
    const auto p = wholeBatchPrefix(all.data(), all.size(), all.size(), /*ceiling=*/1);
    EXPECT_EQ(p.bytes, one);
    EXPECT_EQ(p.endOffset, 1);
}

TEST(BatchPrefixRegression, FirstBatchLargerThanCapYieldsNothing) {
    std::size_t one = 0;
    const auto all = threeBatches(one);
    const auto p = wholeBatchPrefix(all.data(), all.size(), one - 1, 1LL << 62);
    EXPECT_EQ(p.bytes, 0u);
    EXPECT_EQ(p.endOffset, -1);
}

TEST(BatchPrefix, TornTailIgnoredAndNullSafe) {
    std::size_t one = 0;
    auto all = threeBatches(one);
    all.resize(all.size() - 3);
    EXPECT_EQ(wholeBatchPrefix(all.data(), all.size(), all.size(), 1LL << 62).bytes, 2 * one);
    EXPECT_EQ(wholeBatchPrefix(nullptr, 0, 100, 1LL << 62).bytes, 0u);
}
```

`libs/replication/tests/replication/ZeroAllocReplicaTest.cpp` (own executable — it replaces global `operator new`):

```cpp
// ZeroAllocReplicaTest.cpp — proves the leader's data-fetch and push paths do
// not allocate (spec §1a.5, §3.5a.2).  Counts every operator new while armed.
#include <gtest/gtest.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <new>
#include <vector>
#include <unistd.h>

namespace {
std::atomic<bool>     g_armed{false};
std::atomic<uint64_t> g_allocs{0};
}
void* operator new(std::size_t n) {
    if (g_armed.load(std::memory_order_relaxed)) g_allocs.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

using namespace rune::replication;
namespace fs = std::filesystem;

namespace {
struct Armed {
    Armed()  { g_allocs.store(0); g_armed.store(true); }
    ~Armed() { g_armed.store(false); }
    uint64_t count() const { return g_allocs.load(); }
};
struct SinkTransport final : PartitionTransport {
    uint64_t bytes = 0, frames = 0;
    void sendFetchRequest(const FetchRequestMsg&) override {}
    void sendFetchResponse(NodeId, const FetchResponseMsg& m) override { bytes += m.records.size(); ++frames; }
};
struct Dir {
    fs::path p = fs::temp_directory_path() / ("rune_zalloc_" + std::to_string(::getpid()));
    Dir() { fs::remove_all(p); fs::create_directories(p); }
    ~Dir() { std::error_code ec; fs::remove_all(p, ec); }
};
PartitionConfig cfg(bool push) {
    PartitionConfig c; c.selfId = 1; c.minIsr = 1; c.pushOnAppend = push;
    c.isrCheckIntervalNs = 1'000'000'000'000LL; c.replicaLagMaxMs = 600'000;
    return c;
}
constexpr int64_t kMs = 1'000'000;
}

TEST(ZeroAllocReplicaRegression, DataFetchAllocatesNothing) {
    Dir d; rune::storage::Log log(d.p.string(), 64LL * 1024 * 1024); log.recover();
    SinkTransport t;
    PartitionReplica leader(cfg(/*push=*/false), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 50; ++i) leader.append(std::span<const uint8_t>(payload));
    leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 0, 1 << 20, 0}, 10 * kMs);   // warm-up
    const uint64_t framesBefore = t.frames;
    uint64_t n = 0;
    {
        Armed a;
        leader.onFetchRequest(FetchRequestMsg{1, 0, 2, 10, 1 << 20, 0}, 20 * kMs);
        n = a.count();
    }
    EXPECT_EQ(t.frames, framesBefore + 1);
    EXPECT_GT(t.bytes, 0u);
    EXPECT_EQ(n, 0u) << "the leader's data-fetch path must not allocate";
}

TEST(ZeroAllocReplicaRegression, PushAllocatesNothing) {
    Dir d; rune::storage::Log log(d.p.string(), 64LL * 1024 * 1024); log.recover();
    SinkTransport t;
    PartitionReplica leader(cfg(/*push=*/true), Role::Leader, 1, {1, 2}, {1, 2}, &log, &t);
    const std::vector<uint8_t> payload(1024, 0x5A);
    for (int i = 0; i < 10; ++i) leader.append(std::span<const uint8_t>(payload));
    leader.doWork(1 * kMs);                                       // warm-up push
    for (int i = 0; i < 10; ++i) leader.append(std::span<const uint8_t>(payload));
    const uint64_t framesBefore = t.frames;
    uint64_t n = 0;
    {
        Armed a;
        leader.doWork(2 * kMs);                                   // pushes [10,20)
        n = a.count();
    }
    EXPECT_EQ(t.frames, framesBefore + 1);
    EXPECT_EQ(n, 0u) << "the leader's push path (doWork) must not allocate";
}
```

If `PushAllocatesNothing` still counts allocations after Step 3, locate each one (`lldb ./rune_replication_zero_alloc_tests -o 'b operator new' -o 'run'` then `bt` while `g_armed` is true) and remove it in the same task (spec §1a.12 fix on sight).

Transport (append to `WorkerReplicationTransportTest.cpp`; add to `SharedFakePubRegistry` and to `MinimalFakeRegistry` in `ReplicaTransportKindTest.cpp`):

```cpp
    int64_t sendGatherToPeer(NodeId peer, const uint8_t* prefix, std::size_t prefixLen,
                             std::span<const uint8_t> body) noexcept
    {
        std::vector<uint8_t> frame(prefix, prefix + prefixLen);
        frame.insert(frame.end(), body.begin(), body.end());
        return sendToPeer(peer, frame.data(), frame.size());
    }
```

```cpp
TEST(WorkerReplicationTransportRegression, GatherSendIsByteIdenticalForLargeRecords)
{
    auto h = makeTestPair(/*workerId=*/0, {2});
    const std::vector<uint8_t> big(64 * 1024, 0xC3);
    FetchResponseMsg resp{};
    resp.leaderEpoch = 3; resp.partition = 1; resp.highWatermark = 9;
    resp.errorCode = 0; resp.topicId = 4; resp.records = std::span<const uint8_t>(big);
    h.transport->sendFetchResponse(2, resp);
    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_EQ(h.pubs.offersTo(2)[0], encodeExpectedFetchResponse(resp));
    h.pubs.clear();
    h.transport->sendReconcileResponse(2, resp);
    ASSERT_EQ(h.pubs.offersTo(2).size(), 1u);
    EXPECT_EQ(peekTemplateId(h.pubs.offersTo(2)[0].data(), 0), kReconcileFetchRespTemplateId);
}
```

CMake: add `tests/storage/InMemoryLogReadViewTest.cpp` to `add_executable(rune_in_memory_log_tests ...)`; add `tests/replication/BatchPrefixTest.cpp` to `rune_replication_tests`; and in the `BUILD_TESTING` block of `libs/replication/CMakeLists.txt`:

```cmake
    add_executable(rune_replication_zero_alloc_tests
        tests/replication/ZeroAllocReplicaTest.cpp)
    target_link_libraries(rune_replication_zero_alloc_tests PRIVATE rune_replication GTest::gtest_main)
    gtest_discover_tests(rune_replication_zero_alloc_tests)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_in_memory_log_tests rune_replication_tests rune_replication_zero_alloc_tests rune_worker_replication_transport_tests`
Expected: FAIL to compile — `'readView' is not a member of 'rune::storage::InMemoryLog'` and `rune/replication/BatchPrefix.h: No such file or directory`. (Building `rune_replication_zero_alloc_tests` alone at this point compiles and FAILS at runtime: `ZeroAllocReplicaRegression.DataFetchAllocatesNothing` reports ≥ 1 allocation from `L.read(...)`'s vector.)

- [ ] **Step 3: Implement**

`libs/storage/include/rune/storage/LogReadView.h`:

```cpp
#pragma once
#include <cstddef>
#include <cstdint>

namespace rune::storage {

/// Zero-copy view over WHOLE record batches (disk: a segment's read mmap;
/// memory: the InMemoryLog ring).  Valid until the next append/truncate on the
/// owning log's thread — consume synchronously.
struct LogReadView {
    const uint8_t* data       = nullptr;
    std::size_t    len        = 0;
    int64_t        baseOffset = 0;
    int64_t        nextOffset = 0;
    bool empty() const noexcept { return data == nullptr || len == 0; }
};

} // namespace rune::storage
```

In `LogSegment.h` delete the `struct LogReadView { ... };` block and add `#include <rune/storage/LogReadView.h>`. In `InMemoryLog.h` add `#include <rune/storage/LogReadView.h>` and, after `read()`:

```cpp
    /// Zero-copy read (spec §3.5a.2): the CONTIGUOUS run of whole batches starting
    /// at the batch containing startOffset, capped at maxBytes — the first batch is
    /// always included (same progress guarantee as LogSegment::readView).  Stops at
    /// a ring wrap (next batch not adjacent); the caller continues from nextOffset.
    LogReadView readView(int64_t startOffset, int32_t maxBytes) noexcept {
        if (startOffset < logStartOffset_ || entryCount_ == 0) return {};
        std::size_t lo = 0, hi = entryCount_;
        while (lo < hi) {
            const std::size_t mid = lo + (hi - lo) / 2;
            if (entryAt(mid).lastOffset < startOffset) lo = mid + 1; else hi = mid;
        }
        if (lo == entryCount_) return {};
        const Entry& first = entryAt(lo);
        LogReadView v;
        v.data       = ring_.data() + first.ringOffset;
        v.len        = first.length;
        v.baseOffset = first.baseOffset;
        v.nextOffset = first.lastOffset + 1;
        const std::size_t cap = maxBytes > 0 ? static_cast<std::size_t>(maxBytes) : 0;
        std::size_t end = first.ringOffset + first.length;
        for (std::size_t i = lo + 1; i < entryCount_; ++i) {
            const Entry& e = entryAt(i);
            if (e.ringOffset != end) break;                   // wrapped: not contiguous
            if (cap == 0 || v.len + e.length > cap) break;
            v.len += e.length;
            end   += e.length;
            v.nextOffset = e.lastOffset + 1;
        }
        return v;
    }
```

`libs/replication/include/rune/replication/BatchPrefix.h`:

```cpp
#pragma once
// BatchPrefix.h — the ONE whole-batch walk shared by push and fetch (GR-1).
#include <rune/protocol/RecordBatch.h>
#include <rune/replication/Types.h>

#include <cstddef>
#include <cstdint>

namespace rune::replication {

struct BatchPrefix {
    std::size_t bytes     = 0;    ///< bytes of the whole-batch prefix (0 = nothing fits)
    Offset      endOffset = -1;   ///< last included batch's lastOffset + 1 (-1 if none)
};

/// Largest prefix of [data, data+len) made of WHOLE batches, total <= cap, every
/// batch's baseOffset < ceiling.  A torn tail is ignored.  Pure, allocation-free.
[[nodiscard]] inline BatchPrefix wholeBatchPrefix(const uint8_t* data, std::size_t len,
                                                  std::size_t cap, Offset ceiling) noexcept
{
    BatchPrefix out;
    const std::size_t kOverhead =
        static_cast<std::size_t>(rune::protocol::batch::kRecordBatchOverhead);
    std::size_t pos = 0;
    while (data != nullptr && pos + kOverhead <= len) {
        rune::protocol::RecordBatchReader r(data + pos, len - pos);
        const int32_t total = r.totalLength();
        if (total <= 0) break;
        if (r.baseOffset() >= ceiling) break;
        const std::size_t next = pos + static_cast<std::size_t>(total);
        if (next > len || next > cap) break;
        out.bytes     = next;
        out.endOffset = r.baseOffset() + r.lastOffsetDelta() + 1;
        pos = next;
    }
    return out;
}

} // namespace rune::replication
```

`PartitionReplica.cpp` (add `#include <rune/replication/BatchPrefix.h>`):

`onFetchRequest` — replace from `std::vector<uint8_t> logBytes;` through the `transport` send at the end of the function with:

```cpp
    // Zero-copy: a view into the log (disk mmap / memory ring).  The transport
    // copies the bytes ONCE, straight into the publication term.
    std::span<const uint8_t> records{};
    if (serve == FetchServe::Data && req.fetchOffset < logEndOffset_()) {
        const rune::storage::LogReadView v =
            onLog([&](auto& L) { return L.readView(req.fetchOffset, req.maxBytes); });
        // GR-1: whole batches only and never above req.maxBytes (the transport's
        // frame ceiling); a first batch larger than the cap is not sent (as before).
        const BatchPrefix pre = wholeBatchPrefix(v.data, v.len,
                                                 static_cast<std::size_t>(req.maxBytes),
                                                 kNoPushCeiling);
        records = std::span<const uint8_t>(v.data, pre.bytes);
    }

    // DR-BUG-3: advertise the min-ISR-GATED committed HW (unchanged comment block kept above).
    const Offset advertiseHw = highWatermark();
    auto resp = makeFetchResponse(advertiseHw, kErrNone, records);
    onTransport([&](auto& t) { t.sendFetchResponse(req.followerId, resp); });

    return kErrNone;
```

`pushToFollowers` — replace from `++rtPushes_;` through the end of the function with:

```cpp
    ++rtPushes_;
    const rune::storage::LogReadView v =
        onLog([&](auto& L) { return L.readView(fromOffset, cfg_.maxFetchBytes); });
    // Advance lastPushedOffset_ by ONLY what is actually pushed (ceiling + cap bound).
    const BatchPrefix pre = wholeBatchPrefix(v.data, v.len,
                                             static_cast<std::size_t>(cfg_.maxFetchBytes),
                                             pushCeiling);
    if (pre.bytes == 0) return;   // nothing deliverable within ceiling/cap this pass
    const Offset pushedThrough = pre.endOffset;

    // DR-BUG-3: advertise the min-ISR-gated HW (see onFetchRequest).
    auto resp = makeFetchResponse(highWatermark(), kErrNone,
                                  std::span<const uint8_t>(v.data, pre.bytes));
    rtPushBytes_ += static_cast<int64_t>(pre.bytes);

    for (NodeId followerId : isrList) {
        if (followerId == cfg_.selfId) continue;
        onTransport([&](auto& t) { t.sendFetchResponse(followerId, resp); });
        if (FollowerSlot* s = followerSlots_.find(followerId)) {
            if (pushedThrough > s->pushedThrough) s->pushedThrough = pushedThrough;
        }
    }
    lastPushedOffset_ = pushedThrough;
```

`WorkerReplicationTransport.h` — `AeronPubRegistry`: replace the `std::unordered_map` member with a flat array and route every lookup through `findPub_`:

```cpp
    struct PeerPub {
        rune::replication::NodeId                    id = -1;
        std::shared_ptr<aeron::ExclusivePublication> pub;
    };
    static constexpr std::size_t kMaxPeers = FollowerSlots::kMaxReplicas;
    std::array<PeerPub, kMaxPeers> pubs_{};
    std::size_t                    pubCount_ = 0;

    [[nodiscard]] aeron::ExclusivePublication* findPub_(rune::replication::NodeId id) const noexcept
    {
        for (std::size_t i = 0; i < pubCount_; ++i) if (pubs_[i].id == id) return pubs_[i].pub.get();
        return nullptr;
    }
```

(constructor: `if (pubCount_ < kMaxPeers) pubs_[pubCount_++] = PeerPub{it->peerId, std::move(pub)};` instead of `pubs_.emplace(...)`; `allResolved_ = pending.empty() && pubCount_ > 0 && peerChannels.size() <= kMaxPeers;`; `maxMessageLength()`/`broadcastToPeers()` iterate `pubs_[0..pubCount_)`; `sendToPeer` uses `findPub_`; `hasPeer` returns `findPub_(peer) != nullptr`; add `#include <array>`, `#include <span>`, `#include <rune/replication/FollowerSlots.h>`). Add:

```cpp
    // Zero-copy send of [prefix | body]: body bytes are copied exactly once, from
    // the log view into the term buffer.  tryClaim for <= maxPayloadLength, else a
    // 2-buffer gather offer() (Aeron fragments it) — no intermediate scratch copy.
    int64_t sendGatherToPeer(rune::replication::NodeId peer,
                             const uint8_t* prefix, std::size_t prefixLen,
                             std::span<const uint8_t> body) noexcept
    {
        aeron::ExclusivePublication* pub = findPub_(peer);
        if (!pub) return -1;
        const std::size_t len = prefixLen + body.size();
        if (len > static_cast<std::size_t>(pub->maxMessageLength())) {
            spdlog::error("WorkerReplicationTransport: drop oversized frame len={} max={}",
                          len, pub->maxMessageLength());
            return -1;
        }
        if (len <= static_cast<std::size_t>(pub->maxPayloadLength())) {
            aeron::concurrent::logbuffer::BufferClaim claim;
            const int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), claim);
            if (pos > 0) {
                uint8_t* dst = claim.buffer().buffer() + claim.offset();
                std::memcpy(dst, prefix, prefixLen);
                if (!body.empty()) std::memcpy(dst + prefixLen, body.data(), body.size());
                claim.commit();
            }
            return pos;
        }
        const std::array<aeron::concurrent::AtomicBuffer, 2> parts{
            aeron::concurrent::AtomicBuffer(const_cast<uint8_t*>(prefix),
                                            static_cast<aeron::util::index_t>(prefixLen)),
            aeron::concurrent::AtomicBuffer(const_cast<uint8_t*>(body.data()),
                                            static_cast<aeron::util::index_t>(body.size()))};
        return pub->offer(parts);
    }
```

`WorkerReplicationTransportImpl::sendFetchResponseImpl_` becomes (and delete the `scratchBuf_` member):

```cpp
    void sendFetchResponseImpl_(rune::replication::NodeId peer,
                                const rune::replication::FetchResponseMsg& resp,
                                bool reconcile) noexcept
    {
        namespace rr = rune::replication;
        static const uint8_t kNoRecords = 0;
        alignas(8) uint8_t prefix[kFetchResponsePrefixMax];
        const std::size_t prefixLen = rr::FetchResponseMsg::encodedSize(0);
        // Encode header + fixed block + a zero-length records prefix, then patch the
        // real length: the body is handed over separately (never copied here).
        rr::FetchResponseMsg::encode(prefix, 0, resp.leaderEpoch, resp.partition,
                                     resp.highWatermark, resp.logStartOffset,
                                     resp.errorCode,
                                     std::span<const uint8_t>(&kNoRecords, 0),
                                     resp.topicId);
        rr::detail::write_le<uint32_t>(prefix + prefixLen - rr::FetchResponseMsg::VAR_HEADER_SIZE,
                                       static_cast<uint32_t>(resp.records.size()));
        if (reconcile) rr::stampTemplateId(prefix, 0, rr::kReconcileFetchRespTemplateId);
        pubReg_.sendGatherToPeer(peer, prefix, prefixLen, resp.records);
    }
    static constexpr std::size_t kFetchResponsePrefixMax = 64;   // encodedSize(0) == 46
```

- [ ] **Step 4: Run to verify they pass**

Run:
```bash
cmake --build build-fast
ctest --test-dir build-fast -R Regression --output-on-failure
./build-fast/libs/storage/rune_in_memory_log_tests
./build-fast/libs/replication/rune_replication_tests --gtest_filter='BatchPrefix*:NoDupFetchGate*'
./build-fast/libs/replication/rune_replication_zero_alloc_tests
./build-fast/libs/node/rune_worker_replication_transport_tests
ctest --test-dir build-fast -E 'udp|UDP|Udp' --output-on-failure -j4
ctest --test-dir build-fast -R 'PerWorkerReplication|GatewayFailover|GatewayProduceReplication' --output-on-failure
```
Expected: PASS, including `ZeroAllocReplicaRegression.*` with 0 allocations.

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/LogReadView.h libs/storage/include/rune/storage/LogSegment.h \
        libs/storage/include/rune/storage/InMemoryLog.h libs/storage/tests/storage/InMemoryLogReadViewTest.cpp \
        libs/storage/CMakeLists.txt libs/replication/include/rune/replication/BatchPrefix.h \
        libs/replication/include/rune/replication/WorkerReplicationTransport.h \
        libs/replication/src/PartitionReplica.cpp libs/replication/CMakeLists.txt \
        libs/replication/tests/replication/BatchPrefixTest.cpp \
        libs/replication/tests/replication/ZeroAllocReplicaTest.cpp \
        libs/node/tests/node/WorkerReplicationTransportTest.cpp libs/node/tests/node/ReplicaTransportKindTest.cpp
git commit -m "perf(replication): zero-copy push/fetch via readView + gather send into the publication

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 6: Shard configuration, per-worker ports, collision validation, hard startup refusal

Spec §3.3. New config: `RUNE_WORKER_MODE=legacy|shard`, `RUNE_RUNTIME_PLACEMENT=thread|fused`, `RUNE_WORKER_INGRESS_BASE_PORT`, `RUNE_WORKER_REPL_BASE_PORT`; `RUNE_PEERS` entries accept `id@host:consensusPort[:clientPort[:replPort[:replBasePort[:ingressBasePort]]]]`. Spec ambiguity resolved: the gateway of *any* node answers Metadata and must advertise the *leader's* worker ingress endpoint, and "no node derives another node's ports" — so every peer's `ingressBasePort` is explicit in `RUNE_PEERS` next to `replBasePort`. Unknown mode/placement strings are a parse error (no silent fallback). Shard mode forces per-worker ingress, replication and egress on (it is built on them). Workers are capped at 64 in shard mode (the runtime→worker announcement mask, Task 13, is a `uint64_t`).

**Files:**
- Modify: `libs/node/include/rune/node/NodeConfig.h` (`PeerConfig` ~line 128; `NodeConfig` after `replBaseStreamId` ~line 194; declarations near `validateStreamRanges` ~line 509)
- Modify: `libs/node/src/NodeConfig.cpp` (`parsePeer` ~line 76-150; `parseNodeConfig` after `RUNE_PER_WORKER_REPLICATION` ~line 940; self `workerCount` stamping ~line 948; new functions after `validateStreamRanges`)
- Modify: `libs/node/src/BrokerNode.cpp:64` (`start()` — refuse before any side effect)
- Test: `libs/node/tests/node/NodeConfigTest.cpp` (append), `libs/node/tests/node/BrokerNodeTest.cpp` (append)

**Interfaces:**
- Produces:
  - `enum class NodeConfig::WorkerMode : uint8_t { Legacy = 0, Shard = 1 };` `NodeConfig::WorkerMode workerMode = WorkerMode::Legacy;`
  - `enum class NodeConfig::RuntimePlacement : uint8_t { Thread = 0, Fused = 1 };` `RuntimePlacement runtimePlacement = RuntimePlacement::Thread;`
  - `int32_t NodeConfig::workerIngressBasePort = 0; int32_t NodeConfig::workerReplBasePort = 0;` (0 = unset)
  - `uint16_t PeerConfig::replBasePort = 0; uint16_t PeerConfig::ingressBasePort = 0;`
  - `bool NodeConfig::shardMode() const noexcept; bool NodeConfig::fused() const noexcept;`
  - `std::optional<std::string> NodeConfig::validateShardConfig() const;`
  - `std::string NodeConfig::clientBindHost() const; std::string NodeConfig::shardIngressBindChannel(int32_t w) const; std::string NodeConfig::shardReplBindChannel(int32_t w) const; static std::string NodeConfig::shardPeerReplChannel(const PeerConfig&, int32_t w); static std::string NodeConfig::shardIngressEndpoint(const PeerConfig&, int32_t w);`
  - `static constexpr int32_t NodeConfig::kMaxShardWorkers = 64;`

- [ ] **Step 1: Write the failing tests** (append inside `namespace rune::node` in `NodeConfigTest.cpp`)

```cpp
// ---------------------------------------------------------------------------
// Self-contained worker shards (spec §3.3): config + validation + refusal.
// ---------------------------------------------------------------------------
static std::map<std::string, std::string> shard3node(int self = 0) {
    return {
        {"RUNE_NODE_ID",          std::to_string(self)},
        {"RUNE_PEERS",            "0@127.0.0.1:40110:40200:40220:40600:40500,"
                                  "1@127.0.0.1:40111:40201:40221:40620:40520,"
                                  "2@127.0.0.1:40112:40202:40222:40640:40540"},
        {"RUNE_CLIENT_ENDPOINT",  "0.0.0.0:" + std::to_string(40200 + self)},
        {"RUNE_DATA_DIR",         "/data/rune"},
        {"RUNE_WORKERS",          "1"},
        {"RUNE_WORKER_MODE",      "shard"},
        {"RUNE_WORKER_INGRESS_BASE_PORT", std::to_string(40500 + 20 * self)},
        {"RUNE_WORKER_REPL_BASE_PORT",    std::to_string(40600 + 20 * self)},
        {"RUNE_WORKER_EGRESS_CONTROL_BASE_PORT", std::to_string(40400 + 20 * self)},
    };
}

TEST(NodeConfig, DefaultsAreLegacyThreadAndValid) {
    auto r = parseNodeConfig(fakeEnv(base1node()));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    EXPECT_EQ(r.config->workerMode, NodeConfig::WorkerMode::Legacy);
    EXPECT_EQ(r.config->runtimePlacement, NodeConfig::RuntimePlacement::Thread);
    EXPECT_EQ(r.config->workerIngressBasePort, 0);
    EXPECT_EQ(r.config->workerReplBasePort, 0);
    EXPECT_FALSE(r.config->validateShardConfig().has_value());
}

TEST(NodeConfig, ParsesShardModePlacementPortsAndPeerBases) {
    auto env = shard3node(1);
    env["RUNE_RUNTIME_PLACEMENT"] = "fused";
    auto r = parseNodeConfig(fakeEnv(env));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    const auto& c = *r.config;
    EXPECT_TRUE(c.shardMode());
    EXPECT_TRUE(c.fused());
    EXPECT_EQ(c.workerIngressBasePort, 40520);
    EXPECT_EQ(c.workerReplBasePort, 40620);
    EXPECT_TRUE(c.perWorkerIngressEnabled);
    EXPECT_TRUE(c.perWorkerReplication);
    EXPECT_TRUE(c.perWorkerEgressEnabled);
    EXPECT_EQ(c.peers[2].replBasePort, 40640);
    EXPECT_EQ(c.peers[2].ingressBasePort, 40540);
    EXPECT_EQ(c.shardIngressBindChannel(0), "aeron:udp?endpoint=0.0.0.0:40520");
    EXPECT_EQ(c.shardReplBindChannel(0), "aeron:udp?endpoint=127.0.0.1:40620");
    EXPECT_EQ(NodeConfig::shardPeerReplChannel(c.peers[2], 0), "aeron:udp?endpoint=127.0.0.1:40640");
    EXPECT_EQ(NodeConfig::shardIngressEndpoint(c.peers[0], 0), "127.0.0.1:40500");
    EXPECT_FALSE(c.validateShardConfig().has_value()) << *c.validateShardConfig();
}

TEST(NodeConfigRegression, UnknownWorkerModeIsParseError) {
    auto env = shard3node(); env["RUNE_WORKER_MODE"] = "shards";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
    EXPECT_NE(r.error.find("RUNE_WORKER_MODE"), std::string::npos);
}

TEST(NodeConfigRegression, UnknownRuntimePlacementIsParseError) {
    auto env = shard3node(); env["RUNE_RUNTIME_PLACEMENT"] = "fuse";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
    EXPECT_NE(r.error.find("RUNE_RUNTIME_PLACEMENT"), std::string::npos);
}

TEST(NodeConfigRegression, PeerEntryWithSixPortsRejected) {
    auto env = base3node();
    env["RUNE_PEERS"] = "0@h:1:2:3:4:5:6,1@h:11,2@h:12";
    auto r = parseNodeConfig(fakeEnv(env));
    EXPECT_FALSE(r.config.has_value());
}

static std::optional<std::string> shardErr(std::map<std::string, std::string> env) {
    auto r = parseNodeConfig(fakeEnv(std::move(env)));
    if (!r.config) return r.error;
    return r.config->validateShardConfig();
}

TEST(ShardConfigRegression, MissingIngressBasePortRefused) {
    auto env = shard3node(); env.erase("RUNE_WORKER_INGRESS_BASE_PORT");
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_WORKER_INGRESS_BASE_PORT"), std::string::npos);
}

TEST(ShardConfigRegression, MissingPeerReplBasePortRefused) {
    auto env = shard3node();
    env["RUNE_PEERS"] = "0@127.0.0.1:40110:40200:40220:40600:40500,"
                        "1@127.0.0.1:40111:40201:40221:40620:40520,"
                        "2@127.0.0.1:40112:40202:40222";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("peer 2"), std::string::npos);
    EXPECT_NE(e->find("replBasePort"), std::string::npos);
}

TEST(ShardConfigRegression, SelfPeerEntryDisagreeingWithEnvRefused) {
    auto env = shard3node(); env["RUNE_WORKER_REPL_BASE_PORT"] = "40700";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_WORKER_REPL_BASE_PORT"), std::string::npos);
}

TEST(ShardConfigRegression, OwnPortCollisionRefusedNamingBothPorts) {
    auto env = shard3node(); env["RUNE_WORKER_EGRESS_CONTROL_BASE_PORT"] = "40500";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("40500"), std::string::npos);
    EXPECT_NE(e->find("workerIngress"), std::string::npos);
    EXPECT_NE(e->find("workerEgressControl"), std::string::npos);
}

TEST(ShardConfigRegression, CoLocatedPeerRangeOverlapRefused) {
    auto env = shard3node(0);
    env["RUNE_PEERS"] = "0@127.0.0.1:40110:40200:40220:40600:40500,"
                        "1@127.0.0.1:40111:40201:40221:40620:40500,"   // same ingress base as node 0
                        "2@127.0.0.1:40112:40202:40222:40640:40540";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("co-located peer 1"), std::string::npos);
    EXPECT_NE(e->find("40500"), std::string::npos);
}

TEST(ShardConfigRegression, AsymmetricWorkerCountRefused) {
    auto r = parseNodeConfig(fakeEnv(shard3node()));
    ASSERT_TRUE(r.config.has_value()) << r.error;
    auto c = *r.config;
    c.peers[1].workerCount = 2;               // peer advertises 2, self runs 1
    auto e = c.validateShardConfig();
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("worker count"), std::string::npos);
}

TEST(ShardConfigRegression, PortRangeOverflowRefused) {
    auto env = shard3node(); env["RUNE_WORKERS"] = "2";
    env["RUNE_WORKER_INGRESS_BASE_PORT"] = "65535";
    env["RUNE_PEERS"] = "0@127.0.0.1:40110:40200:40220:40600:65535,"
                        "1@127.0.0.1:40111:40201:40221:40620:40520,"
                        "2@127.0.0.1:40112:40202:40222:40640:40540";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_WORKER_INGRESS_BASE_PORT"), std::string::npos);
}

TEST(ShardConfigRegression, FusedRequiresShard) {
    auto env = base1node(); env["RUNE_RUNTIME_PLACEMENT"] = "fused";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("fused requires"), std::string::npos);
}

TEST(ShardConfigRegression, MoreThanSixtyFourShardWorkersRefused) {
    auto env = shard3node(); env["RUNE_WORKERS"] = "65";
    auto e = shardErr(env);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("64"), std::string::npos);
}
```

Append to `BrokerNodeTest.cpp`:

```cpp
TEST(BrokerNodeRegression, ShardInvalidConfigRefusesStartBeforeAnySideEffect) {
    const fs::path dir = makeTempDataDir("shardbad_");
    NodeConfig cfg = makeSingleNodeConfig(dir);
    cfg.workerMode = NodeConfig::WorkerMode::Shard;     // no per-worker ports configured
    BrokerNode node{cfg};
    EXPECT_THROW(node.start(), std::runtime_error);
    EXPECT_FALSE(fs::exists(cfg.effectiveAeronDir())) << "refusal must precede the driver start";
    std::error_code ec; fs::remove_all(dir, ec);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_node_tests rune_broker_node_tests`
Expected: FAIL to compile — `'WorkerMode' is not a member of 'rune::node::NodeConfig'`.

- [ ] **Step 3: Implement**

`NodeConfig.h` — in `PeerConfig` after `workerCount`:

```cpp
    // Self-contained worker shards (spec §3.3): this peer's explicit per-worker base
    // ports.  Worker w listens on replBasePort + w (replication) and
    // ingressBasePort + w (client produce).  0 = not advertised (legacy peers).
    uint16_t replBasePort    = 0;
    uint16_t ingressBasePort = 0;
```

in `NodeConfig` after `replBaseStreamId`:

```cpp
    // Self-contained worker shards (spec §3.1-§3.3).  Defaults = today's behaviour.
    enum class WorkerMode : uint8_t { Legacy = 0, Shard = 1 };          // RUNE_WORKER_MODE
    enum class RuntimePlacement : uint8_t { Thread = 0, Fused = 1 };    // RUNE_RUNTIME_PLACEMENT
    WorkerMode       workerMode       = WorkerMode::Legacy;
    RuntimePlacement runtimePlacement = RuntimePlacement::Thread;
    int32_t workerIngressBasePort = 0;   // RUNE_WORKER_INGRESS_BASE_PORT (0 = unset)
    int32_t workerReplBasePort    = 0;   // RUNE_WORKER_REPL_BASE_PORT    (0 = unset)
    static constexpr int32_t kMaxShardWorkers = 64;   // runtime→worker announce mask width
    [[nodiscard]] bool shardMode() const noexcept { return workerMode == WorkerMode::Shard; }
    [[nodiscard]] bool fused() const noexcept { return runtimePlacement == RuntimePlacement::Fused; }
```

next to `validateStreamRanges()`:

```cpp
    /// Shard mode: every port this node binds + co-located peers' advertised
    /// ranges, peer base ports, symmetric worker count, stream ranges.  nullopt =
    /// valid (always nullopt in legacy mode unless placement=fused).  BrokerNode
    /// refuses to start on an error (no silent fallback).
    std::optional<std::string> validateShardConfig() const;

    /// Host part of clientEndpoint.channel (the client-facing bind host).
    std::string clientBindHost() const;
    std::string shardIngressBindChannel(int32_t w) const;  // aeron:udp?endpoint=<clientBindHost>:<ingressBase+w>
    std::string shardReplBindChannel(int32_t w) const;     // aeron:udp?endpoint=<self.host>:<replBase+w>
    static std::string shardPeerReplChannel(const PeerConfig& p, int32_t w);  // <p.host>:<p.replBasePort+w>
    static std::string shardIngressEndpoint(const PeerConfig& p, int32_t w);  // "<p.host>:<p.ingressBasePort+w>"
```

`NodeConfig.cpp` — replace `parsePeer` (keep its doc comment, updated to the 5-port grammar) with:

```cpp
static std::string parsePeer(std::string_view entry, PeerConfig& out) {
    auto at = entry.find('@');
    if (at == std::string_view::npos)
        return std::string("malformed peer entry (missing '@'): ") + std::string(entry);
    std::string_view idPart   = trim(entry.substr(0, at));
    std::string_view hostPort = trim(entry.substr(at + 1));
    int32_t id{};
    if (!parseInt32(idPart, id))
        return std::string("peer id is not a valid integer in entry: ") + std::string(entry);
    auto firstColon = hostPort.find(':');
    if (firstColon == std::string_view::npos)
        return std::string("malformed peer entry (missing ':' in host:port): ") + std::string(entry);
    std::string_view host = trim(hostPort.substr(0, firstColon));
    if (host.empty())
        return std::string("peer host is empty in entry: ") + std::string(entry);

    // consensusPort[:clientPort[:replicationPort[:replBasePort[:ingressBasePort]]]]
    static constexpr const char* kNames[5] = {
        "consensusPort", "clientPort", "replicationPort", "replBasePort", "ingressBasePort"};
    std::array<std::string_view, 5> parts{};
    std::size_t n = 0;
    std::string_view rem = trim(hostPort.substr(firstColon + 1));
    for (;;) {
        if (n == parts.size())
            return std::string("too many ':' port segments (max 5) in peer entry: ") + std::string(entry);
        const auto c = rem.find(':');
        parts[n++] = trim(c == std::string_view::npos ? rem : rem.substr(0, c));
        if (c == std::string_view::npos) break;
        rem = rem.substr(c + 1);
    }
    std::array<uint16_t, 5> ports{};
    for (std::size_t i = 0; i < n; ++i) {
        if (!parsePort(parts[i], ports[i]))
            return std::string("peer ") + kNames[i] + " is invalid or out of range in entry: " +
                   std::string(entry);
    }
    out.id              = id;
    out.host            = std::string(host);
    out.consensusPort   = ports[0];
    out.clientPort      = ports[1];
    out.replicationPort = ports[2];
    out.replBasePort    = ports[3];
    out.ingressBasePort = ports[4];
    return {};
}
```

(add `#include <array>` and `#include <optional>`). In `parseNodeConfig`, after the `RUNE_PER_WORKER_REPLICATION` block:

```cpp
    // --- Self-contained worker shards (spec §3.1-§3.3) ---
    auto lower = [](std::string s) {
        for (auto& ch : s) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
        return s;
    };
    if (const char* raw = getenv_fn("RUNE_WORKER_MODE"); raw && *raw) {
        const std::string s = lower(std::string(trim(raw)));
        if (s == "legacy")     cfg.workerMode = NodeConfig::WorkerMode::Legacy;
        else if (s == "shard") cfg.workerMode = NodeConfig::WorkerMode::Shard;
        else return {{}, std::string("RUNE_WORKER_MODE must be 'legacy' or 'shard', got: ") + raw};
    }
    if (const char* raw = getenv_fn("RUNE_RUNTIME_PLACEMENT"); raw && *raw) {
        const std::string s = lower(std::string(trim(raw)));
        if (s == "thread")     cfg.runtimePlacement = NodeConfig::RuntimePlacement::Thread;
        else if (s == "fused") cfg.runtimePlacement = NodeConfig::RuntimePlacement::Fused;
        else return {{}, std::string("RUNE_RUNTIME_PLACEMENT must be 'thread' or 'fused', got: ") + raw};
    }
    if (const char* raw = getenv_fn("RUNE_WORKER_INGRESS_BASE_PORT"); raw && *raw) {
        if (!parseInt32(raw, cfg.workerIngressBasePort))
            return {{}, std::string("RUNE_WORKER_INGRESS_BASE_PORT is not a valid integer: ") + raw};
    }
    if (const char* raw = getenv_fn("RUNE_WORKER_REPL_BASE_PORT"); raw && *raw) {
        if (!parseInt32(raw, cfg.workerReplBasePort))
            return {{}, std::string("RUNE_WORKER_REPL_BASE_PORT is not a valid integer: ") + raw};
    }
    if (cfg.shardMode()) {
        // A shard IS per-worker ingress + replication + egress on the worker's own driver.
        cfg.perWorkerIngressEnabled = true;
        cfg.perWorkerReplication    = true;
        cfg.perWorkerEgressEnabled  = true;
    }
```

and change the self-peer stamping guard to `if (cfg.workerCount > 1 || cfg.shardMode()) {`. After `validateStreamRanges()` add:

```cpp
std::string NodeConfig::clientBindHost() const {
    const std::string_view ch = clientEndpoint.channel;
    const auto ep = ch.find("endpoint=");
    if (ep == std::string_view::npos) return "0.0.0.0";
    std::string_view hp = ch.substr(ep + 9);
    if (const auto bar = hp.find('|'); bar != std::string_view::npos) hp = hp.substr(0, bar);
    const auto colon = hp.rfind(':');
    return std::string(colon == std::string_view::npos ? hp : hp.substr(0, colon));
}
std::string NodeConfig::shardIngressBindChannel(int32_t w) const {
    return "aeron:udp?endpoint=" + clientBindHost() + ":" + std::to_string(workerIngressBasePort + w);
}
std::string NodeConfig::shardReplBindChannel(int32_t w) const {
    return "aeron:udp?endpoint=" + self().host + ":" + std::to_string(workerReplBasePort + w);
}
std::string NodeConfig::shardPeerReplChannel(const PeerConfig& p, int32_t w) {
    return "aeron:udp?endpoint=" + p.host + ":" + std::to_string(static_cast<int32_t>(p.replBasePort) + w);
}
std::string NodeConfig::shardIngressEndpoint(const PeerConfig& p, int32_t w) {
    return p.host + ":" + std::to_string(static_cast<int32_t>(p.ingressBasePort) + w);
}

std::optional<std::string> NodeConfig::validateShardConfig() const {
    if (fused() && !shardMode())
        return std::string("RUNE_RUNTIME_PLACEMENT=fused requires RUNE_WORKER_MODE=shard");
    if (!shardMode()) return std::nullopt;

    const int32_t W = workerCount >= 1 ? workerCount : 1;
    if (W > kMaxShardWorkers)
        return "shard mode supports at most " + std::to_string(kMaxShardWorkers) +
               " workers per node, got RUNE_WORKERS=" + std::to_string(W);
    auto baseOk = [W](int32_t base) { return base >= 1 && base + W - 1 <= 65535; };
    if (!baseOk(workerIngressBasePort))
        return "shard mode requires RUNE_WORKER_INGRESS_BASE_PORT in [1, " + std::to_string(65536 - W) +
               "], got " + std::to_string(workerIngressBasePort);
    if (!baseOk(workerReplBasePort))
        return "shard mode requires RUNE_WORKER_REPL_BASE_PORT in [1, " + std::to_string(65536 - W) +
               "], got " + std::to_string(workerReplBasePort);

    for (const auto& p : peers) {
        if (p.replBasePort == 0)
            return "shard mode requires replBasePort for peer " + std::to_string(p.id) +
                   " in RUNE_PEERS (id@host:consensus:client:repl:replBase:ingressBase)";
        if (p.ingressBasePort == 0)
            return "shard mode requires ingressBasePort for peer " + std::to_string(p.id) +
                   " in RUNE_PEERS (id@host:consensus:client:repl:replBase:ingressBase)";
        if (p.id != nodeId && p.workerCount != 0 && p.workerCount != W)
            return "shard mode requires a symmetric worker count: peer " + std::to_string(p.id) +
                   " advertises " + std::to_string(p.workerCount) + ", self has " + std::to_string(W);
    }
    const PeerConfig& me = self();
    if (me.replBasePort != workerReplBasePort)
        return "RUNE_WORKER_REPL_BASE_PORT=" + std::to_string(workerReplBasePort) +
               " disagrees with self RUNE_PEERS replBasePort=" + std::to_string(me.replBasePort);
    if (me.ingressBasePort != workerIngressBasePort)
        return "RUNE_WORKER_INGRESS_BASE_PORT=" + std::to_string(workerIngressBasePort) +
               " disagrees with self RUNE_PEERS ingressBasePort=" + std::to_string(me.ingressBasePort);

    struct Bind { int32_t port; std::string what; };
    std::vector<Bind> binds;
    binds.push_back({me.consensusPort, "consensusPort"});
    if (me.clientPort != 0)      binds.push_back({me.clientPort, "clientPort"});
    if (me.replicationPort != 0) binds.push_back({me.replicationPort, "replicationPort"});
    for (int32_t w = 0; w < W; ++w) {
        const std::string tag = "[w" + std::to_string(w) + "]";
        binds.push_back({workerIngressBasePort + w, "workerIngress" + tag});
        binds.push_back({workerReplBasePort + w, "workerRepl" + tag});
        binds.push_back({workerEgressControlBasePort + w, "workerEgressControl" + tag});
    }
    for (std::size_t i = 0; i < binds.size(); ++i)
        for (std::size_t j = i + 1; j < binds.size(); ++j)
            if (binds[i].port == binds[j].port)
                return "port collision: " + binds[i].what + " and " + binds[j].what +
                       " both bind " + std::to_string(binds[i].port);

    for (const auto& p : peers) {
        if (p.id == nodeId || p.host != me.host) continue;       // only co-located peers can collide
        std::vector<Bind> theirs{{p.consensusPort, "consensusPort"}};
        if (p.clientPort != 0)      theirs.push_back({p.clientPort, "clientPort"});
        if (p.replicationPort != 0) theirs.push_back({p.replicationPort, "replicationPort"});
        for (int32_t w = 0; w < W; ++w) {
            theirs.push_back({p.ingressBasePort + w, "workerIngress[w" + std::to_string(w) + "]"});
            theirs.push_back({p.replBasePort + w, "workerRepl[w" + std::to_string(w) + "]"});
        }
        for (const auto& a : binds)
            for (const auto& b : theirs)
                if (a.port == b.port)
                    return "port collision with co-located peer " + std::to_string(p.id) + " (" +
                           p.host + "): our " + a.what + " and its " + b.what + " both use " +
                           std::to_string(a.port);
    }
    return validateStreamRanges();
}
```

`BrokerNode.cpp` — first statements of `start()` after the idempotence guard:

```cpp
    // Spec §3.3: shard-mode misconfiguration is a HARD startup refusal, before any
    // side effect (no dataDir, no driver, no ports).
    if (auto err = config_.validateShardConfig(); err.has_value()) {
        throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) +
                                 "]: refusing to start — " + *err);
    }
```

- [ ] **Step 4: Run to verify they pass**

Run:
```bash
cmake --build build-fast --target rune_node_tests rune_broker_node_tests
./build-fast/libs/node/rune_node_tests
./build-fast/libs/node/rune_broker_node_tests --gtest_filter='BrokerNodeRegression.*:BrokerNodeTest.*'
ctest --test-dir build-fast -R Regression --output-on-failure
```
Expected: PASS (all pre-existing `NodeConfig.*` peer-parse tests still pass: same messages for the first three ports).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/NodeConfig.h libs/node/src/NodeConfig.cpp libs/node/src/BrokerNode.cpp \
        libs/node/tests/node/NodeConfigTest.cpp libs/node/tests/node/BrokerNodeTest.cpp
git commit -m "feat(node): shard-mode config (worker mode, placement, per-worker ports) + hard refusal

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: `EmbeddedDriver` — one driver, one pumping thread; fix the two-thread pumping race

Spec §3.6. Today `BrokerNode::startEmbeddedDriver` starts `driverThread_` (which loops `aeron_driver_main_do_work`) and then its readiness loop **also** calls `aeron_driver_main_do_work` from the caller thread — two threads driving one driver. The loop is also pointless: `aeron_driver_init` writes `cnc.dat` (`aeron_driver.c:864`) and stamps the to-driver heartbeat (`:911`), so a client can connect as soon as init returns.

**Threading-mode decision (source evidence, Aeron 1.41.4, `~/.conan2/p/b/aeronb73a55f36892b/b/src/aeron-driver/src/main/c/`):** `aeron_driver_init` (`aeron_driver.c:939-1030`) builds agent runners per mode: `INVOKER` and `SHARED` share one runner (`runners[0]`, `"[conductor, sender, receiver]"`, `aeron_driver_shared_do_work` = conductor + sender + receiver, `:712-722`); `SHARED_NETWORK` adds a `[sender, receiver]` runner and `DEDICATED` adds separate sender and receiver runners. `aeron_driver_start(driver, manual_main_loop=true)` (`:1046-1082`) only makes `runners[0]` manual and then **starts a thread for every other initialised runner (`for i = 1..MAX: if INITED → aeron_agent_start`)**, so DEDICATED/SHARED_NETWORK always spawn sender/receiver threads even with a manual main loop. `aeron_driver_main_do_work` (`:1091-1100`) runs only `runners[0]`. `INVOKER` additionally **rejects** `manual_main_loop=false` (`:1054-1059`) and makes the conductor/receiver proxies call the agents directly (`AERON_THREADING_MODE_IS_SHARED_OR_INVOKER`, `aeron_driver_conductor_proxy.c`, `aeron_driver_receiver_proxy.c`). **Shard drivers therefore use `AERON_THREADING_MODE_INVOKER`**: the whole duty cycle runs inside `aeron_driver_main_do_work` on the owning thread and no driver thread exists. The node-level driver keeps today's configurable SHARED/DEDICATED + dedicated thread in `thread` placement (Task 11 switches it to INVOKER for `fused`).

**Files:**
- Create: `libs/node/include/rune/node/EmbeddedDriver.h`, `libs/node/src/EmbeddedDriver.cpp`
- Modify: `libs/node/CMakeLists.txt` (`src/EmbeddedDriver.cpp` into `rune_node`; new `rune_embedded_driver_tests`)
- Modify: `libs/node/include/rune/node/BrokerNode.h:615-618` (driver members → `std::unique_ptr<EmbeddedDriver> nodeDriver_;` + `driverForTest()`)
- Modify: `libs/node/src/BrokerNode.cpp:2399-2610` (`startEmbeddedDriver` / `stopEmbeddedDriver` bodies)
- Create: `libs/node/tests/node/EmbeddedDriverTest.cpp`

**Interfaces:**
- Consumes: Task 1 `effectiveAeronDir()`.
- Produces:
```cpp
struct EmbeddedDriverConfig {
    std::string dir; int32_t termBufferBytes; int32_t mtuBytes; int32_t rcvWindowBytes;
    int32_t socketBufferBytes; NodeConfig::AeronThreadingMode threadingMode;
    std::string idleStrategy; bool deleteDirOnShutdown = true;
    static EmbeddedDriverConfig fromNode(const NodeConfig&, std::string dir);
};
enum class DriverMode : uint8_t { Threaded, Invoker };
class EmbeddedDriver {
public:
    EmbeddedDriver(EmbeddedDriverConfig, DriverMode);   // throws std::runtime_error
    ~EmbeddedDriver();
    int  doWork() noexcept;              // Invoker: pumps iff caller is the bound thread; Threaded: never pumps (counts)
    void bindPumpThread() noexcept;      // the calling thread becomes the only pumper (Invoker)
    void close() noexcept;               // idempotent; caller must be the (now only) pumper
    [[nodiscard]] DriverMode mode() const noexcept;
    [[nodiscard]] const std::string& dir() const noexcept;
    [[nodiscard]] uint64_t foreignPumpAttempts() const noexcept;
    [[nodiscard]] uint64_t callerPumpsWhileThreadRunning() const noexcept;
};
[[nodiscard]] std::optional<std::string> checkAeronSpace(uint64_t freeBytes, int32_t drivers, int32_t termBufferBytes);
EmbeddedDriver* BrokerNode::driverForTest() const noexcept;
```

- [ ] **Step 1: Write the failing tests** — `libs/node/tests/node/EmbeddedDriverTest.cpp`

```cpp
// EmbeddedDriverTest.cpp — one embedded driver is driven by exactly one thread
// (spec §3.6); invoker mode has no driver thread; stale/active dir handling.
#include <rune/node/EmbeddedDriver.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unistd.h>

using namespace rune::node;
namespace fs = std::filesystem;

namespace {
struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               ("rune_edrv_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
EmbeddedDriverConfig cfgAt(const fs::path& p) {
    EmbeddedDriverConfig c;
    c.dir = p.string();
    c.termBufferBytes = 64 * 1024;
    return c;
}
std::shared_ptr<aeron::Aeron> connect(const fs::path& p, bool invoker) {
    aeron::Context ctx;
    ctx.aeronDir(p.string());
    ctx.useConductorAgentInvoker(invoker);
    return aeron::Aeron::connect(ctx);
}
}

TEST(EmbeddedDriverRegression, ThreadedStartNeverPumpsFromCaller) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Threaded);
    EXPECT_EQ(drv.callerPumpsWhileThreadRunning(), 0u)
        << "startup must not drive the driver from the caller while its thread runs";
    auto a = connect(d.path, /*invoker=*/false);
    ASSERT_TRUE(a) << "cnc.dat exists after init — connect needs no caller pumping";
    EXPECT_EQ(drv.doWork(), 0) << "a threaded driver may never be pumped by a second thread";
    EXPECT_EQ(drv.foreignPumpAttempts(), 1u);
    a.reset();
}

TEST(EmbeddedDriver, InvokerModeRoundTripOnOneThreadOnly) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    auto a = connect(d.path, /*invoker=*/true);
    ASSERT_TRUE(a);
    const int64_t sreg = a->addSubscription("aeron:ipc", 9001);
    const int64_t preg = a->addExclusivePublication("aeron:ipc", 9001);
    std::shared_ptr<aeron::Subscription> sub;
    std::shared_ptr<aeron::ExclusivePublication> pub;
    for (int i = 0; i < 100'000 && !(sub && pub && pub->isConnected()); ++i) {
        drv.doWork();
        a->conductorAgentInvoker().invoke();
        if (!sub) sub = a->findSubscription(sreg);
        if (!pub) pub = a->findExclusivePublication(preg);
    }
    ASSERT_TRUE(sub && pub && pub->isConnected()) << "invoker driver must progress when pumped";
    uint8_t msg[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    aeron::concurrent::AtomicBuffer ab(msg, sizeof(msg));
    ASSERT_GT(pub->offer(ab, 0, sizeof(msg)), 0);
    int got = 0;
    for (int i = 0; i < 100'000 && got == 0; ++i) {
        drv.doWork();
        a->conductorAgentInvoker().invoke();
        got += sub->poll([](aeron::concurrent::AtomicBuffer&, aeron::util::index_t,
                            aeron::util::index_t, aeron::Header&) {}, 10);
    }
    EXPECT_EQ(got, 1);
    EXPECT_EQ(drv.foreignPumpAttempts(), 0u);
    sub.reset(); pub.reset(); a.reset();
}

TEST(EmbeddedDriverRegression, InvokerForeignThreadPumpIsRefused) {
    TmpDir d;
    EmbeddedDriver drv(cfgAt(d.path), DriverMode::Invoker);
    int r = -1;
    std::thread t([&] { r = drv.doWork(); });
    t.join();
    EXPECT_EQ(r, 0);
    EXPECT_EQ(drv.foreignPumpAttempts(), 1u);
}

TEST(EmbeddedDriverRegression, StaleInactiveDirIsReclaimed) {
    TmpDir d;
    {
        auto c = cfgAt(d.path);
        c.deleteDirOnShutdown = false;
        EmbeddedDriver a(c, DriverMode::Invoker);
        a.doWork();
    }   // closed: conductor nulls its heartbeat (aeron_driver_conductor.c:3103), cnc.dat left behind
    ASSERT_TRUE(fs::exists(d.path / "cnc.dat"));
    EXPECT_NO_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker));
}

TEST(EmbeddedDriverRegression, ActiveDirIsRefused) {
    TmpDir d;
    EmbeddedDriver a(cfgAt(d.path), DriverMode::Invoker);
    a.doWork();
    EXPECT_THROW(EmbeddedDriver b(cfgAt(d.path), DriverMode::Invoker), std::runtime_error);
}

TEST(EmbeddedDriverRegression, AeronSpaceCheck) {
    EXPECT_FALSE(checkAeronSpace(1ULL << 30, 2, 4 * 1024 * 1024).has_value());
    auto e = checkAeronSpace(8ULL * 1024 * 1024, 2, 4 * 1024 * 1024);
    ASSERT_TRUE(e.has_value());
    EXPECT_NE(e->find("RUNE_AERON_TERM_BUFFER_BYTES"), std::string::npos);
}
```

CMake (inside the `BUILD_BROKER_NODE_TESTS` / `aeron_FOUND` block that defines `rune_broker_node_tests`):

```cmake
            add_executable(rune_embedded_driver_tests tests/node/EmbeddedDriverTest.cpp)
            target_link_libraries(rune_embedded_driver_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_embedded_driver_tests
                PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
```

and add `src/EmbeddedDriver.cpp` to `add_library(rune_node ...)`.

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_embedded_driver_tests`
Expected: FAIL to compile — `rune/node/EmbeddedDriver.h: No such file or directory`.

- [ ] **Step 3: Extract verbatim (red), then fix (green)**

Create `libs/node/include/rune/node/EmbeddedDriver.h`:

```cpp
#pragma once
// EmbeddedDriver.h — one embedded Aeron C media driver, driven by EXACTLY ONE
// thread (spec §3.6).
//   Threaded: today's node driver — a dedicated std::thread loops the main duty
//             cycle; no other thread may pump it (doWork() refuses and counts).
//   Invoker : AERON_THREADING_MODE_INVOKER — conductor+sender+receiver run ONLY
//             inside doWork() on the bound thread (shard worker / fused runtime).
extern "C" {
#include <aeronmd/aeronmd.h>
}
#include <rune/node/NodeConfig.h>

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <thread>

namespace rune::node {

struct EmbeddedDriverConfig {
    std::string dir;
    int32_t     termBufferBytes   = 4 * 1024 * 1024;
    int32_t     mtuBytes          = 1408;
    int32_t     rcvWindowBytes    = 128 * 1024;
    int32_t     socketBufferBytes = 2 * 1024 * 1024;
    NodeConfig::AeronThreadingMode threadingMode = NodeConfig::AeronThreadingMode::Shared;
    std::string idleStrategy      = "sleep-ns";
    bool        deleteDirOnShutdown = true;

    static EmbeddedDriverConfig fromNode(const NodeConfig& c, std::string dir);
};

enum class DriverMode : uint8_t { Threaded, Invoker };

/// tmpfs budget: every driver needs at least one full log (3 terms + metadata).
[[nodiscard]] std::optional<std::string>
checkAeronSpace(uint64_t freeBytes, int32_t drivers, int32_t termBufferBytes);

class EmbeddedDriver {
public:
    EmbeddedDriver(EmbeddedDriverConfig cfg, DriverMode mode);
    ~EmbeddedDriver();
    EmbeddedDriver(const EmbeddedDriver&)            = delete;
    EmbeddedDriver& operator=(const EmbeddedDriver&) = delete;

    int  doWork() noexcept;
    void bindPumpThread() noexcept { pumpOwner_ = std::this_thread::get_id(); }
    void close() noexcept;

    [[nodiscard]] DriverMode         mode() const noexcept { return mode_; }
    [[nodiscard]] const std::string& dir() const noexcept { return cfg_.dir; }
    [[nodiscard]] uint64_t foreignPumpAttempts() const noexcept { return foreignPumpAttempts_; }
    [[nodiscard]] uint64_t callerPumpsWhileThreadRunning() const noexcept { return callerPumpsWhileThreadRunning_; }

private:
    int callerPump_() noexcept;   // any pump NOT made by the driver thread

    EmbeddedDriverConfig    cfg_;
    DriverMode              mode_;
    aeron_driver_context_t* ctx_    = nullptr;
    aeron_driver_t*         driver_ = nullptr;
    std::atomic<bool>       running_{false};   // Threaded: stop flag for the driver thread
    std::thread             thread_;
    std::thread::id         pumpOwner_{};
    uint64_t                foreignPumpAttempts_           = 0;
    uint64_t                callerPumpsWhileThreadRunning_ = 0;
};

} // namespace rune::node
```

Create `libs/node/src/EmbeddedDriver.cpp` — first move today's `startEmbeddedDriver` body **verbatim** into the constructor (context setup, init, start, thread, and the readiness loop, with each `aeron_driver_main_do_work(driver_)` inside the readiness loop replaced by `callerPump_()`), so Step 4 shows the regression red:

```cpp
#include <rune/node/EmbeddedDriver.h>

#include <spdlog/spdlog.h>

#include <stdexcept>

namespace rune::node {

EmbeddedDriverConfig EmbeddedDriverConfig::fromNode(const NodeConfig& c, std::string dir) {
    EmbeddedDriverConfig e;
    e.dir               = std::move(dir);
    e.termBufferBytes   = c.aeronTermBufferBytes;
    e.mtuBytes          = c.aeronMtuBytes;
    e.rcvWindowBytes    = c.aeronRcvWindowBytes;
    e.socketBufferBytes = c.aeronSocketBufferBytes;
    e.threadingMode     = c.aeronThreadingMode;
    e.idleStrategy      = c.driverIdleStrategy;
    return e;
}

std::optional<std::string> checkAeronSpace(uint64_t freeBytes, int32_t drivers, int32_t termBufferBytes) {
    const uint64_t perDriver = 3ULL * static_cast<uint64_t>(termBufferBytes) + 64ULL * 1024;
    const uint64_t need      = perDriver * static_cast<uint64_t>(drivers);
    if (freeBytes >= need) return std::nullopt;
    return "aeron dir has " + std::to_string(freeBytes) + " bytes free but " + std::to_string(drivers) +
           " driver(s) need at least " + std::to_string(need) +
           " (3 x RUNE_AERON_TERM_BUFFER_BYTES + 64 KiB each) — lower RUNE_AERON_TERM_BUFFER_BYTES, "
           "fewer RUNE_WORKERS, or a larger tmpfs";
}

int EmbeddedDriver::callerPump_() noexcept {
    if (running_.load(std::memory_order_acquire)) ++callerPumpsWhileThreadRunning_;
    return aeron_driver_main_do_work(driver_);
}

EmbeddedDriver::EmbeddedDriver(EmbeddedDriverConfig cfg, DriverMode mode)
    : cfg_(std::move(cfg)), mode_(mode)
{
    if (aeron_driver_context_init(&ctx_) < 0)
        throw std::runtime_error(std::string("EmbeddedDriver: aeron_driver_context_init failed: ") +
                                 aeron_errmsg());
    if (aeron_is_driver_active(cfg_.dir.c_str(), /*timeout_ms=*/1000, +[](const char*) {})) {
        aeron_driver_context_close(ctx_);
        ctx_ = nullptr;
        throw std::runtime_error(
            "EmbeddedDriver: aeron dir '" + cfg_.dir + "' is already in use by an ACTIVE media "
            "driver (a prior broker instance may still be running, or was killed without clean "
            "shutdown). Use a distinct RUNE_AERON_DIR/RUNE_DATA_DIR, or ensure the previous "
            "instance exited.");
    }
    aeron_driver_context_set_dir(ctx_, cfg_.dir.c_str());
    const char* idle = cfg_.idleStrategy.c_str();
    if (mode_ == DriverMode::Invoker) {
        // INVOKER: SHARED runner layout, manual main loop REQUIRED, no agent thread
        // (aeron_driver.c:939-956, 1046-1082).  DEDICATED / SHARED_NETWORK would still
        // spawn sender/receiver threads under a manual main loop.
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_INVOKER);
    } else if (cfg_.threadingMode == NodeConfig::AeronThreadingMode::Dedicated) {
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_DEDICATED);
        aeron_driver_context_set_conductor_idle_strategy(ctx_, idle);
        aeron_driver_context_set_sender_idle_strategy(ctx_, idle);
        aeron_driver_context_set_receiver_idle_strategy(ctx_, idle);
    } else {
        aeron_driver_context_set_threading_mode(ctx_, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_shared_idle_strategy(ctx_, idle);
    }
    aeron_driver_context_set_dir_delete_on_start(ctx_, true);
    aeron_driver_context_set_dir_delete_on_shutdown(ctx_, cfg_.deleteDirOnShutdown);
    aeron_driver_context_set_term_buffer_sparse_file(ctx_, true);
    aeron_driver_context_set_term_buffer_length(ctx_, static_cast<uint64_t>(cfg_.termBufferBytes));
    aeron_driver_context_set_mtu_length(ctx_, static_cast<size_t>(cfg_.mtuBytes));
    aeron_driver_context_set_rcv_initial_window_length(ctx_, static_cast<size_t>(cfg_.rcvWindowBytes));
    if (cfg_.socketBufferBytes > 0) {
        aeron_driver_context_set_socket_so_sndbuf(ctx_, static_cast<size_t>(cfg_.socketBufferBytes));
        aeron_driver_context_set_socket_so_rcvbuf(ctx_, static_cast<size_t>(cfg_.socketBufferBytes));
    }
    if (aeron_driver_init(&driver_, ctx_) < 0) {
        aeron_driver_context_close(ctx_);
        ctx_ = nullptr;
        throw std::runtime_error(std::string("EmbeddedDriver: aeron_driver_init failed: ") + aeron_errmsg());
    }
    if (aeron_driver_start(driver_, /*manual_main_loop=*/true) < 0) {
        aeron_driver_close(driver_);
        aeron_driver_context_close(ctx_);
        driver_ = nullptr;
        ctx_    = nullptr;
        throw std::runtime_error(std::string("EmbeddedDriver: aeron_driver_start failed: ") + aeron_errmsg());
    }
    if (mode_ == DriverMode::Threaded) {
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] {
            while (running_.load(std::memory_order_acquire))
                aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
        });
        // VERBATIM COPY of the old readiness loop (RED step — removed below).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{200};
        while (std::chrono::steady_clock::now() < deadline) {
            callerPump_();
            if (std::filesystem::exists(cfg_.dir + "/cnc.dat")) { callerPump_(); callerPump_(); break; }
            std::this_thread::sleep_for(std::chrono::microseconds{500});
        }
    } else {
        pumpOwner_ = std::this_thread::get_id();
    }
    spdlog::info("EmbeddedDriver: started (dir={}, mode={})", cfg_.dir,
                 mode_ == DriverMode::Invoker ? "invoker" : "threaded");
}

int EmbeddedDriver::doWork() noexcept {
    if (mode_ == DriverMode::Threaded || std::this_thread::get_id() != pumpOwner_) {
        ++foreignPumpAttempts_;          // a driver is driven by exactly one thread
        return 0;
    }
    return aeron_driver_main_do_work(driver_);
}

void EmbeddedDriver::close() noexcept {
    if (mode_ == DriverMode::Threaded && running_.exchange(false, std::memory_order_acq_rel)) {
        if (thread_.joinable()) thread_.join();
    }
    // The driver thread (if any) has exited: the closing thread is now the ONLY
    // pumper.  Drain the client-close commands still queued in the CnC ring before
    // aeron_driver_close (prevents the conductor on_close use-after-free).
    if (driver_) {
        constexpr int kMaxDrainPumps = 10'000;
        int idle = 0;
        for (int i = 0; i < kMaxDrainPumps && idle < 2; ++i)
            idle = (aeron_driver_main_do_work(driver_) == 0) ? idle + 1 : 0;
        if (aeron_driver_close(driver_) < 0)
            spdlog::warn("EmbeddedDriver: aeron_driver_close error (dir={}): {}", cfg_.dir, aeron_errmsg());
        driver_ = nullptr;
    }
    if (ctx_) {
        if (aeron_driver_context_close(ctx_) < 0)
            spdlog::warn("EmbeddedDriver: aeron_driver_context_close error (dir={}): {}", cfg_.dir, aeron_errmsg());
        ctx_ = nullptr;
    }
}

EmbeddedDriver::~EmbeddedDriver() { close(); }

} // namespace rune::node
```

(add `#include <chrono>` and `#include <filesystem>` for the verbatim loop). Build and run `rune_embedded_driver_tests --gtest_filter='EmbeddedDriverRegression.ThreadedStartNeverPumpsFromCaller'`: **expected FAIL** (`callerPumpsWhileThreadRunning() >= 1`) — the race reproduced. Now delete the verbatim readiness loop (the block from `// VERBATIM COPY` through its closing `}`), replace it with the comment

```cpp
        // No caller-side readiness pumping: aeron_driver_init already wrote cnc.dat
        // (aeron_driver.c:864) and stamped the heartbeat (:911); only thread_ drives.
```

and drop the now-unused `<chrono>`/`<filesystem>` includes.

`BrokerNode.h`: replace `driverCtx_`, `driver_`, `driverRunning_`, `driverThread_` with

```cpp
    std::unique_ptr<EmbeddedDriver> nodeDriver_;   // node-level driver (runtime / legacy workers)
```

add `#include <rune/node/EmbeddedDriver.h>` and the accessor `EmbeddedDriver* driverForTest() const noexcept { return nodeDriver_.get(); }`. `BrokerNode.cpp`:

```cpp
void BrokerNode::startEmbeddedDriver() {
    nodeDriver_ = std::make_unique<EmbeddedDriver>(
        EmbeddedDriverConfig::fromNode(config_, config_.effectiveAeronDir()),
        DriverMode::Threaded);
}

void BrokerNode::stopEmbeddedDriver() noexcept {
    nodeDriver_.reset();   // joins the driver thread, drains, closes (EmbeddedDriver::close)
}
```

Append to `BrokerNodeTest.cpp`:

```cpp
TEST(BrokerNodeRegression, StartNeverPumpsNodeDriverFromCallerThread) {
    const fs::path dir = makeTempDataDir("drvrace_");
    BrokerNode node{makeSingleNodeConfig(dir)};
    node.start();
    ASSERT_NE(node.driverForTest(), nullptr);
    EXPECT_EQ(node.driverForTest()->callerPumpsWhileThreadRunning(), 0u);
    EXPECT_EQ(node.driverForTest()->foreignPumpAttempts(), 0u);
    node.stop();
    std::error_code ec; fs::remove_all(dir, ec);
}
```

- [ ] **Step 4: Run to verify they pass (default path end to end)**

Run:
```bash
cmake --build build-fast
./build-fast/libs/node/rune_embedded_driver_tests
./build-fast/libs/node/rune_broker_node_tests
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast --output-on-failure -j2
```
Expected: PASS (every embedded-driver suite — BrokerNode, gateway, UDP cluster, per-worker replication — now starts through `EmbeddedDriver`).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/EmbeddedDriver.h libs/node/src/EmbeddedDriver.cpp \
        libs/node/include/rune/node/BrokerNode.h libs/node/src/BrokerNode.cpp \
        libs/node/CMakeLists.txt libs/node/tests/node/EmbeddedDriverTest.cpp libs/node/tests/node/BrokerNodeTest.cpp
git commit -m "fix(node): EmbeddedDriver — one pumping thread per driver; remove two-thread startup pumping race

Invoker mode (AERON_THREADING_MODE_INVOKER, no driver threads) for shard drivers.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: `AeronPump` — "add, then pump this shard's own driver + conductor until found"

Spec §3.6. Every startup resolve loop that sleeps while a background driver/conductor thread makes progress (`WorkerIngress.h:84-95`, `WorkerEgress.h:63-74`, `WorkerReplicationTransport.h` `AeronPubRegistry` ctor + inbound subscription, `UdpMessageSink.h:88-94`) takes an `AeronPump`. In invoker mode the resolving thread IS the driver: it pumps the driver (`EmbeddedDriver::doWork`) and the client conductor (`conductorAgentInvoker().invoke()`) between attempts and never sleeps; with the default pump (threaded mode) the loop keeps today's 1 ms sleep — the default path is unchanged. `AeronPump` is a header-only INTERFACE library (`rune_aeron_util`) so consensus, replication, gateway and node can all use it (Task 11 extends it to the runtime's objects).

**Files:**
- Create: `libs/aeronutil/CMakeLists.txt`, `libs/aeronutil/include/rune/aeronutil/AeronPump.h`
- Modify: `CMakeLists.txt:81` (`add_subdirectory(libs/aeronutil)` before `add_subdirectory(libs/metadata)`)
- Modify: `libs/replication/CMakeLists.txt`, `libs/node/CMakeLists.txt` (link `rune_aeron_util`)
- Modify: `libs/node/include/rune/node/EmbeddedDriver.h` (`pumpWith`)
- Modify: `libs/node/include/rune/node/WorkerIngress.h`, `libs/node/include/rune/node/WorkerEgress.h`, `libs/node/include/rune/node/UdpMessageSink.h`, `libs/replication/include/rune/replication/WorkerReplicationTransport.h`
- Create: `libs/node/tests/node/AeronPumpInvokerTest.cpp` (+ target `rune_aeron_pump_tests`)

**Interfaces:**
- Consumes: Task 7 `EmbeddedDriver`.
- Produces:
```cpp
namespace rune::aeronutil {
class AeronPump {
public:
    using DriverPumpFn = int (*)(void*) noexcept;
    AeronPump() noexcept = default;                                   // threaded: no pumping
    AeronPump(DriverPumpFn fn, void* driverCtx, aeron::Aeron* client) noexcept;
    [[nodiscard]] bool invoker() const noexcept;
    int pump() noexcept;
    template <class Find> auto resolve(Find&& find, std::chrono::milliseconds budget) -> decltype(find());
    [[nodiscard]] uint64_t pumps() const noexcept;
};
}
rune::aeronutil::AeronPump EmbeddedDriver::pumpWith(aeron::Aeron* client) noexcept;
WorkerIngress(std::shared_ptr<aeron::Aeron>, std::string channel, int32_t streamId, rune::aeronutil::AeronPump pump = {}, std::chrono::milliseconds budget = std::chrono::milliseconds{5000}) noexcept;
WorkerEgress(std::shared_ptr<aeron::Aeron>, std::string controlEndpoint, int32_t streamId, rune::aeronutil::AeronPump pump = {}, std::chrono::milliseconds budget = std::chrono::milliseconds{5000}) noexcept;
AeronPubRegistry(std::shared_ptr<aeron::Aeron>, const std::vector<std::pair<NodeId,std::string>>&, int32_t peerStreamId, rune::aeronutil::AeronPump pump = {}) noexcept;
WorkerReplicationTransportImpl(std::shared_ptr<aeron::Aeron>, int32_t workerId, std::string inboundChannel, int32_t inboundStreamId, std::vector<std::pair<NodeId,std::string>> peerChannels, int32_t peerStreamId, rune::aeronutil::AeronPump pump = {}) noexcept;
UdpMessageSink(std::shared_ptr<aeron::Aeron>, const std::unordered_map<NodeId, PeerEndpoint>&, rune::aeronutil::AeronPump pump = {});
```

- [ ] **Step 1: Write the failing tests** — `libs/node/tests/node/AeronPumpInvokerTest.cpp`

```cpp
// AeronPumpInvokerTest.cpp — with NO driver thread and NO client conductor thread,
// every startup resolve completes by pumping its own driver + conductor (spec §3.6).
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/UdpMessageSink.h>
#include <rune/node/WorkerEgress.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerReplicationTransport.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <string>

using namespace rune::node;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {
uint16_t freeUdpPort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    socklen_t len = sizeof(a);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}

struct InvokerShard {
    fs::path dir;
    std::unique_ptr<EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron> client;
    InvokerShard() {
        static std::atomic<int> seq{0};
        dir = fs::temp_directory_path() / ("rune_pump_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024;
        driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(dir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
    }
    ~InvokerShard() { client.reset(); driver.reset(); std::error_code ec; fs::remove_all(dir, ec); }
    rune::aeronutil::AeronPump pump() { return driver->pumpWith(client.get()); }
};
}

TEST(AeronPumpInvoker, WorkerIngressResolvesByPumpingOwnDriver) {
    InvokerShard s;
    WorkerIngress ing(s.client, "aeron:ipc", 7001, s.pump());
    EXPECT_TRUE(ing.resolved());
}

TEST(AeronPumpInvokerRegression, WithoutPumpInvokerResolveCannotProgress) {
    InvokerShard s;
    WorkerIngress ing(s.client, "aeron:ipc", 7002, rune::aeronutil::AeronPump{}, 50ms);
    EXPECT_FALSE(ing.resolved()) << "nothing drives an invoker driver unless the resolver pumps it";
}

TEST(AeronPumpInvoker, WorkerEgressResolvesByPumpingOwnDriver) {
    InvokerShard s;
    WorkerEgress eg(s.client, "127.0.0.1:" + std::to_string(freeUdpPort()), 7003, s.pump());
    EXPECT_TRUE(eg.resolved());
}

TEST(AeronPumpInvoker, ReplicationTransportResolvesSubAndPeerPubs) {
    InvokerShard s;
    WorkerReplicationTransport t(s.client, 0, "aeron:ipc", 7004,
                                 {{2, std::string("aeron:ipc")}}, 7004, s.pump());
    EXPECT_TRUE(t.resolved());
}

TEST(AeronPumpInvoker, UdpMessageSinkResolvesPeers) {
    InvokerShard s;
    std::unordered_map<rune::consensus::NodeId, UdpMessageSink::PeerEndpoint> peers{
        {2, {"aeron:ipc", 7005}}};
    UdpMessageSink sink(s.client, peers, s.pump());
    EXPECT_EQ(sink.unresolvedForTest(), 0) << "all peer publications resolved with no background thread";
}
```

`resolvePending()` is private in `UdpMessageSink` (`UdpMessageSink.h:173`); add this public test accessor next to the other public members in this task:

```cpp
    /// Test-only: number of peer publications still unresolved (runs one resolve probe).
    [[nodiscard]] int unresolvedForTest() noexcept { return resolvePending(); }
```

CMake (next to `rune_embedded_driver_tests`):

```cmake
            add_executable(rune_aeron_pump_tests tests/node/AeronPumpInvokerTest.cpp)
            target_link_libraries(rune_aeron_pump_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_aeron_pump_tests PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_aeron_pump_tests`
Expected: FAIL to compile — `rune/aeronutil/AeronPump.h: No such file or directory` / `no member named 'pumpWith'`.

- [ ] **Step 3: Implement**

`libs/aeronutil/CMakeLists.txt`:

```cmake
# rune_aeron_util — header-only Aeron helpers shared by consensus/replication/
# gateway/node (spec §3.6 invoker pumping).  INTERFACE: no sources.
add_library(rune_aeron_util INTERFACE)
target_include_directories(rune_aeron_util INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rune_aeron_util INTERFACE aeron::aeron)
target_compile_features(rune_aeron_util INTERFACE cxx_std_23)
```

`CMakeLists.txt`: insert `add_subdirectory(libs/aeronutil)` immediately before `add_subdirectory(libs/metadata)`. Link: `target_link_libraries(rune_replication PUBLIC rune_aeron_util)` (in the `aeron_FOUND` block added in Task 4) and `rune_aeron_util` in `rune_node`'s `target_link_libraries(rune_node PUBLIC ...)` list.

`libs/aeronutil/include/rune/aeronutil/AeronPump.h`:

```cpp
#pragma once
// AeronPump.h — spec §3.6: "add, then pump THIS shard's own driver + conductor
// until found or deadline".  Startup/control path only (a function pointer is
// fine here; nothing on the per-record path calls it).
//   default-constructed (threaded driver + conductor threads): resolve() sleeps
//     1 ms between attempts — exactly the legacy behaviour;
//   invoker (driver + client conductor owned by the resolving thread): resolve()
//     pumps both between attempts and never sleeps.
#include <Aeron.h>

#include <chrono>
#include <cstdint>
#include <thread>

namespace rune::aeronutil {

class AeronPump {
public:
    using DriverPumpFn = int (*)(void*) noexcept;

    AeronPump() noexcept = default;
    AeronPump(DriverPumpFn fn, void* driverCtx, aeron::Aeron* client) noexcept
        : fn_(fn), ctx_(driverCtx), client_(client) {}

    [[nodiscard]] bool invoker() const noexcept { return fn_ != nullptr; }

    int pump() noexcept {
        if (!fn_) return 0;
        ++pumps_;
        int w = fn_(ctx_);
        if (client_) w += client_->conductorAgentInvoker().invoke();
        return w;
    }

    template <class Find>
    auto resolve(Find&& find, std::chrono::milliseconds budget) -> decltype(find()) {
        using clock = std::chrono::steady_clock;
        const auto deadline = clock::now() + budget;
        for (;;) {
            auto r = find();
            if (r || clock::now() >= deadline) return r;
            if (invoker()) pump();
            else           std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
    }

    [[nodiscard]] uint64_t pumps() const noexcept { return pumps_; }

private:
    DriverPumpFn  fn_     = nullptr;
    void*         ctx_    = nullptr;
    aeron::Aeron* client_ = nullptr;
    uint64_t      pumps_  = 0;
};

} // namespace rune::aeronutil
```

`EmbeddedDriver.h`: add `#include <rune/aeronutil/AeronPump.h>` and, public:

```cpp
    /// A pump that drives THIS driver (via doWork's single-thread check) + `client`'s conductor.
    [[nodiscard]] rune::aeronutil::AeronPump pumpWith(aeron::Aeron* client) noexcept {
        return rune::aeronutil::AeronPump(&EmbeddedDriver::pumpTrampoline_, this, client);
    }
```
private: `static int pumpTrampoline_(void* self) noexcept { return static_cast<EmbeddedDriver*>(self)->doWork(); }`

`WorkerIngress.h` (add `#include <rune/aeronutil/AeronPump.h>`) — constructor signature and resolve:

```cpp
    WorkerIngress(std::shared_ptr<aeron::Aeron> aeron,
                  std::string                   channel,
                  int32_t                       streamId,
                  rune::aeronutil::AeronPump    pump   = {},
                  std::chrono::milliseconds     budget = std::chrono::milliseconds{kResolveMaxMs}) noexcept
```

```cpp
        if (!aeron_) return;
        const int64_t regId = aeron_->addSubscription(channel_, streamId_);
        sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); }, budget);
```

(delete `kResolveSliceMs`). `WorkerEgress.h` identically with `findExclusivePublication(regId)` into `pub_`. `UdpMessageSink.h`: add the parameter `rune::aeronutil::AeronPump pump = {}` and replace the 5 s loop with

```cpp
        (void)pump.resolve([&] { return resolvePending() == 0; }, std::chrono::seconds{5});
```

`WorkerReplicationTransport.h` (replication): `AeronPubRegistry` ctor gains `rune::aeronutil::AeronPump pump = {}` and its loop becomes

```cpp
        (void)pump.resolve([&] {
            for (auto it = pending.begin(); it != pending.end(); ) {
                auto pub = aeron->findExclusivePublication(it->regId);
                if (pub) {
                    if (pubCount_ < kMaxPeers) pubs_[pubCount_++] = PeerPub{it->peerId, std::move(pub)};
                    it = pending.erase(it);
                } else {
                    ++it;
                }
            }
            return pending.empty();
        }, std::chrono::milliseconds{kResolveMaxMs});
```

`WorkerReplicationTransportImpl`'s production ctor gains `rune::aeronutil::AeronPump pump = {}` as the last parameter, passes `pump` to `pubReg_(aeron, peerChannels, peerStreamId, pump)`, and resolves its inbound subscription with `sub_ = pump.resolve([&] { return aeron->findSubscription(regId); }, std::chrono::milliseconds{5000});` (pass the pump by value to the registry and reuse the copy for the subscription — `AeronPump` is a trivially copyable 32-byte value).

- [ ] **Step 4: Run to verify they pass**

Run:
```bash
cmake --build build-fast
./build-fast/libs/node/rune_aeron_pump_tests
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast --output-on-failure -j2
```
Expected: PASS (legacy callers use the default pump → unchanged behaviour).

- [ ] **Step 5: Commit**

```bash
git add CMakeLists.txt libs/aeronutil libs/replication/CMakeLists.txt libs/node/CMakeLists.txt \
        libs/node/include/rune/node/EmbeddedDriver.h libs/node/include/rune/node/WorkerIngress.h \
        libs/node/include/rune/node/WorkerEgress.h libs/node/include/rune/node/UdpMessageSink.h \
        libs/replication/include/rune/replication/WorkerReplicationTransport.h \
        libs/node/tests/node/AeronPumpInvokerTest.cpp
git commit -m "feat(node): AeronPump — resolve by pumping the shard's own invoker driver + conductor

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 9: Worker shard — per-worker invoker driver + client, `ShardAgent`, shard-mode wiring

Spec §3.1. In shard mode, `BrokerNode` builds for each worker `w`: `EmbeddedDriver(<aeronDir>/w<w>, Invoker)` + an Aeron client with `useConductorAgentInvoker(true)`; binds the worker's client ingress (`shardIngressBindChannel(w)`, stream `workerIngressBaseStreamId + w`), its replication plane (`shardReplBindChannel(w)` → each peer's `shardPeerReplChannel(peer, w)`, stream `replBaseStreamId + w`) and its record egress to **that** client, resolving everything with the worker's own `AeronPump` on the startup thread; any unresolved per-worker resource is a hard refusal. A `ShardAgent` (final) runs the pass: (1) driver duty cycle, (2) client conductor, (3–6) `Worker::doWork` in shard order. Pinned workers (`RUNE_WORKER_CORE_BASE >= 0`) busy-spin; unpinned shard workers use the backoff idle (principle §1a.3: never busy-spin a shared core). A loop-duration alarm counts passes longer than 100 µs from the injected clock only.

**Files:**
- Modify (rune-fr): `~/Projects/rune-fr/include/rune_fr/Sites.h` (add `ShardPass`)
- Create: `libs/node/include/rune/node/ShardAgent.h`, `libs/node/src/ShardAgent.cpp`
- Modify: `libs/node/include/rune/node/Worker.h` — `class Worker final`; `setShardMode`; `doWork` (~line 590) dispatch; new `doWorkShard_`
- Modify: `libs/node/include/rune/node/WorkerPool.h` — `startShardRunners`
- Modify: `libs/node/include/rune/node/BrokerNode.h` (members + accessors), `libs/node/src/BrokerNode.cpp` (`start()`, new `buildShardRuntimes_()`, `buildReplication()` shard branch ~line 468, `buildWorkerPool()` shard branch ~line 958, `stop()`)
- Modify: `libs/node/CMakeLists.txt` (`src/ShardAgent.cpp`; tests)
- Create: `libs/node/tests/support/ShardTestClient.h`, `libs/node/tests/node/ShardAgentTest.cpp`, `libs/node/tests/integration/ShardModeTest.cpp`

**Interfaces:**
- Consumes: Task 6 config helpers, Task 7 `EmbeddedDriver`/`checkAeronSpace`, Task 8 `AeronPump`/`pumpWith`.
- Produces:
  - `class ShardAgent final : public rune::consensus::Agent { ShardAgent(Worker&, EmbeddedDriver&, aeron::Aeron&) noexcept; int doWork(int64_t nowNs) override; static constexpr int64_t kSlowPassNs = 100'000; uint64_t slowPassesForTest() const noexcept; }`
  - `void Worker::setShardMode(bool on) noexcept; bool Worker::shardMode() const noexcept;`
  - `void WorkerPool::startShardRunners(std::vector<std::unique_ptr<ShardAgent>>& agents, rune::consensus::NanoClock& clock, bool busySpin, int64_t maxParkUs);`
  - `std::size_t BrokerNode::shardDriverCountForTest() const noexcept;`
  - Test support `rune::node::test::ShardTestClient` (connect / produce / awaitProduceResponse / metadata / disconnect), `freeUdpPort()`, `freeUdpPortBlocks(int n, int width)`.

- [ ] **Step 1: Write the failing tests**

`libs/node/tests/support/ShardTestClient.h`:

```cpp
#pragma once
// ShardTestClient.h — a minimal REAL wire client for shard-mode integration
// tests: its own (threaded) embedded driver + Aeron client, raw Rune protocol
// frames over UDP.  Every wait is a bounded poll of a real condition (hard
// iteration cap), never a sleep-for-correctness.
#include <rune/node/EmbeddedDriver.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/ControlMessages.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/MessageHeader.h>
#include <rune/protocol/client/MetadataMessages.h>
#include <rune/protocol/client/Protocol.h>

#include <Aeron.h>
#include <FragmentAssembler.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace rune::node::test {

namespace pc = rune::protocol::client;

inline bool udpPortFree(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = htons(port);
    const bool ok = ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0;
    ::close(fd);
    return ok;
}

inline uint16_t freeUdpPort() {
    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); a.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
    socklen_t len = sizeof(a);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
    ::close(fd);
    return ntohs(a.sin_port);
}

/// n disjoint blocks of `width` consecutive free UDP ports (per-worker base ranges).
inline std::vector<uint16_t> freeUdpPortBlocks(int n, int width) {
    std::vector<uint16_t> out;
    for (int attempt = 0; attempt < 2000 && static_cast<int>(out.size()) < n; ++attempt) {
        const uint16_t p = freeUdpPort();
        if (p == 0 || static_cast<int>(p) + width > 65535) continue;
        bool ok = true;
        for (int k = 0; k < width && ok; ++k) ok = udpPortFree(static_cast<uint16_t>(p + k));
        for (uint16_t q : out) if (p < q + width && q < p + width) ok = false;
        if (ok) out.push_back(p);
    }
    return out;
}

struct PartitionInfo {
    int32_t topicId = 0, partition = 0, leaderId = -1, ingressStreamId = 0;
    std::string ingressEndpoint;
};

inline std::vector<uint8_t> makeBatch(int records) {
    rune::protocol::RecordBatchBuilder b(0, 1000);
    for (int i = 0; i < records; ++i) b.append("k", "v", 1000 + i);
    return b.build();
}

class ShardTestClient {
public:
    static constexpr int32_t kEgressStream = 4242;
    static constexpr int     kMaxIters     = 50'000;   // × 100 µs ⇒ 5 s hard cap per wait

    explicit ShardTestClient(const std::filesystem::path& aeronDir)
        : driver_(cfgFor(aeronDir), DriverMode::Threaded)
    {
        aeron::Context ctx;
        ctx.aeronDir(aeronDir.string());
        aeron_ = aeron::Aeron::connect(ctx);
        egressChannel_ = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(freeUdpPort());
        const int64_t reg = aeron_->addSubscription(egressChannel_, kEgressStream);
        for (int i = 0; i < kMaxIters && !egress_; ++i) { egress_ = aeron_->findSubscription(reg); pause_(); }
    }
    ~ShardTestClient() { pubs_.clear(); egress_.reset(); aeron_.reset(); }

    const std::string& egressChannel() const noexcept { return egressChannel_; }

    int64_t connect(const std::string& gatewayChannel, int32_t stream) {
        connect_.reset();                                  // each connect waits for ITS response
        pc::ConnectRequest req;
        req.correlationId  = ++corr_;
        req.egressStreamId = kEgressStream;
        req.clientVersion  = 1;
        req.egressChannel  = egressChannel_;
        std::vector<uint8_t> buf(req.encodedSize());
        req.encode(buf.data());
        if (!send(gatewayChannel, stream, buf.data(), buf.size())) return -1;
        for (int i = 0; i < kMaxIters && !connect_; ++i) { poll_(); pause_(); }
        return connect_ && connect_->code == 0 ? connect_->assignedSessionId : -1;
    }

    bool produce(const std::string& channel, int32_t stream, int64_t sessionId, int64_t corr,
                 int32_t topicId, int32_t partition, uint8_t acks, const std::vector<uint8_t>& batch) {
        pc::Produce msg;
        msg.sessionId = sessionId; msg.correlationId = corr;
        msg.topicId = topicId; msg.partition = partition; msg.partitionKnown = 1;
        msg.acks = acks; msg.producerId = -1; msg.producerEpoch = 0; msg.baseSequence = -1;
        msg.recordBatch = std::span<const uint8_t>(batch.data(), batch.size());
        std::vector<uint8_t> buf(msg.encodedSize());
        msg.encode(buf.data());
        return send(channel, stream, buf.data(), buf.size());
    }

    std::optional<pc::ProduceResponse> awaitProduceResponse(int64_t corr) {
        for (int i = 0; i < kMaxIters; ++i) {
            poll_();
            if (auto it = acks_.find(corr); it != acks_.end()) return it->second;
            pause_();
        }
        return std::nullopt;
    }

    /// Produce until an OK ack (retries NotLeader while leadership settles).  Returns the ack.
    std::optional<pc::ProduceResponse> produceUntilOk(const std::string& channel, int32_t stream,
                                                      int64_t sessionId, int32_t topicId,
                                                      int32_t partition, uint8_t acks,
                                                      const std::vector<uint8_t>& batch,
                                                      int maxAttempts = 100) {
        for (int a = 0; a < maxAttempts; ++a) {
            const int64_t c = ++corr_;
            if (!produce(channel, stream, sessionId, c, topicId, partition, acks, batch)) continue;
            auto ack = awaitProduceResponse(c);
            if (ack && ack->errorCode == 0) return ack;
            std::this_thread::sleep_for(std::chrono::milliseconds{20});   // leadership settling
        }
        return std::nullopt;
    }

    std::vector<PartitionInfo> metadata(const std::string& gatewayChannel, int32_t stream, int64_t sessionId) {
        pc::MetadataRequest req;
        req.sessionId = sessionId; req.correlationId = ++corr_;
        std::vector<uint8_t> buf(req.encodedSize());
        req.encode(buf.data());
        metadata_.reset();
        if (!send(gatewayChannel, stream, buf.data(), buf.size())) return {};
        for (int i = 0; i < kMaxIters && !metadata_; ++i) { poll_(); pause_(); }
        return metadata_.value_or(std::vector<PartitionInfo>{});
    }

    bool disconnect(const std::string& gatewayChannel, int32_t stream, int64_t sessionId) {
        pc::Disconnect d; d.sessionId = sessionId; d.correlationId = ++corr_;
        std::vector<uint8_t> buf(pc::MessageHeader::ENCODED_SIZE + pc::Disconnect::BODY_FIXED);
        d.encode(buf.data());
        return send(gatewayChannel, stream, buf.data(), buf.size());
    }

    bool send(const std::string& channel, int32_t stream, const uint8_t* buf, std::size_t len) {
        auto& pub = pubs_[{channel, stream}];
        if (!pub) {
            const int64_t reg = aeron_->addPublication(channel, stream);
            for (int i = 0; i < kMaxIters && !pub; ++i) { pub = aeron_->findPublication(reg); pause_(); }
            if (!pub) return false;
        }
        aeron::concurrent::AtomicBuffer ab(const_cast<uint8_t*>(buf), static_cast<aeron::util::index_t>(len));
        for (int i = 0; i < kMaxIters; ++i) {
            if (pub->offer(ab, 0, static_cast<aeron::util::index_t>(len)) > 0) return true;
            pause_();
        }
        return false;
    }

private:
    static EmbeddedDriverConfig cfgFor(const std::filesystem::path& d) {
        EmbeddedDriverConfig c; c.dir = d.string(); c.termBufferBytes = 1 << 20; return c;
    }
    static void pause_() { std::this_thread::sleep_for(std::chrono::microseconds{100}); }

    void poll_() {
        if (!egress_) return;
        egress_->poll(assembler_.handler(), 64);
    }
    void onFrame_(const uint8_t* p, std::size_t n) {
        auto hdr = pc::MessageHeader::decode(p, n);
        if (!hdr) return;
        switch (static_cast<pc::TemplateId>(hdr->templateId)) {
            case pc::TemplateId::ConnectResponse:
                if (auto r = pc::ConnectResponse::decode(p, n)) connect_ = *r;
                break;
            case pc::TemplateId::ProduceResponse:
                if (auto r = pc::ProduceResponse::decode(p, n)) acks_[r->correlationId] = *r;
                break;
            case pc::TemplateId::MetadataResponse:
                if (auto r = pc::MetadataResponse::decode(p, n)) {
                    std::vector<PartitionInfo> v;
                    for (const auto& e : r->partitions)
                        v.push_back({e.topicId, e.partition, e.leaderId, e.ingressStreamId,
                                     std::string(e.ingressEndpoint)});
                    metadata_ = std::move(v);
                }
                break;
            default: break;
        }
    }

    EmbeddedDriver                                   driver_;
    std::shared_ptr<aeron::Aeron>                    aeron_;
    std::string                                      egressChannel_;
    std::shared_ptr<aeron::Subscription>             egress_;
    std::map<std::pair<std::string, int32_t>, std::shared_ptr<aeron::Publication>> pubs_;
    aeron::FragmentAssembler assembler_{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t off,
                                               aeron::util::index_t len, aeron::Header&) {
        onFrame_(reinterpret_cast<const uint8_t*>(b.buffer() + off), static_cast<std::size_t>(len));
    }};
    std::optional<pc::ConnectResponse>              connect_;
    std::map<int64_t, pc::ProduceResponse>          acks_;
    std::optional<std::vector<PartitionInfo>>       metadata_;
    int64_t                                         corr_ = 0;
};

} // namespace rune::node::test
```

`libs/node/tests/node/ShardAgentTest.cpp`:

```cpp
// ShardAgentTest.cpp — the shard pass drives its OWN driver + conductor; the
// loop-duration alarm uses only the injected clock (no wall time).
#include <rune/node/ShardAgent.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <unistd.h>

using namespace rune::node;
namespace fs = std::filesystem;

namespace {
struct Shard {
    fs::path dir;
    std::unique_ptr<EmbeddedDriver> driver;
    std::shared_ptr<aeron::Aeron> client;
    Worker worker{0, 1 << 16, 1 << 16, 16, 16};
    Shard() {
        static std::atomic<int> seq{0};
        dir = fs::temp_directory_path() / ("rune_shagent_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        EmbeddedDriverConfig c; c.dir = dir.string(); c.termBufferBytes = 64 * 1024;
        driver = std::make_unique<EmbeddedDriver>(c, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(dir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        worker.setShardMode(true);
    }
    ~Shard() { client.reset(); driver.reset(); std::error_code ec; fs::remove_all(dir, ec); }
};
}

TEST(ShardAgent, SlowPassAlarmCountsOnlyFromInjectedClock) {
    Shard s;
    ShardAgent a(s.worker, *s.driver, *s.client);
    a.doWork(1'000'000);
    a.doWork(1'050'000);                        // 50 µs gap
    EXPECT_EQ(a.slowPassesForTest(), 0u);
    a.doWork(1'050'000 + ShardAgent::kSlowPassNs + 1);
    EXPECT_EQ(a.slowPassesForTest(), 1u);
}

TEST(ShardAgent, PassDrivesOwnDriverAndConductor) {
    Shard s;
    ShardAgent a(s.worker, *s.driver, *s.client);
    const int64_t reg = s.client->addSubscription("aeron:ipc", 9100);
    std::shared_ptr<aeron::Subscription> sub;
    int64_t now = 1'000;
    for (int i = 0; i < 100'000 && !sub; ++i) {
        a.doWork(now += 1'000);                  // the pass is the ONLY thing driving anything
        sub = s.client->findSubscription(reg);
    }
    EXPECT_TRUE(sub) << "subscription resolves only if the shard pass pumps driver + conductor";
    EXPECT_EQ(s.driver->foreignPumpAttempts(), 0u);
}
```

`libs/node/tests/integration/ShardModeTest.cpp`:

```cpp
// ShardModeTest.cpp — in-process shard mode over REAL Aeron UDP: raw wire client
// → gateway (Connect) → worker shard ingress (Produce) → worker replication plane
// → ProduceResponse.  Legacy default must build no shard runtime.
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {

struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_shard_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};

constexpr int32_t kClientStream  = 110;
constexpr int32_t kIngressStream = 111;

struct ShardPorts {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static ShardPorts discover(int nodes, int workers) {
        ShardPorts p;
        auto blocks = freeUdpPortBlocks(3 * nodes, workers);
        for (int i = 0; i < nodes; ++i) {
            p.cons.push_back(freeUdpPort()); p.client.push_back(freeUdpPort()); p.repl.push_back(freeUdpPort());
            p.ib.push_back(blocks[3 * i]); p.rb.push_back(blocks[3 * i + 1]); p.eg.push_back(blocks[3 * i + 2]);
        }
        return p;
    }
};

NodeConfig makeShardConfig(int nodeId, const fs::path& dataDir, const ShardPorts& p, int nodes,
                           int32_t workers, bool fused) {
    NodeConfig cfg;
    cfg.nodeId = nodeId;
    cfg.dataDir = dataDir.string();
    cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90;
    cfg.replicationStreamId = 100;
    cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.workerCount = workers;
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = fused ? NodeConfig::RuntimePlacement::Fused : NodeConfig::RuntimePlacement::Thread;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.workerIngressBasePort = p.ib[nodeId];
    cfg.workerReplBasePort    = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1";
    cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = nodes == 1 ? 1 : 2;
    cfg.aeronTermBufferBytes = 1 << 20;
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc;
        pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = workers;
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel  = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t; t.name = "st"; t.partitionCount = 1; t.topicId = 0;
    cfg.topics.push_back(t);
    return cfg;
}

std::string gatewayChannel(const ShardPorts& p, int node) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[node]);
}
std::string workerIngressChannel(const ShardPorts& p, int node, int w) {
    return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.ib[node] + w);
}
int64_t logEndOffsetAfterStop(const fs::path& dataDir) {
    rune::storage::Log log((dataDir / "topics" / "st" / "0").string(), 64LL * 1024 * 1024);
    log.recover();
    return log.logEndOffset();
}

} // namespace

TEST(ShardMode, SingleNodeProduceRoundTripOnWorkerShard) {
    TmpDir dir{"n0"}, cdir{"client"};
    const auto ports = ShardPorts::discover(1, 1);
    const auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/false);
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_EQ(node.shardDriverCountForTest(), 1u);
    EXPECT_TRUE(fs::exists(fs::path(cfg.effectiveAeronDir()) / "w0" / "cnc.dat"));
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0);
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kIngressStream, sid, 0, 0,
                                    /*acks=*/1, makeBatch(3));
        ASSERT_TRUE(ack.has_value()) << "no OK ack from the worker shard";
        EXPECT_EQ(ack->partition, 0);
    }
    node.stop();
    EXPECT_GE(logEndOffsetAfterStop(dir.path), 3);
}

TEST(ShardMode, ThreeNodeAcksAllCommitsOverShardReplicationPlanes) {
    TmpDir d0{"t0"}, d1{"t1"}, d2{"t2"}, cdir{"tc"};
    const auto ports = ShardPorts::discover(3, 1);
    BrokerNode n0{makeShardConfig(0, d0.path, ports, 3, 1, false)};
    BrokerNode n1{makeShardConfig(1, d1.path, ports, 3, 1, false)};
    BrokerNode n2{makeShardConfig(2, d2.path, ports, 3, 1, false)};
    n0.start(); n1.start(); n2.start();
    ASSERT_TRUE(n0.awaitLeader(15'000ms));
    int32_t leader = -1;
    {
        ShardTestClient c(cdir.path);
        const int64_t sid0 = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid0, 0);
        for (int i = 0; i < 200 && leader < 0; ++i) {
            for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid0))
                if (pi.topicId == 0 && pi.partition == 0) leader = pi.leaderId;
            if (leader < 0) std::this_thread::sleep_for(50ms);
        }
        ASSERT_GE(leader, 0) << "no partition leader advertised";
        const int64_t sid = c.connect(gatewayChannel(ports, leader), kClientStream);
        ASSERT_GT(sid, 0);
        for (int b = 0; b < 5; ++b) {
            auto ack = c.produceUntilOk(workerIngressChannel(ports, leader, 0), kIngressStream, sid, 0, 0,
                                        /*acks=all*/1, makeBatch(4));
            ASSERT_TRUE(ack.has_value()) << "acks=all batch " << b << " not committed";
        }
    }
    n0.stop(); n1.stop(); n2.stop();
    int atLeast20 = 0;
    for (const auto* d : {&d0, &d1, &d2}) atLeast20 += logEndOffsetAfterStop(d->path) >= 20 ? 1 : 0;
    EXPECT_GE(atLeast20, 2) << "every acked record must be on a majority";
}

TEST(ShardModeRegression, LegacyDefaultBuildsNoShardRuntime) {
    TmpDir dir{"legacy"};
    const auto ports = ShardPorts::discover(1, 1);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, false);
    cfg.workerMode = NodeConfig::WorkerMode::Legacy;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = false;
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    EXPECT_EQ(node.shardDriverCountForTest(), 0u);
    EXPECT_FALSE(fs::exists(fs::path(cfg.effectiveAeronDir()) / "w0"));
    node.stop();
}
```

CMake (`libs/node/CMakeLists.txt`): add `src/ShardAgent.cpp` to `rune_node`; next to `rune_aeron_pump_tests`:

```cmake
            add_executable(rune_shard_agent_tests tests/node/ShardAgentTest.cpp)
            target_link_libraries(rune_shard_agent_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_shard_agent_tests PROPERTIES TIMEOUT 60 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
```

and inside the `if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)` block that defines `rune_per_worker_replication_integration_tests`:

```cmake
        add_executable(rune_shard_mode_tests tests/integration/ShardModeTest.cpp)
        target_include_directories(rune_shard_mode_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_shard_mode_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_shard_mode_tests PROPERTIES TIMEOUT 360 RESOURCE_LOCK rune_udp_ports)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_shard_agent_tests rune_shard_mode_tests`
Expected: FAIL to compile — `rune/node/ShardAgent.h: No such file or directory`, `no member named 'setShardMode' in 'rune::node::Worker'`, `no member named 'shardDriverCountForTest'`.

- [ ] **Step 3: Implement**

rune-fr `Sites.h`: append to the enum, before `// ... grows as more paths are instrumented ...`:

```cpp
    // --- self-contained worker shards (Category::Runtime) ---
    ShardPass,                // ShardAgent::doWork — one full shard pass (driver+conductor+worker[+fused runtime])
```

and to `kSiteMeta` (same position): `{"ShardPass", Category::Runtime},`.

`libs/node/include/rune/node/ShardAgent.h`:

```cpp
#pragma once
// ShardAgent.h — the self-contained worker shard duty cycle (spec §3.1).  One
// thread (pinned, busy-spin when pinned) per worker; each pass, in order:
//   1. own embedded driver (INVOKER: conductor + sender + receiver)
//   2. own Aeron client conductor (useConductorAgentInvoker)
//   3-6. Worker::doWork in shard order (intake, replication, acks, io reap)
// Task 11 appends the fused runtime (its own driver + conductor + NodeRuntimeAgent).
#include <rune/consensus/ConsensusAgent.h>
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>

#include <Aeron.h>

#include <cstdint>

namespace rune::node {

class ShardAgent final : public rune::consensus::Agent {
public:
    /// A pass longer than this (the whole p99 budget) is an alarm (spec §8).
    static constexpr int64_t kSlowPassNs = 100'000;

    ShardAgent(Worker& worker, EmbeddedDriver& driver, aeron::Aeron& client) noexcept
        : worker_(worker), driver_(driver), client_(client) {}

    const char* name() const noexcept override { return worker_.name(); }
    int doWork(int64_t nowNs) override;

    [[nodiscard]] uint64_t slowPassesForTest() const noexcept { return slowPasses_; }

private:
    Worker&         worker_;
    EmbeddedDriver& driver_;
    aeron::Aeron&   client_;
    bool            bound_      = false;
    int64_t         lastNowNs_  = 0;
    uint64_t        slowPasses_ = 0;
};

} // namespace rune::node
```

`libs/node/src/ShardAgent.cpp`:

```cpp
#include <rune/node/ShardAgent.h>

#include <rune_fr/FlightRecorder.h>

namespace rune::node {

int ShardAgent::doWork(int64_t nowNs) {
    RUNE_FR_SPAN_RUNTIME(rune_fr::Site::ShardPass, 0);
    if (!bound_) [[unlikely]] {
        driver_.bindPumpThread();          // ownership hand-off: startup thread → this thread
        bound_ = true;
    }
    // Loop-duration alarm from the injected clock only (sampled once per pass by the
    // runner): under busy-spin the gap between passes IS the previous pass's cost.
    if (lastNowNs_ != 0 && nowNs - lastNowNs_ > kSlowPassNs) ++slowPasses_;
    lastNowNs_ = nowNs;

    int work = driver_.doWork();                        // 1. own driver duty cycle
    work += client_.conductorAgentInvoker().invoke();   // 2. own client conductor
    work += worker_.doWork(nowNs);                      // 3-6. Worker (final → direct call)
    return work;
}

} // namespace rune::node
```

`Worker.h`: change `class Worker : public rune::consensus::Agent {` to `class Worker final : public rune::consensus::Agent {`; add public

```cpp
    // Self-contained worker shard (spec §3.1).  Set before the runner starts.
    void setShardMode(bool on) noexcept { shardMode_ = on; }
    [[nodiscard]] bool shardMode() const noexcept { return shardMode_; }
```

private `bool shardMode_ = false;`; in `doWork`, right after the one-shot pin block:

```cpp
        if (shardMode_) return doWorkShard_(nowNs);
```

and the shard pass (private, next to `doWork`):

```cpp
    // Shard pass order (spec §3.1 steps 3-6).  The ShardAgent already ran the
    // driver (1) and client conductor (2).  Never blocks; never waits on another
    // component (a full return ring is a non-blocking intake skip, spec §3.6).
    int doWorkShard_(int64_t nowNs) {
        int work = 0;
        // 2b. runtime→worker ring first: ownership / role changes (and, Task 13,
        //     session announcements) must be applied before the intake they govern.
        work += static_cast<int>(drainIngress_());
        // 3. client ingress → append (G-A6 gate unchanged).
        work += flushRetOverflow_();
        if (retOverflow_.empty() &&
            ret_.freeSlotsForMsgSize(ackSlotBytes_) > static_cast<int64_t>(pendingAcksAll_.size())) {
            work += pollWorkerIngress_(nowNs);
        }
        work += submitCoalescedFlushes_();
        // 4. replication on this worker's plane: apply inbound (inline, fast-acks),
        //    then push / ISR / HW per owned replica.
        if (replTransportCtx_) work += replTransportPollFn_(replTransportCtx_, kReplFetchLimit, this, nowNs);
        for (auto* replica : owned_) {
            if (replica) work += replica->doWork(nowNs);
        }
        // 5. fire due produce acks.
        work += flushOwedAcksAll_(nowNs);
        // 6. reap durability completions.
        work += reapDurableCompletions_();
        // Cold tail: consumer egress push, cleaner drains, time-gated log sync.
        work += pushOwnedSubscriptions_(nowNs);
        for (auto* replica : owned_) {
            if (replica && replica->isDiskBacked()) {
                work += replica->log()->drainCleanerSwaps();
                work += replica->log()->drainRetentionDeletes();
                work += replica->log()->drainStagingRenamesDone();
            }
        }
        work += syncOwnedLogs_(nowNs);   // made asynchronous in Task 10
        return work;
    }
```

`WorkerPool.h`: `#include <rune/node/ShardAgent.h>` and

```cpp
    // Shard mode (spec §3.1): one runner per ShardAgent.  Busy-spin ONLY when the
    // worker is pinned to its own core; otherwise the backoff idle (§1a.3).
    void startShardRunners(std::vector<std::unique_ptr<ShardAgent>>& agents,
                           rune::consensus::NanoClock& clock,
                           bool busySpin, int64_t maxParkUs) {
        if (!runners_.empty()) return;
        idles_.reserve(agents.size());
        runners_.reserve(agents.size());
        for (auto& a : agents) {
            if (busySpin) idles_.emplace_back(std::make_unique<rune::consensus::BusySpinIdleStrategy>());
            else          idles_.emplace_back(makeBackoffIdle(maxParkUs));
            runners_.emplace_back(std::make_unique<rune::consensus::AgentRunner>(*a, *idles_.back(), clock));
            runners_.back()->start();
        }
        runnersStarted_ = true;
    }
```

`BrokerNode.h` members (after `perWorkerReplTransports_`), plus includes `<rune/node/ShardAgent.h>`:

```cpp
    // Self-contained worker shards (spec §3.1).  Index = worker id.  Destroyed in
    // stop() after the worker runners are joined, before the node driver.
    std::vector<std::unique_ptr<EmbeddedDriver>> shardDrivers_;
    std::vector<std::shared_ptr<aeron::Aeron>>   shardClients_;
    std::vector<std::unique_ptr<ShardAgent>>     shardAgents_;
    void buildShardRuntimes_();
public:
    [[nodiscard]] std::size_t shardDriverCountForTest() const noexcept { return shardDrivers_.size(); }
```

`BrokerNode.cpp`:

```cpp
void BrokerNode::buildShardRuntimes_() {
    const int32_t wc = config_.workerCount >= 1 ? config_.workerCount : 1;
    std::error_code ec;
    const auto space = fs::space(config_.effectiveAeronDir(), ec);
    if (!ec) {
        if (auto err = checkAeronSpace(space.available, wc, config_.aeronTermBufferBytes)) {
            throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) +
                                     "]: refusing shard mode — " + *err);
        }
    }
    shardDrivers_.reserve(static_cast<std::size_t>(wc));
    shardClients_.reserve(static_cast<std::size_t>(wc));
    for (int32_t w = 0; w < wc; ++w) {
        const std::string dir = (fs::path{config_.effectiveAeronDir()} / ("w" + std::to_string(w))).string();
        shardDrivers_.push_back(std::make_unique<EmbeddedDriver>(
            EmbeddedDriverConfig::fromNode(config_, dir), DriverMode::Invoker));
        aeron::Context ctx;
        ctx.aeronDir(dir);
        ctx.useConductorAgentInvoker(true);
        auto client = aeron::Aeron::connect(ctx);
        if (!client)
            throw std::runtime_error("BrokerNode: shard " + std::to_string(w) + " Aeron client connect failed");
        shardClients_.push_back(std::move(client));
    }
    spdlog::info("BrokerNode[{}]: {} shard runtime(s) (invoker driver + invoker client) under {}/w*",
                 config_.nodeId, wc, config_.effectiveAeronDir());
}
```

In `start()`, right after `startEmbeddedDriver();`: `if (config_.shardMode()) buildShardRuntimes_();`. Replace the worker runner start:

```cpp
    if (workerPool_) {
        if (config_.shardMode()) {
            workerPool_->startShardRunners(shardAgents_, clock_,
                                           /*busySpin=*/config_.workerCoreBase >= 0,
                                           config_.idleMaxParkUs);
        } else {
            workerPool_->startRunners(config_.workerIdleStrategy, clock_, config_.idleMaxParkUs);
        }
        spdlog::info("BrokerNode[{}]: {} worker thread(s) started (shard={})",
                     config_.nodeId, workerPool_->workerCount(), config_.shardMode());
    }
```

`buildReplication()`: change `if (config_.perWorkerReplication && aeron_) {` (the per-worker transport block) to be preceded by a shard branch:

```cpp
    if (config_.shardMode()) {
        const int32_t wc = static_cast<int32_t>(shardClients_.size());
        for (int32_t w = 0; w < wc; ++w) {
            std::vector<std::pair<rune::replication::NodeId, std::string>> peersW;
            for (const auto& peer : config_.peers) {
                if (peer.id == config_.nodeId) continue;
                peersW.push_back({static_cast<rune::replication::NodeId>(peer.id),
                                  NodeConfig::shardPeerReplChannel(peer, w)});
            }
            if (peersW.empty()) break;                           // single node: no planes
            const int32_t streamId = config_.replBaseStreamId + w;
            auto t = std::make_unique<WorkerReplicationTransport>(
                shardClients_[static_cast<std::size_t>(w)], w, config_.shardReplBindChannel(w),
                streamId, peersW, streamId,
                shardDrivers_[static_cast<std::size_t>(w)]->pumpWith(shardClients_[static_cast<std::size_t>(w)].get()));
            if (!t->resolved())
                throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) +
                                         "]: shard " + std::to_string(w) +
                                         " replication plane did not resolve — refusing to start");
            perWorkerReplTransports_.push_back(std::move(t));
        }
    } else if (config_.perWorkerReplication && aeron_) {
```

(the existing block body is unchanged; the rest of `buildReplication` — router, per-partition transport selection — already routes `topicId >= 0` replicas to `perWorkerReplTransports_[workerForPartition]`).

`buildWorkerPool()`: wrap the existing per-worker ingress and egress blocks as the `else` of a shard branch placed right after the `setDesiredFragmentLimit` loop:

```cpp
    if (config_.shardMode()) {
        int32_t egressBase = config_.workerEgressBaseStreamId;
        if (egressBase <= 0) egressBase = config_.workerIngressBaseStreamId + wc;
        for (int32_t i = 0; i < wc; ++i) {
            const auto idx = static_cast<std::size_t>(i);
            auto pump = shardDrivers_[idx]->pumpWith(shardClients_[idx].get());
            auto ing = std::make_unique<WorkerIngress>(shardClients_[idx], config_.shardIngressBindChannel(i),
                                                       config_.workerIngressBaseStreamId + i, pump);
            const std::string control = config_.workerEgressControlHost + ":" +
                                        std::to_string(config_.workerEgressControlBasePort + i);
            auto eg = std::make_unique<WorkerEgress>(shardClients_[idx], control, egressBase + i, pump);
            if (!ing->resolved() || !eg->resolved())
                throw std::runtime_error("BrokerNode[" + std::to_string(config_.nodeId) + "]: shard " +
                                         std::to_string(i) + " ingress/egress did not resolve — refusing to start");
            Worker& wk = workerPool_->worker(i);
            wk.setWorkerIngress(ing.get(), /*active=*/true);
            wk.setWorkerEgress(eg.get());
            wk.setShardMode(true);
            workerIngresses_.push_back(std::move(ing));
            workerEgresses_.push_back(std::move(eg));
            shardAgents_.push_back(std::make_unique<ShardAgent>(wk, *shardDrivers_[idx], *shardClients_[idx]));
        }
        if (gateway_) {
            gateway_->setPerWorkerIngressActive(true);
            gateway_->setPerWorkerEgressActive(true);
            gateway_->setWorkerEgressParams(config_.workerEgressControlBasePort, egressBase);
            for (const auto& ts : config_.topics)
                for (int32_t p = 0; p < ts.partitionCount; ++p)
                    if (auto* log = topicRegistry_->log(ts.topicId, p)) gateway_->addWorkerOwnedLog(log);
        }
    } else {
        // ... existing `if (config_.perWorkerIngressEnabled && aeron_) {...} else if ...` and
        //     `if (config_.perWorkerEgressEnabled && aeron_) {...} else if ...` blocks, unchanged ...
    }
```

`stop()`: after `perWorkerReplTransports_.clear();` add

```cpp
    // Shard teardown: per-worker Aeron resources before their clients, clients before
    // their drivers (each EmbeddedDriver drains the client-close commands on close).
    workerIngresses_.clear();
    workerEgresses_.clear();
    shardAgents_.clear();
    shardClients_.clear();
    shardDrivers_.clear();
```

- [ ] **Step 4: Run to verify they pass (flag-on and default)**

Run:
```bash
cd ~/Projects/rune-fr && git diff --stat
cd ~/Projects/rune-broker
cmake --build build-fast
./build-fast/libs/node/rune_shard_agent_tests
./build-fast/libs/node/rune_shard_mode_tests
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast --output-on-failure -j2
```
Expected: PASS — shard tests green; the full suite (default config) unchanged.

- [ ] **Step 5: Commit**

```bash
cd ~/Projects/rune-fr
git add include/rune_fr/Sites.h
git commit -m "feat(sites): ShardPass (self-contained worker shard pass)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
cd ~/Projects/rune-broker
git add libs/node/include/rune/node/ShardAgent.h libs/node/src/ShardAgent.cpp \
        libs/node/include/rune/node/Worker.h libs/node/include/rune/node/WorkerPool.h \
        libs/node/include/rune/node/BrokerNode.h libs/node/src/BrokerNode.cpp libs/node/CMakeLists.txt \
        libs/node/tests/support/ShardTestClient.h libs/node/tests/node/ShardAgentTest.cpp \
        libs/node/tests/integration/ShardModeTest.cpp
git commit -m "feat(node): self-contained worker shards — per-worker invoker driver + client, ShardAgent

RUNE_WORKER_MODE=shard (default legacy): each worker owns its driver, client, ports
and pinned busy-spin loop; unresolved per-worker resources refuse startup.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 10: Non-blocking shard loop — follower flush and owned-log sync through the io backend

Spec §3.1 "Never blocks". Three synchronous disk calls remain on a shard loop: the follower flush (`PartitionReplica::doWork` → `L.sync()`), the worker's time-based `syncOwnedLogs_` (`log->sync()`), and the Strict-tier inline `log->sync()` inside `handleProduceBatch` (`durabilityTier == 2`). In shard mode all three become submissions to the worker's existing `IoBackend` (`UringFlushBackend` on Linux, `PortableFlushBackend` elsewhere). Background flushes live in their own fixed table; `reapDurableCompletions_` retires a background token **first**, so such a completion can never reach the produce-ack matching (token routing). The `.index` msync that `Log::sync` also performs is not on the async path: `Log::recover` rebuilds the index from the `.log` (`Log.cpp` ~374, ~642-651), so fdatasync of the active segment fd is the durability point. Legacy mode is unchanged.

**Files:**
- Modify: `libs/storage/include/rune/storage/Log.h:105` + `libs/storage/src/Log.cpp:552` (`syncCallsForTest`)
- Modify: `libs/replication/include/rune/replication/PartitionReplica.h` (public `setExternalFollowerFlush`, `takeFollowerFlushRequest`; fields next to `followerApplyDirty_` ~line 756)
- Modify: `libs/replication/src/PartitionReplica.cpp` (`doWork` follower flush block ~1511)
- Modify: `libs/node/include/rune/node/Worker.h` (`DurablePending` ~line 2126; `reapDurableCompletions_` ~877; `doWorkShard_`; `handleWorkerIngressFrame_` tier handling ~2594; new bg-flush members/functions; test hooks)
- Modify: `libs/node/src/BrokerNode.cpp` (`buildReplication` replica loop + `createRuntimeUserReplicas`: `setExternalFollowerFlush(true)` in shard mode)
- Create: `libs/node/tests/integration/ShardNonBlockingTest.cpp`; Modify: `libs/node/CMakeLists.txt` (`rune_shard_nonblocking_tests`)

**Interfaces:**
- Consumes: Task 7 `EmbeddedDriver`, Task 8 `AeronPump`, Task 9 `setShardMode`/`doWorkShard_`.
- Produces: `uint64_t rune::storage::Log::syncCallsForTest() const noexcept`; `void PartitionReplica::setExternalFollowerFlush(bool) noexcept; bool PartitionReplica::takeFollowerFlushRequest() noexcept`; Worker test hooks `std::size_t bgFlushInFlightForTest() const noexcept; uint64_t bgFlushSubmitsForTest() const noexcept; uint64_t bgFlushErrorsForTest() const noexcept; CompletionToken submitBgFlushForTest(rune::storage::Log*); CompletionToken durablePendingTokenForTest(std::size_t) const; void deliverCompletionForTest(const FlushCompletion&);`

- [ ] **Step 1: Write the failing tests** — `libs/node/tests/integration/ShardNonBlockingTest.cpp`

```cpp
// ShardNonBlockingTest.cpp — spec §3.1: no synchronous disk call on a shard loop;
// background flush tokens can never fire a produce ack (token routing).
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace rune::node;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
using rune::storage::Log;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int64_t kMs = 1'000'000;
constexpr int kCap = 2'000'000;   // hard cap on any drive loop

struct TmpDir {
    fs::path path;
    TmpDir() {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() / ("rune_shnb_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
class NullTransport final : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {}
};
std::vector<uint8_t> batchAt(int64_t base, int n = 1) {
    rune::protocol::RecordBatchBuilder b(base, 1000);
    for (int i = 0; i < n; ++i) b.append("k", "v", 1000 + i);
    return b.build();
}
PartitionConfig cfg(rune::replication::NodeId self, uint8_t tier = 1) {
    PartitionConfig c; c.topicId = 1; c.selfId = self; c.minIsr = 1; c.durabilityTier = tier;
    c.fetchIntervalNs = 1'000 * kMs; c.isrCheckIntervalNs = 1'000 * kMs; c.pushOnAppend = false;
    return c;
}
} // namespace

TEST(ShardNonBlockingRegression, FollowerFlushNeverSyncsOnTheWorkerThread) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica follower(cfg(2), Role::Follower, 1, {1, 2}, {1, 2}, &log, &t);
    follower.becomeFollower(1, 1);
    follower.setExternalFollowerFlush(true);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&follower});
    w.setShardMode(true);
    const auto b = batchAt(0);
    rune::replication::FetchResponseMsg resp{1, 0, 0, 0, rune::replication::kErrNone, 1,
                                             std::span<const uint8_t>(b)};
    follower.applyReplicated(resp);                        // follower now dirty
    w.doWork(25 * kMs);                                    // past the 20 ms flush interval
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "the follower flush must not block the shard loop";
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u) << "it is submitted to the io backend instead";
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) {
        w.doWork(26 * kMs);
        std::this_thread::yield();
    }
    EXPECT_EQ(w.bgFlushInFlightForTest(), 0u);
    EXPECT_EQ(w.bgFlushErrorsForTest(), 0u);
}

TEST(ShardNonBlockingRegression, OwnedLogSyncIsAsyncAndIdleLogIsNotResubmitted) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setShardMode(true);
    const std::vector<uint8_t> payload(16, 1);
    leader.append(std::span<const uint8_t>(payload));
    w.doWork(1 * kMs);                                     // arms the sync interval
    w.doWork(12 * kMs);                                    // > 10 ms: sync due
    EXPECT_EQ(log.syncCallsForTest(), 0u);
    ASSERT_EQ(w.bgFlushSubmitsForTest(), 1u);
    for (int i = 0; i < kCap && w.bgFlushInFlightForTest() > 0; ++i) { w.doWork(13 * kMs); std::this_thread::yield(); }
    ASSERT_EQ(w.bgFlushInFlightForTest(), 0u);
    w.doWork(30 * kMs);                                    // next interval, no new appends
    EXPECT_EQ(w.bgFlushSubmitsForTest(), 1u) << "an unchanged log is not resubmitted (no phantom work)";
}

TEST(ShardNonBlockingRegression, BackgroundFlushCompletionNeverFiresProduceAck) {
    TmpDir d;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    WorkerOp op;
    op.type = WorkerOp::Type::Produce; op.topicId = 1; op.partition = 0;
    op.replica = &leader; op.logPtr = &log; op.sessionId = 1; op.correlationId = 1;
    op.lastOffsetDelta = 0; op.recordCount = 1; op.durabilityTier = 2;
    op.recordBatchBytes = batchAt(0);
    ASSERT_TRUE(w.tryEnqueueOp(std::move(op)));
    w.doWork(1);                                           // append + one coalesced submit (token T1)
    const auto t1 = w.durablePendingTokenForTest(0);
    ASSERT_NE(t1, kNoToken);
    const auto t2 = w.submitBgFlushForTest(&log);          // a background flush on the SAME log
    ASSERT_NE(t2, kNoToken);
    int acks = 0;
    auto drain = [&] { return static_cast<int>(w.drainReturn([&](WorkerReturn) { ++acks; })); };
    w.deliverCompletionForTest(FlushCompletion{t2, 0, log.flushBoundary()});
    drain();
    EXPECT_EQ(acks, 0) << "a background flush token must never fire a produce ack";
    w.deliverCompletionForTest(FlushCompletion{t1, 0, log.flushBoundary()});
    drain();
    EXPECT_EQ(acks, 1);
    for (int i = 0; i < 10'000; ++i) { w.doWork(2 + i); drain(); std::this_thread::yield(); }  // real completions arrive later
    EXPECT_EQ(acks, 1) << "late real completions for T1/T2 must be ignored";
}

TEST(ShardNonBlocking, StrictTierAcksFromCompletionWithoutInlineSync) {
    TmpDir d, ad;
    Log log(d.path.string(), 64LL * 1024 * 1024); log.recover();
    NullTransport t;
    PartitionReplica leader(cfg(1, /*tier=*/2), Role::Leader, 1, {1}, {1}, &log, &t);
    EmbeddedDriverConfig dc; dc.dir = ad.path.string(); dc.termBufferBytes = 64 * 1024;
    EmbeddedDriver drv(dc, DriverMode::Invoker);
    aeron::Context ctx; ctx.aeronDir(ad.path.string()); ctx.useConductorAgentInvoker(true);
    auto client = aeron::Aeron::connect(ctx);
    WorkerIngress ing(client, "aeron:ipc", 7101, drv.pumpWith(client.get()));
    ASSERT_TRUE(ing.resolved());
    Worker w(0, 1 << 16, 1 << 16, 64, 64);
    w.setOwnedDirect(Worker::OwnedSet{&leader});
    w.setWorkerIngress(&ing, true);
    w.setShardMode(true);
    const int64_t preg = client->addExclusivePublication("aeron:ipc", 7101);
    std::shared_ptr<aeron::ExclusivePublication> pub;
    for (int i = 0; i < kCap && !(pub && pub->isConnected()); ++i) {
        drv.doWork(); client->conductorAgentInvoker().invoke();
        if (!pub) pub = client->findExclusivePublication(preg);
    }
    ASSERT_TRUE(pub && pub->isConnected());
    pc::Produce msg; const auto b = batchAt(0);
    msg.sessionId = 1; msg.correlationId = 9; msg.topicId = 1; msg.partition = 0; msg.partitionKnown = 1;
    msg.acks = 1; msg.recordBatch = std::span<const uint8_t>(b);
    std::vector<uint8_t> frame(msg.encodedSize()); msg.encode(frame.data());
    aeron::concurrent::AtomicBuffer ab(frame.data(), static_cast<aeron::util::index_t>(frame.size()));
    ASSERT_GT(pub->offer(ab, 0, static_cast<aeron::util::index_t>(frame.size())), 0);
    int acks = 0; int16_t err = -1;
    for (int i = 0; i < kCap && acks == 0; ++i) {
        drv.doWork(); client->conductorAgentInvoker().invoke();
        w.doWork(1 + i);
        w.drainReturn([&](WorkerReturn r) { ++acks; err = r.errorCode; });
    }
    ASSERT_EQ(acks, 1);
    EXPECT_EQ(err, 0);
    EXPECT_EQ(log.syncCallsForTest(), 0u) << "Strict tier must not fsync inline on the shard loop";
    pub.reset();
}
```

CMake (in the `aeron_FOUND` test area, next to `rune_async_durability_tests`):

```cmake
    if(aeron_FOUND)
        add_executable(rune_shard_nonblocking_tests tests/integration/ShardNonBlockingTest.cpp)
        target_link_libraries(rune_shard_nonblocking_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_shard_nonblocking_tests PROPERTIES TIMEOUT 120)
    endif()
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_shard_nonblocking_tests`
Expected: FAIL to compile — `no member named 'setExternalFollowerFlush'`, `'syncCallsForTest'`, `'bgFlushSubmitsForTest'`.

- [ ] **Step 3: Implement**

`Log.h` (public, next to `sync()`): `[[nodiscard]] uint64_t syncCallsForTest() const noexcept { return syncCalls_; }`; private: `uint64_t syncCalls_ = 0;`. `Log.cpp`: `void Log::sync() { ++syncCalls_; activeSegment().sync(); }`.

`PartitionReplica.h` public:

```cpp
    /// Shard mode (spec §3.1): the owning worker performs the time-gated follower
    /// flush asynchronously through its io backend; doWork only raises a request.
    void setExternalFollowerFlush(bool on) noexcept { externalFollowerFlush_ = on; }
    /// Worker side: consume a pending follower-flush request (true once per request).
    [[nodiscard]] bool takeFollowerFlushRequest() noexcept {
        const bool r = followerFlushRequested_;
        followerFlushRequested_ = false;
        return r;
    }
```

private, next to `followerApplyDirty_`: `bool externalFollowerFlush_ = false;` and `bool followerFlushRequested_ = false;`.

`PartitionReplica.cpp` `doWork` — the follower flush block becomes:

```cpp
    if (followerApplyDirty_ && !memoryBacked_
        && nowNs - lastFollowerSyncNs_ >= kFollowerFlushIntervalNs) {
        followerApplyDirty_ = false;
        lastFollowerSyncNs_ = nowNs;
        if (externalFollowerFlush_) {
            followerFlushRequested_ = true;     // the worker submits it (never blocks the loop)
        } else {
            RUNE_FR_SPAN_STORAGE(rune_fr::Site::StorageFlush, static_cast<uint32_t>(cfg_.partitionId));
            onLog([&](auto& L){ L.sync(); });
        }
        ++work;
    }
```

`Worker.h` — `DurablePending` gains two members (Strict in shard mode waits for HW after fsync):

```cpp
        rune::replication::PartitionReplica* thenAwaitHw = nullptr;  ///< shard Strict acks=all: HW gate after fsync
        int64_t                              deadlineNs  = 0;
```

Background-flush machinery (private):

```cpp
    // ---- Background (non-ack) flushes through io_ (spec §3.1 never-block) ------
    enum class BgFlushKind : uint8_t { Follower, OwnedLogSync };
    struct BgFlush {
        CompletionToken     token = kNoToken;
        rune::storage::Log* log   = nullptr;
        int64_t             leo   = 0;
        BgFlushKind         kind  = BgFlushKind::Follower;
    };
    static constexpr std::size_t kMaxBgFlushes = 256;
    std::array<BgFlush, kMaxBgFlushes> bgFlush_{};
    std::size_t                        bgFlushCount_ = 0;
    std::array<BgFlush, kMaxBgFlushes> bgRetry_{};      // submit refused → retried next pass
    std::size_t                        bgRetryCount_ = 0;
    uint64_t                           bgFlushSubmits_ = 0;
    uint64_t                           bgFlushErrors_  = 0;

    bool bgInFlight_(const rune::storage::Log* log, BgFlushKind kind) const noexcept {
        for (std::size_t i = 0; i < bgFlushCount_; ++i)
            if (bgFlush_[i].log == log && bgFlush_[i].kind == kind) return true;
        return false;
    }
    void queueBgRetry_(rune::storage::Log* log, BgFlushKind kind) noexcept {
        for (std::size_t i = 0; i < bgRetryCount_; ++i)
            if (bgRetry_[i].log == log && bgRetry_[i].kind == kind) return;
        if (bgRetryCount_ < kMaxBgFlushes) bgRetry_[bgRetryCount_++] = BgFlush{kNoToken, log, 0, kind};
    }
    void submitBgFlush_(rune::storage::Log* log, BgFlushKind kind) noexcept {
        if (bgInFlight_(log, kind)) return;                 // one in flight per (log, kind)
        if (bgFlushCount_ == kMaxBgFlushes) { queueBgRetry_(log, kind); return; }
        const CompletionToken t = io_.submit(log->activeSegmentFd(), log->flushBoundary());
        if (t == kNoToken) { queueBgRetry_(log, kind); return; }
        bgFlush_[bgFlushCount_++] = BgFlush{t, log, log->logEndOffset(), kind};
        ++bgFlushSubmits_;
    }
    int retryBgFlushes_() noexcept {
        const std::size_t n = bgRetryCount_;
        bgRetryCount_ = 0;                                  // re-queues land at indices already processed
        for (std::size_t i = 0; i < n; ++i) submitBgFlush_(bgRetry_[i].log, bgRetry_[i].kind);
        return static_cast<int>(n);
    }
    // Token routing: a background token is retired HERE and never reaches the
    // produce-ack matching.  Returns true iff c belonged to a background flush.
    bool retireBgFlush_(const FlushCompletion& c) noexcept {
        for (std::size_t i = 0; i < bgFlushCount_; ++i) {
            if (bgFlush_[i].token != c.token) continue;
            const BgFlush f = bgFlush_[i];
            bgFlush_[i] = bgFlush_[--bgFlushCount_];
            if (c.errorCode != 0) {
                ++bgFlushErrors_;
                spdlog::error("Worker {}: background fsync failed errno={} — retrying", workerId_, c.errorCode);
                queueBgRetry_(f.log, f.kind);
            } else if (f.kind == BgFlushKind::OwnedLogSync) {
                int64_t& last = lastSyncedLeo_[f.log];
                if (f.leo > last) last = f.leo;
            }
            return true;
        }
        return false;
    }
```

Replace `reapDurableCompletions_` with a single-handler version (the old fast path discarded completions when `durablePending_` was empty; background tokens must always be retired):

```cpp
    int reapDurableCompletions_() {
        int fired = 0;
        io_.reap([&](const FlushCompletion& c) { fired += onFlushCompletion_(c); });
        return fired + retryBgFlushes_();
    }

    int onFlushCompletion_(const FlushCompletion& c) {
        if (retireBgFlush_(c)) return 1;
        int fired = 0;
        for (std::size_t i = 0; i < durablePending_.size(); ) {
            auto& entry = durablePending_[i];
            if (entry.token != 0 && entry.token == c.token && entry.lastOffset <= c.flushBoundary) {
                if (c.errorCode != 0) {
                    spdlog::error("Worker {}: IoBackend fsync completion error={} for token={} "
                                  "lastOffset={}; acking Internal error",
                                  workerId_, c.errorCode, c.token, entry.lastOffset);
                    entry.ret.errorCode = static_cast<int16_t>(6);   // Internal
                    enqueueReturn_(entry.ret);
                } else if (entry.thenAwaitHw) {
                    entry.ret.errorCode = 0;
                    pendingAcksAll_.push_back(PendingAcksAll{entry.thenAwaitHw, entry.lastOffset,
                                                             entry.deadlineNs, std::move(entry.ret)});
                } else {
                    entry.ret.errorCode = 0;
                    enqueueReturn_(entry.ret);
                }
                durablePending_[i] = std::move(durablePending_.back());
                durablePending_.pop_back();
                ++fired;
            } else {
                ++i;
            }
        }
        return fired;
    }
```

Async owned-log sync (replaces `syncOwnedLogs_` in `doWorkShard_`; `syncOwnedLogs_` stays for the legacy path):

```cpp
    int syncOwnedLogsAsync_(int64_t nowNs) {
        if (lastSyncNs_ == 0) { lastSyncNs_ = nowNs; return 0; }
        if (nowNs - lastSyncNs_ < kWorkerSyncIntervalNs) return 0;
        lastSyncNs_ = nowNs;
        int n = 0;
        for (auto* replica : owned_) {
            if (!replica || !replica->isDiskBacked()) continue;
            rune::storage::Log* log = replica->log();
            const auto it = lastSyncedLeo_.find(log);
            const int64_t last = (it == lastSyncedLeo_.end()) ? 0 : it->second;
            if (log->logEndOffset() <= last) continue;      // unchanged: no phantom work
            submitBgFlush_(log, BgFlushKind::OwnedLogSync);
            ++n;
        }
        return n;
    }
```

In `doWorkShard_`: after the owned-replica `doWork` loop add

```cpp
        for (auto* replica : owned_) {
            if (replica && replica->takeFollowerFlushRequest())
                submitBgFlush_(replica->log(), BgFlushKind::Follower);
        }
```

and replace `work += syncOwnedLogs_(nowNs);` with `work += syncOwnedLogsAsync_(nowNs);`.

In `handleWorkerIngressFrame_`, replace the `durabilityTier` / `handleProduceBatch` lines with:

```cpp
        const uint8_t durabilityTier = replica ? replica->durabilityTier() : 1;
        // Shard mode never fsyncs inline on the loop: Strict appends page-cache only
        // and acks from its io_ completion (then the HW gate for acks=all).
        const bool asyncStrict = shardMode_ && durabilityTier == 2;
        rune::storage::ProduceOutcome outcome = rune::storage::handleProduceBatch(
            rbSpan.data(), rbSpan.size(),
            msg.topicId, msg.partition,
            msg.producerId, msg.producerEpoch, msg.baseSequence,
            *log, psm_,
            acksAll, isrSize, minIsr, asyncStrict ? uint8_t{1} : durabilityTier);
```

and, right after `if (replica) { replica->onLeaderLogAppended(); }`:

```cpp
        if (asyncStrict && !outcome.isDuplicate) {
            ret.errorCode = 0;
            DurablePending dp;
            dp.token       = kNoToken;
            dp.lastOffset  = outcome.lastOffset;
            dp.logPtr      = log;
            dp.ret         = std::move(ret);
            dp.thenAwaitHw = acksAll ? replica : nullptr;
            dp.deadlineNs  = nowNs + ackTimeoutNs_;
            durablePending_.push_back(std::move(dp));
            if (std::find(dirtyThisCycle_.begin(), dirtyThisCycle_.end(), log) == dirtyThisCycle_.end())
                dirtyThisCycle_.push_back(log);
            return 1;
        }
```

Test hooks (public):

```cpp
    [[nodiscard]] std::size_t bgFlushInFlightForTest() const noexcept { return bgFlushCount_; }
    [[nodiscard]] uint64_t    bgFlushSubmitsForTest() const noexcept { return bgFlushSubmits_; }
    [[nodiscard]] uint64_t    bgFlushErrorsForTest() const noexcept { return bgFlushErrors_; }
    CompletionToken submitBgFlushForTest(rune::storage::Log* log) {
        submitBgFlush_(log, BgFlushKind::Follower);
        return bgFlushCount_ ? bgFlush_[bgFlushCount_ - 1].token : kNoToken;
    }
    [[nodiscard]] CompletionToken durablePendingTokenForTest(std::size_t i) const { return durablePending_.at(i).token; }
    void deliverCompletionForTest(const FlushCompletion& c) { (void)onFlushCompletion_(c); }
```

(add `#include <array>`). `BrokerNode.cpp`: in `buildReplication`, after `auto replica = isMemory ? ... : ...;` add `if (config_.shardMode() && topicId >= 0) replica->setExternalFollowerFlush(true);`, and the same line (with the runtime topic id) where `createRuntimeUserReplicas` constructs replicas.

- [ ] **Step 4: Run to verify they pass**

Run:
```bash
cmake --build build-fast
./build-fast/libs/node/rune_shard_nonblocking_tests
./build-fast/libs/node/rune_async_durability_tests
./build-fast/libs/node/rune_shard_mode_tests
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast --output-on-failure -j2
```
Expected: PASS (legacy `AsyncDurability.*` unchanged; `rune_uring_durability_tests` runs for real on the Linux VM in Task 16).

- [ ] **Step 5: Commit**

```bash
git add libs/storage/include/rune/storage/Log.h libs/storage/src/Log.cpp \
        libs/replication/include/rune/replication/PartitionReplica.h libs/replication/src/PartitionReplica.cpp \
        libs/node/include/rune/node/Worker.h libs/node/src/BrokerNode.cpp libs/node/CMakeLists.txt \
        libs/node/tests/integration/ShardNonBlockingTest.cpp
git commit -m "fix(node): shard loop never blocks — follower flush, log sync and Strict tier via the io backend

Background flush tokens are retired before produce-ack matching (token routing).

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 11: Runtime placement `fused` — the runtime duty cycle inside worker 0's loop

Spec §3.2. With `RUNE_RUNTIME_PLACEMENT=fused` the node-level driver becomes an INVOKER `EmbeddedDriver`, the node Aeron client uses the conductor invoker, the runtime `AgentRunner` is never started, and `ShardAgent[0]` appends to its pass: runtime driver → runtime conductor → `NodeRuntimeAgent::doWork` (a `final` class → direct call). During `start()` the startup thread owns the runtime driver: every startup resolve on the runtime client (consensus `AeronIngress`, `UdpMessageSink`, `AeronPartitionTransport`, `ClientGateway`) takes the node pump; ownership hands over to worker 0 at its first pass. `ClientGateway::resolvePublication` — a blocking 5 s sleep-poll on the gateway (runtime) thread with no remaining callers — is deleted (fix on sight: it would deadlock a fused loop). `thread` placement is byte-for-byte today's behaviour.

**Files:**
- Modify: `libs/node/include/rune/node/ShardAgent.h`, `libs/node/src/ShardAgent.cpp` (fused runtime)
- Modify: `libs/node/src/BrokerNode.cpp` (`startEmbeddedDriver`, `start()` client connect + pump + runner start; `buildMultiNodeConsensus` ~line 263/288; `buildReplication` ~line 421; `buildGateway` ~line 1123), `libs/node/include/rune/node/BrokerNode.h` (`nodePump_`, `runtimeThreadStartedForTest()`)
- Modify: `libs/consensus/include/rune/consensus/AeronIngress.h:39-70`, `libs/consensus/CMakeLists.txt` (link `rune_aeron_util`)
- Modify: `libs/replication/include/rune/replication/AeronPartitionTransport.h:104`, `libs/replication/src/AeronPartitionTransport.cpp:50-75`
- Modify: `libs/gateway/include/rune/gateway/ClientGateway.h:128-133, :1014`, `libs/gateway/src/ClientGateway.cpp:59-103, :1885-1910`, `libs/gateway/CMakeLists.txt` (link `rune_aeron_util`)
- Modify: `libs/node/tests/node/AeronPumpInvokerTest.cpp`, `libs/node/tests/integration/ShardModeTest.cpp`

**Interfaces:**
- Consumes: Tasks 7–9.
- Produces: `void ShardAgent::setFusedRuntime(NodeRuntimeAgent* runtime, EmbeddedDriver* runtimeDriver, aeron::Aeron* runtimeClient) noexcept;` `AeronIngress(std::shared_ptr<aeron::Aeron>, const std::string&, int32_t, IngressRing&, rune::aeronutil::AeronPump pump = {})`; `AeronPartitionTransport(..., rune::aeronutil::AeronPump pump = {})` (last parameter); `ClientGateway(..., int64_t connectResolveTimeoutNs = ..., rune::aeronutil::AeronPump pump = {})`; `bool BrokerNode::runtimeThreadStartedForTest() const noexcept`.

- [ ] **Step 1: Write the failing tests**

Append to `AeronPumpInvokerTest.cpp` (add `#include <rune/consensus/AeronIngress.h>` and `#include <rune/consensus/IngressRing.h>`):

```cpp
TEST(AeronPumpInvoker, ConsensusIngressResolvesOnInvokerDriver) {
    InvokerShard s;
    rune::consensus::IngressRing ring(64 * 1024);
    rune::consensus::AeronIngress ing(s.client, "aeron:ipc", 7201, ring, s.pump());
    SUCCEED() << "constructor would throw 'subscription not resolved' without the pump";
}
```

(`IngressRing(int payloadCapacity)` — `IngressRing.h:54`; `BrokerNode` passes `kIngressCapacity = 64 * 1024`, `BrokerNode.cpp:45`.)

Append to `ShardModeTest.cpp`:

```cpp
TEST(ShardModeFused, SingleNodeRunsRuntimeOnWorkerZero) {
    TmpDir dir{"fused"}, cdir{"fc"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms)) << "consensus must run inside worker 0's loop";
    EXPECT_FALSE(node.runtimeThreadStartedForTest());
    {
        ShardTestClient c(cdir.path);
        const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
        ASSERT_GT(sid, 0) << "gateway Connect served by the fused runtime";
        auto ack = c.produceUntilOk(workerIngressChannel(ports, 0, 0), kIngressStream, sid, 0, 0, 1, makeBatch(2));
        ASSERT_TRUE(ack.has_value());
    }
    node.stop();
}

TEST(ShardModeFusedRegression, ReturnRingPressureNeverDeadlocksTheFusedLoop) {
    TmpDir dir{"fusedring"}, cdir{"frc"};
    const auto ports = ShardPorts::discover(1, 1);
    auto cfg = makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true);
    cfg.workerReturnRingBytes = 64 * 1024;        // tiny: forces intake skips
    cfg.workerAckSlotBytes = 256;
    BrokerNode node{cfg};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0, 0), kIngressStream, sid, 0, 0, 0, makeBatch(1)));
    constexpr int kN = 2000;
    for (int i = 0; i < kN; ++i)
        ASSERT_TRUE(c.produce(workerIngressChannel(ports, 0, 0), kIngressStream, sid, 10'000 + i, 0, 0, 0, makeBatch(1)));
    int ok = 0;
    for (int i = 0; i < kN; ++i) {
        auto a = c.awaitProduceResponse(10'000 + i);
        ok += (a && a->errorCode == 0) ? 1 : 0;
    }
    EXPECT_EQ(ok, kN) << "producer and consumer of ret_ share one thread — no path may wait on the other";
    node.stop();
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_aeron_pump_tests rune_shard_mode_tests`
Expected: FAIL to compile — `no matching constructor for initialization of 'rune::consensus::AeronIngress'` (5 args) and `no member named 'runtimeThreadStartedForTest'`.

- [ ] **Step 3: Implement**

`ShardAgent.h`: forward-declare `class NodeRuntimeAgent;`, add public

```cpp
    /// Spec §3.2 fused placement: run the runtime (its own invoker driver + client
    /// conductor + NodeRuntimeAgent) at the end of this shard's pass.  Worker 0 only;
    /// set before the runner starts.
    void setFusedRuntime(NodeRuntimeAgent* runtime, EmbeddedDriver* runtimeDriver,
                         aeron::Aeron* runtimeClient) noexcept {
        runtime_ = runtime; runtimeDriver_ = runtimeDriver; runtimeClient_ = runtimeClient;
    }
```

and private `NodeRuntimeAgent* runtime_ = nullptr; EmbeddedDriver* runtimeDriver_ = nullptr; aeron::Aeron* runtimeClient_ = nullptr;`. `ShardAgent.cpp`: `#include <rune/node/NodeRuntimeAgent.h>`; in the bind block add `if (runtimeDriver_) runtimeDriver_->bindPumpThread();`; before `return work;`:

```cpp
    if (runtime_) {                                         // fused runtime (worker 0)
        work += runtimeDriver_->doWork();
        work += runtimeClient_->conductorAgentInvoker().invoke();
        work += runtime_->doWork(nowNs);                    // NodeRuntimeAgent is final → direct call
    }
```

`AeronIngress.h`: add `#include <rune/aeronutil/AeronPump.h>`, parameter `rune::aeronutil::AeronPump pump = {}` after `IngressRing& ingressRing`, and replace the 5 s loop with

```cpp
        int64_t regId = aeron_->addSubscription(channel, streamId);
        sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); }, std::chrono::seconds{5});
        if (!sub_) {
            throw std::runtime_error("AeronIngress: subscription not resolved within timeout");
        }
```

`libs/consensus/CMakeLists.txt`: add `rune_aeron_util` to `rune_consensus`'s `target_link_libraries(... PUBLIC ...)`. Do the same in `AeronPartitionTransport` (header: new last ctor parameter `rune::aeronutil::AeronPump pump = {}`; `.cpp:62-70` → `sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); }, std::chrono::milliseconds{kResolveMaxMs});`) and `ClientGateway` (header ctor: new last parameter `rune::aeronutil::AeronPump pump = {}`; `.cpp:89-99` → `sub_ = pump.resolve([&] { return aeron_->findSubscription(regId); }, std::chrono::milliseconds{kResolveMaxMs});`; link `rune_aeron_util` into `rune_gateway`). Delete `ClientGateway::resolvePublication` (declaration `ClientGateway.h:1013-1014` and definition `ClientGateway.cpp:1885-1910`); verify `grep -rn 'resolvePublication(' libs` only matches the historical comment at `ClientGateway.cpp:707`.

`BrokerNode.h`: `rune::aeronutil::AeronPump nodePump_;` and `bool runtimeThreadStartedForTest() const noexcept { return agentRunner_ && agentRunner_->isRunning(); }`.

`BrokerNode.cpp`:

```cpp
void BrokerNode::startEmbeddedDriver() {
    nodeDriver_ = std::make_unique<EmbeddedDriver>(
        EmbeddedDriverConfig::fromNode(config_, config_.effectiveAeronDir()),
        config_.fused() ? DriverMode::Invoker : DriverMode::Threaded);
}
```

In `start()` step 4: `ctx.useConductorAgentInvoker(config_.fused());` before `aeron::Aeron::connect(ctx)`, and after the connect: `if (config_.fused()) nodePump_ = nodeDriver_->pumpWith(aeron_.get());` Pass `nodePump_` as the new last argument to: `std::make_unique<AeronIngress>(aeron_, selfPeer.consensusChannel(), config_.consensusStreamId, *ingressRing_, nodePump_)`, `std::make_unique<UdpMessageSink>(aeron_, peerEndpoints, nodePump_)`, `std::make_unique<rune::replication::AeronPartitionTransport>(aeron_, ingressChannel, ingressStream, selfId, config_.replicationOutboundRingBytes, nodePump_)`, and the `ClientGateway` construction in `buildGateway` (append `rune::gateway::ClientGateway::kDefaultConnectResolveTimeoutNs, nodePump_` after `std::move(leaderViewFn)`). In `start()` step 7 (`BrokerNode.cpp:134`) replace `agentRunner_->start();` with:

```cpp
    // Spec §3.2: in fused placement the runtime has no thread of its own.
    if (!config_.fused()) agentRunner_->start();
```

and in step 7b, inside `if (config_.shardMode()) {` immediately before `workerPool_->startShardRunners(...)`:

```cpp
            if (config_.fused()) {
                // The fused hook must be set before worker 0's thread exists; the
                // runtime driver/client pass to that thread at its first pass.
                shardAgents_.at(0)->setFusedRuntime(runtimeAgent_.get(), nodeDriver_.get(), aeron_.get());
            }
```

- [ ] **Step 4: Run to verify they pass**

Run:
```bash
cmake --build build-fast
./build-fast/libs/node/rune_aeron_pump_tests
./build-fast/libs/node/rune_shard_mode_tests
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast --output-on-failure -j2
```
Expected: PASS (thread placement suites unchanged).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/ShardAgent.h libs/node/src/ShardAgent.cpp \
        libs/node/include/rune/node/BrokerNode.h libs/node/src/BrokerNode.cpp \
        libs/consensus/include/rune/consensus/AeronIngress.h libs/consensus/CMakeLists.txt \
        libs/replication/include/rune/replication/AeronPartitionTransport.h libs/replication/src/AeronPartitionTransport.cpp \
        libs/gateway/include/rune/gateway/ClientGateway.h libs/gateway/src/ClientGateway.cpp libs/gateway/CMakeLists.txt \
        libs/node/tests/node/AeronPumpInvokerTest.cpp libs/node/tests/integration/ShardModeTest.cpp
git commit -m "feat(node): RUNE_RUNTIME_PLACEMENT=fused — runtime duty cycle inside worker 0's loop

Runtime keeps its own invoker driver + client, pumped by that loop; startup resolves
pump the runtime driver; dead blocking ClientGateway::resolvePublication removed.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---
### Task 12: rune-protocol internal ring — session announce/close + post-ack produce notification

Spec §3.2 / §3.4: "Runtime ↔ worker traffic stays on the existing SPSC rings (… session announcements, post-ack notifications)". The process-local SBE schema (`schemaId=7`) gains two op types, one tail kind, one scalar and one return type; the generated flyweights are regenerated with the vendored `sbe-all-1.40.2.jar`. The broker's `WorkerOp::Type` / `WorkerReturn::Type` enums map to these by value (`static_cast`), so the new values are 13, 14 and 2 exactly. The broker's `executeFromWire_` switches on its own enum, so the broker still builds after this task unchanged.

**Files:**
- Modify: `~/Projects/rune-protocol/schema/rune-internal-ring.xml` (enums `OpType`, `TailKind`, `ReturnType`; `WorkerOp` field after `egressPub`)
- Regenerate: `~/Projects/rune-protocol/include/rune_protocol_internal_sbe/*.h`
- Test: `~/Projects/rune-protocol/tests/protocol/internal/RingSbeTest.cpp` (append)

**Interfaces:**
- Produces (namespace `rune::protocol::internal::sbe`): `OpType::Value::SESSION_ANNOUNCE` (13), `OpType::Value::SESSION_CLOSE` (14), `TailKind::Value::EGRESS_CHANNEL` (7), `ReturnType::Value::PRODUCE_NOTIFY` (2), `WorkerOp::egressStreamId(int32_t)` / `int32_t WorkerOp::egressStreamId() const`.

- [ ] **Step 1: Write the failing tests** (append to `RingSbeTest.cpp`)

```cpp
TEST(RingSbe, SessionAnnounceCarriesEgressChannelAndStream) {
    std::array<char, 1024> buf{};
    const std::string ch = "aeron:udp?endpoint=10.0.0.9:40900";
    sbe::WorkerOp enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.opType(sbe::OpType::Value::SESSION_ANNOUNCE)
       .sessionId(77)
       .egressStreamId(9)
       .tailKind(sbe::TailKind::Value::EGRESS_CHANNEL);
    enc.putTail(ch);
    EXPECT_EQ(enc.encodedLength(), sbe::WorkerOp::computeLength(ch.size()));

    auto dec = decodeAt<sbe::WorkerOp>(buf.data(), buf.size());
    EXPECT_EQ(dec.opType(), sbe::OpType::Value::SESSION_ANNOUNCE);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::SESSION_ANNOUNCE), 13);
    EXPECT_EQ(static_cast<int>(sbe::OpType::Value::SESSION_CLOSE), 14);
    EXPECT_EQ(dec.sessionId(), 77);
    EXPECT_EQ(dec.egressStreamId(), 9);
    EXPECT_EQ(dec.tailKind(), sbe::TailKind::Value::EGRESS_CHANNEL);
    const std::uint32_t len = dec.tailLength();
    const char* p = dec.tail();
    EXPECT_EQ(std::string(p, len), ch);
}

TEST(RingSbe, ProduceNotifyReturnTypeRoundTrips) {
    std::array<char, 512> buf{};
    sbe::WorkerReturn enc;
    enc.wrapAndApplyHeader(buf.data(), 0, buf.size());
    enc.returnType(sbe::ReturnType::Value::PRODUCE_NOTIFY)
       .sessionId(0).correlationId(0).partition(3).baseOffset(1234).lastOffset(1240)
       .errorCode(0).deferAck(0).replica(0).producerId(55).producerEpoch(1)
       .baseSequence(10).recordCount(7).topicId(2).batchAttrs(0x10).enqueuedAtNs(0)
       .subscriptionId(0).readTopicId(0).readPartition(0).newNextOffset(0).hwBound(0)
       .isolation(0).isGroup(0).groupCarrierReceived(0);
    enc.putChunk(nullptr, 0);
    auto dec = decodeAt<sbe::WorkerReturn>(buf.data(), buf.size());
    EXPECT_EQ(dec.returnType(), sbe::ReturnType::Value::PRODUCE_NOTIFY);
    EXPECT_EQ(static_cast<int>(sbe::ReturnType::Value::PRODUCE_NOTIFY), 2);
    EXPECT_EQ(dec.producerId(), 55);
    EXPECT_EQ(dec.baseOffset(), 1234);
    EXPECT_EQ(dec.batchAttrs(), 0x10);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-protocol/build --target rune_protocol_tests`
Expected: FAIL to compile — `'SESSION_ANNOUNCE' is not a member of 'OpType::Value'`, `no member named 'egressStreamId'`.

- [ ] **Step 3: Edit the schema and regenerate**

In `schema/rune-internal-ring.xml`:
- in `<enum name="OpType">` after `SCHEMA_UPDATE`:

```xml
      <!-- Self-contained worker shards (spec §3.4): runtime → every worker.
           SESSION_ANNOUNCE: sessionId + egressStreamId + egress channel (tail,
           TailKind = EGRESS_CHANNEL); the worker opens its OWN exclusive publication.
           SESSION_CLOSE: sessionId; the worker drops the session and its queued acks. -->
      <validValue name="SESSION_ANNOUNCE">13</validValue>
      <validValue name="SESSION_CLOSE">14</validValue>
```
- in `<enum name="TailKind">` after `DESCRIPTOR_BLOB`: `<validValue name="EGRESS_CHANNEL">7</validValue>`
- in `<enum name="ReturnType">` after `PUSH_READ_RESULT`:

```xml
      <!-- Post-ack, fire-and-forget worker → runtime notification (spec §3.4):
           PSM mirror + transaction LSO bookkeeping; never on the ack path. -->
      <validValue name="PRODUCE_NOTIFY">2</validValue>
```
- in `<sbe:message name="WorkerOp">` immediately after the `egressPub` field:

```xml
    <!-- SESSION_ANNOUNCE: the client session's egress stream id (channel in the tail). -->
    <field name="egressStreamId"  id="45" type="int32"/>
```

Regenerate:

```bash
cd ~/Projects/rune-protocol
JAVA=/Applications/CLion.app/Contents/jbr/Contents/Home/bin/java
"$JAVA" -Dsbe.target.language=cpp -Dsbe.output.dir=include \
    -jar tools/sbe-all-1.40.2.jar schema/rune-internal-ring.xml
git status --short include/rune_protocol_internal_sbe
```
Expected: `OpType.h`, `TailKind.h`, `ReturnType.h`, `WorkerOp.h` modified; no other file changes.

- [ ] **Step 4: Run to verify they pass (protocol + broker still build and pass)**

Run:
```bash
cmake --build ~/Projects/rune-protocol/build --target rune_protocol_tests && ~/Projects/rune-protocol/build/rune_protocol_tests
cmake --build ~/Projects/rune-broker/build-fast
ctest --test-dir ~/Projects/rune-broker/build-fast -R Regression --output-on-failure
ctest --test-dir ~/Projects/rune-broker/build-fast -E 'udp|UDP|Udp' --output-on-failure -j4
```
Expected: PASS (the broker ring codec encodes the new `egressStreamId` scalar as whatever the claimed slot held until Task 13 sets it; nothing reads it yet).

- [ ] **Step 5: Commit**

```bash
cd ~/Projects/rune-protocol
git add schema/rune-internal-ring.xml include/rune_protocol_internal_sbe tests/protocol/internal/RingSbeTest.cpp
git commit -m "feat(protocol): internal ring SESSION_ANNOUNCE/SESSION_CLOSE + PRODUCE_NOTIFY (worker-side acks)

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 13: Worker-side acks — session announcement, per-session publications, coalescing, G-A6 backlog, post-ack notify, PSM authority

Spec §3.4. In shard mode the worker encodes the `ProduceResponse` and claims it on **its own** exclusive publication to the client session; every ack due in a pass is claimed before the shard's next driver duty cycle, so the sender ships each session's acks together (coalescing at the datagram level — the client protocol, frames and correlation ids are unchanged; resolution of the "one frame" wording in §3.4). The runtime announces every accepted session to every worker over the existing ring **before** it sends `ConnectResponse` (announce-before-accept), and closes it on Disconnect / reap / connect timeout. After the ack, a fire-and-forget `ProduceNotify` goes to the runtime (PSM mirror + transaction `LsoTracker`); if the return ring is full it waits in a FIFO backlog and is retried next pass — never dropped, never delaying an ack. The worker `psm_` is the single owner in shard mode: the gateway no longer validates user-partition produces on its mirror, and gateway-received user produces reach the worker through the same `produceCore_` as worker-ingress produces. A demoted leader fails its parked acks=all promptly with retriable `NotLeader`.

**Files:**
- Create: `libs/node/include/rune/node/SessionAckSink.h`, `libs/node/include/rune/node/SessionAnnouncer.h`
- Modify: `libs/node/include/rune/node/WorkerOp.h` (`WorkerOp::Type` + fields; `WorkerReturn::Type`)
- Modify: `libs/node/include/rune/node/Worker.h` (`tryEnqueueOp` ~line 370, `executeFromWire_` ~960, `executeProduceWire_` ~1273, `flushOwedAcksAll_`, `pollWorkerIngress_`, `handleWorkerIngressFrame_` → `produceCore_`, `doWorkShard_`, new members/hooks)
- Modify: `libs/gateway/include/rune/gateway/ClientGateway.h`, `libs/gateway/src/ClientGateway.cpp` (`doWork` ~397, `reapExpired` ~539, `pollPendingConnects` ~814-978, `handleDisconnect` ~1092, produce PSM gate ~1646, `drainWorkerReturns_` ~2874)
- Modify: `libs/node/src/BrokerNode.cpp` (shard branch of `buildWorkerPool`: `setShardClient`, `gateway_->setShardMode(true)`)
- Create tests: `libs/node/tests/node/SessionAckSinkTest.cpp`, `libs/node/tests/node/SessionAnnouncerTest.cpp`, `libs/node/tests/node/ShardWorkerAckTest.cpp`, `libs/gateway/tests/gateway/ShardNotifyTest.cpp`; Modify: `libs/node/tests/integration/ShardModeTest.cpp`, CMake files

**Interfaces:**
- Consumes: Task 12 SBE values; Task 9 `doWorkShard_`, `setShardMode`; Task 10 `onFlushCompletion_`, `DurablePending::thenAwaitHw`.
- Produces:
  - `struct AckEntry { int64_t sessionId, correlationId, baseOffset; int32_t partition; int16_t errorCode; };` `enum class PubFind : uint8_t { Pending, Ready, Error };` `template <class PubPolicy> class SessionAckSinkT` with `bool announce(int64_t, std::string_view, int32_t) noexcept; void close(int64_t) noexcept; bool enqueue(const AckEntry&) noexcept; int flush(int64_t nowNs) noexcept; bool intakeAllowed() const noexcept; std::size_t freeAckSlots() const noexcept;` `struct AeronSessionPubPolicy`; `using SessionAckSink = SessionAckSinkT<AeronSessionPubPolicy>;`
  - `class SessionAnnouncer { bool announce(WorkerPool&, int64_t, std::string_view, int32_t); void close(WorkerPool&, int64_t); int retryCloses(WorkerPool&); std::size_t pendingClosesForTest() const noexcept; };`
  - `WorkerOp::Type::SessionAnnounce` (13), `WorkerOp::Type::SessionClose` (14); `int32_t WorkerOp::egressStreamId`; `std::string_view WorkerOp::egressChannel`; `WorkerReturn::Type::ProduceNotify` (2)
  - `void Worker::setShardClient(aeron::Aeron* client) noexcept;` test hooks `uint64_t sessionAnnouncesSeenForTest() const noexcept; bool injectReturnForTest(const WorkerReturn&);`
  - `void ClientGateway::setShardMode(bool) noexcept; void ClientGateway::drainWorkerReturnsForTest(); rune::storage::ProducerStateManager& ClientGateway::producerStateManagerForTest() noexcept;`

- [ ] **Step 1: Write the failing tests**

`libs/node/tests/node/SessionAckSinkTest.cpp`:

```cpp
// SessionAckSinkTest.cpp — worker-side ack publication table + backlog state
// machines (spec §3.4, G-A6).  Fake publication policy; injected clock only.
#include <rune/node/SessionAckSink.h>
#include <rune/protocol/client/DataMessages.h>

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

using namespace rune::node;
namespace pc = rune::protocol::client;

namespace {
struct FakePub { int id = 0; };
struct FakePolicy {
    using PubHandle = std::shared_ptr<FakePub>;
    struct State {
        int64_t nextReg = 1;
        std::map<int64_t, std::pair<std::string, int32_t>> added;
        bool findReady = true, findError = false;
        std::set<int> blocked;                                  // pub ids that back-pressure
        std::vector<std::pair<int, std::vector<uint8_t>>> sent; // (pub id, frame)
    };
    std::shared_ptr<State> s = std::make_shared<State>();
    int64_t add(std::string_view ch, int32_t stream) noexcept {
        const int64_t r = s->nextReg++;
        s->added[r] = {std::string(ch), stream};
        return r;
    }
    PubFind find(int64_t reg, PubHandle& out) noexcept {
        if (s->findError) return PubFind::Error;
        if (!s->findReady) return PubFind::Pending;
        out = std::make_shared<FakePub>(FakePub{static_cast<int>(reg)});
        return PubFind::Ready;
    }
    template <class E> int64_t claim(PubHandle& p, std::size_t len, E&& enc) noexcept {
        if (s->blocked.count(p->id)) return -2;                 // BACK_PRESSURED
        std::vector<uint8_t> b(len);
        enc(b.data());
        s->sent.push_back({p->id, std::move(b)});
        return 1;
    }
};
using Sink = SessionAckSinkT<FakePolicy>;
constexpr int64_t kMs = 1'000'000;

AckEntry ack(int64_t sid, int64_t corr, int64_t base = 0) { return AckEntry{sid, corr, base, 0, 0}; }

// Announce + resolve (two flushes: Announced→Resolving, Resolving→Ready).
void ready(Sink& s, int64_t sid, int64_t& now) {
    ASSERT_TRUE(s.announce(sid, "aeron:udp?endpoint=127.0.0.1:1", 7));
    s.flush(now += kMs);
    s.flush(now += kMs);
}
} // namespace

TEST(SessionAckSink, AnnounceResolvesThenSendsEncodedProduceResponse) {
    FakePolicy pol; auto st = pol.s;
    Sink s(pol);
    ASSERT_TRUE(s.announce(7, "aeron:udp?endpoint=h:1", 42));
    ASSERT_TRUE(s.enqueue(AckEntry{7, 100, 5, 2, 0}));
    s.flush(1 * kMs);                                       // Announced → Resolving; ack held
    EXPECT_TRUE(st->sent.empty());
    EXPECT_FALSE(s.intakeAllowed());
    s.flush(2 * kMs);                                       // Resolving → Ready; ack sent
    ASSERT_EQ(st->sent.size(), 1u);
    EXPECT_TRUE(s.intakeAllowed());
    EXPECT_EQ(st->added.begin()->second.first, "aeron:udp?endpoint=h:1");
    EXPECT_EQ(st->added.begin()->second.second, 42);
    auto r = pc::ProduceResponse::decode(st->sent[0].second.data(), st->sent[0].second.size());
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->sessionId, 7); EXPECT_EQ(r->correlationId, 100);
    EXPECT_EQ(r->baseOffset, 5); EXPECT_EQ(r->partition, 2); EXPECT_EQ(r->errorCode, 0);
}

TEST(SessionAckSink, CoalescesEveryAckOfAPassOnTheSessionPublication) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now); ready(s, 8, now);
    s.enqueue(ack(7, 1)); s.enqueue(ack(7, 2)); s.enqueue(ack(8, 3)); s.enqueue(ack(7, 4));
    EXPECT_TRUE(st->sent.empty()) << "nothing leaves before the pass flush";
    s.flush(now += kMs);
    ASSERT_EQ(st->sent.size(), 4u);
    std::vector<int64_t> corrs7;
    for (auto& [pub, f] : st->sent)
        if (auto r = pc::ProduceResponse::decode(f.data(), f.size()); r && r->sessionId == 7) corrs7.push_back(r->correlationId);
    EXPECT_EQ(corrs7, (std::vector<int64_t>{1, 2, 4}));
}

TEST(SessionAckSinkRegression, BackPressureBacklogsPausesIntakeAndPreservesOrder) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);
    st->blocked.insert(1);                                  // the session's pub (reg 1)
    s.enqueue(ack(7, 10)); s.enqueue(ack(7, 11));
    s.flush(now += kMs);
    EXPECT_FALSE(s.intakeAllowed()) << "G-A6: back-pressured acks pause intake";
    EXPECT_EQ(s.pendingForTest(), 2u);
    st->blocked.clear();
    s.flush(now += kMs);
    ASSERT_EQ(st->sent.size(), 2u);
    EXPECT_EQ(pc::ProduceResponse::decode(st->sent[0].second.data(), st->sent[0].second.size())->correlationId, 10);
    EXPECT_EQ(pc::ProduceResponse::decode(st->sent[1].second.data(), st->sent[1].second.size())->correlationId, 11);
    EXPECT_TRUE(s.intakeAllowed());
}

TEST(SessionAckSinkRegression, CloseWhileBackloggedClearsAndResumesIntake) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);
    st->blocked.insert(1);
    s.enqueue(ack(7, 10));
    s.flush(now += kMs);
    ASSERT_FALSE(s.intakeAllowed());
    s.close(7);
    s.flush(now += kMs);
    EXPECT_TRUE(s.intakeAllowed()) << "a closed session's acks must not pause the shard";
    EXPECT_EQ(s.droppedForTest(), 1u);
}

TEST(SessionAckSinkRegression, StalledSessionDroppedAfterStallWindow) {
    FakePolicy pol; auto st = pol.s; Sink s(pol); int64_t now = 0;
    ready(s, 7, now);                                       // pub id 1
    ready(s, 8, now);                                       // pub id 2
    st->blocked.insert(1);
    s.enqueue(ack(7, 1));
    const int64_t t0 = (now += kMs);
    s.flush(t0);
    s.enqueue(ack(8, 2));
    s.flush(t0 + kMs);
    EXPECT_FALSE(s.intakeAllowed());
    EXPECT_EQ(st->sent.size(), 1u) << "other sessions keep flowing";
    s.flush(t0 + Sink::kStallDropNs);
    EXPECT_TRUE(s.intakeAllowed()) << "a never-connecting client cannot block the shard";
    ASSERT_NE(s.stateForTest(7), nullptr);
    EXPECT_TRUE(std::holds_alternative<Sink::PubFailed>(*s.stateForTest(7)));
    s.enqueue(ack(7, 3));
    s.flush(t0 + Sink::kStallDropNs + kMs);
    EXPECT_TRUE(s.intakeAllowed());
}

TEST(SessionAckSink, ResolveTimeoutFailsTheSession) {
    FakePolicy pol; auto st = pol.s; Sink s(pol);
    st->findReady = false;
    ASSERT_TRUE(s.announce(7, "c", 1));
    s.enqueue(ack(7, 1));
    s.flush(1 * kMs);
    s.flush(2 * kMs);
    EXPECT_FALSE(s.intakeAllowed());
    s.flush(1 * kMs + Sink::kResolveTimeoutNs + 1);
    EXPECT_TRUE(s.intakeAllowed());
    EXPECT_EQ(s.droppedForTest(), 1u);
}

TEST(SessionAckSink, UnknownSessionAckIsDroppedAndCounted) {
    FakePolicy pol; Sink s(pol);
    s.enqueue(ack(99, 1));
    s.flush(1 * kMs);
    EXPECT_TRUE(s.intakeAllowed());
    EXPECT_EQ(s.droppedForTest(), 1u);
}

TEST(SessionAckSink, OversizedChannelAnnouncementRefused) {
    FakePolicy pol; Sink s(pol);
    EXPECT_FALSE(s.announce(7, std::string(Sink::kMaxChannelBytes + 1, 'x'), 1));
}
```

`libs/node/tests/node/SessionAnnouncerTest.cpp`:

```cpp
// SessionAnnouncerTest.cpp — announce-before-accept over the runtime→worker rings.
#include <rune/node/SessionAnnouncer.h>
#include <rune/node/WorkerPool.h>
#include <rune/metadata/ShardRouter.h>

#include <gtest/gtest.h>

using namespace rune::node;

namespace {
std::unique_ptr<WorkerPool> pool(int32_t n, std::size_t ringBytes = 1 << 16) {
    return std::make_unique<WorkerPool>(n, rune::metadata::ShardRouter(n), ringBytes, 4096, 16, 16);
}
void drainAll(WorkerPool& p) { for (int32_t w = 0; w < p.workerCount(); ++w) p.worker(w).doWork(1); }
}

TEST(SessionAnnouncer, AnnouncesOnceToEveryWorker) {
    auto p = pool(3);
    SessionAnnouncer a;
    EXPECT_TRUE(a.announce(*p, 5, "aeron:udp?endpoint=h:1", 9));
    drainAll(*p);
    for (int32_t w = 0; w < 3; ++w) EXPECT_EQ(p->worker(w).sessionAnnouncesSeenForTest(), 1u);
}

TEST(SessionAnnouncerRegression, FullRingDefersAndRetriesOnlyTheMissingWorker) {
    auto p = pool(2, /*ringBytes=*/4096);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(1).tryEnqueueOp(filler)) {}             // worker 1's ring is now full
    SessionAnnouncer a;
    EXPECT_FALSE(a.announce(*p, 5, "c", 9)) << "no ConnectResponse until every worker knows the session";
    p->worker(1).doWork(1);                                  // drains worker 1
    EXPECT_TRUE(a.announce(*p, 5, "c", 9));
    drainAll(*p);
    EXPECT_EQ(p->worker(0).sessionAnnouncesSeenForTest(), 1u) << "worker 0 must not get a duplicate";
    EXPECT_EQ(p->worker(1).sessionAnnouncesSeenForTest(), 1u);
}

TEST(SessionAnnouncerRegression, CloseRetriesUntilEveryWorkerAccepted) {
    auto p = pool(2, /*ringBytes=*/4096);
    WorkerOp filler; filler.type = WorkerOp::Type::SessionClose; filler.sessionId = 1;
    while (p->worker(0).tryEnqueueOp(filler)) {}
    SessionAnnouncer a;
    a.close(*p, 5);
    EXPECT_EQ(a.pendingClosesForTest(), 1u);
    p->worker(0).doWork(1);
    EXPECT_EQ(a.retryCloses(*p), 1);
    EXPECT_EQ(a.pendingClosesForTest(), 0u);
}
```

`libs/node/tests/node/ShardWorkerAckTest.cpp`:

```cpp
// ShardWorkerAckTest.cpp — the worker acks the client directly on its own session
// publication (REAL invoker driver, aeron:ipc), notifies the runtime after the ack,
// and fails parked acks=all promptly on demotion.
#include <rune/node/EmbeddedDriver.h>
#include <rune/node/Worker.h>
#include <rune/node/WorkerIngress.h>
#include <rune/node/WorkerOp.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/protocol/client/DataMessages.h>
#include <rune/protocol/client/Protocol.h>
#include <rune/replication/PartitionReplica.h>
#include <rune/storage/Log.h>

#include <Aeron.h>
#include <FragmentAssembler.h>
#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <map>
#include <unistd.h>

using namespace rune::node;
using rune::replication::PartitionConfig;
using rune::replication::PartitionReplica;
using rune::replication::Role;
namespace fs = std::filesystem;
namespace pc = rune::protocol::client;

namespace {
constexpr int kCap = 200'000;
constexpr int32_t kIngress = 7301, kEgress = 7302;
class NullTransport final : public rune::replication::PartitionTransport {
public:
    void sendFetchRequest(const rune::replication::FetchRequestMsg&) override {}
    void sendFetchResponse(rune::replication::NodeId, const rune::replication::FetchResponseMsg&) override {}
};

struct Fixture {
    fs::path dir, adir;
    std::unique_ptr<rune::storage::Log> log;
    NullTransport t;
    std::unique_ptr<PartitionReplica> replica;
    std::unique_ptr<EmbeddedDriver> drv;
    std::shared_ptr<aeron::Aeron> client;
    std::unique_ptr<WorkerIngress> ing;
    // unique_ptr: the worker (its session publications) must die BEFORE the Aeron client.
    std::unique_ptr<Worker> worker = std::make_unique<Worker>(0, 1 << 16, 1 << 16, 64, 64);
    std::shared_ptr<aeron::ExclusivePublication> producePub;
    std::shared_ptr<aeron::Subscription> egress;
    std::map<int64_t, pc::ProduceResponse> acks;
    aeron::FragmentAssembler asmb{[this](aeron::concurrent::AtomicBuffer& b, aeron::util::index_t o,
                                         aeron::util::index_t l, aeron::Header&) {
        if (auto r = pc::ProduceResponse::decode(reinterpret_cast<const uint8_t*>(b.buffer() + o),
                                                 static_cast<std::size_t>(l)))
            acks[r->correlationId] = *r;
    }};
    int64_t now = 1'000;

    explicit Fixture(std::vector<rune::replication::NodeId> replicas, int minIsr) {
        static std::atomic<int> seq{0};
        const std::string tag = std::to_string(::getpid()) + "_" + std::to_string(seq++);
        dir  = fs::temp_directory_path() / ("rune_swa_log_" + tag);
        adir = fs::temp_directory_path() / ("rune_swa_aeron_" + tag);
        fs::create_directories(dir);
        log = std::make_unique<rune::storage::Log>(dir.string(), 64LL * 1024 * 1024);
        log->recover();
        PartitionConfig c; c.topicId = 1; c.selfId = 1; c.minIsr = minIsr;
        c.fetchIntervalNs = 1'000'000'000; c.isrCheckIntervalNs = 1'000'000'000'000LL;
        c.replicaLagMaxMs = 600'000; c.pushOnAppend = false;
        replica = std::make_unique<PartitionReplica>(c, Role::Leader, 1, replicas, replicas, log.get(), &t);
        EmbeddedDriverConfig dc; dc.dir = adir.string(); dc.termBufferBytes = 64 * 1024;
        drv = std::make_unique<EmbeddedDriver>(dc, DriverMode::Invoker);
        aeron::Context ctx; ctx.aeronDir(adir.string()); ctx.useConductorAgentInvoker(true);
        client = aeron::Aeron::connect(ctx);
        ing = std::make_unique<WorkerIngress>(client, "aeron:ipc", kIngress, drv->pumpWith(client.get()));
        worker->setOwnedDirect(Worker::OwnedSet{replica.get()});
        worker->setWorkerIngress(ing.get(), true);
        worker->setShardMode(true);
        worker->setShardClient(client.get());
        const int64_t sreg = client->addSubscription("aeron:ipc", kEgress);
        const int64_t preg = client->addExclusivePublication("aeron:ipc", kIngress);
        for (int i = 0; i < kCap && !(egress && producePub && producePub->isConnected()); ++i) {
            pump();
            if (!egress) egress = client->findSubscription(sreg);
            if (!producePub) producePub = client->findExclusivePublication(preg);
        }
        WorkerOp a; a.type = WorkerOp::Type::SessionAnnounce; a.sessionId = 5;
        a.egressStreamId = kEgress; a.egressChannel = "aeron:ipc";
        EXPECT_TRUE(worker->tryEnqueueOp(std::move(a)));
    }
    ~Fixture() {
        worker.reset();
        producePub.reset(); egress.reset(); ing.reset(); client.reset(); drv.reset();
        std::error_code ec; fs::remove_all(dir, ec); fs::remove_all(adir, ec);
    }
    void pump() { drv->doWork(); client->conductorAgentInvoker().invoke(); }
    void step() { pump(); worker->doWork(now += 10'000); if (egress) egress->poll(asmb.handler(), 64); }
    void produce(int64_t corr, uint8_t acksAll, int64_t pid = -1, int32_t seq = -1) {
        rune::protocol::RecordBatchBuilder b(0, 1000); b.append("k", "v", 1000);
        const auto batch = b.build();
        pc::Produce m; m.sessionId = 5; m.correlationId = corr; m.topicId = 1; m.partition = 0;
        m.partitionKnown = 1; m.acks = acksAll; m.producerId = pid; m.producerEpoch = 0; m.baseSequence = seq;
        m.recordBatch = std::span<const uint8_t>(batch);
        std::vector<uint8_t> f(m.encodedSize()); m.encode(f.data());
        aeron::concurrent::AtomicBuffer ab(f.data(), static_cast<aeron::util::index_t>(f.size()));
        for (int i = 0; i < kCap && producePub->offer(ab, 0, static_cast<aeron::util::index_t>(f.size())) <= 0; ++i) pump();
    }
    bool awaitAck(int64_t corr) {
        for (int i = 0; i < kCap && !acks.count(corr); ++i) step();
        return acks.count(corr) == 1;
    }
};
} // namespace

TEST(ShardWorkerAck, AcksDirectlyOnTheSessionPublicationNotTheReturnRing) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/42, /*acks=*/0);
    ASSERT_TRUE(f.awaitAck(42));
    EXPECT_EQ(f.acks[42].errorCode, 0);
    int produceResults = 0;
    f.worker->drainReturn([&](WorkerReturn r) { if (r.type == WorkerReturn::Type::ProduceResult) ++produceResults; });
    EXPECT_EQ(produceResults, 0) << "shard mode: the runtime never relays produce acks";
}

TEST(ShardWorkerAckRegression, DemotedLeaderFiresNotLeaderPromptly) {
    Fixture f({1, 2}, /*minIsr=*/2);                          // HW cannot advance without node 2
    f.produce(/*corr=*/7, /*acks=all*/1);
    for (int i = 0; i < 2'000; ++i) f.step();                 // appended + parked
    ASSERT_EQ(f.acks.count(7), 0u);
    f.replica->becomeFollower(/*leaderEpoch=*/2, /*leaderId=*/2);
    ASSERT_TRUE(f.awaitAck(7));
    EXPECT_EQ(f.acks[7].errorCode, static_cast<int16_t>(pc::ErrorCode::NotLeader));
    EXPECT_LT(f.now, 1'000 + Worker::kDefaultAckTimeoutNs) << "must not wait for the 30 s ack timeout";
}

TEST(ShardWorkerAck, ProduceNotifyFollowsTheAckOnTheReturnRing) {
    Fixture f({1}, 1);
    f.produce(/*corr=*/9, /*acks=*/0, /*pid=*/11, /*seq=*/0);
    ASSERT_TRUE(f.awaitAck(9));
    int notifies = 0; int64_t pid = -1;
    f.worker->drainReturn([&](WorkerReturn r) {
        if (r.type == WorkerReturn::Type::ProduceNotify) { ++notifies; pid = r.producerId; }
    });
    EXPECT_EQ(notifies, 1);
    EXPECT_EQ(pid, 11);
}
```

`libs/gateway/tests/gateway/ShardNotifyTest.cpp`:

```cpp
// ShardNotifyTest.cpp — the runtime consumes post-ack notifications (PSM mirror +
// LSO bookkeeping) and never sends a ProduceResponse for them.
#include <rune/gateway/ClientGateway.h>
#include <rune/metadata/ShardRouter.h>
#include <rune/node/WorkerPool.h>

#include <gtest/gtest.h>

using namespace rune::gateway;

TEST(GatewayShardNotify, ProduceNotifyUpdatesPsmMirrorAndSendsNothing) {
    int frames = 0;
    ClientGateway gw = ClientGateway::makeForTest([](const uint8_t*, std::size_t) -> int64_t { return 1; }, 1);
    gw.setOnEgressFrame([&](int64_t, const uint8_t*, std::size_t) { ++frames; });
    rune::node::WorkerPool pool(1, rune::metadata::ShardRouter(1), 1 << 16, 1 << 16, 16, 16);
    gw.setWorkerPool(&pool);
    gw.setShardMode(true);
    rune::node::WorkerReturn n;
    n.type = rune::node::WorkerReturn::Type::ProduceNotify;
    n.topicId = 1; n.partition = 0; n.producerId = 11; n.producerEpoch = 0;
    n.baseSequence = 0; n.recordCount = 1; n.baseOffset = 0;
    ASSERT_TRUE(pool.worker(0).injectReturnForTest(n));
    gw.drainWorkerReturnsForTest();
    EXPECT_EQ(frames, 0) << "the ack was already sent by the worker";
    EXPECT_TRUE(gw.producerStateManagerForTest().validate(1, 11, 0, 0, 0, 0).isDuplicate())
        << "PSM mirror updated from the notification";
}
```

Append to `ShardModeTest.cpp` (end-to-end over UDP; the ack now comes from the worker's own publication):

```cpp
TEST(ShardMode, WorkerSideAcksEndToEndAndSessionReconnect) {
    TmpDir dir{"wsa"}, cdir{"wsac"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, /*fused=*/true)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid1 = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid1, 0);
    ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0, 0), kIngressStream, sid1, 0, 0, 1, makeBatch(2)));
    ASSERT_TRUE(c.disconnect(gatewayChannel(ports, 0), kClientStream, sid1));
    const int64_t sid2 = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid2, 0);
    ASSERT_NE(sid2, sid1);
    ASSERT_TRUE(c.produceUntilOk(workerIngressChannel(ports, 0, 0), kIngressStream, sid2, 0, 0, 1, makeBatch(2)))
        << "a new session is announced and acked by the worker after the old one closed";
    node.stop();
}
```

CMake: node (no Aeron driver needed) —

```cmake
    add_executable(rune_session_ack_sink_tests tests/node/SessionAckSinkTest.cpp)
    target_link_libraries(rune_session_ack_sink_tests PRIVATE rune_node GTest::gtest_main)
    gtest_discover_tests(rune_session_ack_sink_tests)
    add_executable(rune_session_announcer_tests tests/node/SessionAnnouncerTest.cpp)
    target_link_libraries(rune_session_announcer_tests PRIVATE rune_node rune_metadata GTest::gtest_main)
    gtest_discover_tests(rune_session_announcer_tests)
```

node (inside the `aeron_FOUND` test block next to `rune_shard_agent_tests`):

```cmake
            add_executable(rune_shard_worker_ack_tests tests/node/ShardWorkerAckTest.cpp)
            target_link_libraries(rune_shard_worker_ack_tests PRIVATE rune_node rune_replication aeron::aeron GTest::gtest_main)
            gtest_discover_tests(rune_shard_worker_ack_tests PROPERTIES TIMEOUT 120 ENVIRONMENT "${RUNE_AERON_TSAN_ENV}")
```

gateway (`libs/gateway/CMakeLists.txt`, next to `rune_gateway_metadata_response_tests`):

```cmake
    add_executable(rune_gateway_shard_notify_tests tests/gateway/ShardNotifyTest.cpp)
    target_link_libraries(rune_gateway_shard_notify_tests PRIVATE rune_gateway rune_node spdlog::spdlog GTest::gtest_main)
    gtest_discover_tests(rune_gateway_shard_notify_tests)
```

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_session_ack_sink_tests rune_session_announcer_tests rune_shard_worker_ack_tests rune_gateway_shard_notify_tests`
Expected: FAIL to compile — `rune/node/SessionAckSink.h: No such file`, `'SessionAnnounce' is not a member of 'WorkerOp::Type'`, `no member named 'setShardClient'`, `no member named 'setShardMode' in 'ClientGateway'`.

- [ ] **Step 3: Implement**

`libs/node/include/rune/node/SessionAckSink.h`:

```cpp
#pragma once
// SessionAckSink.h — worker-side produce acks (spec §3.4).
//
// The worker encodes each ProduceResponse itself and claims it on ITS OWN
// exclusive publication to the client session (single writer).  Sessions are
// announced by the runtime over the runtime→worker ring; the publication is
// added on the first flush after the announcement and resolved without blocking.
//
// Rich state (spec §1a.4):
//   per session  PubState = Announced(chan, len, stream) → Resolving(regId, deadlineNs)
//                           → Ready(pub, stalledSinceNs, blockedPass) | Failed(sinceNs)
//   whole sink   Backlog  = Clear | Backlogged(pending, sinceNs)  — G-A6: intake pauses
// Coalescing: all acks due in a pass are claimed before the shard's next driver
// duty cycle, so the sender ships each session's acks together (≤ MTU datagram).
// Zero allocation per pass: fixed arrays, open addressing; std::string only in
// the policy's add() (announcement, cold path).
#include <rune/protocol/client/DataMessages.h>

#include <Aeron.h>
#include <ExclusivePublication.h>
#include <concurrent/logbuffer/BufferClaim.h>

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <variant>

namespace rune::node {

struct AckEntry {
    int64_t sessionId     = 0;
    int64_t correlationId = 0;
    int64_t baseOffset    = 0;
    int32_t partition     = 0;
    int16_t errorCode     = 0;
};

enum class PubFind : uint8_t { Pending, Ready, Error };

template <class PubPolicy>
class SessionAckSinkT {
public:
    using PubHandle = typename PubPolicy::PubHandle;

    static constexpr std::size_t kMaxSessions      = 1024;             // power of two
    static constexpr std::size_t kMaxUnresolved    = 64;
    static constexpr std::size_t kAckCapacity      = 4096;
    static constexpr std::size_t kMaxChannelBytes  = 255;
    static constexpr int64_t     kResolveTimeoutNs = 5'000'000'000LL;
    static constexpr int64_t     kStallDropNs      = 5'000'000'000LL;

    struct PubAnnounced { uint16_t chan = 0; uint16_t chanLen = 0; int32_t streamId = 0; };
    struct PubResolving { int64_t regId = -1; int64_t deadlineNs = 0; };
    struct PubReady     { PubHandle pub{}; int64_t stalledSinceNs = 0; uint64_t blockedPass = 0; };
    struct PubFailed    { int64_t sinceNs = 0; };
    using PubState = std::variant<PubAnnounced, PubResolving, PubReady, PubFailed>;

    struct Clear {};
    struct Backlogged { uint32_t pending = 0; int64_t sinceNs = 0; };
    using Backlog = std::variant<Clear, Backlogged>;

    explicit SessionAckSinkT(PubPolicy policy) noexcept : policy_(std::move(policy)) {
        for (std::size_t i = 0; i < kMaxUnresolved; ++i) freeChan_[i] = static_cast<uint16_t>(i);
        freeChanCount_ = kMaxUnresolved;
    }

    bool announce(int64_t sessionId, std::string_view channel, int32_t streamId) noexcept {
        if (sessionId <= 0) return false;
        std::size_t idx = find_(sessionId);
        if (idx == kNone) idx = insert_(sessionId);
        if (idx == kNone) return false;                                   // table full
        releaseChan_(states_[idx]);
        if (channel.size() > kMaxChannelBytes || freeChanCount_ == 0 ||
            unresolvedCount_ == kMaxUnresolved) {
            states_[idx] = PubFailed{0};
            return false;
        }
        const uint16_t c = freeChan_[--freeChanCount_];
        std::memcpy(chanPool_[c].data(), channel.data(), channel.size());
        states_[idx] = PubAnnounced{c, static_cast<uint16_t>(channel.size()), streamId};
        unresolved_[unresolvedCount_++] = sessionId;
        return true;
    }

    void close(int64_t sessionId) noexcept {
        const std::size_t idx = find_(sessionId);
        if (idx == kNone) return;
        releaseChan_(states_[idx]);
        erase_(idx);                                   // its queued acks drop at the next flush
    }

    bool enqueue(const AckEntry& a) noexcept {
        if (ackCount_ == kAckCapacity) return false;   // caller's reservation guarantees room
        acks_[ackCount_++] = a;
        return true;
    }

    int flush(int64_t nowNs) noexcept {
        ++pass_;
        int work = resolvePending_(nowNs);
        std::size_t keep = 0;
        for (std::size_t i = 0; i < ackCount_; ++i) {
            const AckEntry a = acks_[i];
            const std::size_t idx = find_(a.sessionId);
            if (idx == kNone) { ++dropped_; continue; }             // closed / never announced
            PubState& st = states_[idx];
            if (auto* r = std::get_if<PubReady>(&st)) {
                if (r->blockedPass != pass_) {                      // per-session order kept
                    if (policy_.claim(r->pub, kAckBytes, [&](uint8_t* dst) { encode_(a, dst); }) > 0) {
                        r->stalledSinceNs = 0;
                        ++work;
                        continue;
                    }
                    r->blockedPass = pass_;
                    if (r->stalledSinceNs == 0) r->stalledSinceNs = nowNs;
                }
                if (nowNs - r->stalledSinceNs >= kStallDropNs) {    // client gone / never connects
                    st = PubFailed{nowNs};
                    ++dropped_;
                    continue;
                }
            } else if (std::holds_alternative<PubFailed>(st)) {
                ++dropped_;
                continue;
            }
            acks_[keep++] = a;                                       // Announced / Resolving / blocked
        }
        ackCount_ = keep;
        if (keep == 0) {
            backlog_ = Clear{};
        } else if (auto* b = std::get_if<Backlogged>(&backlog_)) {
            b->pending = static_cast<uint32_t>(keep);
        } else {
            backlog_ = Backlogged{static_cast<uint32_t>(keep), nowNs};
        }
        return work;
    }

    [[nodiscard]] bool        intakeAllowed() const noexcept { return std::holds_alternative<Clear>(backlog_); }
    [[nodiscard]] std::size_t freeAckSlots() const noexcept { return kAckCapacity - ackCount_; }
    [[nodiscard]] std::size_t pendingForTest() const noexcept { return ackCount_; }
    [[nodiscard]] uint64_t    droppedForTest() const noexcept { return dropped_; }
    [[nodiscard]] const PubState* stateForTest(int64_t sessionId) const noexcept {
        const std::size_t idx = find_(sessionId);
        return idx == kNone ? nullptr : &states_[idx];
    }

private:
    static constexpr std::size_t kNone     = static_cast<std::size_t>(-1);
    static constexpr std::size_t kMask     = kMaxSessions - 1;
    static constexpr int         kBits     = std::countr_zero(kMaxSessions);
    static constexpr std::size_t kAckBytes = rune::protocol::client::ProduceResponse::ENCODED_SIZE;
    static_assert(std::has_single_bit(kMaxSessions), "kMaxSessions must be a power of two");

    static std::size_t hash_(int64_t sid) noexcept {
        return static_cast<std::size_t>((static_cast<uint64_t>(sid) * 0x9E3779B97F4A7C15ULL) >> (64 - kBits));
    }
    std::size_t find_(int64_t sid) const noexcept {
        std::size_t i = hash_(sid);
        for (std::size_t n = 0; n < kMaxSessions; ++n, i = (i + 1) & kMask) {
            if (keys_[i] == 0)   return kNone;
            if (keys_[i] == sid) return i;
        }
        return kNone;
    }
    std::size_t insert_(int64_t sid) noexcept {
        if (count_ == kMaxSessions - 1) return kNone;               // one empty slot ends probes
        std::size_t i = hash_(sid);
        while (keys_[i] != 0) i = (i + 1) & kMask;
        keys_[i] = sid;
        states_[i] = PubFailed{0};
        ++count_;
        return i;
    }
    void erase_(std::size_t i) noexcept {                            // backward-shift deletion
        keys_[i] = 0;
        states_[i] = PubFailed{0};
        --count_;
        std::size_t j = i;
        for (;;) {
            j = (j + 1) & kMask;
            if (keys_[j] == 0) return;
            const std::size_t home = hash_(keys_[j]);
            const bool stays = (i <= j) ? (i < home && home <= j) : (i < home || home <= j);
            if (stays) continue;
            keys_[i] = keys_[j];
            states_[i] = std::move(states_[j]);
            keys_[j] = 0;
            states_[j] = PubFailed{0};
            i = j;
        }
    }
    void releaseChan_(const PubState& st) noexcept {
        if (const auto* a = std::get_if<PubAnnounced>(&st); a && freeChanCount_ < kMaxUnresolved)
            freeChan_[freeChanCount_++] = a->chan;
    }
    int resolvePending_(int64_t nowNs) noexcept {
        int work = 0;
        std::size_t keep = 0;
        for (std::size_t k = 0; k < unresolvedCount_; ++k) {
            const int64_t sid = unresolved_[k];
            const std::size_t idx = find_(sid);
            if (idx == kNone) continue;                              // closed meanwhile
            PubState& st = states_[idx];
            if (const auto* a = std::get_if<PubAnnounced>(&st)) {
                const int64_t reg = policy_.add(std::string_view(chanPool_[a->chan].data(), a->chanLen),
                                                a->streamId);
                releaseChan_(st);
                if (reg < 0) { st = PubFailed{nowNs}; ++work; continue; }
                st = PubResolving{reg, nowNs + kResolveTimeoutNs};
                unresolved_[keep++] = sid;
                ++work;
                continue;
            }
            if (auto* r = std::get_if<PubResolving>(&st)) {
                PubHandle pub{};
                const PubFind f = policy_.find(r->regId, pub);
                if (f == PubFind::Ready) { st = PubReady{std::move(pub), 0, 0}; ++work; continue; }
                if (f == PubFind::Error || nowNs >= r->deadlineNs) { st = PubFailed{nowNs}; ++work; continue; }
                unresolved_[keep++] = sid;
            }
        }
        unresolvedCount_ = keep;
        return work;
    }
    static void encode_(const AckEntry& a, uint8_t* dst) noexcept {
        rune::protocol::client::ProduceResponse r;
        r.sessionId     = a.sessionId;
        r.correlationId = a.correlationId;
        r.errorCode     = a.errorCode;
        r.partition     = a.partition;
        r.baseOffset    = a.baseOffset;
        r.encode(dst);
    }

    PubPolicy                                                    policy_;
    std::array<int64_t, kMaxSessions>                            keys_{};      // 0 = empty
    std::array<PubState, kMaxSessions>                           states_{};
    std::size_t                                                  count_ = 0;
    std::array<int64_t, kMaxUnresolved>                          unresolved_{};
    std::size_t                                                  unresolvedCount_ = 0;
    std::array<std::array<char, kMaxChannelBytes>, kMaxUnresolved> chanPool_{};
    std::array<uint16_t, kMaxUnresolved>                         freeChan_{};
    std::size_t                                                  freeChanCount_ = 0;
    std::array<AckEntry, kAckCapacity>                           acks_{};
    std::size_t                                                  ackCount_ = 0;
    Backlog                                                      backlog_{Clear{}};
    uint64_t                                                     pass_    = 0;
    uint64_t                                                     dropped_ = 0;
};

/// Production policy: the worker's own invoker-mode Aeron client.
struct AeronSessionPubPolicy {
    using PubHandle = std::shared_ptr<aeron::ExclusivePublication>;
    aeron::Aeron* client = nullptr;

    int64_t add(std::string_view channel, int32_t streamId) noexcept {
        try { return client->addExclusivePublication(std::string(channel), streamId); }
        catch (...) { return -1; }
    }
    PubFind find(int64_t regId, PubHandle& out) noexcept {
        try {
            out = client->findExclusivePublication(regId);
            return out ? PubFind::Ready : PubFind::Pending;
        } catch (...) {
            return PubFind::Error;
        }
    }
    template <class Encode>
    int64_t claim(PubHandle& pub, std::size_t len, Encode&& encode) noexcept {
        aeron::concurrent::logbuffer::BufferClaim c;
        const int64_t pos = pub->tryClaim(static_cast<aeron::util::index_t>(len), c);
        if (pos > 0) {
            encode(c.buffer().buffer() + c.offset());               // zero-copy: encode in the claim
            c.commit();
        }
        return pos;
    }
};

using SessionAckSink = SessionAckSinkT<AeronSessionPubPolicy>;

} // namespace rune::node
```

`libs/node/include/rune/node/SessionAnnouncer.h`:

```cpp
#pragma once
// SessionAnnouncer.h — gateway-side announce-before-accept (spec §3.4).  The
// runtime hands every accepted session's reply destination to EVERY worker over
// its runtime→worker ring BEFORE sending ConnectResponse, so no worker can see a
// Produce for a session it does not know.  Resumable per worker:
//   state Announcing(sessionId, remainingMask) — a ring-full worker is retried on
//   the next call; Closing(sessionId, remainingMask) likewise for SessionClose.
// Control plane (connect / disconnect rate), runtime thread only.
#include <rune/node/WorkerOp.h>
#include <rune/node/WorkerPool.h>

#include <cstdint>
#include <string_view>
#include <vector>

namespace rune::node {

class SessionAnnouncer {
public:
    bool announce(WorkerPool& pool, int64_t sessionId, std::string_view channel, int32_t streamId) {
        Announcing* a = findAnnouncing_(sessionId);
        if (!a) { announcing_.push_back({sessionId, allMask_(pool)}); a = &announcing_.back(); }
        for (int32_t w = 0; w < pool.workerCount(); ++w) {
            const uint64_t bit = uint64_t{1} << w;
            if (!(a->remaining & bit)) continue;
            WorkerOp op;
            op.type           = WorkerOp::Type::SessionAnnounce;
            op.sessionId      = sessionId;
            op.egressStreamId = streamId;
            op.egressChannel  = channel;
            if (pool.worker(w).tryEnqueueOp(std::move(op))) a->remaining &= ~bit;
        }
        if (a->remaining != 0) return false;
        eraseAnnouncing_(sessionId);
        return true;
    }

    void close(WorkerPool& pool, int64_t sessionId) {
        eraseAnnouncing_(sessionId);
        closing_.push_back({sessionId, allMask_(pool)});
        (void)retryCloses(pool);
    }

    int retryCloses(WorkerPool& pool) {
        int done = 0;
        for (std::size_t i = 0; i < closing_.size(); ) {
            Closing& c = closing_[i];
            for (int32_t w = 0; w < pool.workerCount(); ++w) {
                const uint64_t bit = uint64_t{1} << w;
                if (!(c.remaining & bit)) continue;
                WorkerOp op;
                op.type      = WorkerOp::Type::SessionClose;
                op.sessionId = c.sessionId;
                if (pool.worker(w).tryEnqueueOp(std::move(op))) c.remaining &= ~bit;
            }
            if (c.remaining == 0) { closing_[i] = closing_.back(); closing_.pop_back(); ++done; }
            else ++i;
        }
        return done;
    }

    [[nodiscard]] std::size_t pendingClosesForTest() const noexcept { return closing_.size(); }

private:
    struct Announcing { int64_t sessionId; uint64_t remaining; };
    struct Closing    { int64_t sessionId; uint64_t remaining; };

    static uint64_t allMask_(WorkerPool& pool) noexcept {
        const int32_t n = pool.workerCount();              // <= 64 (validateShardConfig)
        return n >= 64 ? ~uint64_t{0} : ((uint64_t{1} << n) - 1);
    }
    Announcing* findAnnouncing_(int64_t sid) noexcept {
        for (auto& a : announcing_) if (a.sessionId == sid) return &a;
        return nullptr;
    }
    void eraseAnnouncing_(int64_t sid) noexcept {
        for (std::size_t i = 0; i < announcing_.size(); ++i)
            if (announcing_[i].sessionId == sid) { announcing_[i] = announcing_.back(); announcing_.pop_back(); return; }
    }

    std::vector<Announcing> announcing_;
    std::vector<Closing>    closing_;
};

} // namespace rune::node
```

`WorkerOp.h`: append to `WorkerOp::Type` after `SchemaUpdate,`:

```cpp
        // Self-contained worker shards (spec §3.4) — values 13/14 = SBE OpType.
        SessionAnnounce,          ///< runtime → worker: session reply destination (tail = egress channel)
        SessionClose,             ///< runtime → worker: forget session + its queued acks
```

fields (after `void* egressPub = nullptr;`):

```cpp
    // SessionAnnounce: the client's egress stream + channel.  egressChannel is a
    // view valid only for the synchronous tryEnqueueOp call (copied into the ring).
    int32_t          egressStreamId = 0;
    std::string_view egressChannel;
```

(add `#include <string_view>`); `WorkerReturn::Type` gains `ProduceNotify,   ///< post-ack worker → runtime bookkeeping (SBE PRODUCE_NOTIFY = 2)`.

`Worker.h`:
- `#include <rune/node/SessionAckSink.h>` and `#include <optional>`.
- public:

```cpp
    // Shard mode (spec §3.4): the worker acks clients on its OWN publications,
    // created on its own invoker client.  Set before the runner starts.
    void setShardClient(aeron::Aeron* client) noexcept { ackSink_.emplace(AeronSessionPubPolicy{client}); }
    [[nodiscard]] uint64_t sessionAnnouncesSeenForTest() const noexcept { return sessionAnnouncesSeen_; }
    bool injectReturnForTest(const WorkerReturn& r) { return tryWriteReturn_(r); }
```

- private members:

```cpp
    std::optional<SessionAckSink> ackSink_;       // engaged iff shard mode with worker-side acks
    uint64_t sessionAnnouncesSeen_ = 0;
    int64_t  lastNowNs_ = 0;                      // this pass's clock, for ring-op handlers
    struct NotifyEntry {
        int64_t producerId; int64_t baseOffset; int32_t topicId; int32_t partition;
        int32_t baseSequence; int32_t recordCount; int16_t producerEpoch; uint16_t batchAttrs;
    };
    static constexpr std::size_t kNotifyBacklogCap = 1024;
    std::array<NotifyEntry, kNotifyBacklogCap> notifyBacklog_{};   // FIFO ring (never drops)
    std::size_t notifyHead_ = 0, notifyCount_ = 0;
    struct ProduceFields {
        int64_t sessionId; int64_t correlationId; int32_t topicId; int32_t partition;
        int16_t acks; int64_t producerId; int16_t producerEpoch; int32_t baseSequence;
    };
```

- first statement of `doWork`: `lastNowNs_ = nowNs;`
- `tryEnqueueOp` tail switch: add

```cpp
        case WorkerOp::Type::SessionAnnounce:
            tailKind = sbe::TailKind::Value::EGRESS_CHANNEL;
            tailSrc  = reinterpret_cast<const uint8_t*>(op.egressChannel.data());
            tailLen  = op.egressChannel.size();
            break;
```

and in the encoder chain after `.egressPub(reinterpret_cast<std::uint64_t>(op.egressPub))` add `.egressStreamId(op.egressStreamId)`.
- `executeFromWire_` switch: add

```cpp
        case WorkerOp::Type::SessionAnnounce:
            executeSessionAnnounceWire_(op);
            break;
        case WorkerOp::Type::SessionClose:
            if (ackSink_) ackSink_->close(op.sessionId());
            break;
```

with

```cpp
    void executeSessionAnnounceWire_(sbe::WorkerOp& op) {
        ++sessionAnnouncesSeen_;
        const int64_t sid    = op.sessionId();
        const int32_t stream = op.egressStreamId();
        const std::uint32_t len = op.tailLength();
        const char* p = op.tail();
        if (!ackSink_) return;                     // legacy / non-acking workers ignore it
        if (!ackSink_->announce(sid, std::string_view(p, len), stream)) {
            spdlog::warn("Worker {}: session {} announcement refused (table full or channel too long)",
                         workerId_, sid);
        }
    }
```

- ack emission — new private helpers:

```cpp
    bool tryWriteReturn_(const WorkerReturn& ret) {
        const std::size_t n = sbe::MessageHeader::encodedLength() +
                              sbe::WorkerReturn::computeLength(ret.chunk.size());
        aeron::util::index_t idx;
        try { idx = ret_.tryClaim(sbe::WorkerReturn::sbeTemplateId(), n); }
        catch (const std::exception& ex) {
            spdlog::error("Worker {}: return tryClaim failed: {}", workerId_, ex.what());
            return false;
        }
        if (idx < 0) return false;
        encodeReturnSbe_(ret, idx, n);
        ret_.commit(idx);
        return true;
    }
    bool writeNotify_(const NotifyEntry& n) {
        WorkerReturn r;
        r.type = WorkerReturn::Type::ProduceNotify;
        r.topicId = n.topicId; r.partition = n.partition; r.producerId = n.producerId;
        r.producerEpoch = n.producerEpoch; r.baseSequence = n.baseSequence;
        r.recordCount = n.recordCount; r.baseOffset = n.baseOffset; r.batchAttrs = n.batchAttrs;
        return tryWriteReturn_(r);
    }
    // Post-ack, fire-and-forget (spec §3.4): FIFO backlog when ret_ is full; never dropped.
    void enqueueNotify_(const WorkerReturn& ack) {
        const NotifyEntry n{ack.producerId, ack.baseOffset, ack.topicId, ack.partition,
                            ack.baseSequence, ack.recordCount, ack.producerEpoch, ack.batchAttrs};
        if (notifyCount_ == 0 && writeNotify_(n)) return;
        if (notifyCount_ < kNotifyBacklogCap) {
            notifyBacklog_[(notifyHead_ + notifyCount_) % kNotifyBacklogCap] = n;
            ++notifyCount_;
            return;
        }
        spdlog::error("Worker {}: notify backlog full — intake reservation violated", workerId_);
    }
    int flushNotifyBacklog_() {
        int n = 0;
        while (notifyCount_ > 0 && writeNotify_(notifyBacklog_[notifyHead_])) {
            notifyHead_ = (notifyHead_ + 1) % kNotifyBacklogCap;
            --notifyCount_;
            ++n;
        }
        return n;
    }
    // Every ProduceResult goes through here.  Shard mode (ackSink_ engaged): the
    // worker acks the client directly, then notifies the runtime.  Else: ret_.
    void emitProduceAck_(const WorkerReturn& ret) {
        if (!ackSink_) { enqueueReturn_(ret); return; }
        if (!ackSink_->enqueue(AckEntry{ret.sessionId, ret.correlationId,
                                        ret.errorCode == 0 ? ret.baseOffset : 0,
                                        ret.partition, ret.errorCode})) {
            spdlog::error("Worker {}: ack sink full — intake reservation violated (session {} corr {})",
                          workerId_, ret.sessionId, ret.correlationId);
        }
        if (ret.errorCode == 0 && ret.producerId >= 0) enqueueNotify_(ret);
    }
```

  Then replace `enqueueReturn_(` with `emitProduceAck_(` at every **ProduceResult** site: in `handleWorkerIngressFrame_`/`produceCore_`, `flushOwedAcksAll_`, `onFlushCompletion_` (both), `submitCoalescedFlushes_`, and `executeProduceWire_`. `executePushReadWire_` keeps `enqueueReturn_` (PushReadResult). Check: `grep -n 'enqueueReturn_(' libs/node/include/rune/node/Worker.h` — the only remaining callers are inside `executePushReadWire_`, `emitProduceAck_` and the definition.

- split `handleWorkerIngressFrame_` into decode + `produceCore_`: keep the decode lines, then

```cpp
        const pc::Produce& m = *msgOpt;
        return produceCore_(ProduceFields{m.sessionId, m.correlationId, m.topicId, m.partition,
                                          static_cast<int16_t>(m.acks), m.producerId,
                                          m.producerEpoch, m.baseSequence},
                            m.recordBatch, nowNs);
    }

    // The ONE produce path (worker ingress, and — in shard mode — gateway-received
    // user produces).  Worker psm_ is authoritative for idempotency (spec §3.4).
    int produceCore_(const ProduceFields& f, std::span<const uint8_t> batch, int64_t nowNs) {
        namespace pc = rune::protocol::client;
```

  and move the remainder of the old body into `produceCore_` with `msg.X` → `f.X`, `msg.recordBatch` → `batch`, `(msg.acks != 0)` → `(f.acks != 0)`.
- `executeProduceWire_(sbe::WorkerOp& op)` — first statements:

```cpp
        if (ackSink_) {   // shard mode: gateway-received produce joins the ONE produce path
            const ProduceFields f{op.sessionId(), op.correlationId(), op.topicId(), op.partition(),
                                  op.acks(), op.producerId(), op.producerEpoch(), op.baseSequence()};
            const std::uint32_t tl = op.tailLength();
            const char* tp = op.tail();
            (void)produceCore_(f, std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(tp), tl),
                               lastNowNs_);
            return;
        }
```

- `pollWorkerIngress_` reservation:

```cpp
        int64_t available;
        if (ackSink_) {
            // Every frame polled now, every parked acks=all and every pending Strict
            // ack must have an ack slot AND a notify slot (G-A6: never drop).
            const int64_t owed = static_cast<int64_t>(pendingAcksAll_.size() + durablePending_.size());
            const int64_t ackFree    = static_cast<int64_t>(ackSink_->freeAckSlots()) - owed;
            const int64_t notifyFree = static_cast<int64_t>(kNotifyBacklogCap - notifyCount_) - owed;
            available = std::max<int64_t>(0, std::min(ackFree, notifyFree));
        } else {
            const int64_t freeSlots = ret_.freeSlotsForMsgSize(ackSlotBytes_);
            const int64_t reserved  = static_cast<int64_t>(retOverflow_.size()) +
                                      static_cast<int64_t>(pendingAcksAll_.size());
            available = (freeSlots > reserved) ? (freeSlots - reserved) : 0;
        }
        const int32_t K = static_cast<int32_t>(std::min(static_cast<int64_t>(desiredFragmentLimit_), available));
```

- `doWorkShard_`: replace the intake gate with

```cpp
        const bool intakeOk = ackSink_
            ? ackSink_->intakeAllowed()
            : (retOverflow_.empty() &&
               ret_.freeSlotsForMsgSize(ackSlotBytes_) > static_cast<int64_t>(pendingAcksAll_.size()));
        if (intakeOk) work += pollWorkerIngress_(nowNs);
```

  and step 5 with

```cpp
        work += flushOwedAcksAll_(nowNs);
        if (ackSink_) {
            work += ackSink_->flush(nowNs);        // claim every due ack on its session pub
            work += flushNotifyBacklog_();         // post-ack runtime notifications
        }
```

- `flushOwedAcksAll_` (Review Focus 1): after `const bool timedOut = ...;` add

```cpp
            // Shard mode: an acks=all parked on a replica that is no longer leader can
            // never commit THIS append — fail fast, retriable (never a false OK, never
            // the 30 s Internal timeout).
            const bool demoted = ackSink_ && entry.replica &&
                                 entry.replica->role() != rune::replication::Role::Leader;
```

  change the condition to `if (hwPassed || timedOut || demoted) {` and the error selection to

```cpp
                if (demoted) {
                    entry.ret.errorCode = static_cast<int16_t>(rune::protocol::client::ErrorCode::NotLeader);
                } else if (timedOut && !hwPassed) {
                    // (existing Internal-timeout branch unchanged)
```

`ClientGateway.h`: `#include <rune/node/SessionAnnouncer.h>`; public

```cpp
    // Shard mode (spec §3.4): announce-before-accept, worker-side acks, worker PSM authority.
    void setShardMode(bool on) noexcept { shardMode_ = on; }
    void drainWorkerReturnsForTest() { drainWorkerReturns_(); }
    rune::storage::ProducerStateManager& producerStateManagerForTest() noexcept { return producerStateManager_; }
```

private `bool shardMode_ = false; rune::node::SessionAnnouncer sessionAnnouncer_;`.

`ClientGateway.cpp`:
- `doWork`, after `pollPendingConnects(nowNs);`: `if (shardMode_ && workerPool_) (void)sessionAnnouncer_.retryCloses(*workerPool_);`
- `pollPendingConnects`: in the timeout branch (`if (nowNs - pending.requestedAtNs > connectResolveTimeoutNs_) {`), before the erase: `if (shardMode_ && workerPool_) sessionAnnouncer_.close(*workerPool_, pending.sessionId);`; and at the top of the non-redirect `else {` branch that builds `pc::ConnectResponse`:

```cpp
            // Spec §3.4 announce-before-accept: every worker must know this session's
            // reply destination before the client can learn its session id.
            if (shardMode_ && workerPool_ &&
                !sessionAnnouncer_.announce(*workerPool_, pending.sessionId,
                                            pending.egressChannel, pending.egressStreamId)) {
                ++it;
                continue;   // a worker ring is full — retry next cycle (bounded by the connect timeout)
            }
```
- `handleDisconnect` and `reapExpired`: after each `sessions_.remove(sid);` add `if (shardMode_ && workerPool_) sessionAnnouncer_.close(*workerPool_, sid);`
- produce path PSM gate (~1646): change `if (msg.producerId >= 0) {` (the "TX-2: Idempotent-producer dedup gate (worker path)") to

```cpp
        // Shard mode: the owning worker's psm_ is the single owner for user
        // partitions (spec §3.4) — the gateway mirror is not consulted.
        if (msg.producerId >= 0 && !(shardMode_ && msg.topicId >= 0)) {
```
- `drainWorkerReturns_`: first branch inside the lambda:

```cpp
        if (ret.type == rune::node::WorkerReturn::Type::ProduceNotify) {
            // Post-ack (spec §3.4): the worker already acked the client.  Mirror the
            // authoritative worker PSM and feed transaction LSO tracking.  No egress.
            if (ret.producerId >= 0) {
                producerStateManager_.update(ret.topicId, ret.producerId, ret.partition,
                                             ret.producerEpoch, ret.baseSequence,
                                             ret.baseSequence + ret.recordCount - 1, ret.baseOffset);
                constexpr uint16_t kTransactionalBit = 0x10u;
                if (ret.batchAttrs & kTransactionalBit) {
                    getTxnState(ret.topicId, ret.partition)
                        .lsoTracker.onProduceBatch(ret.producerId, ret.baseOffset);
                }
            }
            return;
        }
```

`BrokerNode.cpp` shard branch of `buildWorkerPool`: after `wk.setShardMode(true);` add `wk.setShardClient(shardClients_[idx].get());`; in the `if (gateway_)` block add `gateway_->setShardMode(true);`.

- [ ] **Step 4: Run to verify they pass**

Run:
```bash
cmake --build build-fast
./build-fast/libs/node/rune_session_ack_sink_tests
./build-fast/libs/node/rune_session_announcer_tests
./build-fast/libs/node/rune_shard_worker_ack_tests
./build-fast/libs/gateway/rune_gateway_shard_notify_tests
./build-fast/libs/node/rune_shard_mode_tests
./build-fast/libs/node/rune_shard_nonblocking_tests
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast --output-on-failure -j2
```
Expected: PASS (legacy suites — `WorkerPerWorkerIngressE2E`, `GatewayAcksAllDeferredAck`, idempotent/txn suites — unchanged because `ackSink_` is never engaged outside shard mode).

- [ ] **Step 5: Commit**

```bash
git add libs/node/include/rune/node/SessionAckSink.h libs/node/include/rune/node/SessionAnnouncer.h \
        libs/node/include/rune/node/WorkerOp.h libs/node/include/rune/node/Worker.h \
        libs/gateway/include/rune/gateway/ClientGateway.h libs/gateway/src/ClientGateway.cpp \
        libs/node/src/BrokerNode.cpp libs/node/CMakeLists.txt libs/gateway/CMakeLists.txt \
        libs/node/tests/node/SessionAckSinkTest.cpp libs/node/tests/node/SessionAnnouncerTest.cpp \
        libs/node/tests/node/ShardWorkerAckTest.cpp libs/gateway/tests/gateway/ShardNotifyTest.cpp \
        libs/node/tests/integration/ShardModeTest.cpp
git commit -m "feat(node): worker-side produce acks — announce-before-accept, per-session pubs, G-A6 backlog

Worker claims ProduceResponses on its own publications (coalesced per pass), then
notifies the runtime post-ack (PSM mirror + LSO); worker psm_ is the single owner;
demoted leaders fail parked acks=all with NotLeader.

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 14: Metadata advertises the leader worker's `ingressEndpoint` in shard mode

Spec §3.3. The gateway of any node answers Metadata; in shard mode each partition entry carries `ingressEndpoint = "<leaderPeer.host>:<leaderPeer.ingressBasePort + ownerWorker>"`, taken from explicit `RUNE_PEERS` configuration (never derived). The owner worker index is the same on every node (symmetric worker count, #4). Leaders without an advertised base port get an empty endpoint (client falls back to the node endpoint). Legacy mode never writes the trailer (Task 2), so default frames are byte-identical.

**Files:**
- Modify: `libs/gateway/include/rune/gateway/ClientGateway.h` (setter + member), `libs/gateway/src/ClientGateway.cpp:4141-4232` (`handleMetadata`)
- Modify: `libs/node/src/BrokerNode.cpp` (shard branch of `buildWorkerPool`)
- Test: `libs/gateway/tests/gateway/MetadataResponseTest.cpp` (append)

**Interfaces:**
- Consumes: Task 2 `PartitionEntry::ingressEndpoint`; Task 6 `PeerConfig::ingressBasePort`.
- Produces: `struct rune::gateway::ShardIngressEndpoint { int32_t nodeId; std::string host; int32_t basePort; };` `void ClientGateway::setShardIngressEndpoints(std::vector<ShardIngressEndpoint>) noexcept;`

- [ ] **Step 1: Write the failing tests** (append inside the anonymous namespace of `MetadataResponseTest.cpp`)

```cpp
// ===========================================================================
// Self-contained worker shards: per-partition ingressEndpoint (spec §3.3).
// ===========================================================================
TEST(MetadataResponseIngressEndpoint, ShardModeAdvertisesLeaderWorkerEndpoint)
{
    constexpr int32_t kWorkers = 2;
    const std::vector<std::pair<int32_t,int32_t>> kParts = {{10, 0}, {10, 1}, {10, 2}, {20, 0}};
    md::ShardRouter router(kWorkers);
    auto pool = std::make_unique<rune::node::WorkerPool>(kWorkers, router, 4096, 4096, 16, 16);
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, 1);
    gw.setRoutingImageSnapshot(makeImage(kParts, /*leader=*/1));
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    gw.setPerWorkerIngressActive(true);
    gw.setShardIngressEndpoints({{0, "10.0.0.1", 40500}, {1, "10.0.0.2", 40520}});
    std::optional<pc::MetadataResponse::Decoded> captured;
    std::vector<uint8_t> frameCopy;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) {
        frameCopy.assign(buf, buf + len);
        captured = pc::MetadataResponse::decode(frameCopy.data(), frameCopy.size());
    });
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());
    ASSERT_TRUE(captured.has_value());
    for (const auto& [tid, pid] : kParts) {
        auto e = findEntry(*captured, tid, pid);
        ASSERT_TRUE(e.has_value());
        EXPECT_EQ(std::string(e->ingressEndpoint),
                  "10.0.0.2:" + std::to_string(40520 + router.workerForPartition(tid, pid)))
            << "topic=" << tid << " part=" << pid;
    }
}

TEST(MetadataResponseIngressEndpoint, LegacyModeAdvertisesNoEndpoint)
{
    md::ShardRouter router(1);
    auto pool = std::make_unique<rune::node::WorkerPool>(1, router, 4096, 4096, 16, 16);
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, 1);
    gw.setRoutingImageSnapshot(makeImage({{10, 0}}));
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    std::vector<uint8_t> frameCopy;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) { frameCopy.assign(buf, buf + len); });
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());
    auto dec = pc::MetadataResponse::decode(frameCopy.data(), frameCopy.size());
    ASSERT_TRUE(dec.has_value());
    EXPECT_TRUE(findEntry(*dec, 10, 0)->ingressEndpoint.empty());
    EXPECT_EQ(frameCopy.size(), rpcLegacySize(dec->brokers, dec->partitions.size()))
        << "default config frames are byte-identical (no trailer)";
}

TEST(MetadataResponseIngressEndpointRegression, LeaderWithoutAdvertisedBaseGetsEmptyEndpoint)
{
    md::ShardRouter router(1);
    auto pool = std::make_unique<rune::node::WorkerPool>(1, router, 4096, 4096, 16, 16);
    ClientGateway gw = ClientGateway::makeForTest(acceptAllEgress, 1);
    gw.setRoutingImageSnapshot(makeImage({{10, 0}}, /*leader=*/2));
    gw.setCommittedMetadataReady(true);
    gw.setWorkerPool(pool.get());
    gw.setShardIngressEndpoints({{0, "10.0.0.1", 40500}});
    std::vector<uint8_t> frameCopy;
    gw.setOnEgressFrame([&](int64_t, const uint8_t* buf, std::size_t len) { frameCopy.assign(buf, buf + len); });
    const int64_t sid = gw.injectTestSession();
    const auto frame = makeMetadataRequestFrame(sid);
    gw.applyMetadataForTest(frame.data(), frame.size());
    auto dec = pc::MetadataResponse::decode(frameCopy.data(), frameCopy.size());
    ASSERT_TRUE(dec.has_value());
    EXPECT_TRUE(findEntry(*dec, 10, 0)->ingressEndpoint.empty()) << "client falls back to the node endpoint";
}
```

with this helper added next to `findEntry`:

```cpp
// Size of a trailer-less (legacy layout) MetadataResponse.
static std::size_t rpcLegacySize(const std::vector<pc::BrokerEntry>& brokers, std::size_t parts) {
    std::size_t sz = pc::MessageHeader::ENCODED_SIZE + 2u;
    for (const auto& b : brokers) sz += 4u + 4u + 2u + b.host.size();
    return sz + 2u + parts * 28u;
}
```

(add `#include <rune/protocol/client/MessageHeader.h>` and `#include <string>`).

- [ ] **Step 2: Run to verify they fail**

Run: `cmake --build ~/Projects/rune-broker/build-fast --target rune_gateway_metadata_response_tests`
Expected: FAIL to compile — `no member named 'setShardIngressEndpoints' in 'rune::gateway::ClientGateway'`.

- [ ] **Step 3: Implement**

`ClientGateway.h` (namespace scope, before the class):

```cpp
/// Shard mode (spec §3.3): a node's explicitly configured per-worker ingress base.
struct ShardIngressEndpoint {
    int32_t     nodeId   = -1;
    std::string host;
    int32_t     basePort = 0;
};
```

public: `void setShardIngressEndpoints(std::vector<ShardIngressEndpoint> eps) noexcept { shardIngressEndpoints_ = std::move(eps); }`; private `std::vector<ShardIngressEndpoint> shardIngressEndpoints_;`.

`ClientGateway.cpp` `handleMetadata`: before `std::vector<pc::PartitionEntry> parts;` add

```cpp
    // Stable storage for the per-partition ingressEndpoint views (deque: element
    // addresses never move).  Control path (Metadata), not the per-record path.
    std::deque<std::string> endpointStore;
```

(`#include <deque>`), and in the partition loop after the `perWorkerEgressActive_` block, before `parts.push_back(p);`:

```cpp
            // Shard mode (spec §3.3): advertise the LEADER's owning-worker ingress
            // endpoint from explicit configuration; empty → node endpoint.
            if (!shardIngressEndpoints_.empty() && workerPool_) {
                for (const auto& ep : shardIngressEndpoints_) {
                    if (ep.nodeId != entry.leaderNodeId) continue;
                    endpointStore.push_back(ep.host + ":" +
                        std::to_string(ep.basePort + workerPool_->ownerWorkerId(topicId, partition)));
                    p.ingressEndpoint = endpointStore.back();
                    break;
                }
            }
```

`BrokerNode.cpp` (shard branch of `buildWorkerPool`, inside `if (gateway_)`):

```cpp
            std::vector<rune::gateway::ShardIngressEndpoint> eps;
            for (const auto& peer : config_.peers)
                eps.push_back({peer.id, peer.host, static_cast<int32_t>(peer.ingressBasePort)});
            gateway_->setShardIngressEndpoints(std::move(eps));
```

Append to `ShardModeTest.cpp`:

```cpp
TEST(ShardMode, MetadataAdvertisesTheLeaderWorkerEndpoint) {
    TmpDir dir{"md"}, cdir{"mdc"};
    const auto ports = ShardPorts::discover(1, 1);
    BrokerNode node{makeShardConfig(0, dir.path, ports, 1, 1, false)};
    node.start();
    ASSERT_TRUE(node.awaitLeader(10'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid = c.connect(gatewayChannel(ports, 0), kClientStream);
    ASSERT_GT(sid, 0);
    std::string ep;
    for (int i = 0; i < 200 && ep.empty(); ++i) {
        for (const auto& pi : c.metadata(gatewayChannel(ports, 0), kClientStream, sid))
            if (pi.topicId == 0 && pi.partition == 0) ep = pi.ingressEndpoint;
        if (ep.empty()) std::this_thread::sleep_for(50ms);
    }
    EXPECT_EQ(ep, "127.0.0.1:" + std::to_string(ports.ib[0]));
    ASSERT_TRUE(c.produceUntilOk("aeron:udp?endpoint=" + ep, kIngressStream, sid, 0, 0, 1, makeBatch(1)))
        << "the advertised endpoint is the live worker ingress";
    node.stop();
}
```

- [ ] **Step 4: Run to verify they pass**

Run:
```bash
cmake --build build-fast
./build-fast/libs/gateway/rune_gateway_metadata_response_tests
./build-fast/libs/gateway/rune_metadata_request_tests
./build-fast/libs/node/rune_shard_mode_tests --gtest_filter='ShardMode.Metadata*'
ctest --test-dir build-fast -R Regression --output-on-failure
```
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add libs/gateway/include/rune/gateway/ClientGateway.h libs/gateway/src/ClientGateway.cpp \
        libs/node/src/BrokerNode.cpp libs/gateway/tests/gateway/MetadataResponseTest.cpp \
        libs/node/tests/integration/ShardModeTest.cpp
git commit -m "feat(gateway): advertise per-partition ingressEndpoint (leader worker host:port) in shard mode

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 15: Harness — shard ranges in `cluster.sh`, shard A/B + idle-box gate in `replbench.sh`, DR compose overlay

Spec §3.3 harness bullet, §5, §6.3. `RUNE_SHARD=1` makes `cluster.sh` give node `i` ingress base `40500+20i`, replication base `40600+20i`, advertise both in `RUNE_PEERS`, set `RUNE_WORKER_MODE=shard`, `RUNE_RUNTIME_PLACEMENT=${RUNE_RUNTIME_PLACEMENT:-fused}` and pin broker `i` to core `i+1` (client on core 0, spec §5). `replbench.sh` refuses to measure on a busy box and prints the core layout + per-thread CPU split. The DR harness appends compose overlays listed in `RUNE_DR_COMPOSE_OVERRIDES`.

**Files:**
- Modify: `bench/kafka-compare/cluster.sh:10-12` (`rune_cluster_peers`), `:24-52` (`rune_start_cluster` env)
- Modify: `bench/kafka-compare/replbench.sh:19-45`
- Create: `docker/docker-compose.shard.yml`
- Modify: `dr-e2e/harness/cluster.py:47-72` (`Cluster.__init__`)
- Create: `dr-e2e/tests/test_harness_overrides.py`

**Interfaces:**
- Produces: shell `rune_shard_env <i>` (echoes the extra `KEY=VALUE` assignments for node `i`, empty unless `RUNE_SHARD=1`); env `RUNE_SHARD`, `RUNE_SHARD_CORE0` (default 1), `REPL_ALLOW_BUSY` (default 0); env `RUNE_DR_COMPOSE_OVERRIDES` (os.pathsep-separated compose files).

- [ ] **Step 1: Write the failing checks**

```bash
cd ~/Projects/rune-broker/bench/kafka-compare
bash -c 'set -e; WORK=/tmp; source ./cluster.sh
  [ "$(RUNE_SHARD=1 rune_cluster_peers)" = "0@127.0.0.1:40110:40200:40220:40600:40500,1@127.0.0.1:40111:40201:40221:40620:40520,2@127.0.0.1:40112:40202:40222:40640:40540" ]
  [ "$(RUNE_SHARD=1 RUNE_RUNTIME_PLACEMENT=fused rune_shard_env 2)" = "RUNE_WORKER_MODE=shard RUNE_RUNTIME_PLACEMENT=fused RUNE_WORKER_INGRESS_BASE_PORT=40540 RUNE_WORKER_REPL_BASE_PORT=40640 RUNE_WORKER_CORE_BASE=3" ]
  [ -z "$(rune_shard_env 1)" ]
  [ "$(rune_cluster_peers)" = "0@127.0.0.1:40110:40200:40220,1@127.0.0.1:40111:40201:40221,2@127.0.0.1:40112:40202:40222" ]
  echo HARNESS-OK'
```

`dr-e2e/tests/test_harness_overrides.py`:

```python
"""RUNE_DR_COMPOSE_OVERRIDES appends compose overlays (shard-mode DR matrix)."""
import os

from harness.cluster import Cluster


def test_env_overrides_are_appended_after_explicit_ones(monkeypatch):
    monkeypatch.setenv("RUNE_DR_COMPOSE_OVERRIDES", os.pathsep.join(["/x/a.yml", "/x/b.yml"]))
    c = Cluster(project="t-ovr", override_files=["/x/c.yml"])
    assert c.compose._override_files == ["/x/c.yml", "/x/a.yml", "/x/b.yml"]


def test_no_env_means_no_extra_overrides(monkeypatch):
    monkeypatch.delenv("RUNE_DR_COMPOSE_OVERRIDES", raising=False)
    c = Cluster(project="t-ovr2")
    assert c.compose._override_files == []
```

- [ ] **Step 2: Run to verify they fail**

Run: the bash block above; then `cd ~/Projects/rune-broker/dr-e2e && python3 -m pytest tests/test_harness_overrides.py -q`
Expected: bash exits non-zero (`rune_shard_env: command not found`); pytest FAILS `test_env_overrides_are_appended_after_explicit_ones`.

- [ ] **Step 3: Implement**

`cluster.sh` — replace `rune_cluster_peers()` with:

```bash
# Shard mode (RUNE_SHARD=1, spec §3.3): node i gets explicit, distinct per-worker
# ranges — ingress 40500+20i, replication 40600+20i — advertised in RUNE_PEERS
# (id@host:consensus:client:repl:replBase:ingressBase).  <= 20 workers per node.
rune_cluster_peers() {
  if [ "${RUNE_SHARD:-0}" = "1" ]; then
    echo "0@127.0.0.1:40110:40200:40220:40600:40500,1@127.0.0.1:40111:40201:40221:40620:40520,2@127.0.0.1:40112:40202:40222:40640:40540"
  else
    echo "0@127.0.0.1:40110:40200:40220,1@127.0.0.1:40111:40201:40221,2@127.0.0.1:40112:40202:40222"
  fi
}
# rune_shard_env <i> -> extra env for node i (empty unless RUNE_SHARD=1).  Broker i's
# shard worker 0 (runtime fused) is pinned to core RUNE_SHARD_CORE0+i (client: core 0).
rune_shard_env() {
  [ "${RUNE_SHARD:-0}" = "1" ] || return 0
  echo "RUNE_WORKER_MODE=shard RUNE_RUNTIME_PLACEMENT=${RUNE_RUNTIME_PLACEMENT:-fused} RUNE_WORKER_INGRESS_BASE_PORT=$((40500+20*$1)) RUNE_WORKER_REPL_BASE_PORT=$((40600+20*$1)) RUNE_WORKER_CORE_BASE=$(( ${RUNE_SHARD_CORE0:-1} + $1 ))"
}
```

In `rune_start_cluster`, add `$(rune_shard_env "$i")` to the `env` argument list right after `RUNE_WORKER_EGRESS_CONTROL_BASE_PORT=$egbase \` (unquoted on purpose: it expands to separate `KEY=VALUE` words; the existing `# shellcheck disable=SC2086` covers it), and when `RUNE_SHARD=1` do not wrap the broker in `$(pin "$CORES_BROKER")` (the broker pins its own shard thread):

```bash
    local pinprefix; pinprefix="$(pin "$CORES_BROKER")"
    [ "${RUNE_SHARD:-0}" = "1" ] && pinprefix=""
```

and use `$pinprefix "$RUNE_BROKER_BIN"` in place of `$(pin "$CORES_BROKER") "$RUNE_BROKER_BIN"`.

`replbench.sh` — after `source cluster.sh`:

```bash
# ── verified-idle box (spec §1a.10 / §5): refuse to measure on a busy box ────────
load1="$(cut -d' ' -f1 /proc/loadavg)"
if [ "${REPL_ALLOW_BUSY:-0}" != "1" ] && awk -v l="$load1" 'BEGIN{exit !(l >= 1.0)}'; then
  echo "BUSY-BOX: load1=$load1 >= 1.0 — kill stray CPU processes first (REPL_ALLOW_BUSY=1 overrides)"; exit 2
fi
echo "=== box: load1=$load1 cores=$(nproc) shard=${RUNE_SHARD:-0} placement=${RUNE_RUNTIME_PLACEMENT:-fused} ==="
```

replace `export RUNE_WORKER_IDLE=backoff RUNE_RUNTIME_IDLE=backoff RUNE_FS_LOG=0` with

```bash
export RUNE_FS_LOG=0
if [ "${RUNE_SHARD:-0}" = "1" ]; then
  export RUNE_SHARD RUNE_RUNTIME_PLACEMENT="${RUNE_RUNTIME_PLACEMENT:-fused}"
else
  export RUNE_WORKER_IDLE=backoff RUNE_RUNTIME_IDLE=backoff
fi
```

and just before `rune_stop_cluster` print the per-thread CPU split:

```bash
echo "=== per-thread CPU (broker threads) ==="
for p in $(pgrep -x rune_broker); do ps -L -o pid,tid,psr,pcpu,comm -p "$p"; done
```

`docker/docker-compose.shard.yml`:

```yaml
# Shard-mode overlay for the dockerized DR matrix (Task 15).  Each container is its
# own host, so every node uses the same per-worker base ports; RUNE_PEERS advertises
# them explicitly (id@host:consensus:client:repl:replBase:ingressBase).
# Use: RUNE_DR_COMPOSE_OVERRIDES=$PWD/docker/docker-compose.shard.yml pytest ...
x-shard-env: &shard-env
  RUNE_WORKER_MODE: "shard"
  RUNE_RUNTIME_PLACEMENT: "fused"
  RUNE_WORKER_INGRESS_BASE_PORT: "40500"
  RUNE_WORKER_REPL_BASE_PORT: "40600"
  RUNE_PEERS: "0@rune-1:40111:40200:40222:40600:40500,1@rune-2:40111:40200:40222:40600:40500,2@rune-3:40111:40200:40222:40600:40500"

services:
  rune-1:
    environment: *shard-env
  rune-2:
    environment: *shard-env
  rune-3:
    environment: *shard-env
```

`dr-e2e/harness/cluster.py` — in `Cluster.__init__` replace the `self.compose = Compose(...)` line with:

```python
        # RUNE_DR_COMPOSE_OVERRIDES: extra overlays (e.g. docker/docker-compose.shard.yml)
        # appended after explicit override_files — runs the whole matrix in shard mode.
        extra = [p for p in os.environ.get("RUNE_DR_COMPOSE_OVERRIDES", "").split(os.pathsep) if p]
        self.compose = Compose(COMPOSE_FILE, project,
                               override_files=list(override_files or []) + extra,
                               image_tag=_sha)
```

- [ ] **Step 4: Run to verify they pass**

Run: the bash block from Step 1 → prints `HARNESS-OK`; `cd ~/Projects/rune-broker/dr-e2e && python3 -m pytest tests/test_harness_overrides.py -q` → `2 passed`; `docker compose -f docker/docker-compose.yml -f docker/docker-compose.shard.yml config >/dev/null && echo COMPOSE-OK` (from the repo root) → `COMPOSE-OK`.

- [ ] **Step 5: Commit**

```bash
git add bench/kafka-compare/cluster.sh bench/kafka-compare/replbench.sh docker/docker-compose.shard.yml \
        dr-e2e/harness/cluster.py dr-e2e/tests/test_harness_overrides.py
git commit -m "bench(harness): shard-mode ranges + pinning, idle-box gate, DR shard compose overlay

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 16: Validation ladder — in-process failover (shard on), TSan, real e2e, DR matrix, VM on/off, pass criterion

Spec §6, §7, §1a.7/§1a.10. Nothing is "done" until every rung is green **in the default configuration and flag-on**. Real-client rungs (4-7) require the rune-client-cpp plan's shard/invoker client (it consumes `ingressEndpoint` and provides the open-loop `rune_produce_throughput --rate N` of spec §5); run them once that plan's corresponding tasks are merged. Defaults stay `legacy`/`thread` (spec §7) — no default flip in this plan.

**Files:**
- Create: `libs/node/tests/integration/ShardFailoverTest.cpp`; Modify: `libs/node/CMakeLists.txt` (`rune_shard_failover_tests`)

**Interfaces:**
- Consumes: everything above; `ShardTestClient`, `ShardPorts`/`makeShardConfig` (copy the two helpers from `ShardModeTest.cpp` into an anonymous namespace here — test files are independent TUs).

- [ ] **Step 1: Write the in-process shard failover suite** — `libs/node/tests/integration/ShardFailoverTest.cpp`

```cpp
// ShardFailoverTest.cpp — spec §6.2: in-process failover with shard mode ON
// (fused runtime), real Aeron UDP, raw wire client routed by Metadata ingressEndpoint.
#include "support/ShardTestClient.h"

#include <rune/node/BrokerNode.h>
#include <rune/node/NodeConfig.h>
#include <rune/protocol/RecordBatch.h>
#include <rune/storage/Log.h>

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <filesystem>
#include <memory>
#include <optional>
#include <unistd.h>

using namespace rune::node;
using namespace rune::node::test;
namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace {
struct TmpDir {
    fs::path path;
    explicit TmpDir(const char* tag) {
        static std::atomic<int> seq{0};
        path = fs::temp_directory_path() /
               (std::string("rune_shfo_") + tag + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq++));
        fs::create_directories(path);
    }
    ~TmpDir() { std::error_code ec; fs::remove_all(path, ec); }
};
constexpr int32_t kClientStream = 110, kIngressStream = 111;

struct ShardPorts {
    std::vector<uint16_t> cons, client, repl, ib, rb, eg;
    static ShardPorts discover(int nodes, int workers) {
        ShardPorts p;
        auto blocks = freeUdpPortBlocks(3 * nodes, workers);
        for (int i = 0; i < nodes; ++i) {
            p.cons.push_back(freeUdpPort()); p.client.push_back(freeUdpPort()); p.repl.push_back(freeUdpPort());
            p.ib.push_back(blocks[3 * i]); p.rb.push_back(blocks[3 * i + 1]); p.eg.push_back(blocks[3 * i + 2]);
        }
        return p;
    }
};

NodeConfig makeShardConfig(int nodeId, const fs::path& dataDir, const ShardPorts& p, int nodes) {
    NodeConfig cfg;
    cfg.nodeId = nodeId; cfg.dataDir = dataDir.string(); cfg.aeronDir = (dataDir / "aeron").string();
    cfg.consensusStreamId = 90; cfg.replicationStreamId = 100; cfg.replBaseStreamId = 200;
    cfg.electionTimeoutMs = 150; cfg.electionStatusIntervalMs = 20; cfg.startupCanvassTimeoutMs = 100;
    cfg.replicaLagMaxMs = 1'000;
    cfg.workerCount = 1;
    cfg.workerMode = NodeConfig::WorkerMode::Shard;
    cfg.runtimePlacement = NodeConfig::RuntimePlacement::Fused;
    cfg.perWorkerIngressEnabled = cfg.perWorkerReplication = cfg.perWorkerEgressEnabled = true;
    cfg.workerIngressBasePort = p.ib[nodeId]; cfg.workerReplBasePort = p.rb[nodeId];
    cfg.workerEgressControlHost = "127.0.0.1"; cfg.workerEgressControlBasePort = p.eg[nodeId];
    cfg.workerIngressBaseStreamId = kIngressStream;
    cfg.gatewayMinIsr = 2;
    cfg.aeronTermBufferBytes = 1 << 20;
    for (int i = 0; i < nodes; ++i) {
        PeerConfig pc; pc.id = i; pc.host = "127.0.0.1";
        pc.consensusPort = p.cons[i]; pc.clientPort = p.client[i]; pc.replicationPort = p.repl[i];
        pc.replBasePort = p.rb[i]; pc.ingressBasePort = p.ib[i]; pc.workerCount = 1;
        cfg.peers.push_back(pc);
    }
    cfg.clientEndpoint.channel = "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[nodeId]);
    cfg.clientEndpoint.streamId = kClientStream;
    TopicSpec t; t.name = "st"; t.partitionCount = 1; t.topicId = 0;
    cfg.topics.push_back(t);
    return cfg;
}

std::string gw(const ShardPorts& p, int n) { return "aeron:udp?endpoint=127.0.0.1:" + std::to_string(p.client[n]); }

// Leader + advertised worker endpoint of st/0 as seen through node `via`'s gateway.
std::optional<PartitionInfo> leaderOf(ShardTestClient& c, const ShardPorts& p, int via, int64_t sid) {
    for (int i = 0; i < 400; ++i) {
        for (const auto& pi : c.metadata(gw(p, via), kClientStream, sid))
            if (pi.topicId == 0 && pi.partition == 0 && pi.leaderId >= 0 && !pi.ingressEndpoint.empty())
                return pi;
        std::this_thread::sleep_for(50ms);
    }
    return std::nullopt;
}

// Produce `n` acks=all single-record batches to st/0's CURRENT leader: every record
// re-resolves the leader through Metadata (a dead leader's endpoint is abandoned
// after a few attempts, never retried forever).
int produceCommitted(ShardTestClient& c, const ShardPorts& p, int via, int n) {
    int ok = 0;
    for (int i = 0; i < n; ++i) {
        for (int attempt = 0; attempt < 20; ++attempt) {
            const int64_t sidVia = c.connect(gw(p, via), kClientStream);
            if (sidVia <= 0) continue;
            auto li = leaderOf(c, p, via, sidVia);
            if (!li) continue;
            const int64_t sid = c.connect(gw(p, li->leaderId), kClientStream);
            if (sid <= 0) continue;
            if (c.produceUntilOk("aeron:udp?endpoint=" + li->ingressEndpoint, kIngressStream, sid, 0, 0,
                                 /*acks=all*/1, makeBatch(1), /*maxAttempts=*/5)) {
                ++ok;
                break;
            }
        }
    }
    return ok;
}

int64_t leoAfterStop(const fs::path& d) {
    rune::storage::Log log((d / "topics" / "st" / "0").string(), 64LL * 1024 * 1024);
    log.recover();
    return log.logEndOffset();
}
} // namespace

TEST(ShardFailover, LeaderKillNoCommittedLossRegression) {
    std::array<TmpDir, 3> d{TmpDir{"k0"}, TmpDir{"k1"}, TmpDir{"k2"}};
    TmpDir cdir{"kc"};
    const auto p = ShardPorts::discover(3, 1);
    std::array<std::unique_ptr<BrokerNode>, 3> n;
    for (int i = 0; i < 3; ++i) { n[i] = std::make_unique<BrokerNode>(makeShardConfig(i, d[i].path, p, 3)); n[i]->start(); }
    ASSERT_TRUE(n[0]->awaitLeader(15'000ms));
    ShardTestClient c(cdir.path);
    ASSERT_EQ(produceCommitted(c, p, 0, 10), 10);
    const int64_t sid0 = c.connect(gw(p, 0), kClientStream);
    const auto before = leaderOf(c, p, 0, sid0);
    ASSERT_TRUE(before.has_value());
    const int dead = before->leaderId;
    n[dead]->stop();                                            // kill the partition leader
    const int via = (dead + 1) % 3;
    ASSERT_EQ(produceCommitted(c, p, via, 5), 5) << "survivors elect a new leader and commit";
    const int64_t sidv = c.connect(gw(p, via), kClientStream);
    const auto after = leaderOf(c, p, via, sidv);
    ASSERT_TRUE(after.has_value());
    EXPECT_NE(after->leaderId, dead);
    for (int i = 0; i < 3; ++i) if (i != dead) n[i]->stop();
    for (int i = 0; i < 3; ++i) {
        if (i == dead) continue;
        EXPECT_GE(leoAfterStop(d[i].path), 15) << "node " << i << " lost committed records";
    }
}

TEST(ShardFailover, RollingRestartConvergesWithoutLoss) {
    std::array<TmpDir, 3> d{TmpDir{"r0"}, TmpDir{"r1"}, TmpDir{"r2"}};
    TmpDir cdir{"rc"};
    const auto p = ShardPorts::discover(3, 1);
    std::array<std::unique_ptr<BrokerNode>, 3> n;
    for (int i = 0; i < 3; ++i) { n[i] = std::make_unique<BrokerNode>(makeShardConfig(i, d[i].path, p, 3)); n[i]->start(); }
    ASSERT_TRUE(n[0]->awaitLeader(15'000ms));
    ShardTestClient c(cdir.path);
    int committed = produceCommitted(c, p, 0, 5);
    for (int r = 0; r < 3; ++r) {
        n[r]->stop();
        n[r] = std::make_unique<BrokerNode>(makeShardConfig(r, d[r].path, p, 3));   // same dirs: restart
        n[r]->start();
        committed += produceCommitted(c, p, (r + 1) % 3, 5);
    }
    EXPECT_EQ(committed, 20);
    for (auto& x : n) x->stop();
    int onAll = 0;
    for (auto& x : d) onAll += leoAfterStop(x.path) >= 20 ? 1 : 0;
    EXPECT_GE(onAll, 2) << "every acked record is on a majority after the rolling restart";
}

TEST(ShardFailover, ClientReconnectToNewLeaderKeepsAcking) {
    std::array<TmpDir, 3> d{TmpDir{"c0"}, TmpDir{"c1"}, TmpDir{"c2"}};
    TmpDir cdir{"cc"};
    const auto p = ShardPorts::discover(3, 1);
    std::array<std::unique_ptr<BrokerNode>, 3> n;
    for (int i = 0; i < 3; ++i) { n[i] = std::make_unique<BrokerNode>(makeShardConfig(i, d[i].path, p, 3)); n[i]->start(); }
    ASSERT_TRUE(n[0]->awaitLeader(15'000ms));
    ShardTestClient c(cdir.path);
    const int64_t sid0 = c.connect(gw(p, 0), kClientStream);
    const auto li = leaderOf(c, p, 0, sid0);
    ASSERT_TRUE(li.has_value());
    const int64_t sid = c.connect(gw(p, li->leaderId), kClientStream);
    ASSERT_TRUE(c.disconnect(gw(p, li->leaderId), kClientStream, sid));
    EXPECT_EQ(produceCommitted(c, p, li->leaderId, 3), 3) << "a fresh session is announced and acked";
    for (auto& x : n) x->stop();
}
```

CMake (inside the `if(BUILD_NODE_UDP_TESTS AND aeron_FOUND)` block with `rune_shard_mode_tests`):

```cmake
        add_executable(rune_shard_failover_tests tests/integration/ShardFailoverTest.cpp)
        target_include_directories(rune_shard_failover_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
        target_link_libraries(rune_shard_failover_tests PRIVATE rune_node aeron::aeron GTest::gtest_main)
        gtest_discover_tests(rune_shard_failover_tests PROPERTIES TIMEOUT 600 RESOURCE_LOCK rune_udp_ports)
```

Run: `cmake --build build-fast --target rune_shard_failover_tests && ./build-fast/libs/node/rune_shard_failover_tests`
Expected: PASS. Any failure is a real bug: write the failing repro at the lowest layer that reproduces it (replica/worker unit with injected clock first), mark it `*Regression*`, fix, re-run this suite (spec §1a.7, `feedback_integration_before_e2e`).

- [ ] **Step 2: Rung 1+2 on the Mac — regression suite first, then everything, default and flag-on**

```bash
cd ~/Projects/rune-broker
cmake --build build-fast
ctest --test-dir build-fast -R Regression --output-on-failure
ctest --test-dir build-fast --output-on-failure -j2
ctest --test-dir build-fast -R 'Shard|PerWorkerReplication|GatewayFailover|RebalanceFailover' --output-on-failure
```
Expected: all PASS.

- [ ] **Step 3: Rung 3 — TSan (shared-nothing proof)**

```bash
cmake --build build-tsan --target rune_shard_mode_tests rune_shard_failover_tests rune_shard_nonblocking_tests \
      rune_shard_worker_ack_tests rune_embedded_driver_tests rune_aeron_pump_tests rune_per_worker_replication_integration_tests
ctest --test-dir build-tsan -R 'Shard|EmbeddedDriver|AeronPump|PerWorkerReplication' --output-on-failure
```
Expected: PASS with zero `ThreadSanitizer: data race` reports outside `cmake/tsan-aeron.supp` (every rune-side race is a stop-and-fix).

- [ ] **Step 3b: VM prep — cleanup script + Docker on k3s-node-01 (DR and benchmarks run there)**

Create `bench/kafka-compare/vm-clean.sh`:

```bash
#!/bin/bash
# vm-clean.sh — make k3s-node-01 a verified-idle box before ANY benchmark or DR run.
# Kills by exact process NAME / anchored pattern only, so it can never kill the ssh
# session that invoked it (a `pkill -f replbench` matches `bash -c '... replbench ...'`).
set -u
pkill -9 -x rune_broker 2>/dev/null
pkill -9 -x rune_produce_th 2>/dev/null
pkill -9 -x aeronmd 2>/dev/null
pkill -9 -x yes 2>/dev/null
pkill -9 -f '^bash (replbench|cluster|run-rune|run-kafka)' 2>/dev/null
pkill -9 -f '^timeout [0-9]+ bash replbench' 2>/dev/null
pkill -9 -f 'kafka\.Kafka' 2>/dev/null
if command -v docker >/dev/null 2>&1; then
  docker ps -aq --filter 'name=rune' | xargs -r docker rm -f >/dev/null 2>&1
fi
rm -rf /tmp/frout* /dev/shm/kbench-data 2>/dev/null
for i in $(seq 1 60); do
  awk '{exit !($1 < 1.0)}' /proc/loadavg && break
  sleep 2
done
stray=$(pgrep -c -x 'rune_broker|rune_produce_th|aeronmd' 2>/dev/null || true)
load=$(cut -d' ' -f1 /proc/loadavg)
rootfree=$(df -BG --output=avail / | tail -1 | tr -dc 0-9)
shmfree=$(df -BM --output=avail /dev/shm | tail -1 | tr -dc 0-9)
echo "vm-clean: stray=${stray:-0} load1=$load rootFreeG=$rootfree shmFreeM=$shmfree"
ps -eo pcpu,comm --sort=-pcpu | head -6
[ "${stray:-0}" = 0 ] || { echo "vm-clean: FAIL stray processes"; exit 1; }
awk -v l="$load" 'BEGIN{exit !(l < 1.0)}' || { echo "vm-clean: FAIL busy box"; exit 1; }
[ "$rootfree" -ge 3 ] || { echo "vm-clean: FAIL root disk < 3G free"; exit 1; }
[ "$shmfree" -ge 1024 ] || { echo "vm-clean: FAIL /dev/shm < 1G free"; exit 1; }
echo "vm-clean: OK"
```

Sync and prove it on the VM (it must exit 0 on an idle box, and exit 1 while a broker runs):

```bash
chmod +x bench/kafka-compare/vm-clean.sh
rsync -az bench/kafka-compare/vm-clean.sh root@$RUNE_VM_HOST:/root/rune-e5/rune-broker/bench/kafka-compare/
ssh root@$RUNE_VM_HOST 'bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh; echo rc=$?'
```
Expected: `vm-clean: OK` and `rc=0`.

Install Docker on k3s-node-01 for the DR matrix (the box has none; root disk is ~92 % used, so check space first and keep only SHA-tagged images of the current run):

```bash
ssh root@$RUNE_VM_HOST 'df -BG --output=avail / | tail -1;   DEBIAN_FRONTEND=noninteractive apt-get install -y -q docker.io docker-compose-v2 python3-pip >/tmp/apt-docker.log 2>&1; echo apt=$?;   systemctl enable --now docker; docker info --format "{{.ServerVersion}}"; docker compose version;   pip3 install -q -r /root/rune-e5/rune-broker/dr-e2e/requirements.txt; df -BG --output=avail / | tail -1'
```
Expected: `apt=0`, a Docker server version, a compose version, and ≥ 3 GiB still free. If free space drops below 3 GiB, prune only Rune images older than the current SHA (`docker images 'rune-*' --format '{{.Repository}}:{{.Tag}}' | grep -v "$RUNE_IMAGE_TAG" | xargs -r docker rmi`) — never touch k3s/containerd data.

- [ ] **Step 4: Rung 4 — real e2e with the real client (rune-client-cpp, after its shard/invoker tasks land)**

Runs on k3s-node-01 (Global Constraints). Sync all four repos first, then build SHA-tagged images there:

```bash
TAG="$(cd ~/Projects/rune-broker && git rev-parse --short HEAD)"
for r in rune-broker rune-protocol rune-fr rune-client-cpp; do
  rsync -az --delete --exclude 'build*' --exclude '.git' ~/Projects/$r/ root@$RUNE_VM_HOST:/root/rune-e5/$r/
done
ssh root@$RUNE_VM_HOST "bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh && \
  cd /root/rune-e5/rune-broker && export RUNE_IMAGE_TAG=$TAG && docker compose -f docker/docker-compose.yml build && \
  cd /root/rune-e5/rune-client-cpp && docker build --build-context rune-protocol=../rune-protocol \
     -f docker/Dockerfile.dr -t rune-dr-client:$TAG . && \
  export DR_CLIENT_IMAGE=rune-dr-client:$TAG && cd /root/rune-e5/rune-broker/dr-e2e && \
  python3 -m pytest tests/test_dr1_baseline.py -x -q && \
  bash ../bench/kafka-compare/vm-clean.sh && \
  RUNE_DR_COMPOSE_OVERRIDES=\$PWD/../docker/docker-compose.shard.yml python3 -m pytest tests/test_dr1_baseline.py -x -q"
```
Expected: both PASS (no loss, ordering, single leader); images tagged `:$TAG`, never `:dev`/`:latest`.

- [ ] **Step 5: Rung 5 — dockerized DR matrix, default and shard**

Runs on k3s-node-01, each matrix preceded by `vm-clean.sh`:

```bash
ssh root@$RUNE_VM_HOST "cd /root/rune-e5/rune-broker/dr-e2e && export RUNE_IMAGE_TAG=$TAG DR_CLIENT_IMAGE=rune-dr-client:$TAG && \
  bash ../bench/kafka-compare/vm-clean.sh && python3 -m pytest tests -q && \
  bash ../bench/kafka-compare/vm-clean.sh && \
  RUNE_DR_COMPOSE_OVERRIDES=\$PWD/../docker/docker-compose.shard.yml python3 -m pytest tests -q"
```
Expected: the matrix that is green on the default config is green in shard mode (zero committed loss, time-independent ISR/epoch convergence). Any shard-only failure: reproduce in `ShardFailoverTest` first, fix, add the `*Regression*` test, re-run.

- [ ] **Step 6: Rung 6 — VM: build, real io_uring suites, clean box, interleaved A/B shard off vs on**

```bash
for r in rune-broker rune-protocol rune-fr; do
  rsync -az --delete --exclude 'build*' --exclude '.git' ~/Projects/$r/ root@$RUNE_VM_HOST:/root/rune-e5/$r/
done
ssh root@$RUNE_VM_HOST 'cd /root/rune-e5/rune-broker && cmake --build build-linux -j4 \
  && ctest --test-dir build-linux -R "Regression|Uring|Shard|AsyncDurability" --output-on-failure'
ssh root@$RUNE_VM_HOST 'bash /root/rune-e5/rune-broker/bench/kafka-compare/vm-clean.sh'
```
Expected: suites PASS on Linux (the `UringFlushBackend` path of Task 10 runs for real here); `uptime` load1 < 1 and top ≥ 90 % idle — otherwise find and kill the CPU consumer and re-check (never measure a busy box). Then, same binary, interleaved ABAB n = 15, 1 KB records, 1 worker per node:

```bash
ssh root@$RUNE_VM_HOST 'cd /root/rune-e5/rune-broker/bench/kafka-compare && : > /tmp/abab.txt; \
  for i in $(seq 1 15); do \
    bash vm-clean.sh >/dev/null || exit 1; \
    RUNE_SHARD=0 REPL_WORKERS=1 REPL_VBYTES=1024 REPL_FR=0 bash replbench.sh 4 1000000 8000 | grep -E "RESULT|BUSY" | sed "s/^/A $i /" >> /tmp/abab.txt; \
    bash vm-clean.sh >/dev/null || exit 1; \
    RUNE_SHARD=1 REPL_WORKERS=1 REPL_VBYTES=1024 REPL_FR=0 bash replbench.sh 4 1000000 8000 | grep -E "RESULT|BUSY" | sed "s/^/B $i /" >> /tmp/abab.txt; \
  done; cat /tmp/abab.txt'
```
Expected: 30 `RESULT` lines, no `BUSY-BOX`. Validate before interpreting (spec §1a.10): same binary for both arms (`sha256sum build-linux/rune_broker` once), every run printed a RESULT, and B runs show `shard=1` plus `rune-wrk-0` threads at ~100 % on cores 1-3 in the per-thread CPU split. Report median + IQR per arm and the paired per-iteration delta.

- [ ] **Step 7: Rung 7 — the pass criterion (spec §1), verified-idle box**

Immediately after a passing `vm-clean.sh` on k3s-node-01, with the rune-client-cpp open-loop producer (spec §5, `--rate 100000`, 60 s, every record in an HdrHistogram measured from its scheduled send time), client shard pinned to core 0, brokers `RUNE_SHARD=1 RUNE_RUNTIME_PLACEMENT=fused`, topic `bench:4:tier=majority:durability=all-in-sync`, 1 KB records: **≥ 100,000 rec/s achieved and p99 ≤ 100 µs**, same run. A p99 > 100 µs is a stop-and-fix: arm the flight recorder (`REPL_FR=1`) and localise with the `ShardPass`, `ReplWorker*`, `WorkerDrainIngress`, `ReplHwAdvance`, `ReplAckSendOff` spans and `ShardAgent::slowPassesForTest`-equivalent counters; the next lever if loopback UDP cost dominates is per-peer multiplexing (spec §8), never hardware.

- [ ] **Step 8: Commit the suite (and any regression tests added while climbing the ladder)**

```bash
cd ~/Projects/rune-broker
git add libs/node/tests/integration/ShardFailoverTest.cpp libs/node/CMakeLists.txt bench/kafka-compare/vm-clean.sh
git commit -m "test(node): shard-mode failover suite (leader kill, rolling restart, reconnect) + validation ladder

Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

## Spec coverage map

| Spec section | Task(s) |
|---|---|
| §1 goal + pass criterion | 16 (step 7); constraints header |
| §1a.1–2 shared-nothing / rings only | 7 (single pumping thread), 9 (per-worker ownership), 11 (fused on one thread), 12–13 (ring-only announce/notify), 16 (TSan) |
| §1a.3 Aeron is the bible (invoker, tryClaim, busy-spin only pinned) | 5 (claim/gather), 7 (INVOKER), 8 (pump), 9 (ShardAgent, pinned busy-spin), 13 (claim-encode acks) |
| §1a.4 rich-state state machines | 3 (FetchServe), 13 (PubState, Backlog, Announcing/Closing) |
| §1a.5 zero allocation / no virtual | 3 (flat slots), 4 (de-virtualise), 5 (zero-copy + alloc test), 13 (fixed arrays, no map) |
| §1a.6 benchmark-proven | 16 (ABAB, pass criterion) |
| §1a.7 regression tests, default + flag-on | every task (`*Regression*` first; default suite each Step 4); 16 |
| §1a.8 injected clock / capped loops | 3, 9, 10, 13 tests |
| §1a.9 time-independent liveness | 13 (announce retries, stall drop bounded by state not race), 16 (DR matrix convergence) |
| §1a.10–11 benchmark gate / no hardware | 15 (idle gate), 16 |
| §1a.12 fix on sight | 3 (`servedFetch_` map), 5 (pub map), 11 (blocking `resolvePublication`) |
| §2 motivation | — (no work item) |
| §3.1 worker shard (driver/client/pinned/order/never blocks) | 7, 8, 9, 10 |
| §3.2 runtime placement thread/fused | 11 |
| §3.3 endpoints, validation, refusal, protocol field, harness | 2, 6, 14, 15 |
| §3.4 produce/ack path (worker ack, announce, psm, notify, G-A6) | 12, 13 |
| §3.5 replication fixes (no-dup, unicast, flat tracker) | 1 (experiment removed), 3 |
| §3.5a fix-on-sight (de-virtualise, zero-copy, state machines) | 4, 5, 13 |
| §3.6 invoker correctness (pump until found, one driving thread, no waits) | 7, 8, 11 (fused ret-ring test) |
| §4 rune-client-cpp | separate plan (consumes Task 2 `ingressEndpoint`, Task 13 worker acks) |
| §5 benchmark | 15 (harness), 16 (steps 6–7; open-loop client from the client plan) |
| §6 validation ladder | 16 |
| §7 rollout (defaults off; done work committed first) | 1, 6 (defaults), 16 |
| §8 risks (liveness on fused loop, tmpfs, loopback cost) | 9 (slow-pass alarm, `checkAeronSpace`), 11, 16 |
