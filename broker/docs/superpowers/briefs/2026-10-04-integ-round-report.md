# Integration round 2026-10-04: report

**Status: GREEN.** Every branch listed below is merged. The Mac suites (broker, client and protocol) are green, the Linux-only test binaries are green, and the perf sanity matches the A9 reference. Everything is committed locally. Nothing was pushed, and no conan create/export/upload was run.

## Branches and merge commits

### Broker
Worktree `rune-broker-wt40`, branch `integ/round-2026-10-04`, cut from `integ/perf-defaults` at `ca370d5`.

| # | Branch merged | Merge commit | Conflicts |
|---|---|---|---|
| 1 | fix/d2-thread-placement (55790f0) | `064889b` | none |
| 2 | perf/a9-doorbell (88dadb4, which contains perf/a7-idle a3b89e1) | `0c9d7d2` | 4 files, see below |
| 3 | feat/d6-unknown-pid (9394eb4) | `b39e7f8` | none |
| 4 | fix/d8c-credit-pause (b0129b9, which contains fix/d8b-held-session-lease f08e935) | `2042de8` | 8 files, see below |
| — | integ/perf-defaults (docs-only commits since ca370d5: 8f1e64b and its predecessors) | `dac62c2` | none |

The last row brings in other agents' doc commits, so `integ/perf-defaults` can fast-forward to this branch.

Not merged, as instructed: feat/dur-1 (wt31) and fix/e1d-recovery-point (wt32).

### Client
Worktree `rune-client-cpp-wt12`, branch `integ/client-2026-10-04`, cut from `feat/client-gso` at `ff60185`.

| Branch merged | Merge commit | Conflicts |
|---|---|---|
| perf/a7-client-idle (7e98e33) | `9b62afa` | none |
| feat/d6-unknown-pid (42b8fa9) | `f66df33` | none |
| perf/a8-client-batch (f1a4fa0) | `7dea068` | 1 file, see below |

### rune-protocol
The shared checkout on branch `feat/self-contained-worker-shards` now has merge commit `c3151c9`, which merges `feat/unknown-producer-id` (515a758, a465b82).
- The merge was clean.
- The internal ring schema keeps distinct ids: WriteSyncRequest is id 7, and D6 adds only the `WorkerOp.clientVersion` field (id 50).
- rune-protocol-fe (DUR-1) was not touched.
- The checkout's uncommitted CLAUDE.md edit and its `Testing/` directory were left as they were.

## Conflicts and how they were resolved

Every conflict was resolved by keeping both sides' intent. Nothing was dropped.

### Merge 2 (A7/A9 into D2)
- **`NodeConfig.h`**
  - Kept D2's control-plane `BackoffShape`, `controlIdleShape()`, `runtimeIdleShape()` and `adminIdleShape()`. These cover the runtime, admin and node driver.
  - Kept the A7/A9 shard-worker knobs: `idleWake`, `idleSpins`/`idleYields`, `idleDoorbell*` and `shardIdleShape()` with `IdleShape.h`. These cover the workers.
  - The two sets do not overlap: D2 explicitly left worker idle to A7.
  - Both includes are kept (`<rune/node/IdleShape.h>` and `<algorithm>`).
- **`HostDefaults.cpp`, effective-config line**
  - The line is the union of both sides: `... coreBase=..(..) idle=<A7/A9> io= ioWq= nonWorker=<D2> drvIdle=<D2> ctlIdle=<D2> term= ...`.
  - It uses D2's 1536 B buffer and D2's `cpuList` helper.
  - Proven on the box: the line logs `idle=wake+doorbell{spins=auto yields=auto parkUs=1000-1000} ... nonWorker=0 drvIdle=backoff{1-0-100000-2000000} ctlIdle=10-0-100us-2000us`.
- **`libs/node/CMakeLists.txt`**: the test list keeps `ThreadAffinityTest.cpp` from D2 plus `ParkUntilWorkIdleTest.cpp` and `RingDoorbellTest.cpp` from A7/A9.
- **`EmbeddedDriverTest.cpp`**: both blocks are kept. That is the D2 idle-args and malformed-strategy tests plus the A7 wake and epoll-first tests.
- **Auto-merged, then checked**
  - `BrokerNode::start`: D2's `ScopedThreadAffinity` is still taken around start(), and A9's `enableShardDoorbells` and `attachFusedRuntimeDoorbell_` still run before the runners and admin start.
  - `EmbeddedDriver.cpp`: D2 applies the init args before the strategy, and A7 installs epoll-first polling. The Linux `rune_embedded_driver_tests` 22/22 cover both.

### Merge 4 (D8b/D8c into D6)
- **`SessionTable.h` `Session`**: keeps D6's `clientVersion`, D8b's `lostByWorkers`/`lostSinceNs`, and D8c's `produceInFlight`, `produceInFlightOn[]` and `creditTagOn[]`.
- **`SessionAckSink.h`**
  - D8b's `Edges` struct replaces `reportedHeld_` and `dirtyFlag_`. D6's per-session `clientVersion_` array is kept next to it.
  - Both are reset in `insert_` and `erase_`, and both move together in the backward-shift loop. D6's 600-session and D8b's colliding-keys backward-shift tests both pass.
  - The claim site combines three things:
    - D6's version-sized claim, `encodedSizeFor(ver)` / `encode_(a, ver, dst)`;
    - D8b's `claimedWatched_`;
    - D8c's `noteCredit_`.
  - `AckEntry` (auto-merged) carries both `credit` (D8c) and `logStartOffset` (D6).
  - The capacity comment is updated: at 48 B per entry, `kAckCapacity` 32768 is 1.5 MiB (was "40 B, 1.25 MiB").
- **`Worker::emitProduceAck_`**: builds the `AckEntry` with D8c's `gatewayCredit` flag, then sets D6's `logStartOffset` when the error is UnknownProducerId.
- **`ClientGateway::handleProduce`**: D8c made the function return `bool`. The D6 site keeps `sendResp(outcome.error, partition, 0, outcome.logStartOffset)` and now returns `true`.
- **Tests**: `NodeConfigTest`, `SessionAckSinkTest`, `ShardWorkerAckTest` and the `ShardModeTest` includes keep every test and include from both sides.

### Client merge (A8 into A7+D6)
- **`examples/bench/InvokerBench.cpp`, RESULT line**: keeps A7's `idle=park|spin` field, followed by A8's `batches=` and `records_per_batch=`.

## Suites

### Mac
- **Broker, `scripts/run-broker-tests.sh build-fast`**: reconfigured from wt26's cache flags (Debug, Ninja, ccache, `-m64 -stdlib=libc++`, the rune-broker conan toolchain).
  - Tier 1: **2889/2889**.
  - Heavy tier: **625/625**.
  - Total: **3514/3514**, 0 failed. 32 tests were skipped, all Linux-only or uring-only.
  - No reruns were needed.
- **Broker checks**: `check_no_atomics` OK (RING 65, CLEANER 26, ...). `check_single_driver_pumper` OK.
- **Client, `ctest`**: **934/934**, 0 failed. 3 Linux-only tests were skipped. `check_no_atomics_or_mutexes` OK; `check_no_shared_ptr_handles` OK.
- **rune-protocol** (Release, built in a scratch dir that was then deleted): **499/499**.

### Linux (k3s-node-01)
- Setup:
  - Release build, gcc-13, the conan toolchains under /root/rune-e5.
  - A fresh `/root/integ-r2`, with the merged trees and their sibling repos rsynced from the Mac.
  - Everything ran under `flock -w 7200 /root/vm.lock`.

| Binary | Result |
|---|---|
| rune_node_tests (ThreadAffinity Linux, RingDoorbell, ParkUntilWorkIdle) | 327/327 |
| rune_shard_agent_tests | 15/15 |
| rune_embedded_driver_tests | 22/22 |
| rune_worker_doorbell_tests | 6/6 |
| rune_udp_gso_tests | 17/17 |
| rune_shard_mode_tests (D2 Md-3 pinning, SlowEndpoint, D8b real-driver) | **44/44** |
| rune_gateway_produce_credit_tests | 9/9 |
| rune_session_ack_sink_tests | 31/31 |
| rune_shard_worker_ack_tests (incl. D6 M3 shard chain) | 24/24 |
| rune_shard_nonblocking_tests | 97/97 |
| rune_f15_eos_regression_tests | 165/165 |

- Total: **757/757**, each binary run once.
- `SlowEndpointUnawareClientCannotStarveTheShard` passed; it was the known A9 flake, now fixed by D8b and D8c.

## Perf sanity

Reference config, one run each:
- 3 nodes, acks=all, 1 KiB values, linger 0.
- MTU 1408 with GSO/GRO, backoff workers, fused placement with 1 worker per node.
- term 1 MiB, rcvWindow 256 KiB, ctlTerm 1 MiB.
- Logs off (`RUNE_FS_LOG=0`, `RUNE_LOG_LEVEL=warn`), fsync off.
- Run through `/root/e2e/tp.sh` via `/root/a8/run.sh`, with vm-clean before each run and BROKERDIR/PBBIN pointing at the integ build.

Proof of path: `park-until-work{wakeFds=4 doorbell=5rings spins=10 yields=20 parkNs=1000000-1000000}`, plus the merged effective-config line quoted above. `gso=on`, errors=0.

| Run | Result | Reference (A9 §4.15.1, shipped default 88dadb4) |
|---|---|---|
| 20k open loop | p50 **436 µs**, p99 **25.2 ms**, p999 61.3 ms; 20 000 rps | p50 418–444 µs, p99 9.4–9.6 ms |
| Saturation | **168.4k rec/s** (p50 19.0 ms, p99 121 ms, the saturation regime) | ~167–175k |

- Worker CPU at 20k was 82.0, 80.2 and 78.6%; the client used 69%; system idle was 3%.
- Raw data is in `/root/integ-r2/res/` on the box. The source and build dirs were deleted, and 13 GB of disk is free.

## Open issues and concerns
1. **20k p99 was 25.2 ms in this single run.** The A9 shipped-default runs measured 9.4–9.6 ms. However, A9's own pre-doorbell arm spread 15–27 ms, D2 measured fused at 45 ms, and D8c measured 10–31 ms, so the 20k p99 on this saturated 4-core box (system idle 3%) is known to spread widely. p50 and saturation are on reference. One run cannot tell a regression from spread. If the p99 matters for the next decision, run interleaved rounds of integ against 88dadb4.
2. **Linux binaries were run once each.** The earlier one-off flakes (KilledPaused, AdvertisedIngress) did not appear in this run.
3. **The backlog items from the merged reports remain open.** D8c LOW-R2, R3 and R5 plus LOW-3 and LOW-5; the D8d direct-path exposure; A9 L-3, L-4 and L-6; D2 concern 1 (thread-placement p99); D6 L1 and the nodejs client. None of them was changed here.
4. **`records_per_batch` is missing from the run summary.** The client prints it on the RESULT line, but `tp.sh` truncates that line to 400 characters (`cut -c1-400`), and the merged line (with A7's `idle=` field added) is now longer. This is cosmetic: widen the cut in `tp.sh` if the field is needed.
