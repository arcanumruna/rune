# Replication Throughput Optimization — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or executing-plans. Steps use `- [ ]` checkboxes.

**Goal:** Drive Rune 3-node acks=all replication to ≥Kafka parity (target 200k/s, 200 MB/s @1KB on a 4-core box; Kafka=156k/152MB/s, Rune currently ~25MB/s).

**Architecture:** Tuned Aeron config (done) + 7 data-path optimizations, ordered safe→architectural. Each lever is unit+regression-green and benchmark-validated before the next.

**Tech Stack:** C++23, Aeron (embedded driver), SBE (rune-protocol), rune-fr flight recorder, planck-free broker; rune-client-cpp producer; dr-e2e (dockerized pytest).

**Spec:** `docs/superpowers/specs/2026-09-30-replication-throughput-optimization-design.md`

## Global Constraints
- dr-e2e + full C++ regression suites GREEN after every lever; every fix ships a `*Regression*` test.
- Benchmark-validate each lever on the 3-node VM, interleaved A/B, CLIENT OFF-BOX; cite the code path.
- Shared-nothing / ring-buffers-only / zero hot-path alloc / no hot-path virtual calls.
- acks=all-in-sync durability = ISR page-cache copies; never ack past un-page-cached bytes.
- Aeron: rcv-window ≤ SO_SNDBUF ≤ sysctl rmem/wmem_max; rcv-window ≤ term/2, power-of-two.

---

## Phase 0 — Environment & measurement (prerequisite; nothing is trusted without it)

### Task 0.1: Reproducible build + bench environment
**Files:** `bench/kafka-compare/provision-ccx.sh` (new), `bench/kafka-compare/replbench.sh`, `bench/kafka-compare/replcompare.sh`
- [ ] Script to provision a ccx23 (hcloud) with cloud-init: sysctl (rmem/wmem_max=16M+), openjdk-17, rsync; **local reaper via run_in_background + cloud-init `shutdown -P +N` backstop**; copy prebuilt binaries + build-agent `libstdc++.so.6` → `/root/rune-libs` (LD_LIBRARY_PATH).
- [ ] Bake the CLAUDE.md optimal 4-core broker config as defaults in the harness.
- [ ] **Client OFF-BOX:** run the producer on a SECOND VM (or pin the co-located client to isolated cores) so throughput isn't client-bound and the client doesn't steal broker cores. Add `REPL_CLIENT_HOST` to the harness.
- [ ] Build on build-agent (`build-linux`, real Aeron) — confirm rune_broker links; keep build-fast for non-Aeron unit tests.
- [ ] Verify: 3-node acks=all @1KB baseline reproduced with client off-box (record the true broker-bound baseline — expected > the 25MB/s client-bound number).

**Benchmark gate:** a stable, client-independent baseline recorded (this is the number every lever moves).

---

## Phase 1 — Safe tier (self-contained; local TDD; no wire change)

### Task 1 (#5): Non-blocking append — fallocate + fsync off the append thread
**Files:** `libs/storage/src/LogSegment.cpp`, `libs/storage/include/rune/storage/LogSegment.h`, `libs/storage/src/Log.cpp` (segment create + bg pre-provisioner), `libs/storage/tests/storage/LogSegmentTest.cpp`
**Interfaces:** add `void LogSegment::preallocate(int64_t bytes) noexcept;` (Linux `fallocate(fd_, FALLOC_FL_KEEP_SIZE, 0, bytes)`; macOS `fcntl(F_PREALLOCATE)`). `Log` calls it on the ACTIVE/staging segment with `rollBytes_` — do it in the **background pre-provisioner** so the fallocate cost is off the hot path too.
- [ ] Write failing test: create a segment, `preallocate(rollBytes)`, assert blocks allocated (stat `st_blocks*512 ≥ rollBytes`) and appends still land at offset 0 (O_APPEND + KEEP_SIZE).
- [ ] Implement `preallocate` (platform-guarded) + wire into Log's segment provisioning.
- [ ] Confirm the coalesced follower fsync (`followerApplyDirty_`) uses the IoBackend async path (off the worker append thread), not a synchronous `sync()` on the worker; move it if not.
- [ ] Run `rune_storage_tests` + `rune_replication_tests` (build-fast) green.
- [ ] Commit.

**Benchmark gate:** StorageWrite p99/max tail drops (was 502µs/15.8ms); throughput ≥ baseline, no regression.

### Task 2 (#6): Bigger end-to-end batches (config + confirm)
**Files:** `bench/kafka-compare/*.sh`, client defaults if applicable
- [ ] Sweep linger/batch on the tuned config; confirm effective records/batch and its throughput effect; lock optimal defaults into CLAUDE.md.

**Benchmark gate:** documented optimal batch; throughput improvement quantified.

### Task 3 (#7): Client CPU optimization (latency + throughput) — rune-client-cpp
**Files:** `rune-client-cpp/src/*` (agent send/ack hot path), client FR sites (add `ClientAgentDoWork` span, `ClientDrvSubmit`)
- [ ] Add finer client instrumentation (agent-loop span + submit) to attribute the ~15µs/record across app-gen / SBE-encode / Aeron claim+commit / ack-match.
- [ ] Kill per-record heap alloc / std::function on the send+ack path; reduce copies/encodes.
- [ ] Parallelize the client agent (multi agent-thread / per-partition sharding / multiple connections) so one client saturates the broker without pegging 2 cores.
- [ ] Client unit tests + a client-only micro-benchmark green.

**Benchmark gate:** client CPU/record down (target ≤ Kafka's producer efficiency); single client can drive ≥ broker ceiling.

---

## Phase 2 — Broker CPU (no wire change)

### Task 4 (#2): Zero-copy replication
**Files:** `libs/replication/src/AeronPartitionTransport.cpp` (enqueueOutbound/drainOutbound/onFragment), `libs/replication/src/PartitionReplica.cpp` (onFetchRequest/applyReplicated), replication tests
- [ ] Encode the replication frame DIRECTLY into the Aeron publication claim (kill `FetchResponseMsg::encode`→scratch→`putPayload` double-copy). Requires claim-then-encode on the control/worker send path.
- [ ] Avoid the recv-side `records.assign(...)` copy out of the term buffer where the apply can consume the view in-place (careful: term-buffer view lifetime is the callback only).
- [ ] Replication + failover regression suites green (correctness under epoch changes/truncation unchanged).
- [ ] Commit.

**Benchmark gate:** per-record broker CPU down; MB/s up at 1KB (the payload-invariant wall should lift); no regression; dr-e2e green.

---

## Phase 3 — Architectural / wire-protocol (dr-e2e-gated; biggest; may spawn sub-plans)

### Task 5 (#1): Multiplex replication per-peer (not per-partition) — WIRE CHANGE
**Files:** `rune-protocol` (new/extended Fetch{Request,Response} carrying multiple partitions), `libs/replication/src/AeronPartitionTransport.cpp` (route per-peer, not per-partition), `libs/replication/src/PartitionReplica.cpp` (build/apply multi-partition fetch), consensus/bridge as needed.
> **This is large — brainstorm + its own sub-plan (writing-plans) before coding.** Sketch: one FetchRequest per (follower,leader) listing {partition, fetchOffset, ...}[]; one FetchResponse with per-partition sections; leader assembles across owned partitions; follower applies each section.
- [ ] Sub-plan written + reviewed.
- [ ] SBE schema + generated codec (rune-protocol) committed.
- [ ] Transport per-peer multiplex + replica multi-partition build/apply.
- [ ] Full dr-e2e + regression green (correctness across failover/rolling/ISR).

**Benchmark gate:** message count/round-trips per record drop ~N×; large throughput step toward Kafka.

### Task 6 (#3): Piggyback acks (combined pull) — WIRE CHANGE, composes with #5
**Files:** rune-protocol (fetch carries ack position), PartitionReplica (drop separate fast-ack; ack rides the fetch), transport.
- [ ] Fold the follower ack into the multi-partition fetch (no separate fast-ack message); leader advances HW from the fetch's carried positions.
- [ ] dr-e2e + regression green.

**Benchmark gate:** ~½ the replication messages/commit; throughput up, latency not worse.

### Task 7 (#4): Per-worker replication transport (thread-per-core) — THREADING
**Files:** `libs/node/*` (WorkerPool, per-worker replication sub/pub like WorkerIngress), `libs/replication/*`, `NodeRuntimeAgent` (keep only internal-partition repl on control thread).
> **Large — brainstorm + sub-plan before coding.** Each worker owns Aeron pub/sub for ITS partitions' replication on per-worker streams; drives poll/drainOutbound in its own doWork; control thread keeps internal (topicId<0) replicas only.
- [ ] Sub-plan written + reviewed.
- [ ] Per-worker replication transport + ownership on rebalance/failover.
- [ ] TSan clean (shared-nothing); full dr-e2e + regression green.

**Benchmark gate:** replication I/O parallelizes across cores; breaks the single-control-thread ceiling toward/past 200k.

---

## Self-review checklist
- Every lever: unit+regression green → benchmark-validated (client off-box) → dr-e2e green for Phase 2/3 → commit.
- #1 and #4 get their own brainstorm+sub-plan before coding (wire/threading, correctness-critical).
- Order rationale: measurement first (else numbers lie), then safe self-contained wins, then broker CPU, then the architectural wire/threading changes that need the most care.
