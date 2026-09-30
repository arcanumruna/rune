# Rune Multi-Node Client + Coordinators — Implementation Plan (rev 2, matches spec rev 4)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Cross-repo (rune-client-cpp + rune-broker + rune-protocol). Steps use `- [ ]`.

**Goal:** `producer.send()` / consume / consumer-groups / transactions work transparently on a cluster of any size, at ULL, with no loss/dup/reorder/hang — validated by 3-node produce/consume/DR/group/txn benchmarks.

**Architecture:** spec `docs/superpowers/specs/2026-09-26-rune-client-multinode-routing-design.md` (rev 4). Reuse the existing single `AeronTransport` stream-multiplexing (no new driver); a "node" = one endpoint-keyed session multiplexing role streams; agent-thread single-owner routing (shared-nothing, no atomics); coordinator == consensus leader (v1).

## Global Constraints
- **Shared-nothing + ring buffers; NO atomics/locks; single-owner-per-state.** All routing/metadata/coordinator state agent-thread-owned plain data.
- **ULL:** no per-send heap alloc, no new hot-path branch for the single-node/leader-known path; single-node numbers within noise (gate each phase, not just the end).
- **No loss/dup/reorder/hang:** reuse `retransmitStalled`/`deliveryTimeoutMs`/`sweepTimedOut`; per-partition sequence continuity across leader moves; idempotent + epoch-validated ack/coordinator-response resolution.
- Local main never pushed; branch `multinode` per repo; commits end `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.
- Client tests: CLion cmake, `build/` dir. Broker tests: ctest / VM build. Validate on build-agent-01 (free) before any paid box.

## Review Focus (pin tests to these)
- Single-node no-regression after EACH phase (throughput + p50/p99/p99.9 within noise; cold-start first-ack not warmup-masked).
- Per-partition order + sequence monotonicity across a leader move (no OutOfOrderSequence/dup).
- Idempotent + epoch-validated resolution: late ack from a deposed leader/coordinator rejected, never double-resolves a future.
- Heartbeat/txn-timeout never starved by produce backpressure.
- Multi-leader txn commit reaches ALL participant partitions' markers (B3) — EOS not silently violated.

---

# PHASE 1 — Client multi-session leader routing (unblocks 3-node produce/consume/DR; NO broker change)

## Task 1.1: Endpoint-keyed session registry (replace single SessionMachine/transport)
**Files:** `rune-client-cpp/include/rune/client/agent/ClientAgent.h` (+ .cpp), `src/Client.cpp`. Tests: existing `rune_producer_tests`/`rune_redirect_tests`/`rune_agent_tests` stay green (pool-of-1 = today).
**Interfaces produced:** agent-owned `std::map<std::string /*endpoint*/, Session>` where `Session = {SessionMachine, per-node ingress pub handle(s), egress image, sessionId, redirect state}`; `Session& sessionForEndpoint(endpoint)` (agent-thread, lazy-open async). Pool-of-1 = the seed session (byte-identical path).
- [ ] Baseline: run the 5 client suites green.
- [ ] Extract today's single `SessionMachine`+transport wiring into a `Session` struct; `ClientAgent` holds a map with exactly one entry initially (seed). Rewire `doWork` to iterate the map (one entry → same as today).
- [ ] Move per-session redirect state (`redirectPending_`, `currentLeaderNode_/Channel_`) INTO `Session` (plain bool/scalars — single-owner agent, no atomics). [G5, arch F-A]
- [ ] Run 5 suites green (pure refactor). Commit.

## Task 1.2: Unify hub SendFn to (ingressStreamId, buf, len) + per-session dispatch
**Files:** `SubscriberHub.h`, `GroupConsumerHub.h`, `TransactionalProducerImpl.h`, `Client.cpp` (dispatch), `ClientAgent`. 
- [ ] Change all hub `SendFn`s from `(buf,len)` to `(int32_t ingressStreamId, buf, len)` (match ProducerHub). [G2]
- [ ] Egress dispatch: demux each incoming frame by the **arrival image/session**, not templateId, and resolve the owning `Session` before any redirect/retarget. [G5,G9]
- [ ] `haveWork` gate includes group/txn pending (not just producer/subscriber). [G4]
- [ ] Run suites green. Commit.

## Task 1.3: Metadata-driven partition→(endpoint,streamId) routing table (agent-owned plain vector)
**Files:** `src/Client.cpp` (onMetadataResponse, routeProduceToLeader→replace), coordinator struct.
**Interfaces:** agent-owned `partition→(endpoint, ingressStreamId)` vector rebuilt in place on metadata change; drain routes each record via array index. Single-node = all slots one endpoint.
- [ ] Delete the single-transport `routeProduceToLeader` retarget model; route at the flush→sendFn boundary using the table + `sessionForEndpoint`.
- [ ] Test: partitions with leaders on 2 nodes → each routes to its leader's session (capture via session send hooks). Single-node fast path = same pointer, no map churn. Commit.

## Task 1.4: Per-partition producer state + hold-in-builder backpressure + reuse retransmit
**Files:** `src/producer/ProducerImpl.cpp`/.h.
- [ ] Keep per-partition `batchMap`/`ackRing`/`nextBaseSequence` (already per-partition); ensure batchMap stays ordered-by-corrId for `retransmitStalled` O(1). [arch GAP2]
- [ ] Unknown/electing leader → reuse the `sessionId==0` "hold in builder" at flush (one predictable branch, zero drain-time cost); sequence assigned only on real offer. [ULL, backpressure]
- [ ] Leader move: re-stamp in-flight in place (same corrId, keep order) + reset `attemptDeadlineNs`; resend on new session in sequence order (§F). Reuse `resendAllRedirected` scoped per-session. [backpressure GAP4]
- [ ] Tests: leader move mid-flight → no dup/loss/reorder; unknown-leader → bounded hold → app backpressure, no OOM. Commit.

## Task 1.5: Wire existing InitProducerId into the plain producer (single identity)
**Files:** `ProducerImpl.cpp`/.h (reuse `TransactionalProducerImpl`'s InitProducerId plumbing).
- [ ] Replace the stub pid with a broker-assigned `(pid,epoch)` via existing InitProducerId; bump epoch on restart/reconnect. [completeness #4, in-flight F9]
- [ ] Test: two producers get distinct pids → no cross-producer false-dedup. Commit.

## Task 1.6: PHASE-1 e2e + no-regression on VM
- [ ] 3-node (per-worker ingress) produce 8 partitions acks=all via ONE seed → all acked; consume all. (was 65/5000)
- [ ] `smoke-dr.sh`: kill leader → partitions re-route, produce resumes, zero acked loss.
- [ ] Single-node run-matrix within noise of pre-change; cold-start first-ack measured. Commit.

---

# PHASE 2 — Broker + protocol: FindCoordinator + cross-node txn markers (B1–B4)

## Task 2.1: FindCoordinator RPC (protocol + broker) [B1,B2]
**Files:** `rune-protocol` (new `FindCoordinator{type,key}`/`FindCoordinatorResponse{errorCode,nodeId,host,port}` template), `rune-broker` Group/TransactionCoordinationService handler returning `leaderId()`+`leaderClientEndpoint`, gateway dispatch.
- [ ] Add template; regenerate. Broker handler returns consensus-leader endpoint. Add coordinator endpoint to `NotCoordinator` responses (Join/Sync/Heartbeat/OffsetCommit + txn). Tests. Commit.

## Task 2.2: Cross-node txn marker fan-out [B3 — CRITICAL]
**Files:** `rune-broker` `TransactionCoordinationService::handleEndTxn`/`writeMarkers`, wire the dead `WriteTxnMarkersRequest` (templateId 7) as a real inter-node RPC; `ClientGateway::appendControlRecord` remote path.
- [ ] Coordinator sends `WriteTxnMarkers` to each participant partition's **leader node**; that leader appends the marker locally (acksAll), idempotent + retried, BEFORE `completeTransaction`.
- [ ] Test: txn spanning partitions on 3 nodes → COMMIT marker reaches ALL; read-committed LSO advances on all; ABORT likewise. (Today: silently dropped on remote partitions.) Commit.

## Task 2.3: Verify/fix cross-node group record delivery [B4]
- [ ] Spike: group member assigned a partition led by a non-coordinator node — does it receive records (push-sub is on coordinator gateway)? If not, route group delivery from the partition leader / cross-node fetch. Test. Commit.

---

# PHASE 3 — Client coordinators: groups + transactions

## Task 3.1: Client FindCoordinator + group coordinator routing
**Files:** `src/group/GroupConsumerHub.cpp`/.h, `src/Client.cpp`.
- [ ] Client `FindCoordinator(group)` → coordinator endpoint → route Join/Sync/Heartbeat/OffsetCommit/OffsetFetch to the coordinator `Session`. `NotCoordinator` → re-FindCoordinator + re-route (not rejoin-same-session). [group gaps 1-4]
- [ ] Fix OffsetCommit response error-code handling (today ignored → silent commit loss). [group gap 5]
- [ ] Coordinator-epoch validation on group responses. [G9]
- [ ] Test: 3-node consumer group → join/assign/consume/commit across nodes; coordinator failover → re-discover, no double-consume, no silent commit loss. Commit.

## Task 3.2: Client transaction coordinator + txn produce routing
**Files:** `src/producer/TransactionalProducerImpl.cpp`/.h, `src/Client.cpp`.
- [ ] `FindCoordinator(transaction)` → route InitProducerId/AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/TxnOffsetCommit to txn coordinator session; `NotCoordinator` re-discover. [txn gaps 3,4]
- [ ] Txn Produce routes to partition leaders (reuse Phase-1 routing) + ordered failover resend (§F). [txn gap 5]
- [ ] Single producer identity (Phase 1.5). Coordinator-epoch validation.
- [ ] Test: 3-node txn spanning partitions on 3 nodes → commit visible on all (with B3); abort hides all; leader AND coordinator failover mid-txn → EOS holds. Commit.

## Task 3.3: Mixed-liveness duty cycle
- [ ] Ensure group heartbeat + txn-timeout servicing runs each cycle independent of the bounded produce drain (heartbeat interval « drain bound); one idle strategy over aggregate work; active-set pruning. [G7,G10]
- [ ] Test: busy/back-pressured leader does not starve heartbeats → no spurious rebalance/txn-abort. Commit.

---

# PHASE 4 — Full multi-node benchmark + assessment
- [ ] Extend `run-all.sh`: 3-node produce/consume (D1 quorum), DR/failover, consumer-group, txn cells — all green on VM.
- [ ] Collect numbers for ALL scenarios (single + multi-node) on the VM; then the clean ccx43 run.

## Self-Review
- Spec rev-4 coverage: §A-K → Phase 1; §L/B1-B4 → Phase 2; groups/txn client → Phase 3; validation → Phase 4.
- Phasing lets multi-node produce/consume/DR be assessed after Phase 1 (no broker change); groups/txn after Phases 2-3.
- ULL/no-regression gated per phase (1.6, and each phase's e2e), not just at the end.
- Placeholders: each task names real files/symbols + a concrete test; deep code details live in the rev-4 spec the executor reads alongside.
