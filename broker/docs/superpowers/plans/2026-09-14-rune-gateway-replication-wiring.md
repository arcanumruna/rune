# Rune — Gateway↔Replication Wiring (acks=All / ISR durability) Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Consensus/durability tickets (GR-2, GR-3, GR-4, GR-5, GR-6, GR-7, GR-9) are MANDATORY Opus safety-review gates. Aeron-bible: single-writer per partition, no hot-path locks, zero per-op alloc, **NO synchronous blocking on the duty-cycle thread** (DR-BUG-2 lesson). Commit local `main`, no push. NEVER conan create/export/upload. Build `-j2`. Run Docker suites in the MAIN session background (agents return mid-run); subagents iterate tiny `-k` subsets; escalate Sonnet regressions to Opus; never trust a subagent "all green" — verify committed state.

## Goal
Wire the **existing** Phase-3 replication engine (`libs/replication`) into the client-facing gateway data plane so committed data survives leader failover. Today the gateway is leader-local: produced data is never replicated, so killing the leader loses everything it held (DR-3 probe lost 200/200 acked records). This blocks the DR durability suite (DR-3/4/5/7/10/15).

Target: `produce → leader appends → replicates to ISR followers → HW advances on majority-ack → acks=Leader (immediate, page-cache) vs acks=All (wait for ISR HWM)`; subscribe bounded by HW; on failover the new leader (from the ISR) already holds committed data; followers truncate uncommitted tail per leader-epoch.

**Scope is ONLY replication + acks=All wiring to unblock DR durability. Consumer-groups / EOS / transactions are OUT (later MC work).**

## Current-state map (the engine exists but is NOT wired into the running broker)
- Gateway produce is leader-local: `ClientGateway.cpp:685 handleProduce` → restamp (CRC-safe, `:819-900`) → `log->appendNoSync` (`:918`) → ack immediately (`:934`). acks=All == Leader today ("replication-ack wiring TODO", `:674-679`, `:902-916`).
- Group-commit flush `flushDirtyLogs()` `:1404`, 10ms cadence (`ClientGateway.h:360`), in `doWork()` `:95`.
- Subscribe read bound is LEO not HW: `pushSubscriptions:1189`, `bound = log->logEndOffset()` `:1209`.
- Leadership view `LeaderView{isLeader, leaderClientEndpoint}` (`ClientGateway.h:62`) is a single CLUSTER-wide flag, not per-partition (`BrokerNode.cpp:281-315`).
- `TopicRegistry` owns `Log` per partition (`TopicRegistry.h:92-102`); hands gateway a raw `Log*` (`:78`).
- Runtime: single AgentRunner → `NodeRuntimeAgent::doWork` (`NodeRuntimeAgent.h:45`): `consensus.doWork` → `gateway->doWork`. Single-writer, BackoffIdle, no busy-spin.
- Replication engine ready to consume: `PartitionReplica.h` shares the same `rune::storage::Log*` type (clean seam); `append(records, acksAll)` `:151`; `onLeaderLogAppended()` `:163` (co-located-writer reconcile — key for keeping the gateway restamp); `onFetchRequest` `:178`; `read(≤HW)` `:186`; `applyReplicated` (truncate-on-epoch) `:201`; `doWork` `:218`; `becomeLeader/becomeFollower` `:134-137`; `highWatermark()`/`logEndOffset()`/`isr()`. `PartitionTransport` is a pure interface — only `InMemoryTransport` test fake exists, **no Aeron impl**. `IsrManager::canProduce(minIsr, acksAll)`; `HighWatermark` majority logic; `Log::truncateTo`.
- Verification target: `dr-e2e/tests/test_dr3_leaderkill.py` should pass once acks=All is wired.

## Target architecture
- **Log ownership:** `PartitionReplica` does NOT take the Log from `TopicRegistry`; both hold a non-owning `Log*` to the same registry Log. **Mode A (chosen):** gateway keeps its restamp + `appendNoSync`, then calls `replica.onLeaderLogAppended()` to advance tracked LEO; followers pull via fetch. (Mode B — replica owns offset assignment — rejected: duplicates the CRC-load-bearing restamp.)
- **Acks:** acks=Leader/None ack immediately (unchanged, page-cache). acks=All DEFERS the ProduceResponse via a **pending-ack queue drained each `doWork()`** after HW advances — never a synchronous wait (DR-BUG-2).
- **Subscribe bound → `replica.highWatermark()`** for committed reads (preserve read_uncommitted path; LSO/read_committed stays MC no-op).
- **Metadata bridge:** per-partition `{leaderId, replicas, ISR, leaderEpoch}` from consensus drives `becomeLeader/becomeFollower`; DR scope: cluster-leader ⇒ leader-of-all-partitions, consensus term ⇒ leader-epoch (monotonic).
- **Transport:** new `AeronPartitionTransport` over per-peer replication `ExclusivePublication`s + replication ingress subscription, non-blocking `tryClaim`, polled in the duty cycle (async resolve, no blocking).

## Ordered tickets
- **GR-1 — AeronPartitionTransport** (real transport over aeron:udp; per-peer pubs + ingress sub; SBE fetch req/resp; non-blocking tryClaim; zero-alloc; async resolve). Tests: encode/decode round-trip + 2-node real-Aeron fetch loop. *Safety review: no.*
- **GR-2 — Consensus→replication metadata bridge** (per-partition leader/ISR/epoch; term→epoch monotonic; role flips + stale-epoch fencing). *Safety review: YES (epoch fencing, split-brain).*
- **GR-3 — Wire PartitionReplica into BrokerNode + duty cycle** (one replica per owned partition sharing registry Log*; inject transport; drive `replica.doWork`+`transport.poll` in `NodeRuntimeAgent::doWork`: consensus→poll→replica→gateway; gateway looks up replica by (topicId,partition)). *Safety review: YES (single-writer, no-blocking).*
- **GR-4 — Route produce through leader replica** (Mode A: after restamp+appendNoSync, call `onLeaderLogAppended`; reject NotLeader per GR-2; acks=Leader unchanged; no offset-assignment change). *Safety review: YES (offset assignment, no phantom-committed).*
- **GR-5 — acks=All deferred-ack** (pending-ack queue drained each cycle when `HW >= lastOffset`; `canProduce(minIsr, acksAll)` gate → reject on min-ISR loss, never falsely ack; bounded, zero-alloc; per-session ack order). Protocol: add `acks` field if absent (coordinate w/ client). *Safety review: YES (majority-durability correctness).*
- **GR-6 — Subscribe/push bounded by HW** (`pushSubscriptions` bound LEO→`highWatermark()`; clamp handleSubscribe nextOffset; no delivery of truncatable records). *Safety review: YES (no uncommitted-across-truncation).*
- **GR-7 — Follower catch-up / truncation / rejoin under failover** (epoch-change truncation via `Log::truncateTo`; becomeLeader from ISR is HW-preserving; reconcile with group-commit flush). *Safety review: YES (log divergence, truncation, fencing).*
- **GR-8 — Produce→majority-ack latency benchmark** (p99 gate; acks=Leader unaffected; acks=All reported vs <100µs — the 5ms fetch cadence dominates, so likely needs leader **push-on-append**; document the trade-off). *Safety review: no.*
- **GR-9 — DR-3 verification + consolidation** (run test_dr3_leaderkill.py w/ acks=All on live cluster; fix surfaced bugs; document acks=Leader tail-loss window [DR-15 input] + acks=All contract; full-area durability review). *Safety review: YES.*

## Risks / trade-offs
- **Majority-ack latency vs <100µs (biggest):** default `fetchIntervalNs=5ms` (`PartitionReplica.h:80`) → acks=All p99 would be ms. To approach 100µs the leader must **push on append** (leader-initiated replicate / immediate fetch-response), not wait for the follower poll. GR-1/GR-5 should support a low-latency push mode; GR-8 measures. acks=Leader stays page-cache (unaffected) — the mode the current DR harness relies on.
- **acks=Leader tail-loss window (DR-15):** un-flushed (≤10ms group-commit) AND un-replicated tail can vanish on hard leader crash; acks=All must lose nothing. Keep both contracts explicit.
- **No-blocking duty cycle (DR-BUG-2):** acks=All MUST be a deferred queue, transport resolve MUST be async — any blocking head-of-line-blocks the gateway.
- **Group-commit interaction:** a role flip must not fsync a tail that then truncates, nor double-count offsets.
- **Per-partition vs cluster leadership:** current `LeaderView` is one flag; GR-2 adds per-partition metadata (DR scope: they coincide, but don't hard-code that).
- **ISR under DR netem:** shrink/expand thresholds interact with injected loss/delay — moderate loss must not spuriously drop ISR below minIsr and stall acks=All (feeds DR-6).

## Verify end-to-end
`dr-e2e/tests/test_dr3_leaderkill.py` (idle/mid_produce/rolling) passes with acks=All: every acked record survives a leader kill, single-leader holds, uncommitted tail truncated, no divergence.
