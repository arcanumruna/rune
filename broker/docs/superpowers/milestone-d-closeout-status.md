# Milestone D — Close-Out Status (2026-09-24)

**Verdict: Milestone D is CODE-COMPLETE on local `main`.** Its remaining close-out
item — physical native-Linux benchmark + dockerized DR-E2E validation — is **parked,
infra-blocked** (see below); D's correctness semantics are already **proven in-process**.

## What Milestone D is
Distributed partition leadership: dynamic rebalancing, position/lineage failover,
deterministic control tick, restart-durability trio, memory-topic tier
([[rune-milestone-d-spec-status]]).

## Completion state
- **Phase 1** (`docs/superpowers/plans/2026-09-22-rune-milestone-d-phase1.md`) — all tasks
  1–13 + final whole-branch review: DONE, on `main`.
- **Phase 2** (`docs/superpowers/plans/2026-09-23-rune-milestone-d-phase2.md`) — all tasks
  1–15 + M1/M2 + DEVIRT + final review: DONE, on `main`.
- **Merge to local main:** already done — there is no separate D branch; the whole of
  Milestone E (thread-per-core runtime) + the ring refactor were built directly on top of
  D on `main`. The spec's Phase-2 close-out sequencing (option (a): merge D now, replace
  the memmove `InMemoryLog` placeholder in E) is satisfied — the `InMemoryLog` was redone
  as a true single-owner ring in **E3 Task 6** (`2c932b8`).

## Close-out validation
Per the spec's non-goals §, D's close-out = native/Linux bench + DR-E2E + merge.

- **Merge:** ✅ done (D is on `main`).
- **DR / failover / no-committed-loss SEMANTICS:** ✅ **proven in-process** by the green
  parity suite that runs on every build:
  `ClusterDisasterRecoveryIntegrationTest`, `RebalanceFailoverIntegrationTest`,
  `GatewayFailoverReplicationTest`, `MultiLeaderProduceTest`, `OffsetReplicationTest`,
  `MetadataReplicationTest`, `GatewayReplicationWiringTest` — and all pass unchanged under
  the N-worker sharded runtime (E1 parity).
- **Native-Linux `<100µs` benchmark matrix:** ⏸ **PARKED — non-authoritative hardware.**
  Ran the benchmark suite on the Hetzner build agent (native Linux 5.15): intrinsic append
  is healthy (`BM_LogSegment_AppendNoSync` ≈ 5µs), but replicated-produce p99 is ms-scale
  because the box is a **shared 4-core CI VM** (co-resident Woodpecker + Artifactory +
  Aeron media driver, no `isolcpus`/`nohz_full`/affinity). This is **not** a valid `<100µs`
  measurement. Needs a **dedicated isolated multi-core Linux host** with E4a affinity
  (`RUNE_WORKER_CORE_BASE`) + `isolcpus`/`nohz_full` + SQPOLL-pinned io_uring.
- **Dockerized DR-E2E:** ⏸ **PARKED — build-image + host-capacity blocked.** The harness
  EXISTS and is comprehensive (`docker/e2e.sh` 3-node aeron:udp cluster + leader-kill;
  `dr-e2e/` py-testinfra suite with netem/crash faults + invariants). It builds the broker
  image `FROM rune-cpp-build:latest`, which is **absent from both the agent and the
  registry**. Running it needs either (a) the `rune-cpp-build` base image published, or
  (b) a native-build → slim-runtime-image path + `docker compose --no-build`, PLUS a
  less-contended host (a 4-core VM cannot cleanly run 3 brokers + probe + media drivers +
  netem).

## Recommendation (morning decision)
The physical bench + DR-E2E validation for **both Milestone D close-out and E4** needs one
thing: **a dedicated isolated Linux host + the `rune-cpp-build` image available** (publish
it to the registry, or build a slim runtime image from the native binary). Once that exists:
1. Run the bench matrix with affinity + `isolcpus` → validate `<100µs` p99 per tier.
2. Run `dr-e2e/` + `docker/e2e.sh` under the sharded runtime → confirm multi-process
   failover / no-committed-loss + netem resilience.

Until then, D is functionally complete and its correctness is covered by the in-process
suite; the parked items are **environment/validation**, not code.
