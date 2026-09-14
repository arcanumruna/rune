# Rune — Dockerized Disaster-Recovery E2E Suite Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Runs BEFORE Milestone C. Every scenario ticket runs the REAL dockerized cluster + multiple client containers, injects faults, asserts invariants, and **fixes every bug it surfaces** (Opus-reviewed; consensus/durability changes get a safety review).

**Goal:** An exhaustive, py-testinfra-style Python+Docker E2E suite that drives the real 3-node `aeron:udp` Rune cluster with multiple concurrent clients while injecting disasters (node/leader/follower crashes in combination, network partitions, packet loss/latency), asserting the broker's safety + delivery invariants and fixing every bug found.

**Architecture:** Python test harness (`pytest` + Docker SDK / `docker compose` + `tc`/`netem`), modeled on the sibling `py-testinfra` C++ integration approach. Reuses the Milestone-B `docker-compose` cluster + a client-container image (produce/subscribe driver with assertions). Faults via `docker kill/stop/pause`, `tc qdisc netem` (loss/delay), and network disconnect. Invariants: no committed-record loss, per-key ordering, exactly-once delivery, single-leader (no split-brain), resume-from-offset, durability across restart.

**Tech Stack:** Python 3.10+ venv, pytest, docker, tc/netem (Linux net-emul; on macOS/Docker-Desktop use container-level netem via `NET_ADMIN`), the rune-broker compose + a rune-client-cpp driver image.

## Global Constraints
- REAL cluster + REAL clients in containers; no mocks. Deterministic-where-possible + bounded (hard timeouts, no infinite waits); clean teardown (`down -v`) even on failure.
- **Invariants asserted every scenario:** no acknowledged-committed-record loss (within the documented `acks=Leader` page-cache tail caveat — quantify it); per-key ordering; exactly-once delivery to subscribers; **single leader at all times** (never two leaders in one term); resume-from-tracked-offset after client reconnect; durability across node restart.
- **Fix every bug found while running** — surface it, fix it (broker or client), Opus-review the fix; consensus/durability/safety changes get a dedicated safety review. Re-run the scenario green before closing.
- Subagents CAN run Docker. Opus reviewers + Sonnet implementers; escalate to Opus on Sonnet regressions. NEVER build/publish conan packages. Commit locally on `main`, do not push. Update live + Planka per ticket.
- py-testinfra style: the harness lives in a new `rune-broker/dr-e2e/` (or a dedicated `rune-dr-e2e` dir) — Python, dockerized, reusable fixtures.

## Tickets

### DR-1 — Harness scaffold + multi-client driver
Python pytest harness: fixtures to build+up/down the 3-node compose cluster (RUNE_TOPICS, per-peer client ports), wait-for-leader, and spawn N **client-driver containers**. A configurable client-driver (extend `rune_produce_subscribe`) that produces to many keys + subscribes + records per-message results to a shared volume/stdout for assertions. Fault-injection helpers: `kill_leader()`, `kill_node(id)`, `pause/unpause`, `netem(node, loss=, delay=)`, `partition(a,b)`, `heal()`. Invariant-assertion helpers (no-loss, ordering, exactly-once, single-leader-from-logs/probe). **Acceptance:** `pytest dr-e2e/` runs a trivial up→produce→subscribe→down green.

### DR-2 — Multi-client concurrent baseline (no faults)
K clients (e.g. 3–5) concurrently produce to many keys across partitions + subscribe; assert per-key ordering, exactly-once, all records delivered, idempotent (no dup on producer retry). Soak a few thousand records. **Acceptance:** invariants hold at scale, no faults.

### DR-3 — Leader-kill scenarios
Kill the leader: (a) idle, (b) mid-produce, (c) mid-subscribe, (d) repeated/rolling kills (kill new leader after re-election, ×3). Assert: re-election each time (single leader), clients recover via redirect-follow, no committed loss, ordering preserved. **Acceptance:** all leader-kill variants green; fix any recovery bug.

### DR-4 — Follower-kill + quorum scenarios
Kill 1 follower (quorum holds → cluster keeps serving); kill 2 of 3 (quorum lost → no progress, NO split-brain, clients get NotLeader/bounded-fail, never stale data); restore quorum → recovery. Assert single-leader-safety under minority. **Acceptance:** quorum-loss is safe (no split-brain, no phantom leader); recovery works.

### DR-5 — Network partition (netem/disconnect)
Partition the leader from the two followers (leader-isolation → leader steps down, majority side re-elects; isolated ex-leader must NOT keep serving writes → no split-brain); partition one follower (lags, catches up on heal); asymmetric partitions. Heal → convergence. Assert no two leaders, no committed loss, no divergence. **Acceptance:** partitions never split-brain; heal converges. (Consensus-safety review on any election fix.)

### DR-6 — Packet loss + latency (netem loss/delay)
Inject netem `loss` (e.g. 5–20%) + `delay` on consensus and/or client links during sustained produce/subscribe. Assert Aeron reliability holds (no loss/dup at the app), ordering preserved, cluster stays stable (no spurious elections at moderate loss; quantify the loss level that triggers re-election). **Acceptance:** lossy links don't corrupt delivery; document the stability envelope.

### DR-7 — Node crash + restart / durability recovery
SIGKILL a node + restart it → rejoins, catches up, no data loss for committed records; crash mid-produce → on restart `recover()` yields a consistent log (acks=Leader page-cache tail-loss caveat quantified, never corruption); crash the leader's disk-flush window. Assert durability + rejoin. **Acceptance:** killed nodes rejoin + recover; committed data survives; no corruption.

### DR-8 — Combined chaos + soak
Combinations while multiple clients act: leader-kill + packet loss; rolling restarts + client churn; a bounded chaos-monkey soak (random faults over N minutes) asserting invariants throughout. **Acceptance:** invariants hold under combined faults; catalogue + fix every bug surfaced.

### DR-9 — Bug consolidation + full-area review + docs
Consolidate + verify all fixes from DR-1..8; a full-area Opus review of the accumulated broker/client changes (esp. consensus/durability) for safety; document the DR suite (how to run, the fault matrix, the invariant set, the acks=Leader durability caveat). **Acceptance:** whole DR suite green + reviewed CLEAN; docs written.

### DR-DONE — Acceptance
The full DR-E2E suite passes against the live cluster (all scenarios), every surfaced bug fixed + reviewed, invariants documented. Milestone C may then begin.

## Self-review (coverage)
- Multiple concurrent clients → DR-1/DR-2/DR-8. Leader crash → DR-3. Follower/quorum → DR-4. Network lost/partition → DR-5. Packet loss → DR-6. Node crash+restart/durability → DR-7. Combinations/chaos → DR-8. Fix-all-bugs → every ticket + DR-9. py-testinfra style → DR-1 harness. Single-leader/no-split-brain safety → DR-4/DR-5 (+ safety reviews). ✓
