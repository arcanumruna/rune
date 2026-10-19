# Rune vs Kafka Benchmark Comparison — Design

**Date:** 2026-09-26
**Status:** Draft for review
**Owner:** vradu

## Goal

Produce a **credible, reproducible, apples-to-apples** performance comparison
between Rune and Apache Kafka, run on a single rented dedicated box, and
publish it as an honest engineering head-to-head page under `rune-site`
(`compare.html`). The page shows the environment/specs, the methodology, the
matched-durability results, scaling curves, read-vs-write behaviour, and DR /
failover behaviour — **including the cells where Kafka wins.**

The centrepiece of this spec is the **Page Layout & Content Template**
(section further down): the exact section order, the exact tables, every cell
empty and ready to be filled from `results.json`, and an explicit commentary
slot under each table for analysis written *after* the numbers are in.

## Non-Goals

- No live streaming of results (rune-live is explicitly out of scope).
- No marketing spin: this is a technical, reproducible comparison.
- Not a distributed multi-host network benchmark — 3-node cells are co-located
  on one box and labelled as replication-protocol cost, not cross-host latency.

## Global Constraints (fairness spine)

Binds every task and every result cell. Copy verbatim into the plan.

1. **Same box, sequential runs.** Rune and Kafka never run at the same time.
   Identical core pinning, NVMe, record generator, record sizes, record counts.
2. **Matched durability tiers only.** A Rune number is only ever compared to a
   Kafka number at the *same* guarantee (D0/D1/D2). No cross-tier comparisons.
3. **Both tuned to their best ("utmost config").** Full config published in the
   appendix. Any cell where Kafka wins is shown, not hidden.
4. **Core budget is the fair cross-tool axis, not "workers."** Head-to-head
   cells hold cores + partitions constant. "Workers" is a Rune-internal scaling
   panel; Kafka's equivalent panel gives Kafka the same core budgets via
   `taskset`.
5. **Steady state + tails.** JVM warm-up discarded (GC logged); Rune warm-up
   discarded. Report **p50 / p99 / p99.9**, never mean-only. **≥3 repeats** per
   cell; report median + spread.
6. **Fully reproducible.** Exact versions, kernel, mount options, every config
   file, every driver script committed.

## Hardware & Environment

- **Box:** Hetzner **ccx43** — 16 dedicated AMD vCPU, 32 GB RAM, ~360 GB NVMe.
- **Core layout (single-node):** broker pinned to **8 cores**; load generator +
  Aeron media driver + OS on the other 8 (no oversubscription).
- **Provisioning:** created via `hcloud` in-session, hard teardown deadline
  (proposed 2 h). Fingerprint captured to `specs.json`.
- **Prototype first on build-agent-01 (178.104.21.208)** before spending on ccx43.

## Durability Tiers (matched-comparison levels)

| Tier | Guarantee | Rune config | Kafka config |
|------|-----------|-------------|--------------|
| **D0** | Leader accepted, page cache | `acks=leader`, `durabilityTier=1` | `acks=1`, OS flush |
| **D1** | Quorum / ISR replicated | `acks=all`, ISR, `durabilityTier=1` | `acks=all`, RF=3, `min.insync.replicas=2` |
| **D2** | Synchronous disk (fsync/batch) | `acks=all` + `durabilityTier=2` (`fdatasync`) | `acks=all` + `flush.messages=1`, `flush.ms=0` |

## Cross-tool axis mapping

| Concept | Rune | Kafka |
|---------|------|-------|
| Parallelism unit | worker (owns co-partition shards) | partition + io/network thread pools |
| Scaling knob | `RUNE_WORKERS` at fixed partitions | partitions + core budget (`taskset`) |
| Head-to-head control | fixed 8 cores + 8 partitions | fixed 8 cores + 8 partitions |
| No-persistence | log dir on tmpfs (`/dev/shm`) | `log.dirs` on tmpfs |
| Idle strategy | `throughput` (Backoff) / `lowlatency` (BusySpin) | n/a (no busy-spin mode) |

### Rune idle-strategy dimension (low-latency mode)

Rune has an optional **low-latency mode** — busy-spin idle on the worker /
runtime / client loops (`RUNE_WORKER_IDLE=busyspin`, `RUNE_RUNTIME_IDLE=busyspin`,
client `IdleMode::BusySpin`) — which removes park/wakeup latency (the Backoff
strategy parks up to ~1 ms). This is a legitimate "utmost config" for
latency-sensitive deployments, and Kafka has no equivalent; the page states
this transparently.

- **Latency cells (§9): report Rune in BOTH modes** (throughput/Backoff and
  lowlatency/BusySpin) alongside Kafka. The sub-100µs claim is specifically the
  low-latency mode.
- **Throughput cells (§3–§8): default throughput/Backoff mode only.** Busy-spin
  does not raise throughput and can starve the Aeron media driver on a shared
  core budget, so it is not used there (noted on the page).

---

# Page Layout & Content Template (`rune-site/compare.html`)

This is the exact structure the page renders, in section order. Style matches
the existing `docs.html` / `selfhost.html` dark arcane theme, self-contained
(inline CSS/JS). Numbers are embedded at author time from `results.json`; no
live feed.

**Placeholder conventions used below (and in the built page's source until data lands):**

- **`▢`** — a numeric cell to be filled from `results.json` after the run.
- **`▢±▢`** — median ± spread (min/max across the ≥3 repeats).
- **`⟨Δ⟩`** — computed ratio Rune÷Kafka (or ×faster), filled at author time.
- **`📝 Commentary (post-run)`** — a blockquote slot for analysis written once
  results exist. One under every table. Empty (italic "TBD after run") until then.
- Every table caption ends with its `results.json` filter, e.g.
  `[scenario=write, tier=D0, record_bytes=256]`, so filling cells is mechanical.

---

## §0 — Hero

> **Rune vs Apache Kafka** — an honest, reproducible head-to-head.
> One-line thesis: ⟨thesis, e.g. "matched durability, Rune delivers ⟨Δ⟩× the write throughput at ⟨Δ⟩× lower p99 on the same 8 cores"⟩.

> 📝 Commentary (post-run): _TBD — the single sentence the whole page proves._

Links: [Methodology](#methodology) · [Full config appendix](docs.html) · [Raw results.json](#)

---

## §1 — Environment & Specs  `[from specs.json]`

| Field | Value |
|-------|-------|
| Provider / instance | Hetzner ccx43 |
| CPU model | ▢ |
| vCPU (dedicated) | 16 |
| Broker cores pinned | 8 |
| RAM | ▢ |
| NVMe model | ▢ |
| Filesystem / mount opts | ▢ |
| Kernel version | ▢ |
| OS / distro | ▢ |
| Kafka version | ▢ |
| JDK version | ▢ |
| Rune git commit | ▢ |
| Client location | co-located, pinned to separate cores |
| Benchmark date | ▢ |

> 📝 Commentary (post-run): _TBD — anything about the box that shaped results (NVMe class, NUMA, turbo, etc.)._

---

## §2 — Methodology & Fairness  {#methodology}

Fixed prose (known now, not a placeholder): the six Global Constraints in
reader-friendly form + the durability-tier table above + the cross-tool axis
table above. Explicit statement: "every compared pair shares the same
durability guarantee, the same 8 cores, the same 8 partitions, the same record
size; each tool was tuned to its best; full configs are in the appendix."

> 📝 Commentary: _fixed text; no post-run fill needed._

---

## §3 — Headline: Write & Read at matched durability

One table **per durability tier × per record size** (6 tables total). Same shape each.

### §3.1 — D0 (leader-only) · 256 B  `[scenario∈{write,read}, tier=D0, record_bytes=256, partitions=8, cores=8]`

| Metric | Rune | Kafka | ⟨Rune vs Kafka⟩ |
|--------|------|-------|-----------------|
| Write throughput (msg/s) | ▢±▢ | ▢±▢ | ⟨Δ⟩ |
| Write throughput (MB/s) | ▢±▢ | ▢±▢ | ⟨Δ⟩ |
| Write p50 latency (µs) | ▢ | ▢ | ⟨Δ⟩ |
| Write p99 latency (µs) | ▢ | ▢ | ⟨Δ⟩ |
| Write p99.9 latency (µs) | ▢ | ▢ | ⟨Δ⟩ |
| Consume throughput (msg/s) | ▢±▢ | ▢±▢ | ⟨Δ⟩ |
| Consume throughput (MB/s) | ▢±▢ | ▢±▢ | ⟨Δ⟩ |

> 📝 Commentary (post-run): _TBD — who wins at D0/256B and why._

### §3.2 — D0 · 1 KB — _same table, `record_bytes=1024`_
### §3.3 — D1 (quorum) · 256 B — _same table, `tier=D1`_
### §3.4 — D1 · 1 KB
### §3.5 — D2 (fsync/batch) · 256 B — _same table, `tier=D2`_
### §3.6 — D2 · 1 KB

> 📝 Commentary (post-run, whole §3): _TBD — how the gap moves as durability tightens D0→D1→D2._

---

## §4 — Write scaling (partitions × record size)

One table **per tool per tier** (rows = partitions, cols = record size,
cells = write msg/s). 2 tools × 3 tiers = 6 tables, same shape.

### §4.1 — Rune · D1  `[tool=rune, scenario=write, tier=D1, cores=8]`

| Partitions ↓ / Record → | 100 B | 256 B | 1 KB | 4 KB |
|-------------------------|-------|-------|------|------|
| 1 | ▢ | ▢ | ▢ | ▢ |
| 2 | ▢ | ▢ | ▢ | ▢ |
| 4 | ▢ | ▢ | ▢ | ▢ |
| 8 | ▢ | ▢ | ▢ | ▢ |

### §4.2 — Kafka · D1 — _same table, `tool=kafka`_
### §4.3–§4.6 — repeat for D0 and D2 (Rune + Kafka each)

> 📝 Commentary (post-run): _TBD — does throughput scale with partitions to 8, where does each tool saturate, small vs large record behaviour._

---

## §5 — Core / worker scaling

### §5.1 — Rune worker scaling (Rune-only panel)  `[tool=rune, scenario=scaling, partitions=8, tier=D1]`

| Workers | Write msg/s | Write MB/s | p99 (µs) | Scaling vs 1-worker |
|---------|-------------|------------|----------|---------------------|
| 1 | ▢ | ▢ | ▢ | 1.00× |
| 2 | ▢ | ▢ | ▢ | ⟨Δ⟩ |
| 4 | ▢ | ▢ | ▢ | ⟨Δ⟩ |
| 8 | ▢ | ▢ | ▢ | ⟨Δ⟩ |

### §5.2 — Kafka core-budget scaling  `[tool=kafka, scenario=scaling, partitions=8, tier=D1]`

| Cores | Write msg/s | Write MB/s | p99 (µs) | Scaling vs 1-core |
|-------|-------------|------------|----------|-------------------|
| 1 | ▢ | ▢ | ▢ | 1.00× |
| 2 | ▢ | ▢ | ▢ | ⟨Δ⟩ |
| 4 | ▢ | ▢ | ▢ | ⟨Δ⟩ |
| 8 | ▢ | ▢ | ▢ | ⟨Δ⟩ |

> 📝 Commentary (post-run): _TBD — linearity of each; where Rune's shared-nothing per-core model helps or doesn't._

---

## §6 — Read / consume & read-vs-write ratio

### §6.1 — Consume throughput vs consumer count  `[scenario=read, tier=D1, partitions=8]`

| Consumers (each drains 8 partitions) | Rune agg msg/s | Kafka agg msg/s | ⟨Δ⟩ |
|--------------------------------------|----------------|-----------------|-----|
| 1 | ▢±▢ | ▢±▢ | ⟨Δ⟩ |
| 2 | ▢±▢ | ▢±▢ | ⟨Δ⟩ |
| 4 | ▢±▢ | ▢±▢ | ⟨Δ⟩ |

### §6.2 — Read-vs-write ratio (the "reads should exceed writes" thesis)

| Tool | Write msg/s (D1) | Peak read msg/s | Read ÷ Write |
|------|------------------|-----------------|--------------|
| Rune | ▢ | ▢ | ⟨Δ⟩ |
| Kafka | ▢ | ▢ | ⟨Δ⟩ |

> 📝 Commentary (post-run): _TBD — does read exceed write, and by how much, for each tool._

---

## §7 — Persistence: NVMe vs tmpfs  `[scenario=write, tier=D1, partitions=8, record_bytes=256]`

| Log location | Rune msg/s | Kafka msg/s | Rune p99 (µs) | Kafka p99 (µs) |
|--------------|------------|-------------|---------------|----------------|
| NVMe | ▢ | ▢ | ▢ | ▢ |
| tmpfs (`/dev/shm`) | ▢ | ▢ | ▢ | ▢ |

> 📝 Commentary (post-run): _TBD — how much of the gap is disk vs protocol._

---

## §8 — Single-node vs 3-node (co-located)  `[scenario=write, tier=D1, partitions=8, record_bytes=256]`

> ⚠️ 3 brokers co-located on one box over loopback, sharing one NVMe. Measures
> replication-protocol cost, **not** cross-host network latency.

| Topology | Rune msg/s | Kafka msg/s | Rune p99 (µs) | Kafka p99 (µs) |
|----------|------------|-------------|---------------|----------------|
| 1 node | ▢ | ▢ | ▢ | ▢ |
| 3 nodes (RF=3) | ▢ | ▢ | ▢ | ▢ |
| replication overhead (%) | ⟨Δ⟩ | ⟨Δ⟩ | ⟨Δ⟩ | ⟨Δ⟩ |

> 📝 Commentary (post-run): _TBD — cost of quorum replication for each._

---

## §9 — Latency-focused (round-trip, window=1)  `[scenario=latency, partitions=8, record_bytes=256]`

Rune reported in both idle modes; Kafka has no busy-spin equivalent. All µs.
Rune: `RUNE_BENCH_WINDOW=1` + `RUNE_WORKER_IDLE`/`RUNE_RUNTIME_IDLE`;
Kafka: `kafka.tools.EndToEndLatency`.

| Tier | Config | p50 | p99 | p99.9 |
|------|--------|-----|-----|-------|
| D0 | Rune — low-latency (busy-spin) | ▢ | ▢ | ▢ |
| D0 | Rune — throughput (backoff) | ▢ | ▢ | ▢ |
| D0 | Kafka | ▢ | ▢ | ▢ |
| D1 | Rune — low-latency (busy-spin) | ▢ | ▢ | ▢ |
| D1 | Rune — throughput (backoff) | ▢ | ▢ | ▢ |
| D1 | Kafka | ▢ | ▢ | ▢ |
| D2 | Rune — low-latency (busy-spin) | ▢ | ▢ | ▢ |
| D2 | Rune — throughput (backoff) | ▢ | ▢ | ▢ |
| D2 | Kafka | ▢ | ▢ | ▢ |

> 📝 Commentary (post-run): _TBD — sub-100µs claim check per tier; busy-spin vs backoff delta; where Rune's low-latency mode lands vs Kafka._

---

## §10 — Durability / DR / failover

| Measure | Rune | Kafka |
|---------|------|-------|
| Leader-kill → new leader elected (ms) | ▢ | ▢ |
| Time to resume producing after kill (ms) | ▢ | ▢ |
| Acked records lost at D1 after `kill -9` | ▢ (expect 0) | ▢ (expect 0) |
| Acked records lost at D2 after `kill -9` | ▢ (expect 0) | ▢ (expect 0) |
| Full-restart recovery time (s) | ▢ | ▢ |

> 📝 Commentary (post-run): _TBD — failover speed + crash-consistency verdict._

---

## §11 — Where Kafka wins (honesty box)

> 📝 Commentary (post-run): _TBD — list every cell/scenario Kafka wins and why
> (e.g. large-batch streaming throughput, mature tiered storage, ecosystem).
> This section is required, not optional._

---

## §12 — Reproduce it yourself / config appendix

Fixed content: link to `bench/kafka-compare/` scripts, the full Rune env + full
Kafka `server.properties`/producer configs, the exact commands, and
`results.json` + `specs.json` downloads.

---

# Harness Architecture

New self-contained directory **`rune-broker/bench/kafka-compare/`** (committed):

- `provision.sh` — `hcloud` create ccx43; install JDK + Kafka release; build
  Rune broker + client; capture box fingerprint → `specs.json`.
- `run-rune.sh` — drive Rune over the matrix via existing
  `rune_produce_throughput` / `rune_consume_throughput`; one JSON line per cell.
- `run-kafka.sh` — drive Kafka via `kafka-producer-perf-test.sh`,
  `kafka-consumer-perf-test.sh`, `kafka.tools.EndToEndLatency`; same schema.
- `lib.sh` — core pinning (`taskset`), tmpfs setup, repeat/aggregate, JSON emit,
  inter-cell cleanup.
- `collect.py` — merge per-cell JSON → `results.json`; validate schema; compute
  medians + spread.
- `teardown.sh` — `hcloud` destroy; print confirmation.

**Result schema (one object per cell)** — fields map 1:1 to the `[filter]`
captions above:
```json
{
  "tool": "rune|kafka",
  "scenario": "write|read|latency|dr|scaling",
  "tier": "D0|D1|D2",
  "record_bytes": 256,
  "partitions": 8,
  "cores": 8,
  "workers": 8,
  "consumers": 1,
  "persistence": "nvme|tmpfs",
  "nodes": 1,
  "idle": "throughput|lowlatency|na",
  "repeats": 3,
  "throughput_msgs_per_sec": {"median": 0, "min": 0, "max": 0},
  "throughput_mb_per_sec": {"median": 0, "min": 0, "max": 0},
  "latency_us": {"p50": 0, "p99": 0, "p999": 0},
  "dr": {"elect_ms": 0, "resume_ms": 0, "acked_lost": 0, "restart_s": 0},
  "notes": ""
}
```
`results.json` + `specs.json` are the single source of truth the page renders from.

## Execution Plan

1. Prototype harness on **build-agent-01** with a tiny matrix; freeze JSON schema.
2. `provision.sh` the **ccx43**; set hard teardown deadline.
3. Run full matrix (Tier headline §3 first, then §4–§10), ×3 repeats.
4. `collect.py` → `results.json`; `scp` results + `specs.json` back.
5. `teardown.sh` **immediately** after collection.
6. Fill `compare.html` cells from real data; commit to `rune-site`.

## Caveats (stated on the page)

- 3-node co-located: replication-protocol cost, not cross-host latency.
- JVM warm-up: Kafka numbers are steady-state post-warm-up.
- Loopback client: co-located, pinned to separate cores; removes NIC latency
  from both tools equally.

## Open Items (confirm before provisioning)

1. **hcloud token** available to this session? — hard blocker for `provision.sh`.
2. **Teardown window** — 2 h recommended (full grid × 3 repeats × 2 tools).
3. **D2 broker knob** — confirm `durabilityTier=2` reachable via broker/topic env
   for the bench topic; wire an env if not exposed.

## Self-Review

- **Placeholders:** the page template intentionally uses `▢` / `⟨Δ⟩` / commentary
  slots — that is the requested deliverable; every such placeholder has a
  `results.json` filter so filling it is mechanical.
- **Consistency:** durability tiers, schema fields, matrix dimensions, and cell
  captions are identical across methodology, page template, and schema.
- **Scope:** single-box, 8-partition, matched-tier — one implementation plan.
- **Ambiguity:** "workers" vs "cores" disambiguated as separate axes; 3-node
  scoped as co-located.
