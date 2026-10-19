# Rune — Ultra-Low-Latency, Durable, Kafka-Semantics Message Broker on Aeron

**Status:** Design (approved foundation, pending spec review)
**Date:** 2026-09-10
**Codename:** `Rune` (inscribing runes = append-only log writes)
**Language:** C++23
**Reference sources studied:** `apache/kafka` (log/index/record internals), `aeron-io/aeron` (transport, Archive, Cluster/KRaft designs), `world-server` + `common` (project/packaging conventions)

---

## 1. Purpose & Scope

Build a general-purpose message broker with **Kafka-like API and semantics** that is **durable, highly available, self-healing, recoverable, and ultra-low-latency**. It is domain-agnostic but its defaults and performance envelope are tuned for **MMORPG-style workloads**: many small messages, strict per-key ordering, high fan-out, low tail latency.

It is **not** tied to any specific game feature. It is infrastructure: topics in, ordered durable streams out.

### 1.1 Hard requirements (the goal)

| Requirement | Definition of done |
|---|---|
| **Ultra low latency** | p99 < **100 µs** for a durable (majority-replicated) produce ack, single datacenter. |
| **Durability** | A committed (acked) message survives any single-node crash; no committed-message loss. |
| **High availability** | Cluster keeps serving while a majority of nodes survive (3 nodes tolerate 1 loss, 5 tolerate 2). |
| **Self-healing** | Automatic leader failover, in-sync-set repair, and re-replication onto healthy nodes without operator action. |
| **Recovery** | Crashed/rejoining nodes catch up automatically; cold start recovers from disk (Archive + snapshots). |
| **Kafka-like API/semantics** | Topics + partitions + keyed ordering, consumer groups + offset commit, retention + compaction, and (final phase) exactly-once/transactions. |

### 1.2 Explicit non-goals (v1)

- **Not** Kafka wire-protocol compatible (a protocol-adapter can come later).
- **Not** multi-datacenter synchronous replication (speed of light vs. 100 µs). Async cross-DC backup is a later phase.
- **Not** fsync-per-message durability (see §3.1 — incompatible with 100 µs; durability is majority-in-memory + async archival).

---

## 2. Key architectural decision: consensus only where it's needed

Raft/consensus is a **means, not a goal**. The design splits into a **consensus'd control plane** (rare, off the hot path) and a **consensus-free data plane** (the hot path).

- **Control plane** — a single small Raft quorum (all 3–5 nodes) owning *cluster metadata*: topic configs, partition→leader assignments, in-sync-replica (ISR) membership, node liveness, consumer-group coordinator assignments. Runs only on membership/leadership/config change.
- **Data plane** — per-partition **leader → follower** replication with an ISR set and a high-watermark. A durable write = **majority in-memory ack**. No consensus round per message.
- **Storage** — each partition log recorded to disk **asynchronously** by Aeron Archive + periodic snapshots. Disk latency is fully decoupled from produce latency.

This is the architecture Kafka itself converged on (KRaft controller + ISR data replication), re-implemented in C++ on Aeron. Each hot-path requirement is satisfied with **zero** consensus rounds; consensus is reserved for the rare decisions that genuinely need global agreement.

---

## 3. Topology & node model

A single cluster of **3 or 5 nodes** in one datacenter. Every node runs one identical binary hosting three roles concurrently:

```
        ┌────────────────────────── NODE (×3–5) ──────────────────────────┐
 client │   ┌─────────────┐   ┌──────────────────┐   ┌─────────────────┐   │
 ─────►  │   │  Gateway    │──►│  Data plane      │◄─►│  Control plane  │   │
 Aeron  │   │  (API/ingr) │   │  partition       │   │  metadata Raft  │   │
        │   └─────────────┘   │  leaders/replicas│   │  quorum         │   │
        │                     └────────┬─────────┘   └────────┬────────┘   │
        │                     ┌────────▼─────────┐   ┌─────────▼────────┐  │
        │                     │ Aeron Archive    │   │ Metadata log +   │  │
        │                     │ (segments, async │   │ snapshots        │  │
        │                     │  record + replay)│   │                  │  │
        │                     └──────────────────┘   └──────────────────┘  │
        └──────────────────────────────────────────────────────────────────┘
            all inter-node + client traffic over Aeron (UDP unicast / IPC)
```

- **Gateway** — terminates client Aeron sessions, authenticates, routes produce/fetch to the correct partition leader (forwards to a peer node if this node doesn't lead that partition).
- **Data plane** — owns the partition replicas (leader and follower roles) assigned to this node. Hot path.
- **Control plane** — the metadata Raft quorum. Mostly idle.
- **Archive** — per-node async recorder of partition logs to local disk + snapshots.

**Single-writer discipline:** each partition has exactly one leader thread owning its log; no locks on the hot path (Aeron/LMAX mechanical-sympathy philosophy). Partition leadership is balanced across nodes.

### 3.1 The durability tradeoff (explicit)

At p99 < 100 µs, fsync-per-message is physically impossible. **"Durable" = replicated to a majority of nodes' memory at ack time** (survives any single node crash), with disk persistence via Aeron Archive happening asynchronously plus periodic snapshots. This is the standard high-end (financial-exchange class) durability model and is the same guarantee Aeron Cluster provides.

---

## 4. Data-plane hot path (the <100 µs write)

**Produce path (partition leader), no consensus round:**

1. Client sends a produce **batch** over Aeron to the partition leader (gateway forwards if needed).
2. Leader appends to its in-memory active segment (ring buffer), assigns offsets, advances the **log-end offset**.
3. Leader multicasts the batch to its followers over Aeron.
4. Each follower appends in memory and acks its replicated offset.
5. When a **majority of the ISR** (leader + enough followers) hold the batch in memory, the leader advances the **high-watermark** and acks the client. **That ack is the durable commit.**
6. *Off the hot path:* Aeron Archive records committed segments to disk; followers likewise; producer state and indexes update.

**Properties delivered:**
- **Durability** = majority-in-memory at ack time; disk via Archive lags but never blocks the ack.
- **Latency** = one hop to followers + majority ack; Aeron busy-spin + batching → tens-of-µs regime; no fsync, no vote.
- **Ordering** = single leader per partition + monotonic offsets ⇒ strict per-partition (hence per-key) order.
- **Backpressure** = Aeron flow control throttles producers when followers lag.

**High-watermark rule (consistency guard):** consumers may only read up to the high-watermark, i.e. only majority-replicated data is ever visible. This is what makes failover safe — a newly elected leader always holds every acked message.

---

## 5. Control plane (metadata Raft quorum)

One Raft group replicating a **metadata state machine**. The only place full consensus runs, and it runs rarely.

**State stored (authoritative cluster state):**
- Topic configs (partition count, retention, compaction policy, replication factor).
- Per partition: `{ leader node, replica set, ISR set, leader epoch }`.
- Node membership & liveness (from Aeron session/heartbeat timeouts).
- Consumer-group → coordinator-node assignments.

**Operations:** create/delete/alter topic; elect new partition leader on failure; shrink/grow ISR; rebalance leadership; register/expire nodes. Each is a small committed entry that bumps a monotonic **leader epoch** to fence stale leaders.

**Implementation:** compact C++ Raft over Aeron transport, using Aeron Cluster's `Election` and KRaft's `QuorumState`/`LeaderState`/`FollowerState` as blueprints, with `RecordingLog`-style metadata snapshotting for fast recovery.

---

## 6. Storage internals

Each partition replica is a **log of immutable segments**, following Kafka's proven layout (`LogSegment`/`OffsetIndex`/`TimeIndex`) but backed by Aeron Archive and tuned for deterministic latency.

### 6.1 Segments
- Log rolls into fixed-size segments (size- or time-based). Only the active segment is written.
- A segment's offset span must fit in `int32` (see relative offsets below) — a primary reason segments roll.

### 6.2 Sparse mmap'd indexes (from Kafka)
- **Offset index entry = 8 bytes:** `int32 relativeOffset` + `int32 physicalPosition`. Offsets stored **relative to segment base offset** → 4 bytes instead of 8.
- **Time index entry = 12 bytes:** `int64 timestamp` + `int32 relativeOffset`. **Optional per-topic** (see §9 divergence) — MMORPG traffic is offset-seek dominated.
- Indexes are **sparse** (one entry per ~4 KB of log): tiny, memory-resident, point to a neighborhood; then scan forward.

### 6.3 Lookup: warm-section search + pinned hot pages
- Adopt Kafka's **warm-section binary search** (search the last ~8 KB of the index first) as a fallback for cold/old segments — plain binary search over a large mmap causes page faults that spiked Kafka's produce p99 to ~1 s.
- **Divergence:** for the active segment we **`mlock`/pin** the index and the log tail so hot reads are *deterministically* fault-free — the guarantee <100 µs actually needs, rather than relying on page-cache warmth.

### 6.4 Record batch format (from Kafka RecordBatch v2)
The **batch** (not the record) is the unit of append, replication, fetch, and archival. Header (61 bytes) then delta-encoded records:

```
baseOffset:8  length:4  partitionLeaderEpoch:4  magic:1  CRC:4(CRC-32C)
attributes:2  lastOffsetDelta:4  baseTimestamp:8  maxTimestamp:8
producerId:8  producerEpoch:2  baseSequence:4  recordCount:4  records...
```

Design choices copied verbatim:
- **`partitionLeaderEpoch` excluded from the CRC** → broker stamps epoch on arrival without recomputing CRC.
- **`producerId`/`producerEpoch`/`baseSequence` in the header** → basis for idempotency/EOS. **Reserved from day one** even though dedup logic ships in Phase 5. Empty batches are retained to preserve a producer's last sequence across failover.
- **Magic byte before CRC** → self-describing, versionable format.
- **CRC-32C (Castagnoli)** computed with **hardware intrinsics** (SSE4.2 / ARMv8).

### 6.5 Two-tier write & snapshots
- Hot path touches only the in-memory tail. A background archiver hands committed ranges to Aeron Archive (`startRecording`/`getRecordingPosition`), which owns fsync cadence.
- Periodic **snapshots** capture "state as of offset N" for compacted/state topics and fast recovery: recovery = load newest snapshot + replay tail via `startReplay`, not replay-from-genesis.

### 6.6 Leader-epoch cache & producer-state
- Per-replica **leader-epoch cache** (`epoch → start offset`) drives correct truncation after failover.
- **Producer-state snapshots** (producerId → last sequence) enable idempotency and correct recovery.

---

## 7. Client API & Kafka semantics

Kafka-*like* mental model, C++-native and Aeron-framed (not wire-compatible).

**Producer**
- `send(topic, key, value, headers)`; partition = `hash(key) % partitions` (keyed ordering) or explicit.
- `acks` modes: `leader` (fastest) / `majority` (default, durable) / `all-ISR`.
- Batching + async futures; per-key order preserved.

**Consumer + consumer groups**
- `subscribe(topics)`, poll-based fetch from the committed region (≤ high-watermark).
- **Group coordinator** (a role assigned per group by the control plane) tracks membership and assigns partitions across members; rebalances on join/leave/failure.
- **Offset commit** stored in an internal compacted `__offsets` topic (Kafka's own mechanism); resume from earliest / latest / specific offset / by-timestamp.
- **Explicit member subscriptions (client / broker version 3, 2026-10-05).** The coordinator assigns partitions but creates **no** subscription: after its OffsetFetch answer each member subscribes to every assigned partition's **leader** node itself, from the committed offset (no commit: the log start). Records carry the `correlationId` of the Subscribe that created the subscription, and session / subscription ids are cluster-unique (`((nodeId + 1) << 40) | counter`), so a member accepts records only from its own current subscription instance.
  - **Compatibility rule.** Client and broker must both be version ≥ 3 (`kClientVersionExplicitGroupSubscriptions`, `kBrokerVersionExplicitGroupSubscriptions` in rune-protocol `Protocol.h`). The broker advertises its version in `ConnectResponse.brokerVersion` (an additive trailing field; the 38-byte frame of an older broker reads as 0). A coordinator refuses `JoinGroup` from a session whose `ConnectRequest.clientVersion` < 3 with `ErrorCode::ProtocolVersion` (non-retriable, logged); a client finishes a `GroupConsumer` with `GroupFailReason::BrokerTooOld` against a broker below 3. Mixed versions would otherwise duplicate or lose records (old auto-subscriptions vs. explicit ones).

**Admin** — create/delete/describe topics, alter partition count & config, inspect ISR/leadership.

**Retention** — per-topic time- and size-based; the cleaner deletes whole segments below the horizon (cheap, no rewrite).

---

## 8. Failure detection, self-healing & recovery

**Detection** — Aeron sessions surface liveness directly (dropped image / heartbeat timeout) — far tighter than TCP timeouts. Lagging followers are flagged.

**Self-healing loop (control-plane driven):**
1. Dead replica → control plane commits an ISR shrink, bumping leader epoch.
2. Dead **leader** → elect a new leader from the surviving ISR (guaranteed to hold all committed data), commit it, fence the old epoch. Clients discover the new leader via metadata refresh and retry.
3. **Under-replication repair** → assign a healthy node as a new follower; it bootstraps from the leader (snapshot + tail replay) and re-enters the ISR.
4. **Rebalance** → migrate leadership to even out load after failures.

**Recovery scenarios:**
- **Follower rejoin** → truncate to the leader's epoch boundary (via leader-epoch cache), stream the gap from the leader, re-enter ISR.
- **Cold cluster start** → reload segments from Archive, control plane reloads metadata snapshot, leaders re-elected from recorded ISR sets.
- **Total node loss** → its partitions were already majority-replicated elsewhere; new leaders elected from surviving ISR. No committed-message loss.

**Split-brain / fencing** — the monotonic **leader epoch** stamped in every batch + the majority requirement mean a partitioned-off old leader cannot commit or be acked; followers reject its stale epoch. Only a quorum-blessed leader makes progress.

---

## 9. Deliberate divergences from Kafka

| Kafka | aeronmq | Rationale |
|---|---|---|
| Relies on OS page cache warmth; 8 KB warm-search hack | **`mlock`/pin** active index + log tail; warm-search only for cold segments | Deterministic fault-free hot reads — what <100 µs demands. |
| `sendfile()` zero-copy to TCP | Batches laid out to match **Aeron term-buffer framing**; batch = replication fragment = archive unit | One framing end-to-end; no re-serialize across produce/replicate/fetch/archive. |
| JVM GC-avoidance contortions (forced unmap, KAFKA-4614) | **Arena/off-heap, no GC**; explicit mmap lifetimes | Sidesteps STW-pause class of problems entirely. |
| Always maintains TimeIndex | TimeIndex **optional per-topic**; derive time-retention from segment first-batch `baseTimestamp` | MMORPG traffic is offset-seek dominated; skip unused write cost. |
| Full Raft for data (via ISR + KRaft) is Kafka's mix already | Same split, but consensus impl is **C++ over Aeron**, not JVM | Single-language stack, Aeron mechanical sympathy. |

---

## 10. Build phasing

1. **Phase 1 — single-node log core:** segments, offset index (relative offsets, warm-search + pinned tail), record-batch v2 format (EOS fields reserved), producer/consumer API, retention, crash recovery (`recover()`-style). Proves storage + API.
2. **Phase 2 — control plane:** C++ Raft metadata quorum, membership, leadership assignment, metadata snapshots.
3. **Phase 3 — data-plane replication + self-healing:** ISR, high-watermark, leader failover, follower catch-up, recovery. **The heart — where <100 µs and HA are proven.**
4. **Phase 4 — consumer groups + log compaction** (background cleaner keeping latest value per key; Kafka `LogCleaner` as reference).
5. **Phase 5 — exactly-once / transactions:** idempotent producer (producerId + sequence dedup) first, then atomic multi-partition transactions with a transaction coordinator + control markers. Heaviest piece, built last.

---

## 11. Project structure, packaging & tooling

The workspace is **three sibling projects** under `~/Projects`, following the exact `world-server`/`common` conventions (Conan 2, CMake ≥3.31, C++23, `conan_provider.cmake`, `VERSION.txt` + `set_version`, packages published `name/x.y.z@<user>/dev` to the `artifactory` remote via Woodpecker CI). Splitting is minimal — only where it earns reuse or independent release.

```
~/Projects/
  rune-protocol/   # Conan library — shared wire + record-batch v2 format, offsets,
                   #   error codes, SBE-style codecs. Single source of truth for the
                   #   on-disk AND on-wire encoding. Released independently.
                   #   deps: common@<user>/dev (Agrona buffers), aeron
  rune-client/     # Conan library — standalone, INDEPENDENTLY RELEASABLE C++ client
                   #   (producer, consumer, consumer-group member, admin). The only
                   #   package downstream apps (e.g. world-server) consume.
                   #   deps: rune-protocol, common, aeron
  rune-broker/     # Conan application — the broker binary. Internally split into
                   #   CMake library targets, each independently unit-tested:
                   #     librune_storage      (segments, indexes, log, retention, recover)
                   #     librune_consensus    (C++ Raft metadata quorum, election)
                   #     librune_replication  (ISR, high-watermark, failover, catch-up)
                   #     librune_gateway      (Aeron ingress, session/auth, routing)
                   #     librune_archive      (Aeron Archive integration, snapshots)
                   #   deps: rune-protocol, common, aeron, gtest, benchmark, spdlog
```

**Per-project files (mirrored from `common`/`world-server`):** `conanfile.py`, `conandata.yml`, `CMakeLists.txt`, `conan_provider.cmake`, `VERSION.txt`, `build.sh`, `build_docker.sh` (broker only), `release.sh`, `update-deps.sh`, `bump_version.py`, `.woodpecker/`, `Dockerfile` (broker only), `include/`, `src/`, `tests/`, `benchmark/`, `docs/`.

**Aeron dependency:** `aeron/1.51.0` from ConanCenter (world-server already ships `aeron/1.41.4`). Builds cross-compile to `x86_64` on ARM Macs for Aeron compatibility, exactly as `world-server/build.sh` does.

**Dockerization:** `rune-broker` ships a `Dockerfile` (BuildKit, Artifactory build-args — same shape as world-server). A top-level `docker-compose.cluster.yml` brings up a **3-node Rune cluster** (+ Aeron media driver / Archive per node) for local runs and as the substrate the Testcontainers cluster tests drive.

**Publishing rule:** all `conan create/export/upload` and image pushes happen **only** through Woodpecker CI. The agent writes these scripts but never runs them locally.

---

## 12. Testing strategy (mandatory, first-class)

Testing is a hard requirement of every phase, not an afterthought. A phase is "done" only when its tests below are green.

**12.1 Unit tests — full coverage, all scenarios**
- Every pure component fully covered: segment append/roll/read, offset & time index (incl. **warm-section search** boundary cases, relative-offset overflow), record-batch v2 encode/decode + **CRC-32C** validation, `recover()` on truncated/torn/corrupt tails, Raft quorum state transitions, election timeouts, high-watermark advancement, producer-state/sequence dedup, retention & compaction key-selection.
- Deterministic (injected clocks, seeded RNG, in-memory fakes for Aeron/disk). Coverage gate enforced in CI (target: **100% of lines/branches on core libraries**; justified exclusions documented).
- Framework: `gtest` (as in world-server), coverage via `llvm-cov`/`gcovr`.

**12.2 Integration tests (single-node & pair)**
- Real Aeron media driver + real Aeron Archive on disk. End-to-end produce→replicate→ack→fetch→commit-offset→re-consume; retention deletes; compaction round-trips; cold restart replays Archive + snapshot and reproduces identical state (byte-for-byte log equivalence).
- Client library integration: `rune-client` against a live single broker — producer acks modes, consumer seek (earliest/latest/offset/timestamp), consumer-group join/rebalance/commit.

**12.3 Cluster tests via Testcontainers (the core of the mandate)**
- Driver spins up a **3-node (and 5-node) Rune cluster** from the Docker image using **Testcontainers**, plus `rune-client` driving real traffic.
- **Failure scenarios exercised, each with a state-assertion after recovery:**
  - Kill the **partition leader** mid-produce → assert failover elects an ISR member, **no committed message lost**, per-partition order intact, client transparently retries and continues.
  - Kill a **follower** → ISR shrinks, producer continues at required acks; follower **rejoins** → truncates to leader epoch, catches up, **re-enters ISR**; assert logs converge.
  - Kill the **control-plane leader** (Raft) → new metadata leader elected; assignments preserved.
  - **Network partition** (isolate minority) → minority cannot commit (fencing by leader epoch); on heal, stale leader steps down, no split-brain / no double-commit.
  - **Pause/stall** a node (SIGSTOP / clock skew) → detected as failed, healed; on resume it is fenced and re-syncs.
  - **Rolling restart** of all nodes → cluster stays available throughout; zero committed loss.
  - **Cold full-cluster restart** → recovers from Archive + snapshots; committed data and offsets intact.
  - **Under-replication repair** → a new node is assigned as follower and bootstraps to ISR.
- **Assertion framework:** after each scenario, a verifier reads every partition from every replica and asserts invariants: (1) **no committed-message loss**, (2) **strict per-partition/per-key order**, (3) **no split-brain double-commit**, (4) **replica log convergence** (identical committed prefix), (5) **consumer-group offsets monotonic & correct**, (6) **client observable behavior** (retries, metadata refresh, no duplicate delivery beyond configured semantics).
- A **linearizability/consistency check** (Knossos/Jepsen-style history checker) over the recorded client history validates ordering and no-loss under concurrency.

**12.4 Performance/latency tests**
- Bench harness (`benchmark`, as in world-server) gating **p99 < 100 µs** majority-durable produce, plus throughput and fetch-latency baselines. Enforced in CI from Phase 3; regressions fail the build.

**12.5 CI wiring (Woodpecker)**
- Per-project pipelines run unit + integration + coverage on every push; cluster (Testcontainers) + latency suites run on the broker pipeline (and nightly for the long chaos runs).

---

## 13. Open questions / to resolve during planning

- **Threading model per node:** one leader thread per partition vs. a sharded thread-per-core model multiplexing many partitions (likely the latter for scale — decide in Phase 1/3).
- **Aeron Archive granularity:** one recording per partition vs. shared recordings; impact on replay and retention deletes.
- **Replication factor default** and min-ISR policy (unclean-leader-election allowed? default **no**).
- **Segment/index sizing defaults** tuned for small MMORPG messages.
- **Testcontainers driver language:** the world ecosystem uses **py-testinfra** (Python) for C++ integration vs. real Redis/Postgres — reuse that Python+Testcontainers harness for Rune cluster tests, or a C++/gtest Testcontainers driver? (Lean Python for parity.)

---

## 14. Feature summary

**Messaging model:** topics; partitions; key→partition hashing with strict **per-key ordering**; batch-oriented append.
**Durability:** majority-in-memory commit (survives any single node); async Aeron Archive to disk; periodic snapshots; CRC-32C integrity; crash recovery with tail truncation.
**Availability & self-healing:** automatic leader failover, ISR shrink/grow, under-replication repair, leadership rebalance, leader-epoch fencing (no split-brain).
**Consumers:** consumer groups with coordinator-driven partition assignment + rebalance; offset commit in compacted `__offsets`; read from earliest/latest/offset/timestamp; high-watermark read isolation.
**Retention & compaction:** time/size retention via whole-segment delete; log compaction (latest-value-per-key) for state topics.
**Exactly-once (Phase 5):** idempotent producers (producerId/epoch/sequence, reserved in the batch header from day one); atomic multi-partition transactions.
**Performance:** p99 < 100 µs durable ack; Aeron busy-spin transport; single-writer per partition; pinned/`mlock`'d hot memory; hardware CRC; zero-copy Aeron-native framing end-to-end; no GC.
**Admin/ops:** topic create/delete/alter, partition/ISR/leadership introspection, configurable replication factor & min-ISR.
**Deployment:** single 3–5 node cluster, one datacenter (tolerates 1 of 3 / 2 of 5 failures); async off-site backup and multi-DC as later phases.
**Client library:** standalone, independently releasable `rune-client` Conan package (producer/consumer/group/admin) — the only artifact downstream apps consume.
**Packaging & tooling:** three Conan 2 projects (`rune-protocol`, `rune-client`, `rune-broker`) mirroring `world-server`/`common` conventions (C++23, CMake ≥3.31, `VERSION.txt`, `build.sh`/`release.sh`/`update-deps.sh`, Woodpecker CI); Aeron via ConanCenter.
**Dockerization:** broker `Dockerfile` + `docker-compose.cluster.yml` for a local 3-node cluster.
**Testing:** full-coverage unit tests; single/pair integration tests on real Aeron+Archive; **Testcontainers cluster chaos tests** (leader/follower/controller kills, partitions, stalls, rolling & cold restarts) with post-recovery state assertions (no loss, order, convergence, offsets, client behavior) + linearizability history checking; p99<100µs latency gates.
