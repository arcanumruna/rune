# Rune Client + Broker Runtime — Design Spec

**Date:** 2026-09-11
**Status:** Approved (design); pending implementation plan
**Scope of this spec:** the first client-facing sub-project — a runnable, clustered `rune-broker` node with a client-facing protocol endpoint, and a **C++ client** that talks to it. Node.js client and the materialized-view / distributed-DB vision are **deferred** and captured in *Future Directions* so this design does not preclude them.

---

## 1. Context & problem

Rune Phases 1–5 built the broker **engine** as tested libraries — `libs/storage` (durable LE log, compaction), `libs/consensus` (Raft metadata + leader election over real Aeron), `libs/replication` (ISR, failover), `libs/coordination` (consumer groups + offsets), `libs/transaction` (exactly-once). What does **not** exist yet:

- an assembled, runnable broker **process** (`src/main.cpp` is a 2-line stub);
- a **client-facing protocol** (all SBE schemas 111–114 are internal; the only Aeron traffic is node↔node consensus);
- any **client library**.

This sub-project stands up the missing **front door** and a first client. It does **not** re-open the engine.

### Goals (v1)
- A `rune-broker` executable that boots the engine + an embedded Aeron media driver, joins a consensus cluster, and exposes a client-facing Aeron endpoint (the **ClientGateway**).
- A **Docker** image + `docker-compose` **N-node cluster** (default 3) with leader election, over `aeron:udp`.
- A **client-facing wire protocol** (little-endian, versioned) covering: session connect/keepalive/resume, ping, produce, and push-subscribe.
- A **C++ client** library (real Aeron C++ client, embedded media driver) with an idiomatic API, exercised end-to-end against the cluster.
- A **language-agnostic conformance suite** (protocol test-vectors) that the C++ client passes — the artifact the future Node client will reuse to prove parity.

### Non-goals (deferred — see §9)
Node.js client · direct-to-partition-leader client routing (Option 2) · the materialized-view / stream-processing / DDL layer · distributed/cross-shard transactions · dynamic membership, TLS, auth · NATS-style subject wildcards · the ephemeral high-frequency (position) tier.

### Milestones
- **A — transport + protocol parity:** client connects over Aeron to the gateway, session + `ping` round-trip. Proves the C++ Aeron client, the wire framing, and (later) C++/Node parity, with no engine coupling.
- **B — produce → push-subscribe** through the gateway into the real engine (`PartitionApi`/`PartitionReplica`).
- **C — consumer-group consume + offset commit** (reuses Phase 4). *(C may slip to a follow-on.)*

---

## 2. Architecture

Three artifacts; the **protocol is the only coupling** between client and broker, and the **ClientGateway is the only component that speaks it** (engine libs stay transport-agnostic and unit-testable as today).

```
                         ┌──────────────── rune-broker node (×N, dockerized) ─────────────────┐
   C++ client            │  ClientGateway  (Aeron ingress/egress, sessions, protocol)         │
  ┌──────────┐  aeron:udp│      │  proxies → engine libs:                                       │
  │ session  │══════════▶│  ────┴──── storage · consensus(leader) · replication(ISR) ·        │
  │ pub/sub  │◀══════════│            coordination(groups/offsets) · transaction(EOS)          │
  │ embedded │  egress   │  MediaDriver (embedded) ── consensus over aeron:udp to peers ◀────▶ │
  │  driver  │           └────────────────────────────────────────────────────────────────────┘
  └──────────┘                      docker-compose: N nodes, leader elected via P2 consensus
```

**Connection model = Option 3 (gateway-proxy):** the client holds **one** Aeron duplex (Publication=ingress, Subscription=egress) to **any** node's gateway; that node proxies data ops to the partition leader internally. Rationale over the latency-optimal Option 2 (direct-to-leader): Option 3 yields a correct end-to-end with a ~1-connection client and keeps the hardest client machinery (cluster-metadata cache, N-connection management, per-partition leader-failover routing, client-side partitioning) out of v1 — machinery we'd otherwise also have to reimplement in Node. The protocol carries **explicit partition addressing**, so evolving specific/server-side clients to direct-to-leader later is **additive, not a rewrite**. A gateway/proxy node is a permanent, legitimate deployment mode (thin/game clients want one endpoint).

---

## 3. Broker node runtime + cluster / Docker topology

### 3.1 The node process (replaces the stub `main`)
Assembles, in order: embedded **Aeron media driver** → **ConsensusAgent** (P2 Aeron-Cluster port, over `aeron:udp`) → **engine** (per-partition `Log` + `PartitionReplica` + coordination + transaction) → **ClientGateway**.

**Duty cycle (faithful to the single-writer / no-busy-spin rule):** the gateway polls the client ingress `Subscription` → decodes a request → hands it to the engine via an **Agrona ring buffer** (lock-free) → the engine's single-writer `doWork` processes it → the response goes out on the client egress `Publication`. Consensus runs its agent in the same duty-cycle under an `IdleStrategy`. No new threading model — the gateway is one more poll source on the existing agent pattern.

### 3.2 Cluster formation & leadership
- **Static membership** in v1 (peer list from config/env). Consensus elects a leader over `aeron:udp`; partition→leader metadata derives from consensus.
- Any node's gateway accepts client sessions; **data ops proxy to the partition leader**; **session/keepalive handled locally**.

### 3.3 Docker
- `Dockerfile` builds `rune-broker` (+ its Aeron dependency), bundles the media driver; entrypoint reads env: `RUNE_NODE_ID`, `RUNE_PEERS` (peer DNS+ports), `RUNE_CLIENT_ENDPOINT`, `RUNE_DATA_DIR`.
- `docker-compose.yml`: **3 nodes** on a docker network, each with a data volume and its client UDP port exposed; readiness = "joined consensus + knows the leader".
- **Container realities to design for:** (1) inter-container Aeron is **`aeron:udp` unicast** to peer DNS names (default bridge lacks multicast) — the P2 channel config already abstracts this; (2) the media driver needs adequate **`/dev/shm`** and a sane **MTU** — set in compose.

---

## 4. Client-facing protocol

### 4.1 Encoding & header
Flat **little-endian** frames (consistent with Rune's all-LE decision), reusable zero-alloc encoder/decoder instances, written zero-copy via Aeron `tryClaim`. Fixed header on every message:

```
blockLength · templateId · schemaId · version · sessionId · correlationId
```

`schemaId`+`version` make the protocol **versioned from day one** (unknown template/version → clean `ErrorResponse`, never a mis-parse). This is what lets consumer-group / txn / view ops be added later without breaking a bare client.

### 4.2 Session model (Aeron-Cluster-client style)
- **Connect:** client offers `ConnectRequest` on ingress carrying the **egress channel+streamId it wants replies on** (its return address). The gateway assigns a `sessionId`, replies `ConnectResponse` — or `Redirect{leaderEndpoint}` (used now only when a node can't serve control; the same mechanism becomes direct-to-leader routing later).
- **Keepalive:** periodic client heartbeat; gateway expires idle sessions (injected-clock/timeout discipline).
- **Resume:** Aeron is link-reliable but **not durable across disconnect**, so on reconnect the client re-presents its `sessionId` + **last-consumed offsets**; end-to-end durability = broker log + those offsets, not the wire.

### 4.3 v1 operation set
- *Control:* `Connect`/`ConnectResponse`, `Keepalive`, `Disconnect`, `ErrorResponse{code}` — codes include `NOT_LEADER`/`REDIRECT` reserved now for the future direct-to-leader path.
- *Ping* (Milestone A): trivial round-trip proving transport + framing + client parity.
- *Produce* (request/response): `Produce{topicId, partition|key, records}` → `ProduceResponse{baseOffset}`.
- *Subscribe* (**push**): `Subscribe{topicId, partition|key, fromOffset, isolation}` → the gateway **pushes** `Records` on egress as they arrive; the broker paces each subscription against the client's egress back-pressure (`offer()` BACK_PRESSURED → pause + resume from the tracked offset). Push (not poll) is the MMORPG-native fit and simplest for a bare client.

### 4.4 Addressing, versioning, future-proofing
- Data messages carry **`topicId` + `partition`** explicitly. The client may send a **key** and let the v1 gateway resolve the partition, but the partition field exists → Option 2 needs no wire change.
- **Reserved `templateId` ranges:** control **1–15** · produce/consume **16–31** · consumer-group/txn **32–63** · view define/query/watch **64–95**. Consumer groups, EOS, and the whole view/DDL layer slot in additively.

---

## 5. Addressing model (topics vs keys vs partitions vs subjects)

The single most important modeling decision. Three **distinct** things:

| Concept | What it is | Cardinality | Example |
|---|---|---|---|
| **Topic** | a named durable stream (numeric `topicId` on the wire) | **few** (tens–hundreds) | `inventory`, `entities`, `chat` |
| **Key** | the **binary** hash/routing/ordering key *inside* a topic | **unbounded** | `accountId`, `zoneId`, `playerId` |
| **Partition** | `hash(key) % N` — scaling + per-key ordering unit | tens–thousands / topic | `inventory` p7 |

**There is never a topic per entity.** `topic="inventory", key=accountId` — one topic, N partitions; account 1's records land ordered in `partition = hash(accountId) % N`. Millions of *keys* in a topic are free; millions of *topics* are ruin (per-topic replication/file/coordination cost).

**Keys are opaque bytes, not strings** (avoids the Redis string-key/serialization tax):
- `Key` accepts `uint64`/`int64`, a byte span, or **any POD struct** (hashed as its bytes); a string only when a string is the natural id.
- **No prefix** — the topic namespaces the key space; a composite dimension is a POD struct, never a concatenated string.
- **Hot path carries `topicId · partition · keyBytes · value`** — no strings per message; key→partition is a cheap `xxhash` over 8–16 bytes; keys are stored/compared as bytes (byte-equality + hash) for compaction/dedup/future joins.

**"Subjects" (`entities/zone:42`) are ergonomic sugar over `(topic, key)`**, resolved **once at subscribe time** — not a physical stream per subject.

**Modeling principle:** *partition/key by your interest & consistency boundary* — the key is the interest unit, the ordering unit, and the future join/consistency unit (the aggregate-root principle). Coarse interest = topic/partition(+key) subscribe (v1); fine per-key **watch** = a hash-view watch keyed by that key (future view layer).

---

## 6. C++ client architecture

### 6.1 Layers (each independently testable)
- **Codec** — the §4 LE encoders/decoders (shared definitions with the broker; zero-alloc). Pure → tested with byte-vectors.
- **Transport** — Aeron duplex (one `Publication` ingress, one `Subscription` egress, a `FragmentAssembler`) + **embedded media driver** in-process. Knows only "send/receive frame."
- **Session** — state machine `Disconnected → Connecting → Connected → (Resuming) → Closed`; owns `sessionId`, keepalive, redirect handling, per-subscription offset tracking.
- **Public API** — the idiomatic surface (§6.3–6.4).

### 6.2 Threading & duty cycle
One internal **client agent** runs `doWork(nowNs)` under an `IdleStrategy` (poll egress, dispatch, keepalive, time out pending requests). App→client calls are handed in over an Agrona ring (lock-free) → the public API is thread-safe while the poll loop stays single-writer. Handlers fire on the agent thread (documented); a **manual-`poll()` mode** lets users own the loop (game main loop / fiber).

### 6.3 Consumer — imperative push-handler core (zero-cost), optional reactive facade
```cpp
auto client = rune::Client::connect({ .bootstrap = { "node1:40123", "node2:40123" },
                                      .embeddedMediaDriver = true });

auto entities = client.topic("entities");   // name → numeric handle, resolved ONCE

// PUSH, zero-copy handler on the agent thread
auto sub = client.subscribe(entities, rune::Key(zoneId),
    { .from = rune::From::Latest, .isolation = rune::Isolation::ReadCommitted },
    [](const rune::Record& r){ handleEntityEvent(r.key(), r.value(), r.offset()); });
sub.unsubscribe();

// Manual-threading mode (own the loop):
auto c = rune::Client::connect(cfg, rune::Threading::Manual);
while (running) { int work = c.poll(); idle.idle(work); }   // no busy-spin
```
An **optional** Reactor/Rx-style facade (`client.stream(entities, Key(zoneId)).filter(...).subscribe(...)`) layers on top for ergonomics; never in the hot path; the Node mirror gets it free via RxJS / async-iterators. Subscription back-pressure borrows Reactive-Streams `request(n)` demand (default: broker-paced push).

### 6.4 Producer — three tiers (§7 details semantics)
```cpp
auto p     = client.producer({ .acks = rune::Acks::Leader, .linger = 200us, .batchBytes = 64_KiB });
auto inv   = client.topic("inventory");        // topic handles are client-level, usable by produce + subscribe
auto fills = client.topic("exchange.fills");

// Tier 1 — default: fire-and-forget, auto-batched, idempotent (95% path)
p.send(inv, rune::Key(accountId), delta);                    // returns void, no per-call alloc
p.send(inv, rune::Key(accountId), delta, [](auto r){ ... }); // opt-in ack callback
auto off = p.send(inv, rune::Key(accountId), delta, rune::want_future).get();

// Tier 2 — explicit atomic batch (same partition, one RecordBatch)
auto b = p.batch(inv, rune::Key(accountId)); b.add(v1); b.add(v2); auto base = b.send().get();

// Tier 3 — transaction (cross-partition/topic, RAII auto-abort)
auto tp = client.producer({ .transactionalId = "match-worker-7" });   // InitProducerId, zombie-fenced
{
    auto txn = tp.begin();
    txn.send(inv,  rune::Key(sellerId), debit);
    txn.send(inv,  rune::Key(buyerId),  credit);
    txn.send(fills, rune::Key(fillId),  fill);
    txn.sendOffsets(group, {{ tpIn, consumedOffset }});   // read-process-write (P5 addOffsetsToTxn)
    txn.commit();                                          // markers; atomic under read_committed
}   // uncommitted scope-exit → dtor aborts → nothing visible
```

---

## 7. Producer semantics

- **Two levels of atomicity, different cost:** a single **RecordBatch to one partition is already atomic** (one log append) — use `p.batch()` for same-partition all-or-nothing, *no transaction needed*. A **transaction** is for atomicity **across** partitions/topics and to commit consumer offsets with the writes (P5 EOS) — more expensive (markers + coordinator).
- **Auto-batching (Tier 1):** `send()` accumulates into a per-`(topic,partition)` `RecordBatchBuilder`; a background flush ships on `linger` (max wait) OR `batchBytes` (max size); `flush()` forces; `maxInFlight` bounds outstanding batches. This is the low-overhead, low-verbosity default.
- **Idempotence on by default** (producerId+epoch+sequence, P5-1) → background retries never duplicate, even without transactions.
- **`acks`:** `None` (fastest) · **`Leader` (default for plain producers)** · `All` (waits for ISR-majority high-watermark, P3 path). Opt into acks/offsets **per-send** so the fast path stays alloc-free. **Idempotent and transactional producers use `acks=All` implicitly** — EOS dedup/atomicity across leader failover requires the record to be majority-durable before it is acknowledged; the `Leader` default applies to non-idempotent producers only.
- Sends still batch per partition **inside** a transaction; `commit()` flushes then writes markers.

---

## 8. Testing strategy

- **Unit:** codec vs byte-vectors; session state-machine + producer batching/txn vs a fake transport; gateway routing vs a fake engine. Bounded, no-spin.
- **Conformance suite:** language-agnostic protocol test-vectors (frames + expected decodes + session-flow scripts) — the C++ client must pass; **reused by the future Node client** to prove parity.
- **In-process integration:** C++ client ↔ broker over Aeron **IPC loopback** — produce / subscribe / batch / txn round-trips, deterministic.
- **Cluster E2E (dockerized):** `compose up` 3 nodes → connect → produce→subscribe across nodes; **leader-failover from the client's POV** (kill the leader container → client resumes from tracked offsets, no committed loss, `read_committed` stays all-or-nothing) — reusing the P3/P5 disaster-recovery ethos.
- **Benchmarks:** produce latency (`acks=Leader`), subscribe delivery latency, batch throughput.

---

## 9. Future Directions (captured, not built)

- **Node.js client** — mirror via the conformance suite; re-implement the Aeron client (media-driver shared-memory protocol); RxJS / async-iterator reactive facade.
- **Option 2 direct-to-partition-leader** — protocol already shaped for it (explicit partition addressing, `NOT_LEADER`/`REDIRECT`); adds metadata discovery + multi-connection + client-side failover when latency demands it.
- **Materialized-view / stream-processing layer** — KStream/KTable-style DDL; **hash vs table** views; placement knob (`local` / `partitioned` / `distributed`); durability knob (`volatile` / `persistent` / `transactional`); versioned view schema; **FK-integrity-as-dataflow** (a joined view materializes a row only when both sides resolve). Two archetypes stay distinct: the *bare client* is the I/O API (Kafka-consumer/librdkafka/Aeron/CoralReactor lineage); the *view layer* is the processing/topology API (Kafka-Streams/Flink/Reactor lineage). Per-key `watch` lands here.
- **Distributed / cross-shard transactions & the DB face** — aggregate-root co-partitioning (`auth ← character ← inventory` as one local-ACID island; create-character returns at Raft **apply** time with read-your-writes, mirroring Aeron Cluster's session response); `exchange_fills` cross-aggregate → **saga default, 2PC only where mandatory**; secondary-index hash views for lookups off the non-partition key. The honest constraint: ultra-low latency + cross-shard ACID + global FK cannot all three hold for arbitrary references — the design keeps ~90% of transactions/FK checks **local** and makes cross-boundary consistency an explicit, costed per-relationship choice.
- **Interest tier & AoI** — NATS-style subject wildcards (`entities/zone:42.>`); a separate best-effort ephemeral fan-out for high-frequency transient signals (positions) that shouldn't pay the durable+EOS cost (the `VOLATILE` vs `DURABLE` knob at the transport tier).
- **Ops/security** — dynamic cluster membership, TLS, auth/authz.

---

## 10. Open decisions (to settle in the implementation plan)

- **Repo layout:** protocol message schema in `rune-protocol` (sibling-header pattern, already used for `RecordBatch`); C++ client as a new `libs/client` in `rune-broker` for v1 (co-located, shares the codec), extractable to a `rune-client` repo later. *(Recommended; confirm in planning.)*
- **v1 partition count / topic bootstrap:** start with a small static topic+partition config to prove the path; dynamic topic creation is a follow-on.
- **Milestone C scope:** whether consumer-group consume + offset commit is in the first plan or a follow-on.
