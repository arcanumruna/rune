# Milestone D — Distributed Partition Leadership: Why / Why-Not (Rationale & Trade-offs)

> **STATUS: DECISION SUPPORT.** Not a design spec. Written to answer one
> question before we commit to an architecture: *should Rune distribute
> partition leadership at all, and if so, on what grounds?* Companion to the two
> idea-capture drafts
> ([`…-distributed-partition-leadership-DRAFT.md`], [`…-db-face-materialized-views-transactions-DRAFT.md`]).
> Once this is agreed, the design spec picks the election model and phasing.

## 0. What "distributed partition leadership" actually means here

Today (Milestone B/C) Rune runs one deliberate simplification: **the cluster
(consensus) leader owns *every* partition.** Partition-hash, per-partition
single-writer logs, ISR replication, and per-partition leader-epoch/truncation
(KIP-101) all already exist and work — but every partition's *leader role* is
pinned to the single node that won the cluster election. All produce traffic
funnels through that one node; the others are pure hot-standbys/read-replicas.

**Milestone D = let different partitions be led by different nodes**, assigned by
a controller and rebalanced on churn. It does **not** change the per-partition
data path (still single-writer log + ISR). It changes *who* is the writer for
*which* partition, and adds the authority that decides and durably records that.

The honest framing: **D is fundamentally a write-scale-out and
product-direction decision, not a per-message-latency decision.** Everything
below flows from that.

---

## 1. The core motivation — write horizontal scale-out

**Perspective: throughput / capacity.**

- A single write leader has a hard ceiling: one node's CPU, NIC, and disk
  bandwidth. Aggregate write throughput ≈ `batch × partitions × **1 node**`.
- The stated headline target is **100M+ msg/s**. That is not reachable on one
  writer at any realistic batch size — it is inherently a *many-writers* number.
  With distributed leadership, throughput ≈ `batch × partitions × **N nodes**`
  and scales roughly linearly with the cluster until the controller/metadata
  plane or the network fabric becomes the limit.
- Reads already scale (followers/read-replicas can serve fetches and, later,
  materialized-view queries). **Writes are the unscaled axis.** D is the piece
  that unlocks it.

**Verdict:** This is the whole reason D exists. If the product never needs more
write throughput than one node can sustain, D is not justified on throughput
grounds alone (see §11 alternatives). If it does — and the 100M target says it
does — D is not optional, only *timeable*.

---

## 2. Latency / the ultra-low-latency hot path

**Perspective: p99 latency, the <100µs hot-path gate.**

This is where D is most easily *misjudged*. Distribution does **not** make a
single write faster.

- **Per-partition hot path is unchanged.** A produce to partition P still hits
  one leader, one log append, one ISR ack. D doesn't touch that code; the
  <100µs per-op story is preserved *by construction* (the change is in the
  control plane, not the append path).
- **The routing hop:** a client must reach the node that leads P. Today it finds
  the single leader once and stays. Under D it must route per-partition. If the
  client routes correctly (cached partition→node map), there is **no extra
  hop** — it connects straight to P's leader, same as today. The cost appears
  only on a *stale* map (one REDIRECT round-trip, then re-cache) — a rare,
  self-correcting event, not a steady-state tax.
- **Tail-latency risk is real but bounded:** cross-partition operations that
  used to be node-local (a transaction spanning partitions, a future cross-shard
  view query) become cross-*node*. Those get slower. The mitigation is the
  co-partitioning discipline already in both drafts — keep consistency
  boundaries inside one partition so the common case stays local.

**Verdict:** D is latency-*neutral* for the single-partition happy path (the
90%+ case) and latency-*negative* only for genuinely cross-node operations,
which the design deliberately makes explicit and rare. It does not erode the ULL
story if co-partitioning is enforced.

---

## 3. Availability & fault tolerance / blast radius

**Perspective: what happens when a node dies.**

This is an under-appreciated *advantage* of D, independent of throughput.

- **Today:** the write leader is a single point of *capacity* — and while
  failover promotes a standby, during the failover window **the entire cluster's
  write path is down**, because one node was carrying all of it. Blast radius of
  a leader failure = 100% of writes.
- **Under D:** if a node leading 1/N of the partitions dies, only *its*
  partitions fail over; the other N-1 nodes keep taking writes for their
  partitions throughout. Blast radius of one node's failure ≈ `1/N` of writes,
  for the failover duration only.
- Rolling restarts, deploys, and hotspot isolation all improve for the same
  reason: work is spread, so losing one worker degrades rather than halts.

**Counterweight:** D *multiplies the number of independent failovers happening
at once* (see §6). The availability win is real, but it is paid for with a
harder correctness problem.

**Verdict:** Genuine availability improvement (smaller blast radius, no
whole-cluster write stall on single-node loss) — a reason to want D even at
moderate throughput.

---

## 4. The strategic unlock — materialized views / the DB face

**Perspective: product direction, not this quarter's throughput.**

This is arguably the *more important* "why" than raw msg/s.

- The materialized-view / DB-face vision (companion draft) is built on
  **co-partitioned view shards**: a view keyed by the partition key is a fold
  over that partition's log, co-located with the partition and following it. That
  only distributes — and only stays off the write hot path — **if partition
  leadership itself distributes.** Without D, every view shard collapses onto the
  one leader node: no scale, and the write leader now also carries all view
  maintenance and all view queries.
- Tier-1 "co-partitioned aggregate = local ACID" (the DB-face happy path) is only
  a *horizontal-scale* story if different aggregate roots live on different
  nodes. That is exactly what D provides. Without D, "local ACID" is real but
  caps at one node — i.e. it's just an embedded DB, not a distributed one.
- Distributed coordinators (`__consumer_offsets`, `__transaction_state` placed by
  hash) — also gated on D — are what stop the coordinator plane from being a
  single-node bottleneck as consumer-group and EOS traffic grow.

**Verdict:** D is the *foundation* for everything in the DB-face roadmap. Even if
throughput weren't pressing, choosing to pursue the materialized-view/DB
direction makes D a prerequisite, not a nice-to-have. Conversely, if that
direction is shelved, one major justification for D weakens.

---

## 5. Resource efficiency / cost

**Perspective: $ per unit of work.**

- **Today:** N nodes, but only 1 does write work. The standbys burn money as
  insurance. Effective write utilization ≈ `1/N`.
- **Under D:** all N nodes do write work; the cluster's cost curve tracks its
  capacity curve. You buy hardware you actually use.
- Nuance: followers still replicate (they aren't idle even today), and D adds
  control-plane overhead. But the step change from "1 of N working" to "N of N
  working" dominates.

**Verdict:** Clear efficiency win once you're running more than a couple of
nodes; negligible for a 1–2 node deployment.

---

## 6. Correctness & consistency — the cost side

**Perspective: what gets harder / riskier.**

This is the principal *disadvantage* and deserves equal weight.

- **The split-brain / fencing surface multiplies.** All the DR hardening (epoch
  lineage, no-committed-loss handoff, deferred fencing, on-lineage survivor
  detection) was proven for *one* leadership. Under D there are up to
  `partitions` independent leaderships failing over, rebalancing, and fencing
  *concurrently* — the DR-10/11 scenarios multiplied by partition count. Each is
  individually solved by existing per-partition machinery, but the *emergent*
  behaviour (many at once, controller-driven) is new test surface.
- **Cross-partition atomicity stops being free.** Today, because one node leads
  everything, a multi-partition operation is node-local. Under D it spans nodes,
  so it needs the transaction-tier ladder (EOS markers / saga / 2PC) to stay
  correct. This is on the roadmap anyway, but D is what *forces* it from "nice"
  to "required."
- **A new consistency-critical component:** the replicated metadata state machine
  holding the partition→leader map is now on the critical path for correctness.
  A bug there can mis-assign a leader (two writers → divergence) — precisely the
  failure the single-leader model made impossible by construction.

**Verdict:** D trades a *simple, provably-single-writer* invariant for a
*distributed-assignment* invariant that must be enforced by consensus + epoch
fencing. The building blocks exist; the combined system is materially harder to
reason about and test.

---

## 7. Operational complexity

**Perspective: running it in production.**

- New concepts operators must understand: the assignment map, rebalancing
  policy, preferred leaders, skew/hotspot handling, per-partition lag/ISR.
- Rebalancing is a new *source of churn*: node join/leave now moves leadership
  and triggers follower catch-up; a bad policy causes thrash.
- Observability must go per-partition (which node leads what, per-partition ISR,
  per-partition lag) instead of one cluster-leader dashboard.
- **Counter:** this is well-trodden (Kafka/Redpanda operators live here daily),
  and Rune can adopt their proven heuristics rather than invent.

**Verdict:** A real, ongoing complexity tax — mitigated by following prior art
and by shipping static assignment before dynamic rebalancing (§11).

---

## 8. Client complexity

**Perspective: what the client library must now do.**

- Clients must maintain a partition→leader map, route per-partition, and refresh
  on REDIRECT / epoch change. Today they find one leader and stay.
- A `Metadata` request must exist (it does not today; discovery is via the
  REDIRECT loop). The producer partitioner already computes the partition; D adds
  the partition→node hop.
- **Counter:** the REDIRECT fallback already exists, so a naive client keeps
  working (correctness without the cache; just extra redirects). The fast path
  (cached map) is an optimization layered on a working fallback — graceful.

**Verdict:** Moderate, additive client work with a working fallback; not a
blocker.

---

## 9. Engineering cost, risk, and sequencing

**Perspective: what it takes to build, and when.**

- **Reuse is high.** Reconnaissance confirms the per-partition machinery is
  already present: `PartitionReplica::becomeLeader(epoch, isr)` takes a
  per-partition epoch + ISR; `IsrManager`, `LeaderEpochCache`, `ReplicaRegistry`
  are per-partition; coordinator placement has explicit `Milestone-D` seams. The
  single-leader assumption is concentrated in **two** spots
  (`ReplicationMetadataBridge::doWork()` broadcasting one epoch/ISR to all, and
  the single-boolean `leaderViewFn`).
- **The one genuinely new build:** a **replicated metadata state machine** on top
  of the elections-only Phase-2 consensus (there is no durable topology log
  today). This is the load-bearing, highest-risk piece.
- **Sequencing vs Milestone C:** C (consumer groups + EOS) is being built against
  "cluster leader = sole coordinator." D generalizes coordinator placement to
  hash-based. The seams are already stubbed, so C-then-D is viable, but D will
  *fill in* those seams — they must not calcify. (User direction 2026-09-15: D is
  its own milestone.)

**Verdict:** Lower than it looks on the *data* path (machinery exists), higher
than it looks on the *control* plane (new consensus-backed state machine + the
multiplied-failover test matrix). Risk is concentrated and identifiable.

---

## 10. Competitive positioning

**Perspective: what Rune is vs the field.**

- Distributed partition leadership is *table stakes* for anything claiming to be
  a scalable log/broker — Kafka, Redpanda, Pulsar all have it. A single-write-leader
  broker reads as "not horizontally scalable for writes," which caps Rune's
  addressable claims regardless of its latency wins.
- Rune's differentiator is ULL + the DB face. D is what lets the ULL story hold
  *at scale* (not just on a benchmark box) and is the substrate for the DB face.
  Without D, Rune is "a very fast single-node broker" — a real but narrow niche.

**Verdict:** D moves Rune from "fast single-node" to "fast *and* scalable,"
which is the category the roadmap is aiming at.

---

## 11. Alternatives to doing D (the honest "why-not")

1. **Do nothing — stay single-write-leader.** Cheapest, simplest, keeps the
   provably-single-writer invariant and all current DR proofs intact. Ceiling:
   one node's write capacity; no distributed DB face; standbys stay idle.
   *Rational if the real workload never exceeds one node and the DB-face
   direction is shelved.*
2. **Vertical scale + intra-node partition parallelism.** Push one leader node
   harder (more cores, faster NIC/NVMe, more partitions on the same node). Buys
   headroom without distributed-leadership complexity, but has a hard ceiling and
   still leaves N-1 nodes idle for writes.
3. **Read-replica scale-out only.** Distribute *reads*/view queries to followers
   while keeping one write leader. Helps the query side of the DB face but does
   nothing for write throughput or write blast radius — a partial measure.
4. **Full per-partition Raft (the heavier D).** Maximal isolation, no shared
   controller, but N consensus groups and a departure from Rune's
   one-consensus-group + ISR-replication architecture and the Aeron-Cluster
   faithful-port ethos. More engine, more failure modes.
5. **Controller-assigns D (the lighter, faithful D).** One metadata consensus
   group becomes a real replicated state machine holding the assignment map;
   ISR-based replication unchanged. Reuses existing machinery; matches Aeron
   Cluster. *This is the front-runner if we do D.*

---

## 12. Consolidated view

| Dimension | With Distributed Leadership (D) | Staying Single-Leader |
|---|---|---|
| Write throughput | Scales ~linearly with nodes (100M target reachable) | Capped at one node |
| Single-op latency (hot path) | Unchanged (<100µs preserved) | Unchanged |
| Cross-node op latency | Worse (mitigated by co-partitioning) | N/A (all node-local) |
| Availability / blast radius | Node loss = ~1/N writes, briefly | Node loss = 100% writes, briefly |
| Resource efficiency | N-of-N nodes do write work | 1-of-N; standbys idle |
| Materialized-view / DB face | **Enabled** (co-partitioned shards distribute) | Collapses onto one node |
| Correctness surface | Multiplied failover/fencing; new consensus-backed map | Provably single-writer; simplest |
| Operational complexity | Higher (assignment, rebalancing, per-partition obs) | Lowest |
| Client complexity | Per-partition routing + Metadata (REDIRECT fallback) | Find one leader, stay |
| Engineering cost | Data path: low (reuse). Control plane: high (new state machine) | Zero |
| Competitive position | "Fast *and* scalable" | "Fast single-node" |

---

## 13. Recommendation

**Do D — but justify it on the DB-face unlock and availability first, throughput
second, and time it deliberately.**

- The throughput argument alone (100M msg/s) is sufficient *eventually* but may
  not be *urgent* if current single-leader throughput meets today's real
  workload. Don't build D purely to chase a benchmark number the product isn't
  yet demanding.
- The **strategic** arguments are the stronger "why now": D is the hard
  prerequisite for the materialized-view / DB-face direction (§4) and it removes
  the whole-cluster-write-stall blast radius (§3). If that direction is the
  product's future, D is on the critical path regardless of current throughput.
- If we proceed, take the **controller-assigns / replicated-metadata-state-machine**
  path (§11.5) — faithful to Aeron Cluster, maximal reuse of existing
  per-partition machinery, one consensus group.
- **De-risk by phasing:** (a) build the replicated metadata state machine +
  *static* partition→leader assignment (no rebalancing) and prove multi-leader
  parallel produce; (b) add dynamic rebalancing + preferred-leader/skew policy;
  (c) generalize coordinator placement (fills the C seams); (d) the multiplied
  failover/fencing DR-E2E matrix. Each phase is independently valuable and
  testable.

## 14. The decision this document asks for

1. **Is the materialized-view / DB-face direction confirmed as Rune's future?**
   (If yes, D is a prerequisite and the recommendation stands. If no, revisit
   whether alternatives §11.1–3 suffice for a pure-broker product.)
2. **Is current single-leader write throughput a real constraint today, or a
   future one?** (Sets urgency, not direction.)
3. **Given 1–2, do we commit to D now** and move to the election-model decision
   (controller-assigns vs per-partition Raft) and the phased design spec?
