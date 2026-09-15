# Rune DB Face — Materialized Views + Transaction Strengths (over distributed partitions)

> **STATUS: DRAFT / IDEA CAPTURE.** Not reviewed, not planned, not scheduled.
> Records the design discussion of 2026-09-15. Do NOT implement from this — it
> needs a full brainstorming + spec pass. Companion to
> [`2026-09-15-rune-milestone-d-distributed-partition-leadership-DRAFT.md`]
> (materialized-view placement rides on partition-leadership distribution).

## 1. Materialized views over distributed partitions

**Principle: co-partitioning.** A materialized view is a fold over a partition's
log. If a view is keyed by the same key that decides partition placement
(`hash(key) → partition`), then each partition `P` has a **view-shard** `V_P`
built by tailing `P`'s own log, co-located on the node hosting `P` (leader or a
chosen read-replica). When Milestone D distributes partition leadership, view
shards distribute *with* their partitions automatically.

Consequences:
- View maintenance stays **local and off the write hot path** — each shard folds
  only its own log, no cross-node coordination to build the view.
- Build/query can be pinned to a **follower/read-replica** of `P` to keep query
  load off the write leader. MVCC / COW snapshots give lock-free consistent reads.
- Placement modes (from §9 of the runtime-design spec): `local` (single node),
  `partitioned` (co-partitioned shards, the default under Milestone D),
  `distributed` (explicitly spread). Durability modes: `volatile` /
  `persistent` / `transactional`.

### Query regimes
1. **Point lookup by view-key (DB fast path):** `get(K) → hash(K) → P → node
   hosting V_P → local read`. Single hop, same routing Milestone D gives the
   producer; scales linearly with nodes.
2. **Cross-key queries (range / global aggregation / secondary index):**
   **scatter-gather** — fan out to all shards, push partial aggregation *down* to
   each shard, coordinator merges (map-reduce). Run on read-replicas so it never
   touches write leaders. Cost = slowest shard + merge.

### Joins
- **Co-partitioned join** (same key, same partition count): fully local, no network.
  The golden rule — model keys so joins are local.
- **Small × large:** broadcast the small side as a **global view** (full replica on
  every node, à la Kafka GlobalKTable) → local join.
- **Large × large, differently keyed:** requires a **repartition/shuffle** via an
  internal repartition topic (re-key so both become co-partitioned). The expensive
  case — should be explicit in the API, never implicit.

### View durability & failover
A view shard is **derived state** — always rebuildable by replaying its
partition's log from the last checkpoint offset. Milestone D leadership handoff
just re-hosts the shard: warm if it was built on a follower, else
replay-the-tail-from-checkpoint. Group-committed view fsync + checkpoint offset =
fast recovery.

## 2. Transaction strength ladder (avoid becoming CockroachDB by construction)

The strategy: keep ~95% of transactions in Tier 1 via co-partitioning, so general
distributed-SQL machinery is never needed.

**Tier 1 — Single-partition / co-partitioned (local ACID). ← lean hard here.**
Co-partition an *aggregate root* (everything a txn touches shares the partition
key → one partition → one node). The partition log is already a total order, so
you get serializable ACID **for free**: no clocks, no timestamp oracle, no
conflict detection. Scales horizontally (different aggregate roots → different
nodes). This is Megastore entity-groups / VoltDB single-partition / DynamoDB
single-partition transactions.

**Tier 2 — Kafka-style multi-partition atomic append / EOS (Phase 5, on roadmap).**
`TransactionCoordinator` + control markers give atomic *append* across partitions
+ atomic offset commit (consume-transform-produce exactly-once). Already
distributed (markers written to partitions led by different nodes under D), cheap
because it's append-only with no read-conflict detection.

**Tier 3 — Genuine cross-shard business txn (read-modify-write across nodes).**
Opt-in only:
- **Saga (default):** per-shard local (Tier-1) txns + compensating actions.
  Eventually consistent, no distributed locks, maximally scalable; app owns
  compensation.
- **2PC (where mandatory):** two-phase commit across the involved partition
  *leaders*. Tractable **because each participant is already a replicated
  Raft/ISR group** — a "prepared" record is never lost to a crash (same
  foundation Spanner/CockroachDB rely on; reuses DR-epic durability). Caveat: the
  classic coordinator-failure blocking window, mitigated (not eliminated) by
  making coordinator state a replicated log (like `__transaction_state`).

**Tier 4 — Full CockroachDB/Spanner (general serializable distributed SQL).**
MVCC write intents + per-txn transaction record + distributed conflict resolution
+ hybrid-logical-clocks / TrueTime to order txns across shards without a single
log. **A different product.** Only pursue on an explicit pivot to "general
distributed SQL"; do not reinvent it by accident.

**Why we can stop at Tier 3:** Cockroach needs Tier-4 machinery for the
cross-shard *ordering* problem (no single log → needs synchronized clocks). Rune
sidesteps it: Tier 1 uses the partition log's total order (free); Tier 2 is
append-only (no conflicts); Tier 3 gives *atomicity* where needed but deliberately
does **not** promise cross-shard serializable snapshot isolation. The only thing
given up vs Cockroach is cross-shard serializable snapshot reads / RMW — an
acceptable trade for a streaming + DB-face system that asks apps to co-partition
their consistency boundaries (good modeling anyway).

## 3. View-update consistency strengths (per-transaction / per-view)

Two first-class modes controlling whether the materialized-view apply is on the
commit critical path. Orthogonal to the Tier ladder above, but their **cost
depends on view placement** (local vs remote).

### Mode SYNC — view-consistent commit (read-your-writes)
Commit does **not** ack until the derived view(s) have applied this txn's
mutations (and fsync'd if the view is `persistent`/`transactional`). After the ack,
any query against the view reflects the transaction.
- **Atomicity:** view apply + log commit are one atomic unit (group-commit +
  `ViewApplyCallback` on the commit marker). Abort ⇒ neither is visible.
- **Enables constraint/uniqueness enforcement:** apply-time predicate validation
  in the view yields a deterministic verdict that can **abort** the commit
  (conditional / constraint transactions).
- **Use when:** the app reads the view right after writing and must see its own
  write ("create order → immediately query order-summary view"); invariants
  enforced in the view.
- **Cost:** commit latency includes view apply; if the view is remote, includes a
  cross-node round-trip (Tier-3 / 2PC-class coordination).

### Mode ASYNC — commit-first (eventual view)
Commit acks on **log durability alone**; the view is updated afterward by the
downstream log-tailer, bounded-lag eventually consistent.
- **Atomicity:** the log commit is authoritative; the view is derived and converges
  by replay. A crash mid-apply just re-applies from checkpoint.
- **Use when:** view feeds analytics / dashboards / search where slight staleness
  is fine; maximum write throughput.
- **Cost:** minimal commit latency; view may lag by the tailer's cadence.

### (Optional) Mode SESSION — read-your-writes without blocking [YAGNI-flagged]
Commit acks immediately (async apply) but returns a **view-watermark token**. A
later view query either routes to a shard/replica that has applied ≥ token, or
briefly waits for it. Gives per-session read-your-writes without paying SYNC
commit latency (causal / session-guarantee consistency). Add only if SYNC latency
proves too costly and ASYNC too weak.

### Cost matrix (mode × placement)

| | Co-partitioned / local view | Distributed / remote view |
|---|---|---|
| **SYNC**  | cheap — local atomic apply in the same commit | expensive — cross-node wait (2PC-class) |
| **ASYNC** | cheap | cheap |

**Sweet spot:** *Tier 1 (co-partitioned aggregate) + local view + SYNC* = **local
ACID including the view**, read-your-writes, single node, single log, no clocks —
the DB-face happy path. Push app data models toward this.

### API shape (sketch)
- **View-level policy** (cleaner): a view is declared `sync` (writers block until
  it applies) or `async` (eventual). A txn touching a `sync` view pays the SYNC
  cost; otherwise ASYNC.
- Optionally overridable **per-transaction**: `commit({ viewConsistency: SYNC |
  ASYNC, views: [...] })`.
- Cross-shard SYNC must name participants and is gated behind the same explicit
  `saga` / `twoPhase` choice as Tier-3 (no accidental hidden distributed commit).

## 4. Recommendation

Co-partition-first; saga-default; 2PC opt-in; **refuse Tier 4 unless the product
explicitly pivots to general distributed SQL.** Make the API force the choice
explicit — a single-aggregate txn is local ACID (and can be SYNC-view for free); a
cross-shard txn must pick `saga`/`twoPhase` and, if SYNC-view, name the view
participants. View consistency is a declared property (SYNC vs ASYNC), defaulting
to ASYNC for throughput with SYNC available where read-your-writes / in-view
constraints are required.

## 5. Open questions (for the real spec pass)
- Exact `ViewApplyCallback` ordering vs the group-commit fsync boundary for SYNC +
  `transactional` durability.
- Cross-shard SYNC: 2PC over view shards vs. commit-then-wait-for-watermark — which
  is the default for the rare case.
- Global (broadcast) view maintenance + its failover cost.
- Secondary indexes as just another co-partitioned/broadcast view — unify or special-case?
- Snapshot reads: per-shard read-committed default; cost/appetite for a global
  consistent cut (snapshot vector) at all.
- Interplay with Milestone D leadership rebalancing: view-shard handoff warm-up
  and checkpoint cadence.
