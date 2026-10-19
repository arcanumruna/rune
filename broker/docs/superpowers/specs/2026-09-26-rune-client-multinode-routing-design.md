# Rune Client Transparent Multi-Node Routing — Design (rev 2, post-review)

**Date:** 2026-09-26
**Status:** Draft for review — rev 2 incorporates round-1 adversarial review (4 lenses: concurrency, failure/liveness, in-flight/ack/exactly-once, hot-path/resource).
**Owner:** arcanumruna
**Repo:** rune-client-cpp (library). Broker unchanged **pending the Task-0 spike** (egress stream-id addressability).

## Goal

The client library produces/consumes transparently against a cluster of any size
— 1 node or N — with **only the endpoint config differing**. `producer.send()` /
consumer land on the correct partition leader automatically, at full throughput,
with **no per-record loss, no hung futures, no duplicates, no reordering**, across
leader changes.

## Problem (confirmed)

`Client` holds a single connection and *retargets* it to the leader on redirect.
Metadata (`MetadataCache::leaderFor/endpointFor`) exists but one connection can't
serve partitions whose leaders are on different nodes → it thrashes between
leaders and stalls (observed 8-partition acks=all → 65/5000 acked on a 3-node
cluster). Fix = route each partition to its leader — but done correctly under the
invariants below.

## Architecture (rev 2 — shared-nothing; coordinator owns state; connections are transports)

```
 app thread            AGENT thread  (SOLE OWNER of all routing/metadata/pool/coordinator state)
 ----------            -------------------------------------------------------------------------
 producer.send() ──► M2O ring ──► drain ──► PartitionCoordinator (plain single-owner data — no atomics/locks)
 (writes record;                              ├─ per-(topic,partition) ProducerState (pid,epoch,nextSeq,in-flight,retained,pending)
  ring-full = backpressure)                   ├─ per-(topic,partition) ConsumerState (lastDeliveredOffset, startPos, leaderEpoch)
                                              ├─ routing table: partition→Connection*  (plain vector, agent-owned)
                                              └─ pending-ack map: corrId→completion    (agent-owned, idempotent)
                                     Connection[]  (STATELESS transports; keyed by endpoint)
                                        each = SessionMachine + Publication + egress image (ONE shared media driver)
```

### A. Concurrency = shared-nothing + ring buffers (the project law) — [supersedes the concurrency-review "race" findings]
Per [[rune-shared-nothing-law]]: **no shared mutable state, no atomics, no locks.**
All cross-thread communication is via ring buffers; each piece of state has exactly
ONE owner thread. There are therefore **no data races to design around** — the
concurrency-review findings about the app thread racing `MetadataCache`/pool are
resolved *by construction*:
- **App thread:** the ONLY thing it does is write `(topicId, partition, key, value,
  corrId)` into the existing per-producer M2O ring. Partition is computed from the
  **static `TopicHandle` partition count** (immutable, owned by the handle). The app
  thread never reads `MetadataCache`, the pool, or the coordinator.
- **Agent thread = sole owner** of `MetadataCache`, the connection pool, connection
  lifecycle, per-partition Producer/Consumer state, the routing table, and ack
  resolution — all **plain data structures, no atomics** (single-writer). It drains
  the ring and routes each record.
- **The only cross-thread concern is BACKPRESSURE** (section H), not races.

### B. Per-partition state ownership — [fixes in-flight F1,F3,F4,F5,F10; the root cause]
`PartitionCoordinator` owns, keyed by `(topicId, partition)`:
- **ProducerState:** `producerId`, `producerEpoch`, `nextBaseSequence`
  (monotonic, gap-free **for the partition's lifetime**, independent of which
  connection currently carries it), the ordered in-flight/retained batch set, and
  the partition's pending corrId slice.
- **ConsumerState:** `lastDeliveredOffset`, subscription start position, leader epoch.
`Connection` owns **only transport-lifetime objects** (SessionMachine, publication,
egress image). A leader move **re-points** the partition's transport; it never
moves state between two sequence counters or two pending maps.

### C. NO new transport/driver — reuse the existing single transport's stream multiplexing [rev3, code-grounded]
**Correction (round 2):** `AeronTransport` embeds its OWN media driver
(`startEmbeddedDriver`); N transports = N drivers = hard-fail. So there is NO
`Connection`-owns-transport object and NO shared-driver-attach API. Instead:
- The existing **single `AeronTransport` already multiplexes** N ingress
  publications (`pubCache_`, keyed by `ingressStreamId`) and N egress
  subscriptions (`egressSubs_`, keyed by `channel#streamId`) over its one driver.
  `sendFn(ingressStreamId, buf, len)` already threads the stream id end-to-end
  (`ClientAgent.cpp:182`, `ProducerImpl.cpp:639`).
- **A "leader" is addressed by `(leaderChannel, ingressStreamId, sessionId)`** — not
  a Connection object. Generalize `retarget()` (today swaps the one `ingressPub_`
  and clears `pubCache_`) into **resolve-and-cache a publication per leader
  channel**, exactly like `pubCache_`/`egressSubs_` already cache per stream.
- **The real structural change is multi-SESSION:** each leader assigns its own
  `sessionId`, so `ClientAgent`/`SessionMachine` (today single-session) must hold
  **one session per leader**. This — not "a connection pool" — is the load-bearing
  refactor. Per-leader session state replaces the single global `redirectPending_`
  latch + `currentLeaderNode_/Channel_` scalars (`Client.h:436,452,460`).
- Term buffers: shrink client pub/egress term length (default 4 MiB,
  `AeronTransport.h:62`) so N ingress pubs + N egress images stay cheap in
  `/dev/shm` (~2·N·term·3). Public `Producer`/`Consumer` handles bind to `Client`,
  never to a leader/session, so a retired session never dangles a handle.

### D. Routing — agent-owned plain array (NO atomics/RCU) — [fixes hot-path F3; ULL]
- The coordinator (agent thread) holds `partition → Connection*` as a **plain flat
  vector**, rebuilt in place only on metadata change. It is read only by the agent
  during drain → **no atomic, no RCU, no snapshot publish** (single owner).
- Hot path = an **array index** on the agent thread, never a map/string compare.
- **Single-node = every slot points at the one connection** → the drain path is the
  same as today (no `poolSize==1` special branch, no added indirection on the ULL
  path).

### E. Ack resolution — agent-owned, idempotent, epoch-validated — [fixes in-flight F3,F7,F8]
- One **agent-owned** `corrId → completion` map (corrId already global via
  `nextCorrId_`, single-writer on the agent). Plain data — no atomics. Each
  connection's egress fragments are dispatched on the agent thread into it.
- **Idempotent:** first ack for a corrId resolves; later/duplicate acks are no-ops
  (handles leader-move double-ack; never double-sets a promise).
- **Epoch-validated:** acks carry the leader epoch; an ack stamped with a **stale**
  leader epoch (from a deposed/zombie leader) is **rejected**, not treated as durable.
- **Offset visibility:** a caller sees an offset only after the ack at its
  **configured durability level** (acks=all ⇒ after commit). Never expose an
  optimistic leader-local offset a failover could truncate.

### F. Leader migration — ordered, serialized handoff — [fixes in-flight F4,F6; failure]
On leader change for partition P (from metadata refresh or `NotLeader`):
1. **Freeze** new sends for P behind an in-migration latch (queue, don't race to B).
2. Flush P's builder into its in-flight set (assign sequences in order).
3. Migrate the ordered in-flight set (lowest baseSequence first) to B's transport.
4. Resend in sequence order (same pid/epoch/baseSequence → broker dedups).
5. Release queued new sends, continuing the sequence monotonically.
Transient failover errors (`NotEnoughReplicas`/`LeaderNotAvailable`/ISR<minIsr)
are **retriable with backoff**, batch kept in-flight, bounded by the delivery
deadline (H), preserving order.

### G. Consumer routing + offset continuity — [fixes in-flight F5]
- Each partition's subscribe routes to its leader's connection (leader-only).
- On leader change, re-subscribe P on the new leader **from
  `lastDeliveredOffset+1`** (never earliest/latest) → no gap, no dup. Handle
  `startOffset > newLeaderHighWatermark` (replication lag) with retry/backoff, not
  error. A partition silent past a bound while others progress surfaces
  `PartitionStalled`.

### H. Backpressure & liveness (first-class — the real cross-thread concern) — [fixes all of failure review + user directive]
**Backpressure is the load-bearing property** (the shared-nothing model removes
races; what remains is not overrunning any stage). Three bounded stages, each of
which pushes back rather than dropping or growing unbounded:
1. **App → agent (M2O ring):** ring-full → `send()` back-pressures the app exactly
   as today (the existing wait-free contract). No change; no new alloc.
2. **Agent → per-partition pending (unknown/electing leader, or connect-in-progress):**
   a **bounded** per-partition hold queue (count/bytes). When full, the agent stops
   draining that partition's records from the ring → the ring fills → app
   back-pressures (stage 1). Never unbounded, never silent-drop. **Hold-and-refresh**
   (do NOT speculatively send to the seed → avoids NotLeader-retransmit amplification).
   Sequence assigned only at first real routing to a leader.
3. **Agent → transport (per connection):** publication back-pressure (Aeron
   offer-back-pressured) already retried on the duty cycle; retained frames stay
   in-flight for retransmit. A slow/failing leader back-pressures only its own
   partitions, not the whole client.

Liveness backstops layered on top:
- **Bounded per-record delivery deadline** (`deliveryTimeoutMs`, Kafka analog):
  every submitted record terminates in ack **or** a typed error within its
  deadline — the universal backstop for unknown leader, no leader, unreachable,
  redirect loop, dead connection. **No code path may leave a future unresolved.**
- **Bounded, backpressured pending** for unknown-leader records (cap by
  count/bytes; on overflow block/`WouldBlock`; never unbounded, never silent drop).
  **Hold-and-refresh** (don't speculatively send to the seed → avoid
  NotLeader-retransmit amplification). Sequence is assigned **at first real
  routing to a leader**, not while unknown.
- **Distinguish** `LeaderUnknown` (never learned) vs `LeaderElecting/NoLeader`
  (known-partition, leaderless) vs `PartitionUnavailable` (all replicas down →
  fail fast after a bound).
- **Metadata:** epoch/version per entry; **single-flight + coalesced + rate-limited**
  refresh (one request resolves the whole topic map — no thundering herd);
  **bounded TTL + proactive background refresh** so stale routing self-corrects
  even without a reject.
- **Bootstrap:** **multiple seeds** (`RUNE_TARGET` comma-separated), tried with
  bounded timeout + backoff; a dead seed must not brick the client;
  `NoBrokersReachable` surfaced, not a hang.
- **Connection open:** **async/non-blocking** with states
  (`Connecting/Ready/Failed/Backoff`); never blocks `send()` or the duty cycle;
  frames park (retained) until `ConnectResponse{OK}`. **Eager-open** all discovered
  leader connections at bootstrap (before the measured phase). Per-endpoint connect
  backoff; evict endpoints no longer in cluster metadata; cap pool size.
- **Redirect bound:** cap redirect hops per record; count redirect time against the
  delivery deadline; on exhaustion refresh metadata once, then fail `TooManyRedirects`.
- **Per-connection redirect state** (not the single global `redirectPending_` latch).

### I. Idempotence / exactly-once contract — [fixes in-flight F2,F7,F9]
- Cross-leader dedup relies on the broker reconstructing P's PSM to the **committed
  log prefix** and deduping by `(pid, epoch, baseSequence)`. Documented as a broker
  assumption; **DR test kills the leader mid-replicate/mid-ack** (not idle) to
  exercise it.
- **producerId stub is a known limitation:** a fixed stub shared across producers/
  restarts causes cross-client false-dedup (silent drops). **Real `InitProducerId`
  (broker-assigned pid) + epoch bump on restart is REQUIRED before multi-producer /
  DR benchmarking.** Treated as a prerequisite task, not deferred silently.

### J. Duty-cycle scaling — [fixes hot-path F2]
- `doWork` polls only **active** connections (intrusive active-list), O(active) not
  O(N); one aggregate work-count drives one idle strategy (no idle-connection
  spin, no busy-connection starving another's backoff). Pool-of-1 keeps the exact
  current single-connection duty-cycle path.

### K. Teardown — [fixes concurrency F9; hot-path F7]
Stop runner (no more agent mutation) → drain each connection's in-flight to
completion/timeout → close each pub/sub/egress → close the shared driver **last**.
Retire (idle/leader-move) only via a command posted to the agent thread, executed
at a safe duty-cycle point, and only when the connection has **zero owned
partitions AND zero outstanding acks**.

## Round-2 corrections, scope & rulings (code-grounded)

**Transport/state (reviewers A,B):**
- Ack/in-flight hoist target = **per-partition batchMaps + ackRing** (what exists),
  NOT one global corrId map — a global map breaks `retransmitStalled`'s O(1)
  front-check (batchMap is ordered by corrId==send-order). Reuse the existing
  two-level fan-out (corrId→BatchInfo→ackRing slice→per-record corrIds).
- Wire record already carries `(topicId,partition,corrId)` inline (zero-alloc);
  routing key is `partition` only (`key` is app-side, already resolved).
- **Hold-for-unknown/electing-leader = reuse the existing `sessionId==0` "hold in
  builder" path at FLUSH time** (`ProducerImpl.cpp:812`), not a new post-encode
  queue. One predictable flush-time branch (`!leaderKnown` ≈ `sessionId==0`), zero
  drain-time per-record cost, preserves "sequence assigned only at real routing"
  (`nextBaseSequence` advances only on successful offer).
- **Reuse `retransmitStalled` / `deliveryTimeoutMs` / `sweepTimedOut` verbatim**
  for the delivery backstop + migration resend. Migration must **re-stamp in place
  (same corrId, keep map order)** like `resendAllRedirected`, and **reset
  `attemptDeadlineNs`** so the time-based retransmit doesn't double-fire.
- **Scope `redirectPending_` + `resendAllRedirected` per-leader** — today they
  re-stamp the WHOLE batchMap with one session; multi-leader would corrupt
  partition-B frames with partition-A's session.
- Duty cycle: keep **one idle strategy over aggregate work** (do not add
  per-connection idle strategies); add active-set pruning + evict stale
  `pubCache_`/`egressSubs_` for O(active).

**Egress (reviewer C) — Task-0 largely answered:** the client ALREADY opens
per-partition **dynamic-MDC egress** subscriptions (`onMetadataResponse` →
`addEgressSubscription`) with **broker-stamped** `egressStreamId`/`egressControlPort`
(`MetadataResponse.PartitionEntry`), routed by `subscriptionId` in `SubscriberHub`.
So egress is **already multi-node-capable and broker-addressed** — **adopt it as-is**;
do NOT add a second per-connection egress regime (would duplicate-deliver / blow
FDs). Preserve the single-node **fallback-host** logic. (Task-0 residual: just
confirm produce-ack egress rides this same per-worker MDC path.)

**InitProducerId (reviewer C) — smaller than thought:** full `InitProducerId`
protocol EXISTS and is used by `TransactionalProducerImpl`; the plain
`ProducerImpl` just uses a fixed stub pid. Task = **wire existing InitProducerId
into `ProducerImpl` + epoch bump on restart** (client-side), not build it. REQUIRED
before multi-producer/DR dedup correctness.

**Metadata (reviewer C):** the existing refresh is already single-flight/backoff/
idempotent + **zero-RTT redirect-learning** (updates cache from the REDIRECT frame,
no round-trip) + single-node fallback host. **Extend** it with bounded TTL +
proactive background refresh + multi-seed bootstrap — **without dropping** the
zero-RTT path or the fallback.

**Rulings (override on review):**
1. **Producers/consumers are created at SETUP (pre-`start()`)** for cluster mode →
   removes the need for `ProducerHub::snapshot_` RCU + `pendingAcks_`/`tier2Mtx_`/
   `txnHubMtx_` app↔agent primitives (DR-BUG-1 post-start `producer()` not
   supported in cluster mode). If post-start creation must stay, those primitives
   stay and "no atomics" is relaxed to "no NEW atomics on the routing hot path."
2. **Consumer GROUPS are IN scope** (user: everything is in scope). Requires
   **FindCoordinator** routing (see §L) — net-new; the client has none today
   (`NotCoordinator` just rejoins the same session → hangs at N>1).
3. **Transactions are IN scope** (user). Requires **transaction-coordinator
   routing** (§L) for `InitProducerId`/`AddPartitionsToTxn`/`EndTxn`/
   `TxnOffsetCommit`, plus reconciling `TransactionalProducerImpl`'s own pid/epoch
   ownership with the coordinator model. Txn `Produce` frames route to partition
   leaders exactly like the plain producer.
4. **Per-record failure isolation:** one partition's dead-leader failure fails only
   that record (typed error at delivery deadline), never the whole batch (matches
   dr_client's continue-on-fail the harness relies on).

## L. Coordinator routing (groups + transactions) — [rev4, code-grounded; CROSS-REPO]

**Round-3 correction:** the "hash group-id/txn-id → coordinator node" model is WRONG
for this broker today. Both `GroupCoordinationService::coordinatorFor` and
`TransactionCoordinationService::coordinatorFor` are **v1 stubs that ignore the id
and return the single Raft consensus leader** ("cluster leader owns all
groups/transactions"). Offsets (`__consumer_offsets`) and txn state
(`__transaction_state`) are single Raft-replicated partitions on that leader. So:
**coordinator == consensus leader** (single coordinator, acceptable for v1 — per-id
sharding is deferred scale work, NOT needed for correctness). The client just needs
to discover/re-discover that leader for the control plane.

**This requires BROKER + PROTOCOL work that does not exist yet (Milestone-D Phase 3):**
- **B1 [small] `FindCoordinator(type=group|transaction)` RPC** → returns the
  consensus-leader endpoint (reuse the connect-path `leaderClientEndpoint`). No hash
  ring. Net-new protocol template + one handler.
- **B2 [small] carry the coordinator endpoint on `NotCoordinator`** responses (today
  they carry none → the client can't re-route; it infinite-rejoins).
- **B3 [CRITICAL, large] cross-node transaction marker fan-out.** `EndTxn` today
  writes COMMIT/ABORT markers ONLY to partitions local to the coordinator node
  (`appendControlRecord`→`TopicRegistry::log` returns `-1` for remote partitions —
  silently dropped); `WriteTxnMarkersRequest` (templateId 7) is a **dead struct**.
  Must be wired as a real inter-node RPC: coordinator → each participant partition's
  leader node → local marker append, retried/idempotent, BEFORE `completeTransaction`.
  Without this, a multi-leader txn commits with a success ack but leaves remote
  partitions permanently un-LSO'd → **silent EOS violation.** This is the
  load-bearing broker item.
- **B4 [verify/likely-fix] cross-node group record delivery.** Group assignment is
  realized as push subscriptions on the COORDINATOR node's gateway
  (`addGroupSubscription(sessionId,...)`); a member assigned a partition led by a
  non-coordinator node may not receive its records. Spike first; fix if confirmed.

Client side (§C multi-session + these):
- **FindCoordinator:** add a client `FindCoordinator(type=group|transaction, key)`
  request → coordinator nodeId → `endpointFor(nodeId)` → route the control plane to
  that node's session (reuse the same multi-session transport as §C; a coordinator
  is just another `(channel, streamId, sessionId)`). **Task-0 must confirm the
  broker exposes FindCoordinator**; if not, a broker addition enters scope.
- **Group control plane** (`GroupConsumerHub`: Join/Sync/Heartbeat/OffsetCommit/
  OffsetFetch) routes to the **group coordinator** session, NOT the single default
  session it uses today. `NotCoordinator` → re-run FindCoordinator + re-route (not
  rejoin-same-session). Group-assigned partition **data** still arrives via the
  existing per-partition MDC egress; only the control plane needs coordinator
  routing. Offset continuity for group consumers is via **OffsetFetch/OffsetCommit**
  (broker-committed), not `lastDeliveredOffset+1` (that is for non-group subscribe).
- **Transaction control plane** (`TransactionalProducerImpl`: InitProducerId/
  AddPartitionsToTxn/AddOffsetsToTxn/EndTxn/TxnOffsetCommit) routes to the
  **transaction coordinator** session (FindCoordinator type=transaction). Txn
  `Produce` frames route to partition leaders (§D). **Single owner of pid/epoch:**
  the txn producer's broker-assigned `(pid,epoch)` (already via existing
  InitProducerId) becomes the identity the coordinator layer uses for that
  producer's partitions — resolve the current double-ownership
  (`TransactionalProducerImpl` atomics vs plain `ProducerImpl` stub) into one
  coordinator-owned producer-identity per producer.
- Coordinator sessions participate in the same duty-cycle poll + delivery-deadline +
  redirect/failover machinery as leader sessions (coordinator can move too →
  re-FindCoordinator on failover).

**Honest limitation to own:** one M2O ring **per producer** ⇒ ABORT backpressure is
**head-of-line-blocking across partitions** (a stuck partition blocks others behind
it in the same ring). "A slow leader back-pressures only its own partitions" is NOT
achievable at the ring layer without **per-partition producer rings** (a larger
change, deferred). Mitigation: unknown-leader holds in-builder with CONTINUE (keep
draining other partitions) until its builder bound; the delivery-deadline backstop
bounds the worst case. Documented, not hidden.

**dr_client subsumption:** dr_client's multi-node "support" is operational (harness
launches one client per partition-leader via `RUNE_SUB_PARTITION_BASE/PARTITIONS`)
+ single-session retarget. This design subsumes it → validation asserts a **single**
client covers all partitions/leaders, and the env-knob workaround is removed.

## No-regression constraint
Single-node produce/consume throughput + p50/p99/p99.9 within noise of today
(routing = one array index; pool-of-1 = same duty cycle). Plus a **cold-start /
first-ack-per-partition latency** measurement that WARMUP does **not** mask.

## Validation / test matrix
- Existing client suites stay green (producer, race, redirect, notleader, agent).
- 3-node multi-partition produce via one seed → **all acked** at throughput; consume all.
- Ordered migration: leader move mid-flight → no dup/loss/reorder; sequence monotonic.
- **Kill leader mid-replicate/mid-ack** → no dup, no loss (I).
- Consumer resume-from-offset across failover **with replication lag** (G).
- acks=all failover with degraded ISR → retriable, bounded, no false-fail (F/F6).
- Late-ack-after-move → idempotent resolve, no double-count; stale-epoch ack rejected (E).
- Unknown-leader / no-leader / all-down → bounded, typed errors, no hang, no OOM (H).
- Multi-producer / restart dedup with **real pid** (I) — or documented as gated.
- Resource: `/dev/shm`, FD, duty-cycle CPU with N=3 and 1 busy leader (C,J).

## Open questions (resolved as rulings; override on review)
1. Egress addressing: **reserved disjoint stream-id ranges** (producer-ack vs
   consumer) + free-list with drain-quarantine on retire — **pending Task-0 spike**.
2. Seeds: **multi-seed** (was single). 3. Retirement: keep-all default, predicate in K.
4. Consume: leader-only. 5. `dr_client` per-leader workaround becomes removable after this.

## Cross-repo scope (converged after 3 adversarial review rounds, 10 reviewers)

**Client (rune-client-cpp) — large single→multi-session rework:**
- Multi-session keyed by **node endpoint** (one SessionMachine/ConnectRequest per
  node; a node's produce-ingress stream + coordinator-control stream multiplex over
  that ONE session — distinct ingress streamIds, shared sessionId/keepalive). [G6]
- Unify ALL hub `SendFn`s to `(ingressStreamId, buf, len)` and make `doWork`
  per-session (agent iterates active sessions, invokes each hub's slice) — today one
  global `sessionId` + stream-0 is fed to every hub. [G1,G2]
- Egress demux by **arrival image/session**, not templateId; per-session redirect/
  retarget state (not the global `redirectPending_`/`currentLeader*` scalars);
  `haveWork` must include group/txn (else coordinator redirects are dropped). [G4,G5]
- Coordinator-**epoch** validation on group/txn responses (reject a deposed
  coordinator's late response), mirroring §E leader-epoch. [G9]
- Mixed-liveness: group heartbeat + txn timeout servicing must NOT be starved by the
  bounded produce drain (heartbeat interval « single-cycle drain bound). [G7]
- Txn producer: leader-route its Produce (today stream-0 → NotLeader stall) + ordered
  failover resend (§F) + NotCoordinator→re-FindCoordinator. [txn gaps 3,4,5]
- OffsetCommit response error-code handling (today ignored → silent commit loss on
  failover). [group gap 5]
- Single "node-down" reaction coalescing leader re-route + coordinator re-FindCoordinator
  off the one metadata refresh. [G10]
- Plus rev-2/3 items: reuse transport multiplexing (no new driver), per-partition
  batchMaps, hold-in-builder backpressure, reuse retransmitStalled/deliveryTimeout,
  wire existing InitProducerId into the plain producer, metadata TTL/multi-seed
  (keep zero-RTT + fallback), one idle strategy + active-set pruning.

**Broker + protocol (rune-broker / rune-protocol) — Milestone-D Phase 3, NOT done:**
- B1 FindCoordinator RPC (small) · B2 coordinator endpoint on NotCoordinator (small)
- **B3 cross-node txn marker fan-out (CRITICAL, large)** — wire the dead
  `WriteTxnMarkersRequest`; without it multi-leader txns silently violate EOS.
- B4 verify/fix cross-node group record delivery.
- (Per-id hashed coordinator placement = deferred scale, not needed for correctness.)

**Bottom line:** "everything multi-node" = **finish broker Milestone-D Phase 3
(distributed coordination + cross-node markers) AND a major client multi-session
rework** — a cross-repo program, not a benchmark unblock. Single-node benchmark is
green and de-risked today.

## Self-review
- Every round-1 finding maps to a section (concurrency→A,D,E,K; failure→H; in-flight→B,E,F,G,I; hot-path→C,D,J + cold-start gate + Task-0).
- Root cause (state-in-Connection) fixed by B (coordinator owns per-partition state; Connection stateless).
- Scope honesty: this is a **cluster-aware producer/consumer coordinator**, materially larger than "a connection pool" — flagged to the owner.
