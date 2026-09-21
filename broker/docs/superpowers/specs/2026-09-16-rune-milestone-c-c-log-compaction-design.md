# Milestone C-C — Log Compaction (design)

> **STATUS: proposed design, ready for review.** Third and final sub-project of the
> rest of Milestone C (A = consumer groups ✅ done; B = exactly-once/transactions;
> **C = log compaction**). Sequencing: **B before C** — compaction of
> `__transaction_state` depends on MC-B existing. Builds on MC-A + MC-B.

## 1. Purpose & motivation

Give Rune **key-based log compaction** (Kafka `cleanup.policy=compact`): retain the
**latest record per key** indefinitely instead of dropping old records by time/size.
Three payoffs:

1. **Bounded internal topics** — `__consumer_offsets` (MC-A) and `__transaction_state`
   (MC-B) grow forever under delete-retention; compaction keeps only the current value
   per `(group,tp)` / `txnId`, so recovery replay stays cheap regardless of uptime.
2. **Changelog / KTable semantics** — a compacted topic is a durable "latest value per
   key" table, the substrate for the future materialized-views / DB-face work.
3. **Tombstone deletes** — a null-value record removes a key from the compacted view.

The Phase-4 engine already contains the compaction machinery (`LogCleaner`,
`OffsetMap`); this milestone **wires it in** — same "engine exists, wire it" pattern as
MC-A/MC-B — but with **one load-bearing prerequisite** (§4.1) that must land first.

## 2. Scope & non-goals

**In scope (v1):**
- Replica/append path carries a **record key** (`append(key, value)`) — the prerequisite.
- Re-key the internal topics: `__consumer_offsets` keyed by `(group,topic,partition)`,
  `__transaction_state` keyed by `transactionalId`.
- `LogCleaner` + `OffsetMap` wired as a **bounded background duty-cycle pass** over
  non-active (sealed) segments; latest-per-key retained, **original offsets preserved**.
- Per-topic `cleanup.policy`: `delete` (default), `compact`, `compact,delete`.
- **Tombstones** (null value = delete key) with a delete-retention window.
- Correctness interactions: **preserve idempotent-producer state** (MC-B) through
  compaction; per-replica-independent deterministic compaction (no HW/LEO perturbation).
- DR-E2E (latest-per-key, tombstone, offsets-preserved, survives restart/failover) +
  compaction throughput benchmarks.

**Non-goals (deferred):**
- Compacting user data topics by default — opt-in via `cleanup.policy` only.
- Cross-node dedup / distributed compaction coordination — each replica compacts its own
  log independently (deterministic ⇒ convergent). Milestone D changes nothing here.
- Compaction-aware fetch optimizations (gap skipping already tolerated by the read path).

## 3. Locked decisions (from brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Prerequisite | **Add `append(key, value)` at the replica layer + re-key internal topics** | Compaction dedups by key; `__consumer_offsets`/`__transaction_state` write an EMPTY key today (identity is in the value) — can't dedup without a real key |
| When compaction runs | **Bounded background pass on the duty cycle; dirty-ratio trigger; non-active segments only** | Single-writer discipline — never races the leader's tail append; bounded work per cycle keeps latency off the hot path |
| Offset handling | **Preserve original offsets (compacted log has gaps)** | Kafka-faithful; the read path already tolerates non-contiguous offsets |
| Replication interaction | **Per-replica independent + deterministic** | Same input ⇒ same output on every replica; compaction stays out of the HW/LEO/replication model |
| Producer state | **Preserve last batch per producer** through compaction | So MC-B idempotent dedup survives (Kafka does exactly this) |
| Policy granularity | **Per-topic `cleanup.policy` in TopicRegistry** | `delete` default keeps existing behavior; opt-in `compact` |

## 4. Architecture: components & boundaries

### 4.1 Prerequisite — `append(key, value)` at the replica layer (load-bearing)
Today `PartitionReplica::append` / `makeBatch` write records with an **empty key**
(payload identity lives in the value). Compaction cannot dedup without a key, so first:
- Extend the replica/append path (and the RecordBatch record encoding it drives) to
  carry an optional **record key** alongside the value — `append(key, value, acks)`.
- Re-key the internal topics written in MC-A/MC-B:
  - `__consumer_offsets`: key = `(group, topic, partition)` (was empty; identity was in
    the value) — so compaction keeps the latest committed offset per `(group,tp)`.
  - `__transaction_state`: key = `transactionalId` — latest txn state per id.
- Producer/idempotent framing (MC-B pid/epoch/seq) is unaffected — key is orthogonal.
This is the central change; everything else composes on top of it.

### 4.2 `LogCleaner` + `OffsetMap` wiring
- Run compaction as a **bounded background duty** (Kafka-style): a **dirty-ratio**
  trigger (dirty bytes / total bytes past a threshold) selects a log to clean; clean
  only **non-active (sealed) segments** so it never touches the segment the leader is
  appending to (single-writer preserved).
- Build an `OffsetMap` (key → highest offset) over the dirty segment range, then rewrite
  the sealed segments keeping, for each key, only the record at its highest offset;
  **original offsets are preserved** (the rewritten log is sparse/gappy).
- Bounded work per cycle (byte/segment budget) so a compaction pass never stalls the
  duty cycle or the append hot path.

### 4.3 Tombstones + topic policy
- `cleanup.policy` per topic in `TopicRegistry`: `delete` (default — existing size/time
  retention), `compact`, or `compact,delete` (compact, then also age out by retention).
- A **null-value record is a tombstone**: after compaction it deletes the key from the
  view; the tombstone itself is retained for a **delete-retention window**
  (`delete.retention.ms`-style) so lagging followers/consumers observe the delete, then
  it is dropped on a later pass.

### 4.4 Correctness interactions
- **Per-replica independent + deterministic** — each replica runs its own `LogCleaner`;
  given identical sealed-segment input the output is identical, so leaders and followers
  converge without extra coordination and the replication/HW/LEO model is untouched
  (compaction only rewrites already-committed, below-HW segments).
- **Preserve idempotent-producer state (MC-B)** — compaction must retain the last batch
  per `producerId` even if its key would otherwise be superseded, so producer-state
  recovery (dedup sequence numbers) survives a compacted log. (Kafka's exact rule.)
- **Internal-topic outcomes** — `__consumer_offsets` compacts to
  latest-offset-per-`(group,tp)`; `__transaction_state` to latest-state-per-`txnId` —
  precisely the recovery-replay set MC-A/MC-B rebuild from.

### 4.5 DR-E2E + benchmarks
- Compaction correctness: **latest-per-key kept**, superseded values gone, **tombstone
  deletes the key**, **original offsets preserved**, compacted state **survives restart
  and leader-kill failover**.
- Producer-state preserved through compaction ⇒ MC-B idempotent dedup still works after a
  compaction pass.
- Internal-topic recovery: rebuild `__consumer_offsets` / `__transaction_state` from a
  compacted log gives the same committed offsets / txn state as before compaction.
- Benchmarks: compaction throughput (MB/s, records/s), OffsetMap build cost, per-cycle
  bounded-work budget honored (no duty-cycle stall). 100µs gate stays on the **append
  hot path** (compaction is background, but must not perturb append p99).

## 5. Data flow (a compaction pass)
1. Duty cycle checks dirty ratio for each compact-policy log; picks one past threshold.
2. Snapshot the **sealed** segment range (exclude the active segment the leader appends
   to); build `OffsetMap` (key → highest offset in range), noting the last batch per
   producerId to preserve.
3. Rewrite the sealed segments: for each record, keep it iff its offset == OffsetMap[key]
   (and it is not a to-be-dropped tombstone past delete-retention) **or** it is a
   preserved producer-state batch; drop everything else. Offsets are preserved (gaps ok).
4. Atomically swap the rewritten segments in; reclaim old segments. Dirty counter resets.
5. Followers independently do the same over their own sealed segments (deterministic ⇒
   convergent) — no cross-node message needed.

## 6. Failover & recovery
- Compaction only rewrites **committed, below-HW** sealed segments, so a mid-compaction
  crash loses at most the in-progress rewrite (atomic swap ⇒ either old or new segment
  set survives) — no committed data lost, HW/LEO unaffected.
- On leader-kill, the new leader's log is already compacted-or-not independently; because
  compaction is deterministic and offset-preserving, replicas remain byte-consistent on
  the retained records (divergence only in *reclaimed* dropped records, which by
  definition are superseded).
- Internal-topic replay after failover reads the compacted `__consumer_offsets` /
  `__transaction_state` and rebuilds the same state (§4.4).

## 7. Milestone-D seam
Compaction is per-replica and needs **no** cluster coordination, so distributing
partition leadership (Milestone D) changes nothing: each node compacts the logs it hosts.
The `append(key, value)` prerequisite (§4.1) is also exactly what the future
materialized-views / DB-face changelog topics need — this milestone lays that groundwork.

## 8. Error handling / edge cases
- Active segment is **never** compacted (single-writer safety); only sealed segments.
- A key that only ever had a tombstone: tombstone retained for the delete-retention
  window, then the key disappears entirely.
- `compact,delete`: compaction runs first, then normal retention ages out old segments —
  order matters (don't delete-retain a segment a producer-state batch must survive in;
  producer-state preservation takes precedence within the compaction pass).
- Compaction must not reorder or renumber offsets — consumers/followers reading by offset
  must still land on the same records (gaps are fine, shifts are not).
- Dirty-ratio thrash guard: minimum cleanable dirty ratio + min-compaction-lag so a
  hot-updated key isn't compacted every cycle.

## 9. Testing & benchmarks
- **Unit:** `OffsetMap` (key → highest offset, collisions, capacity); `LogCleaner`
  segment rewrite (latest-per-key, tombstone drop after window, offset preservation,
  producer-state batch preserved); `append(key,value)` encode/decode round-trip;
  `cleanup.policy` parsing.
- **Integration:** re-keyed `__consumer_offsets` compacts to latest-per-`(group,tp)` and
  rebuild matches; `__transaction_state` compacts to latest-per-`txnId`; tombstone
  delete visible after a pass; idempotent dedup survives a compaction pass.
- **DR-E2E:** latest-per-key + tombstone + offset-preserved correctness across
  restart/leader-kill; producer-state-through-compaction EOS; internal-topic recovery.
- **Benchmarks (100µs gate on the append hot path):** compaction throughput (MB/s,
  rec/s), OffsetMap build cost, per-cycle bounded-work budget honored, append p99
  unperturbed while a compaction pass runs.

## 10. Ticket breakdown (proposed)
- **CP-1** — `append(key, value)` replica/protocol change + RecordBatch key encoding +
  re-key `__consumer_offsets` and `__transaction_state` + conformance. (Prerequisite.)
- **CP-2** — `LogCleaner` + `OffsetMap` wired as a bounded background duty-cycle pass over
  non-active segments; dirty-ratio trigger; offset-preserving rewrite + atomic swap.
- **CP-3** — per-topic `cleanup.policy` in TopicRegistry + tombstones + delete-retention.
- **CP-4** — correctness interactions: producer-state preservation through compaction;
  replication/HW/LEO non-perturbation; internal-topic recovery from compacted logs.
- **CP-5** — DR-E2E scenarios + compaction throughput benchmarks + full-area Opus review
  to CLEAN.

## 11. Open questions (resolve during planning)
- `OffsetMap` sizing/eviction: fixed-capacity hash with a per-pass segment budget vs
  a full-log map — lean fixed-capacity + multi-pass for large logs (Kafka's approach).
- Producer-state preservation mechanism: piggyback on Phase-1 producer-state snapshot vs
  a "keep last batch per pid" rule inside the cleaner — pick one in CP-4.
- Delete-retention accounting: wall-clock vs offset-lag horizon for tombstone drop —
  lean a lag/time hybrid consistent with existing retention config.
- Whether `compact,delete` interleaves per-pass or runs compaction-then-delete as
  separate duties — lean separate duties for simplicity in v1.
