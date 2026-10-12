# Rune Phase 2b — retractable Min/Max/Last, zero-copy produce, typed-topic routing, `rune console`

**Status:** design approved in conversation 2026-10-11 (aggregate store; Last semantics; LastNonNull; numeric Min/Max; fixed-size zero-copy first). Builds on Phase 2a (`feature/materialized-views`: broker 6965024, protocol d0578c4, client 0e8505c).
**Parent spec:** `2026-10-08-rune-view-engine-single-source-design.md` (§6 aggregate table, §6.4 nulls, §9.2 aux state is checkpointed, §14.0 latency gates).
**Public API already published** on arcanum.regressorspath.com/rune/docs.html (v0.5.6): `tryClaim`, `claimBatch`, `PositionEncoder`, `TypedTopic::claim`, `rune>` console. 2b makes those names real. Composite keys, joins and `create view` in Rune QL (also on the site) are **Phase 3**, not 2b.

Four parts, four independent implementation plans:

| Part | What | Repos |
|---|---|---|
| **A** | Retractable `Min`/`Max`/`Last`/`LastNonNull` | broker (libs/view, libs/node), client (enum mirror, probe) |
| **B** | Zero-copy produce: `tryClaim`, `claimBatch`, SBE-style flyweights | client, codec |
| **C** | Typed topics route by key (fix: always partition 0) | client, protocol, broker |
| **D** | `rune console` (Rune QL); `rune schema` read-only | ql, client |
| **E** | Transient topics: rename memory topics, definable from Rune QL and the client | broker, protocol, client, ql |
| **F** | One producer: transactions on `Producer` (every produce path), broker-assigned producer ids, consumer-group rewind after abort | client (+ broker only if a gap is found) |

Order: **C → E → E2 → D → B → F → A** (F added 2026-10-12, after B because transactional claims reuse B's claim paths), with A runnable in parallel with B/D (disjoint repos/files). C first because it is a correctness bug and B's typed claim builds on it.

---

## A. Retractable Min / Max / Last / LastNonNull

### Semantics (user-approved)
- `Min`/`Max`: numeric source types only (Int32/Int64/UInt16/UInt32/UInt64/Float64); output type = source type; nulls ignored; NaN/±Inf treated as NULL and counted in `aggNonFinite` (as 2a). Empty / all-null → NULL.
- `Last`: value of the **most recently written** contributor (highest log offset), **including null**. Any fixed-width source type; output type = source type.
- `LastNonNull`: value of the most recently written contributor whose value is non-null; NULL if none. New wire rule **`MergeRule::LastNonNull = 8`** (append-only).
- Table semantics, co-partition rule (single-partition source), and retraction on update / group move / delete are exactly 2a's.

### Storage (user-approved: per-group contributor list)
Only views that use any of the four rules get the extra columns; 2a-only views keep their 2a layout byte-for-byte.
- **Contributor row** gains `[prev u64][next u64]` — a doubly linked list of the group's contributors, keys as links (`kNoContributor = UINT64_MAX`), in **MRU order**: every write of a contributor moves it to the list head.
- **Group row** gains `[head u64]` and, per `Min`/`Max` field, a cached `[value 8B][valid u8]`.
- **Input value slots get the source field's width** (Last/LastNonNull over `string(N)` stores N bytes) instead of a fixed 8; `AggLayout` carries per-input offset + width.

### Maintenance
- **Add** (insert or update of contributor `c` into group `g`): unlink `c` from its old list if any, link at head of `g`; for each Min/Max field with a non-null value: compare with the cached extreme, replace if better (O(1)).
- **Retract** (update, move, tombstone, missing group value): unlink `c`; for each Min/Max field whose cached value equals `c`'s old value → mark the field **rescan-pending**. After the record's add (if any) completes, each rescan-pending field walks `g`'s list once and recomputes the extreme (O(group size)). An in-place update whose new value is already at least as extreme skips the rescan.
- **Last** = head's value — O(1) always (MRU order; unlinking the head promotes the next). **LastNonNull** = first non-null walking from the head (stops at the first hit).
- Emit writes Min/Max from the cache (NULL if `nonNull == 0`), Last/LastNonNull from the list.
- Inconsistency (a link pointing at a missing contributor, a rescan finding a non-null count mismatch) increments `aggInconsistent` and never crashes or invents state (2a rule).

### Persistence
No new tables, no format change: links, heads and caches live in the contributor and group tables the 2a v2 multi-table record already checkpoints offset-atomically. Recovery is unchanged; the per-pass delta cap's worst-case-entries-per-record bound is recomputed (a contributor move dirties: contributor, both neighbours in the old list, the old head's group, the new head, both groups, both view rows — the plan derives the exact bound and the test pins it).

### Gates
- Typical-case fold: record p99 < 5 µs, batch p99 < 10 µs, zero steady-state allocation (§14.0).
- Separate benchmark: retracting the cached extreme in a group of 1k and 10k contributors — reported, not gated (the known O(group size) path).
- Tests: extreme retraction surfaces the next (incl. duplicates of the extreme, group moves, in-place raise/lower), Last after deleting the newest, LastNonNull skipping nulls, Float NaN/Inf, restart then retraction of the extreme still exact (Review-Focus style regression), e2e: `user_totals` gains `max`, `min`, `last` across the broker restart.

---

## B. Zero-copy produce

### What "zero-copy" means here (honest copy count)
Today, Thread-mode `send(span)` copies the value 3× (user span → producer M2O ring → Produce frame → Aeron term); `TypedTopic::send` adds a 4th (struct → reused buffer). A claim lets the caller **encode directly into the producer's ring** — removing the caller-side buffer and every struct round-trip: **2 internal copies remain in Thread mode** (ring → frame → Aeron). In **Invoker mode** (owner-thread, no ring) a claim writes straight into the Produce frame being built: **1 copy** (frame → Aeron). Removing the remaining internal copies (frames built in place in the ring / Aeron `tryClaim` of the frame) is out of 2b scope.

### API
```cpp
// raw
std::optional<Claim> Producer::tryClaim(TopicHandle t, const Key& k, uint32_t valueLen);   // nullopt = BackPressured
Claim:  std::span<uint8_t> buffer();  void commit();  void abort();  ~Claim() aborts if not committed
// batch: N records of the same length, ONE atomic append (one RecordBatch, contiguous offsets)
std::optional<BatchClaim> Producer::claimBatch(TopicHandle t, const Key& k, uint32_t n, uint32_t recLen);
BatchClaim: std::span<uint8_t> record(uint32_t i);  commit();  abort();  ~ aborts
// typed (generated flyweight; routed by the key field — needs Part C)
auto w = typedTopic.claim(keyValue);   // w is the generated encoder over the claimed bytes, w.commit()
```
- `valueLen` / `recLen` ≤ the existing per-record cap (16 KiB incl. key); `n × recLen` ≤ the ring's `maxMsgLength`. Over-cap → `MessageTooLarge` (non-retryable) as today.
- A held claim blocks the producer's single consumer: claims are RAII, must not span blocking work, and an abandoned claim is aborted by its destructor (same rule the ring already uses internally).
- Acks/completions: `tryClaim(..., Completions&, Token)` / `AckToken` overloads mirror `trySend` (same credit reservation).
- Thread rules unchanged: Thread mode any thread; Invoker mode owner thread only (`WrongThread` otherwise).

### Implementation shape
- Thread mode `tryClaim`: claim a `kMsgSendRecord` slot of `38 + valueLen`, write the header (topic, partition, key, flags, ackHandle, valueLen), hand back `base + 38 .. + valueLen`. `commit()` commits the ring slot. The agent path is unchanged.
- Invoker mode `tryClaim`: reserve the record's bytes directly in `ProduceFrameBuilder` (varint header is computable from `valueLen` + key), return the value span, finalize on `commit()`, roll back on `abort()`.
- `claimBatch`: new ring message type **`kMsgClaimBatch = 4`** carrying `[topic][partition][key][n][recLen]` + `n × recLen` bytes contiguously; the agent appends all `n` records into **one** RecordBatch (atomic append, same ordering rules as Tier-2 batches). Invoker mode builds the batch in the frame directly.
- Value offset 38 in the ring is not 8-aligned and generated records are packed: flyweights use `memcpy` loads/stores (no aliasing UB).

### Generated flyweights (codegen, rune-codec `generateHeader`)
For schemas whose fields are **all fixed-size** (approved scope), the generator additionally emits:
- `XEncoder`: `explicit XEncoder(std::span<uint8_t>)` writes `[blockLength][version]`; one chainable setter per field (`.zone(int32_t)`, `.name(std::string_view)` for `string(N)`, zero-padded) at compile-time offsets; `static constexpr uint32_t kBlockLength`, `kEncodedLength = 4 + kBlockLength`; `key()` returning the key-field value.
- `XDecoder`: `explicit XDecoder(std::span<const uint8_t>)` with one getter per field (bounds checked against `blockLength`, tolerant of newer versions' longer blocks exactly like today's `decode`).
- Schemas with var-size fields (`string`, `bytes`) keep today's struct + `encode()`; zero-copy for them is a later step (writer appends var fields in order; length must be known before the claim).
- The bound-version stamp `TypedTopic` writes at bytes `[2..3]` is applied by the typed claim.

### Gates
- Benchmarks (client): `send(span)` vs `tryClaim` vs `TypedTopic::send` vs typed `claim`, per-record p50/p99 and allocations; claim paths **0 allocations** steady state.
- Tests: commit/abort/destructor-abort; BackPressured when the ring is full; claimBatch atomicity (all N land contiguously or none); ordering vs `send` and Tier-2 batches on one partition; Invoker vs Thread mode; flyweight round-trip against `decode()` for every fixed type incl. `string(N)`; version stamp.

---

## C. Typed topics route by key (bug fix)

**Bug:** `TypedTopic::send` always sends `TopicHandle{id, 1}` with an empty `Key`, so every typed record lands on partition 0 and carries no record key. Cause: `TopicSchemaResult` has no partition count, and the generator emits nothing for the key field.

**Fix:**
- Protocol: the `GetTopicSchema` response gains `partitionCount` (appended field; old clients ignore it). Broker fills it from `RegistryEntry.partitionCount`.
- Client: `TopicSchemaResult.partitionCount`; `TypedTopic` stores `TopicHandle{id, partitionCount}`.
- Codegen: struct gains `key()` returning the `isKey` field's value; `TypedTopic::send(rec)` sends `Key(rec.key())` → correct partition and record key.
- A typed topic whose schema has no key field keeps today's behaviour (partition 0, empty key) — documented.
- Regression test (suite `*Regression*`): an 8-partition typed topic distributes records by key exactly like `Producer::send` with the same `Key`.

---

## D. `rune console`; `rune schema` read-only

### Rune QL
- New `parseScript(std::string_view) → std::expected<std::vector<Statement>, ParseError>` (one or more statements, each `;`-terminated; `--` comments; positions reported per statement). `parse()` stays for single statements.

### `rune console`
- `rune console [--broker <aeron channel>] [--cli-egress <ch>] [-f script.rql]`; broker also from `RUNE_BROKER` / `RUNE_EGRESS` (existing `buildClientConfig`).
- Interactive: prompt `rune> `, continuation `   ...> ` until a `;` outside comments; executes each statement with one connected client for the whole session; prints `ok topic player v1 (8 partitions)` / `ok topic player v2` or the error (`line:col: message` for parse errors, the broker error name for broker errors); an error never ends the session. `exit` / `quit` / EOF leaves.
- `-f script.rql`: same execution over a file; stops at the first error, exit code non-zero.
- Statements in 2b: `topic …;`, `transient topic …;`, `alter topic …;`. (`create view`, `get` arrive with Phase 3.)

### `rune schema`
- Keeps `list`, `get`, `generate`. **Removes `apply` and `alter`**; their offline/live tests are migrated to console tests (same assertions through the console).
- Known limitation carried forward: `schema list` shows only topics known to the session (broker-side listing is not in 2b).

### Gates
- Console tests: multi-statement script, multi-line statement, parse error keeps the session, broker error keeps the session, `-f` stops at first error with non-zero exit, `exit`/EOF; live tests against the existing CLI test gateway.

---

## E. Transient topics

**Today:** a topic can be memory-backed (`PersistenceMode::Memory`, `InMemoryLog`): replicated to a majority before ack, never written to disk. It can only be declared in the broker's static `RUNE_TOPICS` config (`persistence=memory`), not from Rune QL, `defineTopic`, or the console.

**2b (user decision 2026-10-11): memory topics are renamed transient topics and become definable at runtime.**
- **Rename everywhere:** `PersistenceMode::Memory` → `PersistenceMode::Transient`; `RUNE_TOPICS … persistence=transient`; docs, logs, metrics labels. `persistence=memory` stays accepted as an alias that logs a one-line deprecation warning (existing compose files keep working).
- **Rune QL:** `transient topic <name> { … } partitions N key K;` → `TopicDef.persistence`. `alter topic` cannot change persistence (rejected with a clear error).
- **Wire:** `DefineTopic` gains an appended `persistence` byte (0 = disk, 1 = transient; old clients send nothing → disk). `Client::defineTopicAndWait(..., Persistence::Transient)` overload.
- **Broker:** `AdminWorker` creates the runtime topic on `InMemoryLog` when transient. `TopicSchema` records the mode (appended trailer field, same pattern as Phase 1's `topicName`) so restart recovery (`recoverRuntimeTopicSpecs`) recreates the topic as transient: metadata survives, data does not (a restarted node rejoins empty and catches up from peers that still hold it; after a full-cluster restart a transient topic is empty — documented).
- **Views over transient topics:** allowed. A persisted view over a transient topic keeps its checkpoint; a transient view over a transient topic is empty after a full restart. Documented, no special casing.
- **Gates:** Rune QL parse/backend tests; DefineTopic round-trip with and without the byte (old-client compatibility, `*Regression*`); broker test that a transient runtime topic is created on `InMemoryLog`, survives node restart as a transient (empty) topic, and is replicated before ack under `acks=All`; e2e: console defines a transient topic, produce + subscribe work, restart → topic exists and is empty.

---

## F. One producer: transactions everywhere; rewind after abort (added 2026-10-12)

User decision: a transaction must not need a separate producer type, and every produce path must work inside a transaction. Today `TransactionalProducer` is a second implementation (`TransactionalProducerImpl`, plain `send()` only), so batching, `batch()`, `tryClaim`, `claimBatch` and typed `claim` are unavailable in a transaction. The plain producer also draws its producer id client-side (random, no fencing across restarts — `ProducerImpl.cpp` TODO milestone-c), and a consumer group cannot re-read the inputs of an aborted transaction without a process restart (no rewind API).

### F1. Transactions on `Producer`
```cpp
auto p = client.producer({.acks = Acks::All, .transactionalId = "trade-worker-1"});
client.connectAndWait();
p.initTransactionsAndWait();
p.beginTransaction();
p.send(...); p.batch(...).send(); p.tryClaim(...)->commit(); p.claimBatch(...)->commit(); typed.claim(k).commit();
p.sendOffsetsToTransactionAndWait(offsets, "economy");
p.commitTransactionAndWait();          // or p.abortTransactionAndWait()
```
- `ProducerConfig::transactionalId` (empty = non-transactional, today's behaviour). Producer gains the full lifecycle now on `TransactionalProducer`, same names and semantics: `initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`, `commitTransaction`, `abortTransaction`, `flush`, their `*AndWait` forms, `state()` (`TxnState`), `producerId()`, `producerEpoch()`, `transactionActive()`.
- Every produce path participates while `InTransaction`: records carry the transactional bit; the first record to a partition in a transaction emits `AddPartitionsToTxn` and that partition's records are held until it is OK (today's F14 gating, now in `ProducerImpl`); the added-partition set is a preallocated per-partition flag (zero steady-state allocation).
- Misuse is explicit: lifecycle calls on a producer without `transactionalId` → token forms `OpResult::InvalidArgument`, `*AndWait` `std::logic_error`; a produce on a transactional producer outside `InTransaction` is refused (`trySend`/`tryClaim`/`claimBatch` return the refusal, `send` throws `std::logic_error`) — never silently dropped.
- `acks` on a transactional producer: `None` is rejected at creation (a transaction needs acks); `Leader`/`All` as configured.
- `TransactionalProducer` and `Client::transactionalProducer()` are **removed** (pre-1.0; no alias). Their tests migrate to `Producer`. The transaction state machine moves into `ProducerImpl` as explicit states (CLAUDE.md rule 3); `TransactionalProducerImpl` is deleted, not wrapped.

### F2. Broker-assigned producer ids for every acked producer
A non-transactional producer with `acks = Leader|All` obtains its id from the broker (`InitProducerId` with no transactional id, as Kafka's idempotent producer) on connect, replacing the client-drawn random id; retries stay deduplicated across reconnects. `acks = None` keeps `-1/-1/-1` (non-idempotent).

### F3. Consumer-group rewind
- `GroupConsumer::seekToCommitted()` (+ `seekToCommittedAndWait`, token form): for every assigned partition, re-fetch the committed offset (the existing OffsetFetch-on-assign path) and restart its push subscription there; records after it are delivered again.
- `GroupConsumer::seek(TopicPartition, int64_t offset)`: restart one assigned partition's subscription at `offset` (not assigned → `InvalidArgument`).
- Exactly-once loop on failure: `abortTransaction` → `seekToCommitted` → `beginTransaction` (no restart).

### Gates
- Tests (client, real broker integration where the path crosses the wire): each produce path (send, batch, tryClaim, claimBatch, typed claim) inside a committed transaction is visible to a ReadCommitted reader and invisible after abort; mixed paths in one transaction; produce outside a transaction refused; lifecycle on a non-transactional producer refused; fencing by a second producer with the same `transactionalId`; in-doubt commit retry; plain acked producer gets a broker id and dedups a retried batch across a reconnect; `seekToCommitted` redelivers exactly the uncommitted records after an aborted consume→produce transaction (exactly-once e2e: output count == input count after an injected abort).
- Benchmarks: transactional `send` / `tryClaim` throughput and per-record p50/p99 vs non-transactional; 0 allocations steady state on every path.
- Docs: site Transaction / Delivery guarantees sections use `Producer` with `transactionalId` and `seekToCommitted`.

---

## Out of scope for 2b (Phase 3)
Views are **persisted by default** (checkpointed fold state, recovered on restart — unchanged from Phase 1/2a); when `create view` lands in Rune QL, persistence is the default and `transient` is the opt-in.
`create transient view` syntax (transient views themselves already exist via the API), composite keys with a partition prefix (`key (userId, characterId) partition by userId`), co-partitioned 1:1 and 1:N joins, `create view` / `get` in Rune QL and the console, generated view decoders, broker-side topic listing, multi-partition aggregates (unlocked by composite keys).

**View queries (added to Phase 3, 2026-10-12, user decision):** SQL `select <cols|*> from <view> where …` in the `rune` console and from code, alongside the short `get <view> (k1, k2);` (kept — user decision: it is the point-read shorthand for `select * … where <full key>`). Planning by the `where` clause: equality on the full key → point read (one row); equality on the partition-key prefix (`userId = 42`) → scan of that one partition; further predicates (`and level > 20 and zone = 4`) → filtered on the broker over that bounded range; no partition-key equality → rejected (`… scans all N partitions (add 'allow scan' to run it)`) unless `allow scan` is given (secondary indexes later). Client: `client.prepareAndWait(sql)` parses/plans once; `q.executeAndWait(args...)` returns a row cursor, zero allocation per execute; rows decode with the generated view decoder. `getViewRow(view, ViewKey(...))` stays as the shortest point read from code. Site 0.5.19 documents this.

**View queries (added to Phase 3, 2026-10-12, user decision):** SQL `select <cols|*> from <view> where …` replaces the console's `get`, in the `rune` console and from code. Planning by the `where` clause: equality on the full key → point read (one row); equality on the partition-key prefix (`userId = 42`) → scan of that one partition; further predicates (`and level > 20 and zone = 4`) → filtered on the broker over that bounded range; no partition-key equality → rejected (`… scans all N partitions (add 'allow scan' to run it)`) unless `allow scan` is given (secondary indexes later). Client: `client.prepareAndWait(sql)` parses/plans once; `q.executeAndWait(args...)` returns a row cursor, zero allocation per execute; rows decode with the generated view decoder. `getViewRow(view, ViewKey(...))` stays as the shortest point read. Site 0.5.19 documents this.

## Global constraints
CLAUDE.md hard rules (shared-nothing, rings only, state machines, zero steady-state allocation, test + benchmark everything, `*Regression*` test per fix, full Regression suite first, fix-on-sight). Wire enums append-only. Nothing pushed without consent. Commit attribution `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
