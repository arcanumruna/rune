# Wide view keys, prefix reads and write-time view constraints

Status: design, 2026-10-22. Not yet approved.

## 1. Why

The RegressorsPath backend is moving off Postgres onto Rune, one service at a time. The first two services, auth-server and character-server, need three things Postgres gives them today and Rune lacks:

- A character name that is unique without regard to case.
- At most 12 characters per user, with no race between concurrent creates. (Today's Postgres version does race.)
- A list of one user's characters.

The owner's design rules, as stated:

- Uniqueness and limits are enforced through materialized views. A write returns success only once the views reflect it. A write that would break a rule fails and leaves no trace.
- Definitions are written in Rune QL, not built in code.
- A delete is a tombstone. Views follow tombstones automatically, so nothing filters on an `isDelete` field.
- Views are clustered indexes. There are no array or `collect()` columns: a user's characters are rows under a composite key.
- The 12-character cap is a `check` constraint on a `count(*)` aggregate view.

Target definitions. Each of these must work end to end when the work is done:

```sql
topic character {
  characterId uint64, userId uint64,
  name string(16), gender uint16,
  hairSkin uint16, hairSheet uint16, headSkin uint16, /* …the rest of the appearance fields… */
  createdAt int64, lastPlayed int64
} partitions 1 replicas 3 key characterId
  with (cleanup.policy = compact, tier = majority);

create view character_by_id key characterId as select * from character;

create unique view character_name key lower(name) as nameKey as
  select characterId from character;

create view account_characters key (userId, characterId) as
  select name, gender, hairSkin, /* … */ lastPlayed from character;

create view account_stats key userId as
  select count(*) as characters from character group by userId
  constraint max_characters check (characters <= 12);
```

```sql
select * from account_characters where userId = 42;              -- prefix read
select characterId from character_name where nameKey = lower('Foo');  -- point read
```

## 2. Goals, non-goals, anti-goals

**Goals**

- G1. View keys of up to 64 bytes: composite keys of several columns, and key expressions (`lower(field)`).
- G2. Point reads by a full key of any width. Prefix reads by the leading key columns.
- G3. `unique` projection views. A write that would give a key to a second source record is rejected.
- G4. `check` constraints on aggregate views. A write that would push a group past the limit is rejected.
- G5. Rejection happens before the append. A rejected batch takes no offset, uses no producer sequence and has no effect on any view.
- G6. Correct under:
  - concurrent writers
  - writes within one batch
  - transactions that commit or abort
  - ack timeouts
  - leader failover
  - restart
  - log truncation
- G7. Rune QL `create [unique] view … as select …`, `select … from <view> where …`, and CLI output for both.
- G8. Topics without constraints pay nothing on the produce path, apart from one branch that is never taken.

**Non-goals for this work**

- Ordered indexes and range scans (`between`, `<`, `order by`).
- Unique or derived-key views over sources with more than one partition. This needs partition-key alignment and comes later.
- Joins, cascades across topics, and views over views.
- `where` filters inside a view definition.
- Check constraints on anything other than `count` with an upper bound.
- Constraints over transient sources. A constrained view needs a persisted source, because a transient ring does not survive failover and the overlay could not be rebuilt from it.

**Anti-goals.** Any of these makes the work a failure even if the features otherwise run:

- A rule can be broken, even for a moment, by any interleaving the system allows.
- A client is told a write failed when it was in fact stored, or the reverse.
- Throughput or latency gets worse for topics that have no constraints.
- The hot path allocates, takes a lock or makes a virtual call.

## 3. Must land first

**#537: views fold records from aborted transactions.** The constraints treat view state as committed truth, so this must be fixed and re-folded first.

**3.1 Every topic is typed, and every write is validated** (owner decision, 2026-10-22)

Today Rune allows writes that don't match any schema:

- **Untyped topics.** `topic events partitions 3;` has no field list and accepts opaque bytes (`Parser.cpp:583`, `Backends.cpp:52`).
- **Typed topics that don't validate.** Validation is opt-in through `--validate` (`CatalogCommands.cpp:250`) and the `validate` argument of `createTopic` (`Client.h:782`). It defaults to false, so the broker accepts any bytes, and the view fold then silently counts undecodable records as `skippedMalformed`.

Such records would bypass every constraint, intent and `autoincrement` rule.

**Rules**

- **`topic <name>` requires a field list.** Opaque payloads become an explicit field: `topic events { payload bytes } partitions 3;`.
- **`key` stays optional.** A topic without a key is an append-only event stream, routed round-robin or sticky like Kafka's null keys.
- **Validation is always on.** The `validate` flag, `--validate` and the `validate` parameter are removed.
  - Every record must decode against a schema version the topic knows, or the whole batch is rejected with `SchemaValidationFailed` and never appended.
  - Control batches are exempt.
- **The wire key must match the key field.** On a keyed topic, the record's wire key must equal the encoded value of its key field. The only exception is the `autoincrement` placeholder, which has 0 in both.
  - Without this rule, routing, compaction and the view identity follow the wire key while readers and view columns see a different value.
  - The view fold already decodes the key field as an output column and the wire key as the identity (`ViewFold.cpp:162-188`), so a mismatch splits them.
- **Rune's internal `__` topics** keep their own fixed formats. Users can't create `__` names.

**How it fits with each feature**

| Feature | Effect |
|---|---|
| Hot path | Validation moves from the `validatedTopics_` hash lookup to the per-partition pointer used in §6.2. The decode is the same single pass as the constraint hook and uses the fold's descriptor cache. A `{payload bytes}` schema costs a header and length check. The latency gate is an ABAB n=3 run on the existing produce benches, with no regression allowed. |
| Schema evolution | A record is still validated against its own writer version (`schemaRejects_` already selects by exact `(topicId, version)`). An unknown version is rejected. |
| Transactions | Data batches are validated. Commit and abort markers are control batches and are exempt. |
| Compaction, views, filters, intents, `autoincrement`, constraints | All of them need a typed topic. Every one except filters also needs a key. A missing key is refused at DDL time with a clear message, not at write time. |
| Transient topics | Same rules. `transient topic t { … }`. |
| Raw `send(topic, key, bytes)` | Still allowed. The bytes must be a valid encoded record, or they are rejected by validation. The typed API is the normal path. |
| Existing clusters | Behind the new feature level: DDL creating an untyped topic or setting `validate = false` is refused. Topics that already exist are converted when the level is raised. An untyped topic becomes `{ payload bytes }`, keeping its old bytes readable as the payload: a catalog `AlterTopic` with a v1 schema that maps the old records verbatim (the builder confirms this reads old records). Validation turns on for every typed topic. A producer that was writing garbage then starts getting `SchemaValidationFailed`, which is the intended outcome. |
| Repo ripple | All of these move to `{ payload bytes }` (or a real schema) in the same part: the e2e topic scripts (`broker/docker/topics/*.rql`, `catalog-e2e.sh`), `broker/bench/kafka-compare/lib.sh`, the DR harness (`broker/dr-e2e/harness/cluster.py`), the QL, CLI and console tests, the protocol conformance vectors, `README.md`, `client-cpp/README.md`, `docs/api/ql.md`, `docs/benchmarks.md`, `SELF-HOSTING.md` and `DOCKERHUB.md`. |

## 4. Keys

**4.1 Key forms**

- `key <col>`: a single column. This is today's form.
- `key (<col>, <col>, …)`: a composite key.
- `key lower(<col>) as <name>`: a key expression. It is valid only on `string(N)` columns.
  - The key column appears in the output under `<name>`.
  - It holds the lowercased bytes.
- `lower` maps ASCII A–Z to a–z and leaves every other byte unchanged.
  - Character names are `[A-Za-z0-9]{1,16}`, so this is exact for the first users.
  - Locale-aware folding is a non-goal, and the docs must say so.
- Key columns must have fixed width: integers, `bool`, `string(N)`. Var-length `string` and `bytes` are rejected at compile time.
- The total key width K is at most 64 bytes.

**4.2 Identity rule.** A projection view that is not `unique` must be functionally keyed by the source record: its key must include the source key column. `account_characters` includes `characterId`, so it passes.

A projection whose key could collide across source records must be declared `unique`. The compiler rejects it otherwise, with the message "key does not identify the source record; declare the view unique or include <sourceKey>". This closes the "two records, one view row, last writer wins" hole.

**4.3 Encoding.** Key bytes are order-preserving, so a later ordered index or checkpoint needs no re-encoding:

- Unsigned integers: big-endian.
- Signed integers: big-endian with the sign bit flipped.
- `bool`: one byte.
- `string(N)`: N bytes, zero-padded.

Names cannot contain NUL, so zero padding cannot make two different names collide. In general a collision can only cause a false reject, never a false accept.

**4.4 Partitioning.** Derived keys and composite keys need a one-partition source in this work, the same rule aggregates follow today (`ViewNotCoPartitioned`). Reads for such views always route to partition 0.

## 5. Storage (`ViewTable`)

**5.1 Index slot.** It stays 13 bytes: `tag u8 | word u64 | rid u32`.

- If K ≤ 8, `word` holds the raw key, as today, and the compare is exact with no trip to the arena. Existing views get no slower.
- If K > 8, `word` holds a cached 64-bit hash of the key bytes.
  - A lookup matches on tag and then hash, and only then compares the key bytes in the row (`memcmp`).
  - Growth migration and backward-shift deletes use the cached hash and never touch the arena.

**5.2 Hash.** The index hash is a seeded 64-bit hash, such as rapidhash, with a random seed per worker. Players choose names, so an unseeded hash would let them craft probe chains and stall the leader thread.

The hash never leaves the process. Partition routing keeps its deterministic FNV-1a.

**5.3 Row.** For K > 8 the key bytes are stored once in the arena row, next to the existing null bitmap and slots. The exact layout is the builder's call, but the key bytes must not be stored in a way that can diverge from the key column values.

**5.4 Prefix index.** A composite key of n columns gets exactly one prefix index, over its first n−1 columns. A shorter-prefix read is rejected at compile time with "needs an ordered index".

- The prefix index is a second hash index from prefix hash to the head rid. A match is confirmed by comparing the prefix bytes in the head row, so no prefix bytes are stored separately.
- Rows sharing a prefix are linked through `prev`/`next` rid fields in the row. Insert and erase are O(1).
- A scan walks the list. The order is unspecified. The client sorts if it needs an order; character-server sorts by `characterId`.

**5.5 Owner map.** This applies to views whose key is not the source wire key (derived or composite).

- A per-instance table maps the source wire key to the rid of its current view row. A tombstone carries no value, so this map is the only way to find the row to erase.
- On an update, the fold computes the new view key. If it differs (a rename, say), the fold erases the old row and then inserts the new one.
- This is the same retraction pattern as the aggregate `contributors` table.

**5.6 Checkpoint v3.**

- A new magic number.
- Entries carry the K-byte key in place of `key u64`.
- The owner map and prefix index are a new table id, or are rebuilt on replay. The builder chooses, but replay must reproduce identical lookup behavior.
- The hash is never persisted; replay recomputes it.
- v1 and v2 checkpoints still load.

**5.7 Unique conflicts during folding.** After §6 these can only come from data that existed before the unique view was created.

- The fold keeps the first source record by offset as the owner.
- A later conflicting record is not inserted into the unique view. It is counted in a `uniqueConflicts` stat, which `show view` reports.
- Other views over the same topic still contain that record.
- This behavior is deterministic across leaders and restarts.
- Migration back-fills must remove duplicates first, the same way Postgres migration V14 did.

## 6. Write-time constraints

**6.1 What is checked**

- `unique` view V: a record whose derived key is already owned by a different source key, in V or in the overlay (§6.3), is a violation.
- `check (c <= N)` or `check (c < N)` on a `count(*)` / `count(col)` aggregate view: the group's folded contributor count plus its pending increments, minus its proven pending decrements (§6.4), must stay within the bound after the record.
  - A record counts as an increment only if it adds a new contributor to that group.
  - An update to an existing member of the same group is not an increment.
  - A move between groups is an increment for the new group and a decrement for the old one.
- A retraction (a tombstone, or a move out of a group) is never rejected. A group already over its limit may shrink.
- Any other check form fails at compile time with "check supports count(…) <= / < constant".

**6.2 Where it runs**

The check runs on the partition leader's produce path, after idempotent dedupe and before the append. Concretely, it sits in `handleProduceBatch` between `producerState.validate` (`ProducePath.cpp:205`) and the min-ISR gate and append.

It must come after dedupe. Otherwise, a retry of a batch that was already stored could be rejected (create X, delete X, retry the create) even though the original write succeeded.

The hook is a template policy parameter, not a virtual call or a `std::function`.

- **Zero cost when unused.** The constraint state is reached through a pointer stored next to the partition's `RcSlot`. A partition without constraints has `nullptr`, so the hook is one `[[unlikely]]` branch.
- **When the hook runs:**
  1. Decode only the fields that feed keys or groups, using the fold's per-writer-version descriptor cache.
  2. Compute each key's hash once and probe both the view table and the overlay with it.
  3. Check every record of the batch in order against a scratch overlay, so a batch can collide with itself (13 creates, or two names that collide).
  4. If any record violates a rule, reject the whole batch. Nothing is appended and nothing is reserved.
  5. If the batch passes, merge the scratch entries into the overlay, tagged with the offsets the append assigns. This happens on the same thread, with nothing interleaved.
- **Several views.** One record can be checked against several constrained views (`character_name` and `account_stats`). It must pass all of them.

**6.3 The pending overlay.** Views fold only up to the LSO, which is bounded by the HW. The overlay covers records between the fold cursor and the log end offset (LEO).

- It is a fixed-capacity, pre-sized, offset-ordered structure for each constrained source partition.
- Each entry is `{offset, sourceKey, per-view effect, txn pid or none, aborted flag}`.
  - For a unique view, the effect is the key it claims and the key it releases.
  - For a check view, it is the old group and the new group.
- Lookups are by key hash, and also by source key, so that a later pending write sees an earlier pending write to the same record (create then rename, or create then delete then create).
- **Release:**
  - Entries leave from the front once the view's fold cursor has passed their offset. Fold cursors are per view; an entry leaves only when every constrained view on the partition has passed it.
  - An entry is marked aborted when its transaction's abort marker is applied (`rc->onMarker`), and stops counting at once.
  - Entries at or above a truncation point are dropped from the back.
  - **An entry is never released when its ack times out.** The record is already in the log.
- **Capacity.**
  - The capacity is fixed per constrained view: a sizing knob, pre-allocated when the view is installed. The default is 65 536 entries (my call).
  - The in-flight ack bound does not bound it. One open transaction pins the LSO, so every later write stays in the overlay until that transaction ends.
  - The transaction timeout bounds how long that can last. For a topic like `character`, which sees a few creates a second, the default capacity covers far more than any timeout.
  - If the overlay is full, the batch gets a retriable refusal, `TransientLogFull`, which clients already retry with the same identity. A full overlay is also a stat and a log line.

**6.4 What a pending entry may be counted for.** The rule: a definite rejection or acceptance may depend only on state that can no longer vanish without the dependent write vanishing too.

- **Increments always count.** That covers a pending claim of a name or a slot, whether it is non-transactional, from an open transaction, or committed. So two open transactions can't both take the 12th slot.
- **A pending release counts in three cases.** A release is a tombstone, a rename freeing a name, or a move out of a group. It counts when:
  - (a) the releasing record is not transactional,
  - (b) its transaction has applied a commit marker, or
  - (c) it belongs to the same open transaction as the later write.

  Case (a) is safe because the log is one ordered sequence. A truncation removes a suffix, so if the releasing record is cut, any later record that relied on it is cut too.

  An abort is the only way an earlier record vanishes while a later one survives. That is why a release from another transaction that is still open never counts.
- **Source keys locked by another transaction.** A write to a source key that has a pending entry from a different, still-open transaction is refused retriably: `ConcurrentTransaction`, an existing retriable code; if none fits, a new retriable code 52 (the builder checks which).
  - Without this, the effect of the later write (update rather than create, same group) would be computed against a record that may be aborted. A missed increment would break the cap.
  - It works like a row lock held until the transaction ends. Non-transactional writers and same-transaction writers don't conflict with it.
- When an abort marker is applied, its entries are marked aborted and stop counting. Rule (c) and the lock mean no other pending entry can depend on them, so nothing needs to be recomputed.

**6.5 Readiness gate.** A constrained write gets retriable `NotLeader` until all of the following hold under the current leadership generation:

- RC is Ready (as `rcGate_` requires today).
- Every constrained view instance on the partition is Ready: its fold cursor has reached the LSO bound, and the HW has reached `leadershipStartOffset()`.
- The overlay has been rebuilt by decoding `[foldCursor, LEO)` from the local log. Records of open transactions come from `RcState`.

The overlay is rebuilt whenever `leadershipGeneration` or `logGeneration` changes, and cleared when the node is not the leader. This follows the `RcState` model: convergence from the log with an idempotent restore, never a grace timer.

Restart and view rebuild (`RebuildingViews`) use the same gate.

**6.6 Changing constraints**

- **Create a constrained view.** Writes to the source are refused, retriably, from the moment the view is installed on the leader until it is Ready. After that, enforcement applies to every write. Groups already over the limit are not repaired; they cannot grow until they shrink below it.
- **Alter (`create or replace`).**
  - The old view's constraints stay in force until the new instance is Ready.
  - Then both are swapped in one worker step.
  - There is never a window with no constraint.
  - Drop then create does leave a window, and the docs say so.
- **Drop.** The constraint is removed when the drop is installed on the leader.

**6.7 Write intents and typed delete**

Today every write is an upsert, and a delete is a record with an empty value. Rune treats an empty value as a tombstone (`LogCleaner::isTombstone`, the view fold), where Kafka uses a null value. A create and an update can't be told apart.

- **Wire.** Each record has an `attributes` byte (`RecordBatch.h:146`). It is always 0 today. Readers skip it, and compaction copies it unchanged (`BatchRebuild.h`). Bits 0–1 of that byte carry the intent:

  | Intent | Bits | Value | Broker rule | Rejection (definite) |
  |---|---|---|---|---|
  | upsert | 0 | row | none (today's behavior; the default) | — |
  | insert | 1 | row | key must not exist | `KeyExists` (new code 53) |
  | update | 2 | row | key must exist | `KeyNotFound` (new code 54) |
  | delete | 0 | empty | none (idempotent, as today) | — |
  | delete existing | 2 | empty | key must exist | `KeyNotFound` |

  Bit pattern 3 is reserved and rejected with `InvalidRequest`.
- **Key index.** A per-partition set of the keys that exist, enabled per topic with `with (key.index = on)`.
  - It uses the same machinery as a projection view keyed by the source key, without the columns, at about 16 bytes per key.
  - The intent check runs in the §6.2 hook against the key index plus the overlay, under the same §6.4 counting rules: a pending insert counts at once, and a pending delete counts only under (a), (b) or (c).
  - It is subject to the same readiness gate (§6.5).
  - An intent other than 0 sent to a topic without a key index is refused with a definite `InvalidRequest`.
- **What the log says.** A write whose intent fails is never appended, so the intents in the log are what actually happened. `insert` was a create, `update` was an update, and a tombstone with intent 2 deleted an existing key.
  - Compaction only removes superseded records, so a key's original `insert` record disappears once a later update replaces it.
  - A consumer reading compacted history must therefore treat records as upserts. The intent is exact only for records read before compaction.
- **Compatibility.** The client sends a non-zero intent only when the cluster feature level (`kViewKeysConstraints`) supports it. An older broker ignores the bits and would silently upsert, so against an old cluster the client fails the call locally.
- **Client API.**
  - Raw producer: `send(topic, key, value, WriteIntent)`, with the intent defaulting to `Upsert`.
  - Typed (`TypedTopic<T>`):
    - `insert(rec)`, `update(rec)`, `upsert(rec)` (alias of today's `send`)
    - `remove(key)`: a tombstone
    - `removeExisting(key)`: a tombstone with intent 2
    - each of these also has an `…AndWait` form
  - `remove` and `removeExisting` take the topic's C++ key type, so the key bytes are always encoded exactly as `send(rec)` routes them. Today a delete needs the raw producer and a hand-encoded `Key`, and a wrong key width silently deletes the wrong key or nothing.
- **Consumer.** `record.op()` returns `Insert`, `Update`, `Upsert` or `Delete`.

## 7. Errors and the client

**7.1 Broker**

- New `ErrorCode::ConstraintViolation = 51`.
  - It means: not stored, because when the check ran, accepting the write would have broken the rule.
  - A pending claim that is later truncated or aborted can make that conflict disappear. A retry is then a legitimate client choice. The guarantee is only "not stored", never "the rule would still be broken now".
- The violated constraint is sent as a u64 id in the `logStartOffset` slot of `ProduceResponse`. This is the same trick D6 uses, so the fixed frame does not grow.
  - The id is `viewObjectId << 16 | constraintOrdinal`. Ordinal 0 means the view's `unique` rule.
- **Who may write to a constrained topic.** The partition answers a definite `InvalidRequest` (or the nearest existing definite code; the builder checks) to:
  - a non-idempotent producer (pid −1). Such writes skip dedupe and `rcGate_`, so neither the "after dedupe" argument nor the readiness gate covers them;
  - acks other than `all`. Today an `acks=leader` write is acked before the HW is reached and before the views fold, which breaks the owner's rule that success means the views reflect the write;
  - a client below version 12. Older clients treat an unknown code as "outcome unknown" and bump their epoch, so they can't use these topics safely.
- `docs/api/wire-protocol.md` documents the code, the version gate, the slot and these refusals.

**7.2 Producer**

Today's producer can't take a definite reject cleanly:

- Any unknown code, and code 24, goes to "outcome unknown": `markUnresolved_` plus an epoch bump.
- The out-of-order-sequence branch (C4) resequences and bumps the epoch as well.
- Batches already in flight behind a rejected one arrive out of order.

So for partitions of constrained topics:

- **One batch in flight per partition.** The client learns a topic is constrained from topic metadata, using a new flag set when a constrained view is installed.
  - With nothing in flight behind it, a rejected batch leaves the producer's next sequence unchanged. The next batch reuses that sequence, with no epoch bump and no resequence.
  - The cost is throughput on these partitions, not latency. Constrained writes are low-rate control-plane writes such as creates and renames.
- **Tier-1 `send()` coalescing.** If a coalesced batch of unrelated records is rejected with 51, the producer resends its records one at a time, in their original order, still one in flight.
  - The failing record's sequence isn't consumed, so the next record reuses it.
  - Each record then gets its own result: the one that breaks the rule gets `ConstraintViolation`, and the others succeed.
- **Explicit atomic batches (`p.batch(...)`)** are never split. The whole batch fails.
- **Inside a transaction,** error 51 aborts the transaction, the same way a constraint violation does in Postgres. The caller sees the transaction fail with the constraint id.
- **Result type.** The result exposes `constraintId`. A helper on `Client` resolves it to a `(viewName, constraintName)` pair from the catalog.

**7.3 CLI.** Fix `errorCodeName`: codes 48–50 are missing today. Add 51. Print the name of the violated constraint.

## 8. Reads

**8.1 Messages**

- **`GetViewRowK`** (template 81): a full key of any width as length-prefixed bytes, plus `requestedFingerprint`.
- **`ScanViewRows`** (82) and **`ScanViewRowsResponse`** (83):
  - Request: the prefix bytes, a `limit` capped by the broker, and a resume token.
  - Response: a status, a row count, length-prefixed rows and the next resume token.
- Both run through the existing pipeline:
  - the gate in `viewRowGate_`
  - READ on VIEW authorization
  - leader-only serving
  - the read lease
  - the fold barrier (#82), so a client reads its own acked writes
  - the restart-readiness gate
- `GetViewRow` (72) stays for views with K ≤ 8.

**8.2 Bounds**

- A scan response stays under the 68 KiB egress scratch. The worker stops at whichever comes first: `limit`, the byte budget, or a fixed maximum number of rows per op. This bounds work on the leader thread.
- A single row that doesn't fit still returns `kRowTooLarge`.

**8.3 Consistency**

- **One page is atomic.** It is served in one worker step, so it reflects one fold position.
- **Across pages there is no snapshot.** The resume token is the last returned key.
  - If that key is gone when the next page is requested, the response is `kCursorInvalidated` (a new status), and the client restarts the scan.
  - A user's character list (≤ 12 rows) always fits in one page.

**8.4 Client**

- `getViewRowAndWait(view, keyBytes)` and `scanViewAndWait(view, prefixBytes, limit)`.
- A typed key builder that encodes values per §4.3 from the view's output descriptor, so callers never hand-encode bytes.
- New `OpKind`s. Results larger than the completion-ring reservation get `ResultTooLarge`, as today.

## 9. Rune QL and the CLI

**9.1 Grammar additions**

- New tokens: `*`, `<`, `<=`, `>`, `>=`.
- `create [or replace] [transient] [unique] view <name> key <keyspec> as select <items> from <topic> [group by <cols>] [constraint <name> check (<col> <op> <int>)]*`
  - `<keyspec>` is `<col>`, `(<col>, …)` or `lower(<col>) as <name>`.
  - `<items>` is `*`, `<col> [as <name>]`, or an aggregate: `count(*)`, `count(col)`, `sum`, `avg`, `min`, `max` or `last`, with `as`.
- `select <cols | *> from <view> where <keycol> = <lit> [and …] [limit n]`
  - All key columns: a point read.
  - The leading n−1 columns: a scan.
  - Anything else: a parse-time error.
  - Literals may be wrapped in `lower('…')`, which applies the same rule as §4.1.
- The constraint is enforced as soon as the view is Ready, so the DDL has no separate enforcement switch.

**9.2 Pipeline**

- **AST.** `CreateViewDef` and `SelectDef`, appended to the `Statement` variant.
- **Plan.** `prepare` resolves the source descriptor from the catalog and produces the authored `ViewPlan` v2. The author type moves to a library that both `ql` and the client can include.
- **Plan v2 adds:** key components (column refs plus an optional `lower`), `unique`, and constraints `{name, column, op, bound}`. The authored and compiled versions both go to 2. Old v1 blobs still decode.
- **Catalog.** The new fields live in `normalizedPlanBlob`, so they affect the fingerprint. Update the golden catalog logs.
- **Rolling upgrade.** DDL that uses any v2 feature is refused until the cluster feature level (`MetadataFeature`) reaches a new `kViewKeysConstraints` level. Brokers on the old version never see a plan they can't compile.
- **Compile errors.** They get explicit codes: new `kViewKeyExpressionInvalid`, `kViewConstraintInvalid` and `kViewUniqueInvalid`. The current mapping searches the message for "key"; that goes away.

**9.3 CLI.** `rune ql "select …"` decodes rows using the view's output descriptor, read from the catalog. It also supports the existing table, kv and json output formats if they exist; otherwise just table.

## 10. Observability

Per constrained view:

- `constraintRejects` for each constraint
- overlay occupancy and its peak
- overlay-full refusals
- gate refusals by reason
- `uniqueConflicts` (§5.7)

Per scan: rows returned and pages returned.

All of these show in `show view` and the metrics export.

## 11. Broker-assigned keys (`autoincrement`)

The owner's requirement: ids are generated in the backend, safely and fast.

**11.1 How the six databases surveyed do it** (Vitess, Citus, TiDB, CockroachDB and YugabyteDB from source, Spanner from docs):

- **A central counter that hands out cached blocks.** Vitess (`execNextval`, `next_id` + `cache`), TiDB (`HInc` on the meta key, 30 000-id windows), YugabyteDB (`sequences_data`, blocks of 100) and CockroachDB SQL sequences.
- **A disjoint high-bit range per node.** Citus (`groupId << 48`) and TiDB `AUTO_RANDOM`.
- **A stride per writer.** MySQL `compute_next_insert_id` (`offset + N×increment`), which Group Replication sets automatically.
- **Time plus node id.** CockroachDB `unique_rowid()`.
- **Bit-reversed ids.** Spanner and CockroachDB `unordered_rowid` use these to avoid hot ranges in range-partitioned stores. Rune hash-partitions, so it doesn't need them.

All of them accept gaps and give no global ordering across nodes. None makes explicit ids collision-proof. The id is always assigned in a trusted server tier, never by the application.

Rune combines the MySQL stride with the durable block reservation of Vitess, TiDB and YugabyteDB, and puts both on the partition leader. This is possible because Rune's partition count is immutable, which a range-partitioned or reshardable store cannot assume.

**11.2 Declaration**

```sql
topic character {
  characterId uint64 autoincrement, userId uint64, name string(16), …
} partitions 1 replicas 3 key characterId
  with (cleanup.policy = compact, key.index = on);
```

- **What it applies to.** `autoincrement` is valid only on the key field, typed `uint64`, `int64` or `uint32`, and the key is mandatory.
- **Implied settings.** It implies `key.index = on` and a modulo partitioner (§11.3).
- **Immutable.** The key field, its type, `autoincrement` and the partition count are all immutable. Today's rules already prevent dropping the key (`Evolve.cpp:100`) and retyping it, and partitions can't be altered (`alter topic … set partitions` is a parse error).

**11.3 Routing: the modulo partitioner**

- An autoincrement topic routes a key by `id % P`, not by `fnv1a(key) % P`. The partitioner is a topic property, so producers, the gateway and the read path all use it.
- Partition `p` assigns only ids `≡ p (mod P)`:
  - The n-th id is `n·P + p`, where `n` is the partition's counter.
  - `n` starts at 1, so id 0 is never assigned.
- Ids stay small and dense across the whole topic, and are JavaScript-safe. The broker refuses to assign above `2^53 − 1`, and the topic can set a lower `autoincrement.max`.
- A batch of k new records gets `base, base+P, …, base+(k−1)P`, so the ack only needs to carry `base`.
- With `partitions 1` the ids are just 1, 2, 3, … and increase globally.

**11.4 Assignment on the write path**

- **The client's part.** The client sends a create as `insert` with key **0** (an 8-byte placeholder), and the key field in the value also set to 0. It routes the batch with a sticky partitioner per producer, as Kafka does for null keys, so each batch targets one partition.
- **The leader's part.** It runs in the §6.2 hook: after dedupe, before the append, on the leader thread.
  1. Take k ids from the in-memory counter.
  2. Overwrite the 8 placeholder bytes of each record's key, and the key field in its value. The value field's offset comes from the writer-version descriptor cache shared with the fold. Lengths don't change, so the batch is never re-encoded.
  3. Recompute the batch CRC32C (hardware-accelerated).
- **Cost.** Nothing is allocated, nothing is looked up, and there is no extra hop.
  - The target is under 200 ns per batch on `ProduceValidateBench` (my call).
  - Topics without `autoincrement` pay only the §6.2 null-pointer branch.
  - A record auto-assigned an id skips the key-index existence check, because the counter makes it unique by construction.

**11.5 Durability: reservation records**

- **Reserving.** Before an id above the current reservation is used, the leader appends a control record to the same partition log, "reserved through n = R". It goes in on the same thread, immediately before the data batch. Its step is `autoincrement.reserve`, default 4096.
- **Replication.** The reservation replicates like any record. The data batch is committed only once the HW passes it, and the reservation sits below it, so it is committed first. No separate sequence store or replication path is needed.
- **Failover and restart.** The new leader reads the latest reservation in its log and resumes at R+1. The unused rest of the block becomes a gap.
- **Truncation.** It cuts a suffix of the log. If a reservation is cut, every data record above it is cut too, so no surviving record's id is ever reassigned.
- **Compaction.** The cleaner keeps the latest reservation record per partition. Otherwise the highest id could disappear once its record is tombstoned and compacted away, and a deleted record's id would be reused.
- **Readiness.** Until the new leader has found its reservation, autoincrement writes are refused with retriable `NotLeader`, as part of the §6.5 gate.

**11.6 Safety rules**

- **Producer requirements.** Writes must come from an idempotent producer with `acks=all`, the same as constrained topics (§7.1). The ack, and so the id, is returned only once the write is committed. A truncated record therefore never gave its id to anyone.
- **Retries.** An idempotent retry that dedupes returns the original offsets and the original `base` id. The broker reads the first key of the stored batch at its base offset. This is a cold path, retries only, from the page cache or the ring, so the producer-state entry doesn't grow.
- **Transactions.** Ids assigned inside a transaction that aborts are burned, leaving a gap, and are never reused.
- **Accepted writes:**
  - `insert` with key 0: assigned by the broker.
  - `insert` with an explicit id: allowed only for a principal with the new `IMPORT` grant on the topic, such as the Postgres back-fill. The broker checks the id against the key index (`KeyExists`), routes it to its owning partition by modulo, and moves that partition's counter past it (`n = id div P`), all on the one leader that could ever assign that id. This is exact, unlike MySQL and TiDB, where explicit ids can still collide.
  - `update`, `remove` and `removeExisting` with an explicit id: always allowed, checked against the key index.
- **Refused writes:**
  - A plain upsert: it could create an id the counter never accounted for (`InvalidRequest`).
  - `insert` with an explicit id from a principal without `IMPORT` (`NotAuthorized`).
  - Assignment past the maximum (`AutoIncrementExhausted`, new code 55, definite).

**11.7 Client API**

```cpp
auto r  = characters.insertAndWait({.userId = u, .name = "Foo", …});   // characterId left 0
auto id = r.key();                                                     // the assigned id
// Writes that depend on the id, e.g. in the same transaction:
skills.insert({id, 98, …}); skills.insert({id, 99, …});
```

- **Reporting the ids.** `ProduceResponse` gets a trailing `assignedBase u64`, gated on client version 12. Older decoders accept the longer block. The fixed scratch arrays in the sink, the gateway and the return carrier each grow by 8 bytes.
- **Per-record ids.** The client works out each record's id as `base + i·P`, and the completion carries it.
- **Async.** `insert(rec)` reports the id in its completion.
- **Writes that depend on a new id** wait for that id's ack. That is one sequential round trip, the same as Postgres `INSERT … RETURNING` followed by the child inserts. Creates are rare control-plane writes, so this is accepted.

**11.8 If the partition count ever becomes mutable.** Changing it remaps every key, autoincrement or not. Before an autoincrement topic changes P, every partition's counter must be raised so that `n·P_new + p` lies above the highest id reserved under P_old. MySQL has the same rule when `auto_increment_increment` changes.

## 12. Hot-path budget and benchmarks

- **Topics without constraints.** Run an ABAB n=3 A/B of `ProducePathBenchmark` and `WorkerProduceLatencyBench`. It must show no regression.
- **Topics with constraints.** The hook adds at most 1 µs p50 to a one-record batch on `ProduceValidateBench` (my call; revisit with data).
- **Folding.** A `ViewFoldBench` case with derived keys plus the owner map stays inside the existing budget of 10 µs per event batch.
- **Table.** `ViewTableBench` gains wide-key and prefix-scan cases. K ≤ 8 views must not change.

## 13. Interaction matrix

Every cell is a test (unit, integration or e2e, wherever it can first be seen). The stress layer model-checks the invariants.

**Operations**

- create
- update of a non-key field
- rename (the derived key changes)
- move between groups (`userId` changes)
- delete (tombstone)
- delete then reuse the name
- duplicate retry

**Entities**

- a new source key
- an existing source key
- the same user
- another user
- a group at 11, at 12, or over the limit (pre-existing)
- a name that is free, taken by another record, or taken by the same record

**Timing and fault**

- one record
- back to back before the ack
- several records in one batch, including one that collides with another in the same batch
- inside a transaction that commits
- inside a transaction that aborts
- two open transactions racing for the last slot or the same name
- a release in a transaction followed by an increase outside it
- an ack timeout, then a retry
- leader failover between the append and the HW
- leader failover between the HW and the fold
- truncation of a pending tail
- restart while the view rebuilds
- the view created on a non-empty topic
- `create or replace` while writes run
- drop while writes run
- a full overlay
- §3.1: an untyped-topic DDL refused, a record matching no schema version rejected, a wire key that doesn't match the key field rejected, control batches passing, a keyless topic refusing views and intents at DDL time, and an existing untyped topic converted to `{ payload bytes }` with its old records still readable
- a write to a source key held by another open transaction, followed by that transaction committing or aborting
- a committed transactional release followed by a reuse before the fold catches up
- a non-idempotent, `acks=leader` or version-11 client writing to a constrained topic
- a coalesced batch with one violating record among valid ones
- every intent × {key absent, key present, key pending insert, key pending delete (transactional and not)}
- `remove` / `removeExisting` through the typed API for every key type width (u16/u32/u64/i64/string(N)), checking that the routed key bytes match `send(rec)`
- an intent sent to a topic without a key index, and to an old-feature-level cluster
- autoincrement: 1 record and k records per batch, P = 1 and P > 1, a batch crossing a reservation boundary, failover right after a reservation and right before one, truncation of a reservation together with its data, compaction after the highest id is deleted (no reuse), an idempotent retry returning the same ids, an aborted transaction (ids burned), an explicit `IMPORT` id below and above the counter, an explicit id without `IMPORT`, a plain upsert refused, exhaustion at the maximum, and an old client (v11) refused
- error 51 inside a transaction

**Reads**

- a point read and a prefix read immediately after an acked write (read your writes)
- a scan across pages with an erase between pages (`kCursorInvalidated`)
- a scan of an empty prefix
- a hash-collision forcing test using a fixed seed and a test hook

**Invariants for the model-checked stress run**

The stress run has racing writers, injected failovers and aborts. At every observable point:

- No user has more than 12 live characters.
- No two live characters share a `lower(name)`.
- Every acked write is visible in every view after the fold barrier.
- No rejected write appears anywhere.
- Every view equals a fresh refold of the log.

**Sanitizers.** ASan, UBSan and TSan runs of `*Regression*` at the checkpoints, as usual.

## 14. Work order

Each part can be shipped on its own:

1. #537, the fix for aborted-transaction folding.
1b. Every topic typed, validation always on, and wire key = key field (§3.1).
2. Keys: wide and derived keys, the owner map, the seeded hash and checkpoint v3. Covers §4 and §5 apart from §5.4.
3. Composite keys, the prefix index, `GetViewRowK`, `ScanViewRows` and the client read API. Covers §5.4 and §8.
4. Rune QL `create view` (plan v2, the feature level) and `select`, plus the CLI. Covers §9.
5. Constraints: the hook, the overlay, the gate, error 51 and the client's definite-reject and split path. Covers §6.1–§6.6 and §7.
5b. Write intents, the key index, and the typed `insert` / `update` / `remove` / `removeExisting` API. Covers §6.7. It reuses the part-5 hook.
5c. `autoincrement`: the modulo partitioner, assignment in the hook, reservation records with cleaner retention, the `assignedBase` ack field, the `IMPORT` grant and the typed `insertAndWait` key result. Covers §11.
6. Docs: `ql.md`, `wire-protocol.md`, `client-cpp.md`, `admin-cli.md`, `CATALOG.md`, the root and client `CLAUDE.md`, and the site's views section, corrected to the shipped syntax.

## 15. Calls made in this design (owner to check)

- The ack stays at acks=all (HW). Read-your-writes comes from the fold barrier on reads, not from making the produce ack wait for the fold. Every read path a client has goes through that barrier.
- Constrained topics require idempotent producers, `acks=all` and client version 12 or later (§7.1).
- One batch in flight per constrained partition (§7.2).
- A constraint violation inside a transaction aborts the transaction.
- A source key with a pending entry from another open transaction is locked until that transaction ends (§6.4).
- Keyless typed topics replace untyped ones. Existing untyped topics are converted to `{ payload bytes }` when the feature level is raised (§3.1).
- `lower()` is ASCII-only. The first users' names are `[A-Za-z0-9]{1,16}`.
- The default overlay capacity is 65 536 entries per view.
- Autoincrement topics use a modulo partitioner (`id % P`). Reservation step 4096. Assignment budget under 200 ns per batch. Dedupe returns ids by reading the stored batch back, so the producer-state entry doesn't grow.

## 16. Left to the builder

- The exact row layout for key bytes and the prefix links.
- Whether the owner map and prefix index are checkpointed or rebuilt on replay.
- The choice of seeded hash.
- How the overlay is structured internally, within the capacity and no-allocation rules.
- The page row cap and the resume token format.
