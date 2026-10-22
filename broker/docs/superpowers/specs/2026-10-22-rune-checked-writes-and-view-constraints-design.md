# Checked writes: typed topics, write intents, broker-assigned ids, wide view keys and view constraints

Status: design, 2026-10-22. Not yet approved.

## 1. Why

The RegressorsPath backend is moving off Postgres and Redis onto Rune, one service at a time, starting with auth-server and character-server. Those two services rely on five things Postgres gives them today and Rune lacks:

| Need | Postgres today | Rune today |
|---|---|---|
| A new user or character gets a unique, small numeric id | `BIGSERIAL` | Nothing. The application picks the key. |
| A create can't silently overwrite an existing row | `INSERT` fails on the primary key | Every write is an upsert. |
| Character names are unique without regard to case | `UNIQUE (LOWER(name)) WHERE NOT is_delete` | Nothing. |
| A user has at most 12 characters | A count, then an insert. It races today. | Nothing. |
| List one user's characters | `WHERE user_id = ?` | Point reads by a `uint64` key only. |

**The owner's decisions (2026-10-22).** These are binding.

1. Uniqueness and limits are enforced through materialized views, and definitions are written in Rune QL.
2. A write that would break a rule fails and leaves no trace. A writer that needs "success only once the views reflect it" adds the new `View` ack flag, as in `Acks::All | Acks::View`. Acks become flags that combine; plain `All` stays (owner decision, 2026-10-22; §8.9).
3. A delete is a tombstone. Views follow tombstones automatically, so nothing filters on `isDelete`.
4. Views are clustered indexes. There are no array or `collect()` columns: a user's characters are rows under a composite key.
5. The 12-character cap is a `check` constraint on a `count(*)` aggregate view.
6. Ids are generated in the backend (the broker), safely and fast. The topic key is mandatory where ids are generated, and it can't be altered.
7. Every typed topic validates every write. A topic declared without a field list is untyped: a raw log of opaque bytes with no database features. Every database feature needs a typed topic.
8. A typed `remove` / `removeExisting` API exists.
9. Strings: `lower()` is ASCII-only for now. A Unicode `casefold()` is reserved for later (§8.1).

**Target.** All of the following must work end to end:

```sql
topic user {
  userId uint64 autoincrement, provider uint16, subject string(64), createdAt int64
} partitions 1 replicas 3 key userId with (cleanup.policy = compact);

create unique view user_identity key (provider, subject) as select userId from user;

topic character {
  characterId uint64 autoincrement, userId uint64,
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
select * from account_characters where userId = 42;                   -- prefix read
select characterId from character_name where nameKey = lower('Foo');  -- point read
```

```cpp
// auth-server, login
auto found = userIdentity.getAndWait({provider, subject});
uint64_t userId = found ? found->userId
                        : users.insertAndWait({.provider = provider, .subject = subject, .createdAt = now}).key();
// If two first logins race, one gets ConstraintViolation(user_identity, conflictOffset). It re-reads with
// getAndWait(key, {.minOffset = conflictOffset}) and uses the winner's userId; that read can't miss the winner.

// character-server, POST /character   (u comes from the verified JWT, never the request body)
auto r = characters.insertAndWait({.userId = u, .name = req.name, …});
//   Ok                                   → r.key() is the new characterId
//   ConstraintViolation(character_name)  → 409 name taken
//   ConstraintViolation(max_characters)  → 400 too many characters

// character-server, DELETE /character/{id}   (ownership is checked against character_by_id first)
characters.removeExistingAndWait(id);   // KeyNotFound → 404
```

## 2. Goals, non-goals, anti-goals

**Goals**

- G1. Typed topics always validate, and the database features exist only on typed topics. Untyped topics stay as raw logs (§4).
- G2. Write intents (`insert`, `update`, `upsert`, delete, delete-existing) are enforced by the broker, together with a typed delete API (§5).
- G3. The broker assigns keys (`autoincrement`) with no extra hop and no reuse, ever (§6).
- G4. View keys up to 128 bytes: composite keys and key expressions (§7).
- G5. Point reads by a full key of any width, and prefix reads by the leading key columns (§9).
- G6. `unique` projection views and `check (count … <= N)` aggregate constraints are enforced before the append (§8).
- G7. A rejected batch stores no data and changes no view. Its sequences are consumed by a durable reject marker, so no copy of it can ever be stored.
- G8. Everything is correct under:
  - concurrent writers
  - collisions inside one batch
  - transactions that commit or abort
  - ack timeouts and retries
  - leader failover
  - restart
  - log truncation
  - compaction
- G9. Everything is defined in Rune QL and shown by the CLI (§10).
- G10. A topic with none of these features pays nothing on the produce path beyond one branch that is never taken.

**Non-goals**

- Ordered indexes and range scans (`between`, `<`, `order by`).
- Unique views, derived keys or composite keys over sources with more than one partition. These need partition-key alignment.
- Joins, cascades across topics, and views over views.
- `where` inside a view definition.
- `check` on anything other than an upper bound on `count`.
- Constraints over transient sources: a transient ring doesn't survive failover, so the overlay couldn't be rebuilt.
- Standalone sequences not tied to a topic key (`create sequence`, `nextId`).
- Locale-aware or Unicode case folding.
- Folding views ahead of commit (rows a failover could lose). The `Leader | View` ack is accepted, but it waits for commit (§8.9).

**Anti-goals.** Any of these makes the work a failure even if the rest works:

- Any interleaving the system allows lets a rule be broken, even for a moment.
- A client is told a write failed when it was stored, or the reverse.
- An id is handed out twice, or reused after a delete.
- A topic without these features gets slower.
- The hot path allocates, takes a lock or makes a virtual call.

## 3. Shape: one check point on the write path

Every new rule runs in **one hook** on the partition leader. It sits in `handleProduceBatch`, after idempotent dedupe (`producerState.validate`, `ProducePath.cpp:205`) and before the min-ISR gate and the append. That point covers both ingress routes, gateway-routed and shard ingress, because both reach `produceCore_` and then `handleProduceBatch`.

- **Why after dedupe.** If the hook ran first, a retry of a batch that was already stored could be rejected (create X, delete X, retry the create) even though the original succeeded.
- **Why before the append.** A rejected batch's data is never stored. Its sequences are consumed by a durable reject marker (§11.3), so a delayed copy of the same batch can never be stored later.
- **How it's built.** The hook is a template policy, with no virtual call and no `std::function`.
  - Its state is reached through a pointer stored next to the partition's `RcSlot`. A partition with no checks has `nullptr`, and the hook is one `[[unlikely]]` branch.
  - Installs and removals are cold control-to-worker ops, like `SchemaUpdate` and `CreateView`. They may allocate on the worker thread.

**A checked topic** is a topic with any of the following:
- `autoincrement`
- `key.index = on`
- a constrained view (`unique`, or a `check`)

Checked topics share these rules:
- the producer requirements (§11.2)
- the readiness gate (§8.6)
- the pending overlay (§8.4)
- one batch in flight per partition on the client (§11.3)

Validation (§4) runs for every typed topic, in the same single decode pass.

**What the hook does for one batch, in order:**
1. Decode each record once, using the per-writer-version descriptor cache shared with the fold. That gives validation (§4).
2. Check the wire key against the key field (§4).
3. Assign `autoincrement` ids and patch them into the record (§6).
4. Check the intent against the key index plus the overlay (§5).
5. Check each constrained view against the view state plus the overlay (§8).
6. Records are processed in order against a scratch overlay, so a batch can collide with itself.
7. If any record fails a check, reject the whole batch. No data is appended, nothing is reserved, and the counter is restored. Only the §11.3 reject marker is appended, so the rejection is durable.
8. Otherwise, merge the scratch into the overlay, tagged with the offsets the append assigns. This happens on the same thread, with nothing interleaved.

## 4. Part A: typed topics always validate; untyped topics are raw logs

**Today.**
- **Untyped topics.** A topic declared without a field list is untyped (`topic events partitions 3;`, `Parser.cpp:583`, `Backends.cpp:52`, `docs/api/ql.md:29`). It takes opaque bytes.
- **Typed topics that don't validate.** Validation is opt-in through `--validate` (`CatalogCommands.cpp:250`) and the `validate` argument of `createTopic` (`Client.h:782`), and it defaults to false (`CatalogEvent.h:62`). The broker therefore accepts any bytes on a typed topic. The view fold then quietly counts the undecodable records as `skippedMalformed`.

**Rules**

- **A topic's schema determines its kind:**
  - With a field list, the topic is **typed**.
  - Without one, it is **untyped**: a raw log of opaque bytes, like a Kafka topic. Nothing else changes; the schema is just absent.
  - `show topic` and the catalog mark untyped topics as `raw`.
- **A typed topic always validates.** The `validate` flag, `--validate` and the `validate` parameter are removed.
  - Every data record must decode against a schema version the topic knows. `schemaRejects_` already selects the validator by the exact `(topicId, version)` pair.
  - If any record fails, the whole batch is rejected with `SchemaValidationFailed` and never appended.
  - Control batches are exempt.
- **On a keyed typed topic, the wire key must equal the key field.** The record's wire key must be byte-identical to the key field's canonical encoding. That encoding is what `TypedTopic` already sends (`TypedTopic::recordKey_`, `claim`):
  - An integer key is its native little-endian bytes at the field's own width: 2, 4 or 8 bytes.
  - A `string(N)` key is the N zero-padded field bytes.

  The validator compares those bytes with `memcmp`. The only exception is the `autoincrement` placeholder (§6.4): all-zero bytes at the field's width in both places.
  - **Width limit.** A key field must encode in at most `Key::kMaxInline` (16) bytes, so integers and `string(N ≤ 16)`. A topic with a wider key field is refused at DDL time.
  - **No more truncation.** `Key(span)` and `Key(pod)` silently truncate today (`Key.h:57-63`). That becomes a hard error.
  - **Two encodings, kept separate.** The view key encoding (§7.3, big-endian, order-preserving) is internal to the view table. It never appears on the wire key.
  - Without this rule, routing, compaction and view identity follow the wire key, while readers and view columns see a different value.
  - The fold already uses the wire key as identity and the value field as a column (`ViewFold.cpp:162-188`).
- **Database features exist only on typed topics.** These are views, subscription filters, intents, the key index, `autoincrement` and constraints.
  - Every one of them except filters also needs a key.
  - Using one on an untyped or keyless topic is refused at DDL time with a message that names the missing piece ("view source must be a typed topic", "autoincrement needs a key").
- **Binding is strict in both directions (owner decision):**
  - `TypedTopic<T>` refuses to bind to an untyped topic, or to a typed topic whose fingerprint differs.
  - A raw (untyped) topic handle refuses to bind to a typed topic. The raw `send(topic, key, bytes)` path is for untyped topics only.
  - The broker backs this up. A typed topic still validates every record, so bytes from a misused raw path are rejected anyway.
- **`partitions` is optional, defaulting to 1 (owner decision).** `topic x { … } key id;` and `topic events;` both create one partition. Explicit `partitions n` is unchanged.
- **Keyless typed topics** stay allowed: append-only typed event streams, routed sticky or round-robin.
- **Hot path.**
  - Validation moves from the `validatedTopics_` hash lookup (`Worker.h:3891`) to the §3 per-partition pointer.
  - Untyped topics and unchecked typed topics pay one predicted branch.
- **Existing clusters.**
  - Behind the new feature level (§12), every existing typed topic starts validating. A producer that was writing invalid bytes to a typed topic then gets `SchemaValidationFailed`, which is the intent.
  - Untyped topics are unaffected.
  - No conversion is needed.

## 5. Part B: write intents, the key index, typed delete

**Today.** Every write is an upsert. A delete is a record with an **empty** value: Rune's tombstone test is `value.empty()` (`LogCleaner.h:364`, and the view fold too), where Kafka uses a null value (`valueLength = -1`). A create and an update can't be told apart, and the typed API (`TypedTopic<T>`) has no delete at all. A delete today needs the raw producer and a hand-encoded `Key`, and the wrong key width silently deletes a different key, or nothing.

**5.1 Wire**

Each record has an `attributes` byte (`RecordBatch.h:146`). It is always 0 today. Every reader skips it, and compaction copies records unchanged (`BatchRebuild.h`). Its bits 0–1 carry the intent:

| Intent | Bits | Value | Broker rule | Rejection (definite) |
|---|---|---|---|---|
| upsert | 0 | row | none (today's behavior, the default) | — |
| insert | 1 | row | key must not exist | `KeyExists` (52) |
| update | 2 | row | key must exist | `KeyNotFound` (53) |
| delete | 0 | empty | none (idempotent, as today) | — |
| delete existing | 2 | empty | key must exist | `KeyNotFound` (53) |

Bit pattern 3 and the other attribute bits are reserved and rejected with `InvalidRequest`.

**5.2 Key index**

- **What it is.** A per-partition set of the keys that exist, enabled by `with (key.index = on)` and implied by `autoincrement`.
  - It is the same machinery as a projection view keyed by the source key, without the columns: about 16 bytes per key, plus the checkpoint and the leader-only rebuild.
- **The check.** Intents are checked in the §3 hook against the key index plus the overlay, under the §8.5 counting rules. A pending insert counts at once. A pending delete counts only in the cases §8.5 allows.
- **Not enabled.** A non-zero intent sent to a topic without a key index is refused with a definite `InvalidRequest`.

**5.3 The log says what happened.**
- A write whose intent fails is never appended. So in the log, an `insert` really was a create, an `update` really was an update, and a tombstone with intent 2 really deleted an existing key.
- Compaction removes superseded records, so a key's original `insert` disappears once a later update replaces it. A consumer reading compacted history therefore treats records as upserts. Intents are exact only for records that compaction hasn't touched.

**5.4 Client API**

- **Raw producer:** `send(topic, key, value, WriteIntent intent = Upsert)`.
- **`TypedTopic<T>`:**
  - `insert(rec)`, `update(rec)`, and `upsert(rec)` (an alias of today's `send`)
  - `remove(key)`, a tombstone
  - `removeExisting(key)`, a tombstone with intent 2
  - each with an `…AndWait` form
- **Key type.** `remove` and `removeExisting` take the topic's C++ key type and encode it exactly as `send(rec)` routes it, so a delete can't land on the wrong key.
- **Consumer.** `record.op()` returns `Insert`, `Update`, `Upsert` or `Delete`.
- **Older clusters.** The client sends a non-zero intent only when the cluster feature level supports it. An older broker would ignore the bits and silently upsert, so the client fails such a call locally.

## 6. Part C: broker-assigned keys (`autoincrement`)

**6.1 Prior art.** Code read in Vitess, Citus, TiDB, CockroachDB, YugabyteDB and MySQL; Spanner from docs only.

| Pattern | Who | Cost per insert | Ids | Explicit ids |
|---|---|---|---|---|
| Central counter with cached blocks | Vitess `execNextval` (`next_id` + `cache`), TiDB `HInc` with 30 000-id windows, YugabyteDB `sequences_data` with blocks of 100, CockroachDB sequences | usually free, a hop per refill | small | not safe |
| Disjoint high-bit range per node | Citus `groupId << 48`, TiDB `AUTO_RANDOM` | local | huge | not tracked |
| Stride per writer | MySQL `compute_next_insert_id` (`offset + N×increment`), set by Group Replication | local | small | only the local counter moves, so other writers can collide |
| Time + node id | CockroachDB `unique_rowid()` | local | huge | not tracked |
| Bit-reversed | Spanner, CockroachDB `unordered_rowid` | — | scattered | — |

- **What they share.** All of them accept gaps and give no global order across nodes. The id is always assigned in a trusted server tier. None of them makes explicit ids collision-proof.
- **Bit reversal.** It exists only to avoid hot ranges in range-partitioned stores. Rune hash-partitions, so it doesn't need it.

**Rune's design** combines the stride (MySQL) with a durable block reservation (Vitess, TiDB and YugabyteDB), both on the partition leader. Only Rune can do this, because its partition count is immutable: `alter topic … set partitions` is a parse error. A range-partitioned store, or one that can be resharded, can't assume a fixed partition count.

**6.2 Declaration**

```sql
topic character { characterId uint64 autoincrement, … } partitions 1 replicas 3 key characterId;
```

- **Where it's allowed.** Only on the key field, typed `uint64`, `int64` or `uint32`. The key is mandatory.
- **What it implies.** `key.index = on`, and the modulo partitioner (§6.3).
- **Immutable.** The key field, its type, `autoincrement` and the partition count can't change.
  - Today you already can't drop the key (`Evolve.cpp:100`), and no alter changes a type or moves the key. Renaming the key field stays allowed, because its bytes don't change.
  - The schema evolution check also rejects any alter that would remove `autoincrement`.

**6.3 Routing: the modulo partitioner**

- **Routing rule.** An autoincrement topic routes key `id` to partition `id % P`, not `fnv1a(key) % P`.
  - The partitioner is a topic property in the catalog.
  - The client, the gateway (`partitionForKey`) and the view-read path all use it.
- **Which ids a partition hands out.** Partition `p` assigns `n·P + p` for n = 1, 2, …, so id 0 is never assigned.
  - Ids are small and dense across the topic.
  - With one partition they are 1, 2, 3, …, increasing globally.
- **Limit.** The broker refuses to assign above `2^53 − 1`, so ids are always exact in JavaScript. A topic can set a lower `autoincrement.max`.
- **Batches.** A batch of k new records gets `base, base+P, …, base+(k−1)·P`.

**6.4 Assignment**

- **Client.** A create is sent as `insert` with key **0**: zero bytes at the key field's width (4 for `uint32`, 8 for `uint64` and `int64`), with the key field in the value also 0. The client routes such a batch with a per-producer sticky partitioner, so one batch targets one partition.
- **Leader (the §3 hook, step 3):**
  1. Take k ids from the in-memory counter.
  2. Copy the batch into a pre-sized per-worker scratch buffer.
     - `handleProduceBatch` takes `const uint8_t*` and appends zero-copy with `pwritev` from the ingress or op ring, so the ring bytes can't be patched in place.
     - The scratch is sized to the topic's maximum batch size and allocated at install time.
     - Only `autoincrement` topics take this path.
  3. Overwrite the placeholder bytes in each record's key, and the key field in its value. The field's offset comes from the writer-version descriptor cache, and validation has already decoded the record. Lengths don't change, so the batch is never re-encoded.
  4. Recompute the batch CRC32C (hardware-accelerated), and append from the scratch buffer.
- **Performance.**
  - No allocation, no lookup, and no extra hop.
  - Target: under 1 µs p50 for a batch of up to 4 KiB, including the copy, on `ProduceValidateBench` (my call).
  - A record with an assigned id skips the key-index existence check, since it is unique by construction.
- **Rejection.** If a later check rejects the batch (§3 step 7), the counter goes back to where it was.

**6.5 Durability: reservation records**

- **Reserve.** Before using an id above the current reservation, the leader appends a control record to the same partition log: "reserved through n = R". It goes in on the same thread, immediately before the data batch. The step is `autoincrement.reserve`, default 4096.
- **Replication.** The reservation replicates like any record. It sits below the data batch that relies on it, so it is committed first. Nothing new is needed: no sequence store and no separate replication path.
- **Failover and restart.** The new leader finds the latest reservation in its log and resumes at R+1. The unused part of the block becomes a gap. Until it has found its reservation, autoincrement writes get retriable `NotLeader` (§8.6).
- **Truncation.** Truncation cuts a suffix. If a reservation is cut, every data record above it is cut too, so no surviving record's id is ever handed out again.
- **Compaction.** The cleaner keeps the latest reservation record per partition. Otherwise the highest id could disappear once its record is tombstoned and compacted away, and a deleted record's id would be reused.
- **Retention.** An `autoincrement` topic must be `cleanup.policy = compact`.
  - `delete` and `compact,delete` are refused at DDL time.
  - So are moves of the log start (delete-records, and retention by time or size).
  - The latest reservation therefore always survives. A new leader never needs to scan further back than the last reservation, which the cleaner moves forward as it rewrites segments.
- **Reading the log.** Reservation records are control records. Consumers and folds skip them, as they skip transaction markers.

**6.6 Write rules on an autoincrement topic**

| Write | Rule |
|---|---|
| `insert` with key 0 | The broker assigns the id. |
| `insert` with an explicit id | Only for a principal with the new `IMPORT` grant on the topic (the Postgres back-fill). The broker checks `KeyExists`. The id routes by modulo to the one partition that could ever assign it, and that partition moves its counter past it (`n = id div P`). If that puts the counter above the current reservation R, the leader first appends a new reservation through `id div P` (§6.5), so a failover can never resume at or below an imported id. This is exact, unlike MySQL and TiDB. Without `IMPORT` it is refused with `NotAuthorized`. |
| `update`, `remove`, `removeExisting` | Explicit id, checked against the key index. |
| plain upsert | Refused with `InvalidRequest`, because it could create an id the counter never accounted for. |
| assigning past the maximum | `AutoIncrementExhausted` (54), definite. |

- **Retries.** An idempotent retry that dedupes returns the original offsets *and* the original base id.
  - Each `autoincrement` partition keeps a side window `(producerId → last 32 batches' assigned base ids)`, in step with the producer-state retry window. Only these partitions have it.
  - It is rebuilt from the log on failover, alongside the producer-state rebuild, from the keys in the stored batches.
  - Nothing is read back, so compaction rewriting the batch doesn't matter.
  - Checked partitions keep one batch in flight (§11.3), so a retry is always the newest batch and always in the window.
  - A duplicate outside the window is answered `DuplicateSequence` with no id. The client reports "stored; id unknown" as its own definite error. It should be unreachable, and the stress run asserts that.
- **Transactions.** Ids taken inside a transaction that aborts are burned, leaving a gap.
- **The ack.** It carries the ids only once the write is committed (acks=all, §11.2), so a truncated record never gave its id to anyone.

**6.7 Client**

- **Ack.** `ProduceResponse` gets a trailing `assignedBase u64`, gated on client version 12.
  - Older decoders accept the longer block (`DataMessages.h:272-285`).
  - The fixed scratch arrays grow by 8 bytes: `SessionAckSink.h:1136`, `ClientGateway.cpp:595`, and the `WorkerReturn` carrier.
- **Ids per record.** The client derives each record's id as `base + i·P`.
  - `insertAndWait(rec).key()` returns it.
  - `insert(rec)` reports it in its completion.
- **Dependent writes**, such as skills keyed by the new `characterId`, wait for that ack: one sequential round trip, the same as Postgres `INSERT … RETURNING` followed by the child inserts. Creates are rare, so this is accepted.

**6.8 If the partition count ever becomes mutable.** Changing P remaps every key. Before an autoincrement topic changes P, every partition's counter must be raised so that `n·P_new + p` lies above the highest id reserved under the old P. MySQL has the same rule when `auto_increment_increment` changes.

## 7. Part D: wide view keys

**7.1 Key forms**

- `key <col>`: one column, as today.
- `key (<col>, <col>, …)`: a composite key.
- `key lower(<col>) as <name>`: a key expression, valid on `string(N)` only. The output has a key column `<name>` holding the lowered bytes.
- Key columns must be fixed-width: integers, `bool` or `string(N)`. A variable-length `string` or `bytes` is refused at compile time.
- The total key width K is at most **128 bytes**. That covers `(provider uint16, subject string(64))` at 66 bytes.

**7.2 Identity rule.** A projection view that isn't `unique` must include the source key in its key, so each source record maps to exactly one row. `account_characters` includes `characterId`, so it passes. Any other projection must be declared `unique`, or the compiler refuses it with:

> key does not identify the source record; declare the view unique or include <sourceKey>

This closes the "two records, one row, last writer wins" hole.

**7.3 Encoding.** Key bytes are order-preserving, so a later ordered index needs no re-encoding:
- Unsigned integers are big-endian.
- Signed integers are big-endian with the sign bit flipped.
- `bool` is one byte.
- `string(N)` is N bytes, zero-padded.

Names can't contain NUL, so padding can't make two names collide. In general, a collision could only cause a false reject, never a false accept.

**7.4 Partitioning.** Derived and composite keys need a source with one partition in this work, the same rule aggregates follow today (`ViewNotCoPartitioned`, `AdminWorker.cpp:1248`). Reads for these views always go to partition 0.

**7.5 Storage (`ViewTable`)**

- **Index slot.** It stays 13 bytes: `tag u8 | word u64 | rid u32`.
  - For K ≤ 8, `word` is the raw key, exactly as today, with no arena access. Existing views get no slower.
  - For K > 8, `word` is a cached 64-bit hash.
    - A lookup matches the tag, then the hash, then `memcmp`s the key bytes in the row.
    - Incremental growth and backward-shift deletes use the cached hash and never touch the arena.
- **Hash.** The index uses a seeded 64-bit hash (for example rapidhash) with a random seed per worker. Players choose names, so an unseeded hash would let them build collision chains that stall the leader thread. The hash never leaves the process. Partition routing keeps its deterministic FNV-1a, or modulo for autoincrement topics (§6.3).
- **Row.** For K > 8 the key bytes are stored once in the arena row, and they can't drift from the key column values.
- **Prefix index.** A composite key of n columns gets exactly one prefix index, over its first n−1 columns.
  - It is a second hash index from the prefix hash to the head rid. A match is confirmed against the head row's bytes.
  - Rows that share a prefix are linked through `prev`/`next` rids in the row, so insert and erase are O(1).
  - A scan walks that list, in no particular order.
  - A read on a shorter prefix is refused at compile time with "needs an ordered index".
- **Owner map.** This applies to views keyed on anything other than the source wire key.
  - A per-instance table maps the source key to the rid of its current row. A tombstone has no value, so this is the only way to find the row to erase.
  - When an update changes the view key (a rename), the fold erases the old row and then inserts the new one, the same retraction pattern as the aggregate `contributors` table.
- **Checkpoint v3.**
  - A new magic number, with K-byte keys instead of `key u64`.
  - The hash is never persisted; it is recomputed on replay.
  - The owner map and prefix index are either checkpointed or rebuilt on replay (the builder decides), with identical lookups afterwards.
  - v1 and v2 checkpoints still load.

## 8. Part E: view constraints

**8.1 Strings and `lower()`**

- `lower` maps ASCII A–Z to a–z and leaves every other byte unchanged. That is exact for today's names, `[A-Za-z0-9]{1,16}` (the `persistence` package's `ALLOWED_NAME_CHARS`).
- **Reserved for later: `casefold()`**, Unicode simple case folding (`CaseFolding.txt`, statuses C and S). It would be:
  - locale-independent;
  - pinned to the Unicode version stored in the view plan, so a broker upgrade can't change existing keys;
  - restricted to input that is valid UTF-8 already in NFC (anything else is rejected);
  - sized at the folded maximum width, since folding can grow a string (Ⱥ is 2 bytes, ⱥ is 3).
- The plan format (§10.2) reserves a key-function id, so `casefold` can be added without another format version.
- Lookalike letters, such as Cyrillic а and Latin a, are an application rule, not Rune's.

**8.2 What is checked**

- **A `unique` view.** A record whose derived key is already owned by a different source key, in the view or in the overlay, is a violation.
- **`check (c <= N)` or `check (c < N)`** on a `count(*)` / `count(col)` aggregate view. After the record is applied, the group's folded contributor count plus its pending increments, minus its countable pending releases (§8.5), must stay within the bound.
  - An increment means the record adds a new contributor to the group. An update to an existing member is not an increment.
  - A move between groups is an increment for the new group and a release for the old one.
- **Never rejected.** A retraction (a tombstone, or a move out of a group) is never rejected. A group already over its limit may shrink.
- **Anything else.** Any other check form is refused at compile time with "check supports count(…) <= / < constant".
- **Several views.** One record may be checked against several views, for example `character_name` and `account_stats`, and must pass all of them.

**8.3 Unique conflicts at fold time.** These can come only from data written before the unique view existed, because the write-time check prevents new ones.

- **Rule.** Where several live source records share a unique key, the view row belongs to the one with the **smallest source key**.
  - A rule based on "first by offset" would not be deterministic, because compaction and deletes change which offsets survive.
  - The smallest source key is the same for every leader, every restart and every refold, so "view = fresh refold" holds.
- **Promotion.** When the owner is deleted or renamed, the next-smallest live record is promoted.
  - The fold tracks the conflicting contributors of each such key, using the retractable Min machinery that aggregates already use (value blocks plus rescan, `AggFold.cpp`).
- **Accounting.** Each extra contributor is counted in `uniqueConflicts`. Other views over the topic still contain those records.
- **Writes.** A write claiming a key that already has any live owner is rejected (§8.2), so the conflict set can only shrink.
- **Back-fills.** They should still remove duplicates first, as Postgres migration V14 did.

**8.4 The pending overlay.** Views fold only up to the LSO, which is bounded by the HW. The overlay covers what lies between each view's fold cursor and the log end offset (LEO).

- **Shape.** For each checked partition there is one fixed-capacity, offset-ordered structure, pre-allocated at install time.
  - Each entry is `{offset, sourceKey, per-check effect, txn pid or none, aborted flag}`.
  - The effect is:
    - for a unique view, the key it claims and the key it releases;
    - for a check view, the old group and the new group;
    - for the key index, an insert or a delete.
  - Lookups are by key hash and by source key. That way a later write sees an earlier pending write to the same record (create then rename, or create, delete, create).
- **Release.**
  - An entry leaves from the front once **every** checked view and the key index on the partition have folded past its offset.
  - An abort marker (`rc->onMarker`) marks its entries aborted, and they stop counting at once.
  - A truncation drops every entry at or above the cut, from the back.
  - An entry is **never** released because its ack timed out. The record is already in the log.
- **Capacity.**
  - It is a sizing knob, default 65 536 entries (my call).
  - Ack in-flight limits do *not* bound it. One open transaction pins the LSO, so later writes stay in the overlay until it ends, and the transaction timeout limits how long that is.
  - When the overlay is full, the batch gets the new retriable `CheckBacklogFull` (55). It is also exported as a stat and logged.

**8.5 What a pending entry may be counted for.** The principle: a definite verdict may depend only on state that can't vanish unless the write depending on it vanishes too.

- **Pending increments (claims) always count:** non-transactional, from an open transaction, or committed. So two open transactions can't both take the 12th slot.
- **A pending release** (a tombstone, a rename that frees a name, or a move out of a group) counts only if:
  - (a) it isn't transactional, or
  - (b) its transaction has applied a commit marker, or
  - (c) it belongs to the same open transaction as the later write.

  Case (a) is safe because truncation always cuts a suffix of the log. If the release is cut, every write that relied on it is cut too. An abort is the only way an earlier record vanishes while a later one survives, so a release from another, still-open transaction never counts.
- **Source keys held by another transaction.** A write to a source key with a pending entry from a different, still-open transaction gets retriable `ConcurrentTransactions` (27, existing).
  - Without this, the later write's effect would be computed against a record that may still abort, and a missed increment would break the cap.
  - It works like a row lock until the transaction ends.
- **When an abort marker arrives,** its entries stop counting. Rule (c) and the lock guarantee that no other entry depends on them, so nothing needs recomputing.

**8.6 Readiness gate.** A write to a checked partition gets retriable `NotLeader` until, under the current leadership generation:

- RC is Ready, as `rcGate_` requires today;
- every checked view instance and the key index are Ready: the fold has reached the LSO, and the HW has reached `leadershipStartOffset()`;
- the autoincrement reservation has been found (§6.5);
- the overlay has been rebuilt by decoding `[min fold cursor, LEO)` from the local log, with open transactions taken from `RcState`.

Like `RcState`, the overlay is rebuilt when `leadershipGeneration` or `logGeneration` changes, and cleared when the node isn't the leader. It always converges from the log, never on a timer. Restart and view rebuild (`RebuildingViews`) use the same gate.

**8.7 Changing constraints**

- **Create.**
  - Writes to the source are refused, retriably, from install until the view is Ready. From then on every write is checked.
  - Groups already over the limit aren't repaired; they can't grow until they shrink.
- **`create or replace`.**
  - The old constraints stay in force until the new instance is Ready. Then the two are swapped in one worker step, so there's never a gap.
  - Drop followed by create *does* leave a gap. The docs say so.
- **Drop.** The constraint is removed when the drop is installed on the leader.

**8.8 Folding (Phase 1–2 rules that still apply).**
- Views fold on the leader only, after commit, bounded by the LSO.
- Read-your-writes comes from the fold barrier (#82): a read waits until the fold has passed the reader's acked writes.
- `All` acks at commit (HW), and reads still wait at the fold barrier. A write with the `View` flag (§8.9) is acked only after the fold.
- With #537 fixed, records of aborted transactions are skipped.

**8.9 Ack flags (owner decision, 2026-10-22)**

Acks become bit flags, so a writer combines a replication guarantee with a view guarantee. The names `None`, `Leader` and `All` stay as they are.

```cpp
enum class Acks : uint8_t {
    None   = 0,
    Leader = 1,       // the leader appended the batch
    All    = 2,       // the HW passed the batch (every in-sync replica, ISR >= minIsr); implies Leader
    View   = 4,       // every view whose source is this partition has folded past the batch
};
constexpr Acks operator|(Acks a, Acks b);   // plus &, and a has(Acks) helper

ProducerConfig{.acks = Acks::All | Acks::View};
```

| Combination | Ack is sent when |
|---|---|
| `None` | never; the client is done once the bytes are handed to the transport |
| `Leader` | the leader has appended the batch |
| `All` | the HW has passed the batch |
| `Leader \| View` | the leader has appended the batch **and** every view on the partition has folded past it |
| `All \| View` | the HW has passed the batch **and** every view on the partition has folded past it |

- **Rules.**
  - `Leader | All` is `All`.
  - `View` alone and `None | View` are refused when the `ProducerConfig` is built (`invalid_argument`), because a fold needs an append.
  - Transactions keep requiring `All`, with or without `View`.
- **Wire.** The produce `acks` byte keeps its meaning: bit 0 set means `All`, clear means `Leader`. Bit 1 is the new `View` flag, so old values decode unchanged.
  - The client sends bit 1 only when the broker supports `kCheckedWrites` and the session is client version 12 or later. Otherwise a `View` write fails locally with `BrokerTooOld`.
  - A broker refuses unknown bits with `InvalidRequest`.
- **`Leader | View` today.** Views fold only committed data: the fold is bounded by the LSO, which is never above the HW. So, with today's fold, `Leader | View` waits for commit and behaves like `All | View`.
  - The combination is still accepted, so the API doesn't change if folds are later allowed to run ahead of commit.
  - Running folds ahead of commit would change what view readers see: rows that a failover can lose. It is a non-goal here (§2).
- **On the leader.** A `View` ack is parked like an `All` ack (`pendingAcksAll_`). `flushOwedAcksAll_` releases it once the replication condition holds *and* the minimum fold cursor over the partition's views is greater than `lastOffset`.
  - Views fold in the same duty cycle, right after `rcAdvanceAll_`, so this normally adds at most one pass.
  - Writes without `View` take exactly today's path, with one extra flag test.
- **Open transactions.** A fold stops at the LSO, so a `View` write behind an open transaction waits for that transaction to resolve. That wait is bounded by the transaction timeout. Writers without `View` on the same partition don't wait.
- **Failures** match `All`: `NotLeader` on a lost leadership, and `Internal` on the ack deadline, with the outcome unknown. On a partition with no views, `View` adds nothing.
- **Checked topics** require `All`, with or without `View`. `Leader`, `Leader | View` and `None` get a definite `InvalidRequest` (§11.2).
- **Reads are unchanged.** The fold barrier still holds every read until it covers the reader's acked writes, so `All` writers also read their own writes.
- **RegressorsPath:** auth-server and character-server use `All | View` on their create, rename and delete paths.

## 9. Reads

- **`GetViewRowK` (template 81).** A full key of any width, as length-prefixed bytes, plus `requestedFingerprint`. `GetViewRow` (72) stays for K ≤ 8.
- **`ScanViewRows` (82) and `ScanViewRowsResponse` (83).**
  - The request carries the prefix bytes, a `limit` (capped by the broker) and a resume token.
  - The response carries a status, a row count, length-prefixed rows and the next resume token.
- **Reading the record that caused a conflict.** Both requests take an optional `minOffset`. The read is held at the fold barrier until the view has folded past it.
  - A rejection with code 51 or 52 carries the offset of the conflicting record: the overlay entry or the owning row's last offset. It travels in the trailing response field that `assignedBase` uses on success.
  - The loser of a race can therefore read the winner's row and never loop.
  - If the winner's record is in an open transaction, the read waits for it to resolve. After 100 ms it answers `NotReady`, as today.
- **Shared pipeline.** Both run through `viewRowGate_`, READ-on-VIEW authorization, leader-only serving, the read lease, the fold barrier and the restart-readiness gate.
- **Bounds.**
  - A response fits the 68 KiB egress scratch (`ClientGateway.h:2209`). The worker stops at the limit, the byte budget or a per-op row cap, which bounds leader work.
  - An oversized single row still gets `kRowTooLarge`.
- **Consistency.**
  - A page is atomic: it is served in one worker step.
  - Across pages there is no snapshot. The resume token is the last key, and if that key is gone the response is the new status `kCursorInvalidated`; the client restarts the scan.
  - A user's ≤ 12 characters always fit in one page.
- **Client.**
  - `getViewRowAndWait(view, key)` and `scanViewAndWait(view, prefix, limit)`.
  - A typed key builder from the view's output descriptor, so callers never hand-encode bytes.
  - New `OpKind`s. Results larger than the completion-ring reservation get `ResultTooLarge`, as today.

## 10. Rune QL and the CLI

**10.1 Grammar**

- New tokens: `*`, `<`, `<=`, `>`, `>=`.
- **Topics:**
  - The field list is required (§4).
  - Fields accept the `autoincrement` modifier.
  - `with (key.index = on)` is a new setting.
- **Views:**

  ```
  create [or replace] [transient] [unique] view <name> key <keyspec> as select <items> from <topic> [group by <cols>] [constraint <name> check (<col> <op> <int>)]*
  ```

  - `<keyspec>` is `<col>`, `(<col>, …)`, or `lower(<col>) as <name>`.
  - `<items>` is `*`, `<col> [as <name>]`, or `count(*)`, `count(col)`, `sum`, `avg`, `min`, `max` or `last`, each with `as`.
- **Reads:** `select <cols | *> from <view> where <keycol> = <lit> [and …] [limit n]`.
  - Every key column given means a point read. The leading n−1 columns mean a scan. Anything else is a parse error.
  - Literals may be wrapped in `lower('…')`.

**10.2 Pipeline**

- **AST.** `CreateViewDef` and `SelectDef` are appended to the `Statement` variant.
- **Prepare.** `prepare` resolves the source descriptor from the catalog and builds the authored `ViewPlan` v2. The author type moves to a library that both `ql` and the client include.
- **Plan v2.** It adds:
  - key components: column refs plus a key function, where 0 is none, 1 is `lower`, and 2 is reserved for `casefold`
  - `unique`
  - constraints `{name, column, op, bound}`

  The authored and compiled versions both become 2, and v1 blobs still decode.
- **Catalog.**
  - The new view fields live in `normalizedPlanBlob`, so they count in the fingerprint.
  - Topic schemas gain `autoincrement`, the partitioner and `key.index`.
  - The golden catalog logs are updated.
- **Compile errors** get their own codes: `kViewKeyExpressionInvalid`, `kViewConstraintInvalid` and `kViewUniqueInvalid`. The current mapping, which searches the message for "key" (`AdminWorker.cpp:1239-1245`), is removed.
- **CLI.**
  - `rune ql "select …"` decodes rows using the view's output descriptor from the catalog.
  - `show view` prints the constraints and the stats from §13.
  - `errorCodeName` (`QlEngine.cpp:134`) gains the missing codes 48–50 and the new 51–55.

## 11. Errors and producer behavior

**11.1 New codes** (each gated on client version 12):

| Code | Name | Meaning | Kind |
|---|---|---|---|
| 51 | `ConstraintViolation` | Not stored: accepting the write would have broken a rule when it was checked. The constraint id (`viewObjectId << 16 \| ordinal`, ordinal 0 = `unique`) is carried in the `ProduceResponse` `logStartOffset` slot, using the D6 trick. | definite |
| 52 | `KeyExists` | `insert` on a key that exists | definite |
| 53 | `KeyNotFound` | `update` or delete-existing on a key that's absent | definite |
| 54 | `AutoIncrementExhausted` | The counter has reached `autoincrement.max` | definite |
| 55 | `CheckBacklogFull` | The overlay is full | retriable, with the same identity |

Existing codes reused:
- `ConcurrentTransactions` (27, retriable) for the source-key lock.
- `NotLeader` (2, retriable) for the readiness gate.
- `SchemaValidationFailed` (24) for validation.
- `InvalidRequest` (39) for forbidden writes.
- `NotAuthorized` (45) for an explicit id without `IMPORT`.

"Definite" means *not stored*. It never means "the rule would still be broken now": a pending claim that is later truncated or aborted can make the conflict disappear, and retrying is then the client's choice.

**11.2 Who may write to a checked topic.** Everything else gets a definite `InvalidRequest`:
- **Idempotent producers only.** Writes with pid −1 skip dedupe and `rcGate_`, so the hook's guarantees wouldn't cover them.
- **`All`, optionally with `View`.** An `acks=leader` write (with or without `View`) is acked before the HW, so it could hand out an id that a failover then truncates.
- **Client version 12 or later.** Older clients treat unknown codes as "outcome unknown" and bump their epoch.

**11.3 Producer** (`ProducerImpl`). Today any unknown code, and code 24, goes to "outcome unknown", which means `markUnresolved_` and an epoch bump (rule D, `ProducerImpl.cpp:2503`). The out-of-order branch (C4) also resequences and bumps the epoch. Batches already in flight behind a rejected batch arrive out of order. So, for checked partitions:

- **One batch in flight per partition.** The client learns that a topic is checked from a new flag in topic metadata.
  - This keeps a coalesced batch's split resend (below) in order with respect to later writes to the same key.
  - The cost is throughput on these low-rate partitions, not latency.
- **Rejections are durable: the reject marker.** Without one, a delayed earlier attempt of a rejected batch could be appended after the client was told it failed, and the next batch, reusing that sequence, would then be deduped as an "OK" without being stored.
  - So when the hook rejects a batch with 51–54, the leader appends a small control record: **reject marker** `{producerId, epoch, baseSequence, lastSequence}`.
  - It advances the producer state exactly like a data batch. It replicates and is part of the producer-state rebuild after failover.
  - The rejection is answered only once the marker is committed (HW), like an acks=all ack.
  - A later or delayed attempt of the same sequences is a duplicate of the marker, and is answered with the same rejection code. It is never appended.
  - Rejections are rare, so this costs one small append each.
  - Readers and folds skip reject markers. The cleaner drops them once the producer-state window has moved past them.
- **The client's definite-reject branch** for codes 51–54:
  - the batch fails, with no `markUnresolved_` and no epoch bump;
  - the sequence counts as consumed, because the marker consumed it, so the next batch continues at `lastSequence + 1`.
- **Coalesced `send()` batches.** If a coalesced batch of unrelated records is rejected, the producer resends its records one at a time, in order.
  - Each resend uses fresh sequences after the marker.
  - Each record gets its own result.
- **Explicit atomic batches** (`p.batch(...)`) are never split.
- **Inside a transaction,** codes 51–54 abort the transaction, as a constraint violation does in Postgres.
- **The result** exposes `constraintId` and the assigned key. A `Client` helper resolves the constraint id to `(viewName, constraintName)` from the catalog.

## 12. Versions and rolling upgrade

- **Feature level.** A new cluster level, `MetadataFeature::kCheckedWrites`, sits next to `kStorageIdentity` (`ReplicationMessages.h:464`). Until every broker supports it, the following are refused:
  - DDL that uses `autoincrement`, `key.index`, a plan-v2 view, or `unique`/`check`;
  - non-zero intents;
- **Version bumps.** Client version 12 covers the `View` ack bit, the new codes, `assignedBase`, `GetViewRowK`, `ScanViewRows` and the checked-topic metadata flag. Plan v2, checkpoint v3, and catalog event formats bump their versions following the existing frozen-format rules. Old formats still decode.

## 13. Observability

- **Per checked partition:**
  - rejects for each constraint and for each intent code;
  - `KeyExists` and `KeyNotFound` counts;
  - overlay occupancy and its peak;
  - `CheckBacklogFull` refusals;
  - readiness-gate refusals by reason;
  - autoincrement: the current counter, the reservation R, and ids burned.
- **Per view:** `uniqueConflicts`, and scan rows and pages.
- All of these appear in `show view` / `show topic` and the metrics export.

## 14. Hot path and benchmarks

- **Untyped topics, and typed topics that already validated.** An ABAB n=3 run of `ProducePathBenchmark` and `WorkerProduceLatencyBench` must show no regression, including the move from the hash-set lookup to the per-partition pointer.
- **Typed topics that didn't validate before.** These now pay the per-record decode. Measure the added cost per batch with the same benches, budgeted at most 1 µs p50 for a one-record batch (my call). This is a deliberate, owner-approved cost, not a regression.
- **Constrained produce.** The hook adds at most 1 µs p50 to a one-record batch on `ProduceValidateBench`. Autoincrement assignment, including the scratch copy, stays under 1 µs per batch of up to 4 KiB. Both are my calls, to be revisited with data.
- **Fold.** `ViewFoldBench` with derived keys plus the owner map stays within 10 µs per event batch.
- **Table.** `ViewTableBench` adds wide-key and prefix-scan cases. Views with K ≤ 8 must not change.

## 15. Interaction matrix

Every cell is a test at the first layer where the behavior is visible: unit, integration or e2e. The stress layer model-checks the invariants below.

**Operations:**
- create: auto-id or explicit `IMPORT`
- update of a non-key field
- rename, which changes the derived key
- move between groups (`userId` changes)
- delete: `remove` and `removeExisting`
- delete followed by reuse of the name
- duplicate retry
- plain upsert

**Entities:**
- source key: new, existing, pending insert, pending delete (transactional or not)
- user: the same user or another user
- group: at 11, at 12, or already over the limit
- name: free, taken by another record, or taken by the same record
- keys of every width: u16, u32, u64, i64 and `string(N)`. The typed `remove` must route exactly as `send(rec)` does.

**Timing and faults:**
- a single record
- writes back to back before the ack
- a batch that collides with itself
- a transaction that commits or aborts
- two open transactions racing for the last slot or the same name
- a write to a source key held by another open transaction, then that transaction commits or aborts
- a transactional release followed by an increment outside the transaction
- a committed release reused before the fold catches up
- an ack timeout, then a retry
- failover between the append and the HW, and between the HW and the fold
- truncation of a pending tail
- restart during a view rebuild
- a view created on a non-empty topic
- `create or replace` while writes are running
- drop while writes are running
- a full overlay

**Autoincrement:**
- 1 record and k records per batch
- P = 1 and P > 1
- a batch that crosses a reservation boundary
- failover just after, and just before, a reservation
- truncation of a reservation together with its data
- compaction after the highest id is deleted: the id must not be reused
- a retry that returns the same ids
- an aborted transaction, whose ids are burned
- an `IMPORT` id below and above the counter
- an explicit id without `IMPORT`
- exhaustion at the maximum
- a batch rejected by a later check, after which the counter must be restored
- an `IMPORT` id above the reservation, then failover: there must be no reuse
- a retry after failover returning the original ids from the rebuilt side window
- DDL refused for `delete` retention, and for delete-records on an `autoincrement` topic

**Validation (§4):**
- a typed-topic record matching no known schema version, including a typed topic that existed before the upgrade
- a wire key that doesn't match the key field
- control batches pass
- an untyped topic and a keyless topic each refuse views, filters (untyped only), intents, the key index and `autoincrement` at DDL time, with the right message
- `TypedTopic<T>` refuses to bind to an untyped topic or to a fingerprint mismatch
- raw `send` works on untyped topics and is validated on typed ones

**Clients:**
- a non-idempotent client, an `acks=leader` client, and a v11 client, each writing to a checked topic
- a coalesced batch with one bad record
- codes 51–54 inside a transaction
- the `View` flag, combined with `Leader` and with `All`:
  - acked only after every view has folded the write;
  - behind an open transaction, it waits until the transaction resolves;
  - on a partition with no views, it adds nothing;
  - leader loss while parked gives `NotLeader`, and a missed deadline gives `Internal`;
  - `View` alone and `None | View` are refused in `ProducerConfig`;
  - unknown `acks` bits get `InvalidRequest`;
  - a broker without `kCheckedWrites` gets `BrokerTooOld` locally;
  - old `acks` values 0 and 1 behave unchanged
- a delayed earlier attempt of a rejected batch arriving after the rejection, before and after a failover: it gets the same rejection, is never stored, and the next batch is not deduped against it
- a login race: the loser's `minOffset` read sees the winner, including when the winner is pending, committed but not yet folded, or in an open transaction
- intents sent to an older cluster

**Reads:**
- a point read and a prefix read right after an acked write
- a scan across pages with an erase between them (`kCursorInvalidated`)
- a scan of an empty prefix
- a forced hash collision, using a fixed seed through a test hook
- pre-existing duplicate names: the smallest source key owns the row, it is promoted correctly on delete and on rename, and a compaction followed by a refold gives the same owner
- `partitions` omitted gives 1 partition
- binding: a typed handle to an untyped topic, and a raw handle to a typed topic, are both refused
- key fields wider than 16 bytes refused at DDL time, and `Key` truncation turned into an error

**Invariants checked in the stress run** (racing writers, injected failovers and aborts):
- No user ever has more than 12 live characters.
- No two live characters share a `lower(name)`.
- No two records ever receive the same autoincrement id, and no id is ever reused.
- Every acked write is visible in every view once the fold barrier is passed.
- No write that was answered with a rejection is ever stored.
- No rejected write appears anywhere.
- Every view equals a fresh refold of the log.

**Sanitizers:** ASan, UBSan and TSan runs of `*Regression*` at each checkpoint.

## 16. Work order

Each part ships on its own:

1. **#537:** skip aborted-transaction records in the fold, and re-fold.
2. **Part A (§4):** validation always on for typed topics, wire key = key field, refusing features on untyped and keyless topics, the `raw` marker, and the `TypedTopic` bind check.
3. **The hook and gate (§3, §8.4–§8.6):** the hook skeleton, the overlay, the readiness gate, the producer changes (§11.2–§11.3), codes 51–55, and the `kCheckedWrites` feature level.
4. **Part B (§5):** intents, the key index, and the typed `insert` / `update` / `remove` / `removeExisting` API.
5. **Part C (§6):** `autoincrement`, the modulo partitioner, reservation records and their retention in the cleaner, `assignedBase`, the `IMPORT` grant, and `insertAndWait(...).key()`.
6. **Part D (§7, §9):** wide and derived keys, the owner map, the seeded hash, checkpoint v3, the prefix index, `GetViewRowK`, `ScanViewRows`, and the client read API.
7. **QL (§10):** `create view` (plan v2), `select`, and the CLI.
8. **Part E (§8.1–§8.3, §8.7):** `unique` and `check` on views.
9. **Docs:**
   - `ql.md`, `wire-protocol.md`, `client-cpp.md` and `admin-cli.md`
   - `CATALOG.md` and `SECURITY.md` (the `IMPORT` grant)
   - the root and client `CLAUDE.md`
   - the site's views section, corrected to the syntax that ships

## 17. Calls made in this design (owner to check)

- Acks are bit flags: `Leader`, `All` and the new `View`, which combine (`Leader | View`, `All | View`). The names stay. **Owner-confirmed.**
- Wire: bit 0 is `All`, as today, and bit 1 is `View`. `View` alone is refused. With today's fold, `Leader | View` waits for commit, like `All | View`. (My call.)
- Checked topics require idempotent producers, `All` (with or without `View`), and client version 12 or later, with one batch in flight per partition.
- Codes 51–54 abort an enclosing transaction.
- A rejection appends a durable reject marker that consumes its sequences.
- Pre-existing unique conflicts: the smallest source key owns the key, with promotion on delete.
- `autoincrement` topics are compact-only, with no retention.
- An `IMPORT` above the reservation appends a new reservation first.
- A conflict rejection carries the conflicting offset, and reads accept `minOffset`.
- The autoincrement scratch copy (copy into a per-worker buffer, patch the ids, append from the buffer). **Owner-confirmed, 2026-10-22.**
  - **Follow-up: patch in place where the bytes are already Rune's.**
    - On the gateway-routed path the batch has already been copied once into the worker's SPSC op ring (`ClientGateway.cpp:4030-4061`). The worker owns that slot, so it could patch it in place if `handleProduceBatch` took a mutable span on this path.
    - On the shard-ingress path the bytes sit in Aeron's term buffer, which must not be modified, so the scratch copy stays there.
    - The builder should measure the copy first. In-place patching is worth doing only if the copy shows up in the budget.
- The key field encodes to at most 16 bytes, and `Key` truncation becomes an error.
- A source key held by another open transaction is locked until that transaction ends (`ConcurrentTransactions`).
- Untyped topics stay as raw logs: no field list means untyped. Every database feature needs a typed topic.
- `lower()` is ASCII-only, and `casefold()` is reserved.
- View keys are capped at 128 bytes.
- Overlay capacity defaults to 65 536 entries, and a full overlay gets code 55.
- Autoincrement topics use the modulo partitioner, with a reservation step of 4096 and an id cap of 2^53 − 1.
- A dedupe retry returns its ids by reading the stored batch back.
- Explicit ids on autoincrement topics need the `IMPORT` grant.
- Performance budgets: under 1 µs for the hook and under 1 µs for assignment, including the copy. No regression for untyped topics or for typed topics that already validated. The new validation cost on typed topics that didn't validate before is measured and accepted.

## 18. Left to the builder

- The exact row layout for key bytes and prefix links.
- Whether the owner map, prefix index and key index are checkpointed or rebuilt on replay.
- The choice of seeded hash.
- The internal structure of the overlay, within the capacity and no-allocation rules.
- The page row cap and the resume-token format.
- The encoding of the reservation control record. It must be distinct from transaction markers and skipped by every reader.
