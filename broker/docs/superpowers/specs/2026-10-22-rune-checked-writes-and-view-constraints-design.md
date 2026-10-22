# Checked writes: typed topics, write intents, broker-assigned ids, wide view keys and view constraints

Status: design, 2026-10-22. Not yet approved. Revised the same day to design B: records are judged at fold (§3).

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
2. A write that breaks a rule is rejected and never becomes visible: no view folds it, no consumer receives it, and compaction removes it from the log. It is judged when the views fold it, after it is stored (design B, owner decision 2026-10-22; §3). A writer that needs "success only once the views reflect it" adds the new `View` ack flag, as in `Acks::All | Acks::View`. Acks become flags that combine; plain `All` stays (owner decision, 2026-10-22; §8.9).
3. A delete is a tombstone. Views follow tombstones automatically, so nothing filters on `isDelete`.
4. Views are clustered indexes. There are no array or `collect()` columns: a user's characters are rows under a composite key.
5. The 12-character cap is a `check` constraint on a `count(*)` aggregate view.
6. Ids are generated in the backend (the broker), safely and fast. The topic key is mandatory where ids are generated, and it can't be altered.
7. Every typed topic validates every write. A topic declared without a field list is untyped: a raw log of opaque bytes with no database features. Every database feature needs a typed topic.
8. A typed `remove` / `removeExisting` API exists.
9. Strings are UTF-8. A typed `string` field must hold valid UTF-8. `lower()` is Unicode simple lowercasing. There are no collations yet, and `casefold()` is reserved for later (§4, §8.1).

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
  with (cleanup.policy = compact);

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
// If two first logins race, one gets ConstraintViolation(user_identity). The winner is already folded when the
// loser is judged, so the loser's getAndWait(key) finds the winner's userId.

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
- G6. `unique` views and `check` predicates are enforced when the views fold each record, against a view that has folded every record before it (§3, §8).
- G7. A rejected record changes no view and is never delivered to a consumer. Its verdict is durable (verdict markers), the same everywhere, and compaction removes it (§8.5).
- G7a. Transactions are validated atomically across topics and views, and become visible in one step on each partition (Part G, §9b).
- G7b. A persisted view may read a transient source. After a restart it keeps its checkpointed rows and has nothing to replay from the source (§9a).
- G8. Everything is correct under:
  - concurrent writers
  - collisions inside one batch
  - transactions that commit, abort or fail their vote
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
- `check` predicates across rows, over other views, or with non-constant operands.
- Constrained views over several topics. A view has exactly one source today (`PlanCompiler.cpp:112-127`), and judging across topics would need one deterministic order over them.
- Atomic visibility of a transaction across partitions (snapshot reads). It belongs with the native query API (§9b).
- Constraints over transient sources: their verdict markers and bitmap would live in a ring that a restart loses. RegressorsPath doesn't need them.
- Standalone sequences not tied to a topic key (`create sequence`, `nextId`).
- Collations, locale-aware case rules (Turkish, Lithuanian and so on), Unicode normalization, and full case folding (`casefold()` is reserved, §8.1).
- Folding views ahead of commit (rows a failover could lose). The `Leader | View` ack is accepted, but it waits for commit (§8.9).

**Anti-goals.** Any of these makes the work a failure even if the rest works:

- Any interleaving the system allows lets a rule be broken, even for a moment.
- A client is told a write was rejected when it is visible, or accepted when it never will be.
- An id is handed out twice, or reused after a delete.
- A topic without these features gets slower.
- The hot path allocates, takes a lock or makes a virtual call.

## 3. Shape: records are judged when the views fold them

**Owner decision (2026-10-22, design B).** A record is stored first and judged when the views fold it. A check is the view engine evaluating a rule on the row the record would produce, against a view that has already folded every record before it. Nothing outside the view holds, counts or recomputes view state, so a rule can be anything the view engine can compute (§8.2).

**Terms.**
- A **verdict view** is a constrained view (`unique` or `check`) or the key index (§5.2). Only verdict views can reject a record.
- A **verdict source** is a partition whose topic feeds at least one verdict view. Only verdict sources carry the reader bound (§8.6) and the verdict markers (§8.5). The transaction rule (§11.2) applies to sources of constrained views, and to explicit-key intents. Other partitions are unchanged.

**The write path does only what must happen before the bytes are stored.** One hook on the partition leader sits in `handleProduceBatch`, after idempotent dedupe (`producerState.validate`, `ProducePath.cpp:205`) and before the min-ISR gate and the append. Both ingress routes, gateway-routed and shard ingress, reach it through `produceCore_`. For one batch, in order:
1. Decode each record once, using the per-writer-version descriptor cache shared with the fold. That gives validation (§4).
2. Check the wire key against the key field (§4).
3. Assign `autoincrement` ids and patch them into the record (§6). An exhausted counter refuses the batch with code 54 before the append.
4. Apply the per-write requirements (§11.2).

- **Why after dedupe.** A retry of a stored batch must get the stored batch's answer, never a second id or a second judgment.
- **How it's built.** The hook is a template policy, with no virtual call and no `std::function`.
  - Its state is reached through a pointer stored next to the partition's `RcSlot`. A partition with none of these features has `nullptr`, and the hook is one `[[unlikely]]` branch.
  - Installs and removals are cold control-to-worker ops, like `SchemaUpdate` and `CreateView`. They may allocate on the worker thread.

**The fold judges.** Views fold on the leader, after commit, in offset order (§8.8). For each record:
1. Every verdict view on the partition computes the rows the record would produce (its **prospective rows**) with the same code that applies them, without writing them.
2. The rules are evaluated on the prospective rows (§8.2), and intents against the key index (§5.2).
3. If everything passes, every view on the partition applies the record. If anything fails, the record is **rejected**: no view applies it, and the rejection goes into the next verdict marker (§8.5).

Views that aren't verdict views fold after the verdict views in the same duty cycle and skip rejected records.

**Why the verdict is exact.** The fold is serial per partition and in log order. Each record is judged against the fold of every accepted record before it: nothing is pending and nothing is counted outside the view. The scope is one partition. The key index is partition-local by nature; unique views and aggregates need a source with one partition (§7.4).

**What a rejected record is.** It stays in the log and replicates like any other record, but:
- no view folds it;
- no consumer ever receives it (§8.6);
- the cleaner removes it, and it never counts as the latest record for its key (§8.5).

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
- **Strings are UTF-8 (owner decision).** Every `string` and `string(N)` field of a typed topic must be valid UTF-8, otherwise the batch gets `SchemaValidationFailed`.
  - This uses the standard well-formedness rules: no overlong forms, no surrogates, nothing above U+10FFFF.
  - The check runs in the validator, with an ASCII fast path, and the padding of a `string(N)` is skipped.
  - `string(N)` means N **bytes**, not characters. Arbitrary binary goes in `bytes`, which is never checked.
  - Existing typed topics start checking behind the feature level (§12), like the rest of validation.
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
- **The check.** The key index is a verdict view (§3). An intent is judged when the key index folds the record, against every record before it.
- **Not enabled.** A non-zero intent sent to a topic without a key index is refused with a definite `InvalidRequest`.

**5.3 The log says what happened.**
- A record whose intent fails is never visible (§8.6). So in what readers see, an `insert` really was a create, an `update` really was an update, and a tombstone with intent 2 really deleted an existing key.
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
- **Batches.** A batch that contains a key-0 create must contain only key-0 creates; otherwise it gets `InvalidRequest`. The client keeps them in their own batches. A batch of k creates gets `base, base+P, …, base+(k−1)·P`, so record i gets `base + i·P`.

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
- **Rejection at fold.** A create rejected by a constraint keeps its id, which is burned and never reused.

**6.5 Durability: reservation records**

- **Reserve.** Before using an id above the current reservation, the leader appends a control record to the same partition log: "reserved through n = R". It goes in on the same thread, immediately before the data batch. The step is `autoincrement.reserve`, default 4096.
- **Replication.** The reservation replicates like any record. It sits below the data batch that relies on it, so it is committed first. Nothing new is needed: no sequence store and no separate replication path.
- **Failover and restart.** The new leader finds the latest reservation in its log and resumes at R+1. The unused part of the block becomes a gap. Until it has found its reservation, autoincrement writes get retriable `NotLeader`.
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
  - It is rebuilt from the log on failover, alongside the producer-state rebuild, from the first key of each stored batch, which is its base id because key-0 batches are homogeneous (§6.3).
  - So that those batches are still there, the cleaner on an `autoincrement` topic never removes records from the last 32 batches of a producer that is still in the producer-state table. This is the same idea as Kafka's cleaner keeping each producer's last batch. Rejected records in those batches are kept too, but they stay invisible.
  - The producer never has more batches in flight than the window holds, so a retry is always in the window.
  - A duplicate outside the window is answered `DuplicateSequence` with no id. The client reports "stored; id unknown" as its own definite error. It should be unreachable, and the stress run asserts that.
- **Transactions.** On a topic with no constrained view, ids taken inside a transaction that aborts are burned, leaving a gap. On a constrained one, transactions are refused (§11.2).
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

- **`lower` is Unicode simple lowercasing (owner decision).** Each code point maps to the one-code-point `Simple_Lowercase_Mapping` from `UnicodeData.txt`.
  - It is locale-independent, so Turkish `I` and German `ß` get no special rules.
  - It is the same mapping Postgres (with ICU), CockroachDB and Spanner apply in `lower()`.
  - **ASCII fast path.** A–Z → a–z inline. Only bytes ≥ 0x80 go through a static table, which is generated at build time and allocates nothing.
  - **Pinned.** The Unicode version is stored in the view plan, so a broker upgrade can't change existing keys. A broker that lacks the plan's version refuses to install the view.
  - **Width.** Lowercasing can grow a string (Ⱥ is 2 bytes, ⱥ is 3).
    - A `lower(string(N))` key component is sized at the largest lowered width of any N-byte input, at the pinned version. That is ⌈3N/2⌉ at Unicode 15/16, and the builder recomputes it from the table.
    - The 128-byte key limit is checked against that width at DDL time, so no write fails at run time for key width.
    - The lowered bytes are zero-padded.
  - **Input** is already valid UTF-8 (§4). Nothing is normalized: `é` and `e`+U+0301 stay different, as with the databases above, and clients should send NFC.
  - **The same table runs everywhere.** Literals in `lower('…')` (§10) and the client's typed key builder use the codec's own table, so a reader's key always matches the broker's.
  - **Today's names** are `[A-Za-z0-9]{1,16}` (the `persistence` package's `ALLOWED_NAME_CHARS`). They stay on the fast path. The `character_name` key component, `lower(string(16))`, is 24 bytes wide, well under the limit.
- **Reserved for later: `casefold()`**, Unicode simple case folding (`CaseFolding.txt`, statuses C and S). It would be:
  - locale-independent;
  - pinned to the Unicode version stored in the view plan, so a broker upgrade can't change existing keys;
  - restricted to input that is valid UTF-8 already in NFC (anything else is rejected);
  - sized at the folded maximum width, since folding can grow a string (Ⱥ is 2 bytes, ⱥ is 3).
- The plan format (§10.2) reserves a key-function id, so `casefold` can be added without another format version.
- Lookalike letters, such as Cyrillic а and Latin a, are an application rule, not Rune's.

**8.2 What is checked**

- **A `unique` view.** A record is rejected if its derived key is already owned by a different source key in the view.
- **`check (<predicate>)`** on any view, projection or aggregate.
  - The predicate is over the view row's own columns, whether key, projected or aggregate (`count`, `sum`, `avg`, `min`, `max`, `last`). It compares them with constants using `=`, `<>`, `<`, `<=`, `>` and `>=`, combined with `and`, `or` and `not`. Examples: `check (characters <= 12)`, `check (balance >= 0)`.
  - It is evaluated on each prospective row the record would produce (§8.4).
  - **A row that satisfies the predicate now** (or doesn't exist yet): the record is rejected if the prospective row fails it. Deletes are judged too: under `check (balance >= 0)`, deleting a positive contributor that would make the sum negative is rejected.
  - **A row that already fails it,** which is possible only for data from before the constraint (§8.7): the record is rejected if it changes a column the predicate reads, unless it is a delete.
    - So in a group already over its limit, a rename passes because it doesn't change `count`, a create fails, and a delete passes. The group can shrink, never grow.
  - A row the record removes entirely (the last contributor of a group leaves) isn't checked.
  - A `unique` view never rejects a delete, since a delete only releases a key.
- **Several views.** One record may be checked against several views, for example `character_name` and `account_stats`, and must pass all of them.
- **Refused at compile time:** predicates across rows, subqueries, columns of other views, and non-constant operands ("check compares this view's columns with constants").

**8.3 Unique conflicts at fold time.** These can come only from data written before the unique view existed, because judging at fold prevents new ones.

- **Rule.** Where several live source records share a unique key, the view row belongs to the one with the **smallest source key**.
  - A rule based on "first by offset" would not be deterministic, because compaction and deletes change which offsets survive.
  - The smallest source key is the same for every leader, every restart and every refold, so "view = fresh refold" holds.
- **Promotion.** When the owner is deleted or renamed, the next-smallest live record is promoted.
  - The fold tracks the conflicting contributors of each such key, using the retractable Min machinery that aggregates already use (value blocks plus rescan, `AggFold.cpp`).
- **Accounting.** Each extra contributor is counted in `uniqueConflicts`. Other views over the topic still contain those records.
- **Writes.** A write claiming a key that already has any live owner is rejected (§8.2), so the conflict set can only shrink.
- **Back-fills.** They should still remove duplicates first, as Postgres migration V14 did.

**8.4 Verdicts at fold**

- **Prospective rows.** For each record, every verdict view computes the rows the record would produce, using the same code that applies them, and holds them without writing. For example:
  - `account_stats` (count by `userId`): user 7's row would go from 11 to 12, so `characters <= 12` is evaluated on 12;
  - `character_name`: the owner map says whether `lower(name)` is owned by a different source key.
- **Several verdict views.** A record must pass all of them. All prospective rows are computed before any view applies anything, so a record rejected by one view changes none.
- **Within a batch.** Records are judged one at a time, in order. Each sees the effect of the accepted records before it, including those in the same batch. Each record gets its own verdict (§11.3).
- **Deletes are judged like any record** (§8.2). A delete fails only a `check` that the removal would break. It never fails a `unique` view, and it passes on a row that already broke its rule.
- **Deterministic.** A verdict depends only on the view state, the record and the constraint set active at its offset (§8.7). The same log gives the same verdicts on every leader, every restart and every refold.
- **No resource-dependent verdicts.** A verdict view never rejects for capacity reasons (row cap, arena). Exhausting capacity stops that partition's fold with an alert, which pauses its delivery and `View` acks, and is never turned into a verdict that another node might not reach.
- **Nothing pending.** A record is judged only once every record before it has been committed and judged. No state about uncommitted records exists anywhere.

**8.5 Verdict markers and the cleaner**

- **The marker.** A control record on the verdict source: `VerdictMarker { finalThrough, constraintSetId, [(offsetDelta, code, constraintId)] }`.
  - Offsets, not segment references, because offsets never change and segments are rewritten by compaction.
  - One marker covers many rejected records.
  - `constraintSetId` names the constraint set its verdicts used (§8.7).
- **When it's written.** The leader appends a marker in the fold step:
  - whenever that step rejects anything;
  - otherwise, when `finalThrough` has moved and `verdict.interval` has passed (default 100 ms, my call), so the watermark below stays recent.
  Acceptance alone writes nothing per record.
- **Replication.** Markers replicate like any record. Consumers and folds skip them as data, as they skip transaction markers.
- **The bitmap.** Each replica builds a local index of rejected offsets per segment from the markers, rebuilt at recovery. It follows `TxnCleanState` (`storage/include/rune/storage/TxnCleanState.h`), which already does this for aborted transactions.
- **The verdict watermark `W`** is the highest `finalThrough` among committed markers. Every verdict below `W` is durable on every replica.
- **The cleaner** on a verdict source:
  - compacts only below `W`;
  - drops rejected records;
  - never counts a rejected record as the latest for its key, so an accepted value is never removed in favor of a rejected one;
  - drops a marker once every offset it lists is gone and a later marker carries a higher `finalThrough`, after the same grace period as tombstones.
  - On a `delete`-retention topic, rejected records and markers age out with everything else.
- **Refold** (restart, view rebuild, new leader):
  - Below `W`, records are applied without being judged, skipping the ones the bitmap marks. A constraint change can therefore never overturn an old verdict.
  - At and above `W`, records are judged as on the live fold. The view state at `W` is exact, because the cleaner never compacts above `W`.
- **Failover.** A record is judged only after it is committed, so it survives any failover. A new leader that inherits records judged after `W` judges them again, reaches the same verdicts (deterministic, §8.4) and writes the markers itself. This is why the writer can be answered before its marker commits (§11.3).
- **Truncation** cuts only uncommitted records, which no fold has judged, so verdicts are unaffected.

**8.6 Readers**

- **Consumers.** On a verdict source, a subscription (raw, consumer group or filtered) is bounded by the minimum fold cursor of the partition's verdict views, as well as by today's bound (HW, or LSO under read_committed). Records the bitmap marks are never delivered, and they leave offset gaps as compaction does.
  - The added delay is the fold's own time on the leader, which is microseconds. Subscriptions are served by the leader only (`ClientGateway.cpp:4932-4948`), and the leader knows its verdicts as soon as it folds.
- **Other views** on the partition skip rejected records (§3).
- **View reads** are unchanged: the fold barrier (§8.8).

**8.7 Changing constraints**

- **The constraint set** of a verdict source is the list of its verdict views and their rules. Each change gets a new `constraintSetId`.
- **Change records.** Every change takes effect at a control record in the source log, a `ConstraintSetChange { constraintSetId }` that the leader appends. The new set judges records at offsets above that record's own offset. The old set judges those below it.
  - Records are committed in order, so any record judged under the new set is committed *after* its change record. Every leader that judges it therefore already has the change record, and judges with the same set. That's the same argument as for reservation records (§6.5).
  - On failover, a new leader takes the set for each offset from the change records in its log, never from local timing. It refuses to judge past a change record whose view it hasn't installed yet, and waits for the catalog.
- **Create.**
  - When the install reaches the leader, it appends the change record. Its offset is the activation offset `A`.
  - The view builds from the log start. Below `A`, it applies records without judging them, which is the pre-existing data of §8.3. From `A` on, records are judged.
  - Writes are never refused for this. Delivery of records at or above `A` to consumers waits until the view has caught up, because their verdicts aren't known yet (§8.6). Creating a constraint on a large topic therefore pauses delivery of new records for the duration of the build. The docs say so.
  - Rows that already break a rule aren't repaired (§8.2).
- **`create or replace`.** The old instance keeps judging until the new one has caught up. The leader then appends the change record at `S` and swaps the instances in the same worker step. The old instance's verdicts stand below `S`, and the new one judges above it. While catching up, the new instance skips records the bitmap marks below `S`. There's never a gap.
- **Drop.** The leader appends a change record without the dropped view, and judging by it stops above that offset. Drop followed by create *does* leave a gap. The docs say so.

**8.8 Folding**

- Views fold on the leader only, after commit, bounded by the LSO, in offset order. Verdict views judge first.
- Read-your-writes comes from the fold barrier (#82): a read waits until the fold has passed the reader's acked writes.
- `All` acks at commit (HW). A write with the `View` flag (§8.9) is acked after the fold, and on a verdict source that ack carries the verdict.
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
- **On a verdict source,** the `View` ack carries the per-record verdicts (§11.3). Writers that need to know whether a write was accepted use `View`.
- **Reads are unchanged.** The fold barrier still holds every read until it covers the reader's acked writes, so `All` writers also read their own writes.
- **RegressorsPath:** auth-server and character-server use `All | View` on their create, rename and delete paths. Every other producer keeps its settings.

## 9. Reads

- **`GetViewRowK` (template 81).** A full key of any width, as length-prefixed bytes, plus `requestedFingerprint`. `GetViewRow` (72) stays for K ≤ 8.
- **`ScanViewRows` (82) and `ScanViewRowsResponse` (83).**
  - The request carries the prefix bytes, a `limit` (capped by the broker) and a resume token.
  - The response carries a status, a row count, length-prefixed rows and the next resume token.
- **Reading the record that caused a conflict.** Both requests take an optional `minOffset`. The read is held at the fold barrier until the view has folded past it.
  - A record is judged only after everything before it is folded, so after a `ConstraintViolation` or `KeyExists` the conflicting row is already in the view. The loser of a race reads the winner's row at once and never loops.
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

## 9a. Part F: persisted views over transient sources (owner decision, 2026-10-22)

**Today** `CreateView` refuses a persisted view over a transient topic with `ViewSourceTransient` (37) (`AdminWorker.cpp:1232`). Only a transient view may read a transient source.

**Rule.** Lift that check. Every combination is allowed: a persisted or transient view over a persisted or transient source. Code 37 stays reserved and is no longer sent.

**Meaning.** The view is as durable as its checkpoint; the source is not durable at all. After a restart the view keeps the rows its checkpoint holds and folds whatever the source still has, which may be nothing.

**What already works.**
- A persisted view checkpoints to its own local file (`ViewSnapshotChannel`, recovery in `installViewForRecovery`). The source's persistence plays no part in that.
- The transient cursor machine (`WorkerViewSet.h:156-184`, `reconcileTransient_` at `WorkerViewSet.cpp:367`) already handles a checkpoint taken against an earlier incarnation (`CursorRecoveredUnreconciled`): it keeps the rows and re-folds from the ring's start.
- **Re-folding is idempotent.** A Replace row is keyed by the source's wire key. An aggregate's contribution is keyed by the source's wire key too (`AggState.h`: a contributor is the record's 8-byte wire key), so folding a record twice replaces its contribution instead of counting it twice.

**Cases.**

| Situation | Source after it | View after it |
|---|---|---|
| Full cluster restart | empty, new lineage | checkpoint rows kept; cursor moves to the new ring start; nothing to replay |
| One node restarts, the cluster stays up | that node rejoins empty and re-replicates the ring | its checkpoint, then a re-fold of the ring from the ring's start |
| Failover to a node whose checkpoint is behind | the ring on the new leader | as above. Records evicted between the checkpoint and the ring's start are never folded on that node. |

**Consequences to document (`ql.md`, `SELF-HOSTING.md`):**
- **The tail is lost.** Rows folded after the last checkpoint write are lost on a full restart, together with the source records that made them.
- **Rows can be stale after a failover.** A row whose later updates were evicted from the ring keeps its checkpointed value until its key is written again. That includes a key that was deleted while the node was behind: its tombstone may have been evicted, so the row can reappear.
- **These are counted, never silent.** The counters are `transientRecordsEvicted` and `transientSourceResets`.

**Changes.**
- **Admin.** Remove the 37 check. Plan and catalog formats are unchanged, since the view already records its own persistence.
- **Recovery.** The "invariant breach (a view over a transient source must be transient)" warnings in `reconcileTransient_` become a normal info log plus `transientSourceResets`. Behaviour is unchanged.
- **Keyed sources only (my call).** A persisted view over a *keyless* transient source is refused (`InvalidRequest`, "a persisted view over a transient source needs a keyed source"), because re-folding keyless records isn't idempotent. A transient view over a keyless transient source is unchanged.
- **Readiness** is unchanged: Recovering → RebuildingViews → Ready. Reads get `NotReady` while the ring is being re-folded.
- **The `View` ack** (§8.9) means "folded". On such a view it doesn't mean "checkpointed".
- **Constraints** over transient sources stay a non-goal (§2).

## 9b. Part G: transactions and atomic batches on verdict sources (owner decision, 2026-10-22)

**What transactions guarantee, by level:**

| Level | Guarantee | Where |
|---|---|---|
| 1. Atomic writes across topics | All of a transaction's records commit, or none do. | Today (Kafka-style), with #537 fixed |
| 2. Atomic validation across topics and views | Every constraint on every partition the transaction touches passes, or nothing commits, and `commitTransaction()` fails with the reason. | **This part** |
| 3a. Atomic visibility within a partition | No read or consumer on a partition ever sees part of a transaction. | **This part** |
| 3b. Atomic visibility across partitions | Reads across views on different partitions see a transaction entirely or not at all. | Non-goal. It needs snapshot reads (MVCC with commit timestamps), which belong with the native query API. |

Until this part ships, the §11.2 refusals apply.

**G.1 Why there is a vote.** The coordinator decides a commit (`PrepareCommit`, durable in the transaction log) before any commit marker exists. The fold can only judge records after that. So validation has to move before the decision.

**G.2 The protocol.**
1. The client calls `commitTransaction()`. Its sends to the transaction's partitions are already acked (`All`), as today.
2. The coordinator, still `Ongoing`, sends a `TxnVote { transactionalId, producerId, epoch }` to the leader of each of the transaction's partitions that might vote. These are the partitions whose topic feeds a constrained view or has a key index, in the catalog either at the transaction's first `AddPartitions` or at commit. Asking both times covers a constraint created or dropped while the transaction is open.
   - **Which partitions actually vote is decided by the partition**, from the constraint set active at the transaction's first record there (§8.7). A partition votes if that set has a constrained view, or if the transaction sent an explicit-key intent there. A partition that doesn't vote answers yes at once, without writing anything.
   - Transactions that touch none of these partitions run exactly as today, with no extra round trip.
3. The voting leader:
   1. waits until its HW covers the transaction's last record on the partition;
   2. appends a `TxnJudge { producerId, epoch }` control record at offset J (§G.3);
   3. once the fold reaches J, judges **all** of the transaction's records on the partition together, in their offset order, against the view state at J;
   4. if they pass, applies them **provisionally**, keeps the before-image of every row they touch, and locks those rows (§G.4);
   5. answers yes, or no with the first failing `(code, constraintId, offset)`. On no, it puts the before-images back at once.
4. If every vote is yes, the coordinator writes `PrepareCommit` and the markers, as today. If any vote is no, or a voter times out, it writes `PrepareAbort`, and `commitTransaction()` fails with that code and constraint id. The transaction is aborted, as in Postgres.
5. The commit marker makes the provisional rows final in **one worker step**, and the abort marker restores the before-images in one worker step. Either way the rows unlock.

- **Votes are idempotent.** A repeated `TxnVote` for the same producer and epoch is answered from the `TxnJudge` already in the log, so a retried `commitTransaction()` or a new coordinator gets the same answer. If the coordinator fails while votes are out, it recovers the transaction as `Ongoing`: the client retries the commit, or the transaction timeout aborts it. No new durable coordinator state is needed.

**G.3 The judging order.** On a verdict source, a record outside a transaction is judged at its own offset. A transaction's records are judged together at their `TxnJudge` record.
- **The fold no longer stops at an open transaction's first record.** It judges and applies other records past it, and defers the transaction's records to its `TxnJudge`. The order comes entirely from the log, so every leader and every refold uses the same one, as §8.4 requires.
- **A commit marker with no `TxnJudge`** for a transaction that should have voted means a coordinator bug. The leader judges the transaction's records at the marker, all or nothing on that partition, counts `txnMarkerWithoutVote` and logs an error. Rules still hold on the partition. Only atomicity across partitions is lost in that case.
- **Failover.**
  - If the `TxnJudge` record committed, a new leader judges at it and reaches the same vote.
  - If it didn't, it was truncated, and the coordinator's retried vote appends a new one.
- **The verdict watermark** `W` (§8.5) never passes a `TxnJudge` whose transaction is undecided, so the cleaner never compacts across one.

**G.4 Locks between the vote and the marker.** A provisionally applied row is locked from the vote until the marker, which is normally one coordinator round trip.
- Judging is in log order, so the fold **pauses at the next record that touches a locked row** until that transaction's marker arrives. Judging it against provisional state would let an abort turn a final verdict wrong, for example a name the transaction released being claimed and then reappearing.
- Records that don't touch locked rows are still behind the paused record, in log order. The pause is bounded by the transaction timeout and counted in `foldPausedOnTxnLock` (§13).

**G.5 What readers see.**
- **View reads** of a provisional row return its before-image until the commit marker. Then the transaction's rows change in one worker step, so no read sees part of it (level 3a).
- **Consumers** on a verdict source stay bounded as in §8.6. They never pass a deferred transaction's records until its marker, which is what `read_committed` gives today. On these partitions, `read_uncommitted` behaves the same way.
- **Partitions that aren't verdict sources** get level 3a too. The fold never ends a step between a committed transaction's first record and its commit marker, so a committed transaction becomes visible in one step.

**G.6 Atomic batches** (`p.batch(...)`) on a verdict source are judged as a unit at their own offset, all records passing or none. They reuse the before-images: the fold applies the batch's records one by one within one worker step, and puts the before-images back if any record fails. No vote is needed, because a batch lives on one partition and in one append. The `View` ack carries a single verdict for the whole batch.

**G.7 Limits and the client.**
- **Size.** A transaction may write at most `txn.judged.records.max` records (default 4096, my call) to one voting partition. Beyond that, the vote is no with the new code `TxnTooLarge` (55). Before-images are pre-allocated per worker within that bound.
- **The client.**
  - `commitTransaction()` and `commitTransactionAndWait()` return the code and the `constraintId` on a no vote.
  - A transactional producer whose acks include `View` also waits, on commit, until every voting partition has made the rows final.
  - Inside a transaction, sends are acked as `All`. The verdict comes from the commit, so `View` on a single send is ignored.
- **Cost.** One extra coordinator round trip on commit, only for transactions that touch a voting partition, plus the `TxnJudge` record and the lock pause.

## 10. Rune QL and the CLI

**10.1 Grammar**

- New tokens: `*`, `<`, `<=`, `>`, `>=`.
- **Topics:**
  - The field list is required (§4).
  - Fields accept the `autoincrement` modifier.
  - `with (key.index = on)` is a new setting.
- **Views:**

  ```
  create [or replace] [transient] [unique] view <name> key <keyspec> as select <items> from <topic> [group by <cols>] [constraint <name> check (<predicate>)]*
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
  - constraints `{name, predicate}`, the predicate as a small expression tree over column refs and constants

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

**11.1 Codes** (51–53 reach only `View` writers, client version 12):

| Code | Name | Meaning | When | Kind |
|---|---|---|---|---|
| 51 | `ConstraintViolation` | The record broke a rule when it was folded. The constraint id is `viewObjectId << 16 \| ordinal`, where ordinal 0 = `unique`. | fold, per record | definite |
| 52 | `KeyExists` | `insert` on a key that exists | fold, per record | definite |
| 53 | `KeyNotFound` | `update` or delete-existing on a key that's absent | fold, per record | definite |
| 54 | `AutoIncrementExhausted` | The counter has reached `autoincrement.max`. The batch isn't stored. | write | definite |
| 55 | `TxnTooLarge` | A transaction wrote more than `txn.judged.records.max` records to one voting partition. The transaction aborts. | vote | definite |

A no vote (51–53, 55) is returned by `commitTransaction()`, with the constraint id, and the transaction is aborted.

Existing codes reused:
- `NotLeader` (2, retriable) while the autoincrement reservation is being found (§6.5).
- `SchemaValidationFailed` (24) for validation.
- `InvalidRequest` (39) for forbidden writes (§11.2).
- `NotAuthorized` (45) for an explicit id without `IMPORT`.

**"Definite" for 51–53 means *never visible*.** The record is stored, but no view folds it and no consumer receives it, and compaction removes it. Verdicts are final: they are the same on every leader and every refold (§8.4, §8.5). A rejection is always true; nothing that might later vanish can cause one, because a record is judged only against committed, judged records.

**11.2 Who may write.** Rules hold for every write, whatever its producer. Requirements apply only to the writes that ask the broker for something:

| Write | Requirement | Otherwise |
|---|---|---|
| Plain upsert or delete, on any topic, including a verdict source | none: any producer, any acks, any client version | — |
| Intent (`insert`, `update`, `removeExisting`) | an idempotent producer, and acks including `View` (`All \| View` or `Leader \| View`) | `InvalidRequest`. The client refuses locally first. |
| `autoincrement` create (key 0) on a topic **without** a constrained view | an idempotent producer, and `All` (with or without `View`). It can't be rejected: its id is unique by construction and skips the key index (§6.4). | `InvalidRequest` |
| `autoincrement` create (key 0) on a topic **with** a constrained view | an idempotent producer, and `All \| View`, so the id is returned only together with the verdict | `InvalidRequest` |
| Transactional or explicit atomic batch (`p.batch`) on a source of a constrained view | allowed once Part G (§9b) ships; refused before that | `InvalidRequest` before Part G |
| An explicit-key intent (`insert` with a key, `update`, `removeExisting`) inside a transaction or atomic batch, on any topic | the same: judged by the Part G vote, refused before it | `InvalidRequest` before Part G |

A key-0 create is not an explicit-key intent. On a topic with only a key index, it is allowed in transactions, as in §6.6.

- **Why intents need `View`.** Their verdict exists only after the fold, so without `View` the writer could never learn it. **Why idempotent:** a non-idempotent retry would store a second copy, which would fail with `KeyExists` against the first.
- **Why autoincrement creates need `All`.** With a `Leader` ack, an id handed out could be cut by a failover and given to another record. Where a constraint can reject the create, they also need `View`; otherwise the client would get an id for a record that never becomes visible.
- **Why transactions and atomic batches are refused on constrained sources.** A record is judged only after its transaction commits. If one record of a committed transaction were then rejected, the transaction would be half applied. Part G (§9b) makes the partitions vote before the commit is decided, which removes the refusal.
- **Old clients** (version 11 and earlier) never send `View`, so they never receive 51–53. Their writes are judged like any other, and a rejected one simply never becomes visible. The rejection count is in the stats (§13).

**11.3 Producer** (`ProducerImpl`)

- **Per-record verdicts.** On a verdict source, a `View` ack carries a trailing rejection list: `count u16`, then `(recordIndex u16, code u16, constraintId u32)` per rejected record, gated on client version 12. An empty list means every record was accepted.
- **When the ack goes out.** As soon as the fold has judged the batch's last record. It doesn't wait for the verdict marker to commit, because a new leader reaches the same verdicts (§8.5).
- **The client.**
  - Each record of a coalesced batch gets its own result: `Ok`, or its code and `constraintId`.
  - The batch was stored, so its sequences were consumed normally. There's no `markUnresolved_`, no epoch bump and no resend.
  - Pipelining is unchanged: any number of batches can be in flight on a verdict source.
- **Retries.** A retry that dedupes gets the stored batch's verdicts. If the fold has passed the batch, they come from the bitmap. Otherwise the retry is parked like a `View` ack until it has.
- **The result** exposes the code, the `constraintId` and the assigned key. A `Client` helper resolves the constraint id to `(viewName, constraintName)` from the catalog.

## 12. Versions and rolling upgrade

- **Feature level.** A new cluster level, `MetadataFeature::kCheckedWrites`, sits next to `kStorageIdentity` (`ReplicationMessages.h:464`). Until every broker supports it, the following are refused:
  - DDL that uses `autoincrement`, `key.index`, a plan-v2 view, or `unique`/`check`;
  - non-zero intents and the `View` ack bit;
- **Version bumps.** Client version 12 covers the `View` ack bit, the per-record rejection list, the new codes, `assignedBase`, `GetViewRowK` and `ScanViewRows`. Verdict markers are a new control-record type, so `kCheckedWrites` also gates them. Plan v2, checkpoint v3, and catalog event formats bump their versions following the existing frozen-format rules. Old formats still decode.

## 13. Observability

- **Per verdict source:**
  - rejections for each constraint and for each intent code;
  - Part G: votes (yes, no, by code), vote latency, `foldPausedOnTxnLock` time, and `txnMarkerWithoutVote`;
  - verdict lag, which is the LEO minus the minimum fold cursor of the verdict views, and so the delay consumers see;
  - the verdict watermark `W`, markers written, and rejected records not yet compacted;
  - autoincrement: the current counter, the reservation R, and ids burned (including ids of rejected creates).
- **Per view:** `uniqueConflicts`, and scan rows and pages.
- All of these appear in `show view` / `show topic` and the metrics export.

## 14. Hot path and benchmarks

- **Untyped topics, and typed topics that already validated.** An ABAB n=3 run of `ProducePathBenchmark` and `WorkerProduceLatencyBench` must show no regression, including the move from the hash-set lookup to the per-partition pointer.
- **Typed topics that didn't validate before.** These now pay the per-record decode. Measure the added cost per batch with the same benches, budgeted at most 1 µs p50 for a one-record batch (my call). This is a deliberate, owner-approved cost, not a regression.
- **Write hook.** Autoincrement assignment, including the scratch copy, stays under 1 µs per batch of up to 4 KiB (my call).
- **Judging.** Computing prospective rows and evaluating rules adds at most 1 µs per record to the fold on `ViewFoldBench`, for one unique view plus one count check (my call). Partitions without verdict views fold exactly as today.
- **Consumers of a verdict source.** End-to-end consume latency rises by no more than the verdict lag. Measure it on `WorkerProduceLatencyBench` with a consumer attached.
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
- source key: new, existing, or written earlier in the same batch
- user: the same user or another user
- group: at 11, at 12, or already over the limit
- name: free, taken by another record, or taken by the same record
- keys of every width: u16, u32, u64, i64 and `string(N)`. The typed `remove` must route exactly as `send(rec)` does.

**Timing and faults:**
- a single record
- writes back to back before the ack, several batches in flight
- a batch that collides with itself
- two writers racing for the last slot or the same name
- an ack timeout, then a retry, before and after the fold has judged the batch
- failover between the append and the HW, between the HW and the fold, and between the fold and the marker's commit
- truncation of an uncommitted tail
- restart during a view rebuild
- a view created on a non-empty topic, with writes running during its build
- `create or replace` while writes are running
- drop while writes are running

**Verdicts, markers and readers:**
- a rejected record is never delivered: raw subscription, consumer group, filtered subscription, and replay from the log start
- other views on the partition skip a rejected record
- a rejected record that is the latest for its key: compaction keeps the accepted value
- the cleaner never compacts above `W`
- a refold below `W` doesn't judge, and a constraint changed since gives the same view
- a new leader judging past `W` reaches the same verdicts and writes the markers
- several verdict views, where one rejects: no view changes
- Part G:
  - a two-partition transaction where one partition votes no: nothing commits, and `commitTransaction()` returns the code and constraint id;
  - all votes yes: both partitions become visible, each in one step, and no read sees part of the transaction;
  - an abort after a yes vote restores the before-images;
  - a record outside the transaction that touches a locked row pauses the fold until the marker, then is judged against the final state;
  - two transactions racing for the same name or the last slot;
  - failover of a voter before and after its `TxnJudge` commits;
  - coordinator failover with votes out, then a client retry: same answer;
  - a constraint created or dropped while a transaction is open;
  - a transaction over `txn.judged.records.max` gets `TxnTooLarge`;
  - an atomic batch with one bad record: the whole batch is rejected and no view changes;
  - the transaction timeout while rows are locked
- a predicate failing on a row that is already over its limit: a rename passes, a create fails, a delete passes
- before Part G, a transactional and an atomic batch on a constrained source are refused; on an unconstrained source they work as today
- verdict lag while a constraint view builds: delivery waits, writes don't
- failover right after a constraint-set change record: the new leader judges every offset with the same set as the old one
- a new leader that hasn't installed the view named by a change record waits rather than judging
- a delete that would break `check (balance >= 0)` is rejected; a delete in a group already over its limit passes
- a batch mixing key-0 creates with other records is refused
- a retry after failover and compaction still returns its ids

**Autoincrement:**
- 1 record and k records per batch
- P = 1 and P > 1
- a batch that crosses a reservation boundary
- failover just after, and just before, a reservation
- truncation of a reservation together with its data
- compaction after the highest id is deleted: the id must not be reused
- a retry that returns the same ids
- a create rejected at fold: its id is burned and never reused
- an `IMPORT` id below and above the counter
- an explicit id without `IMPORT`
- exhaustion at the maximum
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
- persisted views over transient sources, each for a Replace view and an aggregate (`count(*)` and `Min`):
  - a full cluster restart keeps the checkpoint rows, and the cursor restarts at the new ring;
  - a single-node restart with the ring re-replicated: a re-fold of already-folded records doesn't double count;
  - failover to a node with an older checkpoint while the ring evicted part of the gap: stale rows stay, and `transientRecordsEvicted` counts the gap;
  - a keyless transient source is refused for a persisted view;
  - the `View` ack on a transient source;
  - code 37 is never sent
- UTF-8:
  - valid input: ASCII, 2-, 3- and 4-byte sequences;
  - invalid input: overlong forms, surrogates, code points above U+10FFFF, a sequence truncated at the end of the field, and a stray continuation byte;
  - the zero padding of `string(N)`;
  - `bytes` fields are never checked
- `lower()`:
  - a reference table cross-checked against `UnicodeData.txt`;
  - growing characters (Ⱥ → ⱥ) at the maximum width;
  - shrinking characters (İ → i);
  - `lower('Foo')` from QL and from the client key builder equals the broker's key;
  - a plan pinned to an unknown Unicode version is refused

**Clients:**
- a non-idempotent client, an `acks=leader` client, an `acks=none` client and a v11 client, each writing plain records to a verdict source: accepted ones are visible, rejected ones never are
- intents without `View`, and from a non-idempotent producer, are refused; autoincrement creates without `All` are refused
- a coalesced batch with one bad record: only that record fails, the others are visible, and the next batch continues
- the `View` flag, combined with `Leader` and with `All`:
  - acked only after every view has folded the write, carrying the verdicts on a verdict source;
  - behind an open transaction on an unconstrained source, it waits until the transaction resolves;
  - on a partition with no views, it adds nothing;
  - leader loss while parked gives `NotLeader`, and a missed deadline gives `Internal`;
  - `View` alone and `None | View` are refused in `ProducerConfig`;
  - unknown `acks` bits get `InvalidRequest`;
  - a broker without `kCheckedWrites` gets `BrokerTooOld` locally;
  - old `acks` values 0 and 1 behave unchanged
- a login race: the loser gets `ConstraintViolation` and its read finds the winner's row
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

**Invariants checked in the stress run** (racing writers, injected failovers, restarts and compaction):
- No user ever has more than 12 live characters.
- No two live characters share a `lower(name)`.
- No two records ever receive the same autoincrement id, and no id is ever reused.
- Every acked write is visible in every view once the fold barrier is passed, unless its verdict was a rejection.
- No rejected record is ever delivered to a consumer or folded by any view.
- Every record has the same verdict on every leader and in every refold.
- Every view equals a fresh refold of the log.
- Compaction never removes an accepted value in favor of a rejected one.
- A transaction is either applied on every partition it touched or on none, and no read ever sees part of it within a partition.

**Sanitizers:** ASan, UBSan and TSan runs of `*Regression*` at each checkpoint.

## 16. Work order

Each part ships on its own:

1. **#537:** skip aborted-transaction records in the fold, and re-fold.
2. **Part A (§4):** validation always on for typed topics, wire key = key field, refusing features on untyped and keyless topics, the `raw` marker, and the `TypedTopic` bind check.
3. **The write hook (§3):** the hook skeleton, the per-write requirements (§11.2) and the `kCheckedWrites` feature level.
4. **The verdict pipeline (§3, §8.4–§8.8, §11):** prospective rows and judging in the fold, verdict markers, the rejected-offset bitmap, the consumer bound and filter, the cleaner rule, refold below `W`, constraint-set changes, and per-record verdicts in the `View` ack.
5. **Part B (§5):** intents, the key index as a verdict view, and the typed `insert` / `update` / `remove` / `removeExisting` API.
6. **Part C (§6):** `autoincrement`, the modulo partitioner, reservation records and their retention in the cleaner, `assignedBase`, the `IMPORT` grant, and `insertAndWait(...).key()`.
7. **Part D (§7, §9):** wide and derived keys, the owner map, the seeded hash, checkpoint v3, the prefix index, `GetViewRowK`, `ScanViewRows`, and the client read API.
8. **QL (§10):** `create view` (plan v2), `select`, and the CLI.
9. **Part E (§8.1–§8.3, §8.7):** `unique` and `check` on views.
10. **Part F (§9a):** persisted views over transient sources.
11. **Part G (§9b):** the transaction vote, `TxnJudge`, judging in log order with deferred transactions, before-images and row locks, atomic batches, one-step visibility, and `TxnTooLarge`.
12. **Docs:**
   - `ql.md`, `wire-protocol.md`, `client-cpp.md` and `admin-cli.md`
   - `CATALOG.md` and `SECURITY.md` (the `IMPORT` grant)
   - the root and client `CLAUDE.md`
   - the site's views section, corrected to the syntax that ships

## 17. Calls made in this design (owner to check)

- **Records are judged at fold, after they are stored (design B).** A rejected record is never visible and is removed by compaction. Verdict markers make verdicts durable and tell the cleaner. **Owner-confirmed, 2026-10-22.**
- Acks are bit flags: `Leader`, `All` and the new `View`, which combine (`Leader | View`, `All | View`). The names stay. **Owner-confirmed.**
- Wire: bit 0 is `All`, as today, and bit 1 is `View`. `View` alone is refused. With today's fold, `Leader | View` waits for commit, like `All | View`. (My call.)
- Requirements are per write, not per topic: none for plain writes; intents need an idempotent producer and `View`; autoincrement creates need an idempotent producer and `All`. (My call, following the owner's objection to constraining every producer of a topic.)
- **Transactions get level 2 (atomic validation) and level 3a (atomic visibility within a partition)** through a vote before the commit (Part G, §9b). Level 3b, across partitions, is left for the query API. **Owner-confirmed, 2026-10-22.**
  - A voting partition judges a transaction's records together at a `TxnJudge` record. The fold pauses at a record that touches a row locked between the vote and the marker. (My call.)
  - A voting partition takes at most 4096 records per transaction (`TxnTooLarge`). (My call.)
  - A commit marker with no vote is judged at the marker, all or nothing on that partition, and counted as a bug. (My call.)
  - Until Part G ships, transactions and atomic batches are refused on constrained sources.
- Deletes are judged like any record, so a delete can fail a `check` it would break (`balance >= 0`). A row that already broke its rule, which is possible only for pre-existing data, accepts deletes and any change that doesn't touch the checked columns. (My call.)
- Every constraint-set change takes effect at a control record in the source log, so every leader judges each offset with the same rules. (My call.)
- A key-0 create needs `All | View` where a constraint could reject it, and `All` otherwise. A batch with key-0 creates contains only key-0 creates. (My call.)
- On `autoincrement` topics, the cleaner keeps each live producer's last 32 batches, so retries after failover can return their ids. (My call.)
- A verdict view never rejects for capacity reasons. Exhaustion stops the fold with an alert. (My call.)
- `check` takes any predicate over the view row's own columns and constants. (My call, following the owner's point that checks aren't only counts.)
- A new constraint judges from the LEO at install. Delivery of newer records waits while it builds, and writes don't. (My call.)
- The verdict marker interval is 100 ms when nothing is rejected. (My call.)
- Pre-existing unique conflicts: the smallest source key owns the key, with promotion on delete.
- `autoincrement` topics are compact-only, with no retention.
- An `IMPORT` above the reservation appends a new reservation first.
- The ids of creates rejected at fold are burned.
- The autoincrement scratch copy (copy into a per-worker buffer, patch the ids, append from the buffer). **Owner-confirmed, 2026-10-22.**
  - **Follow-up: patch in place where the bytes are already Rune's.**
    - On the gateway-routed path the batch has already been copied once into the worker's SPSC op ring (`ClientGateway.cpp:4030-4061`). The worker owns that slot, so it could patch it in place if `handleProduceBatch` took a mutable span on this path.
    - On the shard-ingress path the bytes sit in Aeron's term buffer, which must not be modified, so the scratch copy stays there.
    - The builder should measure the copy first. In-place patching is worth doing only if the copy shows up in the budget.
- The key field encodes to at most 16 bytes, and `Key` truncation becomes an error.
- Untyped topics stay as raw logs: no field list means untyped. Every database feature needs a typed topic.
- Persisted views may read transient sources. They keep their checkpointed rows across a restart and have nothing to replay. **Owner-confirmed, 2026-10-22.** A keyless transient source can feed only a transient view. (My call.)
- Typed strings must be valid UTF-8, and `lower()` is Unicode simple lowercasing with an ASCII fast path, pinned to a Unicode version. There are no collations, and `casefold()` is reserved. **Owner-confirmed, 2026-10-22.**
- A `lower(string(N))` key component is sized at the largest lowered width, and the 128-byte limit is checked at DDL time. (My call.)
- View keys are capped at 128 bytes.
- Autoincrement topics use the modulo partitioner, with a reservation step of 4096 and an id cap of 2^53 − 1.
- Explicit ids on autoincrement topics need the `IMPORT` grant.
- Performance budgets: under 1 µs per record for judging, and under 1 µs for id assignment, including the copy. No regression for untyped topics or for typed topics that already validated. The new validation cost on typed topics that didn't validate before is measured and accepted.

## 18. Left to the builder

- The exact row layout for key bytes and prefix links.
- Whether the owner map, prefix index and key index are checkpointed or rebuilt on replay.
- The choice of seeded hash.
- How a verdict view computes prospective rows without writing them, within the no-allocation rule.
- The layout of before-images and row locks, within the `txn.judged.records.max` bound.
- The wire format of `TxnVote` and its response, and of the `TxnJudge` record.
- The encodings of the verdict marker and the reservation control record. Both must be distinct from transaction markers and skipped by every reader.
- The on-disk form of the rejected-offset bitmap, following `TxnCleanState`.
- The page row cap and the resume-token format.
