# Rune — Catalog history and replication (`__catalog`)

**Status:** approved by the user on 2026-10-13.
**Scheduling:** **before Phase 2b Part D**, confirmed by the user on 2026-10-13, so that D's `rune console` / `rune schema` are built on these reads.
**Repos:**
- rune-broker: the catalog log, AdminWorker, apply, bootstrap, and the removal of `RUNE_TOPICS`.
- rune-protocol: DDL Redirect, and the reads, which use the existing Subscribe.
- rune-codec: the shared replay library (`rune_catalog`), which the broker, the client and the CLI all compile from source.
- rune-client-cpp: the history / as-of / diff API over the shared replay library.
- rune-ql: `SHOW HISTORY`, `AS OF`, `DIFF`.
- The `rune` CLI: `catalog export | replay | diff`.

*Amended 2026-10-13 (pre-flight): the shared replay library lives in rune-codec, not rune-client-cpp (ruling F21).*

## 1. Goal

Every catalog change becomes an **event** in a replicated internal topic, `__catalog`. That one log provides four things:

1. **Audit:** who changed what, when, and with which statement.
2. **Time travel and diff:** any object as of a version, time or offset; the differences between two points.
3. **Replay to rebuild:** reconstruct a catalog, or clone one into another cluster.
4. **Replication:** every broker derives its catalog by applying the same ordered log.

The same log **fixes multi-broker runtime DDL**. That is the E2 carry-over: runtime topic creation fails on 3 nodes because the catalog was single-node. It also gives cluster-wide DDL serialization.

There is no production deployment, so there is **no migration**. `__catalog` replaces `__topic_schemas` outright.

## 2. Scope

- **In the log** (user-visible catalog objects):
  - topics: `CreateTopic`, `DropTopic`, `AlterTopicConfig`;
  - schemas: `CreateSchema`, `AlterSchema`, `SetCompatibility`;
  - views: `CreateView`, `AlterView`, `DropView`.
  
  Event kinds are reserved for tenants and ACLs, which the multi-tenancy work adds to this same log.
- **Not in the log:**
  - internal topics: `__catalog` itself, `__consumer_offsets` and transaction state. The cluster creates these at bootstrap, and they have no history.
  - partition placement and leadership. That stays in the consensus metadata log (§5.4).
- **Removed:** `RUNE_TOPICS` and every static-topic path (§8).

## 3. `__catalog`

- One partition, replicated to **every broker**.
- Written with acks=all; minIsr is a majority.
- **Unclean leader election is disabled.**
- **Never compacted, never retention-deleted.**
- Always disk-backed. It is never transient.
- It is the **single source of truth** for the catalog. A broker's in-memory catalog is purely a function of the `__catalog` prefix it has applied.

## 4. Events

Each event is one record in `__catalog`. Its encoding is fixed-layout and versioned.

| Field | Meaning |
|---|---|
| `formatVer` u16 | The event-format version. It selects the frozen apply rules (§6). |
| `kind` u16 | One of the kinds in §2. |
| `objectId` u64 | The object's permanent identity. It is assigned at creation and never reused. |
| `objectName` | The name at the time of the event. |
| `prevVersion` / `newVersion` u32 | The object's version before and after the event. A create has `prevVersion` 0. |
| `statement` | The original DDL text, verbatim. Empty for API-issued DDL that has no text. |
| `ops` | The parsed, resolved operations, for example field names resolved to durable tags. |
| `principal` | Who issued it. Until authentication exists: session id, client name and source address. Afterwards: the authenticated principal, in the same field. |
| `replayedFrom` | Set only on events written by `rune catalog replay`: the source cluster's id, offset and original principal. |
| `timestampMs` i64 | The leader's wall clock when it appends the event (the commit time is not known before the append, and the event is immutable). Informational only; the **offset** is the order. |
| `fingerprint` u64 | A fingerprint of the object's **resulting state**. The resulting state itself is NOT stored. |

**Drops** are events. A dropped name may be reused, but the new object gets a new `objectId`, so history never conflates the two.

*Amended 2026-10-13 (pre-flight): `timestampMs` is stamped at append, not at commit (ruling F22).*

## 5. Write path, serialization, apply

### 5.1 Writing a DDL

1. A client sends DDL to any broker. A broker that is not the `__catalog` leader answers with the existing **Redirect** to the leader, which clients already follow.
2. On the leader, the single **AdminWorker** thread does the following, in this order:
   - validates against its applied catalog: name collisions, schema compatibility mode, view validity, `replicationFactor` bounds, and so on;
   - assigns `objectId` and the versions;
   - computes the `fingerprint`;
   - appends the event.
3. Once the event is committed (HW past it), the AdminWorker applies it locally (§5.3) and answers OK.
   - For `CreateTopic`, the OK is held until the topic serves. This keeps today's C1 rule: an uncommitted topic serves nothing, and an OK that still isn't serving after the bound is answered `Internal`.

The result is **cluster-wide serialization**: one leader and one AdminWorker, so concurrent DDL and name collisions are impossible across brokers.

**Name case.** Names are case-sensitive, but a CREATE whose ASCII-case-folded name equals a live object's folded name is refused (same code as a duplicate name), because topic directories share a case-insensitive filesystem on macOS; BrokerNode.cpp folds at every name lookup. (Amendment, catalog-history Task 21 fix round 1, controller ruling; the rule is frozen with `formatVer` 1 in rune-codec `TopicName.h` / `ApplyV1.cpp`.)

### 5.2 A `__catalog` leader change

- The old leader answers every in-flight DDL explicitly with the retriable `NotLeader`. It is never silent.
- The new leader accepts DDL **only after** it has applied the log up to the HW, so it can never validate against a stale catalog.
- Clients retry within their budget and then fail explicitly.

### 5.3 Applying on every broker

Each broker holds a `__catalog` replica. Its AdminWorker applies events strictly in offset order, from its last applied offset. For each event it:

1. applies the event under the rules of its `formatVer`;
2. **verifies the fingerprint** of the resulting object;
3. updates the local catalog;
4. distributes the change to the workers through the existing rings.

Lifecycle:
- **Restart:** the broker replays its local `__catalog` copy, then catches up from the leader.
- **New broker:** the controller adds a `__catalog` replica, and the broker replays from offset 0.
- **Fingerprint mismatch:** that broker's catalog apply **stops** with a loud error and a metric. The broker keeps serving what it had already applied, but accepts no further catalog changes until an operator intervenes. It never diverges silently.

### 5.4 Definitions versus placement

- `__catalog` holds **definitions**: topic, schema, view.
- The consensus metadata log holds **placement**: partition assignment, leadership and transient lineages.
- When the controller sees a `CreateTopic` applied, it assigns that topic's partitions.
- A topic serves on a broker only when **both** of these hold:
  - its `CreateTopic` is applied on that broker;
  - its placement is committed.
  
  This generalizes C1.

### 5.5 `__catalog` unavailable

When `__catalog` has fewer than minIsr in-sync replicas:
- DDL is answered with the retriable `NotLeader` and fails explicitly once the client's budget runs out.
- Produce and consume on existing topics are unaffected.

### 5.6 Bootstrap

`__catalog` is created during cluster formation, replicated to every founding broker. Its first leader starts with an empty catalog.

## 6. Deterministic replay

- The apply rules for a `formatVer` are **frozen**. A rule change ships under a new `formatVer`, and old events keep replaying under their own rules.
- **One shared replay library** implements the apply rules. The broker AdminWorker, the client API and the CLI all link it, so they cannot disagree.
- Every replay verifies each event's `fingerprint`, wherever the replay runs. A mismatch is an explicit error.
- **Golden logs:** a recorded event log per `formatVer` is checked in. It must replay to fixed fingerprints forever.

## 7. Reads

`__catalog` is a topic. Reads are **client-side**: the client consumes it, with the existing Subscribe from offset 0, and replays it with the shared library. The broker only serves that existing Subscribe for the internal topic id (−6) from its replica, which is transport, not a special read API; no broker-side query, history or replay path exists. Access control comes later with multi-tenancy ACLs.

*Amended 2026-10-13 (pre-flight): the broker serves the existing Subscribe on `__catalog`; replay stays client-side (ruling F19).*

| Need | QL / console | Client API |
|---|---|---|
| Audit | `SHOW HISTORY [TOPIC t \| VIEW v \| SCHEMA t] [SINCE ts]` | `catalogHistory(filter)` |
| Time travel | `SHOW SCHEMA t AS OF VERSION n \| TIME ts \| OFFSET o`, and the same for `VIEW` and `TOPIC` | `catalogAsOf(point)` |
| Diff | `DIFF SCHEMA t VERSION a TO b`, `DIFF CATALOG OFFSET a TO b` | `catalogDiff(a, b)` |

A year of DDL is thousands of events. A full replay takes milliseconds, and the client caches the replayed prefix.

### Replay to rebuild (CLI)

- `rune catalog export [--up-to offset] > file`: the raw events.
- `rune catalog replay --from <file|cluster> --into <cluster> [--up-to offset]`:
  - Each event is re-issued as DDL on the target. The target assigns its own offsets and ids, and records `replayedFrom`.
  - After each step, the target's resulting fingerprint must equal the source event's. A mismatch aborts loudly, at that event.
- `rune catalog diff --from <cluster|file> --to <cluster|file>` compares two catalogs read-only, by object and by fingerprint.

## 8. Removing `RUNE_TOPICS`

- **Delete:**
  - the `RUNE_TOPICS` setting and its parsing;
  - static topic creation and recovery at boot;
  - the static-versus-runtime name-collision boot rule, which becomes moot;
  - the `rf=N` option added in E2.
- **Topics exist only through DDL.** `CreateTopic` carries the partition count, persistence (`disk` / `transient`), the schema, and `replicationFactor`.
  - `replicationFactor` is validated with explicit errors: `replicationFactor ≤ broker count` and `replicationFactor ≥ minIsr`.
- **Tests:**
  - in-process tests use a one-line `TestCluster::defineTopic(...)`;
  - docker e2e scripts create their topics through the CLI or the probe before producing;
  - every existing `RUNE_TOPICS` use is migrated, and none remain (grep gate).
- **Docs:** SELF-HOSTING.md gets a "Topics are created by DDL" section, and every `RUNE_TOPICS` mention is removed.

## 9. Errors (every request answered; no silent fallback)

| Condition | Answer |
|---|---|
| DDL sent to a broker that is not the `__catalog` leader | Redirect to the leader |
| A `__catalog` leader change mid-DDL; a new leader not yet caught up; `__catalog` below minIsr | `NotLeader` (retriable), then an explicit client failure when the budget is exhausted |
| Name collision | `TopicExists` (25), as today |
| Invalid DDL: compatibility, view, `replicationFactor`, … | the existing specific codes, otherwise `InvalidRequest` (39) with detail |
| Fingerprint mismatch on apply | the broker stops applying, with a loud error and a metric. DDL sent to that broker is answered `Internal` (6) until resolved |
| Replay or diff fingerprint mismatch | an explicit tool or API error naming the event offset |

## 10. Testing (interaction matrix)

- **Event kind × timing.** Every kind of §2 is crossed with:
  - steady state;
  - a `__catalog` leader change mid-DDL;
  - a broker restart mid-apply;
  - a new broker joining;
  - `__catalog` below minIsr;
  - concurrent DDL sent to different brokers;
  - drop and reuse of a name;
  - `CreateTopic` racing placement.
- **Golden replay:** per `formatVer`, the recorded logs replay to fixed fingerprints.
- **Seeded, model-checked stress:** random DDL sequences with leader moves and restarts, checked against an independent catalog model. Invariants:
  - every broker has an identical fingerprint at the same applied offset;
  - no DDL is lost or duplicated;
  - every DDL is answered;
  - two DDLs on one name never both succeed.
- **BrokerNode integration, 3 nodes:** runtime `CreateTopic` and `CreateView` through every broker, with a leader kill mid-DDL. This closes the E2 carry-over.
- **Docker e2e, 3 nodes:**
  1. DDL through each broker;
  2. the `__catalog` leader killed mid-DDL;
  3. `rune catalog export`, then `replay` into a fresh cluster;
  4. `rune catalog diff` reports no differences.
- **Gates:**
  - Boot replay of 10,000 events finishes in under 1 s.
  - Produce/consume hot-path benchmarks are unchanged within noise, with zero allocations in steady state.
  - Every DDL is answered in every matrix cell.

## 11. Relation to other work

- **Closes:**
  - the E2 carry-over "multi-node runtime DDL fails";
  - the deferred "multi-node `__topic_schemas` replication" (schema-evolution spec §8).
- **Multi-tenancy and auth** (next design): tenant and ACL changes become `__catalog` event kinds, and `principal` becomes authenticated.
- **Part D:** `rune schema` / the console read through §7. Hence the proposed ordering before D.
