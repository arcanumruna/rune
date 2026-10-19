# The catalog: events, history, replay and the halted-apply runbook

Every change to a topic, schema or view is one **event** in `__catalog`, a replicated internal topic
(see [SELF-HOSTING.md](SELF-HOSTING.md#the-catalog-__catalog)). A broker's catalog is the result of
applying that log in offset order. The same library applies it in the broker, in the client and in
the `rune` CLI, so they cannot disagree.

## The event model

Each event is one record. Its layout is fixed per `formatVer`.

| Field | Meaning |
|---|---|
| `formatVer` | The event-format version. It selects the frozen apply rules. Only 1 exists. |
| `kind` | What changed (the table below). |
| `objectId` | The object's permanent id. Assigned at creation, never reused. |
| `objectName` | The object's name at the time of the event. |
| `prevVersion` / `newVersion` | The object's version before and after. A create has `prevVersion` 0. |
| `statement` | The original DDL text. Empty for DDL issued from the API without text. |
| `ops` | The parsed, resolved operations. Schema changes name fields by their durable tag, not by name. |
| `principal` | Who issued it: session id, client name, source address, the authenticated principal (empty until authentication exists), and the DDL's request id. |
| `replayedFrom` | Set only on events written by `rune catalog replay`: the source cluster id, offset and original principal. |
| `timestampMs` | The leader's wall clock when it appended the event. Informational: the **offset** is the order. |
| `fingerprint` | A hash of the object's resulting state. The state itself is not stored. |

Kinds:

| Kind | Number | Issued by |
|---|---|---|
| `CreateTopic` | 1 | `topic ...;`, `transient topic ...;`, `defineTopic` |
| `DropTopic` | 2 | `drop topic` |
| `AlterTopicConfig` | 3 | `alter topic ... set` |
| `CreateSchema` | 4 | `defineTopic` with a schema on an existing untyped topic |
| `AlterSchema` | 5 | `alter topic ... add column / drop column / rename column` |
| `SetCompatibility` | 6 | `alter topic ... set compatibility` |
| `CreateView` | 7 | `createView` |
| `AlterView` | 8 | `alterView` (a fresh output topic under the same object id) |
| `DropView` | 9 | `drop view` |

Kinds 32 to 63 are reserved for later features; `formatVer` 1 refuses them.

A drop is an event. A dropped name can be created again, and the new object gets a new `objectId`,
so history never mixes the two.

**`formatVer` rules.** The apply rules of a `formatVer` are frozen and never edited. A rule change
ships under a new `formatVer`, and old events keep replaying under their own rules. A reader that
meets a `formatVer` it has no rules for stops with an `UnsupportedFormat` error. Every replay, in
every tool, recomputes each event's fingerprint and compares it with the event's own; a difference is
an error that names the offset.

## Reading the catalog

Reads are done by the client: it subscribes to `__catalog` from offset 0 on any broker, replays it
with the shared library, and answers from the replayed prefix. The broker has no history or query
path of its own.

| Need | Rune QL | Client API |
|---|---|---|
| Audit | `show history [topic T \| view V \| schema T] [since 'TIME'];` | `catalogHistory(filter, timeout)` |
| Time travel | `show topic T as of version N \| time 'TIME' \| offset N;` (also `schema`, `view`) | `catalogAsOf(point, timeout)` |
| Diff | `diff schema T version A to B;` and `diff catalog offset A to B;` | `catalogDiff(a, b, timeout)` |

Times are ISO-8601 UTC, for example `'2026-10-13T12:00:00Z'`. `as of version N` is the object's
version for a topic or view, and the schema version for a schema. `as of time` replays events whose
`timestampMs` is at or before the time, in offset order, and stops at the first later one.

```bash
rune ql -e "show history topic events;" --broker rune-1:40200
rune ql -e "show topic events as of version 2;" --broker rune-1:40200
rune ql -e "show schema player as of time '2026-10-13T12:00:00Z';" --broker rune-1:40200
rune ql -e "diff schema player version 1 to 2;" --broker rune-1:40200
rune ql -e "diff catalog offset 3 to 9;" --broker rune-1:40200
```

A history row is one line: offset, time (UTC), kind, object and object id, version change,
principal, `replayedFrom` (`<cluster>@<offset>` or `-`) and the statement. A diff line is
`+ topic NAME` (only in the second point), `- view NAME` (only in the first) or
`~ topic NAME: field, field` (changed, with the differing fields).

From code, `Client::catalogHistory`, `catalogAsOf` and `catalogDiff` do the same. They block, and
only the thread that constructed the client may call them; another thread constructs its own
`CatalogReader`. A read never hangs: if the broker does not answer before `timeout` the result is a
`Timeout` error, and a fingerprint or decode failure is a `Replay` error that names its offset.

## Exporting, replaying and comparing catalogs

```bash
# the raw events of a cluster, verified, into a file
rune catalog export --broker rune-1:40200 > prod.rcat
rune catalog export --up-to 120 --broker rune-1:40200 > prod-120.rcat

# rebuild: re-issue every event as DDL on another cluster
rune catalog replay --from prod.rcat --into fresh-1:40200
rune catalog replay --from rune-1:40200 --into fresh-1:40200 --up-to 120 --cluster-id prod

# compare two catalogs, read-only, by object and fingerprint
rune catalog diff --from rune-1:40200 --to fresh-1:40200
rune catalog diff --from prod.rcat --to rune-1:40200
```

- `export` verifies every event before it writes; a catalog that does not verify writes no file.
- `replay` first verifies the whole source and issues nothing if it does not. Then it re-issues each
  event as DDL on the target, which assigns its own offsets and ids, and records `replayedFrom`
  (`--cluster-id`, by default the `--from` argument as typed). After every step the target's event
  must be the replay of the source event, with the same fingerprint. A mismatch aborts at that event:
  `replay aborted at source offset N (<kind> '<name>'): ...`.
- Each replayed event has a request id derived from the cluster id and the source offset. A re-run
  after a crash skips the steps that already applied and prints `replayed N events (...): X issued,
  Y already applied`.
- `diff` exits 0 when the catalogs are equal, 3 when they differ (the differences are printed), 1 on
  an error and 2 on a usage error.

## Errors

Every DDL is answered. No request is silently dropped.

| Condition | Answer |
|---|---|
| DDL sent to a broker that is not the `__catalog` leader | `Redirect` (3) to the leader. Clients and the CLI follow it, at most 3 hops. |
| A leader change during the DDL, a new leader not yet caught up, or `__catalog` below its minimum ISR | `NotLeader` (2), retriable. The client retries for `ddlRetryBudget` (10 s by default), then fails with `NotLeader`. |
| A name that is taken, or differs from a live name only in ASCII letter case | `TopicExists` (25) |
| A bad name | `InvalidTopicName` (40) |
| Invalid DDL: replication factor, a setting out of range, a view over a transient topic, ... | The specific code (for example `SchemaIncompatible` 30, `SchemaConflict` 31, `ViewExists` 32, `ViewSourceNotFound` 33, `ViewSourceTransient` 37, `ViewNotAlterable` 38, `ViewSourceIsView` 41), otherwise `InvalidRequest` (39) with a detail. |
| An unknown topic or view | `UnknownTopic` (7) |
| The broker's catalog apply halted | `Internal` (6) until an operator resolves it. |
| A DDL from a client older than version 8 on a broker that is not the leader | `InvalidRequest` (39) in the request's own response type. See the release note. |
| A DDL answered `Internal` after its event was written | In doubt: it may still take effect. Retry with the same request id. |
| A replay or diff fingerprint mismatch (CLI, API) | An error that names the event's offset. |

A DDL carries a 128-bit request id that stays the same across every resend. The leader answers a
retry of a DDL that already applied with the original result, and answers `InvalidRequest` if the
same id arrives with a different statement.

## halted

Each broker applies the catalog in offset order and checks every event's fingerprint against the one
it computes. If they differ, or an event cannot be applied, that broker's apply **halts**.

**What it means.** The broker keeps serving the topics, schemas and views it had already applied. It
accepts no catalog change, and answers every DDL with `Internal` (6) and the offset. It logs one
critical line, `CatalogApplier: apply HALTED at offset N ...`, naming this page. The cluster's other
brokers are not affected, and a halted broker is not chosen to hold new topic replicas. A halt means
this broker's copy or its software disagrees with the log; it never means data in the topics was
lost.

**The metrics.** Two Aeron counters of the broker's media driver (in `dataDir/aeron` unless
`RUNE_AERON_DIR` is set):

| Counter | Type id | Value |
|---|---|---|
| `rune-catalog-apply-halted node=<id>` | 1 000 217 | 0 while applying. After a halt: the halted offset plus 1. |
| `rune-catalog-applied-offset node=<id>` | 1 000 218 | The last applied offset. |

Alert on the first being non-zero.

**What to do.**

1. Find the halted broker from the counter or the critical log line, and note the offset.
2. Stop that broker.
3. Move `dataDir/__catalog` aside (do not delete it). It holds the log and the `applied` file.
4. Start the broker. It has no local catalog, so it fetches the whole log from the catalog leader and
   applies it from offset 0. Topic directories are left in place and are checked again once the
   catch-up is done (see [Data directory clean-up at boot](SELF-HOSTING.md#data-directory-clean-up-at-boot)).
5. If it halts again at the same offset, the log or the software is at fault, not the copy. Keep the
   moved-aside directory, and compare catalogs, as below.

If the **leader itself** halted, or the halt repeats on a fresh copy, find out which catalog is right:

1. Export from a healthy broker: `rune catalog export --broker <healthy:port> > healthy.rcat`.
2. Export from the halted broker the same way (a halted broker still serves reads of its log):
   `rune catalog export --broker <halted:port> > halted.rcat`. An export verifies every event, so if it
   fails it names the first offset that does not verify.
3. Compare them: `rune catalog diff --from healthy.rcat --to halted.rcat`. The output names the
   objects that differ; exit status 3 means they differ.

Do not use this procedure on a single-node broker, or on a broker that is the only copy of the
catalog. Its local `__catalog` is the only history, so restore it from a backup instead; a broker
without its catalog and without a leader to catch up from serves none of its topics and refuses DDL.
