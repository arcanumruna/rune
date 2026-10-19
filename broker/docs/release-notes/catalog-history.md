# Release note: catalog history

Every topic, schema and view change is now an event in a replicated log, `__catalog`. Topics are
created only by DDL. This release has breaking changes and no migration.

## 1. Breaking: the boot-topics variable is removed

`RUNE_TOPICS` is gone. A broker that finds it set (even to an empty string) refuses to start, with
an error that points to [SELF-HOSTING.md](../SELF-HOSTING.md#topics-are-created-by-ddl). Remove it
from your compose files and unit files, and create the topics with Rune QL once the cluster is up:

```bash
rune ql -f topics.rql --request-id 0123456789abcdef0123456789abcd00 --broker rune-1:40200
```

`SELF-HOSTING.md` has a table that maps every former option to a `with (...)` setting or to a
statement form (`replicas`, `transient topic`). Topics are no longer recovered from the old
`__topic_schemas` and `__views` directories: they are ignored and never changed, and there is no
migration. Delete them by hand when you no longer need them. A topic directory that holds records
and no `topic-id` marker (made by an earlier build) is moved to `dataDir/quarantine/` at the first
boot; its data is never deleted.

## 2. Breaking: client version 8

The client library and `rune` CLI speak protocol version 8. Every DDL now carries a DDL extension:
the statement text, a client name, a 128-bit request id that stays the same across resends, and for
`rune catalog replay` the source it replays.

- New DDL operations: `DropTopic`, `DropView`, `AlterView`, `AlterTopicConfig`. They answer with a
  `DdlResponse` (object id, new version, detail). `DefineTopic` also carries a replication factor and
  the topic config.
- Codes used by DDL: `Redirect` (3), `NotLeader` (2), `Internal` (6), `UnknownTopic` (7),
  `TopicExists` (25), `InvalidRequest` (39, now with a detail text) and the existing schema and view
  codes (30 to 38, 40, 41). The client follows a Redirect (at most 3 hops) and retries `NotLeader`
  with a backoff for `ClientConfig::ddlRetryBudget` (10 s by default).

**Pre-v8 clients.** A DDL from a client older than version 8 cannot follow a Redirect. If it reaches
a broker that is not the catalog leader, the broker answers `InvalidRequest` in the DDL's own
response type, with the text "client too old for cluster DDL routing; upgrade", and logs a warning.
Upgrade every client before you run DDL against a multi-node cluster. On a single node, and on the
leader, an older client's DDL works.

## 3. New

- `show history`, `show ... as of`, and `diff` in Rune QL, and `catalogHistory`, `catalogAsOf` and
  `catalogDiff` in the client. See [CATALOG.md](../CATALOG.md).
- `drop topic`, `drop view` and `alter topic ... set` in Rune QL.
- `rune ql -f <file>` runs a script in order and stops at the first error. `--request-id <32 hex
  digits>` fixes the DDL's id, so a re-run returns the original results instead of `TopicExists`.
- `rune catalog export`, `replay` and `diff`.
- Multi-node runtime DDL: a topic or view created through any broker is created on all of them, and
  two DDLs on one name never both succeed.
- `RUNE_CATALOG_REPL_STREAM_ID` (default 30), the stream of the catalog's replication.
- Metrics: the Aeron counters `rune-catalog-apply-halted` (type 1 000 217) and
  `rune-catalog-applied-offset` (type 1 000 218). A non-zero halted counter means a broker stopped
  applying the catalog; see [the runbook](../CATALOG.md#halted).

## 4. Upgrading

1. Stop the cluster, remove `RUNE_TOPICS` from every broker's environment, and start it. The brokers
   form a cluster with an empty `__catalog`.
2. Create your topics with `rune ql -f`.
3. Upgrade clients before you run DDL from them against more than one broker.

There is no migration: the old catalog directories are not read, so topics from an earlier release
are not carried over, and you create them again. What the broker does with a topic directory that
the new catalog does not name is described in
[Data directory clean-up at boot](../SELF-HOSTING.md#data-directory-clean-up-at-boot): it is moved to
`dataDir/quarantine/`, never deleted.
