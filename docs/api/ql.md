# Rune QL

Rune QL is the statement language of the `rune` command line and the DDL of the catalog. It defines and evolves
topics, drops topics and views, reads the catalog's history, and manages users, roles and grants. The grammar is the one
in [`ql/src/Parser.cpp`](../../ql/src/Parser.cpp); every example on this page was parsed with it.

**What Rune QL does not have yet.** The parser has no `create view`, no `select`, no `get`/`scan` fetches and no composite
keys (`key (a, b)`, `partition by`). Views are created from code (`Client::createView` with a plan built by
`rune/client/view/ViewPlanAuthor.h`, see [client-cpp.md](client-cpp.md#materialized-views)) and dropped with
`drop view`. The website's API page shows view and query syntax that is not in the parser; it is ahead of the code.

## Lexical rules

- Keywords are case-insensitive. Names keep their case.
- A statement ends with `;`. `rune ql -e` takes exactly one statement; `-f` takes many.
- `--` starts a line comment. There are no block comments.
- A string is `'...'` with no escapes. Time strings are ISO 8601 UTC, `'YYYY-MM-DDTHH:MM:SSZ'`.
- A name is an identifier (letters, digits, underscore). A name with `.`, `@` or `-` is written as a string.
- Dotted setting keys and `all-in-sync` are written without spaces.

## Topics

```sql
topic <name> [ { <field> <type>, ... } ] [ partitions <n> ] [ replicas <r> ] [ key <field> ]
      [ with ( <setting> = <value>, ... ) ] ;
transient topic ... ;
```

- `partitions` defaults to 1.
- An **untyped** (raw) topic has no field list and no `key`: records are opaque bytes. It has no database features
  (views, filters, key index, intents, `autoincrement`, constraints); those need a typed topic.
- A **typed** topic needs both the field list and `key <field>`; the schema goes to the registry and C++ types are
  generated from it (`rune schema generate`). A typed topic validates every write: each record decodes against a schema
  version the topic knows, every `string` and `string(N)` holds valid UTF-8 (`string(N)` is `N` bytes, zero-padded), and
  the record key equals the key field's bytes. A batch with any invalid record is refused with `SchemaValidationFailed` (24).
- A key field is an integer type or `string(N)` with `N` at most 16; any other key field type is refused with
  `InvalidSchema` (23).
- `transient topic` replicates in memory and never writes to disk. Persistence is fixed at creation.
- `replicas` is the replication factor, `0` to `32767`; `0` or omitted means every broker. It must be at most the
  broker count and at least `RUNE_MIN_ISR`.
- Topic names: 1 to 249 bytes of ASCII letters, digits, `.`, `_` and `-`, not `.` or `..`, not starting with `__`. Two
  names that differ only by letter case are not allowed.

Field types: `int32`, `int64`, `uint16`, `uint32`, `uint64`, `float64`, `bool`, `bytes`, `string` and `string(N)`
(a fixed-width string of `N` bytes).

```sql
topic events;
topic logs partitions 3 replicas 3;
transient topic presence;
topic player { userId uint64, name string(32), score int64 } partitions 4 key userId;
topic orders partitions 4 replicas 3 with (tier = leader, durability = all-in-sync);
topic audit partitions 1 with (cleanup.policy = compact, segment.bytes = 1048576);
```

### Creation settings (`with (...)`)

Each key may appear once.

| Key | Values | Default | Alterable |
|---|---|---|---|
| `cleanup.policy` | `delete`, `compact`, `compact_delete` | `delete` | no |
| `segment.bytes` | 1 to 2 080 374 783 | 64 MiB | no |
| `retention.check.ms` | at least 1 | 300 000 | no |
| `file.delete.delay.ms` | at least 1 | 60 000 | no |
| `durability` | `majority`, `all-in-sync` | `majority` | no |
| `tier` | `leader`, `majority`, `strict` | `majority` | no |
| `copartition.group` | string | none | no |
| `partition.key` | string | none | no |
| `retention.ms` | at least -1 (-1 = unbounded) | -1 | yes |
| `retention.bytes` | at least -1 | -1 | yes |
| `delete.retention.ms` | at least 0 | 86 400 000 | yes |
| `min.cleanable.dirty.ratio` | decimal 0 to 1, at most 6 places | 0.5 | yes |
| `min.compaction.lag.ms` | at least 0 | 0 | yes |
| `max.compaction.lag.ms` | at least -1 (-1 = off) | -1 | yes |

A value outside its range is answered `InvalidRequest` (39) with a detail such as `retention.ms -2 < -1`. The
`tier` setting is the durability tier of an acknowledged write: `leader` acks on the leader's append, `majority` once a
majority of the in-sync replicas hold it, `strict` also after an fsync.

## Changing and dropping

```sql
alter topic events set retention.ms = 604800000, retention.bytes = 1073741824;
alter topic player add column guildId int64, set compatibility backward;
alter topic player rename column score to points;
alter topic player drop column guildId;
drop topic events;
drop view user_stats;
```

- `alter topic NAME set key = value, ...` changes only the six alterable settings above.
- Schema operations: `add column <field> <type>`, `drop column <name>`, `rename column <old> to <new>`,
  `set compatibility (backward | forward)`. Under `forward` compatibility, `drop column` is refused
  (`SchemaIncompatible`, 30). A stale `expectedVersion` is `SchemaConflict` (31). A schema change never
  invalidates old data: a record is decoded by the schema version it was written with.
- `alter transient` and `set persistence` are rejected.
- `drop topic` on a view is `InvalidRequest` ("is a view; use DROP VIEW"); on the source of a live view it names the
  view. Dropping an unknown name is `UnknownTopic` (7). A dropped name may be created again with a new id.

## History reads

Every DDL is an event in `__catalog`. These statements read it back, client-side, verifying each event's fingerprint.
They need `DESCRIBE` on `CLUSTER`.

```sql
show history;
show history topic events since '2026-10-13T00:00:00Z';
show topic events as of version 2;
show schema player as of offset 12;
diff schema player version 1 to 2;
diff catalog offset 3 to 9;
```

`show history [(topic | view | schema) NAME] [since 'TIME']`; `show (schema | view | topic) NAME as of (version N |
time 'TIME' | offset N)`; `diff schema NAME version A to B`; `diff catalog offset A to B`. The event model and the
halted-apply runbook are in [CATALOG.md](../../broker/docs/CATALOG.md).

## Security statements

All need `ADMIN` on `CLUSTER`, except a user changing their own password. A statement that carries a password literal
must come from a file or stdin (`rune ql -f`); `-e` refuses it, so the password never shows in `ps` or shell history. The
client turns the password into a SCRAM verifier before sending and the log records the statement with
`'<redacted>'`. The minimum password length is 12. The model, the operation-by-request table and the revocation
semantics are in [SECURITY.md](../../broker/docs/SECURITY.md).

```sql
CREATE USER ws1 WITH PASSWORD 'correct horse battery';
ALTER  USER ws1 ADD PASSWORD 'a second password 1234';
ALTER  USER ws1 DROP PASSWORD 1;
ALTER  USER ws1 SET PASSWORD 'a brand new password 99' TERMINATE SESSIONS;
ALTER  USER ws1 DISABLE;
ALTER  USER ws1 ENABLE;
DROP   USER ws1;

CREATE ROLE world_server;
GRANT READ  ON TOPIC player_events   TO ROLE world_server;
GRANT WRITE ON TOPIC PREFIX 'world_' TO ROLE world_server;
GRANT READ  ON GROUP PREFIX 'world-' TO ROLE world_server;
GRANT WRITE ON TRANSACTIONAL_ID 'ws-1' TO USER ws1;
GRANT CREATE ON CLUSTER TO USER ops;
GRANT ROLE world_server TO USER ws1;
REVOKE WRITE ON TOPIC PREFIX 'world_' FROM ROLE world_server;
REVOKE ROLE world_server FROM USER ws1;
DROP ROLE world_server;

SHOW USERS;
SHOW ROLES;
SHOW GRANTS FOR USER ws1;
SHOW GRANTS FOR ROLE world_server AS OF OFFSET 12;
SHOW GRANTS FOR ROLE world_server AS OF TIME '2026-10-13T00:00:00Z';
SHOW SECURITY HISTORY;
SHOW SECURITY HISTORY USER ws1 SINCE '2026-10-13T00:00:00Z';
```

Grammar:

```text
createStmt := "create" ( "user" NAME "with" "password" STRING | "role" NAME ) ";"
alterUser  := "alter" "user" NAME ( "add" "password" STRING | "drop" "password" (1 | 2)
                                  | "set" "password" STRING [ "terminate" "sessions" ] | "disable" | "enable" ) ";"
dropStmt   := "drop" ( "topic" | "view" ) IDENT ";"  |  "drop" ( "user" | "role" ) NAME ";"
grantStmt  := "grant"  ( OPERATION "on" resource "to"   ( "user" | "role" ) NAME | "role" NAME "to"   "user" NAME ) ";"
revokeStmt := "revoke" ( OPERATION "on" resource "from" ( "user" | "role" ) NAME | "role" NAME "from" "user" NAME ) ";"
resource   := "cluster" | ( "topic" | "view" | "group" | "transactional_id" ) ( "prefix" STRING | NAME )
OPERATION  := "read" | "write" | "create" | "alter" | "drop" | "describe" | "admin"
NAME       := IDENT | STRING
```

Valid operation and resource pairs (anything else is refused): `TOPIC` takes `READ WRITE CREATE ALTER DROP DESCRIBE`;
`VIEW` takes `READ CREATE ALTER DROP DESCRIBE`; `GROUP` takes `READ DESCRIBE`; `TRANSACTIONAL_ID` takes `WRITE DESCRIBE`;
`CLUSTER` takes `CREATE ALTER DROP DESCRIBE ADMIN`. User and role names are `[A-Za-z0-9._@-]`, 1 to 128 characters,
not starting with `__`.

## The `rune ql` command

```bash
rune ql -e "topic events partitions 3 replicas 3;" --broker rune-1:40200
rune ql -f topics.rql --request-id 0123456789abcdef0123456789abcd00 --broker rune-1:40200
rune ql -e "show history topic events;" --broker rune-1:40200
rune ql -e "topic events partitions 3;" --dry-run            # parse and print the plan, no broker
rune ql --user admin --password-file admin.pw -f users.rql --broker rune-1:40200
```

- `--broker` may name any broker. A broker that is not the catalog leader answers a DDL with a redirect and the CLI
  follows it (at most 3 hops).
- `-f` runs the statements in order and stops at the first error, naming its statement.
- `--request-id` (exactly 32 hex digits, not all zero) fixes the DDL's id. With `-f`, statement `N` uses the id plus
  `N - 1`. A re-run returns the original result instead of `TopicExists`.
- `--verbose` prints every followed redirect and every retry to stderr. `--dry-run` parses and prints the plan.

The full command list is in [admin-cli.md](admin-cli.md).

## Using the parser from C++

```cpp
#include <rune/ql/Parser.h>
#include <rune/ql/Backends.h>

auto stmt = rune::ql::parse("topic events partitions 3 replicas 3;");   // std::expected<Statement, ParseError>
if (stmt) {
    if (auto* t = std::get_if<rune::ql::TopicDef>(&*stmt)) {
        rune::ql::DefineTopicPlan plan = rune::ql::applyPlan(*t);       // name, partitions, descriptor blob, config
    }
}
auto script = rune::ql::parseScript(text);   // every statement, or the index of the first failure
```

`Statement` is a `std::variant` of `TopicDef`, `AlterTopicDef`, `AlterTopicConfigDef`, `DropTopicDef`, `DropViewDef`,
`ShowHistoryDef`, `ShowAsOfDef`, `DiffDef` and the security definitions (`CreateUserDef` through
`ShowSecurityHistoryDef`). `applySecurityPlan` turns a security statement into a `SecurityPlan` for
`Client::securityDdlAndWait`; `redactedText` renders the text that is sent and logged.
