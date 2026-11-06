# Rune QL

Rune QL is the statement language of the `rune` command line and the DDL of the catalog. It defines and evolves
topics, creates and drops views, reads view rows, reads the catalog's history, and manages users, roles and grants. The grammar is the one
in [`ql/src/Parser.cpp`](../../ql/src/Parser.cpp); every example on this page was parsed with it.

## Lexical rules

- Keywords are case-insensitive. Names keep their case.
- A statement ends with `;`. `rune ql -e` takes exactly one statement; `-f` takes many.
- `--` starts a line comment. There are no block comments.
- A string is `'...'` with no escapes. Time strings are ISO 8601 UTC, `'YYYY-MM-DDTHH:MM:SSZ'`.
- A name is an identifier (letters, digits, underscore). A name with `.`, `@` or `-` is written as a string.
- Dotted setting keys and `all-in-sync` are written without spaces.

## Topics

```sql
topic <name> [ { <field> <type> [ autoincrement ], ... } ] [ partitions <n> ] [ replicas <r> ] [ key <field> ]
      [ with ( <setting> = <value>, ... ) ] ;
transient topic ... ;
```

- `partitions` defaults to 1.
- An **untyped** (raw) topic has no field list and no `key`: records are opaque bytes. It has no database features
  (views, filters, key index, intents, `autoincrement`, constraints); those need a typed topic.
- A **typed** topic has a field list; `key <field>` is optional. A typed topic **without** a key is an append-only typed
  stream: records are validated, but it has no database features (key index, intents, `autoincrement`, views). With a key, the
  schema goes to the registry and C++ types are generated from it (`rune schema generate`). A typed topic validates every write: each record decodes against a schema
  version the topic knows, every `string` and `string(N)` holds valid UTF-8 (`string(N)` is `N` bytes, zero-padded), and
  the record key equals the key field's bytes. A batch with any invalid record is refused with `SchemaValidationFailed` (24).
- A key field is an integer type or `string(N)` with `N` at most 16; any other key field type is refused with
  `InvalidSchema` (23).
- `transient topic` replicates in memory and never writes to disk. Persistence is fixed at creation.
- A view may read a keyed transient topic, persisted or transient; a persisted one keeps its checkpointed rows across a restart (see SELF-HOSTING.md, Transient topics).
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
| `key.index` | `on`, `off` | `off` (`on` with `autoincrement`) | no |
| `autoincrement.reserve` | 1 to 1 048 576 | 4096 | no |
| `autoincrement.max` | at least `2 * partitions - 1`, at most 2^53 - 1 | 2^53 - 1 | no |
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

### Autoincrement

- `autoincrement` on a `uint64`, `int64` or `uint32` key field makes the broker assign the key of every record written
  with key 0 (a create). It implies `key.index` and modulo routing (`id mod partitions`), needs
  `cleanup.policy = compact` and no retention (`retention.ms` and `retention.bytes` stay -1), and needs a new, persisted
  topic and every broker at feature level 5. The key field, its type, `autoincrement` and the partition count never change.
- The ids of partition `p` of `P` are `n * P + p` for `n = 1, 2, ...`: dense per partition, never reused, with gaps after a
  failover, a rejected record or an aborted transaction.
- `autoincrement.reserve` (default 4096, 1 to 1048576) is how many ids the leader reserves in the log at a time.
  `autoincrement.max` (default 2^53 - 1, 2^32 - 1 for `uint32`; at least `2 * partitions - 1`) is the last id; past it a create is
  refused with `AutoIncrementExhausted` (58). Both need `autoincrement`.
- A record with an explicit key is an import: an `insert` the broker stores as sent, after the counter is moved above it.
  It needs the `IMPORT` grant (below) and a key that is at most `autoincrement.max` and belongs to its partition. A plain
  upsert is refused; `update`, `removeExisting` and deletes behave as on any key-index topic.

```sql
grant import on topic 'user' to user backfill;
```

`IMPORT` is a topic operation that nothing implies, and it needs every broker at feature level 5.

`key.index`, `autoincrement.reserve` and `autoincrement.max` are creation-only, like the other settings marked "no" above.

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

## Views

```text
createView := "create" [ "or" "replace" ] [ "transient" ] [ "unique" ] "view" NAME "key" keyspec "as" "select" items
              "from" NAME [ "group" "by" groupItem ( "," groupItem )* ]
              ( "constraint" NAME "check" "(" predicate ")" )* ";"
keyspec    := keyItem | "(" keyItem ( "," keyItem )* ")"
keyItem    := column | "lower" "(" column ")" "as" NAME
items      := "*" | item ( "," item )*
item       := column [ "as" NAME ]  |  AGG "(" ( "*" | column ) ")" "as" NAME      AGG := count | sum | avg | min | max | last
groupItem  := column | "lower" "(" column ")"
predicate  := orExpr      orExpr := andExpr ( "or" andExpr )*      andExpr := notExpr ( "and" notExpr )*
notExpr    := "not" notExpr | "(" orExpr ")" | column CMP literal
CMP        := "=" | "<>" | "<" | "<=" | ">" | ">="      literal := [ "-" ] INT | STRING | "true" | "false"
```

A view is a materialized table that the broker keeps current from one typed, keyed source topic, and that a client reads by key.

- **Key.** One column, or a parenthesized list. Each component is an integer, `bool` or `string(N)` column; `lower(name) as nameKey`
  keys a `string(N)` column by its lowercased value, under the output name after `as`. The key bytes are the components in
  declaration order, at most **128 bytes**: an integer takes its width (`uint16` 2, `int64` 8, ...), `string(N)` takes `N`, and
  `lower(string(N))` takes `floor(3N / 2)` bytes (the widest lowering at Unicode 16.0.0; `string(16)` is 24). The encoding is in
  [codec.md](codec.md#view-keys). `lower()` is Unicode simple lowercasing with no locale rules, and the plan pins the Unicode version.
- **Select items.** `*` is every source column. `name as alias` renames a column. An aggregate always has an `as` name.
  `count(*)` counts the group's records.
- **Aggregates and `group by`.** A view with any aggregate item is an aggregate view: its `group by` columns are its key
  (`group by` must equal the `key` list, and `group by lower(name)` matches `lower(name) as ...`). A non-aggregate view takes no `group by`.
  An aggregate view needs a source with one partition (`ViewNotCoPartitioned`, 36) and an integer source key of at most 8 bytes.
  A group folds the latest record per source key, so to count events per user the source needs a per-record key (see
  `device_login` in the example below).
- **The identity rule.** A projection view must keep each source record identifiable: its key must include the source key
  column, or the view must be declared `unique`. Otherwise the broker answers `ViewKeyExpressionInvalid` (59) with
  `key does not identify the source record; declare the view unique or include <keyField>`.
- **`unique`.** `create unique view` declares that no two source records may produce the same view key; a second one would be
  refused at fold time as `ConstraintViolation` (55). A unique view is a projection (an aggregate with `unique` is 61).
- **`constraint NAME check (...)`.** A rule over the view's own output row, judged when a record is folded: `characters <= 12`
  rejects the record that would make an aggregate row break it. A predicate compares a view column with a constant of the same type, joined by `and`,
  `or`, `not` and parentheses; constraint names are unique within the view. The compared columns are fixed-width of at most 16 bytes
  (an integer, `bool`, `float64` or `string(N)` with `N` up to 16); QL refuses a wider or variable column locally.
- **`unique` and `check` are not enforced yet.** The statements parse and are authored into the plan, but the broker refuses a plan that
  carries `unique` with `ViewUniqueInvalid` (61) and one that carries `check` with `ViewConstraintInvalid` (60) until the
  view-constraint engine ships. Every other view statement works.
- **`transient`** makes the view in-memory only (no checkpoint). **`or replace`** alters a view that exists (`AlterView`, at its
  current version) and creates it when it does not; without it an existing name is `ViewExists` (32). A view cannot read another view.

```sql
create view character_by_id key characterId as select userId, name, gender from character;
create view account_characters key (userId, characterId) as select name, gender from character;
create view account_stats key userId as select count(*) as characters from character group by userId;
-- parsed and authored; the broker answers 61 until the constraint engine ships:
create unique view character_name key lower(name) as nameKey as select characterId from character;
drop view account_stats;
```

## Reads

```text
select := "select" ( "*" | column ( "," column )* ) "from" NAME "where" column "=" value ( "and" column "=" value )* [ "limit" INT ] ";"
value  := literal | "lower" "(" STRING ")"
```

`select` reads rows of a view by its key; it needs `READ` on the view.

- A **point read** names every key column (`where userId = 42 and characterId = 7`) and answers one row, or `(no row)`.
- A **scan** names every key column but the last, in key order (`where userId = 42`), and answers the rows under that prefix.
  `limit` caps a scan (a point read takes none). A view with a one-component key has no scan. Rows come in table order, not key
  order. A scan whose view changes under it mid-way stops with `the scan was invalidated by a concurrent change; run it again`.
- Any other `where` is refused before anything is sent, for example `where must name every key column (userId, characterId) for a
  point read, or the leading (userId) for a scan`. There is no read by a shorter prefix or by a non-key column.
- `lower('Ada')` in a `where` lowers the literal the way the view's `lower()` key component does, so
  `select * from character_name where nameKey = lower('Ada Lovelace');` finds the row stored under `ada lovelace`. A literal is checked
  against the column's type and width (`'...' is 20 bytes; column 'name' is string(16)`).
- The result is a table with a header line and a last line `(N rows)`:

```text
rune ql -e "select name, gender from account_characters where userId = 42;" --broker rune-1:40200
name  gender
Ada   1
Brin  0
(2 rows)
```

## Showing topics and views

```sql
show topic character;
show view account_stats;
```

`show topic NAME` prints the topic's definition (partitions, replication, kind, `key.index`, `autoincrement`, partitioner, config, schema) and
then, for each partition, the counters its leader reports (see [`rune stats`](admin-cli.md#rune-stats)). `show view NAME` prints the
sources, key, sink partitions, persistence, output columns, the key spec, `unique`, `group by`, each `constraint ... check (...)`
and the pinned Unicode version, then the view's counters. Both need `DESCRIBE`; the counters are the [GetStats](wire-protocol.md#stats-client-version-17) ones. `show history`, `show ... as of` and `diff` are the catalog reads
described below.

## View compile codes

A `create view` that the broker cannot compile is answered with one of these codes (the detail names the column or rule). The
C++ constants are `ErrorCode` values; codes 59 to 61 reach only sessions at client version 17 or above (an older client sees `InvalidRequest`, 39).

| Code | Name | Triggered by |
|---|---|---|
| 34 | `ViewFieldUnresolved` | The plan names a column the source does not have (or an ambiguous one), or an aggregate view's non-key column is not an aggregate. |
| 35 | `ViewKeyInvalid` | The key does not appear in the output, `group by` does not equal the key, `group by` has no aggregate column, or the source has no key (a keyless topic cannot be a view source). |
| 36 | `ViewNotCoPartitioned` | An aggregate view, or a view keyed by anything but the source key, over a source with more than one partition. |
| 59 | `ViewKeyExpressionInvalid` | A key component that does not compile: a type that is not an integer, enum, `bool` or `string(N)`; `lower()` on a type other than `string(N)`; a duplicate key column; a key over 128 bytes; a `lower()` pinned to another Unicode version; the identity rule above; an aggregate over a source key wider than 8 bytes. |
| 60 | `ViewConstraintInvalid` | A `check` constraint: no name, a duplicate name, a column that is not in the view, or a comparison of different types. While the constraint engine is missing, any `check`. |
| 61 | `ViewUniqueInvalid` | A `unique` clause that does not compile (`unique` on an aggregate). While the constraint engine is missing, any `unique`. |

## The RegressorsPath schema

The whole backend schema of the RegressorsPath game, as one script
([`ql/tests/golden/regressorspath.rql`](../../ql/tests/golden/regressorspath.rql); the test suite parses, plans and runs it):

```sql
topic user { userId uint64 autoincrement, provider uint16, subject string(64), createdAt int64 } partitions 1 replicas 3 key userId with (cleanup.policy = compact);
create unique view user_identity key (provider, subject) as select userId from user;
topic device_login { loginId uint64 autoincrement, userId uint64, deviceFp string(64), platform uint16, ts int64 } partitions 1 replicas 3 key loginId with (cleanup.policy = compact);
create view device_history key (userId, deviceFp) as select min(ts) as firstSeen, max(ts) as lastSeen, count(*) as logins, last(platform) as platform from device_login group by userId, deviceFp;
topic character { characterId uint64 autoincrement, userId uint64, name string(16), gender uint16, hairSkin uint16, hairSheet uint16, headSkin uint16, createdAt int64, lastPlayed int64 } partitions 1 replicas 3 key characterId with (cleanup.policy = compact);
create unique view character_name key lower(name) as nameKey as select characterId from character;
create view account_stats key userId as select count(*) as characters from character group by userId constraint max_characters check (characters <= 12);
```

- `device_login` is keyed by a per-login `loginId`, not by `userId`: an aggregate keeps the latest record per source key, so a
  per-user key would count every user's logins as 1. A login is written with key 0 and the broker assigns the `loginId`.
- `user_identity` and `character_name` (`unique`) and `account_stats` (`check`) show the constraint statements. Against a broker
  before the view-constraint engine they are answered 61 and 60, so a deployment creates the other views now and these later.

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
OPERATION  := "read" | "write" | "create" | "alter" | "drop" | "describe" | "admin" | "import"
NAME       := IDENT | STRING
```

Valid operation and resource pairs (anything else is refused): `TOPIC` takes `READ WRITE CREATE ALTER DROP DESCRIBE IMPORT`;
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
`ShowHistoryDef`, `ShowAsOfDef`, `DiffDef`, the security definitions (`CreateUserDef` through
`ShowSecurityHistoryDef`), then `CreateViewDef`, `SelectDef`, `ShowTopicDef` and `ShowViewDef`.
`viewSourceOf(topic)` describes a topic as a view source and `applyViewPlan(createView, source)` turns a `create view` into the
authored `rune::view::ViewPlan` that `Client::createViewAndWait` sends (see [client-cpp.md](client-cpp.md#materialized-views));
the broker, not the parser, judges widths, partitions and the identity rule. `applySecurityPlan` turns a security statement into a `SecurityPlan` for
`Client::securityDdlAndWait`; `redactedText` renders the text that is sent and logged.
