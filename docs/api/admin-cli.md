# The `rune` command line

One executable, six command groups: `rune console`, `rune schema`, `rune ql`, `rune stats`, `rune catalog` and `rune security`. It is built from
`client-cpp/tools/rune_cli.cpp` and `client-cpp/src/cli/`, and talks to a broker with the C++ client in invoker mode.
Run it with no arguments to print the usage.

```bash
cmake --build build --target rune          # root build: the binary is build/rune-client/rune
```

## Connecting and credentials

Every command that connects takes the broker's client endpoint and, because authentication is on by default, credentials.

| Flag | Environment | Meaning |
|---|---|---|
| `--broker host:port` | | Any broker's client endpoint (the port of `RUNE_CLIENT_ENDPOINT`, `40200` in the shipped compose files). A non-leader answers a DDL with a redirect that the CLI follows. |
| `--user <name>` | `RUNE_USER` | The principal to log in as. |
| `--password-file <path>` | `RUNE_PASSWORD_FILE` | A file holding the password (trailing newline stripped). |
| `--cli-egress <channel>` | `RUNE_EGRESS` | The channel replies arrive on. By default the CLI derives a local UDP endpoint toward the broker. |
| `--cli-stream-in <n>`, `--cli-stream-out <n>` | | Aeron stream ids, when the broker is not on the default `1` and `2`. |
| `--cli-data-dir <dir>` | | Scratch directory of the embedded driver. Default, and for an empty value (`--cli-data-dir ""`): a private temporary directory, removed on exit. A non-empty directory is used as given and left in place. |

A password is **never** accepted on the command line or from the environment: `--password` and `RUNE_PASSWORD` are
refused with an explanation. Against a broker in dev mode (`RUNE_AUTH=disabled`) the CLI connects without credentials and
prints a banner.

Exit codes: `0` success, `1` error, `2` usage error, and `3` for `rune catalog diff` when the two sides differ. `rune console -f` exits with the broker's wire error code (see below).

## `rune ql`

```text
rune ql (-e "<statement>" | -f <file|->) [--dry-run] [--verbose] [--request-id <32 hex digits>]
        [--expected-version N] --broker host:port
```

Executes Rune QL: DDL (topics, views, drops, security) and reads (`SELECT`, `SHOW TOPIC`, `SHOW VIEW`, `SHOW HISTORY`,
`SHOW ... AS OF`, `DIFF`, `SHOW USERS`, ...). The statement text is recorded in the catalog. The grammar and examples are in [ql.md](ql.md).

| Flag | Meaning |
|---|---|
| `-e "<statement>"` | One statement. A statement carrying a password literal is refused here. |
| `-f <file\|->` | A script (`-` is stdin). Runs every statement in order and stops at the first error, naming it (`statement N: ...`). Nothing is issued unless every statement parses. |
| `--dry-run` | Parse and print the plan. No broker needed. |
| `--verbose` | Print every followed redirect and NotLeader retry to stderr. |
| `--request-id <32 hex>` | Fix the DDL's request id (not all zero). With `-f`, statement `N` uses the id plus `N - 1`. A re-run returns the original results. |
| `--expected-version N` | The optimistic-concurrency gate for `alter topic` (0..65535, default 0 = none). A stale value is `SchemaConflict`. |

`rune ql` exits `1` on any failure; it does not map a broker refusal to its wire code (`rune console -f` does).

**`select` and `show`.** `select` prints a table: a header line of column names, one line per row, then `(N rows)`; a point read that
finds nothing prints `(no row)`; a `NULL` aggregate prints `NULL`. Columns are padded to the widest cell. Point and scan rules are in
[ql.md](ql.md#reads).

```bash
rune ql -e "select * from account_stats where userId = 42;" --broker rune-1:40200
rune ql -e "show view account_stats;" --broker rune-1:40200
rune ql -e "show topic character;" --broker rune-1:40200
```

`show topic NAME` and `show view NAME` print the definition (the same text as `show ... as of` prints for the current version),
then the partition counters exactly as `rune stats` prints them. `--dry-run` prints a `create view` with its key, output
columns, constraints and plan version (`v2` when the plan needs it), and a `select` as parsed, without a broker.

**Back-filling an autoincrement topic.** Importing rows with their old ids needs the `IMPORT` grant
([ql.md](ql.md#autoincrement)), which nothing else implies:

```bash
rune ql -e "grant import on topic 'user' to user backfill" --broker rune-1:40200
```

The back-fill job writes every row with its id kept (`insert`, `Acks::All | Acks::View`, an idempotent producer). The broker
moves the counter above the highest imported id, so live inserts that follow with key 0 get fresh ids. Revoke the grant
when the import is done: `revoke import on topic 'user' from user backfill`.

## `rune stats`

```text
rune stats (topic | view) <name> [--partition N] [--json] --broker host:port
```

Prints the counters of every partition of a topic or view, each read from the partition's leader with `GetStats`
([wire-protocol.md](wire-protocol.md#stats-client-version-17)). It needs `DESCRIBE` on the object and a broker at version
`kBrokerVersionCheckedWrites` (10) or above; the connection flags above apply.

| Flag | Meaning |
|---|---|
| `--partition N` | Only partition `N` (default: all). |
| `--json` | One line of JSON instead of text. |
| `--broker host:port` | Any broker (or `RUNE_BROKER`). |

Text: a `partition N` line, then one `<name> <value>` line per reported counter in id order and one `constraint <name> <rejections>`
line per constraint, labelled `<view>.unique` or `<view>.<constraint name>` (`constraint character_name.unique 3`,
`constraint account_stats.max_characters 1`), or `0x%08x` of the constraint id when no view definition names it. A view that was dropped or replaced
still gives its name. `rune stats view <unique view>` also prints `uniqueConflicts`, the number of extra claimants that share a key with its owner. A view's constraint counts are the rejections its source
partition's judge recorded, so `rune stats view` reads the source partition too. A counter the partition does not have is not printed,
and a partition with none prints `(no counters)`.

```text
$ rune stats topic character --broker rune-1:40200
  partition 0
    rejectionsKeyExists 0
    rejectionsKeyNotFound 0
    rejectionsConstraint 0
    verdictLag 0
    verdictWatermark 1048
    verdictMarkersWritten 11
    rejectedNotCompacted 0
    autoIncrementCounter 1049
    autoIncrementReservedThrough 4097
    autoIncrementIdsBurned 0
```

JSON, with the same names and the constraints as `{constraintId, label, rejections}`:

```text
$ rune stats view account_stats --partition 0 --json --broker rune-1:40200
{"view": "account_stats", "partitions": [{"partition": 0, "stats": {"scanRows": 0, "scanPages": 0, "transientRecordsEvicted": 0, "transientSourceResets": 0}, "constraints": []}]}
```

Exit codes: `0` success, `1` an unknown topic or view or a stats read that failed (the first failing partition ends the command with
`stats of partition N: <reason>`, such as `not authorized` or `the partition's leader was not reached`), `2` usage.

## `rune console`

The interactive Rune QL shell, and the way to run a `.rql` script (topic DDL included). It replaces the former
`rune schema apply` and `rune schema alter`.

```text
rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run]
             [--expected-version N] [--request-id <32 hex>] [--verbose]
             [--user <u>] [--password-file <p>] [--tls-ca <pem>] [--tls-host <name>] [--insecure-plaintext]
```

| Flag | Meaning |
|---|---|
| `-f <file\|->` | Run a script (`-` is stdin) instead of the interactive shell. |
| `--dry-run` | Parse and print the plan. No broker needed. |
| `--expected-version N` | The optimistic-concurrency gate for `alter topic` (0..65535, default 0 = none). A stale value is `SchemaConflict`. |
| `--request-id <32 hex>` | Fix the DDL's request id (not all zero). With `-f`, statement `N` uses the id plus `N - 1`. A re-run returns the original results; without it, defining a topic that exists is `TopicExists`. |
| `--verbose` | Print every followed redirect and NotLeader retry to stderr. |

Interactive: the prompt is `rune> ` (`   ...> ` while a statement continues). A statement ends at a `;` outside
comments and strings. Each completed input is parsed, then run in order over one broker session. A failure skips the
rest of that input and keeps the session. `exit`, `quit` or end of input leaves with `0`. The one failure that ends
the session is `SessionRevoked`. The terminal requirement applies only when a user is set (`--user` or `RUNE_USER`)
and no `--password-file` or `RUNE_PASSWORD_FILE` is given: the console then prompts for the password on the terminal,
and refuses to start without one. Without a user (a broker in dev mode) no terminal is needed and a piped session
works. Input from a pipe (`producer | rune console`) is read like a terminal: the console keeps the broker session
alive while it waits for the next line, so a long gap between statements does not lose the session.

With `-f`: every statement is parsed and prepared first, so nothing is issued unless the whole script is valid. The
statements then run in order and the run stops at the first failure. Stderr reads
`<file>:statement N: <CodeName> (<code>) ...`. The exit code is the broker's wire error code, or `1` for a local
error, or `2` for a usage error. For example `TopicExists` is `25`, `SchemaIncompatible` is `30` and `SchemaConflict`
is `31`.

Unlike the removed `rune schema apply`, which printed "already exists" and exited `0`, `-f` treats a `topic` statement
for a topic that already exists as an error: `TopicExists`, exit `25`. A CI script that re-applies the same define file
therefore fails unless it passes `--request-id` (a re-run with the same id returns the original result).

## `rune schema`

Read-only access to topic schemas. Define or change a topic with `rune console -f` (or `rune ql`).

```text
rune schema get <topic> --broker <ep>
rune schema list [--verbose] --broker <ep>
rune schema generate --from-file <blob> --out <dir>
rune schema generate <topic> --out <dir> [--broker <ep>]
```

| Command | What it does |
|---|---|
| `get` | Fetch a topic's schema and print it. |
| `list` | List the topics the broker knows; `--verbose` prints more detail per topic. |
| `generate --from-file` | Generate a C++ header from a serialized descriptor blob, offline. |
| `generate <topic>` | Fetch the topic's schema lineage from the broker and write `<out>/<topic>.gen.h`, a type that reads records of every version up to the current one. |

`generate` refuses a schema with a field whose name ends in `_`: a trailing `_` is reserved for the generated code's own names.

## `rune catalog`

The catalog is the replicated log of every DDL ([CATALOG.md](../../broker/docs/CATALOG.md)). These commands need
`DESCRIBE` on `CLUSTER`.

```text
rune catalog export [--up-to <offset>] --broker host:port > file.rcat
rune catalog replay --from <file.rcat|host:port> --into host:port [--up-to <offset>] [--cluster-id <id>]
rune catalog diff   --from <file.rcat|host:port> --to <file.rcat|host:port>
```

| Command | What it does |
|---|---|
| `export` | Write the catalog's event log as an `.rcat` file on stdout. `--up-to` stops at an offset. |
| `replay` | Re-issue the events of an `.rcat` file (or of another cluster) as DDL against `--into`, recording where each came from. `--cluster-id` names the source (default: the `--from` argument verbatim). |
| `diff` | Compare two catalogs, each a file or a live cluster. Exit `0` equal, `3` differ (printed), `1` error, `2` usage. |

## `rune security`

```text
rune security gen-cluster-key --out <path>
rune security bootstrap --admin <name> --cluster-key-file <path> --broker host:port [--password-file <path>]
```

| Command | What it does |
|---|---|
| `gen-cluster-key` | Write a new cluster key: 64 hex characters at mode `0600`. An existing file is never overwritten. |
| `bootstrap` | Create the first security admin on a fresh cluster, once. It proves possession of the cluster key (`--cluster-key-file`, default `RUNE_CLUSTER_KEY_FILE`). Without `--password-file` it generates a 24-character one-time password and prints it once; the admin must change it at first login. A second run is refused. |

Users, roles and grants are managed with Rune QL (`CREATE USER`, `GRANT`, ... in [ql.md](ql.md#security-statements)).

Quickstart: [wiki, Security Quickstart](https://github.com/arcanumruna/rune/wiki/Security-Quickstart). Reference: [SECURITY.md](../../broker/docs/SECURITY.md).
Every command that connects reaches a UDP broker over TLS; with credentials or for the bootstrap TLS is required.

| Option | What it does |
|---|---|
| `--tls-ca <pem>` (or `RUNE_TLS_CA_FILE`) | Trust this CA bundle instead of the system store. |
| `--tls-host <name>` (or `RUNE_TLS_HOST`) | Certificate name to check. Default: the host of each broker's endpoint. |
| `--insecure-plaintext` | Connect without TLS, for a dev-mode broker. It never sends credentials or the bootstrap: with `--user`, a password file or `security bootstrap` it is refused before connecting. |

```bash
rune security bootstrap --admin admin --cluster-key-file cluster.key --broker rune-1:40200 --tls-ca ca.pem
RUNE_TLS_CA_FILE=ca.pem rune ql --user admin --password-file admin.pw -e "show users;" --broker rune-1:40200
```

## Related tools

| Binary | Source | Purpose |
|---|---|---|
| `rune_schema_gen` | `client-cpp/tools/rune_schema_gen.cpp` | The standalone form of `rune schema generate`: `generate <topic> --out <dir> [--broker <endpoint>]` or `generate --from-file <blob> --out <dir>`. |
| `rune_codec_gen` | `codec/tools/rune_codec_gen_main.cpp` | `rune_codec_gen <descriptor.blob> <out.h>`. |
| `rune_role` | `broker/src/rune_role.cpp` | `rune_role [<metricsFile>]` prints one `ROLE nodeId=... role=LEADER\|FOLLOWER\|CANDIDATE\|NONE term=...` line and one `STORAGE ...` line from the broker's metrics file (default `$RUNE_METRICS_FILE`, else `$RUNE_DATA_DIR/metrics/rune-metrics.dat`, `RUNE_DATA_DIR` defaulting to `/var/lib/rune`). Exit 0 found, 1 file missing, unreadable or not ready (the status is printed on stderr), 2 role metric not present yet. The DR harness uses it. |
| `rune_broker storage format` | `broker/src/main.cpp` | Format a data directory: `--new`, `--join` or `--reassign-join`, `[--data-dir D] [--ignore-formatted]`. See [operations](../operations.md#formatting-a-data-directory). |
