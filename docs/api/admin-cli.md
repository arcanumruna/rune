# The `rune` command line

One executable, five command groups: `rune console`, `rune schema`, `rune ql`, `rune catalog` and `rune security`. It is built from
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
        [--validate] [--expected-version N] --broker host:port
```

Executes Rune QL: DDL (topics, drops, security) and reads (`SHOW HISTORY`, `SHOW ... AS OF`, `DIFF`, `SHOW USERS`, ...).
The statement text is recorded in the catalog. The grammar and examples are in [ql.md](ql.md).

| Flag | Meaning |
|---|---|
| `-e "<statement>"` | One statement. A statement carrying a password literal is refused here. |
| `-f <file\|->` | A script (`-` is stdin). Runs every statement in order and stops at the first error, naming it (`statement N: ...`). Nothing is issued unless every statement parses. |
| `--dry-run` | Parse and print the plan. No broker needed. |
| `--verbose` | Print every followed redirect and NotLeader retry to stderr. |
| `--request-id <32 hex>` | Fix the DDL's request id (not all zero). With `-f`, statement `N` uses the id plus `N - 1`. A re-run returns the original results. |
| `--validate` | Make the broker validate every produced record against the schema of each `topic` the input defines. |
| `--expected-version N` | The optimistic-concurrency gate for `alter topic` (0..65535, default 0 = none). A stale value is `SchemaConflict`. |

`rune ql` exits `1` on any failure; it does not map a broker refusal to its wire code (`rune console -f` does).

## `rune console`

The interactive Rune QL shell, and the way to run a `.rql` script (topic DDL included). It replaces the former
`rune schema apply` and `rune schema alter`.

```text
rune console [--broker <ep>] [--cli-egress <ch>] [-f <script.rql|->] [--dry-run]
             [--validate] [--expected-version N] [--request-id <32 hex>] [--verbose]
             [--user <u>] [--password-file <p>] [--tls-ca <pem>] [--tls-host <name>] [--insecure-plaintext]
```

| Flag | Meaning |
|---|---|
| `-f <file\|->` | Run a script (`-` is stdin) instead of the interactive shell. |
| `--dry-run` | Parse and print the plan. No broker needed. |
| `--validate` | Make the broker validate every produced record against the schema of each `topic` the input defines. |
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
