# The `rune` command line

One executable, four command groups: `rune schema`, `rune ql`, `rune catalog` and `rune security`. It is built from
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
| `--cli-data-dir <dir>` | | Scratch directory of the embedded driver. Default: a private temporary directory, removed on exit. |

A password is **never** accepted on the command line or from the environment: `--password` and `RUNE_PASSWORD` are
refused with an explanation. Against a broker in dev mode (`RUNE_AUTH=disabled`) the CLI connects without credentials and
prints a banner.

Exit codes: `0` success, `1` error, `2` usage error, and `3` for `rune catalog diff` when the two sides differ.

## `rune ql`

```text
rune ql (-e "<statement>" | -f <file|->) [--dry-run] [--verbose] [--request-id <32 hex digits>] --broker host:port
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

## `rune schema`

Topic schemas, directly against the registry.

```text
rune schema apply <file|-> [--dry-run] [--validate] [--broker <ep>]
rune schema alter <file|-> [--dry-run] [--expected-version N] [--broker <ep>]
rune schema get <topic> --broker <ep>
rune schema list [--verbose] --broker <ep>
rune schema generate --from-file <blob> --out <dir>
rune schema generate <topic> --out <dir> [--broker <ep>]
```

| Command | What it does |
|---|---|
| `apply` | Parse a `.rql` file holding a `topic` or an `alter topic` statement. `--dry-run` prints the descriptor or the alter plan. Without it, define (or alter) the topic on the broker. `--validate` makes the broker validate every produced record against the schema. |
| `alter` | Parse an `alter topic` file. `--expected-version N` is the optimistic-concurrency gate (0..65535, default 0 = none); a stale value is `SchemaConflict`. |
| `get` | Fetch a topic's schema and print it. |
| `list` | List the topics the broker knows; `--verbose` prints more detail per topic. |
| `generate --from-file` | Generate a C++ header from a serialized descriptor blob, offline. |
| `generate <topic>` | Fetch the topic's schema from the broker and write `<out>/<topic>.gen.h`. |

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

Quickstart: [README, Security](../../README.md#security-quickstart). Reference: [SECURITY.md](../../broker/docs/SECURITY.md).
The CLI has no TLS support yet (tracked in SECURITY.md), so it needs a plaintext listener; a broker with
`RUNE_REQUIRE_TLS=true` refuses it.

## Related tools

| Binary | Source | Purpose |
|---|---|---|
| `rune_schema_gen` | `client-cpp/tools/rune_schema_gen.cpp` | The standalone form of `rune schema generate`: `generate <topic> --out <dir> [--broker <endpoint>]` or `generate --from-file <blob> --out <dir>`. |
| `rune_codec_gen` | `codec/tools/rune_codec_gen_main.cpp` | `rune_codec_gen <descriptor.blob> <out.h>`. |
| `rune_role` | `broker/src/rune_role.cpp` | `rune_role [<aeronDir>]` prints one `ROLE nodeId=... role=LEADER\|FOLLOWER\|CANDIDATE\|NONE term=...` line and one `STORAGE ...` line from the running broker's Aeron counters (default directory `$RUNE_DATA_DIR/aeron`). Exit 0 found, 1 no CnC file, 2 counter not present yet. The DR harness uses it. |
| `rune_broker storage format` | `broker/src/main.cpp` | Format a data directory: `--new`, `--join` or `--reassign-join`, `[--data-dir D] [--ignore-formatted]`. See [operations](../operations.md#formatting-a-data-directory). |
