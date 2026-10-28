# Configuration reference

Every broker setting is an environment variable, read once at start by `parseNodeConfigFromEnv()`
(`broker/libs/node/src/NodeConfig.cpp`, defaults in `broker/libs/node/include/rune/node/NodeConfig.h`).
A malformed or out-of-range value stops the broker with `rune-broker: configuration error: ...` and exit code 2.
Unless a row says otherwise, an unset or empty variable keeps the default.

This page is generated from that code. Where [SELF-HOSTING.md](../broker/docs/SELF-HOSTING.md) lists a shorter
table, this page is the complete one: the broker reads every variable below, and no other.

Contents: [required](#required) · [membership and timing](#membership-election-and-timing) ·
[storage](#storage) · [authentication and TLS](#authentication-and-tls) · [Aeron transport](#aeron-transport) ·
[threads, idling and affinity](#threads-idling-and-affinity) · [rings and limits](#rings-and-limits) ·
[cleaner and segments](#cleaner-and-segments) · [diagnostics](#diagnostics-and-logging) ·
[flight recorder](#flight-recorder) · [client](#client-configuration) · [CLI](#cli-environment)

Conventions: *bool* accepts `1`/`true` and `0`/`false` unless the row says `1`/`true`/`TRUE` only (those read any other
value as false). *ring/size* values must be powers of two; any other value keeps the default.

## Required

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_NODE_ID` | required | This node's integer id. It must appear in `RUNE_PEERS`. |
| `RUNE_PEERS` | required | Comma-separated `id@host:consensusPort[:clientPort[:replicationPort[:replBasePort[:ingressBasePort[:egressControlBasePort[:responseControlBasePort[:gatewayResponseControlPort]]]]]]]` for every node including this one. Ids are unique; whitespace is trimmed; an empty or trailing entry is refused. Ports are 1 to 65535; more than eight port segments are refused. The 7th and 8th segments advertise the [response-control ports](#derived-ports) (`0` or absent = derived in shard mode). A single node needs only the consensus port (`0@127.0.0.1:40111`). **A multi-node cluster needs the first three ports** (`id@host:40111:40200:40222`: consensus, client, replication): peers replicate partitions and the `__catalog` over the replication port, and followers redirect clients to the client port. The shipped compose files use exactly that. In shard mode (the default) the per-worker ports are derived from the node id when the later optional segments are absent (see [derived ports](#derived-ports)). `host` is the **advertised** name peers dial. With authentication on: at most 16 peers and node ids up to 65535. |
| `RUNE_CLIENT_ENDPOINT` | required | `host:port` the client gateway listens on (the broker prepends `aeron:udp?endpoint=`). A value that already starts with `aeron:` is used as given, so both forms work. Use `0.0.0.0:40200` to bind every interface. |
| `RUNE_DATA_DIR` | required | Writable directory for node state, the term log, the catalog and security logs, and partition logs. The image entrypoint defaults it to `/var/lib/rune`. |
| `RUNE_CLUSTER_KEY_FILE` | required unless `RUNE_AUTH=disabled` | Shared cluster key file: at least 32 bytes after trailing whitespace is trimmed, not readable by group or others. See [operations](operations.md#the-cluster-key). |

`RUNE_TOPICS` is **removed**: a broker that finds it set refuses to start. Topics are created by DDL
([Rune QL](api/ql.md)).

## Membership, election and timing

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_ELECTION_TIMEOUT_MS` | `150` | Maximum wait per election round (positive ms). Raise it for slow or cross-datacenter links. |
| `RUNE_ELECTION_STATUS_INTERVAL_MS` | `20` | Canvass and heartbeat cadence (positive ms). The leader-heartbeat timeout is `max(election timeout, 3 x this)`. |
| `RUNE_STARTUP_CANVASS_TIMEOUT_MS` | `100` | Extended canvass window on first boot, for peer discovery (positive ms). |
| `RUNE_CONTROLLED_SHUTDOWN_MS` | `5000` | Budget for handing partition leadership to in-sync peers on SIGTERM. `0` = no hand-off. |
| `RUNE_MIN_ISR` | majority (`peers / 2 + 1`) | Minimum in-sync replicas for an `acks=all` produce, and the lower bound for a topic's `replicas`. Integer of at least 1. |
| `RUNE_BROKER_LIVENESS_TIMEOUT_MS` | `0` (3 x the leader-heartbeat timeout) | How long the controller waits without a heartbeat before it fails a node's partitions over. A fault-test knob. |
| `RUNE_SURVIVOR_LEADER_ELECTION` | off | `1`/`true`/`TRUE` enables the position-proven survivor election for a partition whose committed ISR has no live member. Off by default, as Kafka's `unclean.leader.election.enable`. |
| `RUNE_UNCLEAN_CONSENSUS_ELECTION` | off | `1`/`true`/`TRUE` removes the candidacy gate of the consensus election: a node missing committed metadata may win, and what it lacks is lost. Recovery only; turn it off again. |
| `RUNE_CLIENT_STREAM_ID` | `1` | Aeron stream id for client traffic. |
| `RUNE_CONSENSUS_STREAM_ID` | `10` | Aeron stream id for consensus traffic. |
| `RUNE_REPLICATION_STREAM_ID` | `20` | Aeron stream id for the shared replication plane. |
| `RUNE_REPL_BASE_STREAM_ID` | `2000` | Base stream id of the per-worker replication planes (worker `w` uses `base + w`). |
| `RUNE_CATALOG_REPL_STREAM_ID` | `30` | Stream id of the `__catalog` replication plane. Must differ from the replication stream and lie outside the per-worker range. Keep it equal on every node. |
| `RUNE_PEER_AUTH_STREAM_ID` | `40` | Stream id of the inter-broker authentication handshake (positive). |
| `RUNE_WORKER_INGRESS_BASE_STREAM` | `RUNE_CLIENT_STREAM_ID + 1` | Base stream id of the per-worker ingress streams. |
| `RUNE_WORKER_EGRESS_BASE_STREAM` | ingress base + worker count | Base stream id of the per-worker egress streams. |
| `RUNE_WORKER_INGRESS_BASE_PORT` | derived per node id | Base UDP port of the per-worker ingress endpoints (1 to 65535). |
| `RUNE_WORKER_REPL_BASE_PORT` | derived per node id | Base UDP port of the per-worker replication endpoints (1 to 65535). |
| `RUNE_WORKER_EGRESS_CONTROL_BASE_PORT` | none (legacy mode binds `40300`; shard mode derives it) | Base UDP control port of the per-worker dynamic-MDC egress. |
| `RUNE_RESPONSE_CONTROL_BASE_PORT` | derived (see below) | Base UDP port of the per-worker response-control endpoints (worker `w` binds base + `w`); 7th `RUNE_PEERS` segment. Unset and not derivable = new clients use the declared egress path. |
| `RUNE_GATEWAY_RESPONSE_CONTROL_PORT` | derived (see below) | UDP port of the gateway's response-control endpoint; 8th `RUNE_PEERS` segment. |
| `RUNE_WORKER_EGRESS_CONTROL_HOST` | derived | Host bound for the egress control port. An explicit loopback host on a node advertised at a non-loopback host refuses to start. |
| `RUNE_PEER_BIND_HOST` | derived | Address the node binds its peer-facing sockets on. Default: the advertised host when it is numeric, else `0.0.0.0`. |

### Derived ports

When a deployment sets no per-worker ports, shard mode derives them from the node id (`HostDefaults.h`): node `n` gets the
block `8200 + 192 n` to `8200 + 192 n + 191`, with the per-worker ingress ports at the base, the replication ports at
`base + 64` and the egress control ports at `base + 128` (each block holds up to 64 workers, so co-located nodes never
collide). This works for node ids 0 to 126 and at most 64 workers; beyond that, set the ports explicitly in `RUNE_PEERS` or with
`RUNE_WORKER_INGRESS_BASE_PORT`, `RUNE_WORKER_REPL_BASE_PORT` and `RUNE_WORKER_EGRESS_CONTROL_BASE_PORT`. The ports fall below
every default ephemeral range, so an auto-bound socket cannot take them. Publish them with the container (all UDP) when
clients or peers reach the node through Docker port mappings.

The response-control ports (one for the gateway, one per worker) sit in the unused top of the node's egress-control block,
so there is no extra range: the gateway binds `egress + 63` and worker `w` binds `egress + 63 - W + w` (`egress` = the block's
egress control base, `W` = `RUNE_WORKERS`). They are derived only for 1 to 31 workers and only when the egress control base is
the derived one. With an explicit `RUNE_WORKER_EGRESS_CONTROL_BASE_PORT` (or more than 31 workers) none are derived and the node
logs a warning: set `RUNE_RESPONSE_CONTROL_BASE_PORT` and `RUNE_GATEWAY_RESPONSE_CONTROL_PORT` and advertise them in
`RUNE_PEERS` (7th and 8th segments). They bind on the egress control host.

## Storage

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_STORAGE_FORMAT` | unset (never format) | `new`, `join` or `auto`. Read only when the data directory is empty and not formatted. Unset: the broker refuses to start (exit 78) and prints the command. `auto` is development only. See [SELF-HOSTING](../broker/docs/SELF-HOSTING.md#formatting-the-data-directory). |
| `RUNE_STORAGE_RECOVERY_VOTE` | off | `1`/`true`/`TRUE`. Unsafe, disaster recovery only: lets an unconfirmed (replaced-disk) node vote. Logged at critical level. |
| `RUNE_AERON_DIR` | `<data dir>/aeron` | Aeron directory (CnC file and mapped term buffers). Put it on tmpfs (`/dev/shm`) for performance. |
| `RUNE_METRICS_FILE` | `<data dir>/metrics/rune-metrics.dat` | The broker's metrics file (arcanum-metrics format), created afresh at every start; the broker refuses to start if it cannot create it. Read it with `rune_role`. Metrics: [operations](operations.md#metrics). |
| `RUNE_METRICS_SYSTEM_INTERVAL_MS` | `1000` | How often the control agent samples `/proc/self` into the process metrics (positive ms). |
| `RUNE_IO_BACKEND` | `uring` when available, else `portable` | `uring` (Linux io_uring) or `portable`. An unset value falls back to `portable` when io_uring cannot start (old kernel, seccomp, Docker Desktop on Apple Silicon). An explicit `uring` fails loudly. |
| `RUNE_TRANSIENT_LOG_RING_BYTES` | `8388608` (8 MiB) | Node-wide ring size of every transient partition (power of two). `RUNE_MEMORY_LOG_RING_BYTES` is a deprecated alias that logs one warning; the canonical name wins when both are set. |
| `RUNE_PRODUCER_ID_EXPIRATION_MS` | `604800000` (7 days) | How long an idle producer id is remembered for idempotence (0 or more). |
| `RUNE_WORKER_SYNC_INTERVAL_MS` | `0` | Periodic fsync of the worker's logs; `0` = never (0 to 2 592 000 000). |
| `RUNE_FOLLOWER_FLUSH_INTERVAL_MS` | `0` | Periodic flush of follower copies; `0` = never (0 to 2 592 000 000). |
| `RUNE_ENABLE_COALESCING` | `true` | `0` or `false` (any case) turns compaction coalescing off. |
| `RUNE_CLEANER_IO_MAX_BYTES_PER_SECOND` | unlimited | Cleaner I/O throttle (positive bytes per second). |

## Authentication and TLS

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_AUTH` | `enabled` | `enabled` or `disabled`. `disabled` is dev mode: no login, every request runs as `anonymous` with full access. A broker whose `RUNE_PEERS` lists more than one node refuses to start in dev mode. |
| `RUNE_CLUSTER_KEY_FILE_NEXT` | unset | Second key for [rotation](operations.md#rotating-the-cluster-key): the broker accepts the current or the next key. |
| `RUNE_AUTH_TIMEOUT_MS` | `10000` | A session that has not finished its login within this is answered `AuthenticationFailed` and ended (positive ms). |
| `RUNE_ANONYMOUS_MAX_CONNECTIONS` | `16` | The most anonymous-reader sessions one gateway holds (`RUNE_ANONYMOUS_READ` on). The next session's first request is answered `NamespaceLimitExceeded`. Integer in [1, 1000000]; applied before any namespace limit. |
| `RUNE_ANONYMOUS_REQUEST_OPS_PER_SEC` | `1000` | One token bucket (burst = one second) shared by every anonymous-reader session on a gateway; a request over it is answered `Throttled` with the retry-after. Integer in [1, 1000000]. |
| `RUNE_NS_LIMIT_TICK_US` | `10000` | Period of the per-namespace limit aggregator (microseconds): every tick it compares each namespace's produce / fetch / request / query rates with its limits and broadcasts a throttle deadline on change only; the workers report usage at the same period. A smaller tick reacts faster and costs more control-thread work. Integer in [1000, 1000000]. |
| `RUNE_SECURITY_MAX_STALENESS_MS` | `1000` | F: a broker whose `__security` leader (or the consensus) has been silent this long answers logins, `__security` reads and security DDL `AuthUnavailable` (48) until it is current again. Must exceed the broker liveness timeout + 500 ms (an explicit smaller value is refused; the default is raised to it). A hard leader failure does not make a current in-ISR follower stale (#460). |
| `RUNE_SECURITY_STALE_SESSION_END_MS` | `10000` | R (> F): a broker stale this long ends its authenticated sessions with a session-level `AuthUnavailable`, so their clients reconnect to a current broker. |
| `RUNE_SECURITY_GATE_DEADLINE_MS` | `5000` | A security DDL OK whose enforcement on this broker is not confirmed within this is answered `Internal` "committed at offset N; enforcement not confirmed" (retry with the same request id). |
| `RUNE_TLS_CERT_FILE` | unset | PEM certificate file, reloaded on SIGHUP. **Required with authentication**: every client session of an authenticating broker is TLS, and a plaintext Connect is answered `HandshakeFailed` (29). |
| `RUNE_TLS_KEY_FILE` | unset | PEM private key file. Same conditions. |
| `RUNE_REQUIRE_TLS` | on with authentication | `true`/`1`: every client session must be TLS (always so with authentication). `false`/`0` is accepted only in dev mode (`RUNE_AUTH=disabled`); with authentication the broker refuses to start. Any other value is refused. |

The TLS variables need a broker built with `-DRUNE_TLS=ON` (the default). A build without it refuses to start with
authentication enabled, and with any of these variables set, instead of starting plaintext.

## Aeron transport

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_AERON_TERM_BUFFER_BYTES` | `1048576` (1 MiB) | Term buffer length of the embedded driver (power of two). Every publication maps three terms. |
| `RUNE_AERON_CONTROL_TERM_BYTES` | `1048576` | Term length of the control-plane publications (power of two, at least 65536). |
| `RUNE_AERON_MTU` | derived from the host | MTU in bytes, `33` to `65504`. Clamped to a 32-byte multiple and to the socket buffer at start. |
| `RUNE_AERON_RCV_WINDOW` | derived (Aeron's is 128 KiB) | Receive window (power of two). It must not exceed the socket buffer or half a term. |
| `RUNE_AERON_SOCKET_BUFFER_BYTES` | derived | `SO_SNDBUF` / `SO_RCVBUF` in bytes; `0` leaves Aeron's default. The kernel clamps it to `net.core.rmem_max` / `wmem_max`. |
| `RUNE_AERON_MAX_MSGS_PER_SEND` | `0` (driver default) | Messages per send call, `1` to `16`. |
| `RUNE_AERON_SENDER_IOV` | `0` (driver default) | Sender iovec capacity, `1` to `16`. |
| `RUNE_AERON_RECEIVER_IOV` | `0` (driver default) | Receiver iovec capacity, `1` to `16`. |
| `RUNE_AERON_UDP_GSO` | `true` | UDP segmentation offload, `0`/`1`/`false`/`true`. Linux only; falls back when the kernel lacks it. |
| `RUNE_AERON_EPOLL_POLL` | `true` | epoll-driven receive poll, `0`/`1`/`false`/`true`. Linux only. |
| `RUNE_AERON_TERM_SPARSE` | derived | Sparse term files, `true`/`false`. |
| `RUNE_AERON_THREADING_MODE` | `shared` | `shared` or `dedicated` (case-insensitive); anything else reads as `shared`. |
| `RUNE_AERON_DRIVER_IDLE` | derived (`backoff` in shard mode) | Idle strategy of the node's driver, in Aeron syntax. |
| `RUNE_AERON_DRIVER_IDLE_ARGS` | derived | Init arguments of that idle strategy; refused at driver start when malformed. |
| `RUNE_GATEWAY_FRAGMENT_LIMIT` | `256` | Fragments the gateway polls per duty cycle. A value that does not parse keeps the default. |
| `RUNE_REPL_FRAGMENT_LIMIT` | `256` | Fragments the replication plane polls per cycle (positive). |
| `RUNE_CONSENSUS_FRAGMENT_LIMIT` | `64` | Fragments the consensus plane polls per cycle (positive). |
| `RUNE_MAX_PRODUCE_BATCH_BYTES` | derived from the transport | Explicit ceiling on an accepted produce batch (positive). A larger batch is answered `MessageTooLarge`. By default the ceiling is the smallest one-frame capacity along the path (about 512 KiB with default rings), never above 8 MiB. |
| `RUNE_GATEWAY_PRODUCE_CREDITS` | `0` (the gateway default, 1024) | Per-session budget of gateway-routed produces whose acks are not yet claimed, `1` to `4096`. At the budget the gateway pauses that client. |
| `RUNE_MAX_HELD_IDLE_MS` | `0` (the gateway default, 10 minutes) | Reaps a back-pressure-paused session that makes no progress for this long, `1` to `86400000` ms. |

## Threads, idling and affinity

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_WORKER_MODE` | `shard` | `shard` or `legacy`. Shard mode (self-contained workers, each with its own ingress, egress and replication) is the default. `legacy` is refused when authentication is on and `RUNE_PEERS` lists more than one broker. |
| `RUNE_RUNTIME_PLACEMENT` | `fused` | `fused` or `thread`: whether the runtime agent shares the worker thread or has its own. |
| `RUNE_WORKERS` | `1` | In-process workers per node (at least 1; a bad value reads as 1). With authentication on, at most 252. |
| `RUNE_WORKER_CORE_BASE` | derived | First CPU of the worker pinning (`-1` = no pinning, `>= 0` = worker `i` on core `base + i`). Unset: Linux shard mode pins to cores `1 + i * W` when the box has them. Unparseable reads as `-1`. |
| `RUNE_IOWQ_CORES` | none | CPU list (`0,4-5`) for the io_uring kernel worker threads. |
| `RUNE_NONWORKER_CORES` | none | CPU list for every non-worker broker thread. |
| `RUNE_WORKER_IDLE` | `backoff` | Worker idle strategy: `backoff`, `busyspin`, `yielding`, `sleeping` (case-insensitive; unknown reads as `backoff`). In shard mode only `busyspin` differs: `yielding` and `sleeping` act as `backoff`. `busyspin` pegs a core. |
| `RUNE_RUNTIME_IDLE` | `backoff` | Idle strategy of the runtime and gateway thread; same values. |
| `RUNE_IDLE_MAX_PARK_US` | `200` | Longest park of the backoff idle strategy, `1` to `1000000` us. Above 10 ms logs a warning. |
| `RUNE_CONTROL_IDLE_MAX_PARK_US` | `2000` | Park cap of the shard-mode control plane, `1` to `1000000` us; must not exceed a quarter of `RUNE_ELECTION_STATUS_INTERVAL_MS`. |
| `RUNE_IDLE_WAKE` | `true` | Shard workers park until work arrives instead of polling, `0`/`1`/`false`/`true`. |
| `RUNE_IDLE_SPINS` | auto (Aeron's 10) | Spins before yielding (non-negative). |
| `RUNE_IDLE_YIELDS` | auto (Aeron's 20) | Yields before parking (non-negative). |
| `RUNE_IDLE_DOORBELL` | `true` | Doorbell wake of parked workers, `0`/`1`/`false`/`true`. |
| `RUNE_IDLE_DOORBELL_MIN_PARK_US` | `1000` | Doorbell minimum park, `1` to `1000000` us; must not exceed the next row. |
| `RUNE_IDLE_DOORBELL_PARK_US` | `1000` | Doorbell park, `1` to `1000000` us. |
| `RUNE_FETCH_INTERVAL_US` | `5000` | Follower periodic-fetch backstop cadence (positive us). |
| `RUNE_PER_WORKER_INGRESS` | `false` | `1`/`true`/`TRUE`. Forced on by shard mode. |
| `RUNE_PER_WORKER_EGRESS` | `false` | Same. Forced on by shard mode. |
| `RUNE_PER_WORKER_REPLICATION` | `false` | Same. Forced on by shard mode. |

## Rings and limits

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_WORKER_INGRESS_RING_BYTES` | `33554432` (32 MiB) | Per-worker ingress ring (power of two). |
| `RUNE_WORKER_RETURN_RING_BYTES` | `33554432` | Per-worker return ring (power of two). |
| `RUNE_REPL_OUTBOUND_RING_BYTES` | `33554432` | Outbound replication ring (power of two). |
| `RUNE_IO_SUBMIT_RING_DEPTH` | `1024` | Slots of each worker's I/O submit ring (power of two). |
| `RUNE_IO_COMPLETION_RING_DEPTH` | `1024` | Slots of each worker's I/O completion ring (power of two). |
| `RUNE_WORKER_INGRESS_DRAIN_LIMIT` | `1024` | Ingress entries a worker drains per pass (positive). |
| `RUNE_WORKER_RETURN_DRAIN_LIMIT` | `1024` | Return entries drained per pass (positive). |
| `RUNE_FILTER_MAX_SUBS_PER_PARTITION` | `4096` | Filtered subscriptions per topic partition, `1` to `1048576`. |
| `RUNE_FILTER_MAX_INDEX_VALUES_PER_PARTITION` | `4096` | Distinct filter index values per topic partition, `1` to `1048576`. |
| `RUNE_FILTER_NOTE_INTERVAL_MS` | `50` | Milliseconds before a filtered subscription that skipped records gets a position note, `1` to `60000`. |
| `RUNE_FILTER_NOTE_RECORDS` | `65536` | Skipped records before a position note, `1` to `2147483647`. |
| `RUNE_FILTER_VERSION_WAIT_MS` | `5000` | Milliseconds a filter waits for a schema version it has not seen, `0` to `600000`. |
| `RUNE_FILTER_MATCH_SCRATCH` | `65536` | Per-worker match scratch, in matches, `64` to `16777216`. |
| `RUNE_WORKER_ACK_SLOT_BYTES` | `256` | Return-ring slot for a produce result (64 or more). |
| `RUNE_WORKER_MAX_OWED_RETURNS` | `4096` | Cap on owed returns, `256` to `8192` (enforced at start). |

An accepted produce batch is limited by every hop's one-frame capacity: about `RUNE_WORKER_INGRESS_RING_BYTES / 8` and,
in a cluster, `RUNE_REPL_OUTBOUND_RING_BYTES / 8` and `min(term / 8, 16 MiB)`. Larger batches are answered
`MessageTooLarge`; they never refuse startup. Keep the client's `min(RUNE_BATCH_BYTES, RUNE_MAX_FRAME_BYTES)` below it.

## Cleaner and segments

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_SEGMENT_PROVISION_DEPTH` | `1` | Pre-created segments per partition, clamped to `1` to `8`. |
| `RUNE_SEGMENT_PROVISION_THRESHOLD_PCT` | `50` | Fill percentage of the active segment that triggers provisioning, clamped to `1` to `100`. |
| `RUNE_SEGMENT_PROVISION_CAP_BYTES` | `-1` (no cap) | Cap on bytes of pre-created segments; negative = none. |
| `RUNE_SEGMENT_PROVISION_CAP_FREE_PCT` | `10` | Cap as a percentage of free disk, clamped to `1` to `100`. |
| `RUNE_SEGMENT_PROVISIONER_MAX_PARK_US` | `10000` | Longest park of the provisioner (1 or more). |

Per-topic retention, compaction and segment settings are **not** environment variables: they are `with (...)`
settings of the `topic` statement ([Rune QL](api/ql.md#topics)).

## Diagnostics and logging

| Variable | Default | Meaning |
|---|---|---|
| `RUNE_LOG_LEVEL` | `info` | spdlog level name: `trace`, `debug`, `info`, `warn`, `error`, `critical`, `off`. |
| `RUNE_FS_LOG` | off | Any value other than empty or `0` turns on per-tick follower-sync and transport-rate logging. It floods logs; for replication debugging only. |
| `RUNE_CTRL_LOG` | off | Set (any value) to log control-plane decisions. |

## Flight recorder

The recorder is compiled into the broker and runtime-disabled until `FLIGHT_RECORDER_ENABLE` is set. The names are
`FLIGHT_RECORDER_*`; the older `RUNE_FR_*` spellings are gone. Full reference:
[operations](operations.md#flight-recorder) and the
[flight-recorder repository](https://github.com/arcanumruna/flight-recorder).

| Variable | Default | Meaning |
|---|---|---|
| `FLIGHT_RECORDER_ENABLE` | off | Any non-empty value other than `0` arms recording. The broker logs `flight recording ENABLED`. |
| `FLIGHT_RECORDER_RING_BYTES` | `134217728` (128 MiB) per thread | Size of each thread's ring. Read before any thread registers. |
| `FLIGHT_RECORDER_OUT` | current directory | Directory the broker writes `fr-hist-*.txt`, `fr-trace-*.json` and `fr-tape-*.bin` to, on SIGUSR1 and at shutdown. Files carry `-n<node id>-p<pid>-<tag>` in their names. |
| `FLIGHT_RECORDER_CATEGORIES` | all | Comma list from `produce`, `replication`, `read`, `consensus`, `storage`, `egress`, `runtime`. Read by `configureFromEnv()`. |
| `FLIGHT_RECORDER_SLOW_SPAN_NS` | `2000` | Slow-span threshold. Read by `configureFromEnv()`. |
| `FLIGHT_RECORDER_THROTTLE_NS` | `50000` | Event throttle. Read by `configureFromEnv()`. |
| `FLIGHT_RECORDER_DRAIN_MS` | `100` | Background drain period; `0` = no background drain. Read by `configureFromEnv()`. |
| `FLIGHT_RECORDER_TAPE_RECORDS_PER_THREAD` | library default | Cap on the records written to a tape per thread. |
| `FLIGHT_RECORDER_AERON_MASK` | library default | Bit mask selecting the C-API Aeron sites. |

`rune_broker` calls `Recorder::configure()` and `enable()` itself (it reads `FLIGHT_RECORDER_RING_BYTES`,
`FLIGHT_RECORDER_ENABLE` and `FLIGHT_RECORDER_OUT`); the variables marked "Read by `configureFromEnv()`" apply to a process
that calls `flight_recorder::configureFromEnv()`, such as the client benchmark harness (`client-cpp/examples/bench/FrRun.h`).
Send `SIGUSR1` to a running broker to dump a report; `SIGHUP` reloads the TLS certificate.

## Client configuration

`rune::client::ClientConfig` ([Client.h](../client-cpp/include/rune/client/Client.h)); the API reference is
[api/client-cpp.md](api/client-cpp.md).

| Field | Default | Meaning |
|---|---|---|
| `gatewayChannel` | `aeron:ipc` | Seed broker's client endpoint, e.g. `aeron:udp?endpoint=node1:40200`. |
| `gatewayStreamId` | `1` | Broker's client stream id (`RUNE_CLIENT_STREAM_ID`). |
| `egressChannel` | `aeron:ipc` | Where replies arrive, e.g. `aeron:udp?endpoint=me:40400`. |
| `egressStreamId` | `2` | Stream id of the reply channel. |
| `dataDir` | empty | Directory of the embedded driver. |
| `connectTimeoutMs` / `pingTimeoutMs` / `produceTimeoutMs` | `5000` | Deadlines of connect, ping (also `getViewRow`) and a produce ack. |
| `subscribeTimeoutMs` | `10000` | Wait for a `SubscribeResponse`. |
| `ddlTimeoutMs` | `15000` | Deadline of every DDL, redirects and retries included. |
| `ddlRetryBudget` | `10 s` | How long a `NotLeader` DDL is retried (100 ms backoff doubling to 1 s). |
| `closeTimeoutMs` | `65000` | Deadline of `close()`'s drain. |
| `termBufferBytes` | `4 MiB` | Embedded driver term length (power of two, 64 KiB to 1 GiB). |
| `mtuBytes` | `1408` | Aeron MTU. `8192` only for paths that carry jumbo datagrams. |
| `socketBufferBytes` | `2 MiB` | `SO_SNDBUF` and `SO_RCVBUF`; `0` = OS default; at least Aeron's 128 KiB window. |
| `idleMode` | `Balanced` | `Balanced`, `LowLatency` (yielding) or `BusySpin` (pegs a core). |
| `runnerMode` | `Thread` | `Thread` or `Invoker` (no threads; you call `doWork()`). |
| `runnerCore` | `-1` | Thread mode: pin the runner to this CPU. |
| `driverOnDutyThread` / `reconnectForeverAfterLive` | `false` | Shard-client behaviour; see the header. |
| `offerCrypto` | `false` | Offer TLS 1.3 on connect. **Required with credentials** (`username`, `password`, `passwordFile`, `securityBootstrapOnly`): construction throws without it. |
| `requireCrypto` | `false`; on with credentials | Never proceed without TLS (needs `offerCrypto`). An answer that arrives before the session has its keys is unauthenticated, so a refusal is retried (then `Backoff` / `Disconnected`, `lastConnectError` 29), never a terminal `Rejected`. |
| `tlsTrustPem` | system store | PEM CA bundle to trust instead; `""` trusts nothing. |
| `tlsExpectHost` | empty | Certificate name to check (DNS name or IP). Empty: each session checks the host of its own endpoint; a redirect answered before TLS never changes the name. The system store with no name to check (an `aeron:ipc` gateway) throws at construction. |
| `insecurePlaintextAuth` | `false` | Tests only: allow credentials over a plaintext session. |
| `username` | empty | Principal to log in as. |
| `password` / `passwordFile` | empty | Password, or a file holding it (read once; trailing newline stripped). Setting both throws. |
| `allowUnauthenticatedBroker` | `false` | Accept a dev-mode broker even with credentials. |
| `minPasswordLength` | `12` | Minimum length when this client builds a new verifier. |
| `scramIterations` | `4096` | Iteration count of a new verifier. Must be 4096..`maxAcceptedScramIterations`, else construction throws. |
| `maxAcceptedScramIterations` | `100000` | Most PBKDF2 iterations a login runs; a broker asking for more is refused before hashing. Set it to at least the cluster's highest verifier count. Range 4096..1000000. |
| `clientName` | `rune-client` | Recorded as the principal's client name in catalog events. |
| `traceDdlRetries` | `false` | Record followed redirects and retries (Invoker mode only). |

`ProducerConfig` ([Producer.h](../client-cpp/include/rune/client/producer/Producer.h)): `acks` (`Leader`), `lingerUs`
(`5000`), `batchBytes` (`65536`), `batchRecords` (`0` = off), `maxInFlight` (`32`, clamped to `32` for acked
producers), `produceTimeoutMs` (`5000`), `transactionOpTimeoutMs` (`60000`), `maxFrameBytes` (`65536`),
`deliveryTimeoutMs` (`30000`), `agentLivenessTimeoutMs` (`1000`), `directDeferredBytes` (`262144`),
`directDeferredPartitionBytes` (`0`). `ProducerConfig::fromEnv()` reads `RUNE_LINGER_US`, `RUNE_BATCH_BYTES`,
`RUNE_BATCH_RECORDS`, `RUNE_MAX_FRAME_BYTES` and `RUNE_DELIVERY_TIMEOUT_MS`.

`GroupConfig` ([GroupConsumer.h](../client-cpp/include/rune/client/group/GroupConsumer.h)): `autoCommit` (`true`),
`autoCommitIntervalMs` (`5000`), `sessionTimeoutMs` (`30000`), `isolation`
(`ReadUncommitted`), `commitSyncTimeoutMs` (`30000`).

Client environment variables that override `ClientConfig`: `RUNE_CLIENT_AERON_MTU`, `RUNE_CLIENT_AERON_UDP_GSO`,
`RUNE_CLIENT_AERON_EPOLL_POLL`, `RUNE_CLIENT_AERON_MAX_MSGS_PER_SEND`, `RUNE_AERON_SOCKET_BUFFER_BYTES` and
`RUNE_CLIENT_AERON_DRIVER_IDLE`. A malformed value is logged once at construction and not applied.

## CLI environment

| Variable | Meaning |
|---|---|
| `RUNE_USER` | Default for `--user`. |
| `RUNE_PASSWORD_FILE` | Default for `--password-file`. |
| `RUNE_CLUSTER_KEY_FILE` | Default for `rune security bootstrap --cluster-key-file`. |
| `RUNE_EGRESS` | Egress channel when `--cli-egress` is not given. |

A password is never read from the command line or the environment: `--password` and `RUNE_PASSWORD` are refused.
