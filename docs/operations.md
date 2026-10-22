# Operations

How to deploy a Rune cluster and keep it healthy. This page is the map; the long reference stays where it is and is
linked, not copied:

| Document | Covers |
|---|---|
| [SELF-HOSTING.md](../broker/docs/SELF-HOSTING.md) | Single node, three nodes, multi-host, storage formatting, topics by DDL, transient topics, open-file limit. |
| [SECURITY.md](../broker/docs/SECURITY.md) | Users, roles, grants, the cluster key, rotation, throttle, revocation, metrics, dev mode. |
| [CATALOG.md](../broker/docs/CATALOG.md) | The catalog event log, history reads, export and replay, the halted-apply runbook. |
| [docker/README.md](../broker/docker/README.md) | The broker image, the compose files, the e2e launchers. |
| [dr-e2e/DR_SUITE.md](../broker/dr-e2e/DR_SUITE.md) | The disaster-recovery suite: faults, invariants, the durability contract. |
| [configuration.md](configuration.md) | Every environment variable. |

## Deploy

Each node runs an **embedded Aeron media driver in the broker process**; there is no separate driver to launch. A node needs
a sized `/dev/shm`, a writable data directory, a peer list and the cluster key. All inter-node and client traffic is
`aeron:udp`.

1. Build the image ([wiki, Docker Images](https://github.com/arcanumruna/rune/wiki/Docker-Images)) or pull your own.
2. Make one cluster key and give every node the same file: `rune security gen-cluster-key --out cluster.key`.
3. Format each node's data directory once ([below](#formatting-a-data-directory)).
4. Start the nodes with `RUNE_NODE_ID`, the same `RUNE_PEERS` everywhere, `RUNE_CLIENT_ENDPOINT`, `RUNE_DATA_DIR` and
   `RUNE_CLUSTER_KEY_FILE`.
5. Bootstrap the first admin, once per cluster, and change its password.
6. Create topics with Rune QL.

```bash
rune security bootstrap --admin admin --cluster-key-file cluster.key --broker rune-1:40200
rune ql --user admin --password-file otp.txt -f change-password.rql --broker rune-1:40200
rune ql --user admin --password-file admin.pw -f topics.rql --broker rune-1:40200
```

Container requirements: `--shm-size=256m` (the default 64 MB is too small), the data directory on a volume, the client UDP
port (and in a multi-host cluster the consensus, replication and per-worker ports) published, and a file-descriptor hard
limit that covers the partitions (the broker raises its soft limit to the hard limit and refuses to start when the hard
limit is below the estimated need; see SELF-HOSTING, "Open-file limit").

Run an **odd** number of nodes: one for development, three or more for availability. A two-node cluster tolerates no
failure. Membership is static: the peer list is fixed at boot.

OS settings that matter for UDP throughput (`net.core.rmem_max` and `wmem_max` cap `RUNE_AERON_SOCKET_BUFFER_BYTES`; the
Aeron receive window must not exceed the socket buffer or half a term) are covered in
[configuration.md](configuration.md#aeron-transport).

### Formatting a data directory

A broker **never formats an empty data directory on its own**. It refuses to start (exit 78) and prints the command:

| Situation | Command |
|---|---|
| New cluster, every founding node | `rune_broker storage format --new` |
| Adding a node, or replacing a lost disk, in a running cluster | `rune_broker storage format --join` |
| After restoring a whole directory from a backup on a multi-node cluster | `rune_broker storage format --reassign-join --data-dir <D>` |
| A disposable development sandbox | `RUNE_STORAGE_FORMAT=auto` |

Never delete or copy `storage.meta` on its own. Details, refusal table and the multi-disk-loss procedure
(`RUNE_STORAGE_RECOVERY_VOTE`) are in SELF-HOSTING.

## Health and failure

- **Leadership.** Each broker logs `rune-broker: node N is LEADER term T` and every change (`leadership change`).
  `rune_role` prints the machine-readable form from the Aeron counters, independent of the log level:
  `docker exec <container> rune_role` prints `ROLE nodeId=... role=LEADER|FOLLOWER|CANDIDATE|NONE term=...` and a `STORAGE`
  line (`state=CONFIRMED|UNCONFIRMED|...`).
- **Shutdown.** `SIGTERM` or `SIGINT` stops cleanly. With `RUNE_CONTROLLED_SHUTDOWN_MS` (default 5000) the node first hands
  partition leadership to in-sync peers. `docker stop` sends `SIGTERM`.
- **Signals.** `SIGUSR1` writes a flight-recorder report; `SIGHUP` reloads the TLS certificate and key (TLS builds).
- **Failover.** Killing a leader triggers re-election among the survivors. An acked `acks=all` produce answered
  `NotLeader` or `Internal`, or not at all, is *in doubt*; the client retries it with the same producer id, epoch and
  sequence and the broker deduplicates.
- **View reads after a failover.** A view row is served only by the leader of its source partition, and only while that
  broker holds a read lease from the controller (renewed every 20 ms, about 120 ms long). A leader cut off from the
  controller stops serving view rows once its lease runs out, before any other broker can take its partitions, and answers
  `NotLeader` (the client retries). A new source leader serves its views about 150 ms after it takes over. Hosts must keep a
  boot-time clock that counts suspend: `CLOCK_BOOTTIME` on Linux, which kvmclock keeps current across a VM pause.
- **Exit codes.** `2` configuration error, `78` storage refusal (empty or damaged data directory), `74` storage failure
  (the broker halts rather than serve from a suspect disk).

## Metrics

Metrics are Aeron counters in the broker's media driver (`<data dir>/aeron/cnc.dat`, or `RUNE_AERON_DIR`). Read them with
Aeron's counter tools or `rune_role`. Labels end with `node=<id>`.

| Counter | Type id | Meaning |
|---|---|---|
| `rune-cluster-node-role` | 1 000 201 | This node's consensus role. |
| `rune-cluster-leadership-term` | 1 000 203 | Current leadership term. |
| `rune-msm-commit-position` | 1 000 204 | Metadata state machine commit position. |
| `rune-txn-stuck-markers` | 1 000 210 | Transaction markers that have not landed. |
| `rune-node-out-of-sync-partitions` | 1 000 211 | Partitions of this node that are out of sync. |
| `rune-segment-provision-*` | 1 000 212 to 216 | Segment pre-creation: build failures, roll holds, un-preallocated rolls, parked drops, read-map failures. |
| `rune-catalog-apply-halted` | 1 000 217 | `0` while applying; non-zero after a catalog apply halt. Alert on it. |
| `rune-catalog-applied-offset` | 1 000 218 | Last applied `__catalog` offset. |
| `rune-catalog-unplaceable-topics` | 1 000 219 | Topics never placed for want of brokers. |
| `rune-catalog-blocked-partitions` | 1 000 220 | Partitions blocked until a broker serves again. |
| link MTU deficit | 1 000 221 | A peer link whose path MTU is below the configured MTU. |
| `rune-storage-gate-state` | 1 000 222 | Storage gate state of this node. |
| `rune-leader-roles-held` | 1 000 223 | Leader roles this node holds. |
| `rune-storage-pending-nodes` | 1 000 224 | Nodes whose new storage is not yet confirmed. |
| `rune-lost-committed-reports` | 1 000 225 | Reports of lost committed data. |
| `rune-security-apply-halted` | 1 000 226 | `0` while applying; the halted offset plus 1 after a halt. Alert on it. |
| `rune-security-applied-offset` | 1 000 227 | Last applied `__security` offset. |
| `rune-auth-mode` | 1 000 228 | `0` authentication enabled, `1` dev mode. Alert on `1` in production. |
| `rune-logins-ok` / `-failed` / `-rate-limited` | 1 000 229 to 231 | Login tallies. Alert on rising failures. |
| `rune-sessions-revoked` | 1 000 232 | Sessions ended by drop, disable, `TERMINATE SESSIONS` or a security halt. |
| `rune-peer-links-established` | 1 000 233 | Peers whose link is currently established. |
| `rune-peer-link-refusals` | 1 000 234 | Peer-link refusals logged by this broker. Alert on a rising count. |
| `rune-peer-frame-drops` | 1 000 235 | Inter-broker frames dropped for want of a link or because they did not open. |
| `rune-txn-offset-commits-stuck` | 1 000 240 | Transaction commits waiting because the group coordinator refuses their consumer-group offsets. Alert on a non-zero value that persists. |
| `rune-txn-offset-stuck-age-ms` | 1 000 241 | Age of the oldest of those, in ms. A WARN names it every 30 s while any is stuck. |

Type ids are defined in `broker/libs/node/include/rune/node/RoleCounter.h`. Logins are metrics and structured logs, not
`__security` events.

## The cluster key

Brokers authenticate each other with one shared file, `RUNE_CLUSTER_KEY_FILE`: a mutual HMAC challenge-response on every
inter-broker connection, and every inter-broker frame is sealed with a per-link key derived from it (HKDF-SHA-512, one key per
direction, 32 bytes of overhead per frame). The key never crosses the wire.

- **Generate** with `rune security gen-cluster-key --out <path>`: 64 hex characters, mode `0600`, never overwriting.
  The contents must be at least 32 bytes after trailing whitespace. The broker refuses a file that is group- or
  world-accessible.
- **Mount read-only** on every node (a docker bind mount or a Kubernetes secret with `defaultMode: 0400`). The image
  entrypoint refuses to start without it and **never generates** one: a key made inside one container would differ from
  its peers' and split the cluster.
- **Losing it** means no node can join; keep a backup in your secret store.

### Rotating the cluster key

Three rolling restarts, no downtime. `RUNE_CLUSTER_KEY_FILE_NEXT` holds a second key; a broker presents its current key
and accepts the current or the next.

1. **Add the next key everywhere.** Restart each broker, one at a time, with `RUNE_CLUSTER_KEY_FILE=<old>` and
   `RUNE_CLUSTER_KEY_FILE_NEXT=<new>`.
2. **Make it current.** Restart each broker with `RUNE_CLUSTER_KEY_FILE=<new>` and `RUNE_CLUSTER_KEY_FILE_NEXT=<old>`.
3. **Remove the old key.** Restart each broker with `RUNE_CLUSTER_KEY_FILE=<new>` and no `_NEXT`.

After each restart wait until the node logs `peer link with node N established` for both peers before touching the
next node. `broker/docker/auth-e2e.sh` runs exactly this under a live producer and consumer and checks that every acked
record arrives ([testing.md](testing.md#end-to-end-docker-suites)).

### Password rotation

A user holds at most two verifiers. `ALTER USER u ADD PASSWORD '...'`, roll the clients to it, then
`ALTER USER u DROP PASSWORD 1`. Both log in meanwhile. `SET PASSWORD ... TERMINATE SESSIONS` replaces everything and
ends live sessions. See SECURITY.md.

## Backups and disaster recovery

What lives where, under `RUNE_DATA_DIR`: `storage.meta` (the storage identity), the node state and term log, the
partition logs under `topics/`, `__catalog/` and `__security/` (the replicated event logs), `dropped/` and `quarantine/`
(never deleted by the broker) and, unless `RUNE_AERON_DIR` moves it, `aeron/`.

- **Replication is the first line**: a topic's replication factor and `tier` decide what survives a node loss. A lost disk
  on one node of three is recovered by `storage format --join`; the node catches up from the cluster and serves
  sessions meanwhile but leads nothing until the controller confirms its storage.
- **Restoring a whole directory from a backup.** On a multi-node cluster run
  `rune_broker storage format --reassign-join --data-dir <D>` first, so the node rejoins like a replaced disk; starting it
  as is would let it vote on stale state. On a **single-node** cluster start the restored directory as it is: there is no
  other copy for it to diverge from. A lost single-node disk is recovered only from a backup.
- **Catalog.** `rune catalog export --broker host:port > prod.rcat` writes the whole DDL history; `rune catalog replay
  --from prod.rcat --into fresh:40200` rebuilds it on another cluster and `rune catalog diff` proves the two equal
  (exit 0). `docker/catalog-e2e.sh` runs this end to end. Do not delete `dataDir/__catalog` to repair a broker: restore it.
- **Security log.** There is no export command; the `__security` history is read with `SHOW SECURITY HISTORY`. A broker
  whose apply halted fails **closed** (it revokes sessions and refuses logins); see the runbook in SECURITY.md.
- **Losing most disks.** If a majority of brokers lost their disks the cluster stays unavailable; set
  `RUNE_STORAGE_RECOVERY_VOTE=1` on the replaced nodes to elect the one intact broker. This can lose what only the lost disks
  held, and is logged at critical level.
- **Halted catalog apply.** A broker whose catalog replay halts moves, renames and deletes nothing and accepts no catalog
  change. Follow the runbook in CATALOG.md.
- **Proof.** The DR suite ([testing.md](testing.md#disaster-recovery-suite-dr-e2e)) kills, pauses, restarts and
  partitions nodes under load and asserts no loss, per-key ordering, exactly-once and a single leader.

## Upgrades

- Upgrade **brokers before clients**: a new client defining a transient topic against an older broker silently gets a
  disk topic.
- A client older than version 9 is refused at the connect on an authenticated cluster; a client older than version 8 cannot
  follow a DDL redirect.
- **View reads during a rolling upgrade.** A broker serves view rows only under the controller's read lease. While the
  controller runs a build without it, it grants none and `getViewRow` answers `NotLeader`; reads resume once the
  controller runs a build that has it.
- A data directory from a build without storage ids is adopted as `legacy` on first start. Old
  `dataDir/__topic_schemas` and `dataDir/__views` are ignored and never changed.
- Release notes: [`broker/docs/release-notes/`](../broker/docs/release-notes) and
  [`client-cpp/docs/release-notes/`](../client-cpp/docs/release-notes).

## Flight recorder

An ultra-low-overhead recorder compiled into the broker (and the client). Spans are timed with `begin()` and `commit()` into
a per-thread ring, drained off the hot path into per-site latency histograms and a Chrome trace. It is **off at runtime**
until enabled.

```bash
FLIGHT_RECORDER_ENABLE=1 FLIGHT_RECORDER_OUT=/var/lib/rune/fr rune_broker   # plus the usual RUNE_* settings
kill -USR1 <pid>                                                             # dump now; shutdown dumps too
```

| Variable | Default | Effect |
|---|---|---|
| `FLIGHT_RECORDER_ENABLE` | off | Arms recording. |
| `FLIGHT_RECORDER_RING_BYTES` | 128 MiB per thread | Ring size per thread. |
| `FLIGHT_RECORDER_OUT` | current directory | Where reports are written. |
| `FLIGHT_RECORDER_CATEGORIES`, `_SLOW_SPAN_NS`, `_THROTTLE_NS`, `_DRAIN_MS` | all, 2000, 50000, 100 | Read by `configureFromEnv()` (the client benchmark harness); see [configuration.md](configuration.md#flight-recorder). |

Each report is three files named `fr-hist-n<node>-p<pid>-<tag>.txt` (per-site p50, p90, p99, p999, max, count, mean in
nanoseconds), `fr-trace-...json` (open in `chrome://tracing`, Perfetto or speedscope) and `fr-tape-...bin` (the versioned raw
tape). `broker/bench/kafka-compare/frwaterfall.py fr-trace-*.json` derives replication commit and ack latency from the traces. The ring is **drop-on-full**:
`commit()` never blocks the hot path. The library, its API and its overhead are documented in the
[flight-recorder repository](https://github.com/arcanumruna/flight-recorder). The old `RUNE_FR_*` names no longer exist.

Measured on an Apple Silicon dev machine: 0.66 ns per span when compiled in but disabled, 8.56 ns enabled (the flight-recorder README has the table).
