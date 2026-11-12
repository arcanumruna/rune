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

Container requirements: `--shm-size=256m` (the default 64 MB is too small), the data directory on a volume, `--network host` (the
low-latency mode: the Docker bridge adds about 35 µs at p50, and published ports make p99 about 10x worse; Linux only in
practice; on a bridge, publish the client port and the node's shard block `8200 + 192n` to `8200 + 192n + 191`; the derived response-control ports sit inside that block), and a
file-descriptor hard limit that covers the partitions (the broker raises its soft limit to the hard limit and refuses to start when the hard
limit is below the estimated need; see SELF-HOSTING, "Open-file limit").

Firewall: allow inbound UDP on the client port, each worker's ingress port and the response-control ports (gateway and
workers) from clients. The per-worker egress control ports are needed only when authentication is off.

Run an **odd** number of nodes: one for development, three or more for availability. A two-node cluster tolerates no
failure. Membership is static: the peer list is fixed at boot.

OS settings that matter for UDP throughput (`net.core.rmem_max` and `wmem_max` cap `RUNE_AERON_SOCKET_BUFFER_BYTES`; the
Aeron receive window must not exceed the socket buffer or half a term) are covered in
[configuration.md](configuration.md#aeron-transport).

### NAT-safe client egress

A client behind NAT (home router, cloud NAT gateway, most Kubernetes pods) needs no inbound rule: every reply returns over
the flow the client itself opened (Aeron response channels). The address a client declares is not used for a client that
sets the gateway response port.

| Port | Who binds it | Open inbound UDP from clients |
|---|---|---|
| client port (`RUNE_CLIENT_ENDPOINT`, 40200) | gateway | yes |
| worker ingress ports | each worker | yes |
| gateway response-control port | gateway | yes |
| per-worker response-control ports | each worker | yes |
| per-worker egress control ports | each worker | only with authentication off |

Derived defaults (shard mode, no explicit ports), for node id `n` and `W` workers:

| Port | Value |
|---|---|
| egress control base | `8200 + 192 n + 128` |
| gateway response | egress base `+ 63` |
| worker `w` response | egress base `+ 63 - W + w` |

Derived response ports need `W <= 31`; see [configuration](configuration.md#derived-ports) for the rule and the explicit
settings. Firewall: inbound UDP on the ports above only.

Client side: set `gatewayResponseControlPort` (`--gateway-response-port`, `RUNE_GATEWAY_RESPONSE_PORT`) to the **seed**
broker's gateway response port. The other brokers' ports come from Metadata. A client that leaves it at `0`, and every
older client, keeps the declared-address path unchanged.

A plaintext session's worker bind token travels in plaintext (`ConnectResponse` and `WorkerBind`). On a TLS session the
token never crosses the wire: the `ConnectResponse` carries an all-zero token and both ends derive it from the TLS keys
(HKDF over the client-to-broker key and the session id). `WorkerBind` and the worker's `WorkerBindAck` are plaintext frames;
the client resends an unacknowledged bind every 250 ms and reconnects after four attempts.

End-to-end proof: `bash broker/docker/nat-e2e.sh` (clients behind a masquerading router container, default config, 3 nodes).

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

### Memory footprint

Two fixed costs set the resident floor of a node, before any topic or view:

- Per node: the cleaner's dedupe map, `RUNE_CLEANER_DEDUPE_BUFFER_BYTES` (default 128 MiB, written whole at start and on every cleaner pass). The offset table stays within that budget; the producer tails add up to 25% of it (minimum 1024 x 64 B) on top, so the total is at most about 1.25 times the budget.
- Per embedded Aeron driver: 3 x 128 MiB command rings, resident from the start. That is the Aeron default; the `aeron/1.51.0-rune.3` build honours the `AERON_DRIVER_COMMAND_QUEUE_CAPACITY` environment variable (bytes per ring), which the broker does not set (see #658).

## Health and failure

- **Leadership.** Each broker logs `rune-broker: node N is LEADER term T` and every change (`leadership change`).
  `rune_role` prints the machine-readable form from the broker's metrics file (see [Metrics](#metrics)), independent of
  the log level:
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
  `NotLeader` (the client retries). A move that takes leadership off a live broker and out of the in-sync set (a controlled
  shutdown, an untrusted storage, a broker exclusion) commits only once that broker's lease has ended: up to about 120 ms
  later, while the broker keeps leading and acking writes. Hosts must keep a boot-time clock that counts suspend:
  `CLOCK_BOOTTIME` on Linux, which kvmclock keeps current across a VM pause.
- **Exit codes.** `2` configuration error, `78` storage refusal (empty or damaged data directory), `74` storage failure
  (the broker halts rather than serve from a suspect disk).

## Namespaces

A namespace is a tenant: its topics, views, schemas, groups, transactions and users belong to it alone. `default` always
exists and cannot be deleted. Objects are named `ns/name`; a session logs in as `ns/user` and names its own objects
unqualified (`t` means `a/t` for `a/alice`). Only a cluster admin may name an object of another namespace.

```bash
rune namespace create a --broker rune-1:40200 --user admin --password-file admin.pw
rune namespace list --broker rune-1:40200 --user admin --password-file admin.pw           # nsId, name, state (--json: limits too)
rune namespace limits a --set max-topics=100 --broker rune-1:40200 --user admin --password-file admin.pw   # no --set: print
rune namespace usage a --broker rune-1:40200 --user admin --password-file admin.pw          # each limit next to its usage (--json too)
rune namespace suspend a --read-only --broker rune-1:40200 --user admin --password-file admin.pw   # or --reject-all
rune namespace resume a --broker rune-1:40200 --user admin --password-file admin.pw
rune namespace delete a --broker rune-1:40200 --user admin --password-file admin.pw           # asks on a terminal; --yes to skip, refused without a terminal
```

| Role | Can do |
|---|---|
| Cluster admin | Everything in every namespace; creates namespaces and sets limits; appoints namespace admins. |
| Namespace admin | Within its namespace: users, grants, topic, view and schema DDL (`GRANT ADMIN ON NAMESPACE a TO USER 'a/admin';`). |
| User | Whatever its grants allow, in its own namespace. |

- **Foreign objects answer exactly like missing ones**: same error code and message, so a name in another namespace is
  never revealed. A namespace admin sees only its own namespace in `rune namespace list`.
- **Anonymous read is off by default.** Only a cluster admin turns it on, and only for `default`.
- Consumer groups and transactions work in every namespace: the same group id or transactional id in two namespaces
  is two groups or two producers, with independent offsets, membership and fencing.
- The data lives under `<data>/ns-<nsId>/topics/<name>/`. A cluster with namespaces is a new cluster; there is no
  in-place upgrade.
- The 3-node check is `docker/namespace-e2e.sh` ([testing](testing.md#end-to-end-docker-suites)).

### Lifecycle: suspend, resume, delete

Only a cluster admin changes a namespace's state (`ADMIN ON CLUSTER`). A state change is one catalog event, so it takes
effect on every node when that event applies; no timer is involved.

| State | Served | Refused | Sessions |
|---|---|---|---|
| Active | everything its grants allow | - | - |
| Suspended, read-only | fetch, metadata, view and QL reads, offset fetch, group join, sync, heartbeat, leave | produce, every DDL (topic, view, schema, user, grant), transactions, offset commit: `NamespaceSuspended (51)` | kept; open transactions are aborted when the suspend applies |
| Suspended, reject-all | nothing | every request; a login with the right password gets `NamespaceSuspended (51)`, a wrong password `AuthenticationFailed (44)` | every session of the namespace is closed when the suspend applies |
| Deleting | nothing: it answers like a name that never existed | everything; a login gets `AuthenticationFailed (44)` | closed when the delete applies |

- The cluster admin is exempt from both suspend modes. A client older than version 14 does not know code 51 and gets
  `InvalidRequest (39)` instead; the same holds for codes 52 to 54 below.
- `resume` returns a suspended namespace to Active; suspending in the mode a namespace is already in, or resuming an Active
  one, is an Ok no-op (suspending in the other mode switches it). `default` cannot be deleted. Suspend, resume and delete of an unknown or Deleting name answer exactly like
  any unknown name.
- **Delete** is one event, and that event is the commit point: the namespace is DELETING at once and **its name is free
  at once** (a new `create` of the same name gets a new nsId and sees nothing of the old namespace). The reaper on the
  `__catalog` leader then aborts the open transactions, drops the views, topics and schemas, tombstones the consumer-group
  offsets and the transaction state, and purges the users, roles and grants. Every step is recorded in the catalog, so a
  crash or a leader change at any point is finished by whichever broker leads next. When all steps are done the namespace
  leaves `rune namespace list`, every node renames `ns-<id>` to `ns-<id>.deleting` and removes it, and the boot reconcile
  removes any leftover. A DELETING namespace still counts toward `RUNE_MAX_NAMESPACES`.
- `RUNE_NS_DELETE_BATCH` (default 256) bounds the tombstones and drops the reaper issues per pass.

### Limits

`rune namespace limits <name> --set <limit>=N` stores a limit (`0` = unlimited where the limit allows it); every limit is
enforced, each with one precise code:

| Limit | Checked | Refused with |
|---|---|---|
| `max-topics`, `max-partitions-per-topic`, `max-partitions-total`, `max-views`, `max-schemas`, `max-versions-per-schema`, `max-groups`, `max-users` | at the DDL (groups at a new group's first join, users at `CREATE USER`) | `NamespaceLimitExceeded (52)` |
| `min-replication-factor`, `max-replication-factor`, `lowest-acks`, `lowest-min-insync` | at topic DDL; the acks floor at every produce | `NamespaceLimitExceeded (52)` |
| feature bits | the `compaction`, `views`, `txns`, `anonymous read` and `scram` bits are enforced at the DDL or at connect / login. The `ql`, `sql` and `redis` bits are stored only: there is no serving path for them yet | `NamespaceLimitExceeded (52)` |
| `max-connections`, `max-connections-per-user`, `max-sessions-per-user`, `max-open-txns`, `max-txn-time-ms` | at login / `InitProducerId` / `AddPartitions`, **per node** | `NamespaceLimitExceeded (52)` |
| `max-disk-bytes` | measured on the node that leads the partition | produce on the leader: `NamespaceLimitExceeded (52)`; follower apply, replication catch-up and the log cleaner are never refused. A transient topic neither charges nor is refused by `max-disk-bytes` (it has no disk bytes) |
| `max-message-bytes`, `max-batch-bytes` | the worker's produce intake on the leader. A batch longer than `max-batch-bytes`, or a record whose length field (its body bytes) is longer than `max-message-bytes`, is refused and nothing is appended. The record fields are checked against that length field; a batch whose records do not match their length fields or the header's record count, or that has trailing bytes, is malformed and refused. 0 = unlimited | `NamespaceLimitExceeded (52)`; a malformed batch: `InvalidRequest (39)` |
| `max-retention-ms`, `max-retention-bytes` | a topic DDL above the maximum is refused; the cleaner applies `min(topic retention, namespace maximum)` | `NamespaceLimitExceeded (52)` at DDL |
| `produce-bytes-per-sec`, `produce-records-per-sec`, `fetch-bytes-per-sec`, `request-ops-per-sec`, `query-cycles-per-sec` | a per-namespace aggregator compares the node's usage with `limit x elapsed` every `RUNE_NS_LIMIT_TICK_US` (default 10000) | `Throttled (53)` carrying a retry-after; the client library waits it out and retries. `fetch-bytes-per-sec` and `request-ops-per-sec` also apply to subscriptions: a throttled namespace is simply not pushed to until the throttle lapses, and a subscription's request ops are counted once per Subscribe |
| `max-view-memory-bytes` | the node's view chunks | the namespace's views pause at their offset and a view read answers `ViewPaused (54)`; they resume when memory falls or the limit is raised |
| `max-leases` | the node's live lease rows (leader-held) | while the node is at or over the limit EVERY grant answers `NamespaceLimitExceeded (52)` (also a grant of a key that is live, which would otherwise be `KeyExists`), and a batch that mixes a grant with updates or revokes is refused whole; keepalives, updates, revokes and expiry continue; a namespace created before the lease feature level keeps `0` (unlimited) after the upgrade until `max-leases` is set; `0` = unlimited, default 16777216 (`RUNE_NS_DEFAULT_MAX_LEASES`), needs every broker at the lease feature level |

- **`max-txn-time-ms` is at most 2592000000 (30 days).** A larger value on `CreateNamespace` or `SetLimits` is refused with
  `InvalidRequest (39)`; so is `RUNE_NS_DEFAULT_MAX_TXN_TIME_MS` above it, at startup.
- **Lowering a limit deletes nothing**, except the retention maximum: objects over a lowered count stay and the next
  create is refused; a lowered retention maximum deletes old segments on the next retention pass and the SetLimits is answered Ok with the warning `retention clamped on N topics` in the
  DDL response's detail.
- Rates, disk and view memory are enforced from the workers' paced reports, so a namespace can exceed one by what its
  node produces in about one report interval plus one aggregator tick before the throttle arrives; the rate debt carries
  the excess forward and the long-run rate converges on the limit. A rate limit never makes a follower refuse a replicated
  batch, so `acks=all` never stalls on it.
- `rune namespace usage <name>` prints each limit next to its current usage on the node it asks (counts including
  schemas, the highest schema version, groups, users and open transactions, disk, view memory, connections and rates;
  fields with no usage meaning print `-`). Usage is per node: groups and open transactions are non-zero only on the node
  whose group and transaction coordinator is loaded.
- Anonymous sessions have their own caps: `RUNE_ANONYMOUS_MAX_CONNECTIONS` (default 16) and
  `RUNE_ANONYMOUS_REQUEST_OPS_PER_SEC` (default 1000).

The error codes, all gated on client version 14:

| Code | Name | Meaning |
|---|---|---|
| 51 | `NamespaceSuspended` | the namespace is suspended and this request is not served in that mode |
| 52 | `NamespaceLimitExceeded` | a count, durability, feature, connection, transaction or disk limit |
| 53 | `Throttled` | a rate limit; for a v14+ client the retry-after in ns rides `ProduceResponse.logStartOffset` |
| 54 | `ViewPaused` | the view is paused over its namespace's memory limit |

## Metrics

The broker writes its metrics to one memory-mapped file in the [arcanum-metrics](https://github.com/arcanumruna/arcanum-metrics)
format: `RUNE_METRICS_FILE`, default `${RUNE_DATA_DIR}/metrics/rune-metrics.dat`. Every metric is allocated once at
startup, before any agent thread runs, and each has exactly one writer thread. The file is created afresh on every start
(directory `0750`, file `0640`); a broker that cannot create it does not start.
Read it with `rune_role` or any arcanum-metrics reader. The Aeron driver's own `cnc.dat` (`<data dir>/aeron/`, or
`RUNE_AERON_DIR`) now holds only the driver's counters; no Rune metric lives there.

Every node metric is keyed by the 4-byte little-endian node id, and its label is `<label> node=<id>`. Each slot holds the
current value only. Counters only go up and gauges are a current level. Readers compute rates and windows themselves by
sampling the file; the broker keeps no history.

### Node metrics

| Type id | Label | Kind | Owner | Meaning |
|---|---|---|---|---|
| 1000201 | `rune-cluster-node-role` | gauge | observer | This node's consensus role. |
| 1000203 | `rune-cluster-leadership-term` | gauge | observer | The leadership term this node sees (`-1` none). |
| 1000210 | `rune-txn-stuck-markers` | gauge | control | Transaction markers that have not landed and are still retrying. |
| 1000211 | `rune-node-out-of-sync-partitions` | gauge | observer | Partitions of this node that are out of the committed ISR (`-1` unknown). |
| 1000212 | `rune-segment-provision-build-failures` | counter | provisioner | Segment pre-creation builds that failed. |
| 1000213 | `rune-segment-provision-roll-holds` | counter | provisioner | Segment rolls held for a segment not ready yet. |
| 1000214 | `rune-segment-provision-unpreallocated-builds` | counter | provisioner | Segments built without preallocation. |
| 1000215 | `rune-segment-provision-parked-demands-dropped` | counter | provisioner | Parked segment demands dropped. |
| 1000216 | `rune-segment-provision-read-map-failures` | counter | provisioner | Segment read maps that failed. |
| 1000217 | `rune-catalog-apply-halted` | gauge | control | `0` while applying; the halted offset plus 1 after a catalog apply halt. Alert on it. |
| 1000218 | `rune-catalog-applied-offset` | gauge | control | Last applied `__catalog` offset (`-1` none). |
| 1000219 | `rune-catalog-unplaceable-topics` | gauge | control | Topics never placed for want of brokers. |
| 1000220 | `rune-catalog-blocked-partitions` | gauge | control | Partitions blocked until a broker serves again. |
| 1000221 | `rune_link_mtu_deficit` | gauge | control | Bytes by which the probed link MTU falls short of `RUNE_AERON_MTU` plus the UDP header (`0` when it fits). Alert on a non-zero value. |
| 1000222 | `rune-storage-gate-state` | gauge | control | Storage gate state: `0` CONFIRMED, `1` UNCONFIRMED, `2` UNDECIDED. |
| 1000223 | `rune-leader-roles-held` | counter | control | Leader roles held back (not taken) because this node's storage gate was closed; each one asks the controller for a failover. |
| 1000224 | `rune-storage-pending-nodes` | gauge | control | Nodes whose new storage is not yet confirmed. |
| 1000225 | `rune-lost-committed-reports` | counter | control | Reports of lost committed data. |
| 1000226 | `rune-security-apply-halted` | gauge | control | `0` while applying; the halted offset plus 1 after a halt. Alert on it. |
| 1000227 | `rune-security-applied-offset` | gauge | control | Last applied `__security` offset (`-1` none). |
| 1000228 | `rune-auth-mode` | gauge | control | `0` authentication enabled, `1` dev mode, `2` in-process unauthenticated (built without an auth mode). Alert on a non-zero value in production. |
| 1000229 | `rune-logins-ok` | counter | control | Successful logins. |
| 1000230 | `rune-logins-failed` | counter | control | Failed logins. Alert on a rising rate. |
| 1000231 | `rune-logins-rate-limited` | counter | control | Login attempts refused by the login throttle (per user and source). |
| 1000232 | `rune-sessions-revoked` | counter | control | Sessions ended by drop, disable or `TERMINATE SESSIONS`. |
| 1000233 | `rune-peer-links-established` | gauge | control | Peers whose link is currently established. |
| 1000234 | `rune-peer-link-refusals` | counter | control | Peer-link refusals logged by this broker. Alert on a rising rate. |
| 1000235 | `rune-peer-frame-drops` | counter | control | Inter-broker frames dropped for want of a link or because they did not open. |
| 1000239 | `rune-unauth-log-suppressed` | counter | control | Log lines not written because the rate limiter held them back: lines an unauthenticated sender can drive (failed decrypts, refused logins, require-TLS refusals, bad bootstrap proofs) are logged the first time and then every 1024th occurrence of their kind, and the rest are counted here, summed over the gateway and every worker. A worker reports its share with each line it logs, so this counter can trail the true count by up to 1023 per log kind per worker. A fast rise means someone is sending the broker bad traffic; the log shows only a sample. |
| 1000240 | `rune-txn-offset-commits-stuck` | gauge | control | Transaction commits waiting because the group coordinator refuses their consumer-group offsets. Alert on a non-zero value that persists. |
| 1000241 | `rune-txn-offset-stuck-age-ms` | gauge | control | Age of the oldest of those, in ms. A WARN names it every 30 s while any is stuck. |
| 1000242 | `rune-ns-slot-mismatch` | counter | control | Namespace slot tags refused by the gateway or a worker (`rune.ns.slot_mismatch`): a message or request whose (slot, nsId) pair the receiver's slot table does not hold. Any rise is a bug. |
| 1000243 | `rune-peer-hello-shed` | counter | control | Peer-link Hello / HelloV2 frames dropped because the sending peer had used its one crypto-costing Hello answer for this duty cycle (a flood, or a legitimate initiator retrying in the same cycle as a forged one). A sustained rise means an off-path Hello flood; the real initiator re-sends every 200 ms and still links. |
| 1000270 | `rune-log-damaged-segments` | gauge | control | Sealed log segments found damaged at boot, summed over every log the node serves: bytes after the last whole batch of a segment below the recovery point. They stay on disk and reads that reach them fail (see Damaged log segments). Set once at start; damage that readers reach later is counted by `rune-log-damaged-reads`. |
| 1000271 | `rune-log-damaged-reads` | counter | control | Subscription reads that reached a damaged or unreadable stored batch since the node started. Each one fails its subscription with `StorageError`. A value above 0 means clients are reading into damage found at boot or later. |

Per-namespace metrics (one series per namespace on this node, allocated when the namespace is created and freed when it is
erased; the key is the namespace id and the slot's owner tag is the namespace id too):

| Type id | Label | Kind | Owner | Meaning |
|---|---|---|---|---|
| 1000300 | `rune-ns-slot` | gauge | control | The node-local slot of the namespace (`default` is slot 0). The slot is the index of every per-namespace array on the hot path. |

The owner is the one thread that writes the metric: `control` the broker's control agent, `observer` the role and ISR
observer, `provisioner` the segment provisioner. Type id 1000204 (a metadata commit position that was never published) has no row, and
1000236 to 1000238 are reserved. The table is `kRuneMetrics` in `broker/libs/node/include/rune/node/RuneMetrics.h`; a
regression test (`BrokerMetricsRegression.OperationsDocTableMatchesRegression`) keeps this table equal to it. Logins are
metrics and structured logs, not `__security` events.

### Process metrics

On Linux the admin agent samples `/proc/self` every `RUNE_METRICS_SYSTEM_INTERVAL_MS` (default 1000) and writes the rows
below. Other hosts have no `/proc`, and their brokers export no process metrics.

| Type id | Label | Kind | Meaning |
|---|---|---|---|
| 1000301 | `process_open_fds` | gauge | Open file descriptors. |
| 1000302 | `process_threads` | gauge | Threads in the broker process. |
| 1000303 | `process_resident_memory_bytes` | gauge | Resident set size, in bytes. |
| 1000304 | `process_virtual_memory_bytes` | gauge | Virtual memory size, in bytes. |
| 1000305 | `process_metrics_read_errors_total` | counter | `/proc` reads that failed; the other values keep their last sample. |

## The cluster key

Brokers authenticate each other with one shared file, `RUNE_CLUSTER_KEY_FILE`: a mutual HMAC challenge-response on every
inter-broker connection, and every inter-broker frame is sealed with a per-link key derived from it and an X25519 ephemeral
secret (HKDF-SHA-512, one key per direction, 32 bytes of overhead per frame, forward secrecy). The key never crosses the wire.
When upgrading to the forward-secret handshake, upgrade brokers in **descending node id**: an upgraded broker only initiates
the new handshake, and a broker from before it does not answer that.

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
  whose apply halted fails **closed**: it ends sessions and refuses logins with the retriable `AuthUnavailable`, so
  clients reconnect to another broker; see the runbook in SECURITY.md.
- **Losing most disks.** If a majority of brokers lost their disks the cluster stays unavailable; set
  `RUNE_STORAGE_RECOVERY_VOTE=1` on the replaced nodes to elect the one intact broker. This can lose what only the lost disks
  held, and is logged at critical level.
- **Halted catalog apply.** A broker whose catalog replay halts moves, renames and deletes nothing and accepts no catalog
  change. Follow the runbook in CATALOG.md.
- **Proof.** The DR suite ([testing.md](testing.md#disaster-recovery-suite-dr-e2e)) kills, pauses, restarts and
  partitions nodes under load and asserts no loss, per-key ordering, exactly-once and a single leader.

## Damaged log segments

Recovery used to cut a sealed log segment at its first bad batch and let readers skip to the next segment. It now keeps the
segment as it is. A sealed segment whose bytes after the last whole, CRC-valid batch are torn or corrupt is **damaged**:

- **Kept.** The file is not touched (no truncation, no rename). The broker logs `Log <dir>: segment <base> is DAMAGED at byte <n>
  (offset <o>)` at error level when it finds it (at boot, or when the legacy compactor or the cleaner reloads the segment),
  and the gauge `rune-log-damaged-segments` counts the damaged segments found at boot; the counter `rune-log-damaged-reads`
  counts the subscription reads that reach damage while the node runs. A node with a damaged **partition**
  segment still starts; a damaged **system log** refuses to start (below).
- **Reads fail at the damage.** A read at or after the damaged offset, up to the next segment, is never skipped past. A consumer
  or subscription that reaches it gets `StorageError (26)` ("the stored batch is corrupt (damaged log segment)") and the client
  fails that subscription with `CorruptBatch`; records before the damage and in later segments are served.
- **Replication.** A leader asked to fetch the damaged range answers `kErrCorruptRecord` (-12). The follower stops at its log end
  and leaves the in-sync set; nothing is repaired automatically.
- **Cleaner.** The log cleaner refuses a log that has a damaged segment until an operator acts.
- **Retention.** Retention deletes the damaged segment whole, once it ages out. The size accounting counts only its valid prefix.
- **System logs refuse to start.** Damage in `__catalog`, `__security`, `__consumer_offsets`, `__transaction_state` or
  `__cluster_metadata` stops the node at boot: it logs `system log <dir> has a damaged segment at offset <o> - refusing to
  start`, halts through the storage-failure path (exit 74) and serves nothing. These logs are replayed into the node's
  state (topics, users and grants, committed group offsets, transactions, partition assignments), so loading around a hole
  would serve a wrong state. The recovery is to wipe that node's data directory and let it resync from its peers (a
  single-node cluster restores from a backup). Damage that appears after boot (a reload) makes the same loaders stop
  instead of skipping: the group coordinator and the transaction coordinator stay unloaded, and the metadata controller
  decides nothing.
- **Group consumers.** A group member whose fetch subscription reaches the damage ends with `CorruptBatch` (an
  undeliverable batch: `UndeliverableBatch`), the same as a plain subscription.
- **Views, checked writes and filtered groups.** The view fold, the checked-write verdict judge and a filtered group's
  scan stop at the damage with no terminal error of their own: a view over the damaged partition never becomes Ready and a
  checked write never gets a verdict until the segment is repaired or retention deletes it. Watch the gauge
  `rune-log-damaged-segments` and the `DAMAGED` log lines.
- **Upgrade.** A data directory written before the recovery-point file existed has no recovery point: an old torn tail in a
  sealed segment then counts as damage at the first start after the upgrade, where it used to be cut silently, and readers
  that reach it fail. The `DAMAGED` line names the segment; restore it from a backup or let retention delete it.
- **Active segment.** The newest segment is different: a torn tail there is a normal crash artefact and is still cut at the last
  whole batch.

**Repair.** Restore the partition directory from a backup (see Backups and disaster recovery), or let retention delete the segment.
There is no automatic repair.

## View checkpoint backups

A persisted view keeps a checkpoint file per partition, `<data dir>/__view_snapshots/view-<id>-p<n>.committed.delta`. When the
broker cannot trust one it throws the rows away and rebuilds them from the log. With `RUNE_VIEW_BACKUP=1` (the default) it first
copies the file to `<data dir>/__view_snapshots/backup/view-<id>-p<n>.<10-digit seq>.<reason>.delta`. The view serves nothing
(`NotReady`) until the copy is on disk and the rebuild has caught up.

| Reason | The checkpoint was discarded because |
|---|---|
| `legacy` | it is in the old format, which may hold rows of aborted transactions |
| `legacy-superseded` | an old-format file sat next to a current one (copied only, never rebuilds the view) |
| `unreadable` | a record failed its CRC or would not decode, or the file is damaged before its last record |
| `ahead-of-log` | its offset is above the end of the source log (the log lost its tail) |
| `ahead-of-verdicts` | a verdict view is ahead of the verdict markers in the local log |
| `below-log-start` | the view's cursor fell below the source's log start while rows were in the table |

Log lines: `VIEW-DISCARD` (rows thrown away, with the reason), `VIEW-BACKUP` (file copied: seq, bytes, name),
`VIEW-BACKUP-FAILED` (see below), `VIEW-START-AT-LOG-START` (a view with nothing to resume from starts at the log start),
`VIEW-CHECKPOINT-TORN-TAIL` and `VIEW-CHECKPOINT-TAIL-TRIMMED` (see below). Per-view counters: `checkpointDiscards`,
`viewBackupFailures`, `checkpointsAheadOfLog`.

**A failed backup.** If the copy fails (disk full, I/O error) the broker logs `VIEW-BACKUP-FAILED` with the errno and does not
rebuild over the unsaved file: the view stays `NotReady` for that boot. Free space or fix the disk and restart the node.

**Disk sizing.** Budget up to `RUNE_VIEW_BACKUPS_KEPT` (default 3) copies of each view partition's checkpoint file, kept until
newer backups push them out.

**Torn tail.** A crash can leave a half-written last record. The broker finds it by CRC, logs `VIEW-CHECKPOINT-TORN-TAIL`, cuts
the file back to the last whole record (`VIEW-CHECKPOINT-TAIL-TRIMMED`) and carries on. No rebuild and no backup happen for
this. Damage anywhere else is `unreadable`.

**Restoring a backup** (for example when the discard was a false alarm). Only some backups are safe to put back:

| Reason | Restore as `committed.delta`? | Why |
|---|---|---|
| `legacy` | **Never** | old format; may hold rows of aborted transactions, which it would serve as committed |
| `legacy-superseded` | **Never** | the same file kind; the committed file next to it is the truth |
| `unreadable` | Only after upgrading to a broker that fixes the cause | a still-damaged file is backed up and rebuilt again; a false alarm (a decode bug) loads once fixed |
| `ahead-of-log` | **Never** | the log lost the records those rows came from; it is discarded again, or, once the log grows past it, serves rows the log does not hold |
| `ahead-of-verdicts` | **Never** | its rows were judged by markers the log no longer holds |
| `below-log-start` | **Never** | the records needed to complete it are gone; it is discarded again |

1. Stop the node.
2. Pick an `unreadable` backup in `__view_snapshots/backup/` by view, partition and seq (the highest seq is the newest).
3. Move the current `view-<id>-p<n>.committed.delta` aside.
4. Copy the backup to `view-<id>-p<n>.committed.delta`.
5. Start the node. It validates the file again; if it is still not usable it is backed up and rebuilt as before.

A whole-directory restore (see Backups and disaster recovery) is the way to get back state the log no longer holds.

## Verdict sources

- A partition whose latest constraint set is non-empty is a verdict source. Its leader judges every record once, in offset
  order, and writes a **verdict marker** (control record 17) after a rejection and otherwise at most every
  `RUNE_VERDICT_INTERVAL_MS` (default 100 ms). **W** is the end of the committed markers; the cleaner never cleans at or above W and
  removes a marker only after a later marker supersedes it and every replica fetched it.
- **Verdict lag** (judgedThrough behind the log end) delays delivery and `View` acks, not `acks=all` for writers that did not
  ask for a verdict. Delivery stops at judgedThrough while a constraint view builds. A judge waits (`judgeWaitsForInstall`)
  while a member of the logged set is not installed on its worker.
- After a crash that lost the marker tail a verdict view backs up and rebuilds a checkpoint ahead of its local markers
  (`verdictCheckpointsDiscarded`). When the rejected-offset index is full the judge stalls and raises `verdictStalls`.
- **Transactions.** A transaction that writes to a verdict source is judged at a `TxnJudge` record (control record 19) that
  the leader appends when the coordinator asks for its vote. The leader's cleaner removes a `TxnJudge` once the transaction's
  COMMIT or ABORT lies below W and every persisted constraint view has checkpointed past it. Only the partition's leader
  compacts: a follower keeps what it replicated, these included, until it leads (follower compaction: #990). A follower does
  delete whole sealed segments that lie below its leader's log start, so retention on the leader bounds a follower's disk
  too. A node restart does not change this.
  - **Votes.** The coordinator asks the leader of every partition the transaction wrote to whether it votes yes. The judge answers
    once it has judged the transaction whole, applying its records provisionally and locking the rows it wrote; the commit or
    abort marker then finalizes the result in one step. Counters are ids 11 to 20 of `rune stats topic <t>`: `txnVotesYes`,
    `txnVotesNo`, one counter each for 55, 56, 57 and 62 (`TxnConflict` no votes count only in `txnVotesNo`), `txnVoteLatencyNsMax` and `txnVoteLatencyNsTotal`.
  - **Lock pause.** While a transaction holds a row locked, the verdicts of other writers' records that touch the row wait for its
    marker; the time is `foldPausedOnTxnLockNs`. A distributed lock wait ends at the transaction timeout at the latest, when the
    coordinator aborts the transaction. A judgement never waits for another transaction's marker to free an overlay: the worker adds
    one when every overlay holds a lock, and drops it again once it is free and the pool has not grown for 10 s.
  - **`txnMarkerWithoutVote`** counts markers that arrived for a transaction nobody asked a vote for. It stays 0; a non-zero
    value is a coordinator bug.
  - **Size.** A transaction over `RUNE_TXN_JUDGED_RECORDS_MAX` records on one partition is voted `TxnTooLarge` (62); an atomic batch
    over it to a verdict source is refused with `InvalidRequest` (39), see [configuration](configuration.md).
  - **Persisted constrained views.** A persisted view of a verdict source checkpoints while the judge runs ahead of undecided
    transactions, and a restart backfills from the `TxnJudge` records at or above its checkpoint.
- **Constraint views** (`unique` and `check`, see [Constraints](api/ql.md#constraints)) join and leave the set through
  change records in the source log, so every leader judges a record with the set that was active at its offset.
  - **Create.** The leader appends the view's plan (control record 20) and then the change record at offset `A`. The view builds
    from the log start: below `A` it folds records without judging them (data from before the constraint), from `A` on they are
    judged. Delivery of records at or above `A` waits until the view has caught up, so creating a constraint on a large topic pauses
    delivery of new records for the build. Writes are not refused.
  - **Replace.** The old view keeps judging while the new one builds. The leader then appends the change record at `S` and swaps the two
    in one step, so no record goes unjudged.
  - **Drop.** The leader appends a change record without the view. The dropped plan stays installed until the committed watermark
    W has passed that record, because a failover can still make a new leader judge the records below it. Drop followed by create
    leaves a gap in which writes are not judged.
  - **Plan records.** The cleaner keeps control record 20; they are small and one is written per joining member. A leader whose catalog has not installed a view of the logged set waits (`judgeWaitsForInstall`) rather than
    judge without it.
  - **`uniqueConflicts`** (view stat 32, `rune stats view <unique view>`) counts the extra claimants of a `unique` view's keys: live
    source records that share a key with its owner, from data written before the view existed. It rises while the view builds, from 0 to the duplicate count; once the view has built it only falls, as owners are deleted or
    renamed and the next claimant is promoted. Remove duplicates before a back-fill.

## Key index

- A typed topic created or adopted with `key.index` keeps, per partition, an in-memory index of its live keys (about
  25-40 bytes per key). It judges write intents (`insert` fails with `KeyExists` 56 on a stored key, `update` and
  `removeExisting` with `KeyNotFound` 57 on an absent one) and makes the partition a verdict source; plain upserts and
  deletes are folded into it and never rejected. It is charged to the topic's namespace.
- The index is never checkpointed. It is rebuilt from the local log on each new leadership and each restart, at most
  65,536 records per worker pass. While it rebuilds, delivery and `View` acks wait; intents are accepted and judged once
  it is done. A key whose tombstone the cleaner already removed is gone from the rebuilt index, as from the log.
- Stats per partition: `keyIndexRebuilds` (not counting the first build after a create or restart; an index adopted while
  the partition already leads is rebuilt, and counted, at its next leadership), the rejections `rejectionsKeyExists` (56) and `rejectionsKeyNotFound` (57),
  both with constraint id 0, and `keyIndexSkippedMalformed` (records whose key is not the key field's width, written
  before an adoption).
- The key field must be at most 8 bytes wide until wide keys ship; a wider one is refused at DDL ("key.index needs a key
  of at most 8 bytes"). `key.index` needs every broker at feature level 5.

## Autoincrement

- **Reservations.** The leader of a partition of an `autoincrement` topic reserves ids in the log: before it hands out an id
  above the reserved `n`, it appends a reservation record (control record 16) that reserves a block of `autoincrement.reserve`
  more (4096 by default). A new leader, and every restart, reads the highest reservation and starts above it. The ids that
  were reserved but not stored are never handed out again: a failover leaves a gap of up to one block, counted in
  `idsBurned`. So do a rejected record and an aborted transaction. An import above the counter moves it, and reserves
  through its id.
- **Retries.** The leader keeps the ids of the last 32 key-0 batches of each idempotent producer, so a retried batch is
  answered with its original ids. A new leader rebuilds this side window from the log during its leadership scan; until
  the scan ends a create is answered `NotLeader` and retried by the client. The window starts at 1024 producers and doubles,
  from a cold step that runs about every 1,024 passes and whenever it is 75 % full, up to 1,048,576. While it is full, a
  create from a producer it does not hold is answered `NotLeader`.
- **The cleaner.** `cleanup.policy = compact` with no retention: the log start never moves and compaction removes no live
  id. The cleaner removes every reservation record except the latest. It copies the last 32 idempotent data batches of each
  live producer whole, so a retry after a failover still finds its ids; a deleted id's tombstone is kept
  while such a batch holds that id. A producer that sent nothing for `RUNE_PRODUCER_ID_EXPIRATION_MS` (by the segment
  append time on the broker, not the client's timestamps) is no longer protected. Keep that setting above the clients' delivery timeout, or a retry whose ack was lost can be stored a second time. The memory budget for these batches is
  the producer-tails capacity times about 1.3 KB; past it the cleaner logs once "the producer batch window ... is over its
  budget" and protects only each producer's last batch, so a late retry may complete `StoredIdUnknown`. Raise
  `RUNE_CLEANER_DEDUPE_BUFFER_BYTES` to avoid it. While a live producer keeps writing, the partition's cleaned-through
  offset (`lastCleanOffset`) stays at the oldest batch the cleaner still protects for that producer; this is by design, and
  compaction above it goes on. It moves forward once the producer's window moves past that batch or the producer expires.
- **Stats** per partition: `idsBurned`, `reservationsAppended`, `keyZeroBatches`, `importsMovedCounter`,
  `windowFullRefusals`, `windowMisses` and `assignedKeyExists`; the checked-writes stats (C16) serve the counter, the
  reserved-through id and `idsBurned` as ids 8 to 10, the first two only on a leader whose scan has finished.
- **Alarms.** `windowMisses` counts a retried key-0 batch the window could not answer (the client sees `StoredIdUnknown`); it
  should stay 0. `assignedKeyExists` counts an assigned id the key index already held and logs `AUTOINC-ID-REUSE` at critical
  level with the topic, partition and id: the counter handed out an id twice. Both are asserted 0 by the tests. A
  `KeyExists` on an import below the counter is the expected answer and is never counted.

## Upgrades

- Upgrade **brokers before clients**: a new client defining a transient topic against an older broker silently gets a
  disk topic.
- A client older than version 9 is refused at the connect on an authenticated cluster; a client older than version 8 cannot
  follow a DDL redirect.
- **View reads during a rolling upgrade.** A broker serves view rows only under the controller's read lease, and a lease
  needs a majority of the members to echo the controller's grants. Until the controller **and a majority of the members**
  run a build that has the lease, no lease is granted and `getViewRow` answers `NotLeader`.
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
