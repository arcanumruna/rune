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
practice; on a bridge, publish the client port and the node's shard block `8200 + 192n` to `8200 + 192n + 191`; the response-control ports sit inside that block), and a
file-descriptor hard limit that covers the partitions (the broker raises its soft limit to the hard limit and refuses to start when the hard
limit is below the estimated need; see SELF-HOSTING, "Open-file limit").

Firewall: allow inbound UDP on the client port, each worker's ingress port and the response-control ports (gateway and
workers). The per-worker egress control ports are needed only when authentication is off.

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
| `max-disk-bytes` | measured on the node that leads the partition | produce on the leader: `NamespaceLimitExceeded (52)`; follower apply, replication catch-up and the log cleaner are never refused |
| `max-message-bytes`, `max-batch-bytes` | the worker's produce intake on the leader. A batch longer than `max-batch-bytes`, or a record whose length field (its body bytes) is longer than `max-message-bytes`, is refused and nothing is appended. The record fields are checked against that length field; a batch whose records do not match their length fields or the header's record count, or that has trailing bytes, is malformed and refused. 0 = unlimited | `NamespaceLimitExceeded (52)`; a malformed batch: `InvalidRequest (39)` |
| `max-retention-ms`, `max-retention-bytes` | a topic DDL above the maximum is refused; the cleaner applies `min(topic retention, namespace maximum)` | `NamespaceLimitExceeded (52)` at DDL |
| `produce-bytes-per-sec`, `produce-records-per-sec`, `fetch-bytes-per-sec`, `request-ops-per-sec`, `query-cycles-per-sec` | a per-namespace aggregator compares the node's usage with `limit x elapsed` every `RUNE_NS_LIMIT_TICK_US` (default 10000) | `Throttled (53)` carrying a retry-after; the client library waits it out and retries |
| `max-view-memory-bytes` | the node's view chunks | the namespace's views pause at their offset and a view read answers `ViewPaused (54)`; they resume when memory falls or the limit is raised |

- **Lowering a limit deletes nothing**, except the retention maximum: objects over a lowered count stay and the next
  create is refused; a lowered retention maximum deletes old segments on the next retention pass and the SetLimits is answered Ok with the warning `retention clamped on N topics` in the
  DDL response's detail.
- Rates, disk and view memory are enforced from the workers' paced reports, so a namespace can exceed one by what its
  node produces in about one report interval plus one aggregator tick before the throttle arrives; the rate debt carries
  the excess forward and the long-run rate converges on the limit. A rate limit never makes a follower refuse a replicated
  batch, so `acks=all` never stalls on it.
- `rune namespace usage <name>` prints each limit next to its current usage on the node it asks (counts, disk, view
  memory, connections and rates; fields with no usage meaning print `-`).
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
| 1000228 | `rune-auth-mode` | gauge | control | `0` authentication enabled, `1` dev mode. Alert on `1` in production. |
| 1000229 | `rune-logins-ok` | counter | control | Successful logins. |
| 1000230 | `rune-logins-failed` | counter | control | Failed logins. Alert on a rising rate. |
| 1000231 | `rune-logins-rate-limited` | counter | control | Login attempts refused by the login throttle (per user and source). |
| 1000232 | `rune-sessions-revoked` | counter | control | Sessions ended by drop, disable or `TERMINATE SESSIONS`. |
| 1000233 | `rune-peer-links-established` | gauge | control | Peers whose link is currently established. |
| 1000234 | `rune-peer-link-refusals` | counter | control | Peer-link refusals logged by this broker. Alert on a rising rate. |
| 1000235 | `rune-peer-frame-drops` | counter | control | Inter-broker frames dropped for want of a link or because they did not open. |
| 1000240 | `rune-txn-offset-commits-stuck` | gauge | control | Transaction commits waiting because the group coordinator refuses their consumer-group offsets. Alert on a non-zero value that persists. |
| 1000241 | `rune-txn-offset-stuck-age-ms` | gauge | control | Age of the oldest of those, in ms. A WARN names it every 30 s while any is stuck. |
| 1000242 | `rune-ns-slot-mismatch` | counter | control | Namespace slot tags refused by the gateway or a worker (`rune.ns.slot_mismatch`): a message or request whose (slot, nsId) pair the receiver's slot table does not hold. Any rise is a bug. |

Per-namespace metrics (one series per namespace on this node, allocated when the namespace is created and freed when it is
erased; the key is the namespace id and the slot's owner tag is the namespace id too):

| Type id | Label | Kind | Owner | Meaning |
|---|---|---|---|---|
| 1000300 | `rune-ns-slot` | gauge | control | The node-local slot of the namespace (`default` is slot 0). The slot is the index of every per-namespace array on the hot path. |

The owner is the one thread that writes the metric: `control` the broker's control agent, `observer` the role and ISR
observer, `provisioner` the segment provisioner. Type id 1000204 (a metadata commit position that was never published) has no row, and
1000236 to 1000239 are reserved. The table is `kRuneMetrics` in `broker/libs/node/include/rune/node/RuneMetrics.h`; a
regression test (`BrokerMetricsRegression.OperationsDocTableMatchesRegression`) keeps this table equal to it. Logins are
metrics and structured logs, not `__security` events.

### Process metrics

The control agent samples `/proc/self` every `RUNE_METRICS_SYSTEM_INTERVAL_MS` (default 1000) and writes:

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
