# Self-Hosting Rune Broker

Each Rune node runs an **embedded Aeron media driver in-process** — there is no separate
driver to launch. A node needs a sized `/dev/shm`, a writable data dir, and a peer list.
All inter-node (consensus) and client traffic is `aeron:udp`.

## Prerequisites

- Docker (with BuildKit).
- The sibling `rune-protocol` repo checked out next to `rune-broker` — its headers are
  supplied at build time via a BuildKit named context (Phase-1 sibling-include).

## Build the image

Run from the `rune-broker` repo root:

```sh
docker build \
  --platform linux/amd64 \
  --build-context rune-protocol=../rune-protocol \
  -f docker/Dockerfile \
  -t rune-broker:latest \
  .
```

> `--platform linux/amd64` matters only on Apple Silicon / non-amd64 hosts (the builder
> uses an x86_64 toolchain; Docker Desktop runs it under Rosetta). It's a no-op on amd64 Linux.

## Configuration reference (all environment variables)

These are **every** variable the broker reads (from `libs/node/src/NodeConfig.cpp`). There
are no others — the process exits non-zero with a clear message if a required one is missing
or malformed.

| Variable | Required | Default | Type | Description |
|---|---|---|---|---|
| `RUNE_NODE_ID` | **yes** | — | int | This node's id (0-based). Must be one of the ids in `RUNE_PEERS`. |
| `RUNE_PEERS` | **yes** | — | list | Comma-separated `id@host:consensusPort` for **all** nodes incl. self. Ids unique; must contain `RUNE_NODE_ID`; `host:port` is **raw** (broker builds `aeron:udp?endpoint=…`). Whitespace trimmed; trailing/empty entries rejected. Single-node: just `0@127.0.0.1:40111`. |
| `RUNE_CLIENT_ENDPOINT` | **yes** | — | host:port | **Raw `host:port`** the client gateway listens on (broker prepends `aeron:udp?endpoint=`). Use `0.0.0.0:40200` to bind all interfaces. Do **not** pass a full `aeron:udp?...` string. |
| `RUNE_DATA_DIR` | **yes** | — | path | Writable dir for durable state (node-state, term log, partition logs). Mount a volume/bind-mount here. (Compose files set it to `/var/lib/rune`.) |
| `RUNE_CLIENT_STREAM_ID` | no | `1` | int32 | Aeron stream id for client traffic. |
| `RUNE_CONSENSUS_STREAM_ID` | no | `10` | int32 | Aeron stream id for inter-node consensus traffic. |
| `RUNE_ELECTION_TIMEOUT_MS` | no | `150` | int64 ms | Max wait per election round. Raise for slow / cross-datacenter links. |
| `RUNE_ELECTION_STATUS_INTERVAL_MS` | no | `20` | int64 ms | Canvass / heartbeat cadence. |
| `RUNE_STARTUP_CANVASS_TIMEOUT_MS` | no | `100` | int64 ms | Extended canvass window on first boot (peer discovery). |
| `RUNE_TRANSIENT_LOG_RING_BYTES` | no | `8388608` (8 MiB) | int32 bytes | Node-wide ring size of every transient partition. Must be a power of two; any other value keeps the default. `RUNE_MEMORY_LOG_RING_BYTES` is a **deprecated** alias that logs one warning; when both are set, the canonical name wins. |

> Not env-configurable (yet): dynamic membership.
> `/dev/shm` size and published ports are **container** settings (`--shm-size`, `-p`), not broker env.

> **Format gotcha:** `RUNE_CLIENT_ENDPOINT` and the `host:port` in `RUNE_PEERS` are **raw**
> (e.g. `0.0.0.0:40200`, `rune-1:40111`) — the broker builds the `aeron:udp?endpoint=…`
> channel itself. Do **not** pass a full `aeron:udp?...` string.

> **`/dev/shm`:** the embedded media driver maps its CnC + log buffers into shared memory.
> The default container 64 MB is too small — always pass `--shm-size=256m` (or set
> `shm_size` in compose).

### Topics are created by DDL

A broker starts with no topics. Every topic is created through the catalog, with Rune QL DDL, once a
catalog leader is up and before anything produces or subscribes:

```bash
rune ql -e "topic events partitions 3;" --broker rune-1:40200
rune ql -e "transient topic presence partitions 1;" --broker rune-1:40200
rune ql -f topics.rql --request-id 0123456789abcdef0123456789abcd00 --broker rune-1:40200
```

`-f` runs the statements in order and stops at the first error, naming its statement. A fixed
`--request-id` makes a re-run return the original results instead of `TopicExists`. A topic's id is
assigned in creation order, from 0. Settings go in a `with (...)` clause (for example
`with (tier = leader)`), and `replicas N` sets the replication factor. The client's `defineTopic`
does the same from code. The docker e2e scripts create their topics with `docker/create-topics.sh`.

### Topic and view names

One rule applies to topic and view names in Rune QL, the client's `defineTopic` and `createView`:

- 1 to 249 bytes of ASCII letters, digits, `.`, `_` and `-`, and not `.` or `..`.
- Names starting with `__` are reserved for internal topics.
- Names are case-sensitive, but two names that differ only by ASCII letter case are not allowed,
  because they would share one data directory on a case-insensitive filesystem.
- A bad name in DDL is rejected with error 40 (`InvalidTopicName`). A name that differs from an
  existing topic or view only by case is rejected with error 25 (`TopicExists`). A partition count
  below 1 is rejected with error 23 (`InvalidSchema`).
- DDL is serialized: one create or alter runs at a time, so of two colliding requests exactly one
  wins. If the leader is demoted, the request in flight and every queued request are answered
  error 6 (`Internal`); retry on the new leader. A request answered 6 after its record was
  written is *in doubt*: it may still take effect, and does at the latest after a restart.
- A runtime topic serves only once its definition commits. Until then it is not listed in
  metadata, and produce and subscribe to it are answered error 2 (`NotLeader`, retriable: the
  client refreshes metadata and retries). The `defineTopic` / `createView` answer is sent only
  when the topic serves, so the first produce after it succeeds. If the topic still does not
  serve 5 s after its definition committed, the request is answered 6 (`Internal`, in doubt)
  instead of OK, and an error line is logged.
- `defineTopic` over a view's output topic is rejected with error 25 (`TopicExists`), also when
  that view's `createView` failed or is in doubt. A `createView` retry with the same name and
  definition reuses its own failed attempt's output topic, which is always empty because nothing
  can write to it before its definition commits.

### Data directory clean-up at boot

`__catalog` is the single source of truth for which topics exist. At boot the broker replays its
local copy of `__catalog` once, before it builds anything, and builds every topic, schema and view
from that replay. The old `dataDir/__topic_schemas` and `dataDir/__views` directories are never
read and never changed (there is no migration); a warning names them if they hold data.

- At boot the broker reconciles `dataDir/topics/` with the catalog. A directory no live topic
  claims is **moved, never deleted**, to `dataDir/quarantine/<name>-<unix-timestamp>/`, and a
  warning names both paths. A name matches exactly or ignoring ASCII letter case. This covers a
  dropped topic whose directory was not purged, a misspelt name, and a directory left by another build.
- `dataDir/dropped/<id>.<name>` holds the data of a dropped topic until its purge finishes. At boot
  the broker finishes any purge that a crash interrupted, but only for a topic id the catalog marks
  dropped. Any other entry is left alone and a warning names it.
- Reconcile only considers direct children of `dataDir/topics/` laid out as
  `<name>/<partition number>/<files>`. It never follows a symlink, and it leaves anything else
  alone. A crash during reconcile is safe: the next boot finishes the same moves.
- **A boot whose catalog replay halts moves, renames, deletes and quarantines nothing.** The
  broker boots with the catalog it had already applied, accepts no catalog change, and logs the
  halt at critical level. Repair the catalog, then restart.
- **A broker whose local `__catalog` is empty or missing while `dataDir/topics/` holds
  catalog-made topics** (a topic directory with a `topic-id` marker; the catalog was lost, or the
  disk is new) touches no topic directory at boot. It writes `dataDir/.catalog-catchup` (`pending`).
  - As a **follower** of a multi-broker cluster it catches up to the catalog leader's committed
    history, serves the topics that history names over their existing directories, and writes
    `caughtup <offset>` into the file. The directories no topic claims are quarantined at the
    **next boot**, after which the file is removed. A crash during catch-up leaves `pending`: the
    next boot touches nothing.
  - A **single-node broker, or any broker that leads `__catalog` before it caught up,** has no
    history to catch up to. It keeps every topic directory in place and serves none of those topics
    (they answer `UnknownTopic`). It refuses every DDL with error 6 (`Internal`), counted by
    `ddlRefusedUntrustedLeader` and logged at critical level, until `__catalog` is restored from a
    backup. Do not delete `dataDir/__catalog` to repair a broker: restore it.
- **A catalog topic whose directory belongs to another incarnation is never opened.** Each catalog
  topic directory holds `topic-id`, the id of the topic that made it. If it names another id, the
  topic is not built: on a halted or still-catching-up boot this is logged at critical level and the
  broker keeps starting without that topic. The next healthy boot moves a directory marked with a
  dropped id to `dataDir/dropped/` and purges it, and quarantines any other mismatch.
- **Upgrade:** a catalog topic directory that holds records but has no `topic-id` marker (made by a
  build before markers) is quarantined at the first boot. Its data is never deleted.
- A boot refuses to start, with an error that names both offsets, when `dataDir/__catalog/applied`
  (the last applied offset) is at or past the end of the local `__catalog` log: events this broker
  already applied are missing, and replaying a shorter history would fork its catalog. Restore the
  data directory from a backup. A corrupt `applied` file refuses to start too. A missing one means
  nothing was applied yet.
- Delete quarantined directories yourself once you no longer need them; the broker never
  deletes from `dataDir/quarantine/`.

### Transient topics

A **transient** topic (`transient topic …;` in Rune QL,
or `Persistence::Transient` on the client's `defineTopic`) is replicated to the in-sync replicas
before a produce is acknowledged but is **never written to disk**.

- Its *metadata* survives restarts: a runtime-defined transient topic is recreated as transient on boot.
- Its *data* does not: a restarted node rejoins with the topic empty; after a **full-cluster**
  restart the topic is empty. Offsets restart from 0.
- Persistence is fixed at definition: `alter topic` cannot change it.
- An explicit `transient topic X` over an existing disk topic `X` answers
  `TopicExists` (error 25); the topic stays a disk topic.
- A view that reads from a transient topic must itself be transient. `Client::createView(...,
  persistence)` (the `CreateView` persistence byte) with a persisted view over a transient source is
  rejected with error 37 (`ViewSourceTransient`); use a transient view. Rune QL `create view`
  arrives later.
- `alter topic` naming a view is rejected with error 38 (`ViewNotAlterable`).
- A view cannot read another view's output yet: `Client::createView(...)` over a view is rejected
  with error 41 (`ViewSourceIsView`). Before this release such a view was accepted but stayed
  empty.
- Renaming a topic's key field with `alter topic … rename` moves the topic's key to the new name.
- Use `durability=majority` (default) or `all-in-sync` if a transient topic must survive a node death.
- Produce (every `acks` mode, idempotent and transactional producers), push subscriptions from
  earliest / latest / an offset, consumer groups and `read_committed` work on transient topics in both
  worker modes (`RUNE_WORKER_MODE=shard`, the default, and `legacy`). `tier=strict` acks like
  `majority` (there is nothing to fsync). They differ from disk topics in these ways:
  - **Fewer in-sync replicas than `minIsr` stops all produce once the ring is full.** While the ISR is
    below `minIsr` the high watermark does not advance, so nothing can be evicted. When the ring fills
    up, every produce is refused with error 42, including `acks=0` and `acks=1` (a disk topic keeps
    accepting `acks=1`). The cause is replicas missing from the ISR, not slow followers: bring the
    missing replicas back.
  - **`read_committed` pauses briefly after a leadership or ownership move.** The new owner rebuilds
    the read-committed state from the ring, a fixed amount per pass (about 6 ms of work for a full
    8 MiB ring, spread over a few hundred passes). Meanwhile `read_committed` reads wait, and
    idempotent and transactional produces get `NotLeader` (retriable). If the ring has evicted the
    first records of a transaction that is still open, `read_committed` consumers stay at the ring's
    oldest record until that transaction's commit or abort marker lands.
  - **Committed consumer-group offsets of a transient partition do not outlive its data.** Each
    transient partition has a data lineage, recorded in the cluster metadata. A new lineage starts
    only when a leader takes over with no copy of the data: after a full-cluster restart, after a
    single-node broker restarts, and at the partition's first leadership. A leader move that keeps
    the data keeps the lineage: a failover, a controlled shutdown, a rolling restart. After a new
    lineage starts, an offset committed before it is answered as "no committed offset", so the
    group starts that partition at its log start (the oldest record the ring still holds, whatever
    the consumer's reset setting) and reads every refilled record. For a moment after a restart
    or a leader move, until the new leadership has confirmed the lineage, OffsetFetch and
    OffsetCommit for that partition answer `NotLeader`. The client retries this automatically.
  - **Committed offsets are answered only once loaded.** After a restart, or when the group
    coordinator moves to another node, the coordinator replays its whole `__consumer_offsets`
    partition before it answers. Until then OffsetFetch and OffsetCommit answer `NotLeader` for
    every topic, disk or transient, and the client retries. A group never resumes from a guessed
    position.
  - **A new lineage rebalances the groups that read the old one.** When a transient partition
    starts a new lineage, every consumer group with a committed offset on it is rebalanced. Its
    members re-join, fetch "no committed offset" and start at the log start. A commit from before
    that rebalance is refused with `IllegalGeneration`, and the client re-joins. This holds even if
    a member fetched the old offset in the moment between a crash and its detection. A node that the
    failure detector finds dead no longer vouches for a lineage.
  - Topics take an optional replication factor: `topic name partitions N replicas R;` (default: every
    node). A transient topic with `rf=1` loses its data whenever its node stops. `replicas` must be at most
    the node count and at least `RUNE_MIN_ISR`; anything else is answered `InvalidRequest` with a
    detail that names the constraint.
  - **Subscriptions follow a leader move.** When a node stops serving a subscription (it lost
    leadership of a `read_committed` partition, or the partition's replica moved off it), it answers
    the subscription instead of going silent. A client of this release resumes from its position on
    the new leader, on that leader's own session, so its other subscriptions are untouched; records
    already delivered are not delivered again. Older clients do not resume: their subscription ends,
    and the application must re-subscribe.
    A new subscription that a node redirects to the partition's leader is also made on that leader's
    own session; the client's other subscriptions stay where they are.
  - **A plain subscription never carries its position into new data.** For a transient partition the
    broker tells the client which data lineage its subscription reads. When the client resumes a
    subscription after losing that node's session, it names that lineage. If the partition's node
    restarted empty meanwhile, the broker answers `PartitionDataReset`. The client restarts the
    subscription at the log start and counts the reset (`Subscription::dataResets()`), so the
    application knows records it had not yet read are gone. Records are never skipped silently.
- A transient topic defined at runtime serves only once its definition has committed, exactly like a
  disk topic: until then produce and subscribe answer `NotLeader` (retriable) and metadata does not list it.
- **Retention is the in-memory ring:** `RUNE_TRANSIENT_LOG_RING_BYTES` per partition (node-wide, default
  8 MiB, at most 1 000 000 batches). When it is full the oldest **committed** records (below the high
  watermark) are evicted. A consumer whose position was evicted resumes at the oldest record still held
  (counted and logged as a reader that fell behind). Subscribing below the oldest record (for example
  from earliest) is not counted that way: the subscription simply starts at the oldest record. Records
  not yet replicated are never evicted: while the ring is full of them a produce is refused with error
  42 (`TransientLogFull`, retriable — the client retries after a 100 ms backoff without refreshing
  metadata; clients older than this release see `NotLeader`) until the followers catch up. A batch
  larger than the whole ring is refused `MessageTooLarge`.
- A node that rejoins a transient partition whose leader already evicted the oldest records restarts
  its copy at the leader's oldest record and catches up from there.
- The ring size is set per node, so keep it the same on every node. A follower whose ring is smaller
  than a batch it must replicate stops at that batch (logged as `follower WEDGED`) and leaves the ISR;
  it does not skip the batch.
- A transient view over a transient topic re-folds when the topic's ring changes under it (counted as
  `transientSourceResets`): from the point where the ring's content changed, when it changed once since
  the view last folded; from the oldest held record when the ring was recreated or changed more than
  once. Records evicted before the view folded them are counted as `transientRecordsEvicted`.
- **Upgrade brokers before clients.** A new client defining a transient topic against an older
  broker silently gets a disk topic, because the older broker ignores the persistence byte.

---

## Single node

A single-node cluster **self-elects itself leader** on boot. Good for local dev and
single-box deployments.

```sh
docker run --rm \
  --platform linux/amd64 \
  --shm-size=256m \
  -e RUNE_NODE_ID=0 \
  -e RUNE_PEERS='0@127.0.0.1:40111' \
  -e RUNE_CLIENT_ENDPOINT='0.0.0.0:40200' \
  -e RUNE_DATA_DIR=/var/lib/rune \
  -v rune-data:/var/lib/rune \
  rune-broker:latest
```

Expected within a few seconds:

```
rune-broker: node 0 is LEADER
rune-broker: running — send SIGINT or SIGTERM to stop
```

Stop with `docker stop <id>` (SIGTERM → clean shutdown). The named volume `rune-data`
persists the node's data dir across restarts.

To reach it from a client outside Docker, publish the client UDP port and set the
endpoint to a routable address, e.g. `-p 40200:40200/udp` with
`RUNE_CLIENT_ENDPOINT='0.0.0.0:40200'`.

---

## Three nodes (clustered)

The three nodes elect **one leader** over `aeron:udp`; they must share a network so the
peer DNS names resolve. The repo ships `docker/docker-compose.yml` wired for exactly this.

```sh
# from the rune-broker repo root
docker compose -f docker/docker-compose.yml up -d rune-1 rune-2 rune-3
docker compose -f docker/docker-compose.yml logs -f
```

Watch for exactly one node to log `is LEADER` within ~10 s.

Each node in the compose is identical except `RUNE_NODE_ID`; they share one `RUNE_PEERS`
membership list, `shm_size: 256m`, and a per-node named volume:

```yaml
# sketch — see docker/docker-compose.yml for the full file
x-broker-env: &broker-env
  RUNE_PEERS: "0@rune-1:40111,1@rune-2:40111,2@rune-3:40111"   # identical on every node
  RUNE_CLIENT_ENDPOINT: "0.0.0.0:40200"                        # raw host:port
  RUNE_DATA_DIR: /var/lib/rune

services:
  rune-1:
    build: { context: .., dockerfile: docker/Dockerfile,
             additional_contexts: { rune-protocol: ../../rune-protocol } }
    platform: linux/amd64
    hostname: rune-1
    shm_size: "256m"
    environment: { <<: *broker-env, RUNE_NODE_ID: "0" }
    volumes: [ "rune-data-1:/var/lib/rune" ]
  rune-2:   # RUNE_NODE_ID: "1", hostname rune-2, volume rune-data-2
  rune-3:   # RUNE_NODE_ID: "2", hostname rune-3, volume rune-data-3
```

Tear down (including volumes): `docker compose -f docker/docker-compose.yml down -v`.

### Key points for a real cluster

- **`RUNE_PEERS` is identical on every node** (the full membership list); only
  `RUNE_NODE_ID` differs.
- **Consensus port** (`40111`) is the `host:port` in `RUNE_PEERS`; the **client port**
  (`40200`) is separate (`RUNE_CLIENT_ENDPOINT`).
- Peer addresses are **DNS names on the shared compose network** (`rune-1`, `rune-2`,
  `rune-3`) — the default Docker bridge has no multicast, so consensus uses `aeron:udp`
  unicast to those names.
- **Static membership** — the peer list is fixed at boot (dynamic membership is a later
  feature).
- Clients connect to **any** node's client endpoint; killing the leader triggers automatic
  re-election among the survivors.
- An `acks=all` produce answered `NotLeader` (2) or `Internal` (6), or not answered at all,
  is *in doubt*: the batch may already be in the log and can still commit — for example
  when leadership moves back to a replica that holds it.  Retry it with the same producer
  id, epoch and sequence (the Rune client does); the broker answers the retry with the
  original offset.  A produce without a producer id (`producerId -1`) retried after such an
  answer can be stored twice.

### Open-file limit (`RLIMIT_NOFILE`)

Every partition log keeps files open for its whole life: up to 4 per open segment (the
`.log` stream and fd, the `.index` and read mappings) plus 2 for the durable recovery
point (its checkpoint file and the log directory).  At startup the broker **raises its
soft limit to the hard limit** and logs both, then **refuses to start** when the hard
limit is below the estimated need — so a too-low limit fails at boot instead of halting
the broker mid-run on `EMFILE` (and again at every restart).  It warns when the hard limit
is within 25% of the need.

Estimate: `1024 + 64 × 18 (internal topics) + Σ topics partitionCount × (openSegments × 4 + 2)`,
where `openSegments = ceil(retentionBytes / segmentRollBytes) + 1` for size-bounded
retention and `4` otherwise.  Every partition is assumed hosted locally.

Set the hard limit high:

- **systemd:** `LimitNOFILE=1048576` in the unit's `[Service]` section.
- **Docker:** `docker run --ulimit nofile=1048576:1048576 …`, or in compose
  `ulimits: { nofile: { soft: 1048576, hard: 1048576 } }` (most engines already default to ~1M).
- **Kubernetes:** inherits the container runtime's limit (containerd/CRI-O default ~1M).
- **Shell:** `ulimit -Hn` shows the hard limit; Linux's default soft limit (1024) is
  exhausted by ~300 partitions.

### Verify the cluster (optional)

The repo ships an end-to-end script that builds + starts the cluster, confirms a single
leader, kills that leader, and asserts re-election:

```sh
bash docker/e2e.sh
```

---

## Single node vs. three nodes — which to run

| | Single node | Three nodes |
|---|---|---|
| Command | `docker run …` (above) | `docker compose … up -d rune-1 rune-2 rune-3` |
| Leadership | self-elects | elects one leader via consensus |
| Fault tolerance | none (single point of failure) | survives 1 node loss (2/3 quorum) |
| Use for | local dev, single-box | production / HA |

Both use the **same image** — clustering is purely a matter of `RUNE_NODE_ID` +
`RUNE_PEERS` + a shared network.

**Avoid two-node clusters.** A 2-node cluster needs **both** nodes for a
quorum (`floor(2/2)+1 = 2`), so it tolerates no node loss.  When one node stops
reporting, the consensus leader steps down after the leader heartbeat timeout
(`max(election timeout, 3 × status interval)`, 150 ms by default) — a minority
never leads — and the survivor stays without a consensus leader (no controller
decisions: failover, ISR changes, topic creation) until the other node returns.  This is Raft / Aeron behaviour, not a
fault.  Run 1 node (dev) or an odd count ≥ 3 (HA).

---

## Customizable 3-node cluster (parameterized)

The repo's `docker/docker-compose.yml` is tuned for the CI/E2E test (YAML anchors, named
volumes, no host ports, `build:` from source). For your own deployment use the
**fully-parameterized** pair:

- `docker/docker-compose.custom.yml` — every env var spelled out per node (no anchors),
  a prebuilt `image:`, host **bind-mounts** for data, published client UDP ports, and
  `restart: unless-stopped`. All tunables are `${VAR}` substitutions.
- `docker/rune-cluster.env.example` — the tunables (image tag, ports, host data dirs,
  shm size, stream ids, election timings).

```sh
# 1. Build (or pull) the image once
docker build --platform linux/amd64 --build-context rune-protocol=../rune-protocol \
    -f docker/Dockerfile -t rune-broker:latest .

# 2. Copy + edit the env file
cp docker/rune-cluster.env.example docker/rune-cluster.env
$EDITOR docker/rune-cluster.env

# 3. Up / logs / down
docker compose --env-file docker/rune-cluster.env -f docker/docker-compose.custom.yml up -d
docker compose --env-file docker/rune-cluster.env -f docker/docker-compose.custom.yml logs -f
docker compose --env-file docker/rune-cluster.env -f docker/docker-compose.custom.yml down
```

Clients outside Docker connect to the published host ports (`CLIENT_HOST_PORT_1..3`, e.g.
`localhost:40201/40202/40203`); intra-cluster consensus stays on the compose network via the
`rune-1/2/3` DNS names. Data lives in the host `DATA_DIR_*` dirs (default `./rune-data/nodeN`)
and survives `down` (use `down -v`-equivalent by deleting those dirs to reset).

### Scaling to N nodes
Add a `rune-4` service (copy a block, set `RUNE_NODE_ID: "3"`, its own volume + host port)
and append `3@rune-4:<CONSENSUS_PORT>` to **every** node's `RUNE_PEERS`. Membership is static,
so all nodes must list all peers. Odd node counts (3, 5, 7) give the best quorum math.

### Multi-host (nodes on separate machines)
Compose DNS names only resolve within one host's network. Across machines, run **one node
per host** with plain `docker run` (or one compose service per host) and use **real routable
hostnames/IPs + published UDP ports** in `RUNE_PEERS` — the consensus port must be reachable
host-to-host. Example for node 0 on `hostA` (peers `hostA/hostB/hostC`):

```sh
docker run -d --name rune-0 --platform linux/amd64 --shm-size=256m \
  -p 40111:40111/udp -p 40200:40200/udp \
  -e RUNE_NODE_ID=0 \
  -e RUNE_PEERS='0@hostA:40111,1@hostB:40111,2@hostC:40111' \
  -e RUNE_CLIENT_ENDPOINT='0.0.0.0:40200' \
  -e RUNE_DATA_DIR=/var/lib/rune \
  -e RUNE_ELECTION_TIMEOUT_MS=1000 \
  -e RUNE_ELECTION_STATUS_INTERVAL_MS=200 \
  -e RUNE_STARTUP_CANVASS_TIMEOUT_MS=2000 \
  -v /srv/rune/node0:/var/lib/rune \
  rune-broker:latest
```

Repeat on `hostB` (`RUNE_NODE_ID=1`) and `hostC` (`RUNE_NODE_ID=2`) with the **same**
`RUNE_PEERS`. Raise the election timings for WAN latency (shown above).
