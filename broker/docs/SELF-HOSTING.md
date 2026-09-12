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

> Not env-configurable (yet): topics/partitions (Milestone B), dynamic membership, TLS/auth.
> `/dev/shm` size and published ports are **container** settings (`--shm-size`, `-p`), not broker env.

> **Format gotcha:** `RUNE_CLIENT_ENDPOINT` and the `host:port` in `RUNE_PEERS` are **raw**
> (e.g. `0.0.0.0:40200`, `rune-1:40111`) — the broker builds the `aeron:udp?endpoint=…`
> channel itself. Do **not** pass a full `aeron:udp?...` string.

> **`/dev/shm`:** the embedded media driver maps its CnC + log buffers into shared memory.
> The default container 64 MB is too small — always pass `--shm-size=256m` (or set
> `shm_size` in compose).

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
