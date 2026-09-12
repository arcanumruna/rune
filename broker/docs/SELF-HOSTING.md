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

## Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `RUNE_NODE_ID` | yes | — | Integer node id (0-based). Must appear in `RUNE_PEERS`. |
| `RUNE_PEERS` | yes | — | Comma list of `id@host:consensusPort` for **all** nodes (including self). |
| `RUNE_CLIENT_ENDPOINT` | yes | — | **Raw `host:port`** clients connect to (the broker prepends `aeron:udp?endpoint=`). Use `0.0.0.0:40200` to listen on all interfaces. |
| `RUNE_CLIENT_STREAM_ID` | no | `1` | Aeron stream id for client traffic. |
| `RUNE_CONSENSUS_STREAM_ID` | no | `10` | Aeron stream id for consensus traffic. |
| `RUNE_DATA_DIR` | no | `/var/lib/rune` | Writable data dir (mount a volume). |
| `RUNE_ELECTION_TIMEOUT_MS` | no | `150` | Election round timeout. Raise for cross-datacenter. |
| `RUNE_ELECTION_STATUS_INTERVAL_MS` | no | `20` | Canvass/heartbeat cadence. |
| `RUNE_STARTUP_CANVASS_TIMEOUT_MS` | no | `100` | Extended canvass on first boot. |

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
