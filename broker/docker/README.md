# rune-broker Docker image — BR-6

Single-node image for the `rune_broker` runtime. The embedded Aeron media
driver runs in-process (no separate driver to launch). A single node
self-elects leader on boot.

---

## Base images

| Stage   | Image                   | Notes                                      |
|---------|-------------------------|--------------------------------------------|
| builder | `gcc:13`                | Debian bookworm base; gcc-13 + Kitware cmake 3.31 + conan 2.x. Same pattern as `world-server`. |
| runtime | `debian:bookworm-slim`  | Shares the builder's glibc (2.36); the newer gcc-13 `libstdc++.so.6` is copied in. C++ runtime + libuuid1. |

Both stages sit on Debian bookworm, so the binary's glibc matches the runtime;
only gcc-13's newer `libstdc++.so.6` is copied from the builder (via `ldconfig`),
keeping the runtime image slim.

---

## Build

```sh
docker build \
  --platform linux/amd64 \
  --build-context rune-protocol=../rune-protocol \
  --build-context rune-fr=../rune-fr \
  --build-context rune-codec=../rune-codec \
  --build-context rune-crypto=../rune-crypto \
  -f docker/Dockerfile \
  -t rune-broker:dev \
  .
```

Run from the `rune-broker` repo root. The four `--build-context` entries supply the
sibling repos the root `CMakeLists.txt` compiles unconditionally: `rune-protocol`
(headers + `src/net`), `rune-fr`, `rune-codec` (incl. the catalog library) and
`rune-crypto`. `docker compose` passes the same four contexts for the broker
image and for the `probe` image (`docker/Dockerfile.probe`), so prefer
`docker compose -f docker/docker-compose.yml build`.

No credentials or private remote required: all dependencies (aeron, spdlog)
come from public ConanCenter. `--build=missing` builds any dep lacking a
prebuilt Linux/amd64 binary from source (aeron pulls a JDK build-requirement
transiently; the first build is therefore slow but fully self-contained).

---

## Authentication (on by default)

A broker refuses to start without a cluster key file, and every client logs in. The compose files mount
`./secrets/cluster.key` (or `$RUNE_SECRETS_DIR/cluster.key`) read-only at `/run/secrets/cluster.key` and set
`RUNE_CLUSTER_KEY_FILE`; `entrypoint.sh` refuses a missing file with the broker's own message and never generates a key.
Make one, then bootstrap the first admin once the cluster has a leader:

```sh
rune security gen-cluster-key --out docker/secrets/cluster.key      # mode 0600, never overwritten
rune security bootstrap --admin admin --cluster-key-file docker/secrets/cluster.key --broker rune-1:40200
```

The e2e launchers (`e2e.sh`, `schema-e2e.sh`, `schema-evolution-e2e.sh`, `view-engine-e2e.sh`, `transient-failover-e2e.sh`,
`catalog-e2e.sh`) do both through `docker/lib/auth.sh` (`rune_gen_cluster_key`, `rune_bootstrap`, `rune_create_e2e_user`,
`rune_auth_prepare`), create an `e2e` role/user, and pass `RUNE_USER` / `RUNE_PASSWORD_FILE` to every probe container.
`docker/auth-e2e.sh` is the authentication e2e itself (3 nodes, `docker-compose.auth.yml`): bootstrap, a role + user + grants,
a world-server-style client producing and consuming 10 000 records, revoke -> explicit failure, and the cluster-key rotation
across a rolling restart. Reference: [../docs/SECURITY.md](../docs/SECURITY.md).

## Environment variables

All config is read by `src/main.cpp` (`parseNodeConfigFromEnv`).

| Variable                        | Required | Default     | Description                                      |
|---------------------------------|----------|-------------|--------------------------------------------------|
| `RUNE_NODE_ID`                  | yes      | —           | Integer node ID (0-based)                        |
| `RUNE_CLUSTER_KEY_FILE`         | yes (auth on, the default) | — | Shared cluster key file; the broker refuses to start without it. `RUNE_AUTH=disabled` is single-node development only |
| `RUNE_PEERS`                    | yes      | —           | Comma-separated `id@host:port` list (all nodes)  |
| `RUNE_CLIENT_ENDPOINT`          | yes      | —           | Aeron UDP endpoint for clients, e.g. `aeron:udp?endpoint=0.0.0.0:40200` |
| `RUNE_CLIENT_STREAM_ID`         | no       | `1`         | Aeron stream ID for client traffic               |
| `RUNE_CONSENSUS_STREAM_ID`      | no       | `10`        | Aeron stream ID for consensus traffic            |
| `RUNE_DATA_DIR`                 | no       | `/var/lib/rune` | Writable data directory (mount a volume here) |
| `RUNE_STORAGE_FORMAT`           | no       | (unset: never format) | `new` / `join` / `auto`; read **only** when the data dir is empty and not formatted. Unset: an empty dir refuses to start (exit 78) and prints the command to run. `auto` is development only. See `docs/SELF-HOSTING.md` "Formatting the data directory" |
| `RUNE_STORAGE_RECOVERY_VOTE`    | no       | `0`         | Unsafe, disaster recovery only (see `docs/SELF-HOSTING.md`) |
| `RUNE_ELECTION_TIMEOUT_MIN_MS`  | no       | (default)   | Min election timeout in ms                       |
| `RUNE_ELECTION_TIMEOUT_MAX_MS`  | no       | (default)   | Max election timeout in ms                       |

---

## /dev/shm requirement

The embedded Aeron media driver maps its CnC file and log buffers into
`/dev/shm`. The default container allocation (64 MB) is **too small**.

**Always** pass `--shm-size=256m` (or `--ipc=host` for trusted environments):

```sh
docker run --rm \
  --platform linux/amd64 \
  --shm-size=256m \
  ...
```

---

## Single-node run (self-elect verification)

Format the data volume once (a broker never formats an empty data dir on its own; see
`docs/SELF-HOSTING.md` "Formatting the data directory"):

```sh
docker run --rm -v rune-data:/var/lib/rune -e RUNE_DATA_DIR=/var/lib/rune rune-broker:dev rune_broker storage format --new
```

Then start it:

```sh
docker run --rm \
  --platform linux/amd64 \
  --shm-size=256m \
  -e RUNE_NODE_ID=0 \
  -e RUNE_PEERS='0@127.0.0.1:40100' \
  -e RUNE_CLIENT_ENDPOINT='aeron:udp?endpoint=0.0.0.0:40200' \
  -e RUNE_DATA_DIR=/var/lib/rune \
  -v rune-data:/var/lib/rune \
  rune-broker:dev
```

Expected log evidence of self-election (within ~5 s):

```
rune-broker: node 0 is LEADER
rune-broker: running — send SIGINT or SIGTERM to stop
```

Stop the container with `docker stop <id>` (sends SIGTERM → clean shutdown).

---

## Health check

The image ships a `HEALTHCHECK` that verifies the process is alive:

```
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=3 \
    CMD pgrep -x rune_broker > /dev/null || exit 1
```

Consensus-readiness (leader known to the cluster) is the richer probe
introduced in BR-7 (3-node compose + UDP peer wiring).

---

## 3-node cluster (BR-7b)

A 3-node Raft/consensus cluster over `aeron:udp` is defined in
`docker/docker-compose.yml`. Nodes communicate on consensus port 40111; clients
connect on port 40200 per node. All nodes share the same compose network so DNS
names (`rune-1`, `rune-2`, `rune-3`) resolve automatically.

### Bring up the cluster

Run from the `rune-broker` repo root:

```sh
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d rune-1 rune-2 rune-3
docker compose -f docker/docker-compose.yml logs -f
```

Watch for exactly one node to log `is LEADER` within ~10 s. Each node has
`shm_size: 256m` (required by the embedded Aeron media driver) and a named
volume for `/var/lib/rune` (durable consensus state).

### Environment variables (per node)

| Variable                        | Value in compose                                  |
|---------------------------------|---------------------------------------------------|
| `RUNE_NODE_ID`                  | 0 / 1 / 2 (one per service)                       |
| `RUNE_PEERS`                    | `0@rune-1:40111,1@rune-2:40111,2@rune-3:40111`    |
| `RUNE_CLIENT_ENDPOINT`          | `0.0.0.0:40200` (listens on all interfaces)       |
| `RUNE_DATA_DIR`                 | `/var/lib/rune`                                   |
| `RUNE_STORAGE_FORMAT`           | `auto` (development: empty volumes are formatted automatically) |
| `RUNE_ELECTION_TIMEOUT_MS`      | `300` (relaxed for cross-container UDP)           |

A wiped or replaced volume rejoins as a replaced disk: with `auto` the empty volume is formatted as a joiner, the
node logs `storage UNCONFIRMED` (`docker exec <container> rune_role` prints `STORAGE ... state=UNCONFIRMED`) while the
controller fences and then confirms it, and it never leads or votes until `state=CONFIRMED`. Do not restart another
node meanwhile. See `docs/release-notes/storage-incarnation.md`.

### Raw-Aeron ping probe

A standalone probe binary (`ping_probe`) is built from
`docker/ping-probe/ping_probe.cpp`. It boots its own embedded Aeron media
driver, connects to a node's client gateway over `aeron:udp`, and runs:

```
ConnectRequest → ConnectResponse{OK} → Ping → Pong
```

Exit 0 means a Pong was received; non-zero means failure (reason on stderr).

The probe service is gated under the `tools` profile so it is not started by
`docker compose up`:

```sh
# Build the probe image.
docker compose -f docker/docker-compose.yml --profile tools build

# Run the probe against rune-1 (while the cluster is up).
docker compose -f docker/docker-compose.yml run --rm \
  -e RUNE_PROBE_TARGET="aeron:udp?endpoint=rune-1:40200" \
  -e RUNE_PROBE_EGRESS="aeron:udp?endpoint=probe:40300" \
  probe
```

The probe container is given hostname `probe` so the node can open a UDP
publication back to `probe:40300` for the Pong egress.

### Full E2E test (build → elect → probe → leader-kill → re-elect → probe)

```sh
bash docker/e2e.sh
```

Run from the repo root. The script:
1. Builds the broker and probe images.
2. Starts the 3 nodes.
3. Waits (≤ 90 s) for exactly one `is LEADER` log entry.
4. Runs the probe against `rune-1` → asserts `PROBE: OK`.
5. Stops the leader container.
6. Waits (≤ 60 s) for re-election among survivors.
7. Runs the probe against a survivor → asserts `PROBE: OK`.
8. Tears down (`docker compose down -v`) even on failure (trap).

### Ports

No host ports are exposed. All communication stays within the compose network.
The probe container uses hostname `probe` (resolvable inside the network) as its
Aeron UDP egress endpoint so nodes can publish Pong frames back to it.

### Cross-container Aeron UDP notes

Each container has its own `/dev/shm` (via `shm_size: 256m`) and its own
embedded media driver. Cross-container UDP requires:
- Unique Aeron dir per container (ensured by `RUNE_DATA_DIR` volume isolation).
- The probe's egress channel must be reachable by the node: `probe:40300`
  resolves via compose DNS, and the probe's media driver binds that UDP port.
- MTU: Docker's bridge network default (1500 B) is sufficient for the probe's
  64 KiB term buffer (Aeron fragments to MTU internally); no extra MTU tuning
  is needed.
