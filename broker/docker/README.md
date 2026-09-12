# rune-broker Docker image — BR-6

Single-node image for the `rune_broker` runtime. The embedded Aeron media
driver runs in-process (no separate driver to launch). A single node
self-elects leader on boot.

---

## Base images

| Stage   | Image                   | Notes                                      |
|---------|-------------------------|--------------------------------------------|
| builder | `ubuntu:24.04`          | gcc-13 + cmake + conan 2.x (via pip)       |
| runtime | `ubuntu:24.04`          | Must match the builder glibc/libstdc++ ABI; C++ runtime + libuuid1. Final image ~30 MB |

---

## Build

```sh
docker build \
  --platform linux/amd64 \
  --build-context rune-protocol=../rune-protocol \
  -f docker/Dockerfile \
  -t rune-broker:dev \
  .
```

Run from the `rune-broker` repo root. The `--build-context rune-protocol=...`
supplies the header-only sibling repo (`libs/storage` includes
`<rune/protocol/RecordBatch.h>`); it is consumed via sibling-include in Phase 1
and will become a conan `requires` once CI publishes `rune-protocol`.

No credentials or private remote required: all dependencies (aeron, spdlog)
come from public ConanCenter. `--build=missing` builds any dep lacking a
prebuilt Linux/amd64 binary from source (aeron pulls a JDK build-requirement
transiently; the first build is therefore slow but fully self-contained).

---

## Environment variables

All config is read by `src/main.cpp` (`parseNodeConfigFromEnv`).

| Variable                        | Required | Default     | Description                                      |
|---------------------------------|----------|-------------|--------------------------------------------------|
| `RUNE_NODE_ID`                  | yes      | —           | Integer node ID (0-based)                        |
| `RUNE_PEERS`                    | yes      | —           | Comma-separated `id@host:port` list (all nodes)  |
| `RUNE_CLIENT_ENDPOINT`          | yes      | —           | Aeron UDP endpoint for clients, e.g. `aeron:udp?endpoint=0.0.0.0:40200` |
| `RUNE_CLIENT_STREAM_ID`         | no       | `1`         | Aeron stream ID for client traffic               |
| `RUNE_CONSENSUS_STREAM_ID`      | no       | `10`        | Aeron stream ID for consensus traffic            |
| `RUNE_DATA_DIR`                 | no       | `/var/lib/rune` | Writable data directory (mount a volume here) |
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

## 3-node compose (BR-7)

Multi-node compose with UDP peer wiring, `/dev/shm` sizing per node, and the
raw-Aeron ping probe are defined in BR-7 — not here.
