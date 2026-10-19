<div align="center">

<a href="https://arcanum.regressorspath.com/rune/">
  <img src="docs/assets/banner.svg" alt="Rune: a durable log that is also your database, built for game backends" width="100%">
</a>

<br>

[![C++23](https://img.shields.io/badge/C%2B%2B-23-22d3ee?style=flat-square&labelColor=0a0a12)](CMakeLists.txt)
[![Transport](https://img.shields.io/badge/transport-Aeron%20UDP-a855f7?style=flat-square&labelColor=0a0a12)](conanfile.txt)
[![Auth](https://img.shields.io/badge/auth-SCRAM--SHA--512%20on%20by%20default-cbc3f5?style=flat-square&labelColor=0a0a12)](broker/docs/SECURITY.md)
[![TLS](https://img.shields.io/badge/TLS%201.3-optional%20build-22d3ee?style=flat-square&labelColor=0a0a12)](crypto/README.md)
[![Docker](https://img.shields.io/badge/docker-broker%20image-a855f7?style=flat-square&labelColor=0a0a12)](broker/docker/Dockerfile)
[![Flight recorder](https://img.shields.io/badge/flight--recorder-separate%20repo-cbc3f5?style=flat-square&labelColor=0a0a12)](https://github.com/arcanumruna/flight-recorder#readme)

**[Website](https://arcanum.regressorspath.com/rune/)** ·
**[Build](#build)** ·
**[Docker](#docker-images)** ·
**[Testing](#testing)** ·
**[Benchmarks](#benchmarks)** ·
**[Security](#security-quickstart)** ·
**[Reference](#reference)**

</div>

<br>

Rune is a replicated, partitioned log that is also a database. You write events, read the latest state of a player by
key, and keep per-user aggregates current, in one system and one client. It is built for MMO backends: the client runs inside
your server tick with no threads and no blocking calls, the hot path allocates nothing, and every packet travels over
reliable UDP on an embedded Aeron driver.

Topics are typed, with schema evolution, and can be compacted into tables. Materialized views are kept current on every
write and read by key. Producers are idempotent, transactions are exactly-once across topics and carry consumer offsets, and
consumer groups rebalance on join, leave or failure. Every node needs a cluster key and every client a login: SCRAM-SHA-512,
users, roles and grants, with the whole history replayable.

<p align="center">
  <img src="docs/assets/stats.svg" alt="About 50 microseconds p50 and about 200 microseconds p99 produce to ack; reliable UDP transport, never TCP; zero allocations on the hot path" width="100%">
</p>

<sub>The latency figures are the headline figures published on the [website](https://arcanum.regressorspath.com/rune/).
This repository holds the means to reproduce them, see [Benchmarks](#benchmarks); no run output is committed.</sub>

## How it works

<p align="center">
  <img src="docs/assets/architecture.svg" alt="A C++ client sends control traffic to the gateway and produce and subscribe traffic straight to the shard worker that owns the partition; workers append to the partition log and replicate it to follower nodes; catalog and security logs and materialized views sit alongside" width="100%">
</p>

- **Gateway.** Connect, login, metadata, DDL and redirects. A DDL sent to any broker is redirected to the catalog leader.
- **Shard workers.** Each worker owns a set of partitions and its own Aeron ingress, egress and replication streams, so the
  data path never crosses a thread. Threads talk only through ring buffers.
- **Replicated log.** A partition is appended on the leader and replicated to in-sync followers. A topic's `tier` chooses what
  an ack means: leader append, a majority, or a majority plus fsync. Transient topics replicate in memory and never touch disk.
- **Catalog and security logs.** Every topic, schema, view, user, role and grant change is one fingerprinted event in a
  replicated log, replayable by the broker, the client and the CLI with the same library.

## Repository layout

| Directory | What it is | Language | README |
|---|---|---|---|
| [`broker/`](broker) | The broker (`rune_broker`), its libraries, tests, docker and e2e harnesses, the DR suite, benchmarks | C++23, bash, Python | [broker/README.md](broker/README.md), [docker/README.md](broker/docker/README.md), [dr-e2e/README.md](broker/dr-e2e/README.md) |
| [`client-cpp/`](client-cpp) | The C++ client library, the `rune` CLI, examples, client benchmarks | C++23 | [client-cpp/README.md](client-cpp/README.md) |
| [`protocol/`](protocol) | Wire format, record-batch v2, error codes; header-only | C++23 | [protocol/README.md](protocol/README.md) |
| [`codec/`](codec) | Schema descriptors, record codec, the catalog and security event libraries | C++23 | see [docs/api/codec.md](docs/api/codec.md) |
| [`crypto/`](crypto) | TLS 1.3, AEAD and SCRAM primitives over OpenSSL | C++23 | [crypto/README.md](crypto/README.md) |
| [`ql/`](ql) | Rune QL parser and plan backends | C++23 | see [docs/api/ql.md](docs/api/ql.md) |
| [`docs/`](docs) | This reference | Markdown | [docs/](docs) |

The **flight recorder** lives in its own repository,
[arcanumruna/flight-recorder](https://github.com/arcanumruna/flight-recorder#readme). CMake clones the pinned commit with
`FetchContent`; for local development pass `-DFLIGHT_RECORDER_DIR=<checkout>` (the docker scripts read the environment variable
`FLIGHT_RECORDER_DIR` too, defaulting to a checkout next to this repository).

## Requirements

| | |
|---|---|
| **Production and tests of Linux-only code** | Linux. io_uring (kernel 5.6 or newer for the opcodes the broker uses; it falls back to a portable backend when io_uring is unavailable), UDP GSO and epoll polling are Linux only. |
| **Compiler** | A C++23 compiler. The Docker builder uses GCC 13; the macOS development builds use Apple clang 17. CMake does not check the version. |
| **CMake** | 3.31 or newer for the root, `broker/`, `client-cpp/`, `crypto/` and `protocol/`; 3.25 for `codec/` and `ql/`. |
| **Conan** | Conan 2 (the Docker image installs `conan~=2.0`). Ninja is the generator used throughout. |
| **Dependencies** (`conanfile.txt`) | `aeron/1.41.4`, `spdlog/1.14.0`, `gtest/1.16.0`, `benchmark/1.8.2`, `openssl/3.3.2`. Aeron built from source needs a JDK (`default-jdk-headless`; its SBE codegen) and, on Linux, `libuuid` (`uuid-dev`) and zlib. io_uring is used through raw syscalls and the kernel headers: **liburing is not required**. |
| **Python 3** | Needed when `BUILD_TESTING=ON` (the source guards are Python scripts); 3.10 or newer for the DR suite. |
| **Docker** | With BuildKit, for the broker image and every e2e suite. |
| **TLS** | Optional: configure with `-DRUNE_TLS=ON` to build the TLS config path (certificate and key files, `RUNE_REQUIRE_TLS`, SIGHUP reload). The docker image is built with it. A source build defaults to OFF and then ignores every `RUNE_TLS_*` setting. |

## Build

Dependencies come from Conan. `conanfile.txt` at the root is the union of every subproject's; each subproject also has its own
`conanfile.py` for building it from its directory. Flight-recorder is cloned by CMake, or pass `-DFLIGHT_RECORDER_DIR`.

### The root build (broker, protocol, client for tests)

```bash
conan install . -of build/conan -s build_type=Debug --build=missing
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE=build/conan/conan_toolchain.cmake \
      -DBUILD_TESTING=ON -DFLIGHT_RECORDER_DIR=../flight-recorder
cmake --build build
ctest --test-dir build -j8
```

The root hosts `broker/` (which compiles the protocol, codec, crypto and ql sources itself and embeds `client-cpp/` for the
client-to-broker tests) and `protocol/`. `RUNE_BUILD_CODEC`, `RUNE_BUILD_CRYPTO`, `RUNE_BUILD_QL` and `RUNE_BUILD_CLIENT` build
one of the others instead, but never together with the broker (it already defines those targets). The binaries are
`build/broker/rune_broker` and `build/broker/rune_role`; the `rune` CLI is a target of the embedded client:

```bash
cmake --build build --target rune        # build/rune-client/rune
```

### Build presets and variants

There are no CMake presets; a variant is a build directory with its own flags. Use one directory per variant.

| Variant | Directory | Configure |
|---|---|---|
| Debug (development, tests) | `build` | as above, `-DCMAKE_BUILD_TYPE=Debug` |
| Release (benchmarks, images) | `build-release` | `conan install . -of build-release/conan -s build_type=Release --build=missing`, then `-DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=build-release/conan/conan_toolchain.cmake` |
| ASan + UBSan | `build-asan` | add `-DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer"` and `-DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"` |
| TSan | `build-tsan` | add `-DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer"` and `-DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"`; the broker links the Aeron suppressions automatically |
| Benchmarks | `build-bench` | Release plus `-DBUILD_BENCHMARKS=ON` |
| With TLS | any | add `-DRUNE_TLS=ON` |

The full commands for the sanitizer and benchmark variants are in [docs/testing.md](docs/testing.md#sanitizer-runs) and
[docs/benchmarks.md](docs/benchmarks.md#build).

### Each library on its own

Each takes the same three steps: `conan install <dir> -of build-<name> -s build_type=Debug --build=missing`, then
`cmake -S <dir> -B build-<name> -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_TOOLCHAIN_FILE=<toolchain>`, then
`cmake --build build-<name>`. The toolchain file is `build-<name>/build/Debug/generators/conan_toolchain.cmake` for the
subprojects that have a `conanfile.py` with a layout (`codec`, `ql`, `crypto`, `client-cpp`, `broker`).

| Library | Options | Notes |
|---|---|---|
| `codec/` | tests always built | Needs GTest. Targets `rune_codec`, `rune_catalog`, `rune_security`, `rune_codec_gen`. |
| `crypto/` | `-DBUILD_TESTING=ON`, `-DBUILD_BENCHMARKS=ON` | OpenSSL 3. Test certificates are generated at build time, never committed. |
| `ql/` | `-DBUILD_TESTING=ON` | Compiles the codec sources it needs (`-DRUNE_CODEC_ROOT`). |
| `protocol/` | `-DBUILD_TESTING=ON`, `-DBUILD_BENCHMARKS=ON` | Header-only. Build it through the root build: its own `conandata.yml` still names a private package. |
| `client-cpp/` | `-DBUILD_TESTING=ON`, `-DBUILD_EXAMPLES=ON`, `-DBUILD_BENCHMARKS=ON`, `-DRUNE_TLS=ON`, `-DENABLE_TSAN=ON` | Targets `rune_client_api` (link this), `rune` (CLI), examples. Needs Aeron on Linux or an x86_64 macOS build. |
| `broker/` | `-DBUILD_TESTING=ON`, `-DBUILD_BENCHMARKS=ON`, `-DRUNE_TLS=ON`, `-DBUILD_PING_PROBE=ON` | Prefer the root build. |
| flight-recorder | separate repo | `FetchContent` of the pinned commit, or `-DFLIGHT_RECORDER_DIR=<checkout>`; `-DENABLE_FLIGHT_RECORDER=OFF` compiles the instrumentation out. |

## Docker images

Dockerfiles use BuildKit **named build contexts** to supply the sibling trees, so each is built from its own directory with
`--build-context` flags that point at the monorepo layout. `FLIGHT_RECORDER_DIR` is a checkout of the flight-recorder repo.

| Image | Dockerfile | Contains |
|---|---|---|
| `rune-broker:<tag>` (compose tags it `rune-broker-node:${RUNE_IMAGE_TAG:-latest}`) | [`broker/docker/Dockerfile`](broker/docker/Dockerfile) | `rune_broker` and `rune_role`, built Release with `-DRUNE_TLS=ON`. Builder `gcc:13` with CMake 3.31.5 and Conan; runtime `debian:bookworm-slim` with the builder's `libstdc++`, `libuuid1`, `libatomic1`, `procps`. Entrypoint `entrypoint.sh`, a `pgrep` health check, volume `/var/lib/rune`. |
| `rune-broker-probe:<tag>` | [`broker/docker/Dockerfile.probe`](broker/docker/Dockerfile.probe) | `ping_probe`, the raw-Aeron probe of `docker/e2e.sh`. |
| `rune-ql-probe:<client sha>` | [`client-cpp/docker/Dockerfile.rune-ql-probe`](client-cpp/docker/Dockerfile.rune-ql-probe) | The `rune` CLI plus the full toolchain (single stage). It is the "CLI image" of the docs: `rune security gen-cluster-key`, `rune ql`. |
| `rune-dr-client:<client sha>` | [`client-cpp/docker/Dockerfile.dr`](client-cpp/docker/Dockerfile.dr) | `dr_client`, the driver of the DR suite (labelled `rune.client.sha`). |
| `rune-client-ping`, `rune-client-data`, schema and view probes | `client-cpp/docker/Dockerfile.client`, `.data`, `.schema-probe`, `.view-probe` | Example and probe binaries for the e2e suites. |
| `rune-crypto-bench` | [`crypto/docker/Dockerfile.bench`](crypto/docker/Dockerfile.bench) | The AEAD benchmark; the build fails over its latency budget. |

Build the broker image from `broker/`:

```bash
cd broker
docker build --platform linux/amd64 \
  --build-context rune-protocol=../protocol \
  --build-context flight-recorder="$FLIGHT_RECORDER_DIR" \
  --build-context rune-codec=../codec \
  --build-context rune-crypto=../crypto \
  -f docker/Dockerfile -t rune-broker:dev .
# or, and preferred for the compose files:
docker compose -f docker/docker-compose.yml build
```

`--platform linux/amd64` matters on Apple Silicon (Docker runs the amd64 builder under Rosetta). The build needs no
credentials: every dependency comes from the public ConanCenter, and the first build compiles Aeron from source.
`CMAKE_BUILD_PARALLEL_LEVEL` caps build parallelism for a small Docker VM. The CLI image is built from `client-cpp/` with
five contexts (`rune-protocol`, `rune-codec`, `rune-ql`, `flight-recorder`, `rune-crypto`); `broker/docker/create-topics.sh
--build-image` does exactly that.

**Run it.** Every broker needs a cluster key; the image never generates one.

```bash
docker run --rm -v rune-data:/var/lib/rune -e RUNE_DATA_DIR=/var/lib/rune rune-broker:dev \
    rune_broker storage format --new
docker run --rm --shm-size=256m \
  -v "$PWD/secrets/cluster.key:/run/secrets/cluster.key:ro" -e RUNE_CLUSTER_KEY_FILE=/run/secrets/cluster.key \
  -e RUNE_NODE_ID=0 -e RUNE_PEERS='0@127.0.0.1:40111' -e RUNE_CLIENT_ENDPOINT='0.0.0.0:40200' \
  -e RUNE_DATA_DIR=/var/lib/rune -v rune-data:/var/lib/rune \
  -p 40200:40200/udp rune-broker:dev
```

| Runtime variable | Meaning |
|---|---|
| `RUNE_NODE_ID`, `RUNE_PEERS`, `RUNE_CLIENT_ENDPOINT` | Identity, the full peer list (identical on every node) and the client listen address. Required. |
| `RUNE_DATA_DIR` | Data directory; the entrypoint defaults it to `/var/lib/rune`. |
| `RUNE_CLUSTER_KEY_FILE` | The shared cluster key, mounted read-only. Required unless `RUNE_AUTH=disabled` (single-node development). |
| `RUNE_STORAGE_FORMAT` | `new`, `join` or `auto`; only read on an empty directory. |
| `RUNE_LOG_LEVEL`, `RUNE_ELECTION_TIMEOUT_MS`, `RUNE_IO_BACKEND`, ... | See [docs/configuration.md](docs/configuration.md). |

Container needs: `--shm-size=256m` (the embedded driver maps its buffers in `/dev/shm`) and the data volume. Three-node and
multi-host recipes, the compose files and the formatting rules are in
[broker/docs/SELF-HOSTING.md](broker/docs/SELF-HOSTING.md) and [broker/docker/README.md](broker/docker/README.md).

## Running on a remote VM

The Linux-only tests, the docker e2e suites, the DR suite and the benchmarks are run on a Linux VM, never on a laptop's Docker.
The repository holds no addresses: everything comes from the environment.

```bash
export RUNE_VM_HOST=<vm address>                  # or RUNE_LINUX_GATE_HOST=user@host
cd broker && scripts/run-linux-regression-vm.sh 3 ../build
```

- **Requirements.** A Linux kernel with io_uring (5.6 or newer), Docker with BuildKit, SSH access as the user named by
  `RUNE_LINUX_GATE_HOST` (default `root@$RUNE_VM_HOST`), and free disk (the scripts want 3 GiB free on `/` and 1 GiB in
  `/dev/shm` before a benchmark).
- **How it runs.** `run-linux-regression-vm.sh` snapshots the compiled trees (refusing uncommitted changes unless
  `RUNE_ALLOW_DIRTY=1`), streams them into a private run directory on the VM, builds the broker builder image there and runs
  the `linux-only` tests and the bind-order proof in containers.
- **Locking.** Every VM job holds `flock -w 7200 /root/vm.lock`, so two jobs never overlap. Wrap manual runs the same way.
- **Cleanup.** The script removes only its own run directory and image on every exit path and never prunes. Do the same for
  anything you start; `broker/scripts/vm-clean.sh` resets the box before a benchmark or DR run.

Details: [docs/testing.md](docs/testing.md#running-on-the-remote-vm).

## Testing

Every feature has unit, integration and end-to-end tests and every fix a regression test named `*Regression*`. The full
reference, with requirements, expected budgets and where logs go, is [docs/testing.md](docs/testing.md).

| What | Command | Needs |
|---|---|---|
| Unit and integration | `ctest --test-dir build -j8 --output-on-failure` | `-DBUILD_TESTING=ON` build, Python 3 |
| Regression suite | `ctest --test-dir build -R Regression -j8` | same |
| Checkpoint gate | `cd broker && scripts/run-regression.sh ../build 10` | same, plus a Linux host or `RUNE_VM_HOST` for stage c |
| Latency gates | `ctest --test-dir build -R Regression -L latency` | an idle host |
| Sanitizers | `ctest --test-dir build-asan -R Regression -LE latency` (also `build-tsan`) | sanitizer build dirs |
| Docker e2e | `cd broker && bash docker/e2e.sh` (also `auth-e2e.sh`, `catalog-e2e.sh`, `schema-e2e.sh`, `schema-evolution-e2e.sh`, `view-engine-e2e.sh`, `transient-failover-e2e.sh`) | Docker, preferably the VM |
| DR e2e | `cd broker/dr-e2e && pytest -v -m "not soak"` | Docker, Python 3.10+, `NET_ADMIN` |

#### End-to-end docker suites
The seven broker launchers each build the images, start a real cluster, run a real client container and tear down on every
exit path; all waits are bounded. See [docs/testing.md](docs/testing.md#end-to-end-docker-suites) for what each proves.

#### Disaster-recovery suite (dr-e2e)
A pytest harness injects kills, pauses, restarts, packet loss, delay, reordering and partitions into the 3-node cluster and
asserts no committed loss, per-key ordering, exactly-once and a single leader. Reference:
[broker/dr-e2e/DR_SUITE.md](broker/dr-e2e/DR_SUITE.md).

## Benchmarks

| Benchmark | Run | Reads as |
|---|---|---|
| Broker components | `build-bench/broker/rune_broker_benchmark --benchmark_min_time=0.01s` | Google Benchmark ns/op with counters; a failed gate sets `gate_failed` and exits non-zero |
| Client | `rune_client_*_bench`, `rune_producer_*_bench` | same, no broker needed |
| Real client to broker | `rune_produce_throughput`, `rune_consume_throughput` (env-driven) | records/s, MB/s and p50 and p99 ack latency |
| Rune vs Kafka | `broker/bench/kafka-compare/run-matrix.sh`, then `collect.py` | `results.json`: median, min, max per cell |
| Replication | `broker/bench/kafka-compare/replbench.sh [parts] [recs] [window]` | 3-node `acks=all` throughput with flight-recorder histograms |
| Crypto | `rune_crypto_bench`, `rune_crypto_scram_bench` | AEAD ns/op with a budget gate; SCRAM login cost |

### Benchmarking guideline

Numbers are only comparable on a prepared box. The full guideline is
[broker/bench/BENCHMARKING.md](broker/bench/BENCHMARKING.md); the short form:

1. **Lock.** Everything runs under `flock -w 7200 /root/vm.lock`, from preparation to restore.
2. **Prepare.** `broker/bench/vm-bench-prepare.sh` kills every benchmark process (`scripts/vm-clean.sh`), clears leftover shm and
   work dirs, stops k3s (never reboot), saves and flushes iptables, adds the loopback `NOTRACK` and `ACCEPT` rules, raises the
   `net.core` socket-buffer sysctls (16 MiB, backlog 100000), and checks load, disk, `/dev/shm`, free ports and CPU.
3. **Method.** Release builds only; defaults (no knobs) with `RUNE_FS_LOG=0`, `RUNE_LOG_LEVEL=warn`, flight recorder off; keep the
   `effective config:` line; saturate for throughput, open loop for latency; interleaved ABAB runs (n at least 5 latency, 3
   throughput), median and noise band; no uprobes on hot paths.
4. **Restore.** `broker/bench/vm-bench-restore.sh` restores sysctls and iptables, starts k3s and waits for Ready.

Numbers are only meaningful on an idle box with a Release build; the harnesses refuse to measure under load. Local and VM
instructions, the variable list and how to read every output: [docs/benchmarks.md](docs/benchmarks.md).

## Security quickstart

Authentication is **on by default**: a broker refuses to start without a cluster key, and every client needs credentials.
Full reference: [broker/docs/SECURITY.md](broker/docs/SECURITY.md).

```bash
# 1. one key for the whole cluster (mode 0600, never overwritten); every node mounts the same file
rune security gen-cluster-key --out secrets/cluster.key

# 2. start the brokers with RUNE_CLUSTER_KEY_FILE pointing at it, then bootstrap the first admin, once per cluster
rune security bootstrap --admin admin --cluster-key-file secrets/cluster.key --broker rune-1:40200
# one-time password for admin: ...            (printed once; change it at first login)
echo "ALTER USER admin SET PASSWORD '<12 or more characters>';" > change.rql
rune ql --user admin --password-file otp.txt -f change.rql --broker rune-1:40200

# 3. roles, users and grants
rune ql --user admin --password-file admin.pw -f users.rql --broker rune-1:40200
```

```sql
CREATE ROLE world_server;
GRANT READ  ON TOPIC PREFIX 'world_' TO ROLE world_server;
GRANT WRITE ON TOPIC PREFIX 'world_' TO ROLE world_server;
CREATE USER ws1 WITH PASSWORD 'correct horse battery';
GRANT ROLE world_server TO USER ws1;
```

- **The cluster key** authenticates brokers to each other and seals every inter-broker frame. Rotate it with three rolling
  restarts using `RUNE_CLUSTER_KEY_FILE_NEXT` ([docs/operations.md](docs/operations.md#rotating-the-cluster-key)).
- **Clients** set `ClientConfig::username` and `passwordFile`; the CLI and the examples take `--user` / `--password-file` or
  `RUNE_USER` / `RUNE_PASSWORD_FILE`. A password is never accepted on a command line or from the environment.
- **Dev mode.** `RUNE_AUTH=disabled` switches authentication off, for **single-node development only**: the broker warns every
  5 minutes and refuses to start with more than one peer.
- **TLS.** SCRAM keeps the password off the wire but does not protect the data. For production build with `-DRUNE_TLS=ON`,
  give the broker `RUNE_TLS_CERT_FILE` and `RUNE_TLS_KEY_FILE`, set `RUNE_REQUIRE_TLS=true`, and set `offerCrypto`,
  `requireCrypto` and `tlsExpectHost` on clients. The `rune` CLI has no TLS support yet.

## Configuration

Every broker setting is an environment variable. The complete table, with defaults and meanings read from the code, is
[docs/configuration.md](docs/configuration.md).

| Group | Where |
|---|---|
| Identity, peers, endpoints, data directory | [Required](docs/configuration.md#required) |
| Election and timing, stream ids, ports | [Membership, election and timing](docs/configuration.md#membership-election-and-timing) |
| Formatting, recovery, io_uring, transient rings | [Storage](docs/configuration.md#storage) |
| Auth, cluster key, TLS | [Authentication and TLS](docs/configuration.md#authentication-and-tls) |
| Aeron term, MTU, buffers, GSO | [Aeron transport](docs/configuration.md#aeron-transport) |
| Workers, idle strategies, CPU pinning | [Threads, idling and affinity](docs/configuration.md#threads-idling-and-affinity) |
| Ring sizes and drain limits | [Rings and limits](docs/configuration.md#rings-and-limits) |
| `FLIGHT_RECORDER_*` | [Flight recorder](docs/configuration.md#flight-recorder) |
| Client, producer and group settings | [Client configuration](docs/configuration.md#client-configuration) |

## Reference

| Document | Covers |
|---|---|
| [docs/api/client-cpp.md](docs/api/client-cpp.md) | The C++ client: connect, produce, subscribe, groups, transactions, typed topics, views, security DDL, errors |
| [docs/api/wire-protocol.md](docs/api/wire-protocol.md) | The frame catalogue, SCRAM flow, record batch, every error code |
| [docs/api/ql.md](docs/api/ql.md) | Rune QL: topics, settings, history, security statements, `rune ql` |
| [docs/api/codec.md](docs/api/codec.md) | Types, schemas, evolution rules, code generation |
| [docs/api/admin-cli.md](docs/api/admin-cli.md) | Every `rune` command and flag |
| [docs/configuration.md](docs/configuration.md) | Every broker variable and client setting |
| [docs/operations.md](docs/operations.md) | Deploy, cluster key and rotation, backups and DR, metrics, flight recorder |
| [docs/testing.md](docs/testing.md), [docs/benchmarks.md](docs/benchmarks.md) | Test and benchmark guides |
| [broker/docs/SELF-HOSTING.md](broker/docs/SELF-HOSTING.md), [SECURITY.md](broker/docs/SECURITY.md), [CATALOG.md](broker/docs/CATALOG.md) | Long-form operator references |
| [flight-recorder](https://github.com/arcanumruna/flight-recorder#readme) | The recorder library: API, macros, tooling, overhead |

## License and links

No license file is present in this repository, so no license is granted by it; ask the maintainers before reuse.

[Website](https://arcanum.regressorspath.com/rune/) · [API page](https://arcanum.regressorspath.com/rune/docs.html) ·
[Self-host page](https://arcanum.regressorspath.com/rune/selfhost.html) ·
[flight-recorder](https://github.com/arcanumruna/flight-recorder#readme) ·
[Source](https://github.com/arcanumruna/rune)
