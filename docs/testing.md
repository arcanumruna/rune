# Testing

Every feature is proven by unit, integration and end-to-end tests, and every fix lands with a regression test. Tests whose
name contains `Regression` form the **regression suite**; it runs together and gates every checkpoint.

Contents: [layers](#layers) · [build](#build-the-tests) · [unit and integration](#unit-and-integration-tests) ·
[regression](#the-regression-suite) · [latency gates](#latency-gates) · [Linux-only](#linux-only-tests) ·
[sanitizers](#sanitizer-runs) · [docker e2e](#end-to-end-docker-suites) · [DR e2e](#disaster-recovery-suite-dr-e2e) ·
[guards](#source-guards) · [on the VM](#running-on-the-remote-vm)

## Layers

| Layer | What it proves | Where |
|---|---|---|
| Unit | One component against its contract, injected clocks, no sleeps. | `*/tests/`, `broker/libs/*/tests/` |
| Integration | Real `BrokerNode`s, real Aeron UDP on loopback, the real client against a real broker (`rune_client_broker_tests`). | `broker/libs/node/tests/integration/`, `client-cpp/tests/` |
| Matrix and stress | Every feature across operation, entity and timing; model-checked seeds. | `*_matrix_tests`, `*_stress_tests` |
| Latency gates | The duty-cycle passes that must never stall. | `libs/node/CMakeLists.txt` (label `latency`) |
| Docker e2e | A real cluster in containers, a real client container. | `broker/docker/*.sh` |
| DR e2e | Faults injected into the real 3-node cluster, with invariants checked. | `broker/dr-e2e/` |

Each process binds only the ports it owns (flock-owned port blocks, `libs/aeronutil/.../testing/FreePort.h`) and uses
pid-scoped directories, so test processes run in parallel without a resource lock.

## Build the tests

Tests are off by default. The root build hosts the broker (which compiles the protocol, codec, crypto and ql sources and
embeds the client) and `protocol/`:

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE=build/conan/conan_toolchain.cmake \
      -DBUILD_TESTING=ON -DFLIGHT_RECORDER_DIR=../flight-recorder
cmake --build build
```

`BUILD_TESTING=ON` needs Python 3 (the source guards are Python scripts) and GTest. `BUILD_INTEGRATION_TESTING` (default `ON`) builds the
heavier real-broker targets. The other libraries build and test on their own: [wiki, Building](https://github.com/arcanumruna/rune/wiki/Building#each-library-on-its-own).

## Unit and integration tests

```bash
ctest --test-dir build -j8 --output-on-failure           # the full suite
ctest --test-dir build -R 'GatewayAuth' -j8              # a slice, by name
ctest --test-dir build -N | tail -1                      # how many tests are registered
```

- The **full suite** is every registered test. In the reference Debug configuration (`BUILD_TESTING` and
  `BUILD_INTEGRATION_TESTING` on) `ctest -N` lists about 11 600 tests; the count grows with the code.
- Each gtest is one ctest test. Binaries made of thousands of millisecond tests (label `small`) are additionally run as
  gtest-sharded batches named `batch:<binary>:<k>of<n>` (label `batch`); the same `*Regression*` tests, fewer processes.
- `broker/scripts/run-broker-tests.sh [build-dir]` is a two-tier runner: a wide `-j` for the fast tests, then the slow
  multi-node suites at `-j2` under a host-wide lock (`/tmp/rune-heavy.flock`), for a machine shared by several agents.
- Logs: `<build>/Testing/Temporary/LastTest.log`; failing output with `--output-on-failure`.
- Temp data: every gtest binary runs under its own `$TMPDIR/rune_t<pid>` root (`cmake/test_support/TestTmpRoot.cpp`),
  removed at exit pass or fail; a killed run's root is swept by the next test binary. `RUNE_TEST_KEEP_TMP=1` keeps it.
  Fixtures still remove their own `rune_*` / `cp*_` dirs as before; tests that hard-code `/tmp` bypass the root.
- Expected duration: not recorded in the repository; the full suite is a long run (thousands of real-UDP tests), the
  regression script below prints per-stage wall times.

## The regression suite

```bash
ctest --test-dir build -R Regression -j8 --output-on-failure      # every *Regression* test, individually
cd broker && scripts/run-regression.sh ../build 10                 # the checkpoint gate: build-dir, jobs
```

`ctest -N -R Regression` lists about 8 200 tests in the reference Debug configuration.
The checkpoint gate `broker/scripts/run-regression.sh [build-dir] [jobs] [extra ctest args]` (default build directory
`build-fast`, relative to the working directory) runs three **mandatory** stages:

| Stage | Runs | Notes |
|---|---|---|
| a | `ctest -R "Regression\|^batch:" -LE "latency\|small" -j<jobs>` | Everything except the latency gates, in parallel. |
| b | `ctest -R Regression -L latency` | The latency gates, serially, on an otherwise idle host. Never skipped. |
| c | The Linux-only tests | On Linux: `scripts/linux-stage-c.sh`. On macOS: `scripts/run-linux-regression-vm.sh` on the VM (needs `RUNE_VM_HOST`); `RUNE_SKIP_LINUX_STAGE=1` skips it and the run is reported **NOT GREEN** (exit 3). |

It also requires exactly four `BenchGateRegression` tests to be registered, prints each stage's wall time and the total,
exits non-zero when any stage fails, and lists non-injected "perturbed cells" (cells relaxed because a scheduler stall
preceded them) from `<build>/perturbed-cells.log`. A latency stage over 60 s is reported.

When to run what: after a fix, build and run the test executables covering the code it touches plus every new or changed
test. The **full** `*Regression*` suite runs at checkpoints: after a batch of related tasks, before a whole-branch review
and after every merge. A checkpoint failure is bisected over the commits since the last green checkpoint.

## Latency gates

Tests labelled `latency` (20 in the reference configuration) assert that duty-cycle passes do not stall while a
compaction, a retention pass, a segment roll, a leadership change, a transaction marker or a transient failover runs
alongside. They are `RUN_SERIAL`, so they never share the CPU with other test processes.

```bash
ctest --test-dir build -R Regression -L latency --output-on-failure
RUNE_LATENCY_GATE_BENCH=1 ctest --test-dir build -R DutyPassLatencyGate -V     # print the numbers, never fail
RUNE_LATENCY_GATE_SCALE=4 ctest --test-dir build -R DutyPassLatencyGate -V     # multiply the pass counts
RUNE_LATENCY_GATES=1 ctest --test-dir build -R KeyRotationAcrossARollingRestart -V  # enforce the latency bounds inside correctness tests
```

A few correctness tests also measure a latency (for example `AuthClusterRegression.KeyRotationAcrossARollingRestart…`:
the time from a broker's restart to the first acknowledged record). They always print it, but assert the bound only
with `RUNE_LATENCY_GATES=1`, on an idle host; otherwise they print that the bound was not enforced.
Timing assertions that are the whole point of a test (the login median gate and the first-leg timing parity in
`AuthGatesRegression`) are instead labelled `latency` and `RUN_SERIAL`, so they run in the latency stage only.

The produce-to-ack latency itself is measured by the benchmarks ([benchmarks.md](benchmarks.md)); the AEAD benchmark carries
its own gate (`RUNE_CRYPTO_BENCH_BUDGET_NS`).

## Linux-only tests

UDP GSO and GRO, the path-MTU regressions and the bind-order proof need Linux and a private network namespace.

```bash
RUNE_LINUX_GATE=1 ctest --test-dir build -L linux-only --output-on-failure     # UdpGsoBindings*: lowers loopback MTU, so run in a private netns
```

`broker/scripts/linux-stage-c.sh [build-dir]` runs both gated runs, each in a private network namespace
(`unshare -n`, loopback up), and prints **NOT GREEN** (exit 3) when it lacks `CAP_NET_ADMIN` or cannot create a namespace.
The bind-order proof is
`BindOrderRegression.FixedPortsBindBeforeAnyEphemeralPublicationRegression` with `RUNE_BIND_ORDER_GATE=1`, the ephemeral
port range narrowed to 40 ports. On a Mac the same stage runs on the VM, see below. The `linux-only` label is empty on
macOS.

## Sanitizer runs

Every checkpoint also runs the `*Regression*` suite under AddressSanitizer plus UBSan and under ThreadSanitizer, each in
its own build directory. The broker's CMake detects `-fsanitize=thread` in `CMAKE_CXX_FLAGS` and links a default-options
object into every executable, so the Aeron suppressions (`broker/cmake/tsan-aeron.supp`, `halt_on_error=1`) apply to every
test.

```bash
# AddressSanitizer + UBSan
conan install . -of build-asan -s build_type=Debug --build=missing
cmake -S . -B build-asan -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE=build-asan/conan_toolchain.cmake -DBUILD_TESTING=ON \
      -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
      -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
cmake --build build-asan && ctest --test-dir build-asan -R Regression -LE latency -j4 --output-on-failure

# ThreadSanitizer
conan install . -of build-tsan -s build_type=Debug --build=missing
cmake -S . -B build-tsan -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE=build-tsan/conan_toolchain.cmake -DBUILD_TESTING=ON \
      -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer" \
      -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build build-tsan && ctest --test-dir build-tsan -R Regression -LE latency -j4 --output-on-failure
```

Exclude the latency gates under a sanitizer (`-LE latency`): they measure time. The client's own build has an extra
TSan target, `rune_group_tsan_tests`, behind `-DENABLE_TSAN=ON`. Sanitizer builds are several times slower and
larger than a plain Debug build; budget disk and time accordingly.

## Source guards

Python scripts run as ctest tests (names end in `Regression`) and fail the build when a rule of the engineering philosophy
is broken:

| Guard | Rule |
|---|---|
| `broker/scripts/check_no_atomics.py` (allowlist `atomics_allowlist.txt`) | No `std::atomic` as coordination. |
| `broker/scripts/check_single_driver_pumper.py` | A media driver is driven by exactly one thread. |
| `broker/scripts/check_no_rune_topics.py` | The removed `RUNE_TOPICS` setting stays removed. |
| `client-cpp/scripts/check_no_atomics_or_mutexes.py` | No atomics, mutexes, locks or condition variables in the client (`--futures` also checks futures). |
| `client-cpp/scripts/check_no_shared_ptr_handles.py` | Move-only handles: no shared ownership. |

## End-to-end docker suites

These build the broker and a client image from source and run them in containers on a compose network.

**Requirements:** Docker with BuildKit, the sibling layout (the scripts take the monorepo root as the parent of
`broker/` and `flight-recorder` from `FLIGHT_RECORDER_DIR`, default the directory next to the monorepo), enough disk for
the images, and **Linux with a kernel that supports io_uring** for production-like runs (the shipped compose sets
`RUNE_IO_BACKEND=portable`, which also works on Docker Desktop). The first build compiles Aeron from source and is slow;
later builds hit the layer cache. Run from `broker/`. All waits are bounded; every failure names its step; teardown
(`docker compose down -v`) runs on every exit path.

| Script | Cluster | What it proves | Hard caps |
|---|---|---|---|
| `docker/e2e.sh` | 3 nodes | One leader elects, the raw-Aeron ping probe answers, the leader is killed, a different node is elected, the probe answers again. Topics are created by DDL, twice (idempotent). | leader 90 s, survivor probe 90 s |
| `docker/auth-e2e.sh` | 3 nodes | Bootstrap, role, user and grants, a world-server client producing and consuming 10 000 records, an unauthorized topic refused, a wrong password refused, revoke ending a live consumer in 2 s, and **cluster-key rotation** across a rolling restart under load. | leader 180 s, client run 600 s |
| `docker/catalog-e2e.sh` | 2 x 3 nodes | DDL through every broker with redirects, the catalog leader killed mid-DDL (exactly one `CreateTopic`), `catalog export`, `replay` into a second cluster, and a `diff` of no differences. | per step |
| `docker/schema-e2e.sh` | 1 node | A typed round trip: define, `openTopic<T>` (fingerprint gate), send, consume, decode, and a malformed record rejected. | leader 120 s |
| `docker/schema-evolution-e2e.sh` | 1 node | add, drop, rename and set-compatibility through the CLI with generated probes; a v1-bound binary still opens the evolved topic; concurrent alters give one `SchemaConflict`. | leader 180 s |
| `docker/view-engine-e2e.sh` | 1 node | Views (persisted and transient), aggregates, typed routing, transient topics, read-committed, and a broker restart leg. | leader 180 s |
| `docker/transient-failover-e2e.sh` | 3 nodes | A real client's transient-topic subscriptions move off a demoted leader without loss or duplicates. | ready 420 s, done 600 s |
| `docker/filter-e2e.sh` | 3 nodes | Subscription filters: five selections (all partitions, key + filter, group of two, unfiltered control) exact against a seeded model through a partition-leader kill and a node restart, the refusals, and ALTER under a live filter. `--scenario` runs the MMORPG load (200 world servers) filtered against unfiltered on the prepared box (needs the host lock). | leader 180 s, consume 600 s |

The client repo also ships `client-cpp/docker/e2e-client.sh` (ping through a re-election), `e2e-data.sh` (produce and subscribe
across a leader kill) and `rune-ql-e2e.sh` (CLI-driven typed round trip). They predate authentication: none has a cluster-key or
login step, so they are not part of the checkpoint and need an adapted (dev-mode) broker; prefer the launchers above.

```bash
cd broker
bash docker/e2e.sh                                    # the leader-kill e2e
E2E_LOG_DIR=/tmp/e2e-logs bash docker/auth-e2e.sh     # keep the brokers' and clients' logs
bash docker/catalog-e2e.sh
```

- **Logs.** The scripts print `[e2e] ...` step lines on stderr. `E2E_LOG_DIR` (auth, catalog, view-engine,
  transient-failover) keeps every container's log at teardown. `E2E_WORK_DIR` moves the scratch directory the client
  container shares as `/work`.
- **Authentication.** Every launcher runs an authenticated cluster: it generates a cluster key before `up`, bootstraps the
  first admin and an `e2e` user once there is a leader, and passes `RUNE_USER` / `RUNE_PASSWORD_FILE` to every probe
  container (`docker/lib/auth.sh`).
- **Expected duration:** dominated by the image builds (first run: tens of minutes; with warm caches: minutes). The script
  caps above are ceilings, not typical times; measured wall times are not recorded in the repository.
- **Variables:** `RUNE_IMAGE_TAG` (default `latest`; the e2e harness tags images by commit), `FLIGHT_RECORDER_DIR`,
  `CMAKE_BUILD_PARALLEL_LEVEL` (cap build parallelism in a small Docker VM; each big translation unit needs about 1.5 GB),
  `RUNE_STORAGE_FORMAT` (default `auto` in the dev compose).

## Disaster-recovery suite (dr-e2e)

A pytest harness drives the real 3-node `aeron:udp` cluster plus multiple client containers, injects faults (kill, pause,
restart, `netem` loss, delay, reorder, duplication, partitions) and checks invariants: no committed loss, per-key ordering,
exactly-once, a single leader, resume and durability after restart. A disaster is a **matrix** across node, count, timing,
intensity, client state, data state, repetition and recovery. The full reference, including the fault taxonomy per ticket
(DR-1 to DR-15), the durability contract and the Docker Desktop caveats, is
[`broker/dr-e2e/DR_SUITE.md`](../broker/dr-e2e/DR_SUITE.md).

**Requirements:** Python 3.10 or newer, Docker, `pytest`, `pytest-timeout` and `pytest-xdist` (`requirements.txt`),
`NET_ADMIN` for the netem sidecar (Docker Desktop works for netem; `iptables`-based faults are Linux only).

```bash
cd broker/dr-e2e
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
pytest -v -m "not soak"            # everything but the slow soak tier
pytest -v -m baseline              # the no-fault correctness floor, fastest
DR_E2E_BUILD=1 pytest -v           # also (re)build the compose images on bring-up
pytest -n 2 --dist load            # two workers, each with its own compose projects
```

- Markers: `baseline`, `fault`, `netem`, `soak`, `compaction`, `milestone_d_p1`, `exclusive`, `perworker`.
- Every test has a hard 300 s timeout (`pytest.ini`); fault tests may override it.
- The harness builds the images it needs once per session (`rune-dr-client:<client sha>`, the `rune` CLI image,
  `rune-netem`); a failed build ends the session before the first test (exit 3). `DR_SKIP_IMAGE_PREP=1` skips that step.
  A prebuilt `DR_CLIENT_IMAGE` must carry a matching `rune.client.sha` label.
- Environment: `DR_E2E_BUILD`, `DR_TIMING_LOG=<file>` (one JSON line per phase; `python3 tools/timing_report.py <file>`
  prints seconds per phase), `DR_DRAIN_QUIET_MS` (default 3000), `DR_ALLOW_STALE_CLIENT_IMAGE`.
- Do not run two pytest sessions against the same compose project; use `-n` for parallelism.
- Logs: pytest's report (`-ra`); the harness keeps containers' output through the compose project of the failing test.

## Running on the remote VM

The Linux-only stage, the docker e2e suites and the DR suite are run on a Linux VM, never on a developer Mac's Docker.
Nothing about the VM is baked into the repo: the address comes from the environment.

```bash
export RUNE_VM_HOST=<vm address>              # or RUNE_LINUX_GATE_HOST=user@host
cd broker && scripts/run-linux-regression-vm.sh 3 ../build     # stage c: jobs, build dir
```

- `run-linux-regression-vm.sh [jobs] [build-dir] [ctest-filter]` snapshots the exact trees the build compiled (refusing
  uncommitted changes unless `RUNE_ALLOW_DIRTY=1`), streams them into a private run directory on the VM, builds the broker
  Dockerfile's builder stage there and runs the `linux-only` label (with `CAP_NET_ADMIN`) and the bind-order proof. It
  removes only its own run directory and image on every exit path and never prunes.
- **Locking.** Everything on the VM takes `flock -w 7200 /root/vm.lock`, so two runs never overlap. Wrap manual runs the
  same way: `ssh root@$RUNE_VM_HOST flock /root/vm.lock <command>`.
- **Before a benchmark or DR run**, `broker/scripts/vm-clean.sh` on the VM kills leftover Rune, Kafka and bench processes by
  exact name, removes `rune-*` containers and `/tmp/frout*`, and refuses to proceed (exit 2 to 5) unless the box is quiet
  (`load1 < 1`) and has room (3 GiB free on `/`, 1 GiB in `/dev/shm`).
- **Disk.** Clean up what you create: remove run directories, `docker image rm` your tags, and avoid `docker system prune`
  on a shared VM.
