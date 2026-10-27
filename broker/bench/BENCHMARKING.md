# Benchmarking Rune

How to prepare the benchmark box and run a measurement so that numbers are comparable. Every value here comes from the
perf record [`docs/superpowers/specs/2026-10-02-throughput-latency-findings.md`](../docs/superpowers/specs/2026-10-02-throughput-latency-findings.md)
(section numbers below refer to it) and from [`scripts/vm-clean.sh`](../scripts/vm-clean.sh). The tools are
[`vm-bench-prepare.sh`](vm-bench-prepare.sh) and [`vm-bench-restore.sh`](vm-bench-restore.sh); the harnesses are in
[`kafka-compare/`](kafka-compare) and the overview of all benchmarks is [docs/benchmarks.md](../../docs/benchmarks.md).

**Targets.** Latency NFR: p50 at most 50 µs and p99 at most 200 µs on the produce to ack path. The 2026-10-02 record (§1) also set
a throughput goal of at least 159k rec/s (3 nodes, acks=all, 1 KiB values, 4-core box) and a p99 under 100 µs open loop.

## Why the box matters

The reference box is a 4 vCPU, 16 GB Linux VM with ext4 and a 3.8 GB `/dev/shm`, running 3 brokers and the client co-located on
loopback (§2). On it, k3s's ~1055 firewall rules and connection tracking, leftover processes and a full `/dev/shm` each moved
results by more than the effect being measured: a 3 GB leftover work dir starved `/dev/shm` and made a correctness run fail
(§4.1), and the loopback fast path alone cut a small UDP send from p50 30 to 19.5 µs and p99 110 to 67 µs (§3 ENV row).

## Box preparation

All of it runs under the host lock, from prepare through restore. Use the scripts; they refuse to run without the lock.

```bash
export RUNE_VM_LOCK_FILE=/root/vm.lock          # the default
flock -w 7200 "$RUNE_VM_LOCK_FILE" bash -c '
  export RUNE_VM_LOCK_HELD=1
  trap "broker/bench/vm-bench-restore.sh" EXIT   # also on failure or Ctrl-C
  broker/bench/vm-bench-prepare.sh
  # ... the benchmark, in this same shell ...
'
```

`vm-bench-prepare.sh` applies, in order (and writes everything it changes to a run directory under
`$RUNE_BENCH_STATE_ROOT`, default `/root/rune-bench-runs/run-<time>-<pid>`, with a `prepare.log` and `box.txt`):

1. **Lock.** Requires `RUNE_VM_LOCK_HELD=1` and checks that `$RUNE_VM_LOCK_FILE` really is locked by someone.
2. **Kill everything.** Runs `scripts/vm-clean.sh`, which SIGKILLs leftover `rune_broker`, bench clients, `aeronmd`, Kafka (by its
   main class), `dr_client` and bench scripts by **exact name**, removes `rune-*` containers and `/tmp/frout*`,
   `/tmp/rune_bn_egress_*`, and waits for load1 below 1 with at least 3 GiB on `/` and 1 GiB on `/dev/shm`. Because it kills on a
   shared box, it runs only inside the lock. Then it removes leftover harness work dirs and Aeron dirs in `/dev/shm`
   (`kbench*`, `aeron-*`) and lists the top CPU consumers.
3. **Stop k3s.** Records `systemctl is-active k3s`, runs `systemctl stop k3s` and `/usr/local/bin/k3s-killall.sh` (stops pods and
   containerd shims, removes the kube chains). It never reboots the machine.
4. **Flush iptables.** `iptables-save` and `ip6tables-save` first, then flush and delete all chains in `filter`, `nat`, `mangle`
   and `raw`, policies ACCEPT.
5. **Loopback fast path.** `-t raw -I PREROUTING -i lo -j NOTRACK`, `-t raw -I OUTPUT -o lo -j NOTRACK`, and `-I INPUT -i lo -j ACCEPT`,
   `-I OUTPUT -o lo -j ACCEPT`. During a run `conntrack -C` should stay near 0.
6. **Sysctls** (old values saved): `net.core.rmem_max`, `wmem_max`, `rmem_default`, `wmem_default` = 16777216 and
   `net.core.netdev_max_backlog` = 100000. Constraint: receive window ≤ socket buffer ≤ `rmem_max`/`wmem_max`, and window ≤ term/2
   (the broker's derived defaults already satisfy it; §4).
7. **Pre-flight.** load1 below 1; `/dev/shm` at least 1 GiB free (non-sparse terms need it); `/` at least 3 GiB; no other
   process above 5% CPU (`RUNE_BENCH_MAX_CPU_PCT`); nothing listening on the bench ports (`RUNE_BENCH_PORTS`, default
   `40100-40700 8200-8600`); a kernel of 4.18 or newer for UDP GSO.

If a step or check fails, prepare restores what it changed and exits non-zero. Register the work dirs your run creates by
appending their paths to `<run dir>/workdirs`; restore removes them.

`vm-bench-restore.sh [run-dir]` reverses steps 6 to 3: restores the sysctls, runs `iptables-restore` (and `ip6tables-restore`), starts
k3s if it was active and waits for `kubectl get nodes` to show Ready (`RUNE_BENCH_K3S_WAIT_S`, default 300 s), then removes the
run's work dirs. It is idempotent (a marker per step), works after a partial prepare, and finds the run dir from its argument,
`RUNE_BENCH_RUN_DIR` or the `latest` link. §8 of the record lists what else a long campaign changes by hand: the Aeron driver library
(restore the stock one before final numbers; a hooked library costs about 4 to 5% of a worker core, §4.3) and
`perf_event_paranoid` (back to 4).

## Run method

- **Builds.** Release only; never Debug or sanitizer builds. Record the git SHAs of the broker, the client and flight-recorder, and
  the box (vCPU, RAM, kernel: `box.txt`).
- **Broker environment.** `RUNE_FS_LOG=0`, `RUNE_LOG_LEVEL=warn`, and the flight recorder off (`FLIGHT_RECORDER_ENABLE` unset) unless
  the run measures it (§4.1, §4.2).
- **Config.** The broker's defaults are the tuned configuration and are derived from the host (`HostDefaults.h`; §4, §4.10): do
  not pass knobs unless the experiment varies them. Every result keeps the broker's `effective config:` log line (logged at warn
  level, so it shows with `RUNE_LOG_LEVEL=warn`), for example `mode=shard placement=fused workers=1 ... mtu=... socketBuffer=...
  rcvWindow=...`. The reference configuration from §4.8 on is MTU 1408 with UDP GSO/GRO and backoff workers; 65504-MTU numbers are
  labelled comparisons only.
- **Harness check (§4.2).** A harness header once exported term, window and ring sizes into every run, so a "default" run was not.
  Before starting a broker run `env | grep '^RUNE_'` and make sure only the variables you mean to set are present; apply
  overrides after any sourced header.
- **Topology.** Shard mode with one worker per node, on cores 1 to 3 for a co-located 3-node run, the client and every other thread
  on core 0 (§2, §4.13). Workers idle with backoff: busy-spin versus backoff made no difference in p50 or p99 (§4.2), and
  backoff frees cores. Use the authenticated bench launchers (`kafka-compare/run-matrix.sh`, `replbench.sh` with `RUNE_SHARD=1`),
  which are auth-compatible.
- **Workloads.** Throughput: saturate mode, acks=all, 1 KiB values, linger 0, 3 nodes, majority tier with all-in-sync durability
  and `RUNE_MIN_ISR=2`. Latency: open loop at a fixed rate (the record uses 20k rec/s; also report 1k rec/s), measured from client
  send to ack, reporting p50, p99 and p99.9.
- **Repetition.** Interleaved A/B (ABAB) runs, at least 5 for latency and at least 3 for throughput (the record's ABAB runs use
  n=5 to 10). Report the median and the run-to-run band; throughput varies about ±10% between runs (§2).
- **Gate.** A change is kept only if no percentile gets worse beyond the noise band, against the latency NFR above.
- **Instruments.** Do not time hot paths with uprobes or perf probes: they perturb what they measure (about 1000 passes per second
  with probes against about 250k without, §2). Use the flight recorder, in a separate, labelled run (about 10 ns per event).
- **Recording.** Keep with each result: the box, `prepare.log`, the effective config line, the environment (`env | grep '^RUNE_'`),
  the SHAs, the exact command lines and the raw output files.
- **Against Kafka.** Same box, same preparation, Kafka's JVM identified by its main class (`kafka.Kafka`, as `vm-clean.sh` does);
  compare at the same record size (§4.9).

## Known baselines (compare against these before investigating anything)

These are measured numbers on the 4-vCPU benchmark box, with 3 brokers and the client co-located on loopback, a prepared box, a Release build and the broker defaults. A new run that lands inside these ranges is **not** a regression, so don't re-investigate it. Only a change against the baseline of the same session (interleaved A/B) counts.

| workload | p50 | p99 | source |
|---|---|---|---|
| 3-node acks=all, 1 KiB, open loop 20k rec/s, fsync off | 350–690 µs | 10–30 ms (run-to-run spread 17–30 ms) | throughput-latency-findings §4.5, §4.8, §4.12–§4.15 (2026-10-03/04) |
| same, monorepo main (2026-10-19, smoke) | 668 µs | 12.8 ms | port A/B session ab1 |
| single node, acks=leader, 20k rec/s | 66–84 µs | ~211 µs | findings §4.2 |
| 3-node acks=all saturation, 1 KiB | — | — | 112–200k rec/s (MTU-dependent; §4.1, §4.8) |
| in-process produce/subscribe hot path (gateway bench) | 3.2 / 3.3 µs | 8.2 / 9.4 µs | auth Checkpoint F |

The headline latency target (p50 50 µs / p99 200 µs) is a single-node hot-path figure. The co-located 3-node acks=all path on this box has **never** met it: replication adds two network hops and cross-node contention on 4 shared cores (§4.5 per-hop waterfall). Don't treat that gap as a new finding.

**Settings that have caused "regressions" before. Check them first:**
- the driver duty cap (duty1): p50 688 µs → 24 ms;
- fsync on;
- MTU 1472 from a wildcard bind (throughput 136k → 14–18k);
- a harness header silently exporting term/window/ring overrides (§4.2);
- /dev/shm short of space;
- an unprepared box (k3s and iptables running, no lo NOTRACK).

## Known baselines (compare against these before investigating anything)

These are measured numbers on the 4-vCPU benchmark box, with 3 brokers and the client co-located on loopback, a prepared box, a Release build and the broker defaults. A new run that lands inside these ranges is **not** a regression, so don't re-investigate it. Only a change against the baseline of the same session (interleaved A/B) counts.

| workload | p50 | p99 | source |
|---|---|---|---|
| 3-node acks=all, 1 KiB, open loop 20k rec/s, fsync off | 350–690 µs | 10–30 ms | throughput-latency-findings §4.5, §4.8, §4.12–§4.15 (2026-10-03/04) |
| same, monorepo main, 2026-10-19 (smoke) | 668 µs | 12.8 ms | port A/B session ab1 |
| single node, acks=leader, 20k rec/s | 66–84 µs | ~211 µs | findings §4.2 |
| 3-node acks=all saturation, 1 KiB | 112–200k rec/s (MTU-dependent) | — | findings §4.1, §4.8 |
| in-process produce / subscribe hot path | 3.2 / 3.3 µs | 8.2 / 9.4 µs | auth Checkpoint F gateway bench |

The headline target (p50 50 µs / p99 200 µs) is a single-node hot-path figure. The co-located 3-node acks=all path on this box has never met it: replication adds two network round trips plus contention on 4 shared cores (§4.5 per-hop waterfall). Don't treat that gap as a new finding.

**Settings that caused past "regressions". Check them first:**
- the driver duty cap (p50 688 µs → 24 ms);
- fsync on;
- MTU 1472 from a wildcard bind (throughput 136k → 14–18k rec/s);
- a harness header silently exporting term/window/ring overrides;
- a short /dev/shm;
- an unprepared box (k3s and iptables running, no lo NOTRACK).

## Micro bench baselines

These benches are in-process and need no prepared box. Still, run them on an idle host for a verdict.

**`ControlDutyCycleBench`** (`broker/benchmark/ControlDutyCycleBench.cpp`, built with `-DBUILD_BENCHMARKS=ON`) measures what
`SystemMetrics` adds to one control-agent duty cycle: `if (systemMetrics_) work += systemMetrics_->doWork(nowNs);`, run on a
real metrics file and the real `/proc/self`. It gates two things after the timed loop: zero allocations in every case, and
the time per cycle shown in the table. The full BrokerNode control `doWork` is not driven here. The produce path is covered by
the ABAB single-node produce p99 run.

| case | per cycle (median, n=5) | gate | source |
|---|---|---|---|
| no `SystemMetrics` (metrics file off) | 0.47 ns | < 50 ns | metrics plan Task 12, 2026-10-27, `-O2`, shared dev host (load ~3) |
| `SystemMetrics`, off-interval (the steady state) | 1.0 ns | < 50 ns | same |
| `SystemMetrics`, on-interval (`pread` of statm and status, `getdents64` of the fd dir) | 5.9 µs | < 50 µs | same |

At the default 1 s interval, the on-interval cost is paid once a second on the control thread. That is about 6 µs/s, or
0.0006 % of one core, and it is never paid on a produce, fetch or query path.

## Checklist

1. Take the lock; run `vm-bench-prepare.sh`; read its log.
2. Release builds of the recorded SHAs; `env | grep '^RUNE_'` clean.
3. Start the cluster with defaults; confirm the `effective config:` line and, for NOTRACK, `conntrack -C` near 0.
4. Warm up, then run ABAB; keep raw outputs.
5. Report medians and noise bands, p50/p99/p99.9 against the NFR.
6. Run `vm-bench-restore.sh` (use the trap above); confirm k3s is Ready.
