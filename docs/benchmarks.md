# Benchmarks

Every performance claim is backed by a benchmark. This page lists them, how to build and run each one locally and on the
VM, what they need, and how to read the output. Test layers are in [testing.md](testing.md).

| Benchmark | Measures | Needs | Section |
|---|---|---|---|
| `rune_broker_benchmark` | Broker internals: log append, replication, gateway, worker produce latency, views, catalog, auth | Google Benchmark | [Broker](#broker-micro-and-component-benchmarks) |
| `rune_client_*_bench`, `rune_producer_*_bench` | Client encode, batching, ack path, routing | Google Benchmark | [Client](#client-benchmarks) |
| `rune_produce_throughput`, `rune_consume_throughput` | Real client to real broker throughput and ack latency over Aeron | a running broker | [Throughput clients](#throughput-clients) |
| `bench/kafka-compare` | Rune against Apache Kafka over a matched-durability matrix on one box | Linux, JDK, Kafka | [Kafka comparison](#rune-versus-kafka) |
| `bench/kafka-compare/replbench.sh` | 3-node replication throughput with per-stage flight-recorder histograms | Linux box | [Replication](#replication-benchmark-replbenchsh) |
| `rune_crypto_bench`, `rune_crypto_scram_bench` | AEAD seal and open, SCRAM login cost | OpenSSL, Google Benchmark | [Crypto](#crypto-benchmarks) |
| `rune_codec_bench`, `rune_ql_bench`, `rune_protocol_benchmark` | Validator, catalog and security replay, parser, frame codecs, CRC | Google Benchmark | [Libraries](#library-benchmarks) |

The website's headline figures are about 50 µs p50 and about 200 µs p99 produce to ack, with zero allocations on the hot
path. Those figures are published targets of the project; this repository holds the means to reproduce them (the broker and
client benchmarks and the throughput clients on a quiet Linux box), not committed run output. Older measured reports
are in [`broker/docs/benchmarks/`](../broker/docs/benchmarks): read their caveats, because some compare an in-process Rune path
with a Kafka client over the network.

## Reading the output

- Google Benchmark prints one line per benchmark: wall time, CPU time and iterations, plus **user counters**
  (for example `records/s`, `allocs/op`, `gate_failed`). Use `--benchmark_filter=<regex>` to pick benchmarks,
  `--benchmark_min_time=0.01s` for a smoke run, `--benchmark_repetitions=N` with `--benchmark_report_aggregates_only=true` for
  mean, median and standard deviation, and `--benchmark_format=json` for machines.
- **Gates.** Some benchmarks assert after the timed loop (latency bounds, zero allocations, correctness). A failed gate sets
  the counter `gate_failed = 1` and makes the process exit non-zero after the run (`broker/benchmark/BenchGate.h`).
  Four `BenchGateRegression` tests prove the gate machinery itself.
- **Latency is p50/p99/p999, never a mean.** A benchmark on a loaded or shared box is meaningless: the harness scripts refuse
  to measure unless the box is idle (`load1 < 1`).
- Allocation counters (`g_allocCount`) come from a replaced `operator new` in `broker/benchmark/BenchmarkMain.cpp`; the
  hot-path benchmarks assert it stays zero.

## Build

Benchmarks are off by default and need Google Benchmark (`benchmark/1.8.2` from Conan).

```bash
# broker benchmarks, from the root build
cmake -S . -B build-bench -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_TOOLCHAIN_FILE=build-bench/conan/conan_toolchain.cmake -DBUILD_BENCHMARKS=ON \
      -DFLIGHT_RECORDER_DIR=../flight-recorder
cmake --build build-bench --target rune_broker_benchmark

# the other libraries, from their own directories
cmake -S client-cpp -B build-client-bench -DBUILD_BENCHMARKS=ON -DBUILD_EXAMPLES=ON ...
cmake -S crypto     -B build-crypto-bench -DBUILD_BENCHMARKS=ON ...
```

Always benchmark a **Release** build. The Debug build the tests use says nothing about latency. (`conan install . -of
build-bench/conan -s build_type=Release --build=missing` produces the toolchain file.)

## Broker micro and component benchmarks

```bash
build-bench/broker/rune_broker_benchmark --benchmark_min_time=0.01s                    # smoke
build-bench/broker/rune_broker_benchmark --benchmark_filter='WorkerProduceLatency' \
    --benchmark_repetitions=5 --benchmark_report_aggregates_only=true
```

The sources are under `broker/benchmark/`, grouped by area: log and storage (`LogBenchmark`, `LogSegmentBenchmark`,
`OffsetIndexBenchmark`, `MappedFileBenchmark`, `StorageBenchmark`, `CompactionBenchmark`, `RetentionBenchmark`),
produce path (`ProducePathBenchmark`, `WorkerProduceLatencyBench`, `ProduceMajorityAckBench`, `ProduceValidateBench`,
`MultiLeaderProduceBench`, `GatewayBench`, `GatewayDataBench`), replication and consensus (`ReplicationBenchmark`,
`PeerFrameBench`, `ConsensusAgentBenchmark`, `ElectionBenchmark`, `ClusterMemberBenchmark`), transactions and state
(`TransactionBenchmark`, `ProducerStateBenchmark`, `LeaderEpochCacheBenchmark`, `MetadataApplyBench`,
`RoutingImageBench`), views (`ViewFoldBench`, `ViewTableBench`, `ViewCheckpointBench`), catalog (`CatalogHotPathBench`,
`CatalogBootReplayBench`) and authentication (`AuthCheckBench`). The produce-to-ack figure is the one to watch:
`WorkerProduceLatencyBench` measures the worker's produce latency distribution.

`broker/bench/micro_repl_send.cpp` is a standalone, pinned, single-threaded microbenchmark of the replication
FetchResponse send path (copies per frame, with zero allocation); it is not part of the CMake build. Compile it against the
broker headers and pin it with `taskset`; ns/op is box-independent.

## Client benchmarks

Under `client-cpp/` with `-DBUILD_BENCHMARKS=ON`: `rune_client_bench` (ping), `rune_client_produce_bench`,
`rune_client_group_bench`, `rune_producer_encode_bench`, `rune_producer_accum_bench`, `rune_producer_acked_bench`,
`rune_producer_handle_bench`, `rune_producer_flush_scaling_bench`, `rune_m2o_claim_bench`, `rune_shard_route_bench` and
`rune_typed_topic_send_bench`. None needs a broker: ping and produce round trips run against an in-test gateway echoer over
`aeron:ipc` loopback, the rest run fully in process. They measure the client's own cost and several carry gates (for example
`p99 < 100 µs` and zero allocations on the hot paths); run them pinned (`taskset -c`) on Linux.

## Throughput clients

`client-cpp/examples/rune_produce_throughput` and `rune_consume_throughput` drive the **real client to a real broker** over
Aeron at maximum rate and report records per second, MB per second and, in acked mode, p50 and p99 ack latency. Build
them with `-DBUILD_EXAMPLES=ON`. They read their configuration from the environment:

| Variable | Meaning |
|---|---|
| `RUNE_TARGET` | Gateway endpoint, for example `aeron:udp?endpoint=127.0.0.1:40200` (required). |
| `RUNE_EGRESS` | This client's egress channel (required). |
| `RUNE_DATA_DIR` | The client's Aeron directory (required). |
| `RUNE_CLIENT_TOPIC_TABLE` | `name:topicId:partitions` of the topic to use, for example `bench:0:8`. |
| `RUNE_BENCH_RECORDS`, `RUNE_BENCH_VALUE_BYTES`, `RUNE_BENCH_KEYS`, `RUNE_BENCH_WARMUP` | Records (default 2 000 000), payload (256), keys, warmup (100 000). |
| `RUNE_BENCH_MODE` | `void` (default, fire and forget) or `acked` (bounded outstanding, with ack latency). |
| `RUNE_BENCH_WINDOW` | Acked mode: outstanding records (default 200 000). |
| `RUNE_ACKS`, `RUNE_LINGER_US`, `RUNE_BATCH_BYTES`, `RUNE_BATCH_RECORDS`, `RUNE_MAX_IN_FLIGHT` | Producer knobs. |
| `RUNE_USER`, `RUNE_PASSWORD_FILE` | Credentials; every broker has authentication on. |

With arguments, `rune_produce_throughput` runs the invoker-shard open-loop bench instead (`--rate`, `--mode`, see
`examples/bench/InvokerBench.h`). Compare with Kafka at the **same record size**: throughput at 1 KiB and at 64 B differ by
several times.

## Rune versus Kafka

`broker/bench/kafka-compare/` drives Rune and Apache Kafka over a matched-durability matrix on one box, sequentially, and
writes `results.jsonl`, then `results.json` and `specs.json`. It is a Linux harness.

| Tier | Rune | Kafka |
|---|---|---|
| D0 | `topic bench partitions P with (tier = leader)`, acks leader | `acks=1` |
| D1 | `tier = majority`, acks all | `acks=all`, RF 3, `min.insync.replicas=2` |
| D2 | `tier = strict` (fdatasync), acks all | `acks=all`, `flush.messages=1` |

```bash
cd broker/bench/kafka-compare
./install-kafka.sh          # JDK + Kafka, once (KAFKA_VERSION, default 4.0.0)
./smoke.sh                  # a tiny end-to-end run: checks the harness and the JSON schema, not performance
./smoke-matrix.sh           # a tiny run of every matrix phase
./run-matrix.sh             # the full grid
python3 collect.py results.jsonl results.json     # merge repeats into median, min, max per cell
```

- **Phases** (toggle with `DO_HEADLINE`, `DO_WRITESCALE`, `DO_WORKERSCALE`, `DO_READ`, `DO_PERSIST`, `DO_LATENCY`): headline
  (partitions and workers fixed), write scaling over partitions and record size, worker scaling, read, persistence (`nvme`
  against `tmpfs`) and latency. Sweeps are space-separated lists: `TIERS`, `HEAD_REC`, `PART_SWEEP`, `REC_SWEEP`,
  `WK_SWEEP`, `CORE_SWEEP`, `CONS_SWEEP`; sizes `REC`, `LATREC`, `WARMUP`, `PARTS_MAIN`, `WK_MAIN`.
- **Configuration** is in `env.sh`, every value overridable from the environment: `RUNE_BROKER_BIN`, `RUNE_PROD_BIN`,
  `RUNE_CONS_BIN`, `RUNE_CLI`, `KAFKA_HOME`, `WORK` (tmpfs scratch, default `/dev/shm/kbench`), `RESULTS_DIR`,
  `DISK_ROOT`, `TMPFS_ROOT`, `CORES_BROKER` and `CORES_CLIENT` (core pinning with `taskset`, for example `0-7` and `8-11`),
  the ports, `KAFKA_VERSION`, `KAFKA_HEAP`, `REPEATS` (default 3).
- **Authenticated.** The harness runs Rune with authentication on, as production does: it generates a cluster key, bootstraps
  the first admin and creates the `bench` user (`RUNE_USER=bench`, password file under `$WORK/auth`), then creates the topics
  with the `rune` CLI.
- **Output.** `results.json` has one cell per `(tool, scenario, tier, record_bytes, partitions, cores, workers, consumers,
  persistence, nodes, idle)` with median, min and max throughput (`throughput_msgs_per_sec`, `throughput_mb_per_sec`) and
  median `p50_us`, `p99_us`, `p999_us`, plus DR timings (`elect_ms`, `resume_ms`, `acked_lost`, `restart_s`).
  `specs.json` records the box. `check-auth-config.sh` is a regression check (run by ctest as
  `BenchLaunchersAreAuthCompatibleRegression`) that the launchers use a topology authentication accepts.
- **Co-located clusters.** `cluster.sh` starts 3-node Rune and Kafka clusters on one box over loopback: it measures
  replication-protocol cost and failover, not cross-host network latency.

Numbers from a shared or loaded box are meaningless. For real numbers use a dedicated machine, pin the broker off the
client cores, and clean up first.

## Replication benchmark (`replbench.sh`)

```bash
broker/bench/kafka-compare/replbench.sh [parts] [recs] [window]       # defaults 4, 1 000 000, 8000
```

A 3-node replication throughput run with per-stage flight-recorder histograms, for the `acks=all` path: leader receive,
last replica append, commit, client ack. It refuses to run when `load1 >= 1.0` (`REPL_ALLOW_BUSY=1` overrides). Tunables
(`REPL_PARTS`, `REPL_RECS`, `REPL_WINDOW`, `REPL_WORKERS`, `REPL_VBYTES`, `REPL_LINGER_US`, `REPL_BATCH_BYTES`,
`REPL_WARMUP`) and the Aeron buffer sizes are environment variables; the flight recorder is armed on all three brokers and
dumped with `SIGUSR1` after the run. Turn a trace into commit and ack latency with
`python3 frwaterfall.py 'fr-trace-*.json'`. The script has VM paths hard-coded in its first lines (its checkout and build
directory); edit them for your box.

## Crypto benchmarks

```bash
build-crypto-bench/rune_crypto_bench                           # AES-256-GCM seal + open of 4096 bytes, ns/op, with a gate
RUNE_CRYPTO_BENCH_BUDGET_NS=3000 build-crypto-bench/rune_crypto_bench
build-crypto-bench/rune_crypto_scram_bench                     # SCRAM-SHA-512: client salting, server verify, a two-verifier exchange
docker build --platform=linux/amd64 -f crypto/docker/Dockerfile.bench -t rune-crypto-bench crypto   # x86_64 AES-NI image
```

`RUNE_CRYPTO_BENCH_BUDGET_NS` bounds the AEAD seal-plus-open ns/op; the benchmark exits non-zero over it, and the image build
fails with it (`--build-arg RUNE_CRYPTO_BENCH_BUDGET_NS=...`). Calibrate the gate on the real box. The SCRAM benchmarks size the
iteration default (4096): a full login is a few milliseconds at most.

## Library benchmarks

| Binary | From | Measures |
|---|---|---|
| `rune_codec_bench` | `codec/` (built when Google Benchmark is found) | Validator, catalog replay, security replay. |
| `rune_ql_bench` | `ql/` (built when Google Benchmark is found) | Parsing. |
| `rune_protocol_benchmark` | `protocol/` with `-DBUILD_BENCHMARKS=ON` | Client frame codecs, CRC-32C, varints, record batches. |
| `flight_recorder_bench` | the flight-recorder repository | The recorder's own overhead per span. |

## On the VM

Prepare and restore the box with the scripts and method of [broker/bench/BENCHMARKING.md](../broker/bench/BENCHMARKING.md)
(`vm-bench-prepare.sh`, `vm-bench-restore.sh`): they apply the loopback `NOTRACK` fast path, flush iptables, stop k3s and set the
socket-buffer sysctls, and undo all of it.

Throughput and latency numbers are taken on the Linux VM, on an idle box, serialized by the host lock
([testing.md](testing.md#running-on-the-remote-vm)):

```bash
ssh root@$RUNE_VM_HOST 'flock /root/vm.lock bash -c "broker/scripts/vm-clean.sh && <benchmark command>"'
```

`vm-clean.sh` kills leftover processes, clears flight-recorder scratch, waits for `load1 < 1` and checks disk and
`/dev/shm` space, and exits non-zero (do not benchmark) when the box is not ready. Copy the build to the VM, run the
benchmark, copy `results.json` back, and remove what you created.
