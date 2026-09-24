# Rune E5 Per-Worker Ingress — Benchmark Results (2026-09-25)

**Run autonomously overnight.** Broker HEAD `cde0884` (E5a per-worker ingress; all
unit tests 2250/2250, dr-e2e per-worker suite A/B/C/D green).

## TL;DR — target MET

- **Target:** 500k rec/s + <1ms batched latency on **4 cores**.
- **Result (dedicated 4-core VM):**
  - **Throughput:** per-worker produce path **~661k rec/s** (256B, 4 pinned workers)
    and gateway batched path **807k rec/s** — both **exceed 500k/s**.
  - **Latency:** produce→ack **p50 2.75µs, p99 7.18µs** (worker path) /
    **p50 4.29µs, p99 14.26µs** (gateway path) — **far under 1ms.**
  - At 1KiB payloads the 4-worker aggregate is ~392k rec/s (byte-bandwidth bound);
    the gateway batched path stays at 807k rec/s.

## What was measured

In-process Google-Benchmark harness (`rune_broker_benchmark`) exercising the REAL
production produce path:
- `BM_WorkerProduce_Majority_*` — Worker::tryEnqueueOp → doWork → log append → ack
  (the thread-per-core per-worker path). Majority durability tier (ack on append/HW).
- `BM_WorkerProduce_Strict_*` — same, but ack on **io_uring fsync** completion
  (durabilityTier=2). Real io_uring on Linux (kernel 5.15).
- `GatewayDataFixture/Produce*` — gateway produce path (batched), p50/p99/p999.

io_uring is native (portable=false path) on both Linux VMs. Per-worker **stream**
delivery (client→broker Aeron per-worker ingress) is validated for CORRECTNESS by
the dr-e2e suite (tests A/B/C/D green); its network throughput is not measured here.

## Dedicated VM (clean numbers)

Hetzner **CCX23** — 4 **dedicated** x86 vCPU @ 2400MHz, 16GB, Ubuntu 22.04,
kernel 5.15 (io_uring), idle (load 0.26). Provisioned + destroyed within the hour.

| Benchmark | Throughput | p50 | p99 | p999 |
|---|---|---|---|---|
| WorkerProduce Majority 256B (solo) | 241k/s | 2.75µs | 7.18µs | — |
| WorkerProduce Majority 1KiB (solo) | 148k/s | 2.99µs | 7.52µs | — |
| **WorkerProduce Majority 256B (4 pinned workers)** | **~661k/s** (4×~165k) | — | — | — |
| WorkerProduce Majority 1KiB (4 pinned workers) | ~392k/s (4×~98k) | — | — | — |
| WorkerProduce Strict 256B (io_uring fsync/ack) | ~968/s | 1022µs | 1.48ms | — |
| WorkerProduce Strict 1KiB (io_uring fsync/ack) | ~1042/s | 955µs | 1.50ms | — |
| Gateway Produce (single, batched) | **807k rec/s** | 4.29µs | 14.26µs | 52.7µs |

## Existing VM (first pass, shared box — noisier)

build-agent-01 — 4 **shared** x86 vCPU @ 2100MHz, load ~2 (concurrent CI).

| Benchmark | Throughput | p50 | p99 |
|---|---|---|---|
| WorkerProduce Majority 256B (solo) | 143k/s | 4.06µs | 13.96µs |
| WorkerProduce Majority 1KiB (solo) | 85k/s | 5.36µs | 20.83µs |
| Gateway Produce (single, batched) | 804k rec/s | — | — |

## Notes / caveats

- **Scaling:** per-worker throughput drops from 241k/s (solo) to ~165k/s when 4 run
  in parallel — shared L3 / memory-bandwidth contention, not lock contention
  (shared-nothing). Aggregate still clears 500k/s at 256B.
- **Strict tier** (fsync-per-ack) is storage-bound (~1k/s, p99 ~1.5ms) — expected;
  the production default is Majority (ack on append/HW).
- **acks=all in-process fixture** and the **3-node MultiLeader** benchmark error on
  warm-up (they need real replication/duty-cycle threads not spun by the in-process
  bench). The real acks=all path is proven functionally green by dr-e2e A/B/C/D.
- **Config used:** io_uring native; rings at defaults (32 MiB ingress/return per
  worker, 8 MiB memlog — already ≥ a few MB); 4 workers via 4 pinned processes.
  A true single-process 8-partition/4-worker network throughput bench (client→broker
  over per-worker Aeron streams) is the natural next step (needs a native
  broker+load-client harness on the VM; not built tonight).

## Reproduce

Build (Linux x86, conan cache present):
```
conan install . --output-folder=conan-release --build=missing -s build_type=Release -s compiler.cppstd=23
cmake -S . -B build-bench -G Ninja -DCMAKE_TOOLCHAIN_FILE=conan-release/build/Release/generators/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=ON
cmake --build build-bench --target rune_broker_benchmark
```
Run:
```
./build-bench/rune_broker_benchmark --benchmark_filter="BM_WorkerProduce_.*|GatewayDataFixture/Produce.*"
# 4-core aggregate:
for w in 0 1 2 3; do taskset -c $w ./build-bench/rune_broker_benchmark --benchmark_filter="BM_WorkerProduce_Majority_256B" --benchmark_min_time=1.0s & done; wait
```
