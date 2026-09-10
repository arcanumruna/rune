# rune-broker

The regressors' distributed log broker: C++ storage engine and ingress gateway for the Rune protocol. Implements the partition-log contract (record-batch v2, leader-epoch stamping, offset indexing, retention) with single-node produce/consume, recovery, and latency benchmarking.

## Build

Install Conan dependencies and generate CMake toolchain, then configure and build:

```bash
conan install . --build=missing -s build_type=Debug -s arch=x86_64
cmake . -B build -DCMAKE_OSX_ARCHITECTURES=x86_64 \
  -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake \
  -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators" \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build -j
```

## Test

Run the unit test suite:

```bash
cd build && ctest --output-on-failure
```

Expected: 65/65 tests pass.

## Benchmark

Build with benchmarks enabled and run the latency harness:

```bash
cmake . -B build -DCMAKE_BUILD_TYPE=Debug -DBUILD_BENCHMARKS=ON \
  -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake \
  -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators"
cmake --build build -j
./build/rune_broker_benchmark --benchmark_min_time=0.01s
```

P99 latency target: <100µs (becomes a hard CI gate at Phase 3).

## Design

See [docs/specs/2026-09-10-rune-broker-design.md](docs/specs/2026-09-10-rune-broker-design.md).
