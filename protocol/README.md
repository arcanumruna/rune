# rune-protocol

Header-only C++ protocol definitions and type contracts for the Rune message broker: shared wire/on-disk record-batch v2 format with CRC-32C, zigzag varint, batch encode/decode, and error codes.

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

Expected: all tests pass; target ≥90% line coverage (100% on pure logic).

## Design

See [the rune-broker design spec](../rune-broker/docs/specs/2026-09-10-rune-broker-design.md) for protocol and integration details.
