# Rune

Monorepo for the Rune log/database broker and its client libraries.

| Directory | Contents |
|---|---|
| `broker/` | the broker, its libs, tests, docker and e2e harnesses |
| `client-cpp/` | the C++ client library, CLI and examples |
| `protocol/` | wire format and record-batch encoders (header-first) |
| `codec/` | record codec, schema registry, catalog and security event logs |
| `crypto/` | TLS 1.3, AEAD and SCRAM primitives |
| `ql/` | Rune QL parser and code generators |

The flight recorder lives in its own repository,
[arcanumruna/flight-recorder](https://github.com/arcanumruna/flight-recorder). CMake clones the
pinned commit with FetchContent; for local development pass
`-DFLIGHT_RECORDER_DIR=<checkout>` (docker scripts read `FLIGHT_RECORDER_DIR` too).

## Build

Dependencies come from Conan (`conanfile.txt` at the root is the union of every subproject's).

```sh
conan install . -of build/conan -s build_type=Debug --build=missing
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug \
      -DCMAKE_TOOLCHAIN_FILE=build/conan/conan_toolchain.cmake \
      -DBUILD_TESTING=ON -DFLIGHT_RECORDER_DIR=../flight-recorder
cmake --build build
ctest --test-dir build -j8
```

The root build hosts `broker/` (which compiles the protocol, codec, crypto and ql sources and embeds
`client-cpp/`) and `protocol/`. Build `codec/`, `crypto/`, `ql/` and `client-cpp/` from their own
directories (`cmake -S codec -B build-codec ...`); each subproject has its own `conanfile.py`.

## Environment

Scripts that need a host or credential read it from the environment and fail when it is unset
(for example `RUNE_VM_HOST` for the Linux VM gate and the docker e2e runs).
