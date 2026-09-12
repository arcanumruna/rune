# rune-client-cpp

The C++ Rune client library. Mirrors `rune-broker`'s Conan2/CMake/C++23 build system so later tickets can add the codec adapter, transport layer, session state machine, and public API. The `rune-protocol` headers are consumed via the sibling include path (`../rune-protocol/include`) exactly as in `rune-broker`.

## Build

```bash
# Resolve Conan dependencies (uses x86_64/gnu17 profile for binary cache matching on macOS)
conan install . -of build -s build_type=Debug -s arch=x86_64 -s compiler.cppstd=gnu17 --build=missing

# Configure and build with tests
cmake -S . -B build -DBUILD_TESTING=ON \
    -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake \
    -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators"
cmake --build build -j2

# Run tests
cd build && ctest --output-on-failure
```
