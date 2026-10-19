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

## Catalog DDL and history

Topics exist only through DDL; the broker has no boot-topics setting. DDL is sent to any broker: a
broker that is not the catalog leader answers `Redirect`, and the client follows it (at most
`Client::kMaxDdlRedirects` hops), then retries `NotLeader` for `ClientConfig::ddlRetryBudget`
(10 s by default). Each DDL has a 128-bit request id that stays the same across resends, so a retry
returns the original result.

```cpp
rune::client::TopicOptions topic;               // replicationFactor 0 = every broker
topic.replicationFactor = 3;
rune::client::DdlOptions ddl;                   // statement text, client name, request id
auto r = client.defineTopicAndWait("events", 3, "", {}, false, Persistence::Disk, topic, ddl, 15s);
auto d = client.dropTopicAndWait("events", ddl, 15s);
// also: dropViewAndWait, alterViewAndWait, alterTopicConfigAndWait, createViewAndWait, alterTopicAndWait

auto history = client.catalogHistory(filter, 10s);   // SHOW HISTORY
auto state   = client.catalogAsOf(point, 10s);       // AS OF offset / time / version
auto diff    = client.catalogDiff(a, b, 10s);        // DIFF
```

The catalog reads block and may be called only from the thread that constructed the `Client`;
another thread constructs its own `CatalogReader`. A DDL answered by a broker older than client
version 8 is not routed; upgrade clients before running DDL against a multi-node cluster.

### CLI

```bash
rune ql -e "topic events partitions 3 replicas 3;" --broker rune-1:40200
rune ql -f topics.rql --request-id 0123456789abcdef0123456789abcd00 --broker rune-1:40200
rune ql -e "show history topic events;" --broker rune-1:40200
rune ql -e "diff catalog offset 3 to 9;" --broker rune-1:40200
rune ql -e "topic events partitions 3;" --dry-run      # parse and print the plan, no broker

rune catalog export --broker rune-1:40200 > prod.rcat
rune catalog replay --from prod.rcat --into fresh-1:40200
rune catalog diff --from prod.rcat --to fresh-1:40200   # exit 0 equal, 3 differ, 1 error, 2 usage
```

`-f` runs every statement in order and stops at the first error, naming its statement. See the
broker's `docs/CATALOG.md` for the event model, the error table and the halted-apply runbook.
