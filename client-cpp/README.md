# rune-client-cpp

The C++ Rune client library. Mirrors `rune-broker`'s Conan2/CMake/C++23 build system so later tickets can add the codec adapter, transport layer, session state machine, and public API. The `rune-protocol` headers are consumed via the sibling include path (`../protocol/include`) exactly as in `rune-broker`.

## Install

Releases are tagged `client-cpp-vX.Y.Z`. C++23, Linux x86_64.

**Conan** (recommended):

```bash
conan remote add arcanum <remote URL>
```

```python
# conanfile.py
requires = "rune-client-cpp/X.Y.Z"
```

```cmake
find_package(rune-client-cpp REQUIRED)
target_link_libraries(my_app PRIVATE rune::client)   # brings aeron, spdlog and OpenSSL
```

**Source tarball:** `rune-client-cpp-X.Y.Z.tar.gz` (with its `.sha256`) on the
[GitHub Releases](https://github.com/arcanumruna/rune/releases) page holds the client, the sibling sources it compiles
and flight-recorder. Build with `-DFLIGHT_RECORDER_DIR=<unpacked dir>/flight-recorder`.

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

## Authentication

Version 9 of the client logs in with SCRAM-SHA-512 once per broker session. A cluster has authentication on by
default, so a client without credentials is answered `AuthenticationFailed` (`ClientConfig::authToken` is removed;
`username` / `password` replace it).

```cpp
rune::client::ClientConfig cfg;
cfg.username     = "ws1";
cfg.passwordFile = "/run/secrets/ws1.pw";   // read once at construction; or cfg.password (never logged or sent)
cfg.offerCrypto  = true;                    // credentials travel only over TLS (construction throws without it)
cfg.tlsTrustPem  = caPem;                   // a private CA; unset = the system store
// cfg.allowUnauthenticatedBroker = true;   // accept a dev-mode broker (RUNE_AUTH=disabled); default: refused
```

An authenticating broker serves only TLS sessions. Each session checks the broker certificate against its own
endpoint's host (`tlsExpectHost` overrides it), and with credentials a session never proceeds without TLS.

- `AuthenticationFailed` (44) ends the session; a rate-limited login (`retryAfterMs`) is retried after the hint.
- `NotAuthorized` (45) fails exactly that request and is never re-sent; a subscription whose `READ` was revoked ends
  with `SubscriptionFailReason::NotAuthorized` (a group consumer: `GroupFailReason::NotAuthorized`).
- `SessionRevoked` (46) completes everything in flight and is terminal for the session (no reconnect).
- Security DDL is a `rune ql` statement (`CREATE USER ... WITH PASSWORD`, `GRANT`, ...) or `Client::securityDdlAndWait(plan, ddl, timeout)`.
  The client builds the SCRAM verifier locally (minimum 12 characters, `ClientConfig::minPasswordLength`); the plaintext
  never reaches a broker.

The `rune` CLI takes `--user <u> --password-file <path>` (or `RUNE_USER` / `RUNE_PASSWORD_FILE`) on every command that connects,
`rune security bootstrap --admin <name> --cluster-key-file <path> --broker host:port [--password-file <path>]` for the first
admin, and `rune security gen-cluster-key --out <path>`. The example binaries (`examples/`) read the same two variables;
`rune_produce_subscribe` with `RUNE_MODE=ws` is the world-server-style producer / group consumer used by the broker's
`docker/auth-e2e.sh`. See the broker repo's `docs/SECURITY.md`.

## Transactions

A transactional producer is a plain `Producer` created with `ProducerConfig::transactionalId`; every produce path (`send`,
`trySend`, `batch()`, `tryClaim`, `claimBatch`, a typed claim) takes part. `TransactionalProducer` is removed.

```cpp
Producer tp = client.producer({.acks = Acks::All, .transactionalId = "trade-worker-1"});
client.connectAndWait();
tp.initTransactionsAndWait();
tp.beginTransaction();
tp.send(out, Key(uint64_t{1}), value);
tp.sendOffsetsToTransactionAndWait(offsets, "economy");   // the consumer group's next offsets
tp.commitTransactionAndWait();                            // outputs and offsets, atomically
```

- Outside a transaction a transactional producer refuses every produce path with `SendResult::NotInTransaction`; on a
  plain producer the lifecycle token calls return `OpResult::InvalidArgument` and `beginTransaction()` and the `*AndWait` forms throw `std::logic_error`. A newer producer with the same id fences the old one
  (`TxnState::FatalError`).
- After an abort, `GroupConsumer::seekToCommittedAndWait()` rewinds the consumer to its committed offsets without a restart
  (`seek(partition, offset)` repositions one partition).
- A producer without a `transactionalId` asks the broker for a producer id when it connects; a resend after a reconnect is
  deduplicated.
- `ProducerConfig::tier2Slots` sizes the pool of Tier-2 `batch()` slots (default 64).
- The full API is in `docs/api/client-cpp.md`; `examples/eos_probe.cpp` is the end-to-end exactly-once loop that
  `broker/docker/eos-e2e.sh` runs on 3 brokers.

## Delivery guarantees

| You want | Producer | Consumer |
|---|---|---|
| At most once | `Acks::None`, or no retry of a failed send | commit before handling |
| At least once | `Acks::Leader` or `Acks::All` (a retry never writes a record twice) | commit after handling |
| Exactly once | `transactionalId`: outputs and consumed offsets commit together | `ReadCommitted` readers; `seekToCommitted()` after an abort |

Durability of an acknowledged record depends on the ack level. `Acks::Leader` (the default, like Kafka `acks=1`)
acknowledges once the partition leader has the record: a leader restart or loss before the followers replicate it can
lose acknowledged records. `Acks::All` waits until the in-sync replicas hold it with at least `minIsr` of them, so an
acknowledged record survives any single broker failure while `minIsr` holds. Use `Acks::All` for durability.

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
another thread constructs its own `CatalogReader`. A client older than version 8 cannot follow a DDL Redirect: a non-leader broker answers its DDL
`InvalidRequest`. Upgrade clients before running DDL against a multi-node cluster.

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
