# rune-protocol — architecture & working agreement

> **What this is:** the **header-only C++23 library** that is the single source
> of truth for every Rune wire and on-disk encoding. Both the broker
> (`rune-broker`) and the client (`rune-client-cpp`) include these headers so
> the two ends can never disagree on a byte layout. No `.cpp` compilation unit
> ships (only `src/dummy.cpp` exists so CLion indexes the headers); the Conan
> package is effectively `INTERFACE`.

Released independently as a Conan package. Consumed by siblings via the source
include path `../rune-protocol/include` (that is how the broker and client
resolve it during development).

---

## 1. Two layers of encoding

The library has two distinct concerns, in two namespaces:

### 1.1 `rune::protocol` (root) — the record/data format
The **on-disk AND on-wire record encoding**, ported faithfully from Kafka's
**RecordBatch v2**, but **all-little-endian** (Rune unified BE→LE across the
project — the byte *offsets* match Kafka; only endianness changed).

- **`RecordBatch.h`** — the batch is the unit of append, replication, fetch, and
  archival. 61-byte header then delta-encoded records. Offset constants
  (`RecordBatch.h`):
  ```
  baseOffset:8  length:4  partitionLeaderEpoch:4  magic:1(=2)  crc:4(CRC-32C)
  attributes:2  lastOffsetDelta:4  baseTimestamp:8  maxTimestamp:8
  producerId:8  producerEpoch:2  baseSequence:4  recordCount:4  records...
  ```
  Deliberate carry-overs from Kafka:
  - `partitionLeaderEpoch` sits **before** the CRC region and is **excluded from
    the CRC** — the broker stamps the epoch on arrival without recomputing CRC.
  - `producerId`/`producerEpoch`/`baseSequence` live in the header — the basis
    for idempotency/EOS, **reserved from day one**.
  - `magic` byte before the CRC → self-describing, versionable.
  - `attributes` bits: `kAttrControl` (0x20, control batch),
    `kAttrTransactional` (0x10).
- **`Crc32c.h`** — CRC-32C (Castagnoli), hardware-intrinsic (SSE4.2 / ARMv8)
  where available. CRC covers `attributes` onward (offset 21).
- **`Varint.h`** — zigzag varint encode/decode for the delta-encoded records.
- **`PartitionHash.h`** — `hash(key) % partitions` for keyed partition routing
  (must match on both ends → strict per-key ordering).
- **`Errors.h`** — shared internal error codes.

### 1.2 `rune::protocol::client` — the client↔gateway wire protocol
The framed request/response protocol a client speaks to the broker gateway.
SBE-style flat fixed layouts, little-endian, zero-alloc, `noexcept`.

- **`Protocol.h`** — the contract constants:
  - `kSchemaId = 120` (client-facing; distinct from internal 111–114),
    `kProtocolVersion = 1`.
  - `TemplateId` with **reserved ranges**: control `1–15`, produce/consume
    `16–31`, groups/txn `32–63`, view `64–95`. Assigned today: Connect(1),
    ConnectResponse(2), Keepalive(3), Disconnect(4), ErrorResponse(5), Ping(6),
    Pong(7); Produce(16)…Unsubscribe(21); JoinGroup(32)…Assign(44).
  - `ErrorCode` — `OK`, `NotLeader`, `Redirect`, `SessionExpired`,
    `MinIsrViolation`, plus consumer-group codes (`StaleGeneration`,
    `RebalanceInProgress`, `NotCoordinator`, …).
- **`MessageHeader.h`** — fixed **24-byte** LE header on every frame:
  `blockLength:2 templateId:2 schemaId:2 version:2 sessionId:8 correlationId:8`.
  Also the shared `detail::putLE*/getLE*` byte helpers.
- **`Codec.h`** — additional LE helpers + var-field primitives (`putVarBytes` =
  u16 length prefix + bytes; bounds-safe `getVarBytes`).
- **`Dispatch.h`** — `checkFrame(buf, avail)` → `FrameCheck`: read-only,
  bounds-safe, zero-alloc, `noexcept`, **forward-compatible**. Rejects (never
  mis-parses) in a fixed order: `TooShort` → `WrongSchema` →
  `UnsupportedVersion` → `UnknownTemplate` → `Ok`. Unknown template ids in the
  reserved gaps are cleanly rejected.
- **Message groups** (encode/decode structs per template):
  `ControlMessages.h` (Connect/Disconnect/Keepalive/Error), `PingMessages.h`,
  `DataMessages.h` (Produce/Subscribe/Records), `GroupMessages.h` (join/sync/
  heartbeat/leave), `OffsetMessages.h` (commit/fetch). `ArrayView.h` is a
  zero-copy span helper.

---

## 2. Design invariants (why it's header-only & LE)
- **One framing end-to-end:** a produced batch is the same bytes that get
  replicated, fetched, and recorded — no re-serialize across stages. Keeping the
  format header-only means broker and client compile against identical layout
  constants.
- **All little-endian:** the whole project was unified to LE; do not reintroduce
  big-endian fields. Byte offsets stay Kafka-compatible for readability.
- **Forward compatibility:** new template ids are added from the reserved ranges
  and `Dispatch.h::isKnownTemplate()` is extended; older peers reject unknown
  ids rather than crash.
- **Zero-alloc / noexcept on encode/decode:** these run on the hot path in both
  broker and client.

---

## 3. Build & test

Header-only, but the test/benchmark targets compile against the headers.
Cross-compile to **x86_64** on ARM Macs; Conan 2 + CMake ≥3.31, C++23.

```bash
conan install . --build=missing -s build_type=Debug -s arch=x86_64
cmake . -B build -DCMAKE_OSX_ARCHITECTURES=x86_64 \
  -DCMAKE_TOOLCHAIN_FILE=build/Debug/generators/conan_toolchain.cmake \
  -DCMAKE_PREFIX_PATH="$PWD/build/Debug/generators" \
  -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON
cmake --build build -j
cd build && ctest --output-on-failure
```

- Unit tests: `tests/protocol/` — CRC-32C, varint, RecordBatch round-trip,
  partition hash, and per-message-group client codec tests.
- **`ConformanceTest.cpp`** validates encodings against golden vectors in
  `conformance/` (`RUNE_CONFORMANCE_DIR`) — the cross-implementation contract.
  Keep these in sync when the wire format changes.
- Coverage target: ≥90% line (100% on pure logic).
- `-DBUILD_BENCHMARKS=ON` builds `rune_protocol_benchmark` (CRC, varint, batch,
  client codec).
- **Never** run `conan create/export/upload` locally — CI publishes packages.

**Changing the format is a cross-repo event:** a layout change here must be
mirrored in `rune-broker` and `rune-client-cpp`, and the conformance vectors
updated.

---

## 4. Engineering law — Aeron/Agrona is the bible

The encoders here embody Rune's global engineering law: flat SBE-style fixed
layouts, reusable encoder/decoder state (zero per-message allocation),
zero-copy, correct little-endian byte order. See `rune-broker/CLAUDE.md` §7 for
the full statement (threading, fences, idling) that governs every Rune repo.

---

## 5. Session workflow — keep the live dashboard updated

Rune streams its development **live** — https://rune.regressorspath.com (fed by
the `rune-live` server). **Every session must keep it updated.**

1. **Tickets — Planka (`planka` MCP).** Board **"Rune / Roadmap"**
   (id `1861164569347490826`): Backlog · In Progress · In Review · Done. Create
   a card before starting; move it as you progress.
2. **Chat:** `scripts/rune-live.sh chat plan "..."` (kinds `plan`/`thought`/`status`).
3. **Tests/benchmarks:** push results with a *stable* suite name as first arg
   (e.g. `rune-protocol/codec`) — the dashboard folds re-runs by it:
   ```
   scripts/rune-live.sh test "rune-protocol/codec" passed \
     passed=N failed=0 total=N commit=$(git rev-parse --short HEAD)
   ```
   Override `RUNE_PUSH_TOKEN` / `RUNE_LIVE_URL` as needed; formats in
   `rune-live/README.md`.
