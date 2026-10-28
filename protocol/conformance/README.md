# Conformance Vectors

This directory contains the **golden binary frames** (`.bin`) for every Rune client protocol message type.  These files ARE the binding wire contract shared between `rune-protocol`, `rune-client-cpp`, and `rune-client-nodejs`.

## Contents

```
conformance/
  vectors/               — 10 committed golden .bin files
    connect_request.bin                  (67 bytes)
    connect_request_empty_channel.bin    (34 bytes)
    connect_request_v16_response.bin     (43 bytes, NAT-safe egress)
    connect_response.bin                 (38 bytes)
    connect_response_redirect.bin        (38 bytes)
    connect_response_v3.bin              (42 bytes, brokerVersion 3; the two above are the v0 form)
    keepalive.bin                        (24 bytes)
    disconnect.bin                       (26 bytes)
    error_response.bin                   (53 bytes)
    error_response_empty.bin             (28 bytes)
    ping.bin                             (32 bytes)
    pong.bin                             (32 bytes)
  manifest.md            — exact field values + byte layouts for every vector
  README.md              — this file
  gen_vectors.cpp        — one-shot generator (run once, then kept for reference)
```

## Wire Format

All integers are **little-endian**. Every frame begins with a 24-byte `MessageHeader`:

```
Offset  Width  Field
0       2      blockLength   (uint16 LE) — byte count of the fixed body (NOT including header or var fields)
2       2      templateId    (uint16 LE) — identifies the message type
4       2      schemaId      (uint16 LE) — always 120 for client-facing messages
6       2      version       (uint16 LE) — always 1
8       8      sessionId     (int64  LE)
16      8      correlationId (int64  LE)
```

Variable-length fields are prefixed by a `uint16 LE` length, followed immediately by that many bytes (no padding, no NUL terminator).

## How the golden files were generated

The `.bin` files were generated **once** using `conformance/gen_vectors.cpp`, which compiles against the production codec headers in `include/rune/protocol/client/`:

```bash
c++ -std=c++23 -I include conformance/gen_vectors.cpp -o /tmp/rune_gen_vectors
/tmp/rune_gen_vectors conformance/vectors
```

After generation the files were verified by the C++ harness (see below) and committed.  **Do NOT regenerate and recommit unless the codec wire format changes** — the harness will fail on any byte difference.

## How to verify (C++)

```bash
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build -j2
cd build && ctest -R 'Conformance' --output-on-failure
```

The harness (`tests/protocol/client/ConformanceTest.cpp`) locates vectors via a compile-time `-DRUNE_CONFORMANCE_DIR` define so it is cwd-independent.

## How to implement a mirror harness (Node.js or other language)

1. Read `manifest.md` — it lists every field's exact value in decimal and hex.
2. For each vector:
   a. **Decode-golden:** Read the `.bin` bytes. Decode according to the layout. Assert every field equals the manifest value.
   b. **Encode-golden:** Build the message from the manifest values. Encode. Assert the produced bytes are byte-identical to the `.bin` file.
3. Implement the session-flow test: decode `[connect_request, connect_response, keepalive, ping, pong, disconnect]` in order; assert the `templateId` sequence is `[1, 2, 3, 6, 7, 4]`.

## Constants

| Constant          | Value |
|-------------------|-------|
| `kSchemaId`       | 120   |
| `kProtocolVersion`| 1     |
| `sessionId`       | `0x1122334455667788` (all vectors) |
| `correlationId`   | `0x0A0B0C0D0E0F1011` (all vectors) |
