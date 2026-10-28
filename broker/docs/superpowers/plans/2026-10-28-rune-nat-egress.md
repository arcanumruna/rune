# Rune — NAT-safe egress Implementation Plan

**Spec:** [2026-10-26-rune-nat-egress-design.md](../specs/2026-10-26-rune-nat-egress-design.md) (D1–D7).

**Goal:**
- Every per-session reply (ConnectResponse, acks, DDL, errors, metadata and, under authentication, Records) reaches a
  client behind NAT over a flow the client opened: an Aeron response channel.
- Older clients keep the declared-channel path.
- The session's source address comes from the Aeron image, never from the client.
- The produce and consume hot paths are unchanged, proven by ABAB.

**Execution:**
- Branch `claude/laughing-dijkstra-yoo44k`; merge `main` in before each phase. Commits carry the session trailers.
- Implementers (mid-tier model) get an exact recipe per task; a reviewer checks each task.
- Per task: build the touched targets and run the named tests.
- Checkpoints, run the full `-R Regression -LE latency` (broker and client) plus ASan/TSan in CI:
  - **A** after Task 1, the Aeron upgrade alone;
  - **B** after Task 6;
  - **C** after Task 9.
- The Docker NAT e2e runs on the shared Linux test VM (`$RUNE_VM_HOST`; lock held, own directory only, own containers
  only, `df` before and after).

## Interaction matrix
See spec §4. Every cell maps to a test in Tasks 5–9:
- a new or old client × auth on or off × plain or TLS × 1 or 3 nodes;
- two clients behind one NAT;
- a NAT port change mid-session;
- a subscribe before the response publication resolves;
- connect racing a leader change.

## Review focus
See spec §5.

## Tasks

### Task 1: Aeron 1.41.4 → 1.51.0 (D1)
- **Change:**
  - `conanfile.txt`, `broker/conanfile.py`'s requirement data and `client-cpp/conanfile.py`: `aeron/1.51.0`.
  - Fix every C++ client API change the compiler reports.
  - Rename nothing of Rune's.
- **Gate:**
  - the full regression suite, broker and client;
  - `MdcEgressSpikeTest` and every `AeronPump` test;
  - an ABAB bench of the 3-node acks=all produce and the single-node point read on the default config against the
    1.41.4 baseline (BENCHMARKING.md counts).
- A regression is root-caused before anything builds on 1.51.
- Commit: `build: Aeron 1.51.0 (NAT-safe egress D1)`.

### Task 2: response-channel spike (gates D3)
- **Test:** `broker/libs/node/tests/integration/ResponseEgressSpikeRegressionTest.cpp`, real media drivers over
  loopback UDP.
  - **`OneControlPortServesManyClientsRegression`:** one server request subscription and N = 8 client drivers. The
    server creates one response publication per image, all on ONE response-control endpoint. Each client gets only
    its own messages: a payload tag per client, over 1,000 messages each.
  - **`ReplyFollowsTheObservedSourceRegression`:** the client's request publication binds a source port different
    from its response subscription. The reply still arrives (the response follows the client's flow, not a declared
    address).
  - **`ClientGoneEndsTheResponsePublicationRegression`:** the client closes. The server's response publication goes
    not-connected within the liveness timeout and is closed with no leak (`countersReader` count back to baseline).
- **Decision recorded in the spec:** a shared control port works, so one port per publisher thread; otherwise stop
  and redesign D3.

### Task 3: protocol (D4)
- `protocol/include/rune/protocol/client/ControlMessages.h`:
  - `kClientVersionResponseEgress`;
  - ConnectRequest's trailing `responseCorrelated` byte, version-gated and absent below it.
- `MetadataMessages.h`: a per-broker `responseControlPort` and a per-partition `responseControlPort` (the owner
  worker's), version-gated.
- Conformance vectors (`protocol/conformance/gen_vectors.cpp`), and old/new round-trip tests in both directions.

### Task 4: config and ports (D3)
- `deriveShardBasePorts` gains the response-control base. `NodeConfig` gains `RUNE_RESPONSE_CONTROL_BASE_PORT` and
  `RUNE_GATEWAY_RESPONSE_CONTROL_PORT`.
- Wire them into `nodeBoundPorts` and `nodeEndpointHosts`, `RUNE_PEERS` advertising, Metadata, and the startup port
  collision checks.
- Document them in `docs/configuration.md` and `docs/operations.md` (firewall table).

### Task 5: gateway (D2, D4)
- **New client** (`clientVersion >= kClientVersionResponseEgress` and `responseCorrelated`):
  - `registerConnectEgress_` creates `aeron:udp?control-mode=response|control=<gateway response-control>|
    response-correlation-id=<image correlation id>`;
  - the image is found from the ConnectRequest fragment's `Header.sessionId` on the client subscription;
  - no host resolve.
- **Old client:** the unchanged declared path.
- **Both paths:** the session's source address comes from `image.sourceIdentity()` (fixes #227;
  `DdlPrincipalSourceIsTheImageRegression`).
- **Per-(session, worker) record egress, legacy mode:** the same response publication rule.
- **Tests:**
  - `ResponseEgressGatewayRegression.*`: a new client connects with an unroutable declared channel and still gets its
    ConnectResponse;
  - an old client still works;
  - two clients from one host on different ports never see each other's replies.

### Task 6: workers, shard mode (D2, D6)
- `SessionAnnouncer` ships the client's ingress image key (the client's Aeron `sessionId` on each worker's ingress)
  in place of the channel for new clients.
- On its own thread, the worker creates:
  - the `SessionAckSink` response publication;
  - the per-session Records response publication.

  Both use the bounded `WorkerEgress`-style resolve.
- A subscribe that arrives before its response publication resolves parks, as #118's `AwaitingEgress` does today.
- **Tests:**
  - `ResponseEgressWorkerRegression.*`: acks and Records reach a new client on response publications only;
  - a park-then-resolve subscribe;
  - a worker whose client image times out closes the publication.

### Task 7: client (D7)
- `AeronTransport`:
  - per broker endpoint, the response subscription first, then the request publication with
    `response-correlation-id`;
  - per worker, the ingress publication is opened for every worker the client produces to OR consumes from (sparse
    64 KiB terms).
- `SessionMachine` sends `responseCorrelated`, and falls back to the declared channel when the broker's version is
  older.
- `ShardConfig` stops offsetting egress ports for response egress.
- **Tests:** client unit tests for the version fallback and the subscription/publication order.

### Task 8: integration slice
- 1-node and 3-node, real UDP:
  - the spec §4 matrix cells;
  - mixed old and new clients;
  - TLS sessions;
  - authentication on and off;
  - a leader change during connect;
  - a client source-port change mid-session (a new client driver): the old session ends, the new one connects, and
    nothing is sent to the stale port.
- Named `NatEgressMatrixRegression.*`.

### Task 9: NAT e2e, bench, docs
- **NAT e2e** (`broker/docker/nat-e2e.sh`):
  - a docker network with a router container doing masquerade;
  - the client in a private network that reaches the brokers only through it;
  - default config, 3 nodes;
  - produce, consume, group and transaction, DDL, and two clients behind the same NAT.
- **ABAB bench** before and after on the default config; record the baseline in BENCHMARKING.md.
- Update the docs: `docs/operations.md` (ports, NAT), `docs/configuration.md`, and the release notes.
