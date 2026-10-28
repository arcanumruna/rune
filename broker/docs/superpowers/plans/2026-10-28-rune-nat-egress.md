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
- **Status: done.** `MdcEgressSpikeTest`, every `AeronPump` test and the transport, driver, GSO, ring and client
  tests are green. The full regression suite, the sanitizers and the ABAB bench are deferred to the next checkpoint
  (issue #736), by the owner's call. 1.51 changes Rune had to absorb:
  - the native C++ client is gone; the header-only wrapper over the C client replaces it;
  - `BufferBuilder` starts empty, so a reassembled message starts at offset 0, not 32
    (`FragmentAssemblerRegression.*`);
  - `aeron_counter_ordered_increment` is gone (`aeron_counter_increment`);
  - transport bindings take a `params` struct; the poller logs a failing socket and polls the rest
    (`EpollFirstPollLogsAFailingSocketAndPollsTheRestRegression`);
  - name resolvers need `start_func`, and an "aeron-executor" thread starts per driver unless disabled
    (`InvokerDriverStartsNoThreadRegression`);
  - the ring-buffer `RecordDescriptor` header is gone (Rune and the flight recorder carry their own copy);
  - the default `network_publication_max_messages_per_send` is 4, not 2.

### Task 2: response-channel spike (gates D3)
- **Test:** `broker/libs/node/tests/integration/ResponseEgressSpikeRegressionTest.cpp`, real media drivers over
  loopback UDP.
  - **`OneControlPortServesManyClientsRegression`:** one server request subscription and N = 8 client drivers. The
    server creates one response publication per image, all on ONE response-control endpoint. Each client gets only
    its own messages: a payload tag per client, over 1,000 messages each.
  - **`ReplyReachesTheResponseSubscriptionRegression`:** the reply is delivered on the client's response
    subscription, and the client's request and response sockets are distinct ports. Following the observed
    (NAT-rewritten) source is proven by Task 8's source-port change and Task 9's NAT e2e.
  - **`ClientGoneEndsTheResponsePublicationRegression`:** the client closes. The server's response publication goes
    not-connected within the liveness timeout and, once closed, no allocated counter is owned by its registration id.
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
- **Layout depends on W.** Worker `w`'s response-control port is `egressBase + 63 - W + w`, so changing `RUNE_WORKERS`
  moves every worker's port. Every node must agree on W (it already must for shard routing); firewall rules cover
  `egressBase + 32 .. egressBase + 63`.

### Task 5: gateway (D2, D4)
- **New client** (`clientVersion >= kClientVersionResponseEgress` and `responseCorrelated`):
  - `registerConnectEgress_` creates `aeron:udp?control-mode=response|control=<gateway response-control>|
    response-correlation-id=<image correlation id>`;
  - the image is found from the ConnectRequest fragment's `Header.sessionId` on the client subscription;
  - no host resolve.
- **Old client:** the unchanged declared path.
- **Both paths:** the session's source address comes from `image.sourceIdentity()` (fixes #227;
  `DdlPrincipalSourceIsTheImageRegression`).
- **Per-(session, worker) record egress, legacy mode (deviation):** keeps the declared channel. One request
  publication correlates exactly one response subscription, so a legacy per-worker Records flow cannot be correlated
  without a new client request flow. Filed as #745; shard mode (the default) is covered by Task 6.
- **Redirect:** for sessions `>= kClientVersionResponseEgress` the Redirect message is
  `leaderClientEndpoint + "#" + leaderGatewayResponsePort` (no suffix when the port is 0).
- **Metadata:** `BrokerEntry.responseControlPort` is filled; `PartitionEntry.responseControlPort` stays 0 until
  Task 6.
- `kBrokerVersion` becomes 9.
- **Tests:**
  - `ResponseEgressGatewayRegression.*`: a new client connects with an unroutable declared channel and still gets its
    ConnectResponse;
  - an old client still works;
  - two clients from one host on different ports never see each other's replies.

### Task 6: workers, shard mode (D2, D6)
- `SessionAnnouncer` ships the client's ingress image key (the client's Aeron `sessionId` on each worker's ingress)
  in place of the channel for new clients.
- On its own thread, the worker creates ONE response publication per (client, worker), carrying both the
  `SessionAckSink` acks and the session's Records, with the bounded `WorkerEgress`-style resolve. One request
  publication (the client's ingress to that worker) correlates exactly one response subscription, so acks and
  Records cannot ride separate response flows.
- A subscribe that arrives before its response publication resolves parks, as #118's `AwaitingEgress` does today.
- **Binding (decided 2026-10-28):** session ids are sequential, so a worker never binds a session to whoever first
  names it. The gateway draws a 16-byte random bind token per response-mode shard session and returns it in a v16
  ConnectResponse tail (on the gateway response publication, so only that client sees it). `SessionAnnouncer` ships the
  token, not a channel. The client sends `WorkerBind{sessionId, correlationId, token}` (TemplateId 86) as the first
  frame on each worker ingress it uses. The worker binds that image once (a second bind never rebinds), opens the
  response publication on its own response-control port, and drops produce frames for the session from any other
  image. Records for the session use that publication and park until it resolves; they never fall back to the MDC.
  `PartitionEntry.responseControlPort` is filled for v16 clients.
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
- The client needs the seed's gateway response-control port before its first connect (a config value, as Aeron
  Archive's response channel). It learns the leader's port from the Redirect `#port` suffix and every broker's port
  from Metadata.
- The client keeps its declared egress subscription, so an older broker can still answer on it.
- client-cpp `ConnectRequest` sends `responseCorrelated = 1`; `kClientVersion` becomes 16.
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
