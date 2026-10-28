# Rune NAT-safe egress: design

**Status:** approved for planning, 2026-10-28 (first draft 2026-10-26). Split out of
[2026-10-24-rune-namespaces-and-truncate-design.md](2026-10-24-rune-namespaces-and-truncate-design.md) §10.4.
Order: this work, then the Arcanum CLI with arcanum-site auth, then TruncateTopic (namespaces Phase 3).

## 1. Problem
Clients run behind NAT: home routers, cloud NAT gateways, most Kubernetes pods. A broker can only reach such a client
over a flow the client opened. Today every per-session reply goes to an address the client **declares**:

| Path | Code | Carries | NAT-safe today |
|---|---|---|---|
| Gateway session egress | `ClientGateway::registerConnectEgress_` (`addExclusivePublication(egressChannel)`) | ConnectResponse, acks, DDL replies, errors, metadata | no |
| Worker session acks (shard mode, the default) | `SessionAckSink` (`addExclusivePublication` on the announced channel) | produce acks sent straight from the worker | no |
| Per-(session, worker) record egress | `ClientGateway` `recordEgressByWorker` (`addExclusivePublication(sess->egressChannel)`) | Records, under authentication (issue #118) | no |
| Per-worker dynamic MDC | `WorkerEgress` (`control-mode=dynamic`) | Records, authentication off only (dev) | yes, but a broadcast to every joiner |

1. The client sends `egressChannel` (default `aeron:udp?endpoint=localhost:40200`) in ConnectRequest.
2. The gateway resolves its host (`offerHostResolve_`) and opens a publication to it.
3. `SessionAnnouncer` ships the same channel to every worker.

Behind NAT that address is private or unmapped, so the client never gets a ConnectResponse and connect times out.
The declared host is also used as the session's source address in the DDL principal (issue #227): a client can name
any address.

## 2. Decision: Aeron response channels
Aeron 1.41.4 (our pin) has no NAT-safe unicast. Dynamic MDC is NAT-safe, but its destination list belongs to the
send endpoint, so every publication on a control port reaches every joiner. That is the broadcast issue #118
already refused under authentication, and a per-session control port does not scale or fit a firewall.

**Aeron response channels** (`control-mode=response`, stable in the 1.4x line) are Aeron's own answer, built for
Aeron Archive and Cluster clients behind NAT:
- the server's response publication sends to the address its correlated request image came from, through the mapping
  the client's own traffic opened;
- it is unicast: one client, one flow;
- a response publication carries `response-correlation-id=<request image correlation id>`. The client's response
  subscription (`control-mode=response|control=<server response-control endpoint>`) is named on its request
  publication with `response-correlation-id=<subscription registration id>`.

Decisions:
- **D1.** Upgrade Aeron from 1.41.4 to **1.51.0**, the version ConanCenter publishes. This is its own task with a full
  regression run and an ABAB bench of the produce, consume and replication hot paths before anything builds on it.
  The TypeScript transport (`aeron-transport-ts`) targets 1.53.3 and stays wire-compatible.
- **D2.** Every per-session egress becomes a response publication correlated with a request image the client
  opened:
  - **Gateway:** correlated with the client's publication to the gateway's client endpoint, the one ConnectRequest
    arrives on. The gateway takes the image from the fragment's `Header.sessionId`.
  - **Worker acks and records:** correlated with the client's per-worker ingress publication (shard mode, the
    default). A client that only consumes from a worker still opens that worker's ingress publication (sparse
    64 KiB terms). It is its request flow and its keepalive.
  - Aeron's heartbeats and status messages keep the NAT mapping open. No extra keepalive is needed; the client
    liveness timeout stays below typical NAT UDP timeouts (about 30 s).
- **D3.** **Response-control endpoints.** Each publisher thread has one response-control port: the gateway, and each
  worker. They sit in the unused top of the node's egress-control block (decided 2026-10-28): gateway =
  `egressBase + 63`, worker `w` = `egressBase + 63 - W + w` (the dynamic-MDC egress uses `egressBase + w`), valid for
  W <= 31 so the two sets never overlap. They are derived only when the egress-control base is the derived one; an
  explicit base derives none and warns (`RUNE_RESPONSE_CONTROL_BASE_PORT`, `RUNE_GATEWAY_RESPONSE_CONTROL_PORT`; port 0 =
  not available, the node serves new clients on the old path). Peers advertise them as `RUNE_PEERS` segments 7 and 8.
  They are advertised in Metadata next to `egressControlPort`, and added to `nodeBoundPorts`, the MTU host list and
  `docs/operations.md`. Task 2 decides whether several response publications can share one control port; if not,
  the design stops and comes back here.
- **D4.** **Wire.** A new `kClientVersionResponseEgress`:
  - **New clients** send an empty `egressChannel` and a `responseCorrelated` flag. The broker answers only on the
    response publication.
  - **Older clients** keep the declared-channel path unchanged, so mixed versions keep working.
  - The gateway records the session's source address from the image's `sourceIdentity`, never from the declared
    channel. This fixes #227 for both paths.
- **D5.** **The per-worker dynamic-MDC record broadcast** stays for authentication-off (dev) only, exactly as #118
  left it. Under authentication Records always ride the session's own response publication.
- **D6.** **Shard-mode `SessionAnnouncer`** ships the worker the client's ingress image key (the client's
  `sessionId` on that worker's ingress) instead of a channel string. The worker creates the response publication
  itself on its own thread, so it stays single-writer, and resolves it with the same bounded resolve as
  `WorkerEgress`.
- **D7.** **Client.**
  - The `AeronTransport` creates one response subscription per broker endpoint before the request publication.
  - `ClientConfig::egressChannel` stays for older brokers (a broker below the version answers with the declared
    path).
  - `ShardConfig`'s per-instance egress port offsets are no longer needed for response egress.
  - The CLI and `dr_client` need no change beyond the library.

## 3. Hot path
- Acks and Records keep the same single-writer `tryClaim`/`offer` path. Only the publication's channel changes, so
  the produce and consume paths are unchanged. This is proven by the ABAB bench on the default config: 3-node
  acks=all and single-node point read, before and after D2.
- Publication creation stays on the connect path (cold) and the subscribe path (cold), as today.

## 4. Interaction matrix
- **Operations:** connect, produce (acks 0/1/all), subscribe and consume, group and transactional requests, DDL,
  disconnect, reconnect.
- **Clients:** new client, old client (declared channel), mixed in one cluster; plain and TLS-encrypted sessions;
  authentication on and off.
- **Topology:** 1 node and 3 nodes; client behind NAT (masquerade), client on the same host, two clients behind one
  NAT (same public IP, different ports).
- **Timings:**
  - sequential;
  - connect racing a leader change;
  - NAT mapping change mid-session (the client's source port changes): the session ends and the client reconnects,
    with no reply sent to the stale address;
  - a worker restarting a response publication after a client's image times out;
  - a subscribe before the worker's response publication resolves.

Required:
- unit tests for the version-gated wire;
- a 1-node and 3-node broker integration slice over real Aeron UDP;
- a NAT e2e leg on the default config: the client in a docker network behind a masquerading router container, on
  the shared Linux test VM.

## 5. Review focus
- No reply ever goes to a client-declared address for a new client. The source address in principals and logs
  comes from the image.
- Two clients behind one NAT never receive each other's acks or Records.
- Old and new clients in one cluster both work. An old client is never sent a response-only frame.
- No new allocation or virtual call on the ack or Records path. The bench is interleaved ABAB.
- Every new port is advertised, bound, in `nodeBoundPorts` and documented.

## 6. Operations
- **Per node:** the client port, each worker's ingress port, and the response-control ports (gateway and each
  worker); the per-worker egress control ports only when authentication is off.
- **Security groups or firewalls:** inbound UDP on those ports only. No inbound rule is needed on the client side.
