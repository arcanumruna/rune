# Rune Client + Broker Runtime — Milestone A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan ticket-by-ticket. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up a runnable, dockerized 3-node `rune-broker` cluster with a client-facing Aeron endpoint, plus a C++ client (`rune-client-cpp`) that connects and completes a `ping` round-trip through the cluster — Milestone A (transport + protocol parity), with produce/subscribe (B) and consumer-groups (C) as follow-on plans.

**Architecture:** Three build units in dependency order — (1) shared client-facing wire codecs in `rune-protocol`; (2) a broker node runtime + `ClientGateway` + Docker cluster in `rune-broker`; (3) the new `rune-client-cpp` repo. Connection model is Option 3 (single Aeron duplex to a gateway node that proxies internally), with the protocol shaped so direct-to-leader is additive later.

**Tech Stack:** C++23, Aeron C++ client (`aeron::aeron`, embedded media driver), Agrona ring buffers + `IdleStrategy`, CMake + Conan 2, gtest, Google Benchmark, Docker + docker-compose.

**Spec:** `docs/superpowers/specs/2026-09-11-rune-client-and-broker-runtime-design.md` (read it alongside this plan).

## Global Constraints

Every ticket's requirements implicitly include this section.

- **Aeron/Agrona is the bible (non-negotiable, all units).** Follow Aeron Cluster's Java implementation patterns EXACTLY for threading, encoders, fences, and low-level optimizations:
  - **Threading:** single-writer agents driven by a `doWork(nowNs)` loop under an `IdleStrategy`; no locks on the hot path; each piece of state owned by exactly one thread; inter-thread handoff ONLY via Aeron/Agrona lock-free ring buffers (`ManyToOne`/`OneToOne`) — never mutexes/condvars.
  - **Encoders:** flat little-endian fixed-layout messages; reusable encoder/decoder instances held as fields (zero per-message allocation); zero-copy publication via `tryClaim`/`BufferClaim`.
  - **Fences:** correct release/acquire ordering (Agrona `putLongOrdered`/`putLongVolatile` ↔ C++ `std::atomic` release/acquire); cache-line padding vs false sharing.
  - **Low-level:** no hot-path allocation; pre-allocated scratch; `NanoClock`/`CachedNanoClock` sampled once per cycle; branch-predictable state machines; mechanical sympathy.
  - **NEVER busy-spin to pass time:** a single injected, advanceable monotonic clock; tests ADVANCE the clock to the next deadline and call `doWork` once per expected transition — no wall-time waits, no `while(cond) doWork()`. Every drive loop has a hard iteration cap and FAILS (not hangs) if progress stalls. Runtime idles via `IdleStrategy` when `doWork` returns 0 work.
  - Use Aeron's own ring buffers / `IdleStrategy` (the C++ client ships them) — do NOT pull in the `common` library.
- **All little-endian** (consistent with the codebase-wide decision).
- **Build with `-j2`** (CPU-constrained machine).
- **NEVER run `conan create/export/upload`** — CI owns package publishing (enforced by a hook).
- **Unique temp paths in tests** (`getpid()` + atomic counter) — no cross-process collisions under `-j4`.
- **Reviews = Opus; implementation = Sonnet.** If a Sonnet fix fails, introduces a new issue, or causes a regression that Opus catches on review, **Opus takes over that fix**. Each area follows the full-area **review → fix → full-area re-review** loop until an Opus pass returns CLEAN, plus a per-iteration edge-case-coverage check.

## Ticket Workflow (Planka board)

- A **Planka card is created per ticket** in Backlog before work starts; cards move Backlog → In Progress → In Review → Done.
- **When you START a ticket, break it into sub-tickets** as the work reveals itself: the bite-sized TDD steps (write failing test → run-fail → implement → run-pass → commit), any **bug tickets** discovered, and any review-fix follow-ups. Sub-tickets are children of the ticket.
- Each ticket ends with an **independently testable deliverable** and an Opus review before Done.
- This plan enumerates the **tickets** (the units); the bite-sized steps live in the sub-tickets created at execution time.

---

## File Structure

**`rune-protocol`** (shared codec — sibling-header, no Conan publish here):
- `include/rune/protocol/client/MessageHeader.h` — fixed LE header (encode/decode).
- `include/rune/protocol/client/ControlMessages.h` — Connect/ConnectResponse/Keepalive/Disconnect/Error.
- `include/rune/protocol/client/PingMessages.h` — Ping/Pong.
- `include/rune/protocol/client/Protocol.h` — schema id, template ids, error codes, reserved ranges.
- `tests/protocol/client/*` + `tests/protocol/client/vectors/*` — unit tests + conformance vectors.

**`rune-broker`** (runtime + gateway):
- `src/main.cpp` — replaced: node bootstrap.
- `src/node/NodeConfig.h/.cpp` — env-driven config.
- `src/node/BrokerNode.h/.cpp` — assembles driver + consensus + engine + gateway; duty-cycle.
- `libs/gateway/…` (new lib `rune_gateway`) — `ClientGateway`, `SessionTable`, request dispatch.
- `docker/Dockerfile`, `docker/docker-compose.yml`, `docker/entrypoint.sh`.
- `tests/gateway/*`, `tests/integration/*` (cluster E2E).

**`rune-client-cpp`** (new repo):
- `CMakeLists.txt`, `conanfile.py`, `.woodpecker/*` — mirror `rune-broker`.
- `include/rune/client/Client.h` — public API.
- `src/transport/*` (Aeron duplex + embedded driver), `src/session/*` (state machine), `src/agent/*` (duty-cycle), `src/codec/*` (thin adapters over `rune-protocol`).
- `tests/*` (unit + conformance) `tests/integration/*` (E2E vs the cluster) `bench/*`.

---

## Sub-plan 1 — `rune-protocol`: client-facing protocol codecs (epic **CP**)

*Foundational, pure, no I/O; both other units depend on it. Aeron-bible: flat LE, reusable zero-alloc encoders/decoders, bounds-checked (`_safe`) decoders on the public path.*

### CP-1 — Client `MessageHeader` codec
- **Files:** create `include/rune/protocol/client/MessageHeader.h` + `tests/protocol/client/MessageHeaderTest.cpp`.
- **Layout (LE, 24 bytes):** `blockLength:u16 · templateId:u16 · schemaId:u16 · version:u16 · sessionId:i64 · correlationId:i64`.
- **Produces:** `struct MessageHeader { … };` with `encode(buf,off)` / `decode(buf,off,avail)->optional`, `static constexpr ENCODED_SIZE=24`.
- **Acceptance:** byte-exact LE round-trip; `static_assert` layout; truncated buffer → `nullopt` (no over-read).

### CP-2 — `ConnectRequest` / `ConnectResponse`
- **Files:** `include/rune/protocol/client/ControlMessages.h` (+ test).
- **Fields:** `ConnectRequest{ egressChannel:var-str, egressStreamId:i32, clientVersion:i32 }` (templateId 1); `ConnectResponse{ code:i16, leaderMemberId:i32, assignedSessionId:i64 }` (templateId 2). `sessionId`/`correlationId` ride the header.
- **Acceptance:** byte-exact round-trip incl. empty + long `egressChannel`; bounds-checked var-str decode.

### CP-3 — `Keepalive` / `Disconnect` / `ErrorResponse`
- **Fields:** `Keepalive{}` (templateId 3, header only); `Disconnect{ reason:i16 }` (4); `ErrorResponse{ code:i16, message:var-str }` (5).
- **Error codes enum:** `OK, PROTOCOL_VERSION, NOT_LEADER, REDIRECT, SESSION_EXPIRED, UNKNOWN_SESSION, INTERNAL`.
- **Acceptance:** round-trips; error message empty + long; codes stable.

### CP-4 — `Ping` / `Pong`
- **Fields:** `Ping{ echo:var-bytes }` (templateId 6); `Pong{ echo:var-bytes }` (7) — echoes verbatim (Milestone A round-trip).
- **Acceptance:** round-trip incl. empty + max payload; bounds-checked.

### CP-5 — Conformance vectors + harness
- **Files:** `tests/protocol/client/vectors/*.bin` + `*.expected.json` + a `ConformanceTest.cpp` harness.
- **Content:** encoded frames + expected decodes for every message; a **session-flow script** (Connect → ConnectResponse → Keepalive → Ping → Pong → Disconnect).
- **Acceptance:** the harness validates every vector; this is the shared artifact the gateway (BR) and client (CC) both run.

### CP-6 — `Protocol.h`: schema/versioning + reserved ranges + safe dispatch
- **Content:** `schemaId=120`; template-id **reserved ranges** (control 1–15 · produce/consume 16–31 · groups/txn 32–63 · view 64–95); a `peekTemplateId(buf,len)` + version guard that rejects unknown template/version with a clean `ErrorResponse` (never mis-parse).
- **Acceptance:** unknown template/version → error path; truncated frame → rejected (no OOB).

---

## Sub-plan 2 — `rune-broker`: node runtime + ClientGateway + Docker (epic **BR**)

*Aeron-bible: the gateway is a single-writer agent in the node duty-cycle; ingress→engine handoff via an Agrona ring; egress via `tryClaim`; no busy-spin; injected clock in tests.*

### BR-1 — `NodeConfig` (env-driven)
- **Files:** `src/node/NodeConfig.h/.cpp` (+ test). Parse/validate `RUNE_NODE_ID`, `RUNE_PEERS` (list of `id@host:port`), `RUNE_CLIENT_ENDPOINT` (`aeron:udp?endpoint=…` + streamId), `RUNE_DATA_DIR`, consensus channel config.
- **Acceptance:** valid config parses; malformed peer list / missing required → clear error. Unit-tested, no I/O.

### BR-2 — `BrokerNode` single-node runtime
- **Files:** `src/node/BrokerNode.h/.cpp`, rewrite `src/main.cpp`. Assemble embedded `MediaDriver` + `ConsensusAgent` (existing) + an `AgentRunner` duty-cycle. A 1-node cluster elects itself leader.
- **Consumes:** `NodeConfig` (BR-1), existing `libs/consensus`.
- **Acceptance:** bounded integration test — node boots, becomes leader, `doWork` idles via `IdleStrategy` (no busy-spin); clean shutdown.

### BR-3 — 3-node consensus over `aeron:udp`
- **Files:** `tests/integration/UdpClusterElectionTest.cpp` (CMake-gated like the existing real-Aeron tests). Switch consensus channels from `ipc` to `udp` via config; verify 3 nodes elect a single leader.
- **Acceptance:** deterministic/bounded election over udp; single leader; no split-brain. (Validates the container transport before Docker.)

### BR-4 — `ClientGateway` (new lib `rune_gateway`)
- **Files:** `libs/gateway/include/rune/gateway/{ClientGateway,SessionTable}.h` + `src/*` (+ tests). Aeron ingress `Subscription` + per-session egress `Publication`; `FragmentAssembler`; a `SessionTable` (`sessionId → egress + lastSeenNs`); dispatch `Connect→ConnectResponse`, `Keepalive`, `Disconnect`, `Ping→Pong`. Ingress→node handoff via an Agrona ring; single-writer.
- **Consumes:** CP codecs; `NodeConfig`.
- **Produces:** `ClientGateway::doWork(nowNs)->int`; `onFragment(...)`.
- **Acceptance:** unit tests with a fake transport (bounded, injected clock); IPC-loopback integration: connect + ping → pong; passes the CP-5 conformance session-flow.

### BR-5 — Session lifecycle + keepalive timeout + control REDIRECT
- **Files:** extend gateway (+ tests). Keepalive refresh; idle-session expiry via `doWork(nowNs)` (injected clock, bounded); a non-leader node answers control with `REDIRECT{leaderEndpoint}` where required.
- **Acceptance:** session expires after timeout (just-before alive / just-after expired boundary test); redirect emitted correctly; no busy-spin.

### BR-6 — Dockerfile
- **Files:** `docker/Dockerfile`, `docker/entrypoint.sh`. Build `rune-broker` (+ Aeron), bundle the media driver; entrypoint maps env → `NodeConfig`.
- **Acceptance:** image builds; `docker run` a single node → boots + self-elects; readiness probe passes.

### BR-7 — `docker-compose` 3-node cluster
- **Files:** `docker/docker-compose.yml`. 3 nodes on a network, data volumes, `/dev/shm` sized, MTU set, client UDP ports exposed; `aeron:udp` unicast to peer DNS names; readiness = joined-consensus + knows-leader.
- **Acceptance (E2E):** `compose up` → leader elected → a **raw Aeron ping probe** (a tiny test binary) connects to any node and gets `Pong`. Bounded; leader-kill → re-election.

### BR-8 — Gateway benchmarks + full-area Opus review
- **Files:** `bench/GatewayBench.cpp`. Ping round-trip latency (IPC loopback + intra-cluster). Then an **Opus full-area review** of `rune_gateway` + node runtime; fix loop to CLEAN.
- **Acceptance:** latency captured; Opus review CLEAN (no busy-spin, correct fences/single-writer, bounded sessions).

---

## Sub-plan 3 — `rune-client-cpp` (new repo) (epic **CC**)

*Aeron-bible: client is a single-writer `doWork` agent under `IdleStrategy`; app→client submission via an Agrona ring; zero-alloc handlers; injected clock in tests.*

### CC-1 — Repo scaffolding
- **Action:** `git init rune-client-cpp`; `CMakeLists.txt` + `conanfile.py` (deps: `aeron`, gtest, google-benchmark; `rune-protocol` via sibling include like `rune-broker`); `.woodpecker` CI mirroring siblings; `.gitignore` (PID+counter temp-path convention noted). *(If the user pre-creates the empty repo + the git host remote, skip the init and target it.)*
- **Acceptance:** empty project builds + runs a trivial gtest under `-j2`.

### CC-2 — Codec adapter + conformance
- **Files:** `src/codec/*` thin adapters over `rune-protocol/client`; `tests/ConformanceTest.cpp` runs the **CP-5 vectors**.
- **Acceptance:** all CP-5 conformance vectors pass in the client build.

### CC-3 — Transport layer (Aeron duplex + embedded driver)
- **Files:** `src/transport/*`. One `Publication` (ingress) + one `Subscription` (egress) + `FragmentAssembler`; embedded media driver.
- **Acceptance:** IPC-loopback echo — send a framed message, receive it back; zero-copy handler.

### CC-4 — Session state machine
- **Files:** `src/session/*`. `Disconnected→Connecting→Connected→(Resuming)→Closed`; `sessionId`; keepalive cadence; `REDIRECT`/`NOT_LEADER` handling; per-subscription offset tracking scaffold (for later resume).
- **Acceptance:** unit tests with a fake transport (bounded, injected clock) drive every transition; stale/timeout paths; redirect reconnect.

### CC-5 — Client agent duty-cycle
- **Files:** `src/agent/*`. `doWork(nowNs)` under `IdleStrategy` (poll egress → dispatch → keepalive → time out pending); app→client submission via an Agrona ring; **manual-`poll()` mode**.
- **Acceptance:** `doWork` idle → 0 work (no busy-spin); bounded driver test; thread-safe submission.

### CC-6 — Public API (`connect`, `ping`, lifecycle)
- **Files:** `include/rune/client/Client.h` + impl. `Client::connect(config)->std::future<void>`; `ping(payload)->std::future<Bytes>`; `onConnected/onDisconnected/onError`. Idiomatic modern C++; correlationId-matched.
- **Acceptance:** unit test against a fake gateway — connect resolves, ping future resolves with the echo, error surfaces on the future.

### CC-7 — End-to-end vs the dockerized cluster
- **Files:** `tests/integration/E2EPingTest.cpp`. Client connects to the BR-7 compose cluster; connect → keepalive → ping→pong; leader-kill → REDIRECT/reconnect → ping still succeeds (resume scaffold). Bounded.
- **Acceptance:** green against `compose up`; no committed-state involved yet (ping only), but reconnect path exercised.

### CC-8 — Client benchmarks + full-area Opus review
- **Files:** `bench/ClientBench.cpp` (ping p50/p99). Then an **Opus full-area review** of `rune-client-cpp`; fix loop to CLEAN; confirm conformance + no busy-spin + zero-alloc hot path.
- **Acceptance:** latency captured; Opus review CLEAN.

---

## Cross-cutting / integration ticket

### MA-DONE — Milestone A acceptance
- **Deliverable:** `docker compose up` (3 nodes) + `rune-client-cpp` example → connect + ping round-trip across the cluster, leader-failover-transparent. Full suites green in all three repos (seq + `-j4`); all epics' Opus reviews CLEAN. Update `rune-build-status` memory + the board.

---

## Self-Review (author checklist — completed)

- **Spec coverage:** §2 architecture → CP/BR/CC epics; §3 broker runtime → BR-1..7; §4 protocol → CP-1..6 + BR-4/5; §5 addressing → (produce/subscribe deferred to Milestone B; ping path only here — noted); §6 client → CC-3..6; §8 testing → conformance (CP-5), unit/integration across all, E2E (BR-7/CC-7), benches (BR-8/CC-8); §9 future → out of scope (Milestones B/C + view layer). Addressing/produce/subscribe intentionally deferred to the Milestone B plan — flagged, not dropped.
- **Placeholder scan:** none — every ticket has concrete files, field layouts, and acceptance criteria; bite-sized TDD steps are created as sub-tickets on start per the ticket workflow.
- **Type consistency:** `MessageHeader.ENCODED_SIZE=24`, `schemaId=120`, template ids 1–7, error-code enum, and `doWork(nowNs)->int` used consistently across CP/BR/CC.

## Follow-on plans (not written yet — after Milestone A is proven)
- **Milestone B plan:** produce (3-tier: auto-batch → explicit batch → RAII transaction) + push-subscribe + addressing (topic handles, binary keys, subjects-as-sugar) through the gateway into the engine.
- **Milestone C plan:** consumer-group consume + offset commit (reuse Phase 4).
- **`rune-client-nodejs` plan:** re-implement the Aeron client; mirror via the CP-5 conformance suite.
