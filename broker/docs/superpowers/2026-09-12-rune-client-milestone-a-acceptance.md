# Rune Client — Milestone A Acceptance

**Date:** 2026-09-12
**Plan:** `docs/superpowers/plans/2026-09-11-rune-client-milestone-a.md`
**Spec:** `docs/superpowers/specs/2026-09-11-rune-client-and-broker-runtime-design.md`

Milestone A delivers a **runnable, clustered Rune broker with a client-facing session gateway**, and a **full C++ client** (`rune-client-cpp`) that connects to it and does request/response (ping→pong) over Aeron `aeron:udp`, surviving cluster leader-failover. Every ticket was implemented by a fresh subagent and gated by an Opus task review (spec + quality + coverage), with a full-area Opus review at the end of each epic driven to CLEAN.

## What shipped

### CP epic — client protocol codecs (`rune-protocol`)
Flat little-endian client wire protocol (schema 120): 24-byte MessageHeader; Connect/ConnectResponse, Keepalive, Disconnect, ErrorResponse, Ping/Pong; bounds-checked var-field codecs; `checkFrame`/`peekTemplateId` safe dispatch (version/template guards). **10 golden conformance vectors** + a language-agnostic harness (both-direction, byte-exact). 175 tests.

### BR epic — broker node runtime + gateway + docker (`rune-broker`)
- **NodeConfig** (env-driven), **BrokerNode** runtime (embedded Aeron media driver + consensus `AgentRunner`); single-node self-elects; **3-node consensus over `aeron:udp`** (per-peer publications + ingress poll); election tuned from ~632ms → ~120-180ms warm.
- **ClientGateway** — the client-facing session gateway: Connect→ConnectResponse, Keepalive, Ping→Pong, Disconnect over Aeron; session table with keepalive-timeout reaping (injected clock); non-leader Connect REDIRECT / degrade-to-accept; **async non-blocking Connect egress-resolve** (a slow/bad client egress can't stall the single-writer consensus thread) with a leak-free drain-release path.
- Gateway runs inside BrokerNode's single-writer duty-cycle (`NodeRuntimeAgent`).
- **Docker:** a slim single-node image (`gcc:13` builder → `debian:bookworm-slim` runtime, ~30MB, self-elects) and a **3-node `docker-compose` cluster over `aeron:udp`** with a raw-Aeron ping probe + a **leader-kill E2E** (kills the real leader, cluster re-elects, probe still gets Pong).
- Gateway ping round-trip benchmark: **p50 1.0µs / p99 1.9µs / p99.9 3.7µs** (aeron:ipc, debug).
- Full-area Opus review of the whole gateway + node runtime: **CLEAN**. Suite **1682/1682**.

### CC epic — the C++ client (`rune-client-cpp`, new repo)
- Conan2/CMake C++23; public ConanCenter deps only; rune-protocol via sibling include.
- **Codec adapters** (thin, byte-faithful) + the **CP-5 conformance vectors run green in the client build** (10/10 both directions).
- **AeronTransport** — embedded driver + ExclusivePublication ingress + Subscription/FragmentAssembler egress; zero-copy poll; tryClaim/offer; teardown-order-safe; `retarget`.
- **SessionMachine** — pure, clock-injected `Disconnected→Connecting→Connected→Resuming→Closed` with connect/redirect(cap)/keepalive/resume/close.
- **ClientAgent** — single-writer duty cycle; lock-free `ManyToOneRingBuffer` app→agent submission; atomic (state,sessionId) snapshot; idle→0 (no busy-spin).
- **Client** — idiomatic future-based public API: `connect()->future<void>`, `ping(Bytes)->future<Bytes>` (correlationId-matched), `onConnected/onDisconnected/onError`; background runner; futures never hang (timeout sweep); double-resolve structurally impossible (extract-under-lock).
- **E2E vs the live 3-node cluster**: the real `Client` connects + ping→pong and **survives a leader-kill + re-election**.
- Client ping RTT benchmark: p50 246µs / p99 466µs (Release — full client path incl. runner + test-gateway-pump thread hops; the raw broker gateway path is ~1-2µs).
- Full-area Opus review of the whole client: **CLEAN** (fixed a snapshot-publish race, a noexcept-retarget terminate hazard, error-path ping handling, a torn (state,sessionId) read). Suite **106/106**.

## Acceptance criteria — met
- [x] 3-node `aeron:udp` broker cluster elects a single leader (docker-compose). *(BR-7b)*
- [x] A raw-Aeron probe connects to a node and gets Pong; leader-kill → re-election → still Pong. *(BR-7b `docker/e2e.sh`)*
- [x] The real `rune-client-cpp` Client connects to the cluster, pings, and gets pongs. *(CC-7 `docker/e2e-client.sh`)*
- [x] The Client survives a node-kill + cluster re-election (pings continue). *(CC-7 / MA-DONE)*
- [x] Wire compatibility proven by shared golden conformance vectors (broker + client). *(CP-5 / CC-2)*
- [x] Every epic full-area Opus-reviewed to CLEAN; broker 1682/1682, client 106/106.

## Deferred to Milestone B (tracked, by design)
- **Produce / subscribe / addressing** (topics + binary keys + partitions; SBE-encoded event values) — the M-A wire path is ping/session only.
- **Real per-peer REDIRECT follow** — the client following a redirect to a *new* leader endpoint needs per-peer client endpoints in `RUNE_PEERS` + a member-id→endpoint map; M-A followers degrade-to-accept (ping needs no leadership). Target-node-death → retarget is therefore M-B.
- **Materialized views / transactional distributed DB** — captured in the spec's Future Directions.
- Minor tracked items: gtest/benchmark as test-only conan deps; resume-backoff dwell; fake-Aeron harness for the gateway/agent ERRORED-throw branch tests; `rune-client-nodejs`.

## How to run the acceptance
```sh
# Broker cluster + raw-probe leader-kill E2E:
cd rune-broker && bash docker/e2e.sh
# Full client vs cluster (connect + ping + survive node-kill):
cd rune-client-cpp && bash docker/e2e-client.sh
```
