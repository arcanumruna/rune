# Rune Client — Milestone B Acceptance

**Date:** 2026-09-14
**Plan:** `docs/superpowers/plans/2026-09-12-rune-client-milestone-b.md`
**Spec:** `docs/superpowers/specs/2026-09-11-rune-client-and-broker-runtime-design.md`

Milestone B delivers **produce → push-subscribe** end-to-end: the client produces records (auto-batched or explicit atomic batches, idempotent) and subscribes to a broker-paced push stream, through the ClientGateway into the real storage engine, across a 3-node `aeron:udp` cluster, surviving a leader kill. Every ticket was implemented by a fresh subagent and gated by an Opus review (spec + quality + coverage + safety), with a full-area Opus review closing each epic.

## Scope (user-confirmed)
- **In:** Tier-1 auto-batch + Tier-2 atomic-batch producer (idempotent on), push Subscribe with offset tracking + back-pressure, client↔engine wiring, data-op REDIRECT to the leader + client redirect-follow, group-commit durability, E2E + benchmarks.
- **Deferred to Milestone C:** Tier-3 transactions (EOS), consumer groups, `acks=All` true ISR-wait (currently `acks=Leader`/page-cache).

## What shipped

### PB epic — produce/subscribe protocol codecs (`rune-protocol`)
Produce/ProduceResponse, Subscribe/SubscribeResponse, Records (push), Unsubscribe (templateIds 16–21); 7 golden conformance vectors both-direction; safe dispatch (1–21). **249/249.**

### GB epic — broker gateway ↔ engine (`rune-broker`)
- **TopicRegistry** over per-partition `Log`s from `RUNE_TOPICS`; the canonical key→partition hash (FNV-1a) is **shared with the client** (rune-protocol) so client-side partitioning == broker resolution.
- **Produce** → leader-local append with broker-assigned offsets (CRC-safe baseOffset restamp; LEO derived from the CRC-covered `lastOffsetDelta`).
- **Subscribe** → egress-paced push with **exactly-once delivery under back-pressure** (offset advances only on `tryClaim` success — pause+resume from the exact offset, no loss/dup).
- **Data-op REDIRECT** — a follower redirects Produce/Subscribe to the leader's client endpoint (per-peer client endpoints in `RUNE_PEERS`); a follower never appends.
- **Group-commit durability** — produce appends to page cache (no per-append fsync) + one fsync/partition/cycle; `acks=Leader` = page-cache-durable (Kafka acks=1), crash-durability via replication (`acks=All`, Milestone C). Fixed a durability Blocker (fsync across segment rolls) in the full-area review.
- Gateway data benchmark: **produce p99 4.8µs, subscribe-delivery p99 7.3µs** (group-commit). Full-area review CLEAN. **1756/1756.**

### CB epic — client producer + subscribe (`rune-client-cpp`)
- Codec adapters + client conformance (7/7 vectors).
- **Tier-1 producer** — `send(topic, Key, value)` **provably zero-alloc + lock-free** (counting-allocator test: 0 heap allocs / 5000 sends), auto-batch (linger/batchBytes), idempotent producerId+epoch+sequence, `acks` None/Leader/All, `maxInFlight`, per-send void/callback/future.
- **Tier-2 producer** — explicit atomic batch (one RecordBatch → one partition → one append).
- **Subscribe** — push RecordHandler (per-record zero-copy on the agent thread), offset tracking, unsubscribe, From Earliest/Latest/Offset.
- **Client redirect-follow** — on a data-op REDIRECT, retarget + reconnect + resend with the new sessionId; failure surface hardened (no hangs; sends gated on Connected; subscribe timeout).
- Client bench: **produce RTT p99 ~35µs, subscribe-delivery p99 ~24µs** (LowLatency, <100µs gate). Full-area review CLEAN. **160/160.**

## Acceptance criteria — met (`rune-client-cpp/docker/e2e-data.sh`, run live)
- [x] 3-node cluster provisions topics (`RUNE_TOPICS`) + per-peer client endpoints.
- [x] The real `Client` connects (to a follower), produces N — records land with **incrementing offsets**, ordered per key (redirect-follow to the leader works).
- [x] Subscribe from Earliest delivers **all N in offset order**.
- [x] **Leader-kill recovery:** killing the leader → the cluster **genuinely re-elects** a new leader → the client recovers and produces/subscribes M more with **no committed-record loss**. `CB-5 C E2E PASSED`, exit 0.
- [x] Every epic full-area Opus-reviewed; the consensus leader-liveness change reviewed for safety (no split-brain / minority-leader / term-regression).

## Real bugs the E2E surfaced + fixed (its whole purpose)
1. **Broker had no follower leader-liveness detection** — Aeron's `ConsensusModuleAgent` heartbeat-timeout → re-election was never ported, so leader-kill **never re-elected**. Milestone A masked it (best-effort assert + ping degrades-to-accept). Ported it faithfully into `Election` (re-canvass on leader silence, strict majority, higher term). **Correction to the Milestone-A claim of "genuine re-election" — it did not actually re-elect before.**
2. **Gateway Earliest-sentinel bug** — all negative `fromOffset` collapsed to LEO, silently dropping Earliest subscriptions.
3. **Client redirect-follow** — stale sessionId on resend, missing subscribe-redirect, resend-before-reconnect, sessionId=0-during-window, hangable subscribe on dead endpoint.

## Deferred to Milestone C / DR epic
- Tier-3 transactions (EOS), consumer groups, `acks=All` ISR-wait.
- Symmetry-gate `registerSubscribe` on Connected (harmless nit).
- **Exhaustive dockerized disaster-recovery E2E suite** (multi-client, netem packet loss/partition, crash combinations) — the next epic, before Milestone C.

## How to run the acceptance
```sh
cd rune-client-cpp && bash docker/e2e-data.sh   # 3-node cluster + client: produce→subscribe + leader-kill recovery
```
