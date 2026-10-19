# Replication Throughput Optimization — Design / Spec

**Date:** 2026-09-30
**Goal:** Bring Rune's 3-node acks=all replication throughput to Kafka parity and beyond — target **200k rec/s / 200 MB/s @ 1 KB** on a 4-core box (Kafka does 156k/152 MB/s there; Rune currently collapses to ~25 MB/s on 3-node).

## Problem statement (measured on ccx23, 4-core/16 GB, 2026-09-29/30)

| Setup (1 KB, acks=all unless noted) | rps | MB/s |
|---|---|---|
| Rune single-node (no replication) | 120k | 117 |
| Rune 3-node acks=leader (no replication) | 22k | 22 |
| Rune 3-node acks=all | 26k | 25 |
| Kafka 3-node acks=all RF=3 | 156k | 152 |

**Root findings (instrumentation-driven, rune-fr flight recorder):**
1. **The benchmark is CLIENT-BOUND.** `rune_produce_throughput` (rune-client-cpp) burns ~196% CPU (~2 cores, ~15µs/record) while the broker sits idle. Single-node's 117 MB/s is the *client's* CPU ceiling, not the broker. On 3-node the client competes with 3 brokers for 4 cores → starved. So all prior "Rune throughput" numbers measured the client, not the broker.
2. **Config was untuned** — recovered 6k→92k @256B via: dedicated Aeron driver, 16M rcv-window (default 128K!), 8K MTU, 10µs idle park, 16M socket buffer. But at 1 KB the payload-invariant ceiling is ~25 MB/s on 3-node — config alone can't beat the per-byte/CPU wall. (See CLAUDE.md "Benchmark: optimal config for the 4-core box".)
3. **Replication data path is CPU/byte-inefficient** vs Kafka: per-partition (not per-peer), 3-4 copies/record, single control thread, push+separate-fast-ack. Storage append is synchronous `pwrite` that stalls (p99 502µs) with no fallocate + a coalesced fsync competing on the append thread.

## The 7 levers (spec)

1. **Multiplex replication per-peer (not per-partition).** Today the transport keys by (topicId,partitionId); FetchRequest/FetchResponse/sendFetchResponse are per-partition → N× messages/SBE/round-trips at N partitions. Target: one inter-broker fetch per (follower,leader) covering ALL partitions (Kafka model). **Wire-protocol change (rune-protocol).** Biggest structural win.
2. **Zero-copy replication.** Eliminate the 3-4 copies/record (leader makeBatch_ → FetchResponseMsg::encode scratch → putPayload into ring → Aeron; follower records.assign out of term buffer). Encode directly into the Aeron publication claim; avoid recv-side copy. **No wire change** (same bytes, fewer copies). Broker-only.
3. **Piggyback acks.** Replace push + separate fast-ack (2 msgs/follower/commit) with a combined pull where the follower's fetch carries its ack position AND requests next data, across all partitions. **Wire-protocol change**; composes with #1.
4. **Per-worker replication transport.** Move replication poll/drainOutbound off the single NodeRuntimeAgent control thread onto the per-partition workers (thread-per-core), like the existing per-worker client ingress. **Threading change.**
5. **Non-blocking append (backend-split, FINAL DECISION).** Writes stay synchronous `pwrite` on BOTH backends (async/io_uring WRITES are UNSAFE — ack/HW-advance before the write CQE lands loses acked records + fd ordering). Fix the stall via: PORTABLE = fallocate segments + fsync off the append thread; LINUX = same + io_uring async fsync (retain UringFlushBackend; SAFE because it's completion-gated `submit→token→reap→deferred ack` and the sync pwrite already put bytes in page cache before the fsync).
6. **Bigger end-to-end batches.** Bigger producer batch/linger to amortize per-op cost (observed ~39-42 rec/batch). Mostly config + verify.
7. **Client optimization (latency + throughput).** Cut the ~15µs/record CPU in rune-client-cpp (fewer copies/encodes/allocs on the send+ack hot path), deeper batching, parallelize the client agent so one client can saturate the broker without pegging 2 cores. rune-client-cpp repo.

## Global Constraints
- **Correctness first:** the full dr-e2e suite + all C++ regression suites MUST stay green after every lever (we just merged 72e87ab to main). Any bug fix ships a `*Regression*` test.
- **Benchmark-validate every lever** on the 3-node VM (interleaved A/B, cite the code path; never blame noise) — re-provision a ccx23 with the CLAUDE.md optimal 4-core config + sysctl. Run the CLIENT off-box (separate machine/cores) so measurements aren't client-bound.
- **8-point engineering philosophy:** shared-nothing, ring buffers only for cross-thread, rich-state state machines, zero hot-path allocation, no virtual calls on hot paths, test+benchmark+regression everything, fix-on-sight.
- **acks=all-in-sync durability = ISR replication** (page-cache copies on ≥minIsr nodes); never ack/advance HW past bytes not yet in page cache.
- Aeron config constraints: `rcv-window ≤ SO_SNDBUF ≤ sysctl rmem/wmem_max`, `rcv-window ≤ term/2`, rcv-window power-of-two.

## References
- Recipe + measurements: memory `rune-replication-benchmark-kafka.md`, `rune-4core-optimal-bench-config.md`.
- Optimal 4-core config: `CLAUDE.md` (Benchmark section).
- Instrumentation: rune-fr sites in Sites.h; broker FR wired in main.cpp; parsers `bench/kafka-compare/{frwaterfall.py, period.py→/tmp, handoff.py→/tmp, clientpipe.py→/tmp}`.
