# broker — area notes for AI sessions

The repo-wide rules (16-point philosophy, Aeron reference, testing, issues, git) are in the root
[CLAUDE.md](../CLAUDE.md). This file holds broker-specific detail only.

Aeron Cluster blueprints: `docs/specs/2026-09-10-rune-aeron-cluster-port-reference.md`,
`docs/specs/2026-09-10-rune-phase2-aeron-raft-notes.md`. Use Aeron's own ring buffers / `IdleStrategy` (the C++
client ships them); do NOT pull in the `common` library for these.

Benchmark procedure, clean-box steps and known baselines: [bench/BENCHMARKING.md](bench/BENCHMARKING.md).

## Benchmark: optimal config for the 4-core box (3-node acks=all)

Custom (non-default) knobs that took shared+backoff 6k → 92k @256B (win2000) / 106k @256B
(win8000+2MB batch). NB the bench was **client-bound** (producer ~2 cores CPU, broker idle) —
these are the right knobs, not the true broker ceiling; the real levers are the architectural
fixes (per-peer batched replication, zero-copy, per-worker repl, non-blocking append, client
CPU) — see the recipe.

### OS sysctl (UDP is subject to these — not tcp-only)
```
net.core.rmem_max=16777216      net.core.wmem_max=16777216
net.core.rmem_default=16777216  net.core.wmem_default=16777216
net.core.netdev_max_backlog=100000
```

### Broker env (value | default)
- `RUNE_AERON_THREADING_MODE=dedicated` | shared  (shared = 1 thread for ALL UDP; dedicated alone 6k→47k)
- `RUNE_AERON_RCV_WINDOW=16777216` | **131072 (128K!)**  — Aeron flow-control window (the "small window")
- `RUNE_AERON_MTU=8192` | 1408
- `RUNE_IDLE_MAX_PARK_US=10` | 200  — control/worker loop park cap (was the ~230µs loop period)
- `RUNE_AERON_SOCKET_BUFFER_BYTES=16777216` | 2M  — SO_SND/RCVBUF (clamped by sysctl above)
- `RUNE_AERON_TERM_BUFFER_BYTES=67108864` | 4M
- `RUNE_REPL_OUTBOUND_RING_BYTES=33554432` | 4M
- `RUNE_WORKER_INGRESS_RING_BYTES=33554432` `RUNE_WORKER_RETURN_RING_BYTES=33554432` | 32M
- `RUNE_WORKERS=2`, `RUNE_PER_WORKER_INGRESS=true`, `RUNE_PER_WORKER_EGRESS=true`
- `RUNE_WORKER_IDLE=backoff` `RUNE_RUNTIME_IDLE=backoff` (busyspin pegs cores on a shared box)
- `RUNE_IO_BACKEND=uring` (Linux; `portable` on macOS/Rosetta), `RUNE_MIN_ISR=2` (3-node)/`1` (single)

### Client (rune_produce_throughput) env
- `RUNE_BENCH_WINDOW=RUNE_BENCH_MAX_IN_FLIGHT=` 2000 (low-latency) … 8000 (throughput); >8000 = diminishing + huge latency (Little's law)
- `RUNE_LINGER_US=500..1000` (0 defeats batching); `RUNE_BATCH_BYTES=1048576..2097152`
- `RUNE_BENCH_VALUE_BYTES=1024` to match Kafka (its 156k was @1KB — always compare same size)

### HARD CONSTRAINTS (violation → broker abort / NO-LEADER)
1. `RUNE_AERON_RCV_WINDOW ≤ RUNE_AERON_SOCKET_BUFFER_BYTES` ("Initial window greater than SO_SNDBUF")
2. `RUNE_AERON_SOCKET_BUFFER_BYTES ≤ net.core.rmem_max/wmem_max` (else silently clamped)
3. `RUNE_AERON_RCV_WINDOW ≤ term_buffer/2`  and must be a power of two
4. Produce intake limit (F18) = min(8M, every hop's one-frame capacity): `RUNE_WORKER_INGRESS_RING_BYTES/8`
   and, multi-node, `RUNE_REPL_OUTBOUND_RING_BYTES/8` and `min(RUNE_AERON_TERM_BUFFER_BYTES/8, 16M)` (each minus
   a few hundred bytes of framing).  Larger batches → MessageTooLarge (never a startup refusal).  Defaults
   give ~512 KiB; the profile above gives ~4 MiB — keep the client's `min(RUNE_BATCH_BYTES, RUNE_MAX_FRAME_BYTES)`
   below it.  A batch STORED above the current frame (e.g. after shrinking these sizes) is unreplicable: the
   leader answers kErrRecordTooLarge and followers wedge at that offset and leave the ISR (logged + counted).
5. Frame uniformity (F18): keep every node's follower apply frame (`RUNE_WORKER_INGRESS_RING_BYTES/8`) ≥
   `min(maxFetchBytes = 1 MiB, the leader's push frame = min(RUNE_REPL_OUTBOUND_RING_BYTES/8, RUNE_AERON_TERM_BUFFER_BYTES/8))`.
   Otherwise a coalesced multi-batch push above the follower's frame is dropped (never wedged) and the follower
   catches up via its capped fetch — ~20 ms per dropped push.  Equal sizes are NOT enough (a uniform 4 MiB worker
   ring gives a 524,037 B apply frame < the default 524,226 B push frame); the default 32 MiB worker ring (~4 MiB
   frame ≥ 1 MiB) always satisfies it.

### Runtime note (Ubuntu 22.04 hosts)
gcc-13 binaries need a newer libstdc++ than stock 22.04 ships (GLIBCXX_3.4.31/32): put the build host's
`libstdc++.so.6` in a private lib dir and point `LD_LIBRARY_PATH` at it.
