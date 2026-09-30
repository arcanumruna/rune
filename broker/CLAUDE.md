# Rune — working agreement for AI sessions

Rune streams its development **live** to a public dashboard —
https://rune.regressorspath.com (fed by the `rune-live` server at
https://live.regressorspath.com). **Every session working on Rune must keep it
updated** as it works. Three habits, always:

## 1. Tickets — Planka board (via the `planka` MCP)
- The board is **"Rune / Roadmap"** (board id `1861164569347490826`), lists:
  **Backlog · In Progress · In Review · Done**.
- **Create a card** in Backlog for each unit of work you intend to do *before* you start it.
- **Move the card** as you progress: Backlog → In Progress (on start) → In Review
  (verifying/PR) → Done (finished). Don't leave work untracked.

## 2. Chat — say what you're about to do
- When you start a task or change direction, post a short note:
  ```
  scripts/rune-live.sh chat plan "Scaffolding the Raft metadata log next"
  ```
  kinds: `plan` (intent) · `thought` (reasoning) · `status` (progress).

## 3. Tests & benchmarks — push results as they run
- After running tests/benchmarks, push the result. The **first argument is the stable
  test name (the suite)** — the dashboard conflates by it, so re-runs fold into one row
  with a run count. Use a stable name (e.g. `rune-broker/storage`, `dr-e2e`); do NOT pass
  a per-run or ticket id there:
  ```
  scripts/rune-live.sh test "rune-broker/storage" passed \
    passed=65 failed=0 total=65 commit=$(git rev-parse --short HEAD)
  ```
- Benchmarks: add `kind=benchmark p99Us=87`. Long runs: push `running` first, then the final result.

Push token/URL default to dev values in the helper; override with `RUNE_PUSH_TOKEN` /
`RUNE_LIVE_URL`. API + payload formats: `rune-live/README.md`.

## 4. Engineering — Aeron/Agrona is the bible (ALL phases, non-negotiable)

Rune targets Aeron-class latency. **Every phase treats Aeron Cluster's Java implementation as the reference to follow EXACTLY** — threading model, encoders, fences, and low-level optimizations. When in doubt, read the Aeron Java source and mirror it.

- **Threading model:** single-writer agents driven by a `doWork(nowNs)` loop under an `IdleStrategy` (busy-spin/backoff); no locks on the hot path; each piece of state owned by exactly one thread; inter-thread handoff ONLY via Aeron/Agrona lock-free ring buffers (`ManyToOne`/`OneToOne`/`Broadcast`) — never shared mutexes/condvars.
- **Encoders:** flat SBE-style fixed-layout messages; reusable encoder/decoder instances held as fields (zero per-message allocation); zero-copy publication via `tryClaim`/`BufferClaim` straight into the term buffer.
- **Fences / memory ordering:** correct release/acquire semantics — Agrona `putLongOrdered`/`putLongVolatile` ↔ C++ `std::atomic` release store / acquire load; single-writer publishes with a release store, consumer reads with an acquire load; no data races; cache-line padding to avoid false sharing.
- **Low-level optimizations:** no hot-path allocation; pre-allocated scratch buffers/arrays (e.g. ranked-positions for quorum); `NanoClock` sampled once per cycle; branch-predictable state machines; mechanical sympathy throughout.
- **Time & idling (NEVER busy-spin to pass time):** a single INJECTED, advanceable monotonic clock (Aeron's `CachedNanoClock`). **Tests advance the clock** to the next deadline and call `doWork` deterministically — one call per expected transition — and assert the result; they do NOT wait on wall time and do NOT loop `while(cond) doWork()`. Any test drive loop MUST have a hard iteration cap and fail (not hang) if the state machine stalls — e.g. `for (int i=0; i<CAP && !done; ++i){ clock.advance(toNextDeadline); node.doWork(clock.now()); }` then `ASSERT(done)`. In the RUNTIME, when `doWork` returns 0 work the agent BACKS OFF via `IdleStrategy` (yield/park) — never a bare spin burning a core for no work.

Blueprints: `docs/specs/2026-09-10-rune-aeron-cluster-port-reference.md`, `docs/specs/2026-09-10-rune-phase2-aeron-raft-notes.md`. Use Aeron's own ring buffers / `IdleStrategy` (the C++ client ships them) — do NOT pull in the `common` library for these.

---

## Engineering Philosophy (HARD RULES — apply everywhere, always)

These are non-negotiable across every Rune repo. Check every design and every fix against all eight:

1. **Shared-nothing.** No shared mutable state, no locks, no atomics-as-coordination, no `thread_local`. Each thread/component owns its data outright.
2. **Ring buffers are the ONLY cross-thread event passing.** All inter-thread / inter-component communication goes through ring buffers (Aeron-style) — never shared memory or cross-thread callbacks.
3. **State machines where each STATE holds exactly the variables that state needs** — not scattered booleans. e.g. `PUBLISHED(buffer, destination, publisher, counter)`, `BEHIND(...)`, `INSYNC(...)`. Every edge case = one explicit state with one code path.
4. **Zero allocations.** No heap allocation on hot paths or in steady state — preallocate, reuse buffers (capacity persists), fixed stack buffers; no per-cycle `std::vector` returns / `std::function` / dynamic containers on the fast path.
5. **Test everything, benchmark everything.** Prove features via real e2e/integration + benchmarks. No hand-waving; no "done" without the real path green.
6. **Regression test on EVERY fix**, marked as a regression suite (e.g. `*Regression*`) so they run together.
7. **On every fix/implementation, run the FULL regression suite FIRST (priority)** — then all other tests. The regression suite is the gate before anything else.
8. **Fix-on-sight.** If while reading any file you find code that violates this philosophy (shared state, cross-thread callback, scattered flags, hot-path allocation, untested/un-regressed fix), you FIX it — don't just note it.

## Benchmark: optimal config for the 4-core box (ccx23 3-node acks=all)

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
- `RUNE_REPL_OUTBOUND_RING_BYTES=16777216` | 4M
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

### ccx23 runtime note
gcc-13 binaries need newer libstdc++ → copy build-agent's `libstdc++.so.6` to `/root/rune-libs` +
`LD_LIBRARY_PATH=/root/rune-libs` (stock Ubuntu 22.04 lacks GLIBCXX_3.4.31/32).
