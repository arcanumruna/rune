# Zero-Copy Per-Worker Read Path — Implementation Plan

**Goal:** Make the broker read/consume path match the write path's engineering
standard — per-worker parallel, zero-copy, zero-allocation, no locks, no
thread-locals — and support many consumers (partitioned groups + fan-out via
dynamic MDC). Reads should exceed write throughput (no fsync/replication on the
read path).

**Baseline (measured, macOS loopback, 2 workers, portable fsync):**
- Write (corrected, all-persisted): ~1.5M rec/s.
- Read (current): ~220–330k rec/s, broker-CPU-bound at ~269% (single gateway
  thread funnel + per-read heap alloc + ~4 copies + single egress publication).

## Root causes of low read throughput (from ClientGateway.cpp)
1. `Log::read` returns `std::vector<uint8_t>` → heap alloc + copy up to 512KB per
   read cycle per subscription.
2. With workers, the chunk round-trips through the PushRead return ring (copy #2).
3. The **single gateway thread** encodes each `Records` frame into `scratch_`
   (copy #3), then offer()/fragments into one egress publication (copy #4).
4. All partitions funnel through one gateway thread + one egress → no parallelism.

## Design (mirror the write path)

### D1 — Zero-copy log read
- Add `Log::readView(startOffset, maxBytes) -> {const uint8_t* data, size_t len, int64_t baseOffset}`
  that returns a span into a memory-mapped segment (mmap the `.log`; the offset
  index is already mmap'd). No per-read allocation, no copy.
- Keep the existing `read()` (vector) for callers that need ownership, or migrate
  them. Single-writer append + mmap read is safe (reader sees committed bytes ≤ HW).

### D2 — Per-worker read + encode + push (concurrency model)
- The owning worker reads its partitions and pushes their `Records` frames
  directly — no gateway funnel. Gateway keeps only subscription setup/teardown.
- Per-worker preallocated encode/gather state (struct members, NOT thread_local).
- Egress publish via `tryClaim` + gather the batch bytes from the log view into
  the claim (one copy into the term buffer), or `offer()` for >MTU (fragmented).

### D3 — Multi-consumer
- **Partitioned (consumer group):** each partition → its one owning consumer, via
  that worker's egress to the consumer (unicast).
- **Fan-out (N subscribers, same partition):** **dynamic MDC** — one MDC
  publication per (worker, egress stream); add each consumer's unicast UDP
  destination on subscribe, remove on unsubscribe. Works on unicast infra (no IP
  multicast needed).

### D4 — Constraints (enforced throughout)
- No heap allocation on the read hot path (preallocated buffers/rings).
- No locks; no thread-locals; shared-nothing per worker (each touches only its
  owned partitions' state).
- Internal MPSC ring (rune::client::detail::M2ORingBuffer analogue already used
  client-side) / existing worker rings for any cross-thread handoff.

## Task breakdown (each independently testable)
- **T1** Zero-copy `Log::readView` (mmap segment) + storage test (offset→span,
  multi-segment, boundary). No behavior change to callers yet.
- **T2** Switch the gateway inline push to `readView` (kill the vector alloc);
  re-measure single-thread read throughput (isolate the alloc/copy win).
- **T3** Per-worker read+encode+push: move the push loop into the worker duty
  cycle for owned partitions; gateway stops funneling. Parallel across workers.
- **T4** Dynamic MDC egress: per-worker MDC publication; subscribe adds a
  destination, unsubscribe removes it. Fan-out delivers once per record.
- **T5** Multi-consumer e2e + bench: (a) group of 2–4 over partitioned topic —
  exact coverage, no dup/loss; (b) N subscribers same partitions via MDC — each
  gets full stream; aggregate read-throughput. Over real UDP (not IPC).

## Exit criteria
- Read throughput > write throughput on the same box.
- 0 alloc / 0 lock / 0 thread-local on the read hot path (verified by inspection +
  a no-malloc assertion test where feasible).
- Multi-consumer correctness: exact-once per group; full stream per fan-out sub;
  no loss/dup under produce-while-consume.
