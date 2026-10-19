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

---

## T3+T4 MERGED — concrete shared-nothing design (updated 2026-09-25)

**Status:** T1 done (`d5cc0d6`), T2 done (`f03a464`), MDC mechanic spike done
(`57e79b1`). T3 and T4 are ONE coupled change (proven below) and are merged here.

### Why T3 and T4 are inseparable
A consumer session can read partitions owned by DIFFERENT workers.
`aeron::ExclusivePublication` is strictly single-writer, so no worker may write a
shared session egress. Therefore each worker must own its OWN egress publication —
which is exactly T4. You cannot move push into the worker (T3) without giving the
worker its own publication (T4).

### Egress transport decision: DYNAMIC MDC (not manual)
Spike (`libs/gateway/tests/integration/MdcEgressSpikeTest.cpp`) proved against the
linked Aeron version:
- `control-mode=dynamic|control=<host>:<port>|fc=min` fans out to N subscribers
  that SELF-REGISTER by connecting to the control address. The driver auto-adds on
  connect and auto-removes on liveness timeout. The publisher never manages the
  destination list. Survivor unaffected when one subscriber leaves.
- Manual MDC `removeDestination(channel)` is BROKEN here (driver confirms the
  remove yet keeps delivering) → do NOT use manual.

Consequence (satisfies user's shared-nothing + ring-only constraints): the control
thread NEVER touches a worker's publication — not even for destination management.
The worker owns ONLY its pub and `offer()`s. Subscribe/Unsubscribe reach the worker
via its ingress RING as new WorkerOps.

### Ownership (each datum lives on exactly one thread; cross-thread only via rings)
- **Control thread (gateway):** Connect handshake; parse Subscribe/Unsubscribe
  frames; route them to the owning worker via that worker's ingress ring (new
  `WorkerOp::Type::Subscribe` / `Unsubscribe`). Tells the client, in the
  Subscribe/Metadata response, the control endpoint + stream for each partition's
  owning worker (`egressControl = <host>:<CTRL_BASE+W>`, `egressStream =
  EGRESS_BASE+W`). Owns NO push cursor and NO egress.
- **Worker thread W:** owns (a) its dynamic-MDC egress `ExclusivePublication`
  (created at worker start via `aeron_->addExclusivePublication`, sole writer);
  (b) its owned-subscription table + per-(sub,partition) `nextOffset` cursors
  (worker-local, preallocated, no heap on hot path); (c) the push loop in doWork:
  for each active owned sub, `log->readView(cursor, chunk)` → encode Records into a
  per-worker scratch → `pub.tryClaim`/`offer` → advance cursor on success, bound by
  HW/LSO. Reuses the T2 batch-parse + isolation/control/aborted-txn filter logic,
  MOVED from the gateway into the worker.
- **Client:** on Subscribe, opens one dynamic-MDC subscription per worker it reads
  from (dedup by worker); filters received Records by subscriptionId; assembles
  per-image. Existing egress single-endpoint model is replaced by per-worker
  control-address subscriptions.

### Increments (each independently buildable + testable)
- **I1 (broker, worker owns egress pub):** Worker gains an `aeron::Aeron*` + config
  (control host, `CTRL_BASE`, `EGRESS_BASE`). Worker creates its dynamic-MDC pub at
  start; sole writer. NodeConfig + BrokerNode wiring. Test: worker pub resolves; a
  raw dynamic-MDC subscriber receives a hand-offered frame. (No push logic yet.)
- **I2 (broker, sub state to worker):** Add `WorkerOp::Subscribe`/`Unsubscribe` +
  worker-local sub table + cursors. Gateway routes Subscribe/Unsubscribe to owner
  worker instead of tracking push itself; PushRead op + return-ring push path
  removed. Test: worker records/removes subs via ring ops.
- **I3 (broker, in-worker read+encode+push):** Move the batch-parse + Records encode
  + isolation/control/aborted-txn filter + HW/LSO bound + tryClaim/offer loop into
  Worker::doWork over owned subs, using `readView` (zero-copy) + per-worker scratch.
  Gateway stops all push/egress for worker-owned partitions. Test (in-process): a
  dynamic-MDC subscriber receives all produced batches, in order, once.
- **I4 (client, per-worker MDC subscribe + filter):** rune-client-cpp subscribes to
  per-worker control endpoints from the Subscribe/Metadata response; filters by
  subscriptionId; per-image assembly. Test: client reads a partition end-to-end
  over UDP.
- **I5 (T5) multi-consumer e2e + read bench:** (a) group of 2–4 over a partitioned
  topic (exact coverage, no dup/loss); (b) N fan-out subs on same partitions (each
  gets full stream); aggregate read throughput over real UDP; exit = read > write.

### Open scope item — read_committed txn state ownership
LSO + aborted-txn index (`PartitionTxnState`, `getTxnStateRO`) currently live in the
gateway. In-worker filtering (I3) needs per-partition txn state on the worker thread.
Options: (a) migrate txn state to worker-owned (shared-nothing, larger); (b) for
read_uncommitted paths (the throughput bench) bound by HW only and defer full
read_committed in-worker filtering to a follow-up. Decision pending; the bench
(exit criterion) uses read_uncommitted, so I3 can land HW-bounded first and add
read_committed worker-side afterward.
