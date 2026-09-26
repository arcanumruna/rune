# Rune Produce Reliability (any-size, zero-loss) — Design

**Date:** 2026-09-26
**Status:** Draft for review
**Owner:** vradu
**Related:** bug `rune-single-partition-produce-loss`; benchmark spec `2026-09-26-rune-vs-kafka-benchmark-design.md`; deferred task #23.

## Goal

Guarantee that **every record a producer accepts is either persisted-and-acked
or explicitly failed after bounded reliable delivery** — at ANY partition
count, ANY batch size, ANY in-flight depth. Never silently lost, never
abandoned. Invariant: **persisted ⟹ acked; acked ⟹ persisted.**

## Overriding Constraint (north star)

**Zero cost on the happy path.** The ack-before-timeout path — the >99.99%
case — must be byte-for-byte as fast as today: no new hot-path branches, no new
per-record allocations, no new copies, no new virtual calls (see
`rune-no-virtual-calls-hot-path`, `rune-ull-engineering-standard`). All
reliability machinery engages ONLY on the cold path (timeout / loss / oversize).
Every change is benchmark-validated against the current numbers before merge.

## Root Cause (established, instrumented)

1. Client ignores `linger=0` (`Producer.cpp:22`, `if (val>0)`) → 5 ms default →
   giant single-partition batches (~980 KB).
2. A giant frame fragments over Aeron/UDP into ~700 fragments; under load one
   drops → FragmentAssembler discards the whole message → broker never sees it →
   not persisted, not acked, no log. Probabilistic. Multi-partition survives
   because frames stay small.
3. Client's produce-timeout path FAILS (abandons) the records instead of
   retransmitting.

## Design — four surgical parts

### Part A — Client per-frame wire-size cap (bounds fragmentation)

The builder currently flushes at `batchBytes`. Introduce a transport-safe
`maxFrameBytes` (default ≈ **60 KB**, below the point where loopback/LAN
fragment-loss becomes likely; tunable). Effective flush threshold =
`min(batchBytes, maxFrameBytes)`. A logical batch larger than the cap is emitted
as **multiple valid RecordBatch frames**, each independently ack-tracked.

- **Hot-path cost:** none. It is a smaller constant compared against the same
  `builderBytes` already computed each drain. Normal batches (< cap) never split.
- Honor `linger` including 0 (change `Producer.cpp:22` to `val >= 0`); a
  sentinel `-1`/unset keeps the 5 ms default.

### Part B — Aeron flow-control + term sizing (makes loss rare)

Configure ingress (client→gateway) and egress (broker→client) publications so
the publisher cannot outrun the subscriber's term buffer (the actual cause of
loopback loss). Pick a flow-control strategy that back-pressures rather than
drops, and size term buffers for the max in-flight window. This turns Aeron's
built-in NAK/retransmit into effective reliability so **Part C retries become a
rare backstop, not a steady-state cost.**

- **Hot-path cost:** none (publication config).
- Decide: min vs. tagged flow control; term-buffer bytes; MTU. Must not
  reintroduce the `fc=min` stall that was removed from WorkerEgress.

### Part C — Client idempotent retransmit (the hard guarantee)

The producer already stamps producer-id + sequence (PSM infra exists). Today the
produce timeout FAILS un-acked records. Change: on timeout, **retransmit the
un-acked frame with the same pid+seq**; broker dedups and re-acks the original
offset. Bounded by a `deliveryTimeoutMs` (total), then fail loudly only if truly
undeliverable.

- **ULL design:** the encoded frame bytes must survive until acked so they can
  be retransmitted. Retain them in the **existing pre-allocated in-flight
  structure** (the maxInFlight window already holds un-acked batches; the
  ack-ring already tracks corrIds) — **no new per-send allocation**. Reclaim on
  ack exactly as today.
- **Hot-path cost:** none. The happy path reclaims on ack before the timer ever
  fires. Retransmit is driven by the SAME coarse per-duty-cycle timeout check
  that exists now — only its *action* changes (fail → retransmit), so no new
  timer, no new scan.
- Retry is per-frame (idempotent), preserving order via pid+seq.

### Part D — Broker: never silent-drop, atomic, dedup-on-Leader

- Any frame that cannot be handled is **loud-rejected** (`MessageTooLarge` /
  explicit error), never silently dropped. (The fragment-loss path never reaches
  the handler — Parts A/B prevent it; this covers residual oversize.)
- Batch append is **atomic** (all-or-nothing) — verify no partial-persist.
- **PSM dedup active for acks=Leader** so a retransmit returns the original
  offset instead of re-appending or dropping. Verify the Leader path (today PSM
  is exercised; confirm the tier-0/Leader route dedups).

## Guarantee argument

- Part A: frames are always small enough to rarely fragment-lose.
- Part B: publisher can't overflow the receiver → Aeron recovers in-term losses.
- Part C: anything Aeron still can't deliver is retransmitted idempotently until
  acked or a hard deadline → **no silent loss, no abandonment**.
- Part D: broker never drops silently and never double-appends → retransmit is
  safe → **persisted ⟺ acked**.

## No-regression validation (mandatory before merge)

Run the existing bench (`bench/kafka-compare/`) on the VM/ccx43 and compare to
pre-change numbers:
- write throughput (multi-partition) — must be within noise of today.
- produce p50/p99/p99.9 latency — must be within noise of today.
- single-partition write at large window/batch — must now be **100% delivered**
  (succeeded == attempted) at any size.
- crash/consume check: consume returns exactly what was acked.

## Scope / files (indicative)

- `rune-client-cpp`: `Producer.cpp` (linger), `ProducerImpl.cpp`/`.h`
  (frame cap + split, retransmit-on-timeout, retain-until-ack), producer config
  (`maxFrameBytes`, `deliveryTimeoutMs`).
- `rune-broker`: gateway produce handler (loud-reject residual oversize; confirm
  atomic append + PSM dedup on Leader); Aeron publication flow-control/term
  config for ingress + egress.

## Open questions (for review)

1. `maxFrameBytes` default — 60 KB, or derive from Aeron MTU? (affects Part A)
2. Flow-control strategy for ingress/egress — which, and term-buffer sizes?
3. `deliveryTimeoutMs` default + retry backoff shape.
4. Does the current acks=Leader path already run PSM dedup, or is that new work?

## Self-review

- Happy-path-zero-cost is stated as the binding constraint and each part
  argues its hot-path cost = none.
- Guarantee argument closes the loop (A+B reduce loss, C guarantees delivery,
  D makes retransmit safe).
- Validation ties back to the existing benchmark so regressions are caught.
