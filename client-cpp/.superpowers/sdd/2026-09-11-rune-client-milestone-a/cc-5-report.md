# CC-5 Fix-Round Report

**Date:** 2026-09-12
**Branch:** main (local commits only)
**Base commit:** a706508

---

## Fix 1 — [MAJOR] Oversized submitSend no longer calls std::terminate

**Status: FIXED**

Added a size guard at the top of `writeCommand()` (noexcept):

```cpp
if (payloadLen > static_cast<std::size_t>(ring_.maxMsgLength())) {
    return false;
}
```

Also wrapped `ring_.write(...)` in a `try { ... } catch (...) { return false; }` block as defense-in-depth for any future Aeron check.

`ring_.maxMsgLength()` = ringCapacity / 8 (Aeron invariant).  With the default 64 KiB ring that is 8192 bytes.  The guard fires before `ManyToOneRingBuffer::checkMsgLength` throws `IllegalArgumentException`, so no `std::terminate` through `noexcept`.

**Test:** `ClientAgent.OversizedSubmitSendReturnsFalse` — submits a 9000-byte frame, asserts false return, then asserts a normal 4-byte frame succeeds.  PASS.

---

## Fix 2 — [MAJOR] sessionState()/sessionId() now thread-safe via atomic snapshots

**Status: FIXED**

Added to `ClientAgent` (header):

```cpp
std::atomic<session::SessionState> stateSnapshot_   {session::SessionState::Disconnected};
std::atomic<int64_t>               sessionIdSnapshot_ {0};
```

Published with **release** order:
- Once at the end of `start()` (initial value).
- At the end of **every** `doWork()` call (Steps 1–3 complete, then snapshot stored).

`sessionState()` / `sessionId()` now read the atomics with **acquire** order — no longer touch `session_.state()` / `session_.sessionId()` directly.  Safe from any thread.  Documented as "eventually-consistent observer snapshot, updated each doWork."

Mirrors the BrokerNode observer-atomic-mirror pattern.  Single-writer preserved: the atomics are the only cross-thread reads; `session_` and `transport_` remain single-writer (agent thread only).

**Test:** `ClientAgent.SessionStateAtomicSnapshotAfterConnect` — verifies pre-start Disconnected, post-start-idle Disconnected, and Connected + sessionId==42 after e2e handshake (snapshot read after doWork).  PASS.

---

## Fix 3 — [MINOR] setEgressObserver pre-start contract documented

**Status: FIXED**

Updated header doc comment:

```
/// MUST be called before start() (single-threaded setup phase only).
/// The observer is read on the agent thread; changing it after start() is
/// a data race.  CC-6 wires the observer once before start() — that is the
/// only supported usage pattern.
```

No runtime assert added (would complicate tests that call `setEgressObserver` before `start()` — all current callers already comply).  The contract is enforced by documentation and code review.

---

## Fix 4 — [NIT] Stale comments removed

**Status: FIXED**

Removed:
- The stale "Scratch buffer for ring write of send payloads" member comment in the header (the scratch buffer member no longer exists — `writeCommand` wraps a stack-frame `AtomicBuffer` directly).
- The abandoned tick-counting comment block in `doWork` step 3 that discussed "phantom work" and tick counting.

Updated the file-level `doWork` contract comment to correctly state "ring records drained + egress fragments polled" (ticks are not counted as work).

---

## New/Changed Tests

**5 new tests** added to `tests/agent/ClientAgentTest.cpp` (total: 7 → 12):

| # | Test | Covers |
|---|------|--------|
| 8 | `OversizedSubmitSendReturnsFalse` | FIX 1: oversized → false, no crash; subsequent normal submit succeeds |
| 9 | `RingFullReturnsFalse` | FIX 1 + review: tiny 128-byte ring, flood without draining → false return; submitConnect also returns false |
| 10 | `RetargetPostNullIngressOfferReturnsFalse` | FIX 3 / review: retarget to bogus UDP endpoint, offer() returns false / no throw, stop() clean |
| 11 | `MalformedEgressFrameDropped` | review: truncated 4-byte egress frame dropped silently; subsequent valid Pong reaches observer |
| 12 | `SessionStateAtomicSnapshotAfterConnect` | FIX 2: atomic snapshot Disconnected pre-start, Disconnected idle, Connected after handshake; sessionId==42 |

---

## Full-Suite Pass

| Suite | Tests | Result |
|-------|-------|--------|
| `rune_client_tests` (CC-1/CC-2) | 13 | ALL PASS |
| `rune_session_tests` (CC-4) | 59 | ALL PASS |
| `rune_transport_tests` (CC-3) | 5 | ALL PASS |
| `rune_agent_tests` (CC-5) | **12** | ALL PASS |

**Total: 89 tests, 0 failures.**

---

## Concerns

None.  All fixes are minimal and contained to the agent layer.  Single-writer invariant preserved (atomics are the only cross-thread data).
