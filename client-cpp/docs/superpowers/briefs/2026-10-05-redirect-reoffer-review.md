# Review: rune-client-cpp 8d2f826 — refused redirect re-offer re-driven by per-pass route resend (DR-2)

Reviewer: Claude (Opus), 2026-10-05. Worktree: /Users/vradu/Projects/rune-client-cpp-wt16.

## Verdict: APPROVE WITH CHANGES (no CRITICAL/MAJOR; two MEDIUM ordering gaps, both EOS-safe)

The fix is correct for the DR-2 failure: a refused redirect re-offer now becomes
`routeResendPending` and `flushBatches -> resendRoute_` re-offers it in order once the
route publication accepts, instead of waiting ~30 s for the delivery timeout.

## Verification
- Built (`cmake --build build`). `rune_invoker_tests` 438/438, `rune_redirect_tests` 5/5,
  `rune_multinode_routing_tests` 6/6, `rune_notleader_retry_tests` 2/2 green.
- With only `src/producer/ProducerImpl.cpp` reverted to 8d2f826^, the new test FAILS at
  ProducerRouteParkTest.cpp:426 ("the refused redirect re-offer was never re-driven"); the other
  10 ProducerRoutePark tests still pass. Source restored afterwards; the worktree is clean.

## What checks out
- **Identity/EOS:** the refused `offerAttempt_` puts the previous tag back (`stampAttempt_(b, prev)`),
  so nothing changes on the wire. The later `resendRoute_` sends the same frame with the same
  producerId, epoch and baseSequence and a new attempt tag. Late seed Redirects for that attempt
  hit the "superseded attempt" branch (ProducerImpl.cpp:2757), so they do not use up redirect budget.
- **NEW-4(b) lanes:** `++ps.laneGen` happens before the offer. A refused offer leaves b.laneGen and
  laneOut unchanged. The first successful re-offer moves laneOut into staleLaneOut and updates laneGen.
  The seed's late answers then decrement staleLaneOut through `noteAnswer_`. This is correct for the
  younger batches swept up by `queueRouteResend_` as well.
- **Redirect budget:** the count goes up once per Redirect actually received. `resendRoute_` does not
  add to it. A refusal does not use up any extra budget.
- **routeParked / NotLeader / OOS:** `queueRouteResend_` takes over (clears) a NotLeader park, and a
  NotLeader that arrives later parks the partition again (N-b). In `flushBatches`, `resendRoute_` runs
  before `resendParked_`, and `offerAttempt_` unparks a re-sent OOS batch. The timer path
  (ProducerImpl.cpp:2719) skips `routeResendPending` batches, so there is no double-drive.
  Delivery timeout still bounds a publication that never connects.
- **Marking ALL in-flight batches pending:** this is dedup-safe. Every batch stored in flight is
  idempotent (acks=Leader/All; the window is clamped to the broker dedup window). In the DR-2 case the
  younger batches were also sent to the seed and will be redirected anyway, so re-offering them in
  order is the right thing to do.
- **Single-node sessionId:** `restampSessionId` writes into `b.wireFrame`, so the change persists. The
  flush sink with no routes calls `transport_.offer` without re-stamping, so the new sessionId goes out.
  With routes installed, `offerProduceToLeader_` re-stamps with the route leader's sid. Correct either way.
- **Hot path:** the extra work runs only on refusal: an O(inflight) flag sweep with no allocation,
  atomics or virtual calls. A pass whose `resendRoute_` is refused counts no work, so the idle strategy
  backs off. There is no busy-spin and no unbounded loop: delivery timeout bounds it.
- **Hub:** the `retryRedirectedProduce` return value now becomes false on refusal.
  `ProducerHub::retryRedirectedProduce` then tries the other impls, which reject by ordinal.
  Client.cpp ignores the result. This is harmless.

## Findings

### MEDIUM-1 — multi-node: a later Redirect can overtake a pending refused re-offer
ProducerImpl.cpp:2767-2773 (`retryRedirectedProduce`) never checks `ps.routeResend`.
- **Scenario:** A (older) and B (younger) were both sent to the seed. A's Redirect is refused, so
  `queueRouteResend_` marks A and B pending. B's Redirect arrives in the same poll or a later one, after
  the publication has connected but before the next `flushBatches` pass. B's tag still matches
  (`queueRouteResend_` does not bump tags), so B is offered directly, ahead of A. The leader answers B
  OutOfOrderSequence. Then `resendRoute_` re-offers A and B again (B gets a new tag, so the OOS answer
  is superseded).
- **Impact:** EOS-safe (sequence dedup plus OOS park), but it costs an extra round trip and a
  duplicate send. It also goes against the "keep order" intent.
- **Fix:** in `retryRedirectedProduce`, if `ps.routeResend` is set (a re-offer to this route is already
  queued), do not offer directly. Just make sure the batch is pending (`queueRouteResend_`) and return
  after the budget bump. Add a regression test: refuse A's redirect, connect, deliver B's redirect,
  and assert the wire order is A then B.

### MEDIUM-2 — single-node `resendAllRedirected`: order is not kept after a refusal (the comment says it is)
ProducerImpl.cpp:2790-2796.
- **Scenario:** `forEachLive` keeps offering after a refusal. `submitSend` is refused when the ring is
  full, and a smaller younger frame B can still fit after a large A was refused. So B goes out now
  through the ring and A goes out later through `resendRoute_`. They also travel on different
  publications: the ring's `kMsgSend` uses the default stream, while the flush sink uses
  `transport_.offer(b.ingressStreamId)`. So even "in order" offers have no ordering across streams.
- **Impact:** EOS-safe through OOS park, but the "(order kept)" comment is wrong.
- **Fix:** after the first refusal in a partition, mark that batch and every younger one
  `routeResendPending` without offering them.

### LOW-1 — single-node refused batches keep the old node's per-worker stream id
ProducerImpl.cpp:2795.
- The single-node refusal path sets the flags inline and does not call `queueRouteResend_`, so
  `b.ingressStreamId` is not refreshed from `ps.ingressStreamId`. The flush sink then offers on the
  stale per-worker stream on the retargeted transport, while the accepted batches went to stream 0
  (the ring fallback).
- **Fix:** refresh it the same way `queueRouteResend_` does, or force stream 0 for consistency with
  the ring path.

### LOW-2 — on a busy route, a transient back-pressure refusal re-offers the whole partition window
- `offerOn` returns false for back-pressure as well as for "not connected". In the multi-node path a
  single back-pressured redirect re-offer therefore re-sends every in-flight batch of the partition,
  including ones already accepted by the leader on the current lane. This is dedup-safe, but it
  multiplies traffic exactly when the system is back-pressured.
- **Option:** mark pending only the batches whose current lane is stale (`b.laneGen != ps.laneGen`),
  plus the refused one.

### LOW-3 — test coverage gaps
- The single-node `resendAllRedirected` refusal branch has no test.
- The new test does not assert that the re-offer carries a new attempt tag
  (`batchcorr::attempt(w.frames[1].corrId) != batchcorr::attempt(first.corrId)`), and it does not
  cover a younger in-flight batch (order plus dedup of the swept-up batches).
