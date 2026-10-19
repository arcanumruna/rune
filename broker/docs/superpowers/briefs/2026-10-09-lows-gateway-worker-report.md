# Lows batch C (gateway, worker, DR harness): report

Worktree `rune-broker-wt72`, branch `fix/lows-gateway-worker`, based on integ/perf-defaults `4b0b6a6`. Not merged or
pushed. No Docker was run.

## Summary

- 7 commits: 3 product fixes and 4 test or harness commits.
- **Found while doing R3: a real data-loss bug.** On an encrypted (TLS) session, a Produce that was decrypted and then
  ABORTed was dropped when it was redelivered, because its own replay check failed. This hit both the gateway's
  full-worker-share deferral and the worker's deferred-roll hold. It is fixed and has RED->GREEN tests on both
  sides (`2663ff8`).
- Two items were already closed by earlier work (`f2ae2c6`, `51312d6`, `b0129b9`, `bc153c3`). They are listed with
  their commits.
- One item is out of scope for this repository: redirect LOW-2 lives in rune-client-cpp. It is still open on
  client `main`.

| Commit | Item |
|---|---|
| `eb00b5b` | D8c cap-expiry reader marks cover the last 64-poll scan window, as in D8d |
| `f937b9d` | Tombstones: sized at kMaxSessions, O(1) lookup |
| `2663ff8` | R3 (cap-deferred produce no longer re-decoded) + the TLS replay drop found with it |
| `b4149c9` | R5: two-worker credit test |
| `a1df60f` | D8b tests moved to the default transport; BACK_PRESSURED->Found and MEDIUM-1 real-driver tests |
| `710915d` | Egress LOW-4: a real publication that pushes, plus non-shard mode |
| `c01359d` | D2-10: harness container cleanup |

## 1. D8b-L (re-review round 2 lows, `2026-10-04-d8b-review.md`)

**LOW-1: lostSinceNs is not restarted by a Found while another worker is still Lost.**
- Status: already done in `f2ae2c6` (D11 batch).
- Test: `GatewayShardNotifyRegression.FoundOnOneWorkerRestartsTheLostLeaseOfAnotherRegression`. It was RED before
  that fix and is GREEN now.

**LOW-2: the Progress-rate comment.**
- Status: already done in `f2ae2c6`.
- The comment at `SessionAckSink.h:154-161` now says the rate is pass-rate / 1024.

**LOW-3: edges_ adds a second cache line per claim. Ruled out, with a reason.**
- Folding a watched bit into `PubReady` would no longer save the `edges_` touch. Since D8c and D8d, every claimed
  ack already reads or writes `edges_[idx]` anyway:
  - D8d's `kDfDelivered` / `kDfExpiring` check sits on the success branch (`SessionAckSink.h:373-375`);
  - every gateway-routed ack goes through `noteCredit_` (`e.credits`);
  - every direct ack goes through `directDone_` (`e.direct`).
- The extra line is therefore paid for by accounting that is required regardless.
- It was measured in D11 (`BM_SessionAckSinkFlush`, 3 interleaved A/B rounds): +0.25-0.35 ns per ack with a
  bump-pointer claim, and under 1% with a real Aeron claim.
- The memory point is moot too. `Edges` is 24 B, about 96 KiB per sink, and it holds the D8c/D8d per-session state.

**LOW-4: real-driver tests.**
- *Gateway image release, and B's full intake while A holds:* already done in `51312d6`.
  `KilledPausedClientsSessionAndAcksAreFreedRegression` asserts both. As of `a1df60f` it runs on the default
  transport.
- *BACK_PRESSURED -> Found path:* done in `a1df60f`.
  - Test: `ShardModeRegression.PausedInvokerClientResumingWithoutReadingIsFoundNotReapedRegression`, real driver,
    default config and transport.
  - Scenario: an Invoker client is held, goes Lost by not pumping, then pumps again without reading. The worker's
    claim then returns BACK_PRESSURED, so the session is Found. It stays Held past the Lost lease (10 s + 2 s), then
    reads every ack.
  - RED evidence: a mutant in which only a successful claim clears Lost fails the test (the session is reaped,
    `ShardModeTest.cpp:1714/1716/1719`).
- *MEDIUM-1 path (the Released edge refreshes the lease):* done in `a1df60f`.
  - Test: `ShardModeRegression.HeldDirectClientRecoveringAfterTheLeaseIsNotReapedOnReleaseRegression`.
  - Scenario: a direct client is held and silent past the 10 s lease. Its driver stays alive, so it is never Lost. It
    then reads a one-ack backlog in one pass, so no Progress is sent.
  - RED evidence: a mutant without the f08e935 refresh on Released fails 3 of 3 runs ("reaped in the cycle it was
    Released").
  - **Design note:** the first version, which produced in steps of 32 frames, was GREEN even under the mutant. A
    diagnostic run showed why: frames the worker had refused while the session was held were admitted after the
    drain, and their F23 ack-enqueue notification (`ClientGateway.cpp` deferAck touch) refreshed the lease. The test
    now sends one frame at a time, waiting for each append, so no refused frame is waiting in the image.
- New test seams:
  - `ClientGateway::sessionHeldForTest`;
  - `ShardTestClient::pumpWithoutReading`.

## 2. D8c / D8d follow-ups

**Align D8c's per-episode reader marks with D8d's 64-pass window.**
- Done in `eb00b5b`.
- Behaviour now: when `onWorkerCapFull_` scans and finds no victim, it issues a fresh window tag, exactly as
  `directEpisode_` does. A held session that credited once early in a long episode and then stalled is expired at
  the next scan.
- Before: it counted as a reader for the whole episode.
- Cost: cold path only.
- Test: `GatewayProduceCreditRegression.CapExpiryReaderMarksCoverTheLastScanWindowOnlyRegression`. RED before the
  fix (0 expiries after 2 windows), GREEN after.

**Tombstone limit 1024.**
- Fixed in `f937b9d`, not just justified.
- **Bug 1, size:** the 1024-entry ring was smaller than the session table (4095).
  - Failure: one gateway reap cycle that closes every session overwrote the oldest tombstones.
  - Effect: those clients' buffered direct frames were appended without being answered, then duplicated by the
    resend after SessionExpired.
- **Bug 2, cost:** `closedRecently_` scanned all 1024 entries linearly for every frame of an unknown session. That
  covers both a closed client's backlog and a session whose announcement has not been applied yet.
  - Measured (-O2, Mac, standalone probe on the real header): 180-345 ns per frame.
  - So one 4096-fragment poll of a closed backlog cost about 1.4 ms, far over the 100 µs pass budget.
- **Fix:**
  - Size: `kClosedTombstones = kMaxSessions`.
  - Structure: a FIFO ring plus an open-addressing set (load ≤ 1/2, backward-shift delete on eviction).
  - Lookup cost: about 2 ns.
  - Memory: +88 KiB per worker sink, heap-allocated once.
- **Why kMaxSessions is enough (justification):**
  - A mass close of every session fits.
  - A tombstone is evicted only after 4096 newer closes, and each of those needs a new session to be announced (a
    Connect round trip).
  - Meanwhile the closed client's image, which holds at most its term window, drains in a few passes: those frames
    are consumed, never appended.
- Test: `SessionAckSinkRegression.MassCloseOfEverySessionKeepsEveryTombstoneRegression`. RED before ("closed session
  1 lost its tombstone"), GREEN after.

**R3: a refused frame is re-decoded each cycle.**
- Fixed in `2663ff8`. This is where the TLS bug was found.
- **Bug found:** `onClientFrame_` decrypted with `openIngressFrame`, which commits the seq to the session's
  anti-replay window before the frame is known to be consumed. The worker's `handleWorkerIngressFrame_` did the same
  with `openProduceFrame`.
  - Gateway path: a Produce deferred by a full worker share is ABORTed. Its redelivery failed the replay check and was
    silently dropped (consumed, never forwarded or answered).
  - Worker path: the same happened to a frame held by a deferred roll (hotpath-A).
- **Fix for the bug:**
  - New functions that open without committing, plus a separate commit step (`openIngressFrameUncommitted` /
    `commitIngressSeq` and `openProduceFrameUncommitted` / `commitProduceSeq`). The old open-and-commit functions
    remain as wrappers.
  - The gateway commits a Produce's seq only when `handleProduce` has consumed it. Other frames are always consumed,
    so they commit at once.
  - The worker commits only after the frame passes the roll hold, or when it drops the frame as undecodable.
- **Fix for R3 itself:**
  - A cap-deferred session is memoised in `Session::capDeferredOn`.
  - While that worker still refuses, `admitClientFrame_` ABORTs the redelivered frame with one probe, before any
    decrypt, decode or registry lookup.
  - Counts and expiry pacing are unchanged: `workerCapProduceAborts_` and `onWorkerCapFull_` are still advanced per
    redelivery.
  - The fast path gains one OR: `capDeferredSessions_` is on the same line as `heldSessions_` and
    `creditBlockedSessions_`.
- **Measured** (Debug build, 64 KiB batch, 1000 sessions, per redelivery):
  - full path: 1.6-2.9 µs plaintext, 23-26 µs TLS;
  - memo: 0.7-1.0 µs.
- **Caveat (documented in SessionTable.h):** a session's frames arrive on one image, so the head frame is the deferred
  one. A second image of the same session would only wait behind it (the LOW-R4 head-of-line). Nothing is lost.
- Tests, all RED before and GREEN after:
  - `GatewayProduceCreditRegression.EncryptedProduceDeferredByAFullWorkerShareIsForwardedNotDroppedRegression`. RED:
    the first redelivery was consumed, that is, dropped. The test also asserts that a real replay of a consumed frame
    is still dropped.
  - `WorkerProduceRegression.EncryptedFrameHeldByADeferredRollIsAppendedOnRedeliveryRegression`. RED: 4 answers, not
    5; the frame was not appended. The test also asserts the replay is dropped.
  - `GatewayProduceCreditRegression.CapDeferredProduceIsNotReDecodedOnRedeliveryRegression`. RED: 0 memo aborts
    where 10 were expected.

**R4: a frame refused for one worker blocks the session's other produces, Metadata and Keepalive.**
- Already documented in `b0129b9`: the LOW-R4 paragraph of the shard-mode poll comment in `ClientGateway.cpp`
  `doWork`.
- `2663ff8` adds the cross-image case to the `capDeferredOn` doc.
- Ruled as by design (Kafka-like in-flight per connection).

**R5: no multi-worker credit test.**
- Done in `b4149c9`.
- Test: `GatewayProduceCreditRegression.CreditsAndCloseAreAttributedPerWorkerWithTwoWorkersRegression`. It uses a
  2-worker pool with partitions picked per owner, and checks four things:
  - the counters are per worker;
  - a Credit naming worker 1 returns only worker 1's share;
  - an excess Credit is clamped and counted, never taken from worker 0;
  - a close returns the session's share on every worker.
- This is coverage only. RED evidence: a mutant that attributes every Credit to worker 0 fails it.

**The two D8b tests still on the legacy transport.**
- Moved in `a1df60f` to the default (host-derived) transport:
  - `KilledPausedClientsSessionAndAcksAreFreedRegression`, with both its clients;
  - the paused-Invoker pair, `PausedInvokerClientSurvivesNotConnectedRegression` and
    `PausedInvokerCreditPauseIsNotAHoldRegression`.
- Stability: 27/27 in 3 concurrent processes ×3, then 30/30 (with the two new tests) in 3 concurrent processes ×2.

## 3. D10-L lows

**Egress review LOW-4: the rig is shard-mode only, and the push never sends.**
- Done in `710915d`, with two tests:
  - `RecordEgressLifetimeRealPubRegression.PushesThroughARealPubUntilTheReleaseThenNever`. It uses a real driver and
    worker. The worker pushes Records through the session's REAL record-egress publication. The publication outlives
    the close until the worker's release ack and is destroyed on that ack. The test then asserts that later appends
    to the partition push nothing more and that no cursor of the closed session remains.
  - `RecordEgressLifetimeRegression.NonShardModeCloseKeepsThePubAliveUntilTheWorkerReleasesIt`.
- RED evidence: a mutant that destroys the publication together with the session fails both tests.

**Redirect review LOW-2: a back-pressure refusal re-offers the whole window. Not fixable here.**
- The code is in rune-client-cpp (`ProducerImpl::queueRouteResend_`), not in the broker, and this brief restricts work
  to wt72.
- It is still open on client `main`: `queueRouteResend_` marks every in-flight batch `routeResendPending`.
- The review's option still applies: mark only the batches whose lane is stale (`b.laneGen != ps.laneGen`) plus the
  refused one.
- It needs a client-repo item.

**D2-10 harness: a timed-out client container leaks.**
- The core fix was already in `bc153c3`, landed 10 minutes after the backlog note was written: `dockerutil.run`
  removes a *named* container on `TimeoutExpired`. Every `docker run` in `driver.py` goes through that function.
- `c01359d` closes the two leaks that remained:
  1. **Unnamed containers** (the netem and volume sidecars in `faults.py`) had nothing to remove by name.
  2. **A session ended by pytest-timeout.** `pytest.ini` uses `timeout_method = thread`, which calls `os._exit`. That
     means no `TimeoutExpired` and no `finally`, so the client containers kept producing into the next session.
- **How it works now:**
  - Every harness `docker run` gets two labels: `rune.dr.owner=<pid>` and `rune.dr.run=<uuid>`.
  - On a timeout, the containers of that invocation are removed by label, whether named or not.
  - An autouse session fixture removes containers whose owning harness process is gone.
  - A live concurrent run is never touched, and if Docker is down the fixture does nothing.
- Unit tests, no Docker (`dr-e2e/tests/test_harness_dockerutil.py`, CLI faked): RED against a stub, 9/9 GREEN. Run
  with Python 3.9 + pytest 8.4 (wt26's dr-e2e venv, used read-only).

## Hot path

- **Gateway per-frame admit:** one more OR in the `[[likely]]` branch.
- **Gateway redelivery of a deferred frame:** now one probe instead of a decrypt, decode and lookups (measured above).
- **Worker:**
  - the tombstone lookup is O(1) instead of a 1024-entry scan, but only for frames of unknown sessions;
  - the commit-after-hold adds one predictable branch, only for encrypted sessions.
- **Rules:** no atomics, allocation or virtual calls added. `check_no_atomics`: OK, with the allowlist unchanged.

## Tests run

- Targets run:
  - `rune_gateway_produce_credit_tests` 14/14;
  - `rune_session_ack_sink_tests` 43/43;
  - `rune_shard_worker_ack_tests` 30/30;
  - `rune_worker_produce_path_tests` 11/11;
  - `rune_gateway_record_egress_lifetime_tests` 11/11;
  - the crypto ctest set 73/73, with 4 RedToggle tests skipped by design;
  - the gateway, sink, worker, crypto and shard-notify ctest set (148/148 mid-run).
- Final affected-target ctest run (`-j4`, 391 tests selected): 138 passed and 0 failed, then it stalled.
  - Cause: from about 00:46 every newly launched unsigned dev binary on the host hung in uninterruptible exec (state
    `UN`, 0 CPU). This hit `rune_gateway_crypto_tests`, `rune_session_ack_sink_tests`, and another agent's
    `rune_group_tests`, while system binaries still ran.
  - It is a host problem (syspolicyd / exec), not a test failure. The remaining 253 were not run in that pass.
  - Every one of those targets had already passed individually after its last code change (list above).
- No latency-gate tests and no Docker were run.

## Concerns

- The TLS replay-drop bug (`2663ff8`) also existed on `main` (Task 22, 2026-10-08) for every encrypted session whose
  produce is deferred by a full worker share or held by a deferred roll. It deserves a line in the D-series tracking
  and a TLS DR-e2e rerun when the box run happens.
- The real-driver D8b tests depend on the wall clock by nature: the 5 s NOT_CONNECTED timeout plus the 10 s lease.
  They take 13-20 s each.
- Redirect LOW-2 needs a ticket in rune-client-cpp.
