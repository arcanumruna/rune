# Review: record-egress pub lifetime (retire-on-ack), 2026-10-05

Branch `fix/egress-pub-lifetime` (wt52), commit `ca7a789` (`git diff 6a67487..ca7a789`).

## Verdict: APPROVE. No critical or major findings. Two mediums and four lows.

The design is correct. The release op travels on the same SPSC op ring (`ingress_`) as every Subscribe that carried the pointer, and the worker matches on the pointer value only. The ack goes through `enqueueSignal_`, which never drops it. Destruction happens only on the ack, on the runtime thread, so no shared_ptr crosses threads. The change adds no atomics, mutexes, or virtual calls. The only hot-path cost is one predictable branch in `doWork` (`retiringUnsent_ == 0`) and one in `executeUnsubscribeWire_`. Nothing is allocated per pass.

## Verification done
- `ctest -R RecordEgressLifetime`: 9/9 green.
- RED check: I removed only the two-line retire loop in `closeSession_` (all other changes kept) and rebuilt. 7/9 then failed, including the core UAF probe (`pub.destroyed == 1` while the worker still references it). The other 2 tests cover the separate resolve-path fixes. The source was restored afterwards and the tree is clean.

## Path audit (check 1): every path that frees or replaces a held pointer
| Path | Result |
|---|---|
| Close, expire, reap, SessionFailed, Disconnect, cap eviction | All of them funnel into `closeSession_`. `sessions_.remove` is the only erase in the codebase, and nothing calls `recordEgressByWorker.erase/clear`. Covered. |
| Re-resolve replacement | `onRecordEgressResolved_` never overwrites a filled slot. Covered. |
| Late resolve after close | Nothing is handed out, and `pendingRecordEgress_` is purged at close. Covered. |
| Unsubscribe, group revoke | Never frees the pub. Safe. |
| Leadership change, subscription "moves" | `router_` is set only in the `WorkerPool` constructor (WorkerPool.h:132/653), so (t,p)→worker is fixed and a pub keyed w only ever reaches w. A Subscribe refused by the ownership guard is parked inactive in `subs_` (`refuseNotOwned_`, Worker.h:4471), and the release still drops it because it scans all of `subs_`. Safe. |
| Worker-pool restart | Not possible: the pool is created once (BrokerNode.cpp:1347). |
| Shutdown | `stop()` joins the runners (2845), then resets `workerPool_` (2919) and then `gateway_` (2929). `~ClientGateway` is defaulted and never calls `closeSession_`. `retiringEgress_` is destroyed before `sessions_`. Safe. |
| Other copies on the worker side | `WorkerSubscription::egressPub` is the only place the worker stores the pointer (grep). The push loop and `sendPushTooLarge_` use it only within a single pass, and the release runs in the drain phase, not during the push loop. Covered. |

## FIFO (check 2)
A Subscribe queued in `pendingSubOps_` never carries a pointer: `trySubOp_` re-resolves it from the Session at send time (ClientGateway.cpp:2660-2668), so once the session is gone the op is moot and dropped. The only ops that carry X were therefore enqueued directly on the ring before close, and the release, whether sent directly or retried later, always lands behind them. The release skipping ahead of the session's queued Unsubscribes is harmless: those later become no-ops. The release also bypasses `WorkerPool::enqueueOp`'s deliver-SetOwnership-first gate, which is fine because Unsubscribe is ownership-neutral.

## Ack integrity (check 3)
- Each token is unique (monotonic `nextRetireToken_`) and the match also requires the worker. Only one release op is ever sent per entry (`enqueued` flag), so there is no duplicate ack.
- Session ids are never reused (`SessionTable::nextSessionId` is monotonic), and the ack is matched on the token, not the sid, anyway.
- Pointer ABA is impossible: X stays alive until its ack, so no live pub can share X's address while a release for X is in flight.
- A stuck worker holds only its own entries and blocks no other session's progress.

## Findings

### MEDIUM-1: `pumpEgressReleases_` has no per-worker break and scans the whole list every cycle (ClientGateway.cpp:2856-2866)
While `retiringUnsent_ > 0`, every gateway `doWork` walks all of `retiringEgress_` (acked entries included) and calls `tryEnqueueOp` (a `tryClaim`) for every unsent entry. It keeps doing this even after the same worker's ring has just refused one.

**Scenario:** a mass disconnect (thousands of sessions closing in one reap) while one worker's ring is saturated. That costs N failed `tryClaim`s plus an O(total retiring) scan per runtime cycle until the ring drains. This lands on the runtime thread, which also drives ingress.

`pumpSubOps_` already uses the right pattern: break per worker on the first refusal. Fixes:
- Remember the refused worker within the pass (a `uint64_t` mask, since there are at most 64 workers) and skip its remaining entries.
- Optionally, keep unsent entries in a per-worker FIFO so the scan is O(unsent) rather than O(retiring).

### MEDIUM-2: the FIFO invariant the design depends on has no regression test (RecordEgressLifetimeTest.cpp)
The safety argument for (2) rests on `trySubOp_` re-resolving the pointer rather than capturing it. No test puts the closing session's own Subscribe into `pendingSubOps_`. In `ReleaseRefusedByAFullRingIsRetriedNeverDropped`, `r.subscribe(sid,0)` runs before the ring is filled, so it goes direct, and the queued ops belong to sessionId 0.

A future change that caches `egressPub` in `PendingSubOp` (it is a natural optimization, and `p.mdc` already sits next to it) would bring the UAF back with every test still green.

Add a test along these lines:
1. Fill the ring.
2. `r.subscribe(sid, 0)` so the Subscribe is queued.
3. Close the session.
4. Pump and run the worker until settled.
5. Assert the worker never holds `pub.ptr()` after the ack, and that `pub.destroyed == 1` only after it has no reference.

### LOW-1: stale contract comments on `enqueueSignal_` (Worker.h:1575, :4917)
Both comments still say the bypass is "StorageFailure / OwnershipAck only". Add EgressReleased to them.

### LOW-2: the release-op wire overload is documented only at the call sites
The release op is `Unsubscribe` with `egressPub != 0`. `WorkerOp.h`, where `Unsubscribe` and `egressPub` are documented, should state the discriminator. Otherwise a future Unsubscribe that sets `egressPub` would silently be treated as a release.

### LOW-3: misleading error log in `onEgressReleased_` (ClientGateway.cpp:2873)
The "unreachable" `!r.enqueued` case breaks out and logs "unknown retire token", which is the wrong diagnosis. Give it its own log line, or an assert.

### LOW-4: test rig coverage gaps
- The rig runs shard mode only, so the non-shard `closeSession_` path is not exercised. The logic is mode-independent, so the risk is low.
- The push loop never reaches a send (the logs are empty), so the test proves reference-dropping, not the absence of a write after the release. This is an accepted limit of the fake pub, but a dockerized DR rerun is still owed, as the report itself notes.

## Memory (check 5)
`retiringEgress_` is bounded by in-flight closes multiplied by workers per session, and it drains on acks. Its capacity is never shrunk, which is acceptable. It grows without bound only if a worker stops draining its op ring, and a worker in that state is already a broker-halt condition. `push_back` and the worker's `pendingSignals_` growth happen at close rate only.
