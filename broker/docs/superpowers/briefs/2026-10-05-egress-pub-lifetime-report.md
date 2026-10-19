# Record-egress publication lifetime: report (2026-10-05)

Branch `fix/egress-pub-lifetime` (worktree `~/Projects/rune-broker-wt52`, base `6a67487`). Commit `ca7a789`.

## Bug
`ClientGateway::closeSession_` called `sessions_.remove(sid)`, which destroyed the session's per-worker record-egress `ExclusivePublication`s. The owning worker still held their raw pointers in `WorkerSubscription::egressPub` and pushed through them on every pass. The result was a cross-thread use-after-free, which showed up as `maxMessageLength 0 B` followed by an aeron `OutOfBoundsException` (exit 133) or a SIGSEGV (exit 139).

## Design: retire-on-ack
- **Gateway side.** On close, each pub in `Session::recordEgressByWorker` moves into `retiringEgress_` (`RetiringEgress{token, sessionId, worker, shared_ptr pub, enqueued}`). The shared_ptr stays on the runtime thread.
- **Release op.** The gateway then enqueues a release op on worker w's **op ring** (`ingress_`). This is the same FIFO ring that carries every Subscribe op holding that pointer, so the release runs after all of them. The op is a `WorkerOp::Type::Unsubscribe` with `egressPub = pointer`, `subscriptionId = 0`, and `correlationId = retire token`.
  - No schema change was needed. A normal Unsubscribe never sets `egressPub`.
  - The lifecycle ring (`SessionClose`) could **not** carry the ack. The worker drains that ring before the op ring, so it would overtake a Subscribe that is still in flight.
- **Worker side.** `executeEgressReleaseWire_` is cold and noinline. It drops every subscription whose `egressPub == pointer` (comparing the value only, never dereferencing it). It then emits `SessionSignal::EgressReleased` (new kind 8, `baseOffset = token`) via `enqueueSignal_`, which is never dropped.
- **Destruction.** `onSessionSignal_` handles `EgressReleased` with `onEgressReleased_(worker, token)`, which erases the entry and so destroys the pub. Nothing else frees it.
- **Never dropped.** If a full ring refuses the release, `retiringUnsent_` stays above zero and `pumpEgressReleases_()` retries it every `doWork`. This is event-driven, with no timer.
- **Hard rules respected.** No atomics or mutexes, no shared_ptr crosses threads, and nothing was added to the push hot loop.

## Audited paths
| Path | Verdict |
|---|---|
| closeSession_ (reap, Disconnect, SessionFailed, worker Expire, cap eviction; every close funnels here) | **Bug, fixed** (retire-on-ack) |
| `pollPendingRecordEgress_` resolves while the session is gone | **Bug, fixed.** It handed the raw pointer of a local pub that died at the end of the scope. Unreachable today because `closeSession_` purges pending resolves, but now nothing is handed out. |
| A re-resolve overwrites `recordEgressByWorker[w]` | **Latent, fixed.** A cached pub is never replaced; the extra resolve dies without ever being handed out. Unreachable today because the forward path only resolves when nothing is cached. |
| `trySubOp_` / `pendingSubOps_` queued Subscribe | Safe. It re-resolves from the Session at send time, and a closed session makes it moot, so no queued op can carry a retired pointer. |
| Unsubscribe (client or group) | Safe. It never frees the pub. |
| Leadership change / group rebalance | Safe. Every path goes through `forwardSubscribeToWorker_` / `enqueueSubscribeOp_`. The router is fixed for the pool's lifetime, so a pub for worker w only ever reaches w. |
| Session close leaves worker cursors behind | **Leak, fixed.** Close now forwards an Unsubscribe for each of the session's worker subscriptions. MDC subscriptions used to outlive the session and push forever. |
| Shutdown | Verified. `BrokerNode::stop` 1a joins the worker runners (fused mode included) before `workerPool_.reset()` and `gateway_.reset()`. The gateway then destroys any still-retiring pubs. `retiringEgress_` is declared after `aeron_` and `sessions_`, so it is destroyed before them. |
| Test-only migration API (`releaseReplicaForTestMigration`) | Not a production path; not changed. |

## Tests
`libs/gateway/tests/gateway/RecordEgressLifetimeTest.cpp` contains `RecordEgressLifetimeRegression`, 9 tests. They use a real gateway, a real `WorkerPool`, a `TopicRegistry` and the `applySubscribeForTest` production path. The publication is a fake: a shared_ptr with a counting deleter, never dereferenced. There are no sleeps and no Aeron.
- CloseKeepsThePubAliveUntilTheWorkerReleasesIt (the crash)
- ReleaseRunsBehindASubscribeStillInTheRing
- ReleaseRefusedByAFullRingIsRetriedNeverDropped
- EachWorkersPubIsFreedByItsOwnAcknowledgement
- AResolveNeverReplacesAPubAWorkerHolds
- AResolveAfterTheSessionClosedHandsNothingOut
- CloseDropsTheSessionsWorkerCursors
- ShutdownWithRetiringPubsReleasesEachOnce
- AnUnknownAcknowledgementFreesNothing

**RED evidence.** With `ClientGateway.cpp` reverted to the 6a67487 logic (test hooks kept), **9 of 9 failed**. The core failure was `pub.destroyed == 1` while `workerReferences(w, pub) == true`, which is the use-after-free. ASan was not run because no ASan build exists and a real pub needs an Aeron driver. The deleter probe shows the same thing deterministically.

Test seam: `forwardSubscribeToWorker_` now checks for a cached pub before checking `egressChannel` / `aeron_`. Behaviour is unchanged, because a cached pub implies both exist.

## Suite
- build-fast full ctest: **100% tests passed, 0 tests failed out of 3752** (1 disabled, pre-existing).
- `-R Regression`: 1030/1030.
- `scripts/check_no_atomics.py` and `scripts/check_single_driver_pumper.py`: OK.

## Concerns
- Not yet verified on the dockerized DR e2e (that needs Docker and an image rebuild).
- `build-fast` in wt52 had `BUILD_TESTING=OFF` at first and was reconfigured to match wt51.
