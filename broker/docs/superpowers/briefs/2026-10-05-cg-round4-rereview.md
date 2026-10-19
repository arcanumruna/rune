# Re-review: CG routing fix round 4

**Date:** 2026-10-05

**Scope:**
- broker `rune-broker-wt51`, `ac34a9b..6a67487`;
- client `rune-client-cpp-wt14`, `c9092f7..141a29d`;
- rune-protocol `8f6b38f..1ec8b4d`.

**Inputs:**
- section A of `2026-10-05-cg-round3-flush-review.md`;
- the "Fix round 4" section of `2026-10-04-cg-routing-report.md`.

## Verdict: APPROVE WITH CHANGES (0 CRITICAL / 0 MAJOR / 1 MEDIUM / 2 LOW)

Every round-3 finding is fixed, and each regression test genuinely fails without its fix (proven for MAJOR-1 and MEDIUM-3 by reverting locally).

One new MEDIUM, R4-MEDIUM-1: the MEDIUM-3 park design polls metadata at about 5 Hz and floods stderr for as long as an old node leads an assigned partition. It does not stall, but it should be made event-driven. The fix is small, and no re-review is needed after it.

## Verification performed

- **rune-protocol `1ec8b4d`.** `rune_protocol_tests`: **501/501**.
  - `connect_response_v3.bin` checked with xxd: blockLength 0x12, the same 36-byte body as v0, and `03 00 00 00` at [38..41].
  - The v0 goldens are unchanged (blockLength 0x0e).
- **Client `141a29d`.** The build was up to date. ctest **980/980** passed (2 platform skips).
- **Broker `6a67487`.** `build-fast` was up to date. 70 related tests passed, including:
  - `QueuedSubscribeKeepsItsCorrelationIdAndReplicaRegression`;
  - `CgPushReadStampRegression.*`;
  - `GatewayLoopbackFixture.Connect_RoundTrip`;
  - `NodeGatewayFixture.Connect_RoundTrip_OverLiveNode`.

  I did not re-run the full 3743-test suite; the implementer reports it green.
- **RED proofs** (local reverts, restored afterwards; both trees are clean at their HEADs):
  - **Broker.** Forcing `op.correlationId = 0` in `trySubOp_` makes `QueuedSubscribeKeepsItsCorrelationIdAndReplicaRegression` fail at `ShardNonBlockingTest.cpp:2431` ("the queued retry dropped the Subscribe's correlationId").
  - **Client.** Disabling the version check in `reconcileImpl_` makes `PartitionLedByAnOldNodeIsParkedNotSilentlyStalledRegression` fail at `GroupRoutingRegressionTest.cpp:1000`.
- **Probe for R4-MEDIUM-1.** This was a temporary test, since reverted. With p1's leader on a v0 node, 3 s of driving produced **15 MetadataRequests** and **16 "partition parked" stderr lines**.

## Per-finding verdicts

| Finding | Verdict | Evidence |
|---|---|---|
| **A-MAJOR-1:** `trySubOp_` dropped `op.correlationId` | **FIXED** | See [A-MAJOR-1](#a-major-1). |
| **A-MAJOR-2:** protocol tests at 38 bytes, no 42-byte golden | **FIXED** | See [A-MAJOR-2](#a-major-2). |
| **A-MEDIUM-1:** push-bind unchecked at seed sid 0 | **FIXED** | See [A-MEDIUM-1](#a-medium-1). |
| **A-MEDIUM-2:** `resendUnanswered` abandons every session's in-flight Subscribes | **FIXED** | See [A-MEDIUM-2](#a-medium-2). |
| **A-MEDIUM-3:** an old partition leader stalls silently | **FIXED, with R4-MEDIUM-1** | See [A-MEDIUM-3](#a-medium-3). |
| **LOW-1:** pending lookup on every group batch | **FIXED** | One `& (1<<61)` compare before `pendingByCorrelation_.find` (`SubscriberHub.cpp:433`). Plain ids come from `nextCorrId_++` and are small and positive, so they can never carry bit 61. |
| **LOW-2:** `learnLateFromRecords_` matches by correlationId only | **FIXED** | `LateSub.brokerSid` is populated in `dropFetch_` (`GroupConsumerHub.cpp:676`) and required at `:619`. |
| **LOW-3:** old-client refusal log flood | **FIXED** | The log fires on the first refusal per session and then every power of two (`GroupCoordinationService.cpp:302-306`). The map is cleared at 4096 entries. It is a cold path, and its allocation is acceptable. |
| **LOW-4:** `replica` omitted on retry | **FIXED** | Via the shared builder. The test asserts `subs[0].replica == m.r`. The replica is now looked up at retry time, which is better than holding a stale pointer. |
| **LOW-5:** no test for the PushRead stamp | **FIXED** | `CgPushReadStampRegression.PushReadRecordsCarryTheSubscribeCorrelationIdRegression` (`F15EosRegressionTest.cpp:3361`) drives the gateway PushRead end to end and has a non-empty precondition. |

<a id="a-major-1"></a>
### A-MAJOR-1: FIXED

**Fix.** `buildSubscribeOp_` (`ClientGateway.cpp:2584-2605`) is now the only builder. It is used by:
- `enqueueSubscribeOp_` (`:2609`);
- `trySubOp_` (`:2654`).

**Path exercised.** The test fills worker 0's ring and calls `forwardSubscribeForTest(…, kCorr)` (session 0, so the MDC path). It asserts that the Subscribe was queued, then pumps the retry queue, so it goes through `trySubOp_`.

**Other callers.** `addGroupSubscription` (`:4962`) still passes the default 0. That is dead in production, as the report states.

**Hot path.** Nothing new: the builder returns by value and is inlined into a gateway control-path call.

<a id="a-major-2"></a>
### A-MAJOR-2: FIXED

**Tests updated.**
- The 3 codec tests now expect 42 bytes, and RoundTrip checks the version at offset 38.
- A new decode test covers the 38-byte frame (`OlderBrokerFrameDecodesAsVersionZeroRegression`).
- The v0 goldens assert `BODY_FIXED_V0`, check `brokerVersion == 0`, and re-encode through the V0 truncation.

**New golden.** `connect_response_v3.bin` is checked in both repos:
- rune-protocol `ConformanceGolden.ConnectResponseV3`;
- client `ClientCodecConformance.ConnectResponseV3Regression`.

The protocol headers are unchanged in this range.

<a id="a-medium-1"></a>
### A-MEDIUM-1: FIXED

**Fix.** `SubscriberHub.cpp:437` now reads `if (currentSessionId == 0 || rv.sessionId != currentSessionId) return;`.

**No regression for the window.** In the fresh-connect window, the client re-sends pending Subscribes only after it processes the ConnectResponse (`Client.cpp:~1905`). The pushes for the new session therefore arrive once the session id is known, and nothing that should bind is lost.

**Test.** It uses session 0 with a foreign push carrying the same correlationId, then its own push. It would fail on round 3, where 0 meant "unchecked".

<a id="a-medium-2"></a>
### A-MEDIUM-2: FIXED

**Fix.** `resendUnanswered(session)` filters on `fs.session == session` (`GroupConsumerHub.cpp:795`). It is called from:
- the leader connect (`Client.cpp:1866`), with `lastConnectSession()`;
- the watcher, per affected session (`Client.cpp:3320`).

The watcher's final call is now `reconcileFetch()`.

**The seed is still covered.** The seed session (index 0) is not passed to it. That is fine, because `connectEpoch` bumps on every accepted connect and `reconcileImpl_` drops instances from a stale epoch.

**Test.** It is behaviorally RED on round 3: session 1's Subscribe would have been re-sent, giving 2 instead of 1.

<a id="a-medium-3"></a>
### A-MEDIUM-3: FIXED, with R4-MEDIUM-1

**Fix.**
- `Session::brokerVersion` is set on the accepted connect (`ClientAgent.cpp:739-742`).
- `FetchRouter::brokerVersion` exposes it.
- `reconcileImpl_` parks a partition whose leader node runs a version below 3 (`GroupConsumerHub.cpp:737-752`).

**Test.** It is RED without the check (proven).

**Stall analysis.** The park resolves when the node is upgraded. Two paths lead there:
- **Watcher path.**
  1. The upgrade restarts the node, so the client loses that leader session.
  2. `fetchUses(i)` is true through the route (`:833`). It fires the watcher event, which calls `requestMetadataNow_`.
  3. The MetadataResponse runs `unparkFetch` and `reconcileFetch`.
  4. While the session is down, the partition takes the `ep == 0` branch (revive, continue), which does **not** re-park it.
  5. The v3 reconnect, through `Client.cpp:1866`, runs `resendUnanswered(i)` and then `reconcileFetch`, which subscribes.
- **Reconnect-first order.** If the reconnect lands while the partition is still parked, the next MetadataResponse unparks and subscribes it.

A leader move to a v3 node resolves through metadata as well.

**No stall.** The cost of the design is R4-MEDIUM-1.

## New findings

### R4-MEDIUM-1. A version-parked partition causes a perpetual ~5 Hz metadata poll and an stderr log flood

**Where:**
- `GroupConsumerHub.cpp:741-751` sets `fs.parked = true` and `metadataWanted_ = true`, and `fprintf`s on every park.
- `Client.cpp:1530`: `takeMetadataWanted()` calls `scheduleMetadataRefresh()`.
- `Client.cpp:3798-3812`: every MetadataResponse resets `metadataBackoffNs_` to 100 ms, then calls `unparkFetch()` and `reconcileFetch()`.

**Scenario.**
1. During a rolling upgrade, or with a mixed cluster left as is, node 2 (v2) leads t/p1, which is assigned to member M.
2. Each MetadataResponse resets the backoff to 100 ms, unparks p1 and reconciles it.
3. The version is still below 3, so p1 is re-parked, `metadataWanted_` is set, and a new line goes to stderr.
4. The next duty pass arms a refresh at now + 100 ms. The backoff never grows, because every answer resets it.

**Measured.** Over 3 s there were 15 MetadataRequests and 16 log lines for one parked partition.

**Impact.**
- The rate multiplies by clients × parked partitions for the whole upgrade window, which could last hours. The load lands on the seed broker, and the logs are unbounded.
- It is also a time-paced retry loop. Convergence does not need it: the event-driven paths above (watcher event, then reconnect, then `resendUnanswered(i)` or metadata) already resolve it.

**Fix.**
- Do **not** park, and do not set `metadataWanted_`, for the version case. Simply `continue`; the check is stateless and cheap. Every relevant event re-runs `reconcileImpl_`: the session's accepted connect, metadata, and watcher events.
- Log once per (tp, target, connectEpoch): keep the last-logged epoch in `FetchSub`.
- Add a `*Regression*` assertion that `gw.metadataRequests` stays flat over N duty cycles while a partition is version-parked. Then assert that an accepted v3 reconnect of that node (`advertisedBrokerVersionAt[n1] = 3` plus `expireSession`) subscribes p1 without a metadata answer.

### R4-LOW-1. Rolling upgrade asymmetry: the seed fails the consumer, a leader parks

**Where:** `GroupConsumerHub.cpp:947`, `:1534`.

**What happens.** If the seed session fails over to a not-yet-upgraded bootstrap node mid-upgrade, `onConnected` makes the GroupConsumer fail **terminally** (`BrokerTooOld`). An old *leader* node, by contrast, only parks its partitions.

This is consistent with the `GroupFailReason` docs ("seed / coordinator must be ≥ 3"), but a rolling upgrade that restarts the seed node first can kill v3 consumers.

**Suggestion.** Document it in the upgrade procedure: upgrade or restart the bootstrap nodes so that the seed always lands on a v3 node, or wait for the cluster to be fully v3 before starting v3 consumers. Alternatively, treat a seed below 3 *after* a prior v3 connect as transient. This is backlog material.

### R4-LOW-2. The bit-61 check is inconsistent with `isFetchCorrId`

**Where:** `SubscriberHub.cpp:433` uses `(c & (1<<61)) != 0`, while `GroupConsumerHub::isFetchCorrId` requires `c > 0 && (c & (3<<61)) == tag`.

**Effect.** Harmless today: plain Subscribe ids are small and positive. Using `GroupConsumerHub::isFetchCorrId` (constexpr) keeps the two definitions single-sourced.

## Rules check

- **Forbidden constructs.**
  - No new atomics or mutexes.
  - No virtual calls on hot paths. The function-pointer `brokerVersion` call is on the cold reconcile path.
  - The only hot-path change (SubscriberHub `onRecords`) removes a map lookup.
- **Allocation.** The only new allocation is the broker's `oldClientRefusals_`, on the cold JoinGroup refusal path.
- **No time-based correctness decisions.** R4-MEDIUM-1 is a time-paced *liveness* retry, not a correctness decision, and it is redundant with the event-driven paths.
