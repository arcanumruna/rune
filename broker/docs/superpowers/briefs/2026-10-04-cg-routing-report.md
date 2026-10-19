# Consumer-group routing fix: report (2026-10-04)

Client worktree: `~/Projects/rune-client-cpp-wt14`, branch `fix/group-subscribe-routing`, rebased on client main `3c7e97b`. Nothing was pushed. No broker change was needed (see Defect 2).

## Defect 1: a member's partition Subscribe never reaches the partition leader

**Root cause (as diagnosed).** `GroupConsumerHub::sendSubscribeForPartitions` sent every assigned partition's Subscribe on the seed (coordinator) session with correlationId 0. A broker refuses a Subscribe for a partition it does not lead (Redirect or NotLeader) and registers nothing. The hub never handled that refusal.

There is a worse side effect. Because a group was active, `Client::onEgressFrame` treated the Redirect as a seed-level data-op redirect: it retargeted the seed transport and reconnected the seed (the coordinator's session) on the partition leader.

**Correction to the brief.** Normal (non-group) subscribes do not route per partition today. `SubscriberHub` sends through `offerDefault`, which is the seed's default publication. Their redirect convergence is that same seed-level retarget (stick-to-leader). Only Produce uses the per-partition route table and leader sessions.

### Design choice: no reuse of SubscriberHub; reuse the agent's routing primitives

I did not reuse `SubscriberHub`, for three reasons:

1. It has no per-partition routing. Its redirect path moves the seed, which is exactly what breaks the group: the coordinator session must stay where it is.
2. Its unit is a user `SubscriptionImpl` with its own status ring, handler, F8c C7 ownership and per-record dispatch by subscriptionId. The group consumer dispatches by (topic, partition) into its own position map (`onRecords`, `DispatchScope`). Wrapping each assigned partition in a SubscriptionImpl would duplicate the ownership and lifecycle machinery for every rebalance.
3. The group's subscription lifecycle is bound to the assignment (generation re-seed via OffsetFetch, revoke leads to Unsubscribe, rejoin leads to re-subscribe from the committed offset). SubscriberHub has no such notion.

Instead I reused the routing primitives that Produce already uses:

- the per-partition route table (`setRoute`, built from MetadataResponse);
- lazily opened per-node sessions (`sessionForEndpoint`);
- the provisional metadata-cache update on Redirect;
- `requestMetadataNow_`.

These are exposed to the hub through a small `FetchRouter` made of plain function pointers plus a context pointer: `leaderSession`, `sessionId` and `offer`.

### Mechanics

- **Per-partition state.** Each assigned partition has a `FetchSub` record in `GroupConsumerImpl::fetch`: session index, session id, correlationId, subscriptionId, offsetKnown, parked, redirects.
- **`reconcileFetch()`.** For every Stable member and every assigned partition, it compares where the subscription lives with the partition's current leader session and that session's id.
  - If they match, nothing happens.
  - Otherwise it sends an Unsubscribe on the old node (best effort) and a Subscribe on the leader's session, stamped with that session's id.
- **Seed vs. other nodes.**
  - On the seed (the coordinator leads the partition), the Subscribe is the isolation carrier of the coordinator's own group subscription, as before. The broker owns the lifecycle of that group subscription, so the client never unsubscribes it.
  - On any other node, the Subscribe is a plain subscription starting at the member's position. It waits for the OffsetFetch answer; with no committed offset it starts at 0, as the coordinator does.
- **Triggers** (all cold paths):
  - SyncGroup OK: every subscription restarts, because the new generation re-seeds positions from the committed offsets.
  - Assign: revoked partitions are unsubscribed, added ones subscribed, and seed carriers re-sent.
  - OffsetFetchResponse.
  - Any ConnectResponse OK, whether seed or leader session.
  - MetadataResponse.
  - Group-Subscribe Redirect or NotLeader.
  - The 100 ms sweep, as a backstop.
- **Refusals.** Group fetch correlationIds carry a tag in bit 61 (producer batch ids use bit 62; connect ids are `session * 1e12`).
  - A Redirect or NotLeader with a tagged id goes to `onGroupFetchRefused_`, never to the seed-level redirect path.
  - Redirect: the cache learns the leader provisionally, `setRoute` updates only that partition, and the partition is re-subscribed at once. After 8 consecutive redirects the partition is parked until fresh metadata or the next sweep.
  - Bare NotLeader: the partition is parked, and metadata is requested now.
  - A SubscribeResponse with a tagged id is consumed by the group hub, never by SubscriberHub.
- **Revoke and close.** A revoked partition's leader-node subscription is unsubscribed on its own node. A Subscribe that was abandoned before its answer is unsubscribed when the answer arrives. `GroupClose` unsubscribes all of the member's leader-node subscriptions.
- **Records.** Pushes from the leader node arrive on the shared egress and are matched by (topic, partition) in the unchanged `onRecords`, including position tracking. OffsetCommit, OffsetFetch, Join, Sync, Heartbeat and Leave stay on the seed (coordinator) session.
- **Leader-session ConnectResponse.** It no longer runs the group's `onConnected`. Previously a JoinGroup stamped with the leader session's id was sent; the ring dropped it as stale, and the member then sat in Joining until the deadline.
- **Agent API.** `leaderSessionIndex(t,p)`, `connectedSessionId(i)`, `offerOnSession(i, ...)` and `isSeedConnectCorrId()`. Sessions are append-only, so a session index is stable.
- **Constraints.** No new atomics or mutexes. The Records and Produce hot paths are unchanged; no per-record allocation (FetchSub map nodes are allocated only when an assignment changes). Both check scripts pass.

## Defect 2: one member gets every partition, the other never rejoins (test_cg_rebalance_stable)

**Root cause (client side; same origin as Defect 1).**

1. At generation 1, member A's Subscribes for partitions the coordinator does not lead were answered with Redirect.
2. On main, that Redirect moved A's seed transport (the coordinator session) to the partition leader and reconnected it there.
3. A still received the eager revoke `Assign{gen2, {}}` on its old session's egress (REVOKE gen=2). After that, every Heartbeat and JoinGroup it sent went to a node that is not the coordinator, and every answer was NotCoordinator. A looped in Rejoining forever.
4. The coordinator waited for A, then evicted it (rebalance timeout or session expiry) and completed generation 3 with B alone. B got all 4 partitions.

This matches the observed member lines exactly.

**Broker check (no change needed).** `GroupCoordinator` waits for every member present at the start of the rebalance (`expectedMemberCount_`), up to the rebalance timeout (the member's `sessionTimeoutMs`). It assigns over all current members with RangeAssignor. Join, Sync and Heartbeat are fenced by `isLocalCoordinator()`, where coordinator = cluster leader (v1). The DR client connects to the leader, so once the seed is no longer moved, group frames stay on the coordinator.

The broker worktree `rune-broker-wt49` was created and then removed unused. I also built `rune-broker-node:a2cb0eb` with the documented compose command; wt26 HEAD `a2cb0eb` differs from `4e669cf` only in `dr-e2e/harness/driver.py`.

**Fix.** Defect 1's fix: a group-Subscribe refusal never retargets the seed. A dedicated regression test reproduces the stuck-member mechanism.

## Commits (branch `fix/group-subscribe-routing`)

| SHA | Commit |
|---|---|
| `bfe31ef` | test(support): InvokerGateway models a group coordinator + per-partition Subscribe leadership |
| `bff9024` | feat(agent): leader-session index / connected id / control offer for group fetch routing |
| `48a5329` | fix(group): route each member's partition Subscribe to the partition leader (+ 4 regression tests) |
| `679f4bc` | test(group): a member re-joins its coordinator after a redirected partition Subscribe (Defect 2; gateway coordinator fence + rebalance) |
| `6d2e013` | test(group): hub-level fetch routing — start offset, revoke / abandoned / new-generation Unsubscribe |

## Tests and RED evidence

Every test is condition-driven with bounded pump loops and no sleeps. All are named `*Regression*`.

**Client-level tests**, in `tests/client/GroupRoutingRegressionTest.cpp` (rune_invoker_tests). The rig is an Invoker client plus an InvokerGateway that models several broker nodes. RED was obtained by restoring main's `src/` and `include/` with the new tests in place: 5 of 5 FAILED (`/tmp/wt14-red2.log`).

| Test | What it checks | RED on main |
|---|---|---|
| `MemberConsumesPartitionsLedByTwoNodesRegression` | p0 led by node 0 (seed, coordinator), p1 by node 1; the member is assigned both and must consume both. p1's Subscribe must carry node 1's session id, no refusals. | "partition 1's Subscribe never reached its leader (node 1)" |
| `RedirectedGroupSubscribeReroutesToTheLeaderRegression` | With stale metadata, the seed redirects p1. The member re-routes only p1 to node 1, consumes it, and the seed transport stays on node 0. | "never re-subscribed on its leader (refusals=1)" |
| `NotLeaderGroupSubscribeConvergesOnMetadataRegression` | A bare NotLeader parks p1; fresh metadata then lets it subscribe on node 1. | "the NotLeader-parked partition never subscribed on its leader" |
| `LeaderChangeResubscribesOnTheNewLeaderRegression` | The node-1 subscription starts at the committed offset (10). When p1 moves to node 2, the member re-subscribes there from its position (15) on node 2's session, and the node-1 subscription is unsubscribed on node 1. | Precondition failure: never subscribed on node 1 |
| `MemberRejoinsTheCoordinatorAfterARedirectedSubscribeRegression` (Defect 2) | After the redirect, the coordinator starts generation 2. The member must re-join on node 0 and be assigned gen 2. All group frames must stay on node 0, with zero NotCoordinator answers. | "never re-joined the coordinator for generation 2 (NotCoordinator answers: 84)" |

**Hub-level tests**, in `tests/group/GroupConsumerTest.cpp`, using a fake FetchRouter:

- `LeaderNodeSubscribeWaitsForTheCommittedOffsetRegression`
- `RevokedLeaderNodePartitionIsUnsubscribedOnItsNodeRegression`
- `AbandonedLeaderNodeSubscribeIsUnsubscribedWhenAnsweredRegression`
- `NewGenerationRemakesLeaderNodeSubscriptionFromCommittedRegression`

Their RED on main is compile-level only: main has no fetch router and never sends an Unsubscribe on revoke.

**Full suite:** `ctest --test-dir build -j6 --timeout 300` gives 100% passed, 949 of 949 (940 before + 9 new; 3 platform tests skipped, as before).

## Docker DR verification (partial)

Client image `rune-dr-client:48a5329`, broker image `rune-broker-node:a2cb0eb` (same broker binary as `4e669cf`).

- `test_cg_rebalance_stable[2]` with the fixed client: **1 passed** (44 s).
- The same test with the main client `rune-dr-client:3c7e97b`: **FAILED**, group-no-committed-loss with 60 records never consumed.
- A full run of `test_cg_rebalance.py`, `test_cg_atleastonce.py` and `test_cg_failover.py` was **stopped at the coordinator's request**. Results so far were `....F..FF` (9 tests), but my runs and the coordinator's DR-10 runs shared the "rune-dr-fault" compose project and tore down each other's clusters, so these results mean nothing. The full CG DR verification needs a clean Docker window. All my processes and containers are stopped.

## Concerns / follow-ups

1. **NotCoordinator never re-discovers the coordinator (pre-existing).** The hub rejoins on the same seed session forever. With this fix the seed is no longer moved by group fetch, but the loop would recur if the coordinator (the cluster leader) moved while the old node stays reachable.
   - A related latent issue: `SessionEndpointIo::retarget` has been a no-op for every session, the seed included, since Task 1.3 (`8c50f57`). So the seed does not follow a connect-time Redirect. DR tests connect to the leader directly, which hides this.
   - Fixing coordinator re-discovery needs a seed re-home path (retarget + reconnect on NotCoordinator, via the connect redirect).
2. **Seed-level redirect stick-to-leader (pre-existing).** Plain `SubscriberHub` subscriptions still use it: they go to the seed and move the whole seed on Redirect. In a client that also runs a group consumer, a plain subscription's Redirect would still move the coordinator session. Plain subscriptions should get the same per-partition routing.
3. **Leader-session ConnectResponses still run seed-only logic (pre-existing; now only the group `onConnected` is gated).** That logic includes `completeConnect_`, `cbConnected_`, pending-subscribe resend and a MetadataRequest stamped with the leader session's id, which the ring drops as stale (4 such drops per test run).
   - Separately, an `ErrorResponse{SessionExpired}` from a leader node is credited to the seed machine (`sessionForConnectCorrId` falls back to the seed).
   - Consumer clients now open leader sessions, which makes both more reachable.
4. **Possible duplicate delivery.** If the coordinator's group subscription for a partition it does not lead ever pushes (from a follower log) while the leader node's subscription also pushes, records are delivered twice. At-least-once still holds, and DR shows the coordinator one does not push today. When a partition moves to the coordinator node, its group subscription resumes from its own cursor (bounded re-delivery).
5. **Slow rebalance start (efficiency, not correctness).** A member learns of a rebalance only from its next Heartbeat (sessionTimeout/3, 10 s in DR). The client does not rejoin on the eager empty `Assign{newer gen}`.
6. **Best-effort Unsubscribe.** An Unsubscribe that the transport refuses is not retried. The orphan subscription only pushes records `onRecords` ignores, and dies with that node session.
7. **Pre-existing gap:** a GroupConsumer created after `connect()` stays Unjoined until the next seed connect.

---

# Fix round 1 (review `2026-10-04-cg-routing-review.md`: REJECT, 1 CRITICAL, 3 MAJOR, 7 MEDIUM)

Branch `fix/group-subscribe-routing` (client worktree `rune-client-cpp-wt14`). Not pushed. No Docker used this round.

## Commits

| SHA | Commit |
|---|---|
| `3ad6fb8` | test(support): InvokerGateway — replies from the arrival node, distinct sources, colliding ids, dead nodes, held/errored OffsetFetch |
| `187f129` | feat(transport,agent): frame arrival source, per-session connect epoch, session revive |
| `14caf1c` | fix(group): fix round 1 — current-instance records, dead-leader rerouting, retried Unsubscribe, per-session record demux (+ tests) |

## Fixes

### C1: silent loss

The ruling's rules, as implemented:

- **Position seeding.** A partition's position is seeded only by OffsetFetch, or by the reset policy when there is no committed offset (position 0, as the coordinator does). A Records batch never seeds it. No Subscribe, on any session, is sent before the start offset is known.
- **Accepted source.** A routed partition accepts a batch only from its current subscription instance. `FetchSub` tracks session index, connect epoch, subscriptionId and selfOwned. A batch must match all of:
  - its arrival session (see M3) equals the subscription's session;
  - that session's connect epoch is the subscription's epoch;
  - its subscriptionId equals the bound id. Before the SubscribeResponse binds, any id of that session episode is accepted unless it is in the retired list. Under per-worker egress, Records can precede the response, and it is the same node log, so the contiguity rule decides. Every dropped or replaced instance is added to `retired_`.
- **Contiguity.**
  - A batch entirely below the position is skipped (MEDIUM-3).
  - A batch that straddles the position is delivered whole (at-least-once).
  - A batch above the position (a gap) never advances it. A gap from the bound current subscription re-makes the subscription from the position.
  - On the seed, the coordinator's group subscription cannot be rewound by a carrier. It is unsubscribed and replaced by a plain subscription of ours (`selfOwned`), which we then own and unsubscribe.
  - A second gap at the same position parks the partition, logs an error and asks for metadata. Records are never skipped silently; this is the retention-below-committed case.
  - The one legal gap: no committed offset, and the first batch of our own fresh subscription (the broker resolved the log start).
- **Rejoining.** On Rejoining, the generation's subscriptions are dead at once: retired, and ours are unsubscribed.

### M1: stall forever, now event-driven

- `Client::watchGroupSessions_` runs each duty pass while a group exists. It compares Connected and Disconnected masks of the leader-node sessions.
- When a session the group depends on is **lost** (leaves Connected) or becomes **terminally Disconnected**:
  - the client asks for metadata now (rate-limited, with a backoff refresh as fallback);
  - a Disconnected session is re-driven (`ClientAgent::reviveSession`), and its own connect budget paces the retries: each failure is the next event;
  - routing is reconciled; the metadata answer re-routes moved partitions.
- Detection itself is the SessionMachine's own staleness rule. No new timeout constant decides correctness.
- `ClientConfig::responseTimeoutNs` is a new passthrough, used by the tests.

### M2: refused Unsubscribes

- A refused fetch Unsubscribe goes into `pendingUnsubs_` and is re-offered every duty pass (`retryFetchUnsubscribes`) until it is sent.
- It becomes moot when its session's connect epoch ends.
- We never Unsubscribe an id in a new episode: the broker's `handleUnsubscribe` does not check the session, and a restarted node re-uses subscription ids.

### M3: per-node record demux

Arrival-node identity **is** available on the shared egress subscription. No protocol change was needed.

- **Source key.** The transport records the Aeron `Header::context()` (the Image) of the frame being dispatched. `currentSourceKey()` maps `Image::sourceIdentity()` (the sending node's socket "ip:port") to a dense id. The string is copied once per image and cached by image correlationId, so there is no per-frame allocation.
- **Why it identifies the node.** Every publication a node's driver sends to this client's egress channel shares one send endpoint, so they share a source. That covers the session publication (ConnectResponse, gateway pushes) and the per-(session, worker) unicast Records publications.
- **Learning the source.** Each agent session records the source key of its ConnectResponse.
- **`recordsSource(frame.sessionId)`.** Among the sessions on the frame's source, it picks the Connected one whose broker id equals the frame's sessionId field. If no session is known on that source, it falls back to the broker id alone, but only if exactly one Connected session carries it. Otherwise the result is unknown.
- **SubscriberHub.**
  - Plain subscriptions live on the seed, so Records from another session are ignored.
  - It also checks the topic and the partition the SubscribeResponse resolved.
  - An unknown source falls back to the old behaviour, plus the topic/partition check.
- **Group hub.** A routed partition accepts an unknown source only if its subscription is on the seed.

### MEDIUM items

- **MEDIUM-1:** liveness uses the per-session connect epoch, not the broker id.
- **MEDIUM-2:** OffsetFetch is re-sent from the sweep while any assigned partition's start offset is unknown (refused send, errored entry, capped answer). Only the unknown entries are re-sent, with 100 ms doubling to 2 s pacing.
- **MEDIUM-3:** a batch entirely below the position is skipped.
- **MEDIUM-4:** while a group is active, a data-op Redirect never retargets the seed.
  - A produce redirect takes the per-partition route even before any metadata route exists.
  - A plain Subscribe redirect fails that one subscription (`failPendingSubscribe(corrId)`; the app re-subscribes).
  - Anything else is retried through its partition route.
- **MEDIUM-6:** tests use kernel-picked free ports. Stream ids are unchanged.
- **MEDIUM-7:** `joinIfConnected` runs after a GroupAdd while the seed is Connected.
- **MEDIUM-5:** backlog (unchanged).

### LOW items

- Fixed:
  - **LOW-1:** `isFetchCorrId` requires `c > 0`.
  - **LOW-2:** the sweep no longer unparks. Parking sets `metadataWanted_`, which the duty cycle turns into a backoff-gated metadata refresh; parked partitions retry only on fresh metadata.
  - **LOW-6:** comment on `leaderSessionIndex`'s `emplace_back`.
- Listed, not fixed:
  - **LOW-3:** an orphan plain subscription on the seed when `addGroupSubscription` failed.
  - **LOW-4:** `autoOffsetReset` is still unused. No committed offset means the log start, consistent with the coordinator.
  - **LOW-5:** rejoin on an eager `Assign{newer gen}`.

## Tests

All tests are `*Regression*`, condition-driven, with bounded pumps and no sleeps, on free ports. Totals: 13 client-level group-routing tests, 9 hub-level fetch tests, plus LOW-1.

**RED evidence.** I made a scratch worktree at the pre-round head `6d2e013`. It contained only test infrastructure: the new InvokerGateway, the test file, and the `ClientConfig::responseTimeoutNs` passthrough (2 lines). Results (`/tmp/r1-red-client.log`):

- The 5 tests from the first round passed.
- All **8 new client-level tests FAILED** with the defect's symptom:

| Test | Symptom on the pre-round code |
|---|---|
| `StaleRecordsAfterRejoinNeverMoveThePositionRegression` (C1) | The stray retired-subscription batch was delivered. The new node-1 subscription had `fromOffset == 11`, not 5. |
| `BatchBelowThePositionIsNotRedeliveredRegression` (MEDIUM-3) | 9 records delivered instead of 6. |
| `DeadLeaderNodeSessionReroutesItsPartitionsRegression` (M1-A) | Never re-routed to node 2. |
| `NeverConnectedLeaderSessionIsRevivedRegression` (M1-B) | The node-1 session gave up for good. |
| `ErroredOffsetFetchIsRetriedRegression` (MEDIUM-2) | The partition was never subscribed. |
| `CollidingIdsAcrossNodesNeverCrossDeliverRegression` (M3) | The plain handler got 5 records instead of 3. |
| `ProduceRedirectNeverMovesTheCoordinatorSessionRegression` (MEDIUM-4) | The seed was retargeted to node 1. |
| `GroupConsumerCreatedAfterConnectJoinsRegression` (MEDIUM-7) | Never joined. |

**Hub-level tests (compile-RED only on the pre-round code; the APIs are new):**

- `OnlyTheCurrentSubscriptionInstanceIsAcceptedRegression`
- `GapResubscribesFromThePositionThenParksRegression`
- `RefusedUnsubscribeIsRetriedUntilSentOrMootRegression`
- `SameSessionIdNewEpisodeResubscribesRegression`
- `NegativeIdsAreNeverFetchIdsRegression`

**Existing hub tests that relied on "the first batch seeds the position":** five of them, which C1 forbids. They now seed via OffsetFetch first, and the dispatch-race test pushes contiguous offsets.

**Full suite:** 962/962 passed (3 platform skips). Both check scripts are OK. One build hit a transient `gtest_discover_tests` 5 s discovery timeout on `rune_invoker_tests` under load; a rebuild passed (pre-existing harness behaviour).

## Concerns

1. **Per-worker egress source identity is unproven in Docker.** The demux assumes Records from a node's per-(session, worker) unicast publications share the node's send endpoint, and therefore its `sourceIdentity`, with the session publication. That is standard Aeron driver behaviour: publications to the same channel share one SendChannelEndpoint.
   - If a deployment breaks it (for example, a worker publication from a different driver or interface), those frames fall back to broker-id-only matching. With colliding ids across nodes the result is "unknown", and leader-node partitions would stall rather than misdeliver.
   - The DR run will show it.
2. **Gap recovery on the seed replaces the coordinator's group subscription with a plain one.** This happens about once per generation, because records pushed between SyncGroup and the OffsetFetch answer are dropped.
   - Correct, at the cost of one Unsubscribe+Subscribe per seed partition per generation.
   - Better fixed on the broker: create group subscriptions at the member's OffsetFetch, not at SyncGroup, or rewind on carrier. Broker ticket.
3. **Retention below the committed offset** parks the partition with an error. Kafka's OffsetOutOfRange plus reset policy needs a protocol answer; backlog.
4. **Broker tickets (from the review, still open):**
   - `handleUnsubscribe` does not check the session; it should. Then the client could also clean up across episodes.
   - Group subscriptions are created or pushed for non-led partitions (MEDIUM-3 broker side).
   - "Fix #4" skips recreate on an equal count.
   - `SessionExpired` has correlationId 0 (MEDIUM-5).
5. **MEDIUM-4 semantics change.** With a group active, a redirected plain Subscribe now fails that subscription (finished) instead of moving the seed. The app must re-subscribe.
6. **CG DR verification** (`test_cg_rebalance`, `test_cg_atleastonce`, `test_cg_failover`) is pending the coordinator's clean Docker window. I'm ready for it.

## Controller note — local Docker run of fix round 1 (client 14caf1c, broker df7fcef/161de4c), 2026-10-04

cg_rebalance/atleastonce/failover: 2 passed, 7 failed (was 0/10).  Assignment is now correct (2/2 per member) and
member 1 consumes both its partitions.  Stable[2]: 20 records missing = exactly ONE partition, and the missing
partition is the one led by rune-2 = consensus leader = group coordinator = bootstrap node (cg p3, also p0 on that
node at times).  No client-side error lines.  A second probe run consumed all 80 → intermittent.
Hypothesis (matches the implementer's concern #2): the coordinator AUTO-creates group subscriptions at SyncGroup for
partitions it leads; the client drops those records until its OffsetFetch answers, then Unsubscribe+Subscribe — a
race there leaves the partition dead.  Clean fix: broker coordinator stops auto-subscribing at SyncGroup (clients
subscribe to leaders explicitly now); or client handles that path idempotently.  Next round: reproduce in-process
first (fake gateway with coordinator auto-push), then fix.

---

# Fix round 2 (redesign)

This round follows the re-review `2026-10-04-cg-routing-rereview.md` (REJECT) and the controller's redesign ruling. Nothing was pushed. I ran no Docker. No conan.

## Commits

**Broker** (`rune-broker-wt51`, branch `fix/cg-unique-ids`, based on `d6f4a7c`; main has since moved to `ea807f2`, not rebased):

| SHA | Commit |
|---|---|
| `b7db334` | feat(gateway): cluster-unique session and subscription ids; Unsubscribe only by the owning session |
| `f820c9f` | fix(coordination): the group coordinator no longer auto-creates member subscriptions at SyncGroup (+ tests) |

**Client** (`rune-client-cpp-wt14`, branch `fix/group-subscribe-routing`):

| SHA | Commit |
|---|---|
| `5c3cd7b` | revert(transport): drop the frame arrival-source key |
| `2701b7b` | test(support): InvokerGateway — node-prefixed subscription ids, pushes from any endpoint (MDC fallback) |
| `a8e6812` | fix(group): fix round 2 redesign — explicit leader subscriptions, id demux, legitimate gaps (+ tests) |

## Broker changes

### 1. Cluster-unique ids

- `SessionTable::nextSessionId()` and the gateway's subscription ids (both allocation sites: `handleSubscribe` and `addGroupSubscription`) now return `((nodeId + 1) << 40) | counter`, which is always positive.
- The production `ClientGateway` constructor sets the prefix from `thisNodeId` (`setClusterIdNode`). The test constructors keep the historical small ids.

**Audit of id widths:**
- The only 32-bit narrowing of either id is the FR marker in `Worker.h:2631`, which is harmless.
- The SBE protocol fields are int64.
- The `int32` session ids in the replication transport and FragmentAssembler are Aeron session ids, a different id space.
- No session/subscription id is used as an index or in arithmetic. The client's A10 keepalive routes Pongs by negative correlationId and is independent of id size. Its stale-frame check compares 64-bit ids.

### 2. No auto-subscription at SyncGroup

- `handleSyncGroup` no longer calls `revokeGroupSubscriptions`/`addGroupSubscription`, which also removes the "Fix #4" skip-recreate logic.
- `onEnterPreparingRebalance` still sends the eager empty `Assign{gen}`. Its `revokeGroupSubscriptions` is now a no-op.
- `addGroupSubscription` stays as an API, used by `ReadCommittedPushTest`.
- The CG4 delivery tests now follow the explicit flow: SyncGroup, then a Subscribe per assigned partition from the committed offset, then Unsubscribe on revoke.

### 3. Unsubscribe session check

`handleUnsubscribe` ignores an Unsubscribe from a session that does not own the subscription (the re-review's broker ticket).

## Client changes

- **Arrival-source machinery removed.** Transport source keys, per-image tracking, `recordsSource` and the per-session source binding are gone. `SubscriberHub` and the group hub demultiplex Records by subscriptionId. SubscriberHub also checks topic/partition.
- **C1.** A partition accepts only its current subscription instance of the current generation:
  - once bound, its exact (cluster-unique) id;
  - before the SubscribeResponse binds it, the Records' `sessionId` must be the session the Subscribe went to, and the id must not be retired.
  - Never two unanswered instances of one partition on one session (the reconcile waits for the old one's answer).
  - Positions are seeded only by OffsetFetch, or by the reset policy (log start) when nothing is committed.
  - Rejoining retires and unsubscribes every subscription at once.
  - Every subscription is the member's own (explicit, after OffsetFetch), so revoke, rejoin and close always unsubscribe it.
- **CRITICAL-1 (legitimate gaps).** The gap rule is gone. A batch entirely below the position is skipped as a duplicate. Otherwise the batch is delivered and the position moves to `base + recordCount`, whether or not offsets in between were skipped (transaction markers, aborted batches, compaction holes).
- **MAJOR-1 (MDC fallback).** Resolved by subscriptionId demux: Records from any publication or socket are matched by id.
- **MAJOR-2 (stale ConnectResponse).** The agent records whether the dispatched ConnectResponse was awaited (Connecting with that correlationId) and accepted (now Connected).
  - Only an accepted one bumps the connect epoch and triggers the produce resend.
  - The Client ignores a ConnectResponse nobody awaited.
  - A leader session's connect only reconciles group fetch; it never runs the seed logic (connect completion, metadata request, JoinGroup, `onConnected`).
- **MEDIUM-1.**
  - (a) A metadata answer that still names an unreachable leader for an assigned partition keeps a backoff-paced refresh armed (`fetchWaitsForSession`).
  - (b) `reconcileImpl_` revives a terminally Disconnected target session (`FetchRouter::revive`), however it became Disconnected.
- **MEDIUM-2.** Covered: pre-bind batches from the right session are accepted, so a gap at the log start is no longer special.
- **MEDIUM-3 (documented, not routed).**
  - A plain Subscribe redirected while a group is active is finished with `SubscriptionFailReason::LeaderElsewhereWhileGroupActive` (new `Subscription::failReason()`). This is documented on `Client::subscribe`, and the comment now correctly says transactional Produce redirects complete at their own deadline.
  - **Why plain subscriptions are not routed per partition:** plain subscriptions have no position or leader-change model. On today's seed they don't follow a leader change either, and key-based ones (`partitionKnown=0`) can't be routed before the broker resolves the partition. Routing them is a separate feature (ticket).
- **MEDIUM-4.** Moot: there are no coordinator-owned subscriptions to replace.
- **MEDIUM-5.** `retired_` entries for a partition are forgotten when its current instance binds (exact match from then on), or when their session episode ends. It stays bounded (test: 50 moves leave at most 1).
- **LOWs fixed:** LOW-2 (double-buffered Unsubscribe retry), LOW-3 (Fatal Unsubscribe logged), LOW-5 (a batch one group consumer does not own is offered to the next).
- **LOWs not fixed:** LOW-1, LOW-6 (moot: the source machinery is gone), LOW-4 (more than 64 sessions in the watcher).

## Tests

All `*Regression*`, condition-driven, bounded pumps, no sleeps, free ports.

**Broker.** In `GroupPushDeliveryTest`, with the branch-base gateway/coordinator sources plus only the `applyUnsubscribeForTest` seam, 4 tests FAIL:
- `SyncGroupCreatesNoSubscriptionRegression` (records pushed before the member subscribed);
- `SessionAndSubscriptionIdsCarryTheNodeIdRegression` (session/subscription id 1 lacks the node prefix);
- `UnsubscribeFromAnotherSessionIsIgnoredRegression` (another session removed the subscription);
- the updated `EachPartitionToExactlyOneMember` (two auto-created subscriptions).

With the fix: 9/9. Full broker suite: **3690/3690**. `check_no_atomics` and `check_single_driver_pumper` OK.

**Client.** RED tree = round-1 head `14caf1c` plus only the test infrastructure: InvokerGateway, test file, and a `testSessionStateAt` hook. The new tests that FAIL there:

| Test | Symptom on the round-1 code |
|---|---|
| `TransactionMarkerGapIsConsumedPastRegression` (CRITICAL-1) | Stalled at the marker |
| `RecordsFromANonSessionSourceAreConsumedRegression` (MAJOR-1) | Records dropped |
| `StaleConnectResponseChangesNothingRegression` (MAJOR-2) | Subscriptions re-made: 3 instead of 2 |
| `StaleMetadataAfterALeaderLossIsAskedAgainRegression` (MEDIUM-1a) | Never moved |
| `AlreadyDisconnectedLeaderSessionIsRevivedRegression` (MEDIUM-1b) | Never revived |
| `CollidingSessionIdsNeverCrossDeliverRegression` (redesigned M3) | Fails |

Compile-RED only (new API):
- `PlainSubscribeRedirectedWhileGroupActiveReportsWhyRegression` (MEDIUM-3)
- hub-level `GapAdvancesThePositionRegression`, `RetiredInstancesAreBoundedRegression`, `OnlyTheCurrentSubscriptionInstanceIsAcceptedRegression` (pre-bind by session id, retired by id)

The C1 test `StaleRecordsAfterRejoinNeverMoveThePositionRegression` is kept and green. All 19 client-level group-routing tests pass. Full client suite: **969/969**. Both check scripts OK.

## Concerns

1. **The client and broker must ship together.**
   - A new client against an old broker: the coordinator still auto-subscribes at SyncGroup, and the member's explicit Subscribe on the seed is merged into that group subscription as a carrier, whose cursor ran ahead. That is the old stall on coordinator-led partitions.
   - An old client against a new broker gets nothing on any partition, because nothing auto-subscribes any more.
2. **Plain Subscribes for a non-seed-led partition still cannot succeed while a group is active.** Documented, with a reason; per-partition routing of plain subscriptions is a ticket.
3. **The C1 pre-bind window trusts the Records' broker `sessionId` plus a non-retired id.** This is safe because ids are cluster-unique and only one unanswered instance per partition and session is allowed. A user plain subscription to the same partition on the same seed session, still pending at the same moment, is the one theoretical overlap (its id becomes known on bind).
4. **Txn Produce redirects while a group is active** complete at their own deadline (not retried, comment corrected). A follow-up could forward them to the transactional producers.
5. **The broker branch is not rebased onto the current main (`ea807f2`).**
6. **Docker DR:** the CG files (and `test_tx_read_committed` / `test_tx_eos`, per the re-review) need your run with both images: broker at `f820c9f`, client at `a8e6812`.

---

# Fix round 3

This round answers review `2026-10-04-cg-redesign-review.md` (CHANGES REQUIRED: 0 CRITICAL, 2 MAJOR, 2 MEDIUM) and the controller's rulings. Nothing was pushed. No Docker. No conan.

## Commits (final SHAs for the images)

| Repo | Branch | SHA | Commit |
|---|---|---|---|
| rune-protocol | `feat/self-contained-worker-shards` | `8f6b38f` | client version 3 + `ConnectResponse.brokerVersion` |
| broker `rune-broker-wt51` | `fix/cg-unique-ids` (on 806c259) | `ac34a9b` | Records carry the Subscribe correlationId, version gate, reliable Subscribe answers |
| client `rune-client-cpp-wt14` | `fix/group-subscribe-routing` | `c9092f7` | exact instance identity by Subscribe correlationId, version gate, unanswered Subscribes re-sent on events |

Both images are built from sources that include the sibling rune-protocol, so they need rune-protocol `8f6b38f`.

## MAJOR-1: exact subscription-instance identity

**Broker.** Every Records push now carries the correlationId of the client Subscribe that created the subscription. That covers:
- the gateway inline push path;
- the gateway PushRead path;
- the worker push path.

The correlationId is kept in `Subscription`, `BufferedSubscribe`, `WorkerOp.correlationId` (an existing SBE field) and `WorkerSubscription`.

**Client, group hub.** A partition accepts a push only if all of these hold:
- `rv.correlationId` equals the instance's own Subscribe correlationId;
- `rv.sessionId` equals the broker session the instance was sent on;
- the session's connect epoch is still current;
- once bound, the subscriptionId matches as well.

The first push binds the subscriptionId if the SubscribeResponse has not arrived yet. Pre-bind acceptance of "any unretired id" is **removed**. Another group consumer's subscription, a plain subscription and every retired instance carry other correlationIds, so they can never match.

**Client, SubscriberHub.** A pending plain subscription is bound by its first push, matched on correlationId, the seed session id and the topic. Bound subscriptions also check the correlationId; a push with correlationId 0 is accepted, for fixtures only.

**Uniqueness of correlationIds within one client:**
- **Group fetch Subscribes** use bit 61 plus the hub's counter, so they are positive and disjoint from everything else.
- **Plain Subscribes** use the Client's duty counter, `nextCorrId_` (small, below 2^61).
- **Different sessions:** each Subscribe takes a fresh value whichever session it goes to, so a correlationId never repeats within one client.

**Two GroupConsumers in one Client (the review's scenario A)** cannot be created through the public API: `subscribeGroup` throws on a second group (`HubCommandClient.SecondSubscribeGroupThrowsRegression`). The hub-level test covers the logic anyway.

## MAJOR-2: version gate

- **rune-protocol.**
  - `kClientVersion = 3` (`kClientVersionExplicitGroupSubscriptions`).
  - `ConnectResponse` gains an **additive trailing `brokerVersion` int32**. The body grows from 14 to 18 bytes, a 42-byte frame, with `kBrokerVersion = 3`.
  - No existing field carried a broker version, so this minimal additive field was added.
  - `decode` still accepts the 38-byte frame of an older broker and reads its version as 0.
  - The golden vectors stay the 38-byte v0 form, which every decoder must keep accepting. The generator now writes that form explicitly; I regenerated all 17 vectors and they are byte-identical. The manifest notes the change.
- **Broker.**
  - The coordinator refuses `JoinGroup` from a session whose `clientVersion` < 3, with `ErrorCode::ProtocolVersion`. This is non-retriable and logs a warning. The session's version is read via `ClientGateway::sessionClientVersion`.
  - `ConnectResponse` advertises version 3.
- **Client.**
  - On a seed connect, the hub learns `brokerVersion`. Below 3, every GroupConsumer is finished with `GroupFailReason::BrokerTooOld` (new `GroupConsumer::failReason()`), without sending a JoinGroup. That includes consumers created after connect.
  - A JoinGroup answered `ProtocolVersion` finishes the consumer with `GroupFailReason::RefusedByBroker`, with no rejoin storm.
- **Documentation.** The compatibility rule is documented in the client header (`GroupFailReason`), in rune-protocol `Protocol.h`, and in the broker design spec §7.

## MEDIUM-1

There is no longer a retired list. Instance identity is exact (correlationId), so nothing can be forgotten at a bind.

- An abandoned instance can never match.
- An abandoned, still-unanswered instance is unsubscribed as soon as its id is revealed, by its SubscribeResponse or by its first push.
- The Fatal log text is corrected.

## MEDIUM-2: no timer

- **Lost answers no longer block.** The "never two unanswered instances" guard is gone, so a lost SubscribeResponse no longer blocks later Subscribes; that was the stall.
- **Re-send on events.** `GroupConsumerHub::resendUnanswered()` re-sends every Subscribe that has neither its answer nor a push. It runs:
  - on a leader session's accepted (re)connect;
  - on the M1 session events (lost or Disconnected).

  Each new generation re-subscribes anyway.
- **Broker.** SubscribeResponses and Subscribe Redirect/NotLeader answers are now queued and retried under back-pressure (`enqueueOrBestEffort_`, the produce answers' ring) on a real egress publication. Test sessions with null egress keep the direct send.

## LOWs

- **Fixed:**
  - LOW-1: the position is last offset + 1, via `RecordsView::lastOffsetDelta` (compaction holes inside a batch).
  - LOW-2: the id counters abort before carrying into the node prefix.
  - LOW-8: the stale-ConnectResponse test now asserts behind a Ping marker.
- **Not fixed (backlog):**
  - LOW-3: the FR marker truncates the session id.
  - LOW-4: ids are unique per process lifetime only.
  - LOW-5: the indirect epoch call on the hot path.
  - LOW-6: the coordinator-subscription machinery is dead code; remove it.
  - LOW-7: log a retention clamp.
  - LOW-9: the watcher ignores sessions above 64.

## Tests

All `*Regression*`, condition-driven, no sleeps.

**Broker.** On HEAD `806c259` sources, 3 new tests FAIL:
- `WorkerPushFixture.RecordsCarryTheSubscribeCorrelationIdRegression` (got 0);
- `CG4DeliveryFixture.GatewayRecordsCarryTheSubscribeCorrelationIdRegression` (got 0);
- `CG4DeliveryFixture.JoinGroupFromAnOldClientIsRefusedRegression` (the old client joined).

With the fix all pass. Full broker suite: **3741/3741**. `check_no_atomics` and `check_single_driver_pumper` OK.

**Client.** RED tree = round-2 head `a8e6812` plus only the test infrastructure (fake gateway, test file, codec `brokerVersion` parameter):
- `PlainSubscriptionNeverFeedsAPendingGroupSubscriptionRegression` FAILS: the member consumed the plain subscription's batch, and its position became 903.
- `LostSubscribeResponseNeverBlocksAReassignRegression` FAILS: p1 was never subscribed again.

Compile-RED only (new API):
- client-level `GroupConsumerRefusesABrokerTooOldRegression` and `JoinGroupRefusedForVersionFinishesTheConsumerRegression`;
- hub-level `AnotherConsumersSubscriptionNeverFeedsThisMemberRegression`, `PlainSubscriptionNeverFeedsAPendingGroupSubscriptionRegression`, `OldInstanceNeverFeedsAfterLaterBindsRegression`, plus the reworked `OnlyTheCurrentSubscriptionInstance` and `GapAdvancesThePosition` tests;
- conformance `ConnectResponseCarriesTheBrokerVersionRegression`.

All 23 client-level group-routing tests pass. Full client suite: **976/976**. Both check scripts OK.

## Concerns

1. **All three repos must ship together:** rune-protocol `8f6b38f`, broker `ac34a9b`, client `c9092f7`. Mixed versions are now refused loudly in both directions instead of losing data. An old client still consumes plain (non-group) subscriptions fine against the new broker.
2. **ConnectResponse frame size.** A broker now sends a 42-byte ConnectResponse. An old client decodes it, because its decoder checks `avail >= 38` and ignores the trailing bytes. Any third-party decoder that insists on exactly 38 bytes would break. There is none in the repos I checked (rune-ql, rune-live, rune-codec and rune-fr do not decode it).
3. **Plain Subscribes on a non-seed-led partition while a group is active** still fail, with a reason, as documented in round 2.
4. **The seed-side direct send for test sessions** stays best-effort, by design. Real sessions are queued.
5. **The pre-existing coordinator-subscription machinery** (`addGroupSubscription`, the carrier merge, the worker `preCarrier` cut) is dead in production; ticket to remove it (LOW-6).
6. **Docker DR** is yours: the CG files plus `test_tx_read_committed` / `test_tx_eos`, with images from the SHAs above.

---

# Fix round 4

This round answers section A of review `2026-10-05-cg-round3-flush-review.md` (2 MAJOR, 3 MEDIUM, 5 LOW). Section B is not mine and is untouched. Nothing was pushed. No Docker.

## Commits (final SHAs)

| Repo | SHA | Commit |
|---|---|---|
| rune-protocol (`feat/self-contained-worker-shards`) | `1ec8b4d` | ConnectResponse tests at the 42-byte form + `connect_response_v3.bin` golden (on top of `8f6b38f`) |
| broker `rune-broker-wt51` (`fix/cg-unique-ids`) | `6a67487` | queued Subscribe retry keeps correlationId + replica; LOW-3; tests |
| client `rune-client-cpp-wt14` (`fix/group-subscribe-routing`) | `141a29d` | MEDIUM-1/2/3, LOW-1/2, v3 conformance |

## A-MAJOR-1: the queued retry dropped the correlationId

- **Fix.** `trySubOp_` rebuilt the worker Subscribe op without `op.correlationId` and without `op.replica` (LOW-4). Both paths now use one builder, `ClientGateway::buildSubscribeOp_`: the direct `enqueueSubscribeOp_` and the queued `trySubOp_`.
- **Audit of every subscribe-op and Records path:**
  - egress-resolve waiters (they copy `BufferedSubscribe`, which carries the correlationId);
  - a parked worker subscription later activated (same op data);
  - a re-subscribe of the same id (updates `subscribeCorrId`);
  - PushRead (`sub.subscribeCorrId`);
  - inline push;
  - worker push.

  All of them carry the correlationId. `addGroupSubscription` (dead in production) passes 0.
- **Tests.**
  - **RED through the gateway's full-ring queue:** `ReplicaLifetimeRegression.QueuedSubscribeKeepsItsCorrelationIdAndReplicaRegression` (ShardNonBlockingTest). It fills worker 0's ring, sends `forwardSubscribeForTest(…, corr)` (the hook gained the parameter), checks the Subscribe was queued, then pumps and drains. It asserts the worker subscription's `subscribeCorrId` and `replica`. RED on `ac34a9b` sources: `subscribeCorrId` 0.
  - **PushRead stamp (LOW-5):** `CgPushReadStampRegression.PushReadRecordsCarryTheSubscribeCorrelationIdRegression` (EosRig, gateway PushRead path end to end).

## A-MAJOR-2: rune-protocol suite

- **Tests updated.** The 3 `ConnectResponseCodec` tests now assert the 42-byte form (`brokerVersion` at offset 38). New: `OlderBrokerFrameDecodesAsVersionZeroRegression`.
- **v0 goldens kept.** `connect_response.bin` and `connect_response_redirect.bin` remain the v0 decode vectors: blockLength `BODY_FIXED_V0`, `brokerVersion == 0`, and they re-encode through the v0 truncation.
- **New golden.** `connect_response_v3.bin` (42 bytes, `brokerVersion = 3`) is in `gen_vectors.cpp`, the manifest and the README. The other 17 regenerated vectors are byte-identical.
- **Checked in both repos.** rune-protocol `ConformanceGolden.ConnectResponseV3`, and client `ClientCodecConformance.ConnectResponseV3Regression`.
- **Broker.** It has no golden-vector tests. Instead, `GatewayLoopbackFixture.Connect_RoundTrip` now asserts the gateway's real ConnectResponse carries `brokerVersion == kBrokerVersion` and a node-prefixed session id.
- **Suite.** rune-protocol: **501/501** (`cmake --build build && ctest --test-dir build`; was 494/499).

## MEDIUMs

- **A-MEDIUM-1.** SubscriberHub binds a pending plain subscription by a push only when the seed session id is non-zero and equals the push's. Mid-connect, only the SubscribeResponse binds.
  - Test: `SubscriberHubCommand.PushNeverBindsAPendingSubscriptionWithoutASessionRegression`. RED on `c9092f7`: the foreign push bound the subscription.
- **A-MEDIUM-2.** `resendUnanswered(session)` re-sends only that session's unanswered Subscribes. It is called with the reconnected leader session's index and with each watcher event's session.
  - Test: hub-level `SessionEventResendsOnlyThatSessionsUnansweredRegression`. Compile-RED (new signature); round 3 re-sent every session's.
- **A-MEDIUM-3.** The agent records each leader session's `brokerVersion` at its accepted connect (`FetchRouter::brokerVersion`). A partition whose leader node runs a version below 3 is **parked and logged** ("partition parked until the node is upgraded") rather than subscribed, so it never stalls silently. Fresh metadata, such as the upgraded node reconnecting, retries it.
  - Documented on `GroupFailReason`: the whole cluster must be at version 3 or later for every partition to be consumed.
  - Test: `PartitionLedByAnOldNodeIsParkedNotSilentlyStalledRegression`. RED on `c9092f7`: it subscribed p1 on the old node.

## LOWs

- **Fixed:**
  - LOW-1: group-tagged pushes skip SubscriberHub's pending-subscription lookup.
  - LOW-2: `late_` learns an abandoned instance's id only from its own session's push (`LateSub.brokerSid`).
  - LOW-3: the old-client JoinGroup refusal log is rate-limited to the first refusal per session and every power of two after it (bounded map).
  - LOW-4: `replica` on retry, via the shared builder.
  - LOW-5: a PushRead stamp test.
- **Open (backlog):** the round-3 backlog LOWs (FR marker truncation, per-process-lifetime ids, indirect epoch call, dead coordinator-subscription machinery, retention-clamp log, watcher > 64 sessions).

## Results

| Suite | Result |
|---|---|
| rune-protocol | 501/501 |
| broker | **3743/3743**; `check_no_atomics` and `check_single_driver_pumper` OK |
| client | **980/980**; both check scripts OK |

## Concerns

1. **Ship together:** rune-protocol `1ec8b4d` (protocol headers unchanged since `8f6b38f`; only tests and vectors were added), broker `6a67487`, client `141a29d`.
2. **During a rolling upgrade**, partitions led by not-yet-upgraded nodes are parked, not consumed, until those nodes are upgraded. That is visible in the log, but `GroupConsumer::failReason()` stays `None`, because the consumer keeps running.
3. **The test-session (null egress) Subscribe answers** stay on the direct send by design. Real sessions are queued.
4. **B (`wt15`) needs A's `tests/ConformanceTest.cpp`** (now plus the v3 golden test) to be green against the new rune-protocol, as the review noted.
