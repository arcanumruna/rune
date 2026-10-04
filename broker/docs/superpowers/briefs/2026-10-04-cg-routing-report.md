# Consumer-group routing fix: report (2026-10-04)

Client worktree: `/Users/vradu/Projects/rune-client-cpp-wt14`, branch `fix/group-subscribe-routing`, rebased on client main `3c7e97b`. Nothing was pushed. No broker change was needed (see Defect 2).

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
