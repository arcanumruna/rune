# Review: consumer-group fetch routing (`fix/group-subscribe-routing`)

**Date:** 2026-10-04
**Worktree:** `~/Projects/rune-client-cpp-wt14`
**Branch:** base `main 3c7e97b`; commits `bfe31ef bff9024 48a5329 679f4bc 6d2e013`
**Report reviewed:** `2026-10-04-cg-routing-report.md`

## Verdict: REJECT (not mergeable as is; the routing core is right, but the items below block the merge)

| Severity | Count |
|---|---|
| CRITICAL | 1 |
| MAJOR | 3 |
| MEDIUM | 7 |
| LOW | 6 |

The routing core is sound and fixes both diagnosed defects:

- Each partition's Subscribe goes to its leader's session, stamped with that session's id.
- A tagged Redirect or NotLeader never reaches the seed-level redirect path.
- Group protocol frames stay on the seed.

The problem is that the branch adds a second source of Records for a partition and does not decide which source owns it. Stale leader-node subscriptions can push records. Those records can set or advance the member's position, and the member then commits past records it never consumed. That is silent loss for an at-least-once group (C1). The branch also has no way to notice that a leader node has died (M1), so the partition stalls.

## Verification performed

- **Build and tests.** `cmake --build build` reported no work to do, so the binaries were current. `ctest --test-dir build -j6 --timeout 300`: **949/949 passed**, 3 platform tests skipped as on main. All 9 new tests ran and passed:
  - client-level tests #873–877;
  - hub-level tests #393–396.
- **Check scripts.**
  - `check_no_atomics_or_mutexes.py`: OK (67 files).
  - `check_no_shared_ptr_handles.py`: OK (67 files; transport allowlist 8 sites).
- **RED on main.**
  - I did not rebuild main myself, because the review was read-only.
  - `/tmp/wt14-red2.log` shows all 5 client-level tests FAILED with the messages quoted in the report.
  - The 4 hub-level tests do not compile on main (there is no `FetchRouter`), so their RED is compile-level only.
- **Broker code checked (wt26):**
  - `ClientGateway::handleSubscribe`, `addGroupSubscription`, `handleUnsubscribe`, `pushSubscriptions` and `sendSessionExpired_`;
  - `GroupCoordinationService::handleSyncGroup` and `handleOffsetFetch`;
  - `SessionTable::nextSessionId`.

---

## CRITICAL

### C1. A retired leader-node subscription's records seed or advance the position, and the member commits past records it never consumed

**Where:**
- `src/group/GroupConsumerHub.cpp:1007-1010`: `onRecords` takes the first Records it sees as the position seed, and otherwise accepts any batch where `newPos > pos`. It does not check which subscription the batch came from.
- `GroupConsumerHub.cpp:183`: `toRejoining_` leaves the leader-node subscriptions alive.
- `GroupConsumerHub.cpp:1112`: on SyncGroup OK, the subscriptions are dropped only after `state = Stable` and the positions are reset to the sentinel.
- `GroupConsumerHub.cpp:1223`: OffsetFetch never lowers a position that was already advanced.

**Scenario.** Data is produced continuously. In the DR netem case, the consumer loses its link to the coordinator but can still reach node 1.

1. Member M is Stable. It owns p1, which node 1 leads, through subscription S1, and has consumed up to P.
2. A heartbeat answers UnknownMember, IllegalGeneration or RebalanceInProgress, and M enters Rejoining. This path has no eager `Assign{gen,{}}`, so S1 is never unsubscribed.
3. Node 1 knows nothing about the rebalance and keeps pushing S1 records P..Q-1. `onRecords` drops them because M is not Stable.
4. SyncGroup OK arrives. M becomes Stable with p1 at the sentinel, and `dropAllFetch_` offers an Unsubscribe to node 1.
5. Records that S1 pushed before node 1 processed the Unsubscribe are still arriving on node 1's image. They come after the SyncGroupResponse, which arrived on a different image, so nothing orders them. The first of them is batch Q. It is delivered to the application and seeds `pos = Q+n`.
6. OffsetFetch returns the committed offset C (C ≤ P). Because `Q+n > C`, the position is kept (line 1223), and `reconcileImpl_` subscribes on node 1 **from Q+n**.
7. Records P..Q-1 are never delivered. The next auto-commit writes Q+n. **Silent loss.**

**Variant.** A stray record arrives *after* the OffsetFetch answer, when `pos == C`.
- The position jumps to Q+n while the new subscription is still re-delivering from C.
- An auto-commit of Q+n followed by a crash or rebalance before the new subscription catches up loses C..Q-1.

**Other sources of stale records:**
- an orphan left by an Unsubscribe the transport refused (M2);
- the old node's subscription after a leader change;
- the coordinator's group subscription for a non-led partition (see MEDIUM-3).

**Why it is new.** On main, the coordinator's group subscription is re-created at SyncGroup on the *same* node, and its egress is ordered with the SyncGroupResponse, so old-cursor records arrive before the transition. Leader-node records have no ordering relative to the coordinator's frames.

**Pre-existing broker case of the same mechanism.** "Fix #4" in `GroupCoordinationService.cpp:~490` skips revoke+recreate when the subscription count is unchanged. The group subscription then continues from its push frontier, past records the client dropped while Rejoining. Track it alongside this fix.

**Fix (required):**
- Decide which subscription owns each partition.
  - Keep the bound `subscriptionId` (and session index) in `FetchSub`.
  - Keep a small cold-path list of *retired* (session, subscriptionId) pairs per partition.
  - In `onRecords`, drop any batch whose `rv.subscriptionId` is retired for that (topic, partition).
  - Under per-worker egress, Records can arrive before the SubscribeResponse because they travel on a different publication. That is why a retired-list filter is needed and an exact-match filter is not.
- Never let Records seed a routed leader-node partition that is still at the sentinel. Its Subscribe is only sent after OffsetFetch answers, so any such record is stale by construction.
- Unsubscribe leader-node subscriptions on entry to Rejoining. This shrinks the window and stops wasted pushes.
- **Regression test (`*Regression*`):**
  1. Stable at P.
  2. The gateway answers a heartbeat with RebalanceInProgress.
  3. The gateway pushes S1 records P..Q-1 while the member is Rejoining.
  4. Release Sync, then push one stray S1 record at Q.
  5. Assert that the next node-1 Subscribe `fromOffset == C`, and that the auto-committed offset never exceeds the last contiguously consumed offset.

---

## MAJOR

### M1. A dead or never-connected leader-node session is never detected, so the partition stalls silently and indefinitely

**Where:**
- `GroupConsumerHub.cpp:641`: `if (sid == 0) continue;` and nothing else happens.
- `Client.cpp` `tickMetadataRefresh` fires only when a refresh is armed. Refreshes are armed only by produce no-progress, Redirect, NotLeader, a ConnectResponse, or NotCoordinator. Nothing arms one when a session is lost.
- `ClientAgent.cpp:901-930`: `sessionForEndpoint` returns the existing (dead) session for that endpoint forever and never re-opens it.
- `SessionMachine` drops to a terminal `Disconnected` when:
  - a session never reached Live, or
  - `reconnectForeverAfterLive=false`, which is the **default** for a plain `Client` (`Client.h:192`).

**Scenario A (test_cg_failover shape).** Node 1, leader of p1, is killed.
1. The consumer's node-1 session times out and goes Resuming, then Disconnected (default config) or Backoff forever (shard config).
2. The cluster elects node 2 for p1.
3. A consumer-only client sends no produce and gets no Redirect, so it never requests metadata.
4. The route stays on node 1, and `reconcileImpl_` waits for `sid != 0` forever.
5. The member keeps heartbeating, so no rebalance hands p1 to anyone else. p1 is never consumed again, and no error is raised.

**Scenario B.** A member starts while node 1 is briefly unreachable (netem, or the driver warming up).
1. The node-1 session exhausts its first-connect budget and becomes terminally Disconnected.
2. Node 1 recovers and still leads p1. The same endpoint maps to the same dead session, so p1 is never consumed.

**Fix:**
- In the sweep: if any assigned partition's target session has not been Connected for longer than the response timeout, call `scheduleMetadataRefresh()`, which is backoff-gated.
- Re-drive `connect()` on a non-seed session that is Disconnected while a route still points at it.
- Add a client-level `*Regression*` test: kill node 1's endpoint, with no manual `sendMetadata`. The gateway answers the client's own MetadataRequest with node 2 as leader. Assert that p1 is consumed on node 2.
- The current `LeaderChangeResubscribes…` test pushes metadata by hand, so it never exercises detection.

### M2. An Unsubscribe the transport refuses is never retried, and the orphan lives for the whole client lifetime

**Where:**
- `GroupConsumerHub.cpp:594`: `(void)router_.offer(...)`, best effort.
- The same applies to `late_` answers at line 553 and to the GroupClose path at line 273.

**The implementer's impact claim is wrong.** The report says the orphan "dies with that node session". But node sessions are append-only and are re-used for every later route to that node (`ClientAgent.h` "sessions are append-only"; `sessionForEndpoint`). An orphan therefore pushes its partition for the client's lifetime.

**Impact:**
1. Wasted broker cursor, push bandwidth, and client decode and dispatch.
2. If the partition is ever re-assigned to this member, the orphan's records are near the log end. They feed C1 directly: the position jumps to the orphan's offset while the new subscription re-delivers from C, and a commit then loses C..orphan.

`offerTo` returns Retry under back-pressure or while the publication is not connected, both of which are normal under load.

**Fix:**
- Keep pending Unsubscribes in a cold vector `{session, sessionId, subscriptionId}`.
- Re-offer them from `reconcileFetch()` (event or sweep) while `sessionId(session) == sessionId`, and drop them when the id changes.
- The C1 retired-id filter also neutralises any orphan that still slips through.

### M3. Subscription ids collide across nodes, so leader-node group records are delivered to an unrelated plain subscription

**Where:**
- `SubscriberHub.cpp:422-428` matches Records **only** by `rv.subscriptionId`.
- `Client.cpp:2287` feeds every Records frame to the SubscriberHub first.
- The broker allocates subscription ids from a per-node counter that starts at 0: `ClientGateway.cpp:3020` and `:4572`, `++nextSubscriptionId_`.

**Scenario.** One client owns a plain subscription on topic B (seed, id 5) and a group consumer of topic A.
1. Node 1 assigns id 5 to the member's p1 fetch subscription.
2. Every A/p1 batch is dispatched to the topic-B handler as well, so the user sees foreign records.
3. Those batches advance `SubscriptionImpl::nextOffset` to A/p1 offsets. The plain subscription's resume and resubscribe offset becomes garbage, which can mean skips or replays on B.

**Why it is new.** Before this branch, all of a client's subscriptions lived on one node, so their ids were unique. The DR "consume-transform-produce" and EOS patterns combine group and plain usage in one client.

**Fix:**
- `SubscriberHub::onRecords` must also check `rv.topicId` and `rv.partition` against the impl. That is one compare on a branch that is already taken.
- Better, also key on the egress session, or stop SubscriberHub from claiming ids it did not bind on that node.
- Add a regression test with colliding ids.

---

## MEDIUM

### MEDIUM-1. A session id reused on the same node after a restart makes a dead subscription look live

**Where:** `GroupConsumerHub.cpp:600` and `:636`. Liveness is `(session index, broker sessionId)`.

**Background.** Cross-node collisions are handled correctly, because the index is compared and not just the id. However, `SessionTable::nextId_` starts at 0 in every broker process (`SessionTable.h:86,153`).

**Scenario.**
1. Node 1 restarts, and the client re-connects its node-1 session.
2. The new session gets the same ordinal id k.
3. Node 1 still leads p1 (preferred leader, or no intervening route change).
4. `reconcileImpl_` sees `fs.sessionId == sessionId(target)` and `continue`s. The restarted node has no subscription, so p1 stalls silently.

**Fix:** have the agent expose a per-session-index *connect generation*, incremented on every ConnectResponse OK, and compare that instead of the broker id. It is cheap and agent-thread only.

### MEDIUM-2. Leader-node subscriptions depend on a single, unretried OffsetFetch

**Where:**
- `GroupConsumerHub.cpp:646` waits for `offsetKnown`.
- `sendOffsetFetch` ignores the `sendFn` result (line 536) and uses correlationId 0.
- Entries with errors are skipped (line 1218).
- The broker caps the response at `outCap`.

**Scenario.** Any of these leaves non-coordinator partitions unsubscribed for the whole generation, while the member heartbeats normally:
- the ring refuses the OffsetFetch;
- an entry answers NotCoordinator or another error;
- the response is capped because the assignment is large.

On main, a missing OffsetFetch only delayed seeding. Here it blocks consumption.

**Fix:**
- Re-send OffsetFetch from the sweep while any assigned leader-node partition is not `offsetKnown` (bounded backoff).
- Treat an errored entry as not known and retry it.

### MEDIUM-3 (implementer concern 4). Two sources per partition give duplicates, and a large re-delivery when leadership moves to the coordinator

**Background:**
- `GroupCoordinationService.cpp:~500-545` creates a group subscription on the coordinator for **every** assigned partition, led or not. Each is seeded at the committed offset, with isolation 1 and `pendingCarrier`.
- Neither `pushSubscriptions` (`ClientGateway.cpp:3128`) nor `Worker::executePushReadWire_` gates on leadership.
- The client sends the carrier for a non-led partition to the leader node, never to the coordinator, so the coordinator's subscription never gets its carrier. Whether it pushes from the follower log depends on read-committed and LSO state on followers. "DR shows it does not" is an observation, not a guarantee.

**Scenario.** p1 moves from node 1 to the coordinator.
1. The client unsubscribes on node 1 and sends a carrier on the seed.
2. The coordinator's group subscription resumes from the generation's **SyncGroup-time** committed offset, which can be a whole generation behind.
3. `onRecords` re-delivers every batch below the position, because it never filters `baseOffset + count <= pos`.

At-least-once still holds, and positions do not regress (`newPos > pos`), but the DR `assert_group_bounded_duplicates` budget is at risk.

**Fix:**
- Client: skip, without dispatching, any batch entirely below the position for routed partitions. This is one compare on the hot path and allocation-free.
- Broker (ticket): create or push group subscriptions only for led partitions, or leader-gate the push.

### MEDIUM-4 (implementer concern 2). A seed-level redirect can still move the coordinator session

**Where:** `Client.cpp:1985` includes `groupActive` in `haveWork`, and `:2086` retargets the seed and reconnects.

**Scenario.** Any untagged Redirect while a group is active moves the coordinator session and brings Defect 2 back:
- a plain `SubscriberHub` Subscribe redirect;
- a produce Redirect before routes exist (`!multiNodeRouting()` or `!havePart`);
- a transactional op.

The canonical EOS consume-transform-produce client hits the produce case.

**Fix (in-scope minimum):** when `!groupConsumerHub_.empty()`, never retarget the seed. Route per partition, or fail the op with a retriable status.

### MEDIUM-5 (implementer concern 3, now reachable from consumers). Leader-node session events are credited to the seed

**Where:**
- `ClientAgent.cpp:945`: `sessionForConnectCorrId` falls back to the seed.
- `ClientGateway.cpp:1166-1175`: the broker's `SessionExpired` carries correlationId 0.
- `Client.cpp:2218`: `sessionFatal` fails **all** pending control ops with SessionLost and fires `cbError_`.
- `Client.cpp:1918`: `cbConnected_` fires for leader sessions.

**Scenario.** A frozen or slow leader node expires the consumer's node-1 session. Its `SessionExpired` is credited to the seed, which is the coordinator session.
- The seed machine goes Resuming, which churns the coordinator session (possible UnknownMember and a rebalance).
- Every pending ping and commit fails with SessionLost.
- The node-1 session itself is never marked dead, so the partition waits for keepalive detection.

**Fix:** backlog ticket now. It needs a broker-side correlationId on `SessionExpired`, or demultiplexing by egress image.

### MEDIUM-6. A test port collides with an existing test and can flake under `ctest -j6`

**Where:**
- `tests/client/GroupRoutingRegressionTest.cpp:265` binds UDP 24880 (seed) and 24881 (node 1).
- `tests/shard/ShardGroupTest.cpp:820-821` (`ShardGroupRegression.UdpEgressAcksArriveOnEachShardsOwnPort`) binds 24880 and 24881 as shard egress ports.

They are in different binaries, so ctest can schedule them concurrently. **Fix:** move them to an unused range.

### MEDIUM-7 (implementer concern 7, pre-existing). A GroupConsumer created after `connect()` never joins

**Where:** `GroupConsumerHub.cpp:853`. The Unjoined state waits for `onConnected`, which fires only on a seed ConnectResponse.

**Scenario.** The application connects and then calls `subscribeGroup`. It consumes nothing until the seed reconnects.

**Classification:** backlog, but a one-line fix: send JoinGroup on `GroupAdd` when `sessionConnected_` is set.

---

## LOW

1. **`isFetchCorrId` ignores the sign** (`GroupConsumerHub.h:240`). A negative id whose bits 61–62 are `01` would match. No such ids exist today: A10 keepalive ids are `-(i+1)`, which has bits 61–62 = `11`. Add `c > 0`.
2. **Parking barely parks.** The 100 ms sweep calls `unparkFetch()` unconditionally (`Client.cpp:1783`).
   - `kMaxFetchRedirects` therefore parks for at most 100 ms.
   - A Fatal offer or an error SubscribeResponse (UnknownTopic) is retried 10 times per second forever.
   - Unpark only on fresh metadata, or with a backoff.
3. **An orphan plain subscription can be created on the seed.** If `addGroupSubscription` failed on the coordinator (for example, the log did not exist yet at SyncGroup), the carrier Subscribe falls through to a plain subscription (`ClientGateway.cpp` step 6). `dropFetch_` never unsubscribes session 0 (`GroupConsumerHub.cpp:600`), so that subscription is orphaned on revoke.
4. **`autoOffsetReset` is ignored.** With no committed offset, a leader-node subscription starts at 0 (`GroupConsumerHub.cpp:647`), while `GroupConfig` defaults to `Latest`. The coordinator does the same (`fromOffset = 0`), so this is consistent and not a regression. But the config field is dead: it is never sent or used. Document it or wire it.
5. **Concern 5 (slow rebalance start).** A member could rejoin on an eager `Assign{newer gen}`. Backlog. It also lengthens the Rejoining window that C1 exploits.
6. **Re-entrancy note.** `leaderSessionIndex` may `emplace_back` into `sessions_` from inside `onEgressFrame`. This is safe today because sessions are held through `unique_ptr` and no `sessions_` range-for is live on that path. Add a comment or assert so a future tick-loop callback cannot invalidate the iterator.

---

## Answers to the specific checks

1. **Correctness:**
   - **Can the coordinator session move?** Not from a group fetch Subscribe or Redirect. A tagged Redirect goes to `onGroupFetchRefused_` at `Client.cpp:1945-1950`. The SessionMachine ignores Redirect while Connected, and while Connecting it matches only its own connect correlationId. Other untagged redirects still can (MEDIUM-4).
   - **Revoke and rebalance.** On revoke, the Unsubscribe goes to the right node and session id. A rebalance does not create double subscriptions: an unanswered Subscribe causes `continue`, and an abandoned one goes to `late_`. The exceptions are a refused Unsubscribe (M2) and stale records (C1).
   - **Records from a leader session.** They reach `onRecords` and position tracking; the agent forwards Records regardless of session.
   - **Coordinator traffic.** Commit, OffsetFetch, Join, Sync, Heartbeat and Leave stay on the seed (`submitSend` and direct send use the default publication).
   - **Session-id stamping.** Subscribes are stamped with the target node's id, and Unsubscribes with the id the subscription was made under, guarded by an index+id match. Cross-node collisions are harmless. Same-node reuse after a restart is not (MEDIUM-1).
   - **A10 keepalive.** No interaction. Pong is demultiplexed by the negative correlationId in the agent (`ClientAgent.cpp:767-775`). Fetch ids are positive with bit 61 set.
2. **Duplicates and positions.** Duplicates come from MEDIUM-3, from in-flight records of the old node after a leader change, and from M2 orphans. Positions never regress (`newPos > pos`, and OffsetFetch only raises them). But positions can **jump forward over unconsumed records** (C1), which is worse than regressing.
3. **Robustness:**
   - **Leader change during a rebalance.** Handled: reconciliation is skipped while not Stable, and SyncGroup and OffsetFetch reconcile against the current route.
   - **A leader-node session that never connects.** A silent, indefinite stall (M1).
   - **A refused Unsubscribe.** A lifetime orphan that feeds C1 (M2).
4. **Implementer's concerns:**

   | # | Concern | Classification | Finding |
   |---|---|---|---|
   | 1 | NotCoordinator re-discovery and seed re-home | Backlog | Pre-existing; DR connects to the leader. Ticket now, because any bootstrap list pointing at a follower never joins. |
   | 2 | Seed stick-to-leader | **In scope now**, minimum guard | MEDIUM-4 |
   | 3 | Leader ConnectResponse and SessionExpired credited to the seed | Backlog ticket now | MEDIUM-5 |
   | 4 | Duplicate delivery | Client-side below-position skip now; broker gating in backlog | MEDIUM-3 |
   | 5 | Slow rebalance start | Backlog | LOW-5 |
   | 6 | Best-effort Unsubscribe | **In scope now** | M2; the "dies with the session" premise is false |
   | 7 | Consumer created after `connect()` stays Unjoined | Backlog, trivial | MEDIUM-7 |

5. **Hot paths.** `onRecords` and Produce are unchanged. There are no new atomics, mutexes or `shared_ptr`, and both scripts pass. `FetchSub` map nodes are allocated per assignment change, and the `late_` vector grows only on cold paths. The sweep reconcile is O(assigned partitions) every 100 ms, with no allocation in steady state. The C1 and MEDIUM-3 fixes each add one branch to `onRecords`, still with no allocation.
6. **Tests:**
   - **RED.** All 5 client-level tests are RED on main per the implementer's log, which I did not re-run. The 4 hub-level tests are compile-RED only.
   - **No sleeps.** Waits are bounded iteration pump loops.
   - **Real clock.** The UDP tests depend on a real clock (connect retry, the 200 ms heartbeat), so they are timing-sensitive in principle but bounded.
   - **Port collision.** See MEDIUM-6.
   - **Coverage gaps:** stale or stray records (C1), detecting a dead leader without hand-fed metadata (M1), a refused Unsubscribe (M2), and colliding subscription ids (M3).

## Required before merge

C1, M1, M2 and M3, each with a `*Regression*` test. Also MEDIUM-4's minimum guard and MEDIUM-6's port change. File tickets for the remaining MEDIUM items. Re-run the full CG DR files (`test_cg_rebalance`, `test_cg_atleastonce`, `test_cg_failover`) in a clean Docker window. The earlier run was contaminated by a shared compose project.
