# D4 client review — coordinator redirect + seed identity (rune-client-cpp)

- Worktree: `~/Projects/rune-client-cpp-wt17`, branch `fix/d4-coordinator-redirect`
- Commits on main (42ff385): `a0f61a1` (coordinator answer moves the seed; reconnect re-joins in-flight joins), `25c2ae5` (seed endpoint follows every retarget)
- Reviewed: full `git diff main..HEAD`, plus every user of `Session::endpoint`, `cfg_.transport.ingressChannel`, `transport_.ingressChannel()`, `sameEndpoint`, `sessionForEndpoint`, route `sessionIndex` caching, crypto seal lookup, Pong/Connect/Error demux, the T15a Backoff retargets, the txn producer resend, and GroupConsumerHub join paths. On the broker side: `ClientGateway::redirectToCoordinator_`, `SessionTable::nextSessionId`, and `GroupCoordinator::join` (new member on an empty memberId).
- Tests run at HEAD (macOS, `build/`):
  - `rune_invoker_tests --gtest_filter=GroupRoutingRegression.*` passed 30/30 in 3 of 4 runs. One run failed in the pre-existing `BatchHeldOnAConnectingDeadLeaderAsksForMetadataRegression` with "exception thrown in the test body"; it passed 3/3 when run alone (see LOW-6).
  - The new `MemberOnAFollowerJoinsTheCoordinatorRegression` passes in about 630 ms.
  - Also green: `IngressEndpointRouting*`, `InvokerAgent.*`, `ShardGroupRegression.*`, `ProducerRoutePark.*` (56), `rune_agent_tests` (15), `rune_multinode_routing_tests` (6), `rune_redirect_tests` (5), `rune_client_crypto_regression_tests` (16).

Verdict: the core idea is right. The seed's endpoint identity must follow the transport, and the new test does exercise both halves: the coordinator move, and the old seed node's partition subscribing on its own leader. But there are three MAJOR problems:

- Moving the seed's identity breaks two invariants that the rest of the agent relies on: one session per node (M6), and the cached `PartitionRoute::sessionIndex`.
- `NotCoordinator` is a new error code that can reach a seed `SessionMachine` while it is `Connecting`, and correlation-id namespaces collide.

Counts: **0 CRITICAL, 3 MAJOR, 5 MEDIUM, 6 LOW.**

---

## MAJOR

### MAJOR-1: A moved seed shares a node with an existing leader session, so that session's control frames are sealed under the seed's crypto state

- `src/agent/ClientAgent.cpp:1061` (`offerControlTo` → `sealForDestination_(sessionForEndpoint(channel)…)`)
- `src/agent/ClientAgent.cpp:1076` (`sessionForEndpoint` checks the seed first)

**What goes wrong:** `offerControlTo` relies on the invariant that `sessionForEndpoint(channel)` resolves to the same session whose `SessionEndpointIo::sendFrame` is calling (see the comment at :1051). That holds only while no other session shares the seed's endpoint (M6: one session per node). Before D4 the seed's `endpoint` was always the bootstrap, so a leader session could never alias it. Now it can.

**Failure scenario** (the exact topology of the new test, with crypto on):
1. The seed is on follower node0. Partition 0 is led by node1, so metadata creates leader session `k` to node1 (`aeron:udp?endpoint=host1:p1`).
2. The follower answers `NotCoordinator{host1:p1}`. The seed retargets, and `syncSeedEndpoint_` sets `seed.endpoint = aeron:udp?endpoint=host1:p1`.
3. Session `k`'s keepalives and Pings go through `offerControlTo(k.io.endpoint)`. `sessionForEndpoint` now returns the seed (checked first, `sameEndpoint` is true), so `k`'s frames are AEAD-sealed under the seed's keys and nonce counter while the header carries `k`'s sessionId.
4. The broker fails authentication for every `k` control frame and drops it. `k` never gets a Pong, times out to `Resuming`, and re-handshakes. The `Connect`/`HandshakeContinue` frames are exempt from sealing, so that succeeds, and then keepalives fail again. `k` flaps forever.
5. Data still routed to `k`, through cached route indexes (`offerOnSession(k)`, `offerProduceToLeader_`), stalls on every flap. Each mis-sealed frame also burns a seed nonce.

**Without crypto:** the behaviour is correct but wasteful. Two live sessions run to node1 (the seed and `k`), `k` keeps sending keepalives and holding broker resources, and new routes resolve to the seed while old routes keep `k`.

**Suggested fix:**
- Seal for the caller's own session, not one looked up by endpoint. `SessionEndpointIo` already identifies its session by identity (`this == &agent->seedSession().io`), so pass the owning `Session&` (or a back-pointer) into `offerControlTo` and drop the `sessionForEndpoint` lookup there.
- Optionally, when `syncSeedEndpoint_` moves the seed onto a node that already has a leader session, retire that session: repoint routes whose `sessionIndex == k` to 0, and let `k` disconnect.
- Add a regression test with crypto on: the seed moves onto a node that has a leader session, and that session must stay `Connected` across 3× `responseTimeout`.

### MAJOR-2: Cached `PartitionRoute::sessionIndex == 0` survives the seed move, so frames carry the new node's sessionId to the old node (shard mode / worker routes)

- `src/agent/ClientAgent.cpp:705` (`syncSeedEndpoint_` invalidates nothing)
- `:1200` (`setRoute` returns early when nothing changed, keeping the cached index)
- `:1231`, `:1339` (index resolved once per route)

**What goes wrong:** a route whose `sessionEndpoint` equals the seed's node but whose `dataChannel` differs is kept, because `isDefault` requires both to match the seed. That is any Task-16 worker route (`ingressEndpoint` non-empty). On first use it resolves `sessionIndex = 0` (the seed). After the seed moves, the metadata answer for that partition is unchanged, so `setRoute` returns false and the stale index 0 stays.

**Failure scenario** (shard-mode brokers, producer plus group consumer in one client, bootstrapped on follower node0):
1. Partition p is led by node0, with worker route `{session=node0, data=worker0}` and `sessionIndex=0`.
2. D4 moves the seed to node1.
3. `offerProduceToLeader_` uses `sessions_[0]`, which is now node1's session. It stamps node1's cluster-unique sessionId and offers on worker0's publication on node0.
4. node0's worker drops the frame as an unknown or closed session (`Worker.h` `kDirectDropClosed` / gateway UnknownSession drop). Every retry takes the same route.
5. The produce never converges and fails only at its deadline.

Group fetch for p also goes out on the seed (to node1). It does heal through `Redirect` → `onGroupFetchRefused_` → `setRoute(node0,node0)`, but produce has no such trigger.

The same class of bug existed for data-op seed retargets before D4. D4 newly moves the seed while a group is active, which MEDIUM-4 previously forbade, and its own commit message claims "the seed's identity follows every retarget".

**Suggested fix:** in `syncSeedEndpoint_` (a cold path), invalidate every cached route session: `for (auto& r : routes_) r.sessionIndex = kNoSession;`. Better still, re-evaluate `isDefault` for routes whose `sessionEndpoint` now equals or no longer equals the seed. Add a regression test using an `IngressEndpointRouting`-style rig: worker route on the bootstrap node, coordinator redirect to node1, then the produce to p must be acked by node0 with node0's session id.

### MAJOR-3: A follower's `NotCoordinator` reply with a colliding correlationId kills the D4 reconnect (seed goes to terminal `Disconnected`)

- `src/agent/ClientAgent.cpp:~922` (ErrorResponse → `sessionForConnectCorrId` → `machine.onErrorResponse`)
- `include/rune/client/session/SessionMachine.h:564-583` (in `Connecting`, any non-Redirect/NotLeader code whose corrId matches → `onConnectError_`)
- `include/rune/client/Client.h:1111` (`nextCorrId_ = 1`)

**What goes wrong:** the seed's connect correlationIds come from its own counter starting at base 0 (1, 2, 3, …). The Client's duty counter `nextCorrId_` also starts at 1 and is shared by MetadataRequest, txn InitProducerId / AddPartitions / EndTxn, and similar requests. Both namespaces are small integers early in a client's life, which is exactly the bootstrap-on-a-follower window D4 targets. Before D4, an established-session `ErrorResponse` that collided with a connect corrId was harmless: Redirect/NotLeader resend the connect. `NotCoordinator` is not harmless, because it goes to `onConnectError_`.

**Failure scenario** (default `reconnectForeverAfterLive=false`):
1. The client connects to the follower with seed corr 1. The metadata request takes Client corr 1, and the txn `InitProducerId` takes Client corr 2. A group member's JoinGroup (corr 0) is also in flight.
2. The JoinGroup's `NotCoordinator` arrives first. D4 retargets and calls `submitReconnect`, and `forceReconnect` sends ConnectRequest with seed corr 2.
3. The follower's `NotCoordinator` for InitProducerId (corr 2) arrives on another image or in a later poll while the seed is `Connecting` with corr 2.
4. `machine.onErrorResponse` sees a matching corrId and calls `onConnectError_`, which sets the state to `Disconnected`.
5. On the next tick, the M2-FIX block in `Client.cpp:1827` sees `redirectPending && Disconnected`. It calls `producerHub_.failAll(SessionLost)`, fails pending subscribes, and fires `cbError("Redirect reconnect failed")`.
6. The seed stays `Disconnected`, and the group and txn never recover.

The D4 `Client` branch then returns early through its idempotency guard, so nothing repairs this.

This is racy, but it is terminal and sits on D4's own happy path.

**Suggested fix:** make the `SessionMachine` ignore error codes that a Connect can never receive (it should accept only Redirect, NotLeader, SessionExpired, UnknownSession, ProtocolVersion, HandshakeFailed and the connect-specific codes), or have `dispatchEgressFrame` not route `NotCoordinator` to a machine. Also make the namespaces disjoint, for example a Client corr base of `1<<60`, as leader sessions already do with `kSessionCorrIdStride`. Add a regression test in which the test gateway echoes the request's corrId equal to the seed's next connect corrId (see LOW-5).

---

## MEDIUM

### MEDIUM-1: A late `NotCoordinator` naming the node the seed is already on re-churns a healthy session and forces a full rebalance

- `src/Client.cpp:3947-3964`

The idempotency guard applies only while `redirectPending()`. Answers come on different images (the old follower vs. the coordinator), so their poll order is arbitrary. A follower answer to a request sent before the move (a heartbeat, OffsetCommit, txn op) can land after the coordinator's `ConnectResponse{OK}` cleared `redirectPending`. When it does:

- `onCoordinatorMoved()` sends every Stable member back to `Rejoining`: a full rebalance and generation bump for the whole group.
- `agent_.retarget(sameChannel)` builds a new exclusive publication (it blocks in `pumpUntil_` and drops the per-stream publications).
- `submitReconnect()` tears down the good coordinator session.

**Fix:** if `sameEndpoint(channel, transport ingress)` and the seed is Connected or Connecting, ignore the answer (no `onCoordinatorMoved`, no retarget).

### MEDIUM-2: No pacing or budget on the coordinator redirect; the retarget-failure branch loops at round-trip speed

- `src/Client.cpp:3951-3958`

`onCoordinatorMoved()` runs before the guard and before the retarget. If `retarget` fails (a malformed advertised endpoint, a driver problem), the loop is:

1. Members go to `Rejoining`.
2. The next `doWork` sends JoinGroup to the follower.
3. `NotCoordinator` comes back, which calls `onCoordinatorMoved` again and another blocking `retarget` (up to `kResolveMaxMs`).

That is a tight loop paced only by RTT, with a blocking resolve on the duty thread every time. `scheduleMetadataRefresh()` does not gate it.

If the named coordinator is reachable at resolve time but dead (a stale `leaderView` during an election), the reconnect exhausts its budget. Under the default config that ends in `Disconnected`, which triggers the M2-FIX `failAll` and leaves the seed stranded there. Data-op redirects have the same exposure, but D4 adds a high-frequency trigger: every heartbeat of every member.

**Fix:**
- Call `onCoordinatorMoved` only after a successful retarget.
- On failure, pace with the existing DR-15 mechanism (wait for a fresh metadata answer plus the doubling `redirectedResendBackoffNs_`) instead of re-acting to every answer.
- Consider not arming `redirectPending` for a pure coordinator move. Nothing in `resendAllRedirected` belongs to it, and arming it lets a failed coordinator reconnect `failAll` unrelated producers.

### MEDIUM-3: Re-joining `Joining` members on every seed connect can create ghost members (empty memberId)

- `src/group/GroupConsumerHub.cpp:1564-1570`

Change (2) applies to every seed `ConnectResponse{OK}`, not only to a coordinator move. Consider a member's first join (memberId empty) that the coordinator is holding during a rebalance (`holdJoins`). If the seed resumes or reconnects to the same coordinator, the old JoinGroup was already processed: `GroupCoordinator::join` calls `generateMemberId()` for an empty memberId. Re-sending JoinGroup with an empty memberId then registers a second member.

The ghost never heartbeats. It gets assigned partitions until its session timeout expires, which leaves those partitions unconsumed for up to `sessionTimeoutMs` and costs an extra rebalance.

Before this change the same thing happened only after the 30 s join deadline. Now it happens on every reconnect during a held join.

**Fix:** re-join `Joining`/`Syncing` members only when the coordinator changed (a flag set by `onCoordinatorMoved`, consumed by `onConnected`), or when the old join provably went to a node that is not the coordinator. Otherwise keep the deadline path. Longer term, add a KIP-394-style MEMBER_ID_REQUIRED on the broker.

### MEDIUM-4: The seed moving under an active group orphans or fails plain (non-group) subscriptions

- `src/Client.cpp:2152-2167`
- `SubscriberHub` has no re-subscribe on a new session

Plain subscriptions ride the seed only. Before D4, the seed never moved while a group was active (the MEDIUM-4 rule). Now a coordinator redirect moves it:

- Established plain subscriptions on the old seed session are not re-established on the new session. Their broker-side session on the old node eventually expires and the records stop.
- Pending subscriptions are resent on the new node, get a `Redirect` there, and are failed terminally with `LeaderElsewhereWhileGroupActive`.

A client that mixes a group consumer with direct subscriptions loses those subscriptions silently on bootstrap-on-a-follower. **Fix:** document this as a known limitation and add a ticket, or re-issue established plain subscriptions after a seed move.

### MEDIUM-5: No test coverage for the txn path, crypto, worker routes or a Stable-member failover

- `tests/client/GroupRoutingRegressionTest.cpp:1228`
- `tests/support/InvokerGateway.h:642-653`

The only test covers a new member's first join. Missing cases:

- (a) Transactional producer on a follower: InitProducerId/EndTxn answered with the ErrorResponse, then resent on the new session. The code path does work (`TransactionalProducerImpl::doWork:890` re-sends on any sessionId change, and broker ids are cluster-unique through `idPrefix_`), but nothing tests it.
- (b) A Stable member whose coordinator moves (heartbeat → redirect).
- (c) MAJOR-1 with crypto.
- (d) MAJOR-2 with worker routes.
- (e) A late `NotCoordinator` after the move (MEDIUM-1).
- (f) A retarget failure or unreachable coordinator (MEDIUM-2).

The test gateway's `coordinatorRedirectTo` is wired only for JoinGroup, SyncGroup and Heartbeat, not for txn frames.

---

## LOW

- **LOW-1**, `src/Client.cpp:3948`: the channel is built inline; reuse the file-local `toChannel()` (N2) used by the other redirect paths.
- **LOW-2**, `include/rune/client/Client.h:828`: `coordinatorRedirects_` is written but never read. It has no accessor or metric and no test asserts it. Expose it, as the broker's `coordinatorRedirects_` is, or drop it.
- **LOW-3**, `src/Client.cpp:3947`: `onCoordinatorRedirect_` does not update `currentLeaderNode()`, which stays at the old node or -1. That stale value is copied into `lastKnownLeaderNode` by the next `retargetSeedToBootstrap_`. Resolve it with `metadataCache_.nodeIdForEndpoint`, as the data Redirect path does.
- **LOW-4**, `src/agent/ClientAgent.cpp:1173`: the added `*ch == transport_.ingressChannel()` is dead code. The `sessions_` loop right below already matches the seed, whose `endpoint` is now an exact copy of the transport channel. Separately, the still-present `*ch == cfg_.transport.ingressChannel` check means that, once the seed has left the bootstrap, a worker or node route publication on the bootstrap channel is never released (a small leak).
- **LOW-5**, `tests/support/InvokerGateway.h:648-651`: the redirect ErrorResponse hard-codes `send(0, …)` and `correlationId 0`. The real broker echoes the request's `hdr.correlationId` and answers on the session's own node. This is fine for group frames (corr 0), but it hides MAJOR-3 and cannot model txn. Use `send(endpoint, …)` and echo the request corrId.
- **LOW-6**: `GroupRoutingRegression.BatchHeldOnAConnectingDeadLeaderAsksForMetadataRegression` failed 1 of 4 full-suite runs with an exception thrown in the test body, and passed 3/3 when run alone. It looks pre-existing and resource-related (port or driver dir), not D4, but it should be ticketed so it doesn't mask a real regression.

Notes, not findings:

- Thread ownership is clean. Everything new runs on the duty thread: `syncSeedEndpoint_`, `onCoordinatorRedirect_` and `onCoordinatorMoved`. No atomics or mutexes are added, and `redirectPending` is single-owner.
- Single-node behaviour is byte-identical. `offerControlTo` compares against `transport_.ingressChannel()`, which equals the seed channel. With no routes, the moved `seed.endpoint` has no other consumer.
- Pong routing (by negative corrId), ConnectResponse and HandshakeContinue demux (by connect corrId), and MetadataResponse routing are unaffected.
- The T15a `retargetToBootstrap` / `retargetToLastKnownLeader` identity check is equivalent to the old behaviour and now correct, since a leader session can alias the bootstrap.
- `onCoordinatorRedirect_` allocates a `std::string` inside the noexcept `doWork` → observer chain (bad_alloc would terminate). The data Redirect path already does the same.
