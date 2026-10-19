# Follower-connect hang — diagnosis and fix (2026-10-14)

Found by Task 15 fix round 1 (R1). A real Client (Thread mode) seeded on a NON-leader broker of a
3-node in-process cluster never connected. Seeded on the leader, it worked.

## Reproduction

`ClientConnectRoleRegression.EveryBrokerOfAnEmptyClusterHostsTheSessionRegression`
(`libs/node/tests/client/ClientConnectRoleRegressionTest.cpp`) connects a real Client to each broker of a
`ThreeNodes` cluster with a bound. Before the fix, broker 1 (the consensus leader) connected. Brokers 2 and 3
failed with `connect(): timed out` after 5.03 s, which is the client's `connectTimeoutMs`.

I added temporary gateway logging (since removed). For each follower it showed:

```
node=2 handleConnect corr=1 routingImage=false isLeader=false ep='aeron:udp?endpoint=127.0.0.1:<node 1>'
node=2 sent Redirect ...           (x6: corr 1..6, all to node 2, none ever reached node 1)
```

Hypothesis check: the same cluster with ONE static topic configured has a routing image from the first
cycle. There, all three brokers accepted the session (`routingImage=true isLeader=true`).

## Root cause

Two defects combine. Either one alone is enough to break the connect:

1. **Broker: the session gate is inferred from whether a routing image exists.**
   `ClientGateway::handleConnect` (`libs/gateway/src/ClientGateway.cpp:1898` at HEAD) runs the pre-MD1
   "only the Raft leader hosts sessions" gate whenever `!routingImage_`. It treats a missing image as a
   short bootstrap window. But `NodeRuntimeAgent` (`NodeRuntimeAgent.h:546-566`) publishes an image only
   when there is a committed USER assignment or a static topic. A cluster whose topics come only from DDL
   (this branch's model; `ThreeNodes` has no static topics) therefore has no image at all until its first
   user topic commits. Until then, every non-Raft-leader answers every Connect with
   `ErrorResponse{Redirect, Raft leader}` (`ClientGateway.cpp:2286`) and creates no session.
2. **Client: nobody follows the seed's connect-time Redirect.** The seed's `SessionMachine`, in
   Connecting, calls `io_.retarget(endpoint)` and resends (`SessionMachine.h:582-591`). The seed's IO is a
   `SessionEndpointIo`, and its `retarget` was a no-op for every session (`src/agent/ClientAgent.cpp:1002`
   at HEAD, Task 1.3). Meanwhile `Client::onEgressFrame` deliberately leaves a connect-time Redirect to the
   SessionMachine (`src/Client.cpp:3001`, `if (!haveWork) break;` from DR-BUG-2). So every resend went back
   to the follower. After `maxRedirects` = 5 (1 + 5 = the 6 Connects logged), `onRetryBudgetExhausted_`
   moved the machine to Disconnected with no signal. The pending connect then waited out its deadline and
   completed `Timeout`.

The Task 15 note said the client waited "40 s". In fact the connect ended at the client's connect timeout.
What never happened was the connect succeeding.

## Pre-existing?

Yes, both defects are on the merge-base `feature/materialized-views`:
- The broker gate `if (leaderView_ && !routingImage_)` is at line 1877 of `ClientGateway.cpp` at
  `80aa3bfd`. The image-publishing code in `NodeRuntimeAgent.h` is unchanged on this branch, apart from
  Task 8's unrelated retire-gen lines.
- The client no-op `SessionEndpointIo::retarget` comes from Task 1.3 (`8c50f57`). This branch changed only
  21 unrelated lines in `src/agent`.

This branch exposed the bug because its clusters get their topics through DDL, so they start with none.
Task 11's `connectSettled` in `CatalogLeadershipBrokerTest.cpp` had already hit the gate ("a broker that is
not the consensus leader redirects connects ... retried") and worked around it by retrying.

## Fix

**Broker** (hard rule 3: an explicit state, not an inference):
- `ClientGateway::SessionAdmission { ClusterLeaderOnly, AnyNode }`, set once before `doWork`
  (`setSessionAdmission`). The default is `ClusterLeaderOnly`, so a bare gateway keeps the BR-5 behaviour
  that `GatewayRedirectTest` and `GatewayDataOpRedirectTest` pin.
- `handleConnect` applies the Raft-leader gate only under `ClusterLeaderOnly`.
- `BrokerNode::buildGateway` sets `AnyNode` once its MetadataService is wired, because metadata-driven
  routing is in place. From then on, every broker hosts the session in every role. Data ops still route
  per partition (Path A, or Path B on an image miss) and DDL still routes to the catalog leader (Task 11),
  so only the op is ever redirected, never the Connect. If the MetadataService setup fails, the node keeps
  `ClusterLeaderOnly`.
- No allocation and no shared state: a 1-byte field written once before the duty cycle starts.

**Client:**
- The seed follows a connect-time Redirect. `SessionEndpointIo::retarget` on the SEED (checked by
  identity) calls `ClientAgent::retargetSeedOnConnectRedirect_`. That method accepts either `host:port` or
  a full channel and retargets the default publication. It updates `seed.currentLeaderChannel` and resets
  `currentLeaderNode` to unknown. A failed retarget keeps the old publication, and the resends stay bounded
  by the redirect budget. Leader sessions keep the no-op. This is a cold path, the same as the existing
  bootstrap and stick-to-leader retargets.
- Rule on the class: a pending connect whose seed machine gave up (Disconnected after the redirect budget
  ran out, after a connect error, or after its own timeout) now completes `NotConnected` right away in
  `Client::sweepControl_`, with an onError message. Before, it waited out the deadline in silence.
  Backoff (reconnect-forever) still retries and is still bounded by the deadline.

## Ruling on the whole class: every connect, every role

| broker role | after fix |
|---|---|
| consensus / catalog leader | hosted there, OK |
| follower (empty cluster, no image) | hosted there, OK (was: Redirect never followed → timeout) |
| follower, DDL on that session | hosted there; DDL Redirected to the catalog leader, OK |
| catching up (restarted from an empty dir) | hosted there at once, and again once back in the ISR |
| halted (catalog applier halted) | hosted there (the halt refuses DDL, not connects) |
| lone survivor below minIsr | hosted there |
| stopped (nothing answers) | ends explicitly `timed out` at connectTimeoutMs |
| a gateway that still Redirects the Connect (ClusterLeaderOnly) | client follows it; a redirect loop ends `NotConnected` when the budget runs out |

## Tests (all `*Regression*`)

Broker, `rune_client_broker_tests`, new file `ClientConnectRoleRegressionTest.cpp`:
- `EveryBrokerOfAnEmptyClusterHostsTheSessionRegression`: RED (both followers timed out), now GREEN.
- `AFollowerHostedSessionRunsDdlThroughTheRedirectRegression`: RED (follower timed out), now GREEN.
- `ACatchingUpBrokerHostsTheSessionRegression`: RED (timed out twice), now GREEN.
- `AHaltedBrokerHostsTheSessionRegression`, `TheLoneSurvivorBelowMinIsrHostsTheSessionRegression`,
  `AStoppedBrokerEndsTheConnectExplicitlyAtTheTimeoutRegression`: GREEN both before and after the fix.
  The event that halts the broker creates a topic, so the image exists. The survivor saw "leader unknown",
  which already fell back to local accept. These tests pin the remaining roles.
- Each session is also checked to be hosted on the broker it connected to, using the SessionTable id
  prefix `(nodeId+1)<<40`. With the broker fix mutated out and the client fix kept, the client converges
  on the leader and these tests fail on "must host the session itself". So the broker fix is required on
  its own.
- `RuneCliBrokerRegression.ReplayAndQlIntoAFollowerSeeTheirOwnWritesRegression` (R1) seeds on a FOLLOWER
  again. RED with both fixes mutated out (replay exit != 0), now GREEN.

Client, `rune_redirect_tests`, new file `tests/client/ConnectRedirectRegressionTest.cpp` (two fake gateways
on separate UDP endpoints):
- `TheSeedFollowsAConnectTimeRedirectToTheNamedGatewayRegression`: RED (A saw 6 Connects, B saw 0, timeout),
  now GREEN.
- `AConnectWhoseRedirectBudgetRunsOutEndsNotConnectedAtOnceRegression`: RED (status Timeout after 30.06 s),
  now GREEN (NotConnected in about 0.4 s).

Affected-area runs: the client run (`SessionMachine|ClientAgent|MultiNodeRouting|SessionLiveness|...|RuneCli|...`)
passed 637/637. The broker run
(`Gateway|Redirect|Connect|Catalog|BrokerNode|Failover|ClusterDisasterRecovery|...|ClusterMember`) is
recorded in the commit message. The full suite and the sanitizers are not run here; they belong to the
next checkpoint.
