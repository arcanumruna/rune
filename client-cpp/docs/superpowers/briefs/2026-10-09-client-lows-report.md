# Client lows batch: report (rune-client-cpp, fix/client-lows)

Worktree `~/Projects/rune-client-cpp-wt18`, branch `fix/client-lows` from `4d4a20a`. Not merged and not
pushed. Mac build: `build/` (x86_64 Debug, configured as the brief says, with `RUNE_PROTOCOL_INCLUDE=/tmp/proto-main/include`
from a `main` worktree of rune-protocol). Linux check: the arm64 `rune-client-dev-arm64:d4` container (Release, gcc 13),
build dir `/tmp/wt18-linux`. No Rosetta hangs.

## 1. A7 client halves

| Item | Status | Commit | Evidence |
|---|---|---|---|
| LOW-1 positive control | done | eff01ab, 35924fb | see below |
| LOW-2 Regression label | done | eff01ab | `InvokerTransportWake.ParkedOwnerWakesOnInboundDatagram` and `InvokerTransportEpollPoll.MultiTransportReceiverDeliversThroughEpollFirst` lose the label. Both are coverage tests that pass on pre-fix code. |
| L5 production accessor | done | eff01ab | `Client::wakeFds` now calls `ClientAgent::wakeFds`, not `testTransport()`. |
| L2 Aeron pin | done | eff01ab | `AeronTransport::kEpollPollAeronPin = {1,41,4}`, the same pin as rune-broker 3763230. With any other runtime Aeron, epoll-first is not installed and a line is logged. RED: with the pin set to 1.41.5, `InvokerTransportEpollPoll.AeronIsTheReleaseEpollFirstWasVerifiedAgainst` fails. GREEN on Mac and Linux. On Linux, `InstalledWhereSupportedAndConfigured` stays green, so the pin matches the runtime. |

**LOW-1 in detail.**
- `PeerDownNeverLeavesWakeFdsReadyAfterAnIdlePassRegression` now has a positive control. When no ICMP error is ever seen, it calls GTEST_SKIP with the reason instead of passing vacuously, like rune-broker 5cc6b16.
- In the Linux container this first version always skipped.
- I checked why. The container does deliver ECONNREFUSED: a connected Python UDP socket sees it. But the driver's next pass takes under 1 µs and consumes the error before the test can see a wake fd readable. In 3 × 350 ms there were about 750k passes and the wake fd was never caught readable.
- 35924fb adds a second witness: `epollSelectedForTest` on the sender poller. These sockets have no peer, so a selection there can only be an EPOLLERR that the pass consumed.
- Linux container: SKIPPED → exercised and green, 3/3. macOS skips, because the path is Linux-only.

## 2. Redirect re-offer LOW-2: done (84dd4bd)

- A refused redirect re-offer now marks only the refused batch and the younger batches behind it (`queueRouteResendFrom_`).
- Older batches are not re-offered. They are either on the route already, or they join the queue when their own Redirect arrives (the MEDIUM-1 join).
- A partition parked on NotLeader still re-offers everything, because the re-offer consumes the park.
- RED→GREEN: `ProducerRoutePark.RefusedRedirectReofferNeverReoffersAnOlderAcceptedBatchRegression`. Before the fix, 6 frames went out; after it, 5. The older batch is no longer re-sent.

## 3. D6 rest

### R2-n1..n5 (task-16 EOS re-review 2 nits)

| Nit | Status | Commit | Evidence |
|---|---|---|---|
| R2-n5 | done | 6935294 | A follower's OOS fast retransmit skipped the MinIsrViolation 100 ms backoff. A batch now carries `retriableBackoff` until its next offer, and the fast retransmit skips it. RED→GREEN: `ProducerRoutePark.FollowerOosNeverSkipsTheMinIsrBackoffRegression` (3 frames before, 2 after). |
| R2-n3 | done | 6935294 | `maybeEndLaneWait_` now ends the wait with the same test that started it: `staleLaneInFlight_` on the waiting head's identity. All live batches share one identity today, so behaviour does not change and there is no RED. |
| R2-n2 | done | 6935294 | One comment line in the C2b section: an old-lane attempt that never gets an answer is re-parked until the delivery timeout. |
| R2-n1 | ruled: already holds | 6935294 (coverage test) | Every D path releases through `releaseResolved_`, which re-arms the parked head once nothing older is unresolved. So the head goes out on the next pass, not on its attempt timer. Pinned by `ProducerRoutePark.DFailureOfTheLastOldLaneHolderEndsTheLaneWait`, which was green before this batch and has no Regression label. |
| R2-n4 | ruled | none | See below. |

**Why R2-n4 is ruled.**
- Today, any MetadataResponse ends the NotLeader wait of a transactional batch.
- A stricter rule ("only a response to a request sent after the NotLeader") would need the Client's metadata correlation order passed down to the txn producer.
- It would also make convergence depend on the urgent-request rate limit actually sending a later request. That breaks the time-independent convergence rule that the R2-1 fix established.
- `ProducerImpl::resendRouteParked` has the same any-response rule.
- The cost of the current rule is at most one wasted re-offer per NotLeader episode, and it corrects itself.

### C-F21: done (377f465)

- The broker answers a push subscription it cannot deliver (F19 / F15 S3) with `ErrorResponse{MessageTooLarge, corr 0, "subscription S topic T ..."}` and parks the subscription. Before this fix, the client only fired `onError` and the subscription stayed active forever.
- Now the named subscription finishes with `failReason() == UndeliverableBatch` (a new enum value) and is unsubscribed, which releases the parked broker subscription.
- Other subscriptions and other ops are untouched, and `onError` still fires.
- RED→GREEN: `HubCommandClient.SubscriptionScopedErrorFailsThatSubscriptionRegression`.
- Not covered: a consumer-group fetch subscription named by the same error still only reaches `onError`. Its subscription id is owned by `GroupConsumerHub`.

### C-F24: done (601b0f9)

- The 11 CB-4 `Subscriber.*` tests used to run a Thread-mode client against a fake broker thread, with mutexes, atomics and sleep_for waits (150 ms, 50 ms, 2 ms).
- They now run on the Invoker runner with the single-threaded `InvokerGateway`. A new `pushBatch` helper pushes caller-built RecordBatches.
- They contain no sleeps, threads, atomics or mutexes. Every wait is a drive loop whose deadline only a failing test reaches.
- New assertions: the wire `fromOffset` (Earliest / Latest / k), and the real subscription id in the late Unsubscribe.
- 20 shuffled repeats are green. This is a fix for a flaky test, not a behaviour change, so there is no RED.

### C-F31: done (ff692ed)

The T15a review items m1–m3, which needed Linux verification:
- **m1:** the cgroup v2 quota is now the tightest `cpu.max` over `/proc/self/cgroup`'s path and every ancestor, not just the root. RED→GREEN: `ShardCountAutoRegression.CgroupV2QuotaIsTheTightestOverTheProcessCgroupAndItsAncestorsRegression`, with the old root-only reading kept in as a temporary RED stub.
- **m2:** `sched_getaffinity` now uses a `CPU_ALLOC` set that grows on EINVAL, and the `{0}` fallback is logged. A >1024-CPU host cannot be reproduced. The Linux container is green for `PlatformCpuSetMatchesSchedGetaffinity`.
- **m3:** the collision check now catches three more cases:
  - a wildcard egress (`0.0.0.0` / `[::]`) against a gateway on this host (loopback or an interface address);
  - names or aliases that resolve to a common address (getaddrinfo, only when the ports already match);
  - a gateway given by its MDC `control=` endpoint.

  RED→GREEN: `ShardConfigRegression.WildcardEgressOrMdcControlGatewayCollisionRejectedRegression`.

### C-F32: done as a guard, with a shrinking baseline (00856de)

- `check_no_atomics_or_mutexes.py --tests` now scans `tests/**`. It excludes the shipped `tests/e2e` probes, which are already checked for futures.
- It uses the same detector plus a SLEEP kind: `sleep_for`, `sleep_until`, `usleep`, `nanosleep` and `::sleep`.
- The existing sites are an exact per-file multiset in `scripts/test_tree_sync_baseline.json`. A new site fails, and a removed site must also be removed from the baseline, so the debt can only go down.
- Permanently allowed: `AllocCounter` and `AeronTestErrors`.
- ctest `NoNewSyncOrSleepsInTestTreeRegression`, plus 9 self-test fixtures. RED: adding one `sleep_for` to a test file fails it.
- **Remaining debt: 369 sites in 31 files** (164 sleeps, 147 atomics, 25 futures). The largest are ClientAgentTest 51, BatchBuilderTest 41 and RedirectTest 37.
- Not converted in this batch, because that is a multi-file rewrite like C-F24. Some race tests (ProducerRace, HandleLifetime) synchronise real app threads on purpose, and need pipes or join in place of atomics, not just removal.

### T13 slow-client test on the old transport: ruled, done in rune-broker

- No client test is pinned to a legacy transport (checked with grep).
- The broker test `ShardModeRegression.SlowEndpointUnawareClientCannotStarveTheShard` lost its legacy-transport workaround in D8c (1803da9 / 2f2c275, per-session produce credits).
- The two D8b tests that remained moved to the default transport in a1df60f.
- Both commits are in rune-broker `main`.

### /dev/shm capacity (192 failures): ruled, not a client issue

- The 192 failures were broker-side: non-sparse 16 MiB terms on node 0 with only 852 MiB of shm free, after the harness had left a 3 GB work dir behind.
- The harness was fixed. D5 / F26 mitigated it: terms are pre-allocated, so a full /dev/shm now fails at publication creation instead of SIGBUS, and 1 MiB terms use 16x less shm.
- The client uses sparse terms (`term_buffer_sparse_file(true)`, 4 MiB, or 1 MiB per shard). Its risk under shm exhaustion is a SIGBUS on page fault, not lost records.
- Backlog, not done here: a client `TERM_SPARSE` knob that mirrors the broker's. The broker measured sparse-term page faults at about 15% of a shard thread. Changing the client default needs its own benchmark A/B.

### Found on the way: IngressEndpointRouting UDP tests (d3314de)

- On the arm64 Linux Release build, 7 `IngressEndpointRouting*` UDP tests failed their connect. This was already the case at `4d4a20a`.
- Cause: they drove for 200k iterations, and real UDP setup plus the connect retry need real time.
- Fix: the default drive now runs to a 10 s deadline that only a failing test reaches.
- Linux: 7 failed → 19/19, three runs in a row. Mac: 19/19.

## Tests

- Mac full `ctest -j6`, final tree: **1075/1075 passed**. The 3 Linux-only tests are skipped: ParkedOwnerWakes, PeerDown, CpuAffinity.
- Linux arm64 container (Release), full `ctest -j6`: **1080/1080 passed**. The 2 debug-only death tests are skipped. PeerDown is exercised, not skipped.
- To get the container build to link, I mounted the current `rune-ql`. The old `rune-ql-base` mount in `rune-dev/cdevrun.sh` lacks `AlterTopicDef`.

## Concerns

- **The broker's peer-down test has the same weak witness.** rune-broker 5cc6b16 likely skips in container CI for the same timing reason (the next pass consumes the error), not because ICMP is never delivered. The `epollSelectedForTest` witness from 35924fb would make it run there too. That is a broker change, so I did not make it.
- **C-F21 does not cover group fetch subscriptions.** It fails only plain subscriptions.
- **C-F32 debt remains:** 369 test sites, now ratcheted.
- **The rune-protocol `main` worktree at /tmp/proto-main is temporary.** It was used for both builds.
