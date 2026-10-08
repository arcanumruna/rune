# D11 / D8w / D8w2 / D8b-L / D8 flakes: report (2026-10-07)

Branch `fix/d11-flakes` (wt60), based on integ/perf-defaults 70217bd.

**Load used.** `ctest -j6 --repeat until-fail:N` plus 12 `yes` CPU hogs, on a shared 18-core Mac. Other agents were also building, so load averages ran from 20 to 46.

**Baseline.** A full `ctest -j6` on the base was green: 3945/3945, 0 failures.

**Discovery run (s1).** All targets were run 20x each under that load. Three failed, each once:
- `GR5_Integration.AcksAll_Deferred_HWGate`
- `ConsensusCandidacyElectionRegression.StaleKnowledgeLearnedThroughExchange_SurvivorsElect`
- `ShardModeRegression.PausedInvokerClientSurvivesNotConnectedRegression`

**Coordinator stop.** The machine reached load 154 and the coordinator stopped all stress loops. The ×100 verification (s3) was cut short. It had finished 100/100 green for GR5 ×3, GatewayReplicationWiring.ThreeNode and Candidacy.StaleKnowledge, and 22/22 green for PausedInvoker. It had not yet reached the other tests. No full-suite run was done after the fixes. The changed tests were run once at -j4: 65/65 passed.

All fixes below are **test bugs**, except D8b-L LOW-1, which is a small product fix. No product sleeps, timeouts or retries were added. `check_no_atomics`: OK, with allowlist counts unchanged.

## Per test

| Test | Reproduced? | Root cause | Fix | Before → after |
|---|---|---|---|---|
| GR5_Integration.AcksAll_Deferred_HWGate | **Yes**, 1/15 in s1 ("No ProduceResponse") | The test sent its produce to the **consensus** leader. Under load node 1 won the election, but partition 0 stayed on node 0 (static assignment). The non-leader gateway answers an ErrorResponse (Redirect), and the test never decodes that. | New `settledPartitionLeader()` (OwnerThreadTestHelpers.h) picks the node whose replica role is Leader on its owner thread, with all other nodes Follower. It is a condition wait. The 300 ms sleeps are removed. (b88bcaa) | 1 fail/15 → 100/100 |
| GR5_Integration.AcksAll_SingleRecord_NotAckedAtHwZero_GR9 (D11) | Not in this run (20/20) | Same defect: it uses the consensus leader as the partition leader. | Same fix. | 100/100 |
| GR5_Integration.MinIsr_FollowerKilled_Refuses | n/a (same code) | Same defect. | Same fix. | 100/100 |
| GatewayReplicationWiring.ThreeNode_FollowersConverge_HWAdvances | Not in this run (20/20) | Same defect: the consensus leader's gateway was used for the produce. | Same fix. | 100/100 |
| ConsensusCandidacyElectionRegression.* (D11) | **Yes**: StaleKnowledge failed 1/18 with "ClientGateway construction failed: Address already in use 127.0.0.1:57713" | The test file had its own bind-0 helper. It returned an **ephemeral** port that another parallel test process then bound (the D8 class). | Ports now come from `allocFreeUdpPort()`. The same fix is applied to every remaining bind-0 helper: GatewayProduceCredit, BrokerNodeTest ×2, WorkerPushTest, EmbeddedDriverTest, AeronPumpInvokerTest. (18457de) | 1 fail/18 → StaleKnowledge 100/100. DeniedCandidacy and Excluded were 20/20 in s1; the ×100 run was cut before reaching them. |
| WorkerLargeBatchPushRegression.* | Not reproduced (20/20) | Found by inspection: fixed ports 24910/24911/24920/24921 lie inside the shared allocator's range (20000–31999), so they can collide with any concurrent test. WorkerEgress and MdcEgressSpike had the same problem (247xx), and BrokerNodeTest used 40121/40122, which is inside the Linux ephemeral range. | All replaced with allocator ports. (18457de) | 20/20; ×100 not run (stopped) |
| ShardModeRegression.PausedInvokerClientSurvivesNotConnectedRegression | **Yes**, 1/18: "the worker holds A's acks" was never true | The pause predicate counted every gateway produce abort. `heldProduceAborts` also counts D8c credit and worker-cap pauses, which hold nothing: at most 1024 acks, which fit in the client's egress window. Under load the worker lags, so the credit pause comes first. Diagnostics showed the Killed test's `pending > 256` alternative firing on a transient backlog: 256 pending, 0 hold aborts, drained moments later. | Both paused-client tests now wait for a **hold** abort only (all − credit − workerCap). Deterministic repro `PausedInvokerCreditPauseIsNotAHoldRegression` (a 1-credit gateway) is RED 3/3 on the old predicate and GREEN after. (09a80be, 51312d6) | 1 fail/18 → 22/22 + 90/90 (stress2) before the stop |
| TX2b_GatewayDriven.FailoverDedup_HandleProducePath (D8 unreproduced) | Not reproduced (20/20) | Found by inspection: the same consensus-vs-partition-leader defect, both before the kill and after it (the new consensus leader was assumed to be the new partition leader, with a 5 s poll). | `settledPartitionLeader` over all nodes, then over the survivors. (fd4374e) | 20/20 before; after, 3/3 alone and 1/1 at -j4 |
| BrokerNodeRegression.DefineTopicValidatorReachesEveryWorkerEndToEndRegression (D8w2) | **Not reproduced** (20/20 under load) | Unknown. Suspects (wall-clock 20 s / 5 s bounds, 10 s session reap, 20 ms backoff) are unchanged. | None → back to backlog | — |
| LargeBatchReplicationRegression.* (D8w, 5 node-level tests) | **Not reproduced** (20/20 each) | Unknown. | None → back to backlog | — |
| CompactionRetentionSmoke.IdempotentDedup_SurvivesRetention (D8) | Not reproduced (20/20) | — | None → back to backlog | — |
| ShardModeFusedRegression.ThreeNodeAcksAll / PartitionLeaderFailsOver (D8) | Not reproduced (20/20) | — | None → back to backlog | — |
| InvokerTransportCrc.* | Not attempted | This test lives in **rune-client**, not in this broker worktree. | Out of scope → backlog (client repo) | — |

## D8b-L lows

- **LOW-1 (product, f2ae2c6).** A Found from one worker now restarts `lostSinceNs` while another worker is still Lost. A Found is a successful claim, which proves the client is alive. A dead client never sends Found, so this can never extend a dead session's life.
  - New test `GatewayShardNotifyRegression.FoundOnOneWorkerRestartsTheLostLeaseOfAnotherRegression`: RED before the fix, GREEN after.
  - The `both` case of the existing Lost-lease test is updated to the new semantics. It was also RED before.
- **LOW-2 (f2ae2c6).** The Progress-rate comment is corrected: the rate is pass-rate/1024, not a fixed ~10/s.
- **LOW-3 (benchmarked, not folded).** `BM_SessionAckSinkFlush`, 3 interleaved A/B rounds × 5 reps, medians. The comparison variant compiles out the `edges_` touch:
  - Cost of the touch: +0.25–0.35 ns per ack (~2.5–3.5%). Examples: 2501 vs 2439 ns for 256 acks / 16 sessions; 13.0 vs 12.7 ns for 1/1.
  - That bench uses a bump-pointer claim. With a real Aeron claim the share is below 1%.
  - Folding a watched bit into `PubReady` touches 16 mutation sites plus the variant state transitions. That is a correctness risk on the ack hot path, so it goes to the backlog with these numbers.
- **LOW-4 (51312d6).** The real-driver kill test now asserts two more things:
  - another session on the same worker is served **while** A pins acks;
  - A's gateway image is released. This uses the new test accessor `ClientGateway::clientImageCountForTest`.

  Still open → backlog: the Invoker resume-without-reading path (BACK_PRESSURED → Found, not reaped 10 s after Lost) and a real-driver MEDIUM-1 test. Both need time-crossing setups and are not small.

## Not done (stopped by the coordinator)

- The ×100 runs for WorkerLargeBatchPush, Candidacy Denied/Excluded, PausedInvoker (only 22 reached), Killed and TX2b.
- The full `ctest -j6` after the fixes.
