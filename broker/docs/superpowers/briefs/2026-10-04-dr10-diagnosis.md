# DR-10 (zombie leader, short pause) — diagnosis 2026-10-04

Test: `dr-e2e/tests/test_dr10_zombie_leader.py::test_dr10_short_vs_long_pause[short]`.  Box (serial run): committed
loss 116.  Local (Docker Desktop, images broker 4e669cf → df7fcef, client 3c7e97b): 5/6 fail before the fix (runs were
first contaminated by a concurrent agent docker run sharing compose project rune-dr-fault — discarded).

## Finding 1 — a frozen broker crashed on resume (FIXED 4dbc0fc)

The paused ex-leader exited 255 "client timeout from driver" (`aeron::ClientConductor::onClientTimeout`): the test
keeps the leader paused through its checks (≫ 10 s); on resume the in-process driver timed out its own in-process
client (Aeron default liveness 10 s).  Fix: every embedded driver's client liveness defaults to 1 h (the client's
in-process value), in-process clients' mediaDriverTimeout follows, timer < liveness < unblock derived.  Regression
`ShardModeRegression.BrokerSurvivesAProcessFreezePastAeronLivenessDefault` (forked broker, 12 s SIGSTOP).

## Finding 2 — the "SPLIT-BRAIN" probe is a false positive under per-partition leadership (harness)

With the crash fixed the test fails `assert_zombie_cannot_serve_write`: a produce "to the zombie" (acks=all, 5
records) succeeds.  Topic-aware RATE logs (df7fcef, RUNE_FS_LOG=1) prove the 5 records landed on the REAL leaders —
the zombie made NO user-topic leader appends after resume and stepped down from every user partition at resume
(`FollowerSync ... becomeFollower epoch=1/2 leader=2/0`).  The probe uses the zombie only as a bootstrap; the client
routes per partition (Milestone D).  Harness fix: the probe must address the zombie's OLD partitions directly (no
routing / redirect following) and require NotLeader, or assert on broker-side leader appends.

## Finding 3 — a deposed controller COMMITS to __cluster_metadata (minIsr=1) — REAL, open

Topic-aware RATE logs after resume (node 1 = zombie, node 2 = new consensus leader):

- `__cluster_metadata` (topic -3, minIsr=1): node 1 still role=L and APPENDED 10 records at 20:58:55 (hw 10)
  while node 2 led it (hw 35); node 1 demoted later (F hw10 leo35).
- topic -4: node 1 still role=L at 20:59:05 (>10 s after resume) alongside node 2.
- `__consumer_offsets` (-1, minIsr=1) and `__transaction_state` (-2): node 1 L alongside node 2 in the resume second.

Mechanism: the resumed zombie still believes it is the consensus leader until it hears the higher term; its metadata
service keeps emitting into `__cluster_metadata` as leader, and with minIsr=1 (BrokerNode.cpp:2102-2105, "leader-local
in Phase 1") those records COMMIT locally without any majority → a divergent committed metadata view (assignments
served to clients / applied to its own roles) until truncation.  Same exposure for `__consumer_offsets` (minIsr=1,
BrokerNode.cpp:1853-1858).  KRaft commits metadata only by Raft majority.

Fix direction: control logs commit only with a majority (minIsr >= majority of the replica set) so a deposed leader
can never commit; check what the "ISR-establishment delay" concern behind minIsr=1 needs instead (the new leader's
ISR = live set from the consensus election, not ISR re-growth).  RED test: in-process 3-node, isolate / stall the
consensus leader, let it emit metadata, assert nothing it appended becomes committed (HW does not move) and its
metadata view never applies an uncommitted assignment; after heal it truncates to the new leader's log.
