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

## Finding 3 — a deposed controller appends to __cluster_metadata — NOT a safety bug (corrected)

Topic-aware RATE logs after resume (node 1 = zombie, node 2 = new consensus leader): the zombie was still role=L on
`__cluster_metadata` (-3, minIsr=1) and APPENDED 10 records at 20:58:55 while node 2 led it — but its HW stayed 10
(`L(hw10,leo10)` -> appends -> `F(hw10,leo35)` after demotion): the appends were NEVER committed and were truncated
when it became a follower (normal Raft/Kafka behaviour for a deposed leader).

Why a frozen-then-resumed zombie cannot lag-shrink its ISR to itself and commit alone (minIsr=1 notwithstanding):
PartitionReplica resets the working ISR to the COMMITTED ISR every pass (`isrManager_->setIsr(committedIsr_)`); an
internal partition's lag-shrink only takes effect once a majority holds it durably (DUR-1 internal-ISR store,
InternalIsrKnowledge), and the consensus never votes for a node outside the newest durable ISR.  -4
(`__topic_schemas`, self-managed) never lag-shrinks.  The earlier draft's "commits locally" claim was wrong (read
from role=L alone, not the HW).

Remaining (LOW, backlog): the zombie keeps role=L on internal partitions until it hears the new consensus term
(-4 seen ~10 s after resume) — harmless while it cannot commit; observability only.
