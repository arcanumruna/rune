# Brief: D6 — UNKNOWN_PRODUCER_ID (protocol + broker + client)

Worktrees (all created for you): rune-protocol /Users/vradu/Projects/rune-protocol-d6 (branch
feat/unknown-producer-id); broker /Users/vradu/Projects/rune-broker-wt34 (branch feat/d6-unknown-pid, from integ);
client /Users/vradu/Projects/rune-client-cpp-wt10 (branch feat/d6-unknown-pid, client incl. B/D7/GSO).
Build broker and client against -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/rune-protocol-d6/include.
Commit locally; NEVER push; never run any conan package command (create/export/upload are forbidden). Do not spawn
subagents. Another agent works on replication in rune-broker-wt31 — stay out of PartitionReplica/replication; the
broker change is in ProducerStateManager::validate + the produce paths (gateway + worker) only.

## Spec (from the F20 PSM/log-consistency report — binding)
**Title:** UNKNOWN_PRODUCER_ID — a producer the broker no longer knows (retention / compaction / memory-ring
eviction removed all its batches) must not be failed with OutOfOrderSequence.

**Today (correct but unfriendly):** after retention (or a DropOldest memory ring) deleted every batch of
producer P from [logStart, LEO), a rebuilt producer state has no entry for P; P's next batch (seq k > 0)
meets trust-on-first-use and is answered OutOfOrderSequence.  The batch really was not appended, so the
contract holds, but the client's rule C4 fails it (BrokerError 20) and re-sequences everything behind it
— a spurious failure.  (The live state keeps P across retention; only a rebuild — failover, restart,
re-promotion — forgets it.)

**Wire (rune-protocol, `client/Protocol.h`):** add `ErrorCode::UnknownProducerId = 28` (Kafka
UNKNOWN_PRODUCER_ID, 59) and carry the partition's `logStartOffset` (int64) in `ProduceResponse`
(Kafka PartitionProduceResponse.logStartOffset) — needed by the client to tell retention from loss.

**Broker rule (ProducerStateManager::validate + both produce paths):** only on a PROVEN state
(the gates added here).  No entry for (topic, pid, partition) and baseSequence != 0 → UnknownProducerId
with the current logStartOffset (instead of OOS).  Everything else unchanged: no entry + seq 0 → accept;
known entry + forward gap → OOS; epoch rules unchanged.  Unproven state → NotLeader / Internal as now.

**Client rule (rune-client-cpp ProducerImpl, Kafka TransactionManager.canRetry / KIP-360):** new rule U,
applied like C to the CURRENT attempt of batch b (stale-attempt answers ignored, rule A):
* U1. `lastAckedOffset(partition) < response.logStartOffset` (every record this identity had acked is
  below the log start — its producer state was legitimately removed) → b was NOT appended: re-sequence the
  partition exactly as C3 (new identity per rule F: epoch bump if on the current one, sequences from 0;
  b and every batch behind it re-stamped and re-sent) — nothing fails.
* U2. otherwise (the broker should still know us, or nothing was acked yet and logStartOffset == 0) →
  outcome UNKNOWN (rule D: b fails, partition unresolved, rule E resolves it).
* Track `lastAckedOffset` per partition from OK answers (the max acked lastOffset; -1 = none).  A
  known-old OK -1 does not move it.
* Until both sides ship: the broker keeps answering OOS (current behaviour); a new broker must not send
  28 to an old client — gate it on the client version in ConnectRequest (clientVersion) or ship the client

## Rulings
- Version gating: the broker sends UnknownProducerId (28) only to clients whose ConnectRequest clientVersion says
  they understand it; older clients keep getting OutOfOrderSequence. Add the client version bump.
- ProduceResponse gains logStartOffset (int64): additive wire field — old decoders must keep working (check the
  codec's versioning/blockLength handling and add a compat test).
- Only on a PROVEN producer state (the existing proof gates); unproven state behaves exactly as today.

## Tests (RED-first, `*Regression*` where they pin the bug)
Broker: rebuilt PSM after retention removed all of P's batches -> seq k>0 answered UnknownProducerId + logStartOffset
(new client) / OOS (old client); seq 0 accepted; known entry + gap still OOS; epoch rules unchanged.
Client: rule U1 re-sequences without failing anything (b and everything behind re-stamped, new identity per rule F);
U2 -> outcome unknown (rule D); lastAckedOffset tracking; stale-attempt answers ignored.
One in-process end-to-end test (real broker + real client in the client repo's broker-model / InvokerGateway or the
broker's integration harness): retention + failover -> producer continues with no spurious failure.
Full Mac suites green in all three repos; check_no_atomics OK. Hot path: no allocation/virtual/std::function.

Report: /Users/vradu/Projects/rune-broker-wt26/docs/superpowers/briefs/2026-10-03-D6-report.md. Return only:
status, commit SHAs per repo, one-line test summary, concerns.
