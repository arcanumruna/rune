# Brief: E1d — durable recovery point (async-seal power-loss gap)

Worktree: ~/Projects/rune-broker-wt32 (branch fix/e1d-recovery-point, from feat/dur-1 f153662 — it has
the DR harness and the disabled acceptance test). Commit locally; NEVER push; never run any conan package command
(create/export/upload are forbidden). Do not spawn subagents. Configure your own build-fast (copy the configure line
from ~/Projects/rune-broker-wt31/build-fast/CMakeCache.txt). Another agent is concurrently reworking
replication divergence in wt31 (PartitionReplica / fetch protocol) — stay in storage + the cleaner + recovery; do
not touch PartitionReplica fetch/divergence logic.

## The bug (confirmed by two reviews)
Production seals segments asynchronously (deferSealSync_: the sealed segment's fdatasync runs later via the io
backend). On power loss before that fsync completes, Log::recover() (Log.cpp ~574-578) sets the LEO from the LAST
segment's recover(): an empty/short later segment makes the log claim offsets it does not hold (e.g. LEO 108 with
records only to 80) -> the replica reports that LEO, gets ISR/HW credit for missing records, fetches from 108 and
leaves a permanent hole. Acceptance test (currently disabled): DISABLED_PowerLoss_AsyncSealNotCompleted_NoOffsetGap
in libs/replication/tests/dr/. A naive "truncate at any gap between segments" is WRONG: compaction legitimately
leaves offset gaps, and the cleaner checkpoint can lag its file renames.

## Binding design (Kafka recovery-point parity)
1. Durable per-Log recovery point R: advanced IN ORDER when a pending seal's fdatasync completes (R = next segment's
   base) and when an active-segment flush completes (R = synced LEO). Persisted with tmp + fsync + rename + dir
   fsync, batched by the owning worker (never on the produce hot path per append; a stale, lower R is always safe).
   Also written on clean shutdown.
2. Recovery: segments ending at or below R are trusted as-is (compaction gaps there are fine). From the segment
   containing R onward: CRC-recover each segment AND require each segment's recovered next offset == the next
   segment's base. At the first mismatch or torn tail: truncate there and DELETE every later segment (Kafka). That
   point is the LEO (flows through isRecovered(), the epoch-cache clamp, and the HW clamp).
3. Cleaner invariant: only compact segments that end at or below R, so compaction never creates gaps in the region
   recovery validates.
4. Missing R (upgrade / first boot): treat R as the last sealed segment's end and log a WARN.
5. Check and state in the report: a follower's acked LEO for acks=all never exceeds its synced LEO or R (tie-in
   with KIP-966 / E1b) — if it can, write that down as a follow-up, do not redesign replication here.

## Rules
RED-first: enable the disabled acceptance test (it must fail first), plus `*Regression*` storage tests for: torn
tail after R, empty later segment, compaction gap below R (must NOT truncate), stale R (lower) recovers correctly,
missing R file, crash between data fdatasync and R persist. Deterministic, no sleeps; use the storage fault seams
(setSyncFaultForTest / setWriteFaultForTest) and the DR power-loss seam (syncedExtentsForTest). Hot path: no
virtual calls, std::function, allocation; no atomics/mutex outside ring internals and the cleaner
(scripts/check_no_atomics.py OK). Full Mac suite green (every test binary the branch builds), DR suite 30x repeat.

Report: ~/Projects/rune-broker-wt26/docs/superpowers/briefs/2026-10-03-E1d-report.md (design as built,
RED->GREEN per test, suite results, follow-ups). Return only: status, commit SHAs, one-line test summary, concerns.
