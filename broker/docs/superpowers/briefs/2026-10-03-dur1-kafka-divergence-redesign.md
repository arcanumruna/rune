# Brief: Kafka-faithful replica divergence handling (replaces DUR-1's heuristic layer)

Worktree: /Users/vradu/Projects/rune-broker-wt31 (branch feat/dur-1, HEAD f153662) for the broker; rune-protocol
changes in a NEW worktree you create: `git -C /Users/vradu/Projects/rune-protocol worktree add -b feat/fetch-epoch
../rune-protocol-fe HEAD` and build the broker against it with -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/rune-protocol-fe/include.
Commit locally; NEVER push; never run any conan package command (create/export/upload are forbidden). Do not
spawn subagents. Configure a build-fast like the existing one in wt31.

## Why (read first)
- /Users/vradu/.claude/jobs/303105aa/tmp/E-report.md — what DUR-1 built (audit, DR suite, fix round 1).
- Two Opus reviews found that the heuristic layer added in b7d877f/f153662 (probe credit for ISR/HW, "wedged"/
  "diverged" follower flags, leader ADOPTION of a survivor's committed prefix across epochs, reconcile pulls at
  LEO-1 with byte-identical boundary matching) keeps producing correctness/liveness holes. Reproductions:
  /tmp/durrev2/Repro2.cpp (stock DrHarness.h) and /tmp/durrev2/Repro3.cpp (harness copy with maxFetchBytes=1),
  plus /tmp/durrev/Repro.cpp. Every one of them must become a `*Regression*` test in the DR suite and pass.
  Review findings to satisfy (all): adoption stall forever when the boundary batch differs/compacted; reconcile
  livelock when the boundary batch uses up the fetch budget; same-epoch re-assert (partitionEpoch-only ISR change)
  clears wedge/diverged/adoption state; ungated METADATA path (MetadataService append(acksAll=false) on an
  incomplete leader -> committed metadata record lost); classic KIP-279 same-length tail divergence (pre-existing);
  TCS/GCS keep stale in-memory state when a complete leader re-enters the completeness gate; probe-fallback plain
  fetch defeats lineage checks; boundary check compares bytes only.

## Binding design (Kafka parity — controller ruling)
1. Every record batch carries its REAL partitionLeaderEpoch (Kafka RecordBatch field). Leader stamps it on append
   (produce, control/markers, metadata, internal keyed stores); replicated batches keep it (no more -1 stamps);
   the follower epoch cache is built from the batches' stamped epochs (assign at the first batch of each new
   epoch), exactly like Kafka's LeaderEpochFileCache.assign on append.
2. Fetch requests carry `lastFetchedEpoch` (KIP-320/595) — additive field in the rune-protocol replication fetch
   message (SBE schema + generated codec; keep old readers working: absent = -1 = no validation). The leader
   validates EVERY fetch: if the follower's (fetchOffset, lastFetchedEpoch) is not on the leader's lineage it
   answers DivergingEpoch{epoch, endOffset} (= leader's endOffsetForEpoch(lastFetchedEpoch)); the follower
   truncates to min(endOffset, its own end offset for that epoch) and refetches (KIP-279 loop). A fetch is credited
   for ISR/HW ONLY after it passed validation. Prefer fetch-path validation over side probes.
3. REMOVE leader adoption (H1c), the probe-credit / wedged / diverged heuristics and the LEO-1 reconcile pull.
   A leader never takes records it did not have when elected. Completeness (isLeaderComplete) stays as the B1 write
   gate, but it is satisfied by ISR soundness, not by pulling data:
4. ISR soundness (KIP-966 data-plane): a replica is in the ISR only if it holds every committed record on the
   leader's lineage. A follower restarted after an UNCLEAN shutdown (recovered LEO < its pre-crash acked/HW
   position, or any recovery truncation) must not be counted for ISR/HW/minIsr until it caught up from the leader
   (validated fetches). The leader-elect path must not elect such a replica while a clean ISR member exists — if
   that needs controller-visible state that requires a protocol field, implement what can be done in the data
   plane and write the rest into the E1b note (do not invent another mechanism).
5. Gate the METADATA path (MetadataService) and TCS/GCS: no append/emit while the replica is not leader-complete;
   queue records until it is (like TCS loadable_); TCS/GCS unload (or reload) when completeness/log generation
   changes. Never ignore an append result.
6. Same-epoch re-asserts (partitionEpoch-only ISR changes) only apply the ISR: no slot/state reset unless
   leaderEpoch increased or the role changed.

## Rules
RED-first `*Regression*` tests (port every review repro first, show it failing on f153662, then green).
Deterministic tests, no sleeps. Hot path: no virtual calls, std::function, allocation; no atomics/mutex outside ring
internals and the cleaner (scripts/check_no_atomics.py OK). Keep the DR suite (rune_dr_integration_tests) and all
its existing scenarios green; 30x repeat at the end. Full Mac suite green (every test binary the branch builds).
Out of scope: the async-seal power-loss recovery point (E1d) — another agent does it; do not touch Log::recover.

Report: /Users/vradu/.claude/jobs/303105aa/tmp/R-report.md (design as built, what was removed, RED->GREEN per
review finding, protocol change, suite results, remaining E1b items). Return only: status, commit SHAs per repo,
one-line test summary, concerns.
