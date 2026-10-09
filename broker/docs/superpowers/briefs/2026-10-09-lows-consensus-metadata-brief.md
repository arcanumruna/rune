# Lows batch A: consensus and metadata

Worktree /Users/vradu/Projects/rune-broker-wt70, branch fix/lows-consensus-metadata (from integ/perf-defaults
4b0b6a6).

## Items
1. **D18: DR-10 review lows.** Details in docs/superpowers/briefs/2026-10-06-dr10-flap-review.md.
   - LOW-1: the MetadataService rebuild trusts committedEnd_ without checking the truncation point. Assert, or
     handle a cut below it.
   - LOW-2: gracefulClosedLeaderId_ persists across elections.
   - LOW-3: duplicated quorum helper: Election::hasActiveFollowerQuorum_ vs ClusterMember::hasActiveQuorum. Check
     the current code; it may already delegate.
   - LOW-4: the behaviour change for a 2-member cluster. Document and test it.
   - LOW-5: the DR-10 oracle's late-election branch settles only 4 s (dr-e2e harness; Python; no Docker run). Make
     it condition-driven.
   - LOW-6: the adopted leadershipTermId is kept in memory only, while the candidate term is durable. Evaluate it
     against the D24 durable follow floor (NodeStateFile slot 24), which may already cover it; rule or fix.
2. **D15b: lows from docs/superpowers/briefs/2026-10-07-d15-review.md.**
   - l1: with more than 256 restored marks at a claim, the first rebalance filters only inflight_, not
     pendingDynamicRecords_.
   - l2: a unit larger than the remaining record budget waits for the next event-driven tick; add a forced re-tick.
   - l5: NO_LEADER records written by main controllers carry no mark.
   - m4: the docs for the MSM snapshot/restore featureLevel + marks. Document only; it is a sibling-protocol item.
     Do not edit rune-protocol; write the needed doc text into the report.

## Global rules (binding)
- Rune broker, C++23, Aeron-based. Read CLAUDE.md at the worktree root first.
- Bug fixes and behaviour changes are RED-first. Write the failing test, show it failing, then fix and show it green.
  Name tests `*Regression*`. Tests never sleep: condition-driven, bounded pump loops.
- No atomics or mutex outside scripts/atomics_allowlist.txt (guard: python3 scripts/check_no_atomics.py).
- No virtual calls and no allocation on hot paths. Never busy-spin.
- Hot-path budget: worker / owner duty pass at most 100 us. Any change touching worker, owner or duty-pass code
  states its cost; measure before/after if it touches the per-pass path.
- Aeron-faithful consensus (documented tightenings only).
- Build dir: create `build-fast` with the same cache settings as /Users/vradu/Projects/rune-broker-wt26/build-fast:
  `cmake -S . -B build-fast -G Ninja` with the CMAKE_* / RUNE_PROTOCOL_INCLUDE / OPENSSL_ROOT_DIR /
  CMAKE_IGNORE_PREFIX_PATH / CMAKE_EXE_LINKER_FLAGS=-lz values copied from that dir's CMakeCache.txt. Ninja and
  ccache are on PATH.
- Run ONLY the affected test targets (-j4, skip latency-gate tests). The controller runs the full suite after merge.
- Each item: fix it, or rule it out with a written reason (no hand-waving). Small logical commits on this worktree's
  branch. End each commit message with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Do NOT merge, push, touch other worktrees, or run Docker. Do NOT dispatch subagents.
- Report: write the report file named below (per item: done + commit + test + RED evidence, or ruled + reason). Return
  only status, commit SHAs, a one-line test summary and concerns.

Report: docs/superpowers/briefs/2026-10-09-lows-consensus-metadata-report.md
