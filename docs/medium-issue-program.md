# sev:medium issue-fix program — runbook and handoff

CLAUDE.md (root, broker/, client-cpp/) is binding; this file only adds the program's process and state.

## 1. Environment
- Deps prebuilt outside the repo (never committed). Configure once: `cmake -B build -DRUNE_TLS=OFF` (see README#build).
  Keep ONE build dir; agents create no new build dirs and no sanitizer builds. Client unit tests
  (e.g. GroupRoutingRegressionTest) need a separate client-cpp build with `-DBUILD_TESTING=ON`.
- Check `df` first (the full build is ~18 GB). If a test fills disk, fix the test.
- Scratch dirs: unique `/tmp/rune_t_<batch>_*`, deleted by exact name.
- GitHub: REST via `gh api` (GraphQL may be blocked). Never `conan create/export/upload`.
- ~50 `rune_node_tests` failures pre-exist under TLS=OFF (#533): compare against main, don't chase.

## 2. Selecting issues
- Query open `sev:medium`. Skip: the tracker #474, #456; issues with someone else's "work started" comment or open PR;
  TLS work (#137, #532, #12); #460 and its mechanism (#543, #581); bench-only (#260, #261, #80, #81, #134 bench arms);
  VM-only (#71, #43); legacy v1 (#553).
- Deferred (need design): #135, #573 (prefix-scan completion), #419, #361, #592, #612 (cross-node channel),
  #610 (ASan), #617, #618, #539 (plane-ack protocol), #582, #507 (catalog knownEnd + ack gate like #518; after M29),
  #68, #41.

## 3. Batching
- ≥ 4 issues per batch sharing one code path (pad with sev:low if needed). Order: bugs → test-gaps → tech-debt/docs.
- Comment "Work started (cloud medium batch Mn)." on each issue.
- Lead does diagnosis + design and writes a recipe (where, algorithm/invariant, every call site, exact tests +
  mutations, don'ts). Implementer: Sonnet for simple recipes, Opus for complex. Max 3 agents; one agent per build dir.

## 4. Implement
- Branch `bugfix/medium-batch-<n>` off latest origin/main. Per issue: failing `*Regression*` test first, fix, green,
  mutation check. One commit per issue with the session `Co-Authored-By:` trailer. No secrets/abs paths/model ids.
- Run only touched test executables (each ≤ 5 min). Push the branch; open the PR with `Fixes #N` lines, root causes,
  tests, deps note.

## 5. Review (fresh Opus agent per batch, background)
- Reads docs/adversarial-review.md; whole-area review; adversarial `…Adversarial…Regression` tests with mutation
  checks, `--repeat until-fail:20`, one `-j3` run.
- One COMMENT review, one fix round, non-blockers filed as labeled issues.
- Merge commit; close issues "Fixed in <sha>: … (regression: <test>)"; comment on #474; leave repo on clean main.
  Never delete branches.

## 6. State (2026-10-24)
- Merged through M27 (main 3984517d).
- **M28 — PR #619** (`bugfix/medium-batch-28`, head 4cfee6a1): #584, #545, #540, #546, #554, #274, #275. NOT reviewed
  (reviewer stopped). Next: review it, and verify the heap-corruption concern in `KeysReachEveryWorkerAndTheAdminThread`.
- **M29 — not started** (work-started posted): #587, #518, #608, #444.
- **M30 — designed, not started** (work-started posted), view recovery/readiness:
  - #590: `readCheckpointFile(..., srcLogStart)`. Committed + legacy → committed wins, legacy removed. Legacy only with
    logStart ≤ 0 → refold. Legacy only with logStart > 0 → rename to the committed name and replay (WARN; on rename
    failure ERROR, still return its bytes). Thread srcLogStart through
    materializeRecoveryInstancesFor / getOrCreate_ / driveFolds / driveRebuild.
  - #159: a checkpoint above committedTail is HW lag, not loss. Ready only when `foldCursor == bound`
    (one `caughtUp_` helper at every flip site).
  - #75: test Rebuilding → CatchingUp → Ready with an unconfirmed HW.
  - #163: readiness doc comment (WorkerViewSet.h).
  - #591: AbortedTxnFilter holds `const AbortedTxnIndex*`; inline `isAborted`; ABAB BM_ViewFoldReplace.
- **M31 — in design (client, SubscriberHub)**:
  - #515: in driveHomed, end with the reject code only if the router's leaderSession is still the home session;
    otherwise follow the route (pending and bound loops).
  - #446: an OK for an unknown or abandoned corrId → send Unsubscribe on the answering session.
  - #514: a bound homed reader on a crashed leader must trigger a metadata refresh before responseTimeoutNs.
    Idea: a session with no inbound activity for > 2× keepalive is suspect and arms scheduleMetadataRefresh
    (SessionMachine `lastActivityNs_`).
  - #510: AeronTransport/ClientAgent start → rich lifecycle state (Failed).
- Branches left on the remote: `bugfix/medium-batch-28` (open PR #619). Merged batch branches were kept (no deletions).
