# Session handoff — 2026-10-28 (delete this folder before PR #691 merges)

This is the working state of the client-batch and M-batch program, saved before a session pause. The next session
reads this file first, then removes `docs/handoff/` in its first commit on this branch.

## Branch `claude/kind-bardeen-ze6yl0` (PR #691)

- **Committed and verified:** C16, C8 (+ review fixes), C17, the #180 control-marker urgent fix, C14 (+ review fix),
  C15, and the TxnStress model update (f4fa08c).
  - The client checkpoint at f4fa08c is green: `ctest -R Regression -LE latency` in `build-client` passed 1700/1700.
- **WIP commit 33637b0 (unverified):** two agent runs were cut off mid-way.
  - **C7 implementation** (`recipe-c7.md`; issues #22 #222 #225 #226 #310). It touches Client.h/.cpp,
    Completion.h, QlEngine.cpp, ProducerImpl.cpp, ClientAuthErrorsTest, ProducerAckPathTest,
    RuneConsoleOfflineTest and OpenTopicGateTest.
    - Check each recipe part against the diff: failing test first, the fix, then the mutation check.
    - Finish what is missing.
  - **C15 Opus review tests** (`review-c15.md`). They are in CoordinatorLinkTest and ProducerTxn{,Data,Offsets}Test.
    - Re-run the review from the brief.
    - Keep the finished tests; any test exposing a real bug stays `DISABLED_` until it is fixed.
- **Broker checkpoint:** the broker `-R Regression -LE latency` suite (9608 tests) was started but did not finish.
  - Rebuild the test executables by name. The list is `ctest -R Regression -LE latency --show-only=json-v1`, taking
    the basename of each command.
  - Then run it.

## Next steps, in order

1. **Verify the WIP and finish both:**
   - build the client test targets;
   - run the focused suites;
   - run the ratchet: `python3 client-cpp/scripts/check_no_atomics_or_mutexes.py --tests client-cpp` must print OK.
2. **Finish C15 and C7:**
   - C15: the review, then its fix round.
   - C7: review it (Opus, adversarial: `docs/adversarial-review.md`), then its fix round.
   - Add `Closes #22 #222 #225 #226 #310` lines to PR #691 (one per line). #638 is not part of C7.
3. **Merge PR #691:**
   - Checkpoint first: broker and client Regression suites.
   - Remove `docs/handoff/` before merging.
   - After the merge, verify that every Closes issue closed. #335 stays open, since it is only partly fixed.
4. **M28 fix round** (PR #619, branch `bugfix/medium-batch-28`):
   - F1: void the Proof in the `establishAsResponder_` deferral.
   - F3: the scrubs.
   - F4: the fanout cap drops the oldest.
   - Add the attack tests in `m28-attack-tests/`: A1 and A4 are red at the PR head; A2, A3, A5 and A6 are guards.
   - Merge, then close #584 #545 #540 #546 #554 #274. #275 is only part-fixed: comment on it instead.
5. **M29** (`recipe-m29.md`: #608 #444 #518), then **M30**.
6. **Remaining client batches** (see `client-batch-plan.md`): C9 runs after M30. Check which C batches are still open
   with `list_issues`.

## Rules carried over

- Build named targets only, under `flock build-client/.build.lock`. No new build dirs: the disk is nearly full.
- Sanitizer CI is manual-only. Run the full suite only at checkpoints, every 3–4 batches.
- At most 3 agents at once. Implementers get exact recipes and never spawn subagents.
- Merge PRs and close issues as batches land, without waiting to be asked.
