# Session handoff — 2026-10-28 (updated) (delete this folder before PR #691 merges)

This is the working state of the client-batch and M-batch program, saved before a session pause. The next session
reads this file first, then removes `docs/handoff/` in its first commit on this branch.

## Branch `claude/kind-bardeen-ze6yl0` (PR #691) — state at 0c82c7b and later

- **Done and verified:** C16, C8, C17, the #180 control-marker fix, C14, C15.
  - The C15 Opus review rated it mergeable and added 11 adversarial tests (already committed).
  - Its minor findings were filed as #698 and #699. #340 got a comment and is now `Refs`, not `Closes`, in the PR.
- **C7 is implemented** (`recipe-c7.md`; #22 #222 #225 #226 #310). All parts are green, with mutation checks. Its Opus review
  (`review-c7.md`) was running when this snapshot was taken. Run it again if no report exists, then do its fix round.
  - WIP commit 937a5c5 captured a C7 mutation (`if (false &&`) in ProducerImpl.cpp. Later commits have the
    correct line. Grep for `if (false &&` before merging.
- **C18** (`recipe-c18.md`; #651 #676 #660 #685 #684) was mid-implementation. The C7 agent may have clobbered
  C18 Client.cpp edits around 08:17. Check that `nextMetadataRefreshAt_` (old name) is gone and the #651
  else-branch is in `onMetadataResponse`, then finish C18, review it, and add its Closes lines to the PR.
- **C19** (`recipe-c19.md`; #692 #693 #689 #656 #655 #657 #675 #674 #673) was mid-implementation. Check each part
  against the diff and finish what is missing, then review it and add its Closes lines.
- **Broker checkpoint:** the first run hit "No space left on device" (521 environmental failures: Bus error,
  `cannot create directories`). Re-run the failed tests alone at `-j2` when no agent is building:
  `ctest --rerun-failed -j2 --output-on-failure` in `build`.
- **Shared-tree rules for agents** are in `shared-tree-rules.md`. Mutations are undone edit by edit, never by
  restoring a whole-file copy. Builds use `TMPDIR=/dev/shm/<name>` and `-j2`.

## Next steps, in order

1. Finish C7's review, C18 and C19, each with an Opus review and a fix round. Add the Closes lines to PR #691.
2. Run the client and broker Regression checkpoint (broker at low parallelism; mind the disk).
3. Remove `docs/handoff/` and merge PR #691. Verify that every Closes issue closed; #335 and #340 stay open.
4. C20 (#672 #690 #681 #680 #678 #682 #671 #495 #688 #668), then C21 (#654 #679 #667 #664 #665 #426 #475).
   See `client-batch-plan.md`. #687 is possibly deferred.
5. The M batches (M28 fix round on PR #619, M29, M30) come after the client batches, at the owner's direction.

## Rules carried over

- Build named targets only, under `flock build-client/.build.lock`. No new build dirs: the disk is nearly full.
- Sanitizer CI is manual-only. Run the full suite only at checkpoints, every 3–4 batches.
- At most 3 agents at once. Implementers get exact recipes and never spawn subagents.
- Merge PRs and close issues as batches land, without waiting to be asked.
