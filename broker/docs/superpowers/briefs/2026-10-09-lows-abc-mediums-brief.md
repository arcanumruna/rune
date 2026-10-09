# Fix round: the MEDIUMs of the lows A/B/C review

Worktree /Users/vradu/Projects/rune-broker-wt71, branch fix/lows-storage-replication. It is already fast-forwarded
or merged to integ/perf-defaults, so it contains all three lows batches.

Read the review first: docs/superpowers/briefs/2026-10-09-lows-abc-review.md (copy it from
/Users/vradu/Projects/rune-broker-wt26/docs/superpowers/briefs/ if missing). Fix:
- **M1 (D26): persistent read EIO.** A persistent pread EIO is retried on every worker pass, which stalls the
  worker, and nothing escalates.
  - Fix: treat it like a write failure. Mark the log storage-failed (sticky), so the log-dir-offline / broker halt
    path runs (Kafka: a read IOException takes the log dir offline).
  - RED-first test.
- **M2 (D23): epoch-state re-create on the worker duty pass.** A slow re-create (fsync / rename / dir fsync) can now
  run on the worker pass at about 338 epochs, or immediately when the startup reservation failed, with no backoff.
  - Fix: keep it off the worker pass (route it to the existing async durability / io backend path), or restore the
    old threshold behaviour, and add backoff on failure.
  - Test, with the hot-path budget stated.
- **M3 (D26 LOW-1): owner-thread mmap after a roll.** After every roll, a log read through the mapping gets an
  owner-thread mmap sized to the whole active segment.
  - Fix: post a provisioner map request when the growth path finds the new segment unmapped, so the owner thread
    never maps.
  - Test.
- **Cheap LOWs:** fix the deferred-produce memo staleness, the double read of a too-large first batch, the stale
  scan-window comment, and the report wordings. Rule the others in the report.

Binding rules: RED-first `*Regression*` tests; no atomics or mutex outside the allowlist; no hot-path allocation or
virtual calls; worker / owner duty pass at most 100 us (measure if touched); never busy-spin.

**IMPORTANT build / test environment.** x86_64 binaries HANG at launch on this Mac (Rosetta is wedged). Do NOT run
Mac-built test binaries. Use the native arm64 Linux dev container:
- `/Users/vradu/Projects/rune-dev/devrun.sh /Users/vradu/Projects/rune-broker-wt71 '<cmd>'`.
  - It mounts the worktree at /src and the build dir at /bt (= <wt>/build-linux), with the sibling repos.
  - Configure once: `devrun.sh WT 'cmake -S /src -B /bt -G Ninja $RUNE_CMAKE'`.
  - Then `devrun.sh WT 'cmake --build /bt --target <t> -j8 && /bt/libs/.../<t> --gtest_filter=...'`.
- Run only the affected targets. Skip the latency-gate tests: this is arm64 under Docker, not the reference box.

Commit small logical commits, ending each message with
`Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`. Do NOT merge, push, or touch other
worktrees. Do NOT dispatch subagents.

Report: docs/superpowers/briefs/2026-10-09-lows-abc-mediums-report.md (per item: done + commit + test + RED evidence,
or ruled + reason). Return status, SHAs, a one-line test summary and concerns.
