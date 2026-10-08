# Lows batch B: storage, replication and transport

Worktree /Users/vradu/Projects/rune-broker-wt71, branch fix/lows-storage-replication (from integ/perf-defaults
4b0b6a6).

## Items
1. **D23: lows from docs/superpowers/briefs/2026-10-07-lows-review.md** (copied into this worktree).
   - A single-batch staging file with a corrupt base above the chain is still accepted (narrowed, not closed).
   - A sparse epoch-state file can fail the epoch write on a full disk (it fails closed; consider a fallocate up
     front).
   - A dup fd is left open in two rare / shutdown cases.
   - A test replaces global new/delete for its binary (isolate it).
   - The errno trick in writeAll_.
   - One extra leadership round after a corrupt state file (by design: rule it).
2. **D26: follow-up lows** from docs/superpowers/briefs/2026-10-08-fix65-review.md. The pread fix itself is done.
   - Provisioner read-map growth is now dead mmap work for push reads; it holds mmap_lock for write. Remove it if it
     is truly unused; prove it with the existing tests and an FR or perf counter.
   - A pread EIO falls back to the mmap (SIGBUS instead of an answerable error); make it an answerable error.
   - Stale F5 comments in Worker.h / LogSegment.h.
   - A pread still copies about maxBytes when the HW bound is just past the cursor; bound the copy by HW.
   - A possible pre-existing stuck cursor inside a compacted tail gap of a sealed segment. Investigate with a test;
     fix or rule.
3. **A7-L: transport lows** from docs/superpowers/briefs/2026-10-04-a7-review.md.
   - The C1 tests don't assert that an ICMP error was pending.
   - 3 coverage tests carry the Regression label but pass on pre-fix code; relabel them.
   - L1: a non-ECONNREFUSED error ends the poll pass early.
   - L2: an Aeron-internals self-check / pin.
   - L3: the runner's virtual idle() / doWork(). Not on the hot path? Evaluate against the no-virtual rule.
   - L5: Client::wakeFds via testTransport().
   - L6 / L7.

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

Report: docs/superpowers/briefs/2026-10-09-lows-storage-replication-report.md
