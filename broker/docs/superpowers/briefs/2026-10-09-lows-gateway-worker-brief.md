# Lows batch C: gateway, worker and DR harness

Worktree ~/Projects/rune-broker-wt72, branch fix/lows-gateway-worker (from integ/perf-defaults 4b0b6a6).

## Items
1. **D8b-L: re-review lows.** Find the D8b re-review in docs/superpowers/briefs (2026-10-04-d8b-review.md and
   later).
   - lostSinceNs is not restarted when one worker reports Found while another is still Lost.
   - The Progress-rate comment (~10/s) is wrong at busy pass rates.
   - A successful claim touches edges_: a 2nd cache line and +56 KiB per sink. Fold a watched bit into PubReady, or
     benchmark it (hot path: measure before/after).
   - Real-driver tests:
     - gateway image release;
     - B's full intake while A holds;
     - the BACK_PRESSURED -> Found path;
     - the MEDIUM-1 path.
2. **D8c / D8d follow-ups.**
   - Align D8c's per-episode reader marks with D8d's 64-pass window.
   - Tombstone limit 1024: justify it or make it configurable.
   - D8c lows R3 (a refused frame is re-decoded each cycle) and R4 (a frame refused for one worker blocks the
     session's other produces + Metadata / Keepalive: document it or fix it).
   - R5: there is no multi-worker credit test.
   - The two D8b tests still on the legacy transport: move them to the default transport.
3. **D10-L lows.**
   - Egress review LOW-4 (2026-10-05-egress-pub-lifetime-review.md): the rig is shard-mode only and the push never
     sends.
   - Redirect review LOW-2 (2026-10-05-redirect-reoffer-review.md): a back-pressure refusal re-offers the whole
     window, which is extra traffic.
   - D2-10 harness (Python, dr-e2e/harness/driver.py): a client container that hits the 240 s pytest timeout is never
     removed and leaks across the run. Kill / rm it on TimeoutExpired, plus a unit test of the helper if feasible
     without Docker.

## Global rules (binding)
- Rune broker, C++23, Aeron-based. Read CLAUDE.md at the worktree root first.
- Bug fixes and behaviour changes are RED-first. Write the failing test, show it failing, then fix and show it green.
  Name tests `*Regression*`. Tests never sleep: condition-driven, bounded pump loops.
- No atomics or mutex outside scripts/atomics_allowlist.txt (guard: python3 scripts/check_no_atomics.py).
- No virtual calls and no allocation on hot paths. Never busy-spin.
- Hot-path budget: worker / owner duty pass at most 100 us. Any change touching worker, owner or duty-pass code
  states its cost; measure before/after if it touches the per-pass path.
- Aeron-faithful consensus (documented tightenings only).
- Build dir: create `build-fast` with the same cache settings as ~/Projects/rune-broker-wt26/build-fast:
  `cmake -S . -B build-fast -G Ninja -DBUILD_TESTING=ON` with the CMAKE_* / RUNE_PROTOCOL_INCLUDE / OPENSSL_ROOT_DIR /
  CMAKE_IGNORE_PREFIX_PATH / CMAKE_EXE_LINKER_FLAGS=-lz values copied from that dir's CMakeCache.txt. Ninja and
  ccache are on PATH.
- Run ONLY the affected test targets (-j4, skip latency-gate tests). The controller runs the full suite after merge.
- Each item: fix it, or rule it out with a written reason (no hand-waving). Small logical commits on this worktree's
  branch. End each commit message with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Do NOT merge, push, touch other worktrees, or run Docker. Do NOT dispatch subagents.
- Report: write the report file named below (per item: done + commit + test + RED evidence, or ruled + reason). Return
  only status, commit SHAs, a one-line test summary and concerns.


Report: docs/superpowers/briefs/2026-10-09-lows-gateway-worker-report.md
