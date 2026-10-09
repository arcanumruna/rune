# Client lows batch: rune-client-cpp

Worktree /Users/vradu/Projects/rune-client-cpp-wt18, branch fix/client-lows (from origin/main 4d4a20a). Read
CLAUDE.md first.

## Items
1. **A7 client halves** (docs/superpowers/briefs/2026-10-04-a7-review.md, copied here).
   - **LOW-1:** the peer-down / ICMP tests must assert that an ICMP error was actually pending (positive control). If
     the environment never delivers one, GTEST_SKIP with the reason; never pass vacuously. The broker side did exactly
     this in rune-broker 5cc6b16 (libs/node/tests/node/EmbeddedDriverTest.cpp).
   - **LOW-2:** remove the Regression label from coverage tests that pass on pre-fix code.
   - **L5:** Client::wakeFds via testTransport(): give it a proper accessor, not a test seam.
   - **L2 (the Aeron pin):** the broker pins epoll-first polling to the Aeron release it was verified against (Aeron
     1.41.4; rune-broker commit 3763230): any other version falls back to stock polling, and a test fails on an Aeron
     upgrade. Do the same in the client if the client has the same epoll-first path.
2. **Redirect LOW-2** (docs/superpowers/briefs/2026-10-05-redirect-reoffer-review.md): a back-pressure refusal
   re-offers the whole window, which is extra traffic. Re-offer only what was refused, if the code allows it cleanly;
   otherwise rule it with numbers.
3. **D6 rest:** client nits R2-n1..n5 and C-F21/F24/F31/F32. Find them in docs/superpowers (grep "R2-n1", "C-F21"
   across this repo and /Users/vradu/Projects/rune-broker-wt26/docs). Also the T13 slow-client test on the old
   transport: move it to the default transport. /dev/shm capacity (192 failures): investigate whether still relevant,
   then fix or rule.

## Rules
- RED-first for every behaviour fix, with `*Regression*` test names. No sleeps in tests.
- Duty thread only; no atomics or mutex outside the allowlist (ctest guard).
- Build: the Mac build works again. Configure a new build dir exactly like
  /Users/vradu/Projects/rune-client-cpp/build (toolchain from rune-client-cpp/build/build/Debug/generators, Ninja,
  -DBUILD_TESTING=ON, OPENSSL_ROOT_DIR=/Users/vradu/.conan2/p/opensc63382a50a0c7/p, -DOpenSSL_DIR=OpenSSL_DIR-NOTFOUND,
  -DCMAKE_IGNORE_PREFIX_PATH=/opt/homebrew, -DCMAKE_EXE_LINKER_FLAGS=-lz,
  -DRUNE_CRYPTO_ROOT=/Users/vradu/Projects/rune-crypto, -DRUNE_PROTOCOL_INCLUDE=/Users/vradu/Projects/rune-protocol/include).
  The sibling rune-protocol checkout is on a feature branch; use
  `git -C /Users/vradu/Projects/rune-protocol worktree add /tmp/proto-main main` and point RUNE_PROTOCOL_INCLUDE there.
- If x86 test binaries hang at launch again (Rosetta), stop and say so in the report.
- Run the affected targets; run the full ctest -j6 once at the end.
- Small logical commits ending with `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`. Do NOT
  merge or push. Do NOT dispatch subagents.

## Report
docs/superpowers/briefs/2026-10-09-client-lows-report.md: per item, done (commit + test + RED evidence) or ruled
(reason). Return status, SHAs, a one-line test summary and concerns.
