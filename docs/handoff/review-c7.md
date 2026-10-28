# Opus review — client batch C7 (issues #222 #22 #226 #225 #310), working tree on branch claude/kind-bardeen-ze6yl0

Repo the repo root, branch claude/kind-bardeen-ze6yl0 (checked out — do NOT switch, commit, push or stash; the
coordinator commits). Read CLAUDE.md, client-cpp/CLAUDE.md and docs/adversarial-review.md FIRST.
Never spawn subagents. Do not fix product code: you report and write tests.

The batch recipe is docs/handoff/recipe-c7.md - read it,
and read each issue (#222 #22 #226 #225 #310) on GitHub (mcp__github__issue_read, arcanumruna/rune) for intent.
The C7 changes are in the WIP commits since 9e125f3 mixed with other batches; the C7 files are:
client-cpp/src/producer/ProducerImpl.cpp (the #222 NotAuthorized-on-first-attempt block before the `// D:` case),
client-cpp/include/rune/client/Client.h + src/Client.cpp (throwOpenTopicFailure / openTypedSchema_ for #22;
SeedStatus.rejectCode + sessionRejectCode() threading rule for #310), client-cpp/include/rune/client/Completion.h
(retryAfterMs, #226), client-cpp/src/cli/QlEngine.cpp (#225), and the tests in ProducerAckPathTest.cpp,
OpenTopicGateTest.cpp, ClientAuthErrorsTest.cpp, RuneConsoleOfflineTest.cpp. Ignore the metadata-refresh /
GroupConsumerHub changes (batch C18, another agent is still editing them).

Implementer notes to verify: (a) #222 decides "not appended" from `b->attempts == 1` - is that sound for every path that
can re-send a batch (retransmit after session change, OOS resequence, leader change, Direct vs Thread mode)? Can a
first attempt be answered 44..47 AFTER it was actually appended (e.g. a duplicate / a session reconnect where the
first send did reach the log)? (b) sessionRejectCode() is no longer noexcept and throws on a foreign thread: check every
caller and the published SeedStatus layout (16 bytes, release/acquire). (c) retryAfterMs accepts NotConnected as well as
BrokerError for Connect/44. (d) openTopic error text for UnknownTopic vs unknown fingerprint vs other codes; the
remaining-budget probe.

## Adversarial pass
Apply every attack class from docs/adversarial-review.md that fits (protocol state: 44..47 on first attempt vs
retransmit, ahead of in-flight batches, during a session change; sequence/epoch integrity: produced = acked, no
duplicate, offsets never move back; concurrency: the threading rule of sessionRejectCode across Invoker/Thread mode;
input bounds: retry-after 0/1/max; openTopic with zero remaining budget). Tests use the injected clock and existing
rigs. Name them `…Adversarial…Regression`. No sleeps; seeded randomness; mutation check each (revert the fix -> red).
`python3 client-cpp/scripts/check_no_atomics_or_mutexes.py --tests client-cpp` must print OK.
A test that exposes a real bug: keep it `DISABLED_` and report it (the coordinator decides fix vs issue).

## Builds
Named targets only, under the lock: `flock build-client/.build.lock ninja -C build-client <targets>`
(rune_invoker_tests, rune_open_topic_gate_tests, rune_client_auth_tests, rune_cli_offline_tests). Never a whole-tree
build, never a new build dir, never reconfigure. Touch only test files in the areas above.

## Shared tree and disk (read this - incidents happened)
- Other agents edit this tree at the same time. For a mutation check make the smallest possible edit, run, then undo
  exactly that edit (Edit tool, old/new swapped). NEVER save a whole-file copy and restore it over the live file - that
  erased another agent's work once. Before reporting, grep your mutation marker / `if (false &&` and confirm none left.
- The disk is nearly full. Build with `TMPDIR=/dev/shm/<yourname>` and `-j2`; delete that dir at the end. Run test
  executables with gtest filters for what you need rather than full suites in parallel. If you see "No space left on
  device", it is the environment - wait (a capped loop) and retry, do not treat it as a product failure. Never delete
  files you did not create.

## Report
Findings ranked (urgent = hang, silent loss, security, broken feature; else minor), each with file:line, the failing
path and the test that shows it. Then each attack class: tests written (name, file, mutation red/green) or why it
does not apply. Verdict: mergeable or not.
