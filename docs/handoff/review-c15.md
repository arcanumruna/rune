# Opus review — client batch C15 (transactional producer leftovers), commit 301a321 (+ f4fa08c model update)

Repo the repo root, branch claude/kind-bardeen-ze6yl0 (checked out — do NOT switch, commit, push or stash; the
coordinator commits). Read CLAUDE.md, client-cpp/CLAUDE.md and docs/adversarial-review.md FIRST.
Never spawn subagents. Do not fix product code: you report and write tests.

Issues fixed by the batch: #340 #344 #346 #347 #348 #349 #353 #354 #368 (read each with the issue text in the
commit message `git show 301a321`; the diff is `git show 301a321`).

## Scope (whole-area, not only the diff)
- CoordinatorLink (client-cpp/include/rune/client/producer/CoordinatorLink.h, src/producer/CoordinatorLink.cpp):
  per-state fields, slot sizing (txn 8 / plain 1), tag static_asserts vs GroupConsumerHub, NotCoordinator refresh
  only when the redirect names the current coordinator (and the Client.cpp side).
- The transaction state machine in src/producer/ProducerTxn.cpp: joiner/flush caps and the second concurrent
  sendOffsets completing InvalidArgument without changing state; TxnOpOrigin; poison → add queue cleared and every
  unsent record failed on the next duty pass (#346/#368); empty offsets complete locally (#348).
- The claim abandon quarantine buffer reserved at Direct ingress (no allocation; ProducerImpl.cpp).
- ProducerRef detach assert, ring alignment, header-only txn control buffer, openTopicAndWait ownership (#349).
- Docs: docs/api/client-cpp.md.

## Adversarial pass
Apply every attack class from docs/adversarial-review.md that fits (input bounds on the caps: cap-1/cap/cap+1;
protocol state: every txn state × each call; concurrency: same op twice, coordinator change mid-op, NotCoordinator
for a stale coordinator; restart/close mid-transaction; resources: bounded queues, zero allocation on the poison
and abandon paths; data integrity: no record both failed and delivered, none silently dropped).
Tests drive the public producer API with the injected clock (see existing ProducerTxn*Test.cpp, TxnMatrixTest.cpp
rigs). Name them `…Adversarial…Regression`. No sleeps; seeded randomness; mutation check each (revert the fix → red).
`python3 client-cpp/scripts/check_no_atomics_or_mutexes.py --tests client-cpp` must print OK.
A test that exposes a real bug: keep it as `DISABLED_` and report it (the coordinator decides fix vs issue).

## Builds
Named targets only, under the lock: `flock build-client/.build.lock ninja -C build-client <targets>`
(rune_invoker_tests holds the producer tests; grep client-cpp/tests/CMakeLists.txt). Never a whole-tree build, never a
new build dir, never reconfigure. Another agent may be editing other files; touch only test files in the areas above.

## Report
Findings ranked (urgent = hang, silent loss, security, broken feature; else minor), each with file:line, the failing
path and the test that shows it. Then each attack class: tests written (name, file, mutation red/green) or why it
does not apply. Verdict: mergeable or not.
