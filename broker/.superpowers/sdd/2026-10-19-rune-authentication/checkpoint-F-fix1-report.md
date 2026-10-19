# Checkpoint F fix round 1

Status: DONE. Commit e3c76181 on feat/auth (not pushed).

## Fixes
1. PeerHandshake::restorePrior_ now takes `std::optional<Established>` by value (libs/peerlink/src/PeerHandshake.cpp and the header). Callers passed `h->prior` / `c->prior`, which live inside the state_ alternative that `state_ = *prior` overwrites (ASan memcpy-param-overlap). Class sweep: every other `state_ = X` in PeerHandshake.cpp assigns a local (hs, cs, e, LinkIdle{}); no other state machine in libs/peerlink, libs/auth-related or libs/gateway has an assignment from a member of the destination. No other occurrences.
2. AuthRig::leaveGroup built `bytes("nobody")` from a temporary std::string (span dangled during encode). Fixed with a local string. `AuthRig::bytes(std::string&&)` is now deleted so the whole class fails to compile; no other helper used a temporary (the build is clean).
3. AuthStressRegressionTest sendChange_: `m.pattern = c.grant.cluster ? "" : c.grant.text` made a temporary std::string that the view dangled from. Now assigned only when not cluster (pattern defaults empty). (First attempt had a comment swallowing the following statement; caught by the test failing, fixed before commit.)

Regression coverage: the existing ASan-failing tests (PeerLinkRegression.ATimedOutHintRekeyDoesNotDelayTheNextOneRegression, AuthClusterRegression.WrongKeyBrokerNeverJoinsRegression, AuthEnforcementRegression.LeaveGroupNeedsNoGrant, AuthStressRegression.SeededRandomGrantRevokeTrafficHoldsEveryInvariant). The test-side bugs are additionally guarded at compile time by the deleted overload.

## Verification
- build-asan, `ctest -R "PeerLink|PeerHandshake|AuthCluster|AuthEnforcement|AuthStress" -j6`: 76/76 pass, no ASan/UBSan report.
- build-fast, same filter: 76/76 pass.
- One transient failure of WrongKeyBrokerNeverJoinsRegression occurred in the first ASan run (load, 4 tests in parallel); it passed alone and in both full filter runs. Not investigated further.

## SCRAM (item 4)
Release build (-O3, conan Release openssl 3.3.2 + benchmark 1.8.2, build dir build-rel-scram/crypto, bench source rune-crypto/bench/ScramBench.cpp, arm64 native, machine load average about 8):
- Full exchange with two verifiers: median 1.89 ms (10 repetitions, range 1.886-1.904 ms, cv 0.3 %); p99 is about 1.90 ms (max of 10). Gate 5 ms: PASS.
- Client salt+password: median 0.946 ms. Server verify: 1.38 us.
The Debug 6.1 ms was a Debug-dependency artefact, not a real miss.

## Follow-ups
Two entries appended to rune-followups/issues.md (TOC rows 276, 277): the sanitizer-only WipedMember precondition, and the load/sanitizer timing flakes (relates to existing #90).
