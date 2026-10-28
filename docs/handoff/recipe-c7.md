# Batch C7 — client error reporting (issues #22, #222, #226, #225, #310)

Repo `the repo root`, branch `claude/kind-bardeen-ze6yl0` (already checked out — do NOT switch, do NOT push, do NOT commit;
the coordinator commits). Read `CLAUDE.md` first. You are an implementer: never spawn subagents.

Build trees (exist, Ninja Debug): `build-client` (client) and `build` (broker). Disk is tight: do NOT create new build
dirs. Build only the targets you need: `cmake --build build-client --target <t> -j8`.

Rules for every part:
- Failing test FIRST (run it, paste the RED line), then the fix, then GREEN.
- Every new test name contains `Regression`.
- After each part, do the listed mutation (revert just the fix line(s)), rebuild, show the test RED, restore.
- No new sleeps or wall-clock waits: reuse each file's existing drive/pump helpers (they have iteration/deadline caps).
- No secrets, emails, IPs, local absolute paths, model names in code or comments.
- Keep comments short; cite the issue number (`// Issue #222: ...`).

(#638 is NOT in this batch: it needs a version-gated wire field; the coordinator handles it separately.)

---------------------------------------------------------------------------------------------------------------------
## Part A — #222: a ProduceResponse 44..47 is a definite non-append (idempotent producer)

File `client-cpp/src/producer/ProducerImpl.cpp`, function handling the produce error (the branch chain ending at the
comment `// D: rejected, but an earlier attempt may be stored — outcome unknown.`, ~line 2562).

Facts (verified by the coordinator — do not re-decide):
- The broker answers 44 AuthenticationFailed / 45 NotAuthorized / 46 SessionRevoked / 47 PasswordChangeRequired on a
  Produce from the gateway's auth gate or `Worker::refuseNotAuthorized_`, always BEFORE the append. So THIS attempt was
  not appended.
- An EARLIER attempt of the same batch could have been appended (OK lost, then the grant revoked, then a retransmit
  answered 45). So the answer is only definite when this batch was sent exactly once: `b->attempts == 1`.
- Sequences are stamped at seal time from `ps.nextBaseSequence` (`encodeAndSend` ~1746/1815, Tier-2 ~1916/1980), so
  rolling `nextBaseSequence` back is safe when b holds the newest sequences.

Insert, immediately BEFORE the `// D:` block (after the MinIsr/TransientLogFull/AuthUnavailable branch):
```cpp
    // Issue #222: auth refusals (44..47) are answered before the append: when this was the batch's ONLY attempt it
    // was definitely not stored — fail it without marking the partition unresolved (no epoch bump).  A retransmit's
    // refusal proves nothing about the earlier attempt: that stays the outcome-unknown path (D) below.
    if (ec >= static_cast<int16_t>(rune::protocol::client::ErrorCode::AuthenticationFailed) &&
        ec <= static_cast<int16_t>(rune::protocol::client::ErrorCode::PasswordChangeRequired) &&
        b->attempts == 1) {
        emitBatchAck_(ps.topicId, ps.partition, b->baseSequence, b->recordCount, -1, ec,
                      OpStatus::BrokerError, units, b->producerEpoch, b->producerId);
        txnNoteLoss_(OpStatus::BrokerError, static_cast<uint16_t>(ec), AbortKind::BumpEpoch);
        completeSlice_(ps, b->ackStart, b->ackCount, -1, OpStatus::BrokerError,
                       static_cast<uint16_t>(ec), b->tier2, b->recordCount);
        // The newest sequences were never used: give them back, so the next batch continues the sequence.
        // Otherwise the later in-flight batches meet the gap (OutOfOrderSequence) and C3 re-sequences them.
        if (b->baseSequence >= 0 && b->producerId == ps.producerId && b->producerEpoch == ps.epoch &&
            b->baseSequence + b->recordCount == ps.nextBaseSequence) {
            ps.nextBaseSequence = b->baseSequence;
        }
        releaseResolved_(ps, *b);
        reclaimAckRing_(ps);
        return;
    }
```
Check the enum names exist in `protocol/include/rune/protocol/client/Protocol.h` (`AuthenticationFailed = 44`,
`PasswordChangeRequired = 47`); use the real names. Check `b->attempts` is the field incremented on every send
(`InFlightRing.h:90`, `++b.attempts` ~2045; initialised to 1 at first send ~1826/2006). If `attempts` is NOT 1 after
exactly one send, stop and report.

Order matters: `b->baseSequence/recordCount` are read BEFORE `releaseResolved_` (which frees b).

Update the comment on the generic `// D:` block to mention "(44..47 after a retransmit too)". Grep
`client-cpp/include/rune/client/Completion.h` and the producer docs for text claiming 45 on a produce is
"outcome unknown"; fix any such text.

### Tests (file `client-cpp/tests/producer/ProducerAckPathTest.cpp`, next to `BrokerRejectionBumpsEpochOnceRegression`
~line 932; use its `ProducerImpl impl(ackCfg(8))`, `SeqCapture`, `sendOne`, `AckLedger` helpers)

A1 `ProducerSequence.NotAuthorizedOnAFirstAttemptIsNotAppendedAndKeepsTheSequenceRegression`
- sendOne → frames[0] (seq 0, epoch e0). `impl.onProduceResponse(frames[0].corrId, -1, 45)`.
- L.batchAcks.size()==1, errorCode==45, producerEpoch==e0. `impl.producerEpoch()==e0` (no bump). `!impl.hasInFlight()`.
- sendOne → frames[1]: epoch==e0, baseSequence==0 (the sequence continues, nothing consumed).
- Answer frames[1] OK (`onProduceResponse(frames[1].corrId, 7, 0)`): batchAcks[1].errorCode==0, epoch==e0, seq==0.
- Loop the first step over codes 44, 45, 46, 47 (fresh `impl` each), same assertions.
- Mutation M-A1: delete the new block → the D path bumps the epoch → RED (epoch e0+1 / seq 0 under a new epoch).

A2 `ProducerSequence.NotAuthorizedAheadOfInFlightBatchesLetsTheGapResequenceThemRegression`
- sendOne ×2 (seq 0 and 1, same epoch e0). Answer frames[0] with 45.
- batchAcks[0].errorCode==45; `impl.producerEpoch()==e0` right after (no bump from the 45 itself).
- Answer frames[1] with OutOfOrderSequence (use the file's `kOos`) → the C3 path re-sequences: producerEpoch()==e0+1;
  `impl.flushBatches(kNow, 42, sink)` re-sends it with baseSequence 0, epoch e0+1; answer it OK → its batchAck is
  errorCode 0. Exactly 2 batchAcks in total. `!impl.hasInFlight()`.
- Mutation M-A2: in the new block replace `releaseResolved_(ps, *b)` with `unpark_(ps,*b); ps.inflight.release(*b);`
  only if that changes the outcome — if it does not, say so and skip this mutation (M-A1 still guards A2: report what
  A2 shows under M-A1).

A3 `ProducerSequence.NotAuthorizedOnARetransmitStaysOutcomeUnknownRegression`
- sendOne; force a retransmit of the same batch the way existing tests do (grep the file for a test whose batch is
  re-sent after its attempt timer: e.g. advance `kNow` past the attempt deadline and call `flushBatches`/`onTimer`;
  copy that pattern exactly) so `attempts == 2`. Answer the RETRANSMIT's corrId with 45.
- batchAcks[0].errorCode==45; after nothing is in flight, `impl.producerEpoch()==e0+1` (the D path: unknown outcome
  → re-identified). Mutation: change `b->attempts == 1` to `true` → RED.

Run: `cmake --build build-client --target <the target holding ProducerAckPathTest.cpp> -j8` (grep
`client-cpp/CMakeLists.txt` ~1532 for it) then that binary with `--gtest_filter='ProducerSequence.*:ProducerAcks.*'`.
Also run the transactional producer tests binary (`TransactionalProducerTest.cpp`'s target) — `txnNoteLoss_` is shared.

---------------------------------------------------------------------------------------------------------------------
## Part B — #22: openTopicAndWait reports a missing topic as missing, not "stale"

`client-cpp/include/rune/client/Client.h` ~1101 and ~1138: both `openTopicAndWait` overloads throw
"generated type is stale" for ANY nonzero errorCode. The broker answers UnknownTopic (7) both when the topic is
missing and when the fingerprint is unknown (`broker/libs/node/src/AdminWorker.cpp` ~627-640).

Fix (no wire change):
1. Add a public non-template member in `Client.h` (next to `lineageAndWait`), defined in `client-cpp/src/Client.cpp`:
   ```cpp
   /// Issue #22: the open failure for a GetTopicSchema(name, fingerprint) error `code` (cold path; throws, never
   /// returns).  UnknownTopic is ambiguous (missing topic or unknown fingerprint): one GetTopicSchema(name, latest)
   /// tells them apart.
   [[noreturn]] void throwOpenTopicFailure_(std::string_view name, uint16_t code, std::chrono::nanoseconds timeout);
   ```
   Body:
   - if `code == UnknownTopic` (use the `rune::protocol::client::ErrorCode::UnknownTopic` value):
     `const auto latest = getTopicSchemaAndWait(name, uint64_t{0}, timeout);`
     - `latest.errorCode == UnknownTopic` → throw `std::runtime_error("openTopic: topic '" + std::string(name) + "' does not exist")`
     - `latest.errorCode == 0` → throw the existing stale message:
       `"openTopic: generated type is stale — rerun rune schema generate"` (keep this exact text; tests match it).
     - else fall through to the next rule with `code = latest.errorCode`.
   - otherwise → throw `std::runtime_error("openTopic: the broker refused '" + name + "' (error " + std::to_string(code) + ")")`.
     If `authErrorName(code)` is non-null, append ` NotAuthorized`-style name: `" (error 45 NotAuthorized)"`.
   Name it without the trailing underscore if the class's public members don't use one (check `Client.h` style:
   public helpers have no underscore — use `throwOpenTopicFailure`; mark it in a `/// Internal:` comment).
2. Overload 1: `if (r.errorCode != 0) throwOpenTopicFailure(name, r.errorCode, timeout);` (keep the separate
   `schemaMatches` check and its stale message).
   Overload 2: split its combined condition the same way (errorCode first, then `schemaMatches` → stale message).
3. `lineageAndWait`: leave as is.
4. Update the doc comment above overload 1 (~1076) to list the three messages.

### Tests (file `client-cpp/tests/client/OpenTopicGateTest.cpp`, target `rune_open_topic_gate_tests`; copy the
harness setup from `StaleFingerprintStillThrows` ~576, including its port-pair pattern: pick two unused port numbers
by grepping the file for the highest `GateHarness h(` pair and adding 2 / 4 / 6)

B1 `OpenTopicGateRegression.AnUndefinedTopicIsReportedAsMissingNotStaleRegression`
- `h.gw->getErrorCode = 7; h.gw->unknownFingerprintErrorCode = 7; h.gw->getBlob = {};`
- `openTopicAndWait<StubV1>("nope")` throws `std::runtime_error` whose `what()` contains `"does not exist"` and does
  NOT contain `"stale"` (catch it explicitly; `ADD_FAILURE` if nothing is thrown).
- Mutation: make `throwOpenTopicFailure` always throw the stale text → RED.

B2 `OpenTopicGateRegression.AnUnknownFingerprintOfAnExistingTopicIsStillStaleRegression`
- As `StaleFingerprintStillThrows` but `unknownFingerprintErrorCode = 7` and latest answers OK (getErrorCode 0, a v2
  blob). what() contains `"stale"`.
- Same for the `Producer&` overload (create a producer as other tests in the file do; if none does, use
  `h.client->producer()` / whatever the file uses — grep `openTopicAndWait<.*>(.*,` ).

B3 `OpenTopicGateRegression.AnotherBrokerErrorIsNamedRegression`
- `unknownFingerprintErrorCode = 45` → what() contains `"error 45"` and `"NotAuthorized"`, not `"stale"`.

Also update the existing `StaleFingerprintStillThrows` only if it now fails: it uses `unknownFingerprintErrorCode = 1`
(default) → that is "another broker error" now, so it would expect "stale" wrongly. Change its gateway to
`unknownFingerprintErrorCode = 7` (the real broker's code), keep its assertion, and add a `what()` check for `"stale"`.
Report every existing test you had to touch and why.

Run: `cmake --build build-client --target rune_open_topic_gate_tests -j8 && ./build-client/client-cpp/rune_open_topic_gate_tests`
(path: find it with `find build-client -name rune_open_topic_gate_tests -type f`).

---------------------------------------------------------------------------------------------------------------------
## Part C — #226: a typed accessor for the login retry hint

`Completion` is a 40-byte, trivially-copyable record (`static_assert(sizeof(Completion) == 40)`; the BatchAck credit
unit depends on it). Do NOT add a field. Add, in `client-cpp/include/rune/client/Completion.h` right after the
`Completion` struct and its static_asserts:
```cpp
/// Issue #226: the broker's login retry hint (ms) of a Connect refused AuthenticationFailed (a rate-limited login),
/// else 0.  The typed way to read it: Completion::aux carries it only for that op/status/code.
[[nodiscard]] constexpr int64_t retryAfterMs(const Completion& c) noexcept {
    return c.op == OpKind::Connect && c.status == OpStatus::BrokerError && c.errorCode == 44 ? c.aux : 0;
}
```
Update the `aux` field comment to point at `retryAfterMs(c)`.
Check whether batch C4 (already in the tree) added any other place that reports a retry-after to the app (grep
`retryAfter` in `client-cpp/include` and `client-cpp/src`): if a different op (e.g. a login retried in Backoff) now
completes with a retry hint, make `retryAfterMs` cover it and test it; report what you found.

Test (file `client-cpp/tests/client/ClientAuthErrorsTest.cpp`, next to
`ARateLimitedLoginReportsRetryAfterOnTheConnectFailure` ~166):
`ClientAuthErrorsRegression.RetryAfterIsReadableThroughItsTypedAccessorRegression`
- same rig as that test (`retryAfterMsOnFailure = 800`): `rc::retryAfterMs(f.outcome().c) == 800` and `aux == 800`.
- a second rig with `retryAfterMsOnFailure = 0` → `retryAfterMs(...) == 0`.
- constexpr checks in the same test: a `Completion` with op=DefineTopic, status=Ok, aux=5 → 0; op=Connect,
  status=BrokerError, errorCode=45, aux=5 → 0.
- If C4 changed that existing test's expectations (a rate-limited 44 may now retry in Backoff before failing), follow
  whatever the existing test does now to get the failed connect; report it.
- Mutation: return `c.aux` unconditionally → the constexpr checks go RED.

---------------------------------------------------------------------------------------------------------------------
## Part D — #225: one source for the auth error names

`client-cpp/src/cli/QlEngine.cpp` ~133 `errorCodeName`: before the table lookup add
```cpp
    // Issue #225: the auth names have one source, rune::client::authErrorName.
    if (const char* a = rune::client::authErrorName(code)) return a;
```
Include `<rune/client/Completion.h>` (header-only; check `rune_cli_lib` can include it without `RUNE_CLI_LIVE_ENABLED`
— it has `client-cpp/include` on its include path; if Completion.h pulls Aeron-only headers, wrap the line in
`#ifdef RUNE_CLI_LIVE_ENABLED` and say so). Keep the table entries (the static_assert indexes by enum).

Test: add to the CLI test file that already tests `errorCodeName` or `brokerFailure` (grep `client-cpp/tests` for
them; if none, add to `tests/RuneCliOfflineTest.cpp`, target `rune_cli_offline_tests`):
`CliErrorNamesRegression.CliAndClientNamesAgreeFor44To48Regression` — for code 44..48:
`EXPECT_EQ(errorCodeName(code), std::string_view(rune::client::authErrorName(code)))`. Also 43 and 49 still give
the table's names (`"PartitionDataReset"`, `"RecordEgressUnavailable"`).
Mutation: temporarily rename the table's entry for 45 to `"NotAuthorizedX"` AND remove the new line → RED; restore
both. (With the line present, the renamed table entry must still pass — show that too.)

---------------------------------------------------------------------------------------------------------------------
## Part E — #310: sessionRejectCode() follows state()'s threading rule

Today `Client::sessionRejectCode()` (`client-cpp/src/Client.cpp` ~704) returns 0 on any non-duty thread.
Make it work exactly like `state()`:
1. `Client.h` `SeedStatus` (~1390): replace `uint8_t pad[4]{};` with `int16_t rejectCode = 0;  // issue #310: the
   seed's reject code while Rejected` + `uint8_t pad[2]{};` (keep the struct size; add
   `static_assert(sizeof(SeedStatus) == 16);` after it if it was 16 before — check with a quick compile).
2. `publishSeedStatus_` (Client.cpp ~745): set
   `seedStatusLast_.rejectCode = st == session::SessionState::Rejected ? m.rejectCode() : int16_t{0};`
   (inside the block that already reads `m`).
3. `sessionRejectCode()`: drop `noexcept` (declaration ~756 too). Body:
   - duty thread → as today (the machine).
   - not the owner thread → `throw std::logic_error("Client::sessionRejectCode(): call from the owner (constructing) thread or a duty-cycle callback");`
   - owner thread, Thread mode → `(void)seedStatus_.drainLatest(seedStatusCache_); return seedStatusCache_.rejectCode;`
   Note Invoker mode: the owner IS the duty thread, so the first branch serves it.
4. Update the header comment: "Same threading rule as state()".
5. The only caller, `client-cpp/src/cli/Console.cpp:329`, runs in Invoker mode — check it is not inside a `noexcept`
   function or a context that relied on noexcept; report.

Is the Rejected transition published? `publishSeedStatus_` runs when `(st, sid)` changes (~3271), and Rejected is a
state change, so yes — but check that `rejectCode()` is already set on the machine when the state becomes Rejected
(read `SessionMachine.h` where it enters Rejected). If it is set AFTER, report it.

Test (file `client-cpp/tests/client/ClientAuthErrorsTest.cpp`, near
`WrongPasswordFailsConnectWithAuthenticationFailed` ~129, which reaches `SessionState::Rejected`):
`ClientAuthErrorsRegression.SessionRejectCodeFollowsTheStateThreadingRuleRegression`
- Thread mode: build the config with `authCfg(<free stream id pair, grep the file for the highest used>, "ws1",
  "wrong password!")`, then `cfg.runnerMode = rc::RunnerMode::Thread; cfg.nanoClock = nullptr;` (the fake clock is a
  plain global — never share it with a runner thread). Construct `rc::Client` + `test_support::InvokerGateway` like
  `Rig` does, but the drive loop calls ONLY `gw.pump()` (the runner thread drives the client; `client.doWork()` throws
  in Thread mode). Keep `Rig::drive`'s 10 s deadline cap.
- Connect (as `connectOp` does), wait until `client.state() == Rejected` (owner thread).
- From the owner thread: `client.sessionRejectCode() == 44`.
- From a `std::thread`: calling it throws `std::logic_error` (record the outcome in a local bool, join, then assert).
- An Invoker-mode check in the same test using the normal `Rig`: after the wrong-password connect,
  `r.client.sessionRejectCode() == 44` (owner == duty thread).
- Mutation M-E1: restore `return 0` for non-duty threads → the owner-thread assertion RED.
- If a Thread-mode client cannot connect to `InvokerGateway` (it may assume Invoker pacing), say so and instead put
  the Thread-mode half in `client-cpp/tests/client/ClientTest.cpp`, using whatever Thread-mode rejection rig exists
  there (grep `Rejected`); if none exists, report it and keep only: Thread-mode client never connected → owner-thread
  call returns 0, other-thread call throws.

---------------------------------------------------------------------------------------------------------------------
## Final run (report all of it)
- Build only the targets you touched: the producer ack-path test target, the transactional producer test target,
  `rune_open_topic_gate_tests`, the target holding `ClientAuthErrorsTest.cpp`, the CLI test target, the `rune` CLI
  executable, and `rune_client_api`.
- Focused tests only (NO full suite — project policy): run those executables (filters: `ProducerSequence.*:ProducerAcks.*`,
  the transactional producer binary whole, `OpenTopicGate*`, `ClientAuthErrors*`, the CLI binary whole). Paste each
  summary line and the command.
- `git diff --stat` and `git status` (no stray files; nothing under `.superpowers/`, no logs).

## Report format
Per part: files changed; each test with its RED line (before fix) and GREEN; each mutation and its RED line; every
existing test you changed and why; anything you could not do and why. Keep it factual.
