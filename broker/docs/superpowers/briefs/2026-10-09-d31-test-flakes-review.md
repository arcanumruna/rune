# D31 test-flake commits review (e050971..6882143)

Branch `integ/perf-defaults`, worktree `rune-broker-wt26`. Read-only review; nothing built or run.

**Verdict: APPROVE WITH CHANGES.** No CRITICAL or MAJOR findings. There is one MEDIUM: the keepalive fix
leaves the same reap window open across the blocking `controlledShutdown()`/`stop()` calls. All four
root causes are correctly diagnosed. None of the fixes masks the bug its test exists to catch.

| # | Sev | Commit | Location | Finding |
|---|-----|--------|----------|---------|
| 1 | MEDIUM | 53f0ce9 | ShardTestClient.h:396-418; ControlledShutdownRegressionTest.cpp:326,401 | Keepalives fire only from `pause_()`. The test thread blocks silently in `controlledShutdown(20'000ms)` and `stop()`, so a lease reap can still happen there. |
| 2 | LOW | eda074a | UringDurabilityTest.cpp:277-291 | The `iou-wrk-` prefix also matches io-wq workers left over from earlier tests when the whole binary runs in one process. It is correct only because the last match wins and tids increase. |
| 3 | LOW | 28fb2c1 | ScopedNoDacOverride.h:30 | A range-for over a braced list needs `<initializer_list>`, so the header is not self-contained. |
| 4 | LOW | 28fb2c1 | ScopedNoDacOverride.h:28,31 | A failed capget/capset turns the guard into a silent no-op, so a root run then fails with a misleading "acked"/"no throw" message. |
| 5 | LOW | 53f0ce9 | ShardTestClient.h:137 | `sessions_` is never pruned on `disconnect()`. A stale session id keeps getting keepalives (harmless, since `touch` is a no-op) and the vector only grows. |
| 6 | LOW | 6882143 | ShardModeTest.cpp:1437-1444 | Retries do not back off when the metadata is unchanged. In the RED case a batch can take about 75 s to fail (5 attempts x 3 produces x 5 s send cap). |

---

## 1. 53f0ce9: ShardTestClient keepalive

### Checked and correct
- **No recursion.** `send()` calls `pause_()`, which calls `keepalive_tick_()`. `keepalive_tick_()` only does
  `pubs_.find` plus a raw `Publication::offer`. It never calls `send()` or `pause_()`, so there is no re-entry.
- **`pubs_` lookup while `send()` inserts.** `send()` holds `auto& pub = pubs_[{ch,st}]`. `std::map` references
  survive other inserts, and `keepalive_tick_` uses `find` (no insert). The entry `send()` is still resolving
  is null, so the `!it->second` guard skips it. If the entry is the same publication `send()` is retrying,
  the Keepalive is one extra frame on the same non-exclusive publication from the same thread. That is fine.
- **Fresh correlationId.** The gateway's `handleKeepalive` (ClientGateway.cpp:2232) only does
  `sessions_.touch` and sends no response, and `SessionTable::touch` ignores unknown ids. So nothing lands in
  `acks_`, `defines_`, `errors_`, `metadata_` or `connect_`. `++corr_` only advances the shared counter, and
  every waiter matches on its own correlation id.
- **Exact frame-count tests.** The flag is off by default. `corr_` and the wire are untouched when it is off.
  The only behaviour change with the flag off is the `sessions_.push_back` in `connect()`, and the return
  value is identical. `keepSessionsAlive` is used only in ControlledShutdownRegressionTest. None of those tests
  counts frames, pause counters or credits. The ShardMode back-pressure tests (offerUntilPaused, D8 credits,
  silent-client lease) keep their silent-client model.
- **Offers to a stopped node.** These return NOT_CONNECTED or BACK_PRESSURED and are ignored. A missed
  keepalive is retried 1 s later, against a 10 s lease.
- **Regression test.** It idles for lease + 2 s through `pollUntil`, which calls `pause_()`, then queries
  metadata on the same session. That is a sound RED/GREEN discriminator. MetadataRequest only touches a known
  session, so a reaped session does not come back to life.

### MEDIUM 1: keepalive only covers waits inside the client, not blocking broker calls
The fixed gap was the bounded drain `pollUntil([]{return false;})` (kMaxIters x pause_, so 5 s or more, and
about 10 s on a loaded host) plus the stop. The drain is now covered. Two calls still block the test thread,
and no `pause_()` runs during them:
- `ns[victim]->controlledShutdown(20'000ms)` at ControlledShutdownRegressionTest.cpp:330 and :405. Its budget
  is twice the 10 s lease. The sessions' last touch is the final `produceAll`.
- `ns[victim]->stop()` at :354 and :427.

Failure scenario: on the box at -j3, the controlled shutdown takes more than about 10 s (well inside its own
20 s budget). Every survivor reaps the client's sessions. `expectNeverSentToVictim(..., "after the controlled
shutdown, before exit")` then fails with "node N answered no metadata". This is the D31 symptom again, at a
different call site.

A real client sends keepalives from its own thread, no matter what the application thread is doing. Fix: run
the blocking broker call on `std::async` and pump the client while it runs. For example, add a helper
`c.pumpWhile([&]{ return fut.wait_for(0s) != ready; })` built on `pollUntil`. Use it for both
`controlledShutdown` and `stop` in the two graceful-stop tests.

### LOW 5
`connect()` appends to `sessions_` on every successful connect, and nothing removes the entry on
`disconnect()`. This is harmless today, because a Keepalive for an unknown session is a no-op. It does
diverge from the model the doc comment describes ("every session this client connected"). Prune in
`disconnect()`.

## 2. 28fb2c1: ScopedNoDacOverride

### Checked and correct
- **Does capset with pid=0 affect only the calling thread?** Yes. On Linux, capabilities are per thread
  (`task->cred`). Since 2.6.24, capset can only target the caller (pid 0 or its own tid), and the raw syscall
  has no glibc setxid-style broadcast. Other threads keep CAP_DAC_OVERRIDE.
- **Is the guarded work done on the same thread?**
  - SegmentLifecycle: `put()` runs the inline segment create synchronously. Provisioning goes through
    `DemandChannelForTest` with a no-op hook, and `provisionReady` is called on the test thread. No
    background provisioner can create the segment while it still holds root caps.
  - InternalIsr: `offer_()` calls `persist_()`, which does the open/fsync/rename and the directory `open` and
    `fsync` synchronously (InternalIsrKnowledge.cpp:178-216).
- **Can capset fail partially? Does the restore always work?** One capset call is all-or-nothing. Only the
  *effective* set is reduced and *permitted* is untouched. Raising effective back up to a subset of permitted
  never needs privilege, so the restore cannot fail with EPERM. The restore also runs during unwinding if
  `fs::permissions` throws.
- **Right capability set.**
  - The InternalIsr directory is mode `-wx`. `open(O_RDONLY|O_DIRECTORY)` needs read, which both
    DAC_OVERRIDE and DAC_READ_SEARCH bypass, and both are dropped.
  - SegmentLifecycle needs write, which DAC_OVERRIDE bypasses.
  - The chmods inside the scope still work without FOWNER, because the process (root) owns the directory.
- **Non-root.** The capset is a no-op, which is correct. On macOS the whole thing compiles out, which is
  fine for a non-root dev host.
- **Other tests needing the same fix.** These are the only two chmod fault injections in `libs`.

### LOW 3
`for (int cap : {CAP_DAC_OVERRIDE, ...})` is a range-for over a braced-init-list. It needs
`<initializer_list>` in scope, and GCC rejects it otherwise. Today it compiles only because gtest is
included first. Add `#include <initializer_list>`.

### LOW 4
If `capget` or `capset` fails, for example because a seccomp profile or LSM denies capset, the guard silently
does nothing. As root the test then fails with "acked a state whose directory entry was never fsynced" or
"no throw" instead of saying the injection never happened. This cannot cause a false GREEN, but it is
misleading. Expose `bool active() const`, and have the tests `GTEST_SKIP` or `ADD_FAILURE` with a clear
message when `geteuid()==0 && !active()`.

## 3. eda074a: io-wq worker matched by prefix

### Checked and correct
The root cause is right. The kernel names the worker `"iou-wrk-%d"` with `wq->task->pid`, which is the
global (init-namespace) tid. Inside a container that is not `gettid()`. `submit()` enters the kernel at
once, so the fsync is punted and the worker is created synchronously inside `io_uring_enter`. The worker
therefore exists by the first scan at i=0.

### LOW 2: the "only submitter in the process" claim is false for a whole-binary run
ctest uses `gtest_discover_tests`, which runs one process per test, so the ctest/box path is fine. When the
binary runs directly (an agent's targeted run, `--gtest_filter=UringDurabilityTest.*`, or shuffle), earlier
tests behave differently:
- SubmitThenReap, GroupCommit and AckOnlyAfterCompletion run on the gtest main thread.
- Their fsyncs are always punted to io-wq.
- io-wq belongs to the *task*, not the ring. The last worker per acct never exits on idle timeout
  (`last_timeout && nr_workers > 1`), and io-wq is torn down only when the task exits.

So a stale `iou-wrk-<main tid>` with the main thread's full CPU mask is still alive. The scan does not stop
at the first match: it overwrites `allowedOut` for every match, and the new worker has the higher tid. That
makes the result correct only by accident (last match wins, tids increase). If the tid wraps, or the
directory order differs, the scan reports the stale `0-N` and the test goes falsely RED. It can never be
falsely GREEN, because the stale mask is not `{cpu}`.

Fix: compute the exact name. Read the first field of the `NSpid:` line in `/proc/thread-self/status` (the
init-namespace tid) and match `"iou-wrk-" + thatTid`. Alternatively, snapshot the iou-wrk tids that exist
before `submit()` and exclude them. Also correct the comment.

## 4. 6882143: ShardMode advertised-ingress re-route

### Does the retry mask F25? No.
The test exists to prove that both routes derived *from the MetadataResponse* reach a live subscriber. The
F25 bug left the (leader node endpoint, advertised stream) pair with no subscriber, so it was NOT_CONNECTED
forever. With the retry:
- Every route still comes only from `c.metadata(...)`. Nothing is hard-coded. The added
  `sidOn[pi.leaderId]` connect also goes to the advertised leader's gateway.
- A broken route stays broken across all 5 attempts, because the metadata re-read returns the same
  (leader, stream, endpoint) unless leadership really moved. So `ASSERT_TRUE(ack)` still fails.
- `produceUntilOk` returns only OK acks, so an error ack never counts as success.

The retry can only hide a bug when a broken route happens to be replaced by a moved, working one within 5
attempts. That needs a real leadership move plus a bug confined to the old leader, which is not the F25
class of bug.

### LOW 6
There is no backoff between attempts while the metadata is unchanged. In the RED case each attempt can cost
`3 x (send cap kMaxIters x 100 us = 5 s)`. That is about 75 s before the assertion fires, which only adds
time to failing runs. A short sleep, or retrying only when `fresh` differs, would bound it. This is
optional.

## Do other tests rely on the old behaviour?
- **ShardTestClient.** `connect()` returns the same values as before. Keepalive is off by default.
  `corr_` and the wire are unchanged with the flag off.
- **ScopedNoDacOverride.** Only the two wrapped tests use it.
- **Uring.** The test is self-contained.
- **ShardMode.** The change is local to the one test.

None of them changes behaviour that another test depends on.
