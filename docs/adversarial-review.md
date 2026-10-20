# Adversarial review

How to try to break the area of a fix before its PR merges. The implementer applies this before opening the PR, and
the reviewer applies it again independently. It applies to every PR that fixes a GitHub issue.

## Mindset

- **Assume the fix is wrong and set out to prove it.** A finding is a failing test, not an opinion.
- **Attack the area, not the diff.** The area is the module the fix lives in, every entry point that reaches the same
  state, and every invariant those paths share.
- **Find the sibling paths first.** The same state is usually reachable from the client gateway, the admin channel,
  replication catch-up, recovery on boot and the CLI. A fix that guards one entry point and misses another is the
  most common miss.
- **Re-run the issue's original repro at every layer:** unit, integration, cluster, and e2e where one exists.

## Attack classes

Go through each class and write at least one test for every one that applies to the area.

| Class | Try |
|---|---|
| **Input bounds** | Lengths 0, 1, max and max+1. u16 and u32 wrap. An empty field against an absent one. Invalid UTF-8. Duplicate ids. Unknown enum values. Trailing bytes. A frame truncated at every byte offset (a sweep). |
| **Protocol state** | A request before Connect or before auth, after Close, or on another session's id. Connect sent twice. Out-of-order sequence numbers. A replayed nonce, SCRAM proof or request. |
| **Version skew** | An old client against a new broker and the reverse. A mixed-version cluster in the middle of a rolling upgrade. A feature used by a client that never negotiated it. |
| **Concurrency** | The same operation from two sessions at once. The operation racing DDL (drop, alter, schema change), a leader change, a group rebalance or a session timeout. Run it under the TSan build. |
| **Crash and restart** | Kill at every durable step: before and after fsync, and between the log append and the index, catalog or `__security` update. Then restart and recover. Replay `__catalog` and `__security` on boot. Disk full. A slow or lagging follower. |
| **Network (rUDP)** | Packet loss, reordering, duplication and delay. A partition healing in the middle of an operation. A peer that goes silent without closing. |
| **Auth** | Every operation with no credentials, wrong credentials, or a grant revoked or a role removed while the session is open. Escalation through a sibling operation, such as getting ALTER's effect via CREATE. Bypassing a throttle or limit through another entry point. |
| **Resources** | Many sessions, subscriptions or groups. Oversize filters and schemas. A slow consumer that never drains. Memory and queues must stay bounded, with explicit backpressure or rejection, never a hang. |
| **Data integrity** | No silent loss or duplication: everything produced is consumed, offsets are monotonic, and a materialized view equals a recompute from the log after a restart. Use model-checked or seeded randomized tests. |
| **Hot path** | If the area is on the produce, consume or query path: an allocation-counting test proves zero allocations, there is no new lock or virtual call, and an ABAB microbenchmark shows no regression. |

## Test rules

- **Assert a concrete outcome:** the error code, the state, or the exact data. "Doesn't crash" counts only when the
  crash is the bug.
- **Mutation check:** revert the fix and confirm that at least one attack test turns red. If none does, the attack
  missed the fix.
- **Put the attack in the name:** for example `…TruncatedAtEveryOffsetAdversarialRegression`. The suffix `Regression` keeps
  the test in the suite.
- **Determinism:** no sleeps for synchronisation. Wait on events, with generous deadlines. Seed every randomized test
  and print the seed when it fails. Run new tests with `ctest --repeat until-fail:20`, and once under parallel load
  (`-j`), to surface flakes before merge.
- **Use the fastest layer that reproduces the failure.** Go up a layer only when the bug needs it.

## When something breaks

- **A break in one of the PR's own issues:** fix it in the PR, and keep the test.
- **An urgent break elsewhere** (a hang, silent loss or a security hole): report it at once. It gets its own fix task.
- **Anything else:** file a labeled GitHub issue (see `CLAUDE.md` §5) that carries the failing
  test as its repro, with Origin "found by adversarial review of PR #N". The test stays out of the suite until the
  issue is fixed.
- **Passing attack tests** stay in the suite as coverage.
