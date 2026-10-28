# Rune — working agreement for AI sessions

Human-readable version: the wiki [Contributing](https://github.com/arcanumruna/rune/wiki/Contributing) page.

Rune is an ultra-low-latency durable log and database (log + materialized views) for MMORPG backends. Low latency is
the overriding requirement; every rule below exists to protect it. These rules override defaults in every subproject.
Subdirectory `CLAUDE.md` files add area-specific detail only.

**Layout:** `broker/` (gateway, node, workers, replication, views, auth), `protocol/` (header-only wire and record
formats), `codec/` (typed schemas, evolution, catalog, filters), `client-cpp/` (C++ client), `ql/` (Rune QL),
`crypto/`, `docs/`. Public repo: `github.com/arcanumruna/rune`. The flight recorder is a separate repo.

Build, test and benchmark commands: [README.md](README.md#build), [docs/testing.md](docs/testing.md),
[broker/bench/BENCHMARKING.md](broker/bench/BENCHMARKING.md).

---

## 1. Engineering philosophy (hard rules — check every design, fix and review against all 16)

**Core design**
1. **Shared-nothing, strictly.** No shared mutable state, no locks, no atomics as coordination, no `thread_local`.
   Each thread/component owns its data outright.
2. **Ring buffers are the ONLY cross-thread event passing** — Aeron's own ring buffers, never shared memory or
   cross-thread callbacks.
3. **Rich-state state machines.** Each state holds exactly the fields it needs (e.g.
   `PUBLISHED(buffer, destination, publisher, counter)`, `BEHIND(...)`, `INSYNC(...)`), never scattered booleans.
   Every edge case is one explicit state with one code path; every state handles every event.
4. **Zero allocations on the hot path and in steady state.** Preallocate, reuse buffers, fixed stack buffers; no
   per-cycle `std::vector` returns, `std::function` or dynamic containers on the fast path.
5. **No virtual calls on any hot path.** Concrete types plus a branch, or templates.
6. **Only cache-friendly, compact, low-latency data structures**, and every optimization is benchmark-validated.

**Testing and correctness**
7. **Test everything, benchmark everything.** Unit + integration + e2e + benchmarks; no hand-waving.
8. **Every fix ships a regression test** named `*Regression*` so they run together.
9. **Failing repro test first**, at the fastest deterministic layer; then fix; then show it green.
10. **Affected-area tests on every task and fix round; the FULL suite at checkpoints** (see §3).
11. **Never call anything done until its REAL e2e path is green** — a real client against a real broker, on the
    DEFAULT config. In-process or InMemory tests do not count; test the default, not only the flag-on case.
12. **Validate fixes with the regression suite AND performance runs.** Never blame "noise": prove a perf claim with
    interleaved A/B runs and name the exact code path.

**Ways of working**
13. **You find it, you fix it.** Pre-existing, other-subsystem or out-of-scope is no excuse: own every red test to
    green. Never diagnose-and-stop, never offer to skip, never ask "fix or not?". Fix philosophy violations on sight
    in any file you read.
14. **Diagnose bugs yourself.** Root-cause diagnosis is never delegated to a subagent; delegate only mechanical work.
15. **Diagnose with logs, not assumptions.** Instrument at component boundaries, run once, localize the hop, fix.
16. **Time-independent convergence.** Failover and leadership converge from ISR + idempotent restore for ANY timing
    window — never from a tuned grace period or timeout constant.

## 2. Aeron/Agrona is the reference implementation

Every subproject mirrors Aeron Cluster's implementation exactly when in doubt:
- **Threading:** single-writer agents driven by `doWork(nowNs)` under an `IdleStrategy`; each piece of state owned by
  one thread; hand-off only through Aeron ring buffers (`ManyToOne` / `OneToOne` / `Broadcast`).
- **Encoding:** flat SBE-style fixed layouts, little-endian, reusable encoder/decoder fields, zero-copy via
  `tryClaim` / `BufferClaim`.
- **Memory ordering:** single writer publishes with a release store, readers use an acquire load; cache-line padding
  against false sharing. Under TSan, cross-thread words use `std::atomic_ref` (same codegen), never broad suppressions.
- **Time:** one injected, advanceable monotonic clock. Tests advance the clock to the next deadline and call
  `doWork` deterministically — never wait on wall time, never `while (cond) doWork()`. Every drive loop has a hard
  iteration cap and fails rather than hangs. At runtime an idle agent backs off through its `IdleStrategy`, never a
  bare spin.

**Latency budgets (hard gates, p99, default config):** view update from one event batch < 10 µs; single-node point
read < 50 µs end-to-end; multi-node query < 200 µs; QL text → wire and wire → plan ≤ 2–5 µs each. A benchmark over
budget is a failing gate, not a note. These are single-node hot-path figures: the co-located 3-node acks=all bench has
its own baselines in [BENCHMARKING.md](broker/bench/BENCHMARKING.md) — compare against those before calling a
regression.

**Hot-path test:** before adding anything, ask "is this on the produce, consume or query path?" If yes, it is
zero-alloc, shared-nothing, ring-only, no-virtual and benchmarked. Control-plane and admin work (DDL, commits, I/O)
goes off the hot path onto its own worker.

## 3. Testing policy

- **Interaction matrix.** Every feature is tested across its full matrix: every operation × every related or
  conflicting entity × every timing (sequential, back-to-back before the reply, mid-operation at each async state,
  leader change mid-operation, restart in between). That means table-driven unit tests (hundreds of cases), a real
  broker integration slice (1-node and 3-node failover), an e2e leg, and a randomized model-checked stress test where
  state is shared. Every plan has an "Interaction matrix" section; reviewers check the matrix, not only the diff.
- **Per task / fix round:** build, then run the test executables covering the touched code plus every new or changed
  test.
- **Per batch:** only the affected-area tests (the executables covering the touched code plus every new or changed
  test). No full suite per batch.
- **Big checkpoints only** (every 3–4 batches, and before a final whole-branch review): the full
  `*Regression*` suite (`-R Regression -LE latency`), plus the same suite under ASan+UBSan and under TSan (broker and
  client; a client TSan build needs `-DBUILD_TESTING=ON`). A checkpoint failure is bisected over the commits since
  the last green checkpoint. After fixing checkpoint failures, re-run only the failed and affected tests.
- **Sanitizer reports are findings:** fix them like a failing test, with a regression test. On macOS x86_64
  (Rosetta) run ASan with `detect_leaks=0` and check leaks with `leaks(1)`.
- **No silently weakened tests.** A deviation that drops or weakens an assertion the plan requires is unfinished
  work. Record every one; fix the ones guarding a hang, loss, security hole or broken feature now, and file the rest
  as issues.
- **Latency gates** (`-L latency`) run on an idle host only.

**Docker e2e and dr-e2e run on the shared Linux test VM, never on local Mac docker** (amd64 emulation is slow and
gives false failures). On that VM:
- hold the shared lock for the whole run;
- work only in your own directory;
- remove only containers and images you created; never `docker system prune`;
- check `df` before and after;
- never restart the machine.

The host comes from `$RUNE_VM_HOST`. Its address and login are private to the operator and are never written into
the repo, issues or commits.

**Benchmarks.** Follow [BENCHMARKING.md](broker/bench/BENCHMARKING.md):
- Prepare the clean box once (stop k3s, flush iptables, `lo` NOTRACK, sysctls, Release build, logs and flight
  recorder off) and restore it once.
- Run the one bench that answers the question: baseline against tip, interleaved ABAB.
- n=3 is a quick check. A verdict needs the BENCHMARKING.md counts (≥5 for latency, ≥3 for throughput), and n≥15
  before claiming a small p99 shift.
- Bisect only on a confirmed regression.
- Record new baselines in the table there.

## 4. Scope and fix discipline

- **Good-enough scope.** The goal is a working version across all planned phases, not a hardened one. Fix rounds
  cover urgent issues only: hangs, silent data loss, security holes, or a feature not working. Everything else (weak
  tests, coverage gaps, flakes a rerun clears, docs, cleanup) is filed as an issue (§5).
- **Optimal fix or defer.** Choose the optimal correct fix, never a stopgap. If it does not fit now, defer the whole
  item.
- **Rule on the whole bug class.** Before a fix, list every component, path and client type that can hit the same
  failure (groups, plain subscriptions, coordinator, client) and fix them all in one round.
- **One fix round, one re-review.** A new silent-loss or hang path found after that is fixed immediately; anything
  else becomes an issue.
- **Make obvious choices yourself** and record them. Ask only for irreversible or outward-facing actions, or a real
  product decision with no obvious answer.
- **Plans** have 1–3 phases of small tasks. Each task carries its own tests. Plans include an Interaction matrix and
  a Review Focus section.

## 5. GitHub issues (`arcanumruna/rune`)

- Non-urgent findings (review minors, parked items, test gaps, flakes, tech debt) go straight to a GitHub issue when
  they are parked. Urgent blockers are not issues: they get their own fix task or phase and are fixed first.
- **Every issue has exactly one type label, one severity label and at least one area label:**
  - type: `bug` | `enhancement` | `tech-debt` | `test-gap` | `flaky` | `docs`
  - severity: `sev:low` | `sev:medium` | `sev:high`
  - area: `area:broker`, `area:client`, `area:protocol`, `area:codec`, `area:ql`, `area:views`, `area:auth`,
    `area:crypto`, `area:replication`, `area:storage`, `area:bench`, `area:ops`, `area:ci`

  A hook in `.claude/settings.json` refuses `gh issue create` without all three.
- **Body:** Description / Steps to reproduce / Expected / Actual / Test reference (test name and repo-relative path,
  or seed/env repro) / Origin (which review or task found it).
- Dedupe first with `gh issue list --search`. Use repo-relative paths only; never emails, IPs, hostnames, tokens or
  local absolute paths.
- **Keep status current.** Comment when work on an issue starts. Close it when fixed:
  `gh issue close N -c "Fixed in <sha>: <one line> (regression: <test>)"`. If it is only partly fixed, comment on
  what is left.

## 6. Git, commits and secrets

- **Pushes to working branches need no per-push consent** (the owner's standing permission). Never push directly to
  `main` and never force-push without an explicit request.
- Work on a branch, never directly on `main`. Create every branch off the latest `main` (`git fetch` first) and keep it
  current by merging `main` in before review.
- Pull request branches are prefixed by kind: `feature/<topic>` for new functionality, `bugfix/<topic>` for fixes.
- **Pull requests are reviewed by the most capable model.** The reviewer leaves comments on the PR where needed; the
  implementer resolves each one (fix, or reply with the reason) and pushes. When the reviewer is satisfied it approves,
  merges the PR and closes the linked issues.
- **Issue-fix PRs: whole-area and adversarial review.** For a PR that fixes GitHub issues, the review covers the
  entire area each fix touches, not just the diff: the module it lives in, its callers, sibling paths that share the
  invariant, and their tests. **Once per batch, at the very end** (after every fix is in), the reviewer writes tests
  meant to break the union of the areas the batch touches, driven through the public API (client API, wire frames,
  admin/CLI). An area shared by several fixes is attacked once; the implementer does not run a separate pass; after a
  fix round only the areas the new diff touches are attacked again.
  - **Mindset:** assume the fix is wrong and prove it with a failing test. Find every sibling entry point to the same
    state first (gateway, admin channel, replication catch-up, boot recovery, CLI). Re-run the issue's repro at every
    layer.
  - **Attack classes — go through every one that applies:** input bounds (0/1/max/max+1, wrap, truncation at every
    offset); protocol state (before auth, after close, replays, out of order); version skew (old ↔ new, mixed cluster);
    concurrency (same op twice, racing DDL, leader change, rebalance; under TSan); crash and restart at every durable
    step; replication and log state (stale-leader fencing, divergent-follower truncation, acks=all with no loss or
    duplicate, offsets never move back, system topics converge); compaction, retention and schema evolution;
    rUDP faults (loss, reorder, duplicate, partition); auth (no or wrong credentials, revoked mid-session, escalation
    through a sibling op); resources (bounded memory and queues, backpressure, never a hang); data integrity (produced
    = consumed, views = recompute from log); hot path (zero allocations, ABAB bench).
  - **Test rules:** assert a concrete outcome; mutation check (revert the fix → an attack test turns red); name it
    `…Adversarial…Regression`; no sleeps, seeded randomness, `--repeat until-fail:20` and one run under `-j` load.

  Passing tests are kept as `Regression` tests. A break in one of the PR's own issues is fixed in the PR. A break that
  is urgent (a hang, silent loss or a security hole) is fixed at once in its own task. Any other break becomes a labeled
  GitHub issue whose body carries the failing test as its repro; that test stays out of the suite until the issue is
  fixed, so the suite stays green.
  The full playbook with examples per class is `docs/adversarial-review.md`.
- Commit messages end with the session's `Co-Authored-By:` trailer.
- **No secrets in the repo:** no emails, passwords, tokens, keys, IPs or local paths. Everything sensitive comes from
  the environment.
- Never commit scratch workspaces (`.superpowers/`, build logs).
- Never run `conan create/export/upload` locally — CI publishes packages.

## 7. Agent workflow

- Plans are executed task by task: one implementer per task, then a task review (spec compliance + code quality),
  then a whole-branch review at the end.
- **Implementers** run on a mid-tier model. They are given an exact recipe: where, what (the algorithm and its
  invariant), every call site, the exact tests (setup, action, expected values, and the mutation that turns each one
  red) and the don'ts. They are never handed design decisions.
- **Agents on GitHub issues.** Every agent dispatched to review an issue-fix PR is told, in its dispatch, to read
  `docs/adversarial-review.md` first and to apply every attack class that fits the batch's areas. Its report lists
  each class with the tests written or why it does not apply, and the issues it filed. Fixers are told the pass
  happens at review, so they do not repeat it.
- **Reviewers** run on the most capable model. A review is exhaustive and adversarial in one pass: every path, state
  transition and input class, concurrency, lifetimes, bounds, vacuous tests. It reports every bug, not the first
  few. A small scoped re-review can use a mid-tier model.
- After the final review, urgent fixes are verified by tests only, with no further review round.
- Implementers never spawn subagents. At most 3 agents run at once. Never stop a running agent unless the owner says
  so.
- Long remote runs go in the background, so the session stays responsive.
- Bulk mechanical work (filing a batch of issues, relabelling): verify the first item, run the rest without
  per-item updates, and give one summary at the end.

## 8. Product direction notes

- **Query API:** Rune QL will get its own native query API, and the SQL and Redis interfaces will be thin facades
  translated onto it. Until then, keep the existing view read API (`getViewRow`). Don't start query-API work unasked.
- **Catalog:** `__catalog` is a replicated DDL event log (fingerprints only, no state).
- **Auth:** SCRAM, a `__security` log, allow-only grants with default deny, and a cluster key.
- **Namespaces (tenants):** `broker/docs/superpowers/specs/2026-10-24-rune-namespaces-and-truncate-design.md`.
  Anonymous read is off by default; a cluster admin turns it on for `default` explicitly.
  Phase 1 is done (namespace DDL, scoped names, permissions, listings); limits are stored only; groups and
  transactions work in every namespace (Phase 2).
