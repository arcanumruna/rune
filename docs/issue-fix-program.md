# Issue-fix program: batches, waves and runbook

The root `CLAUDE.md` (and `broker/`, `client-cpp/` CLAUDE.md) is binding; this file adds the program's process, the
batch plan and its state. It replaces the sev:medium-only runbook (medium batches M1–M28).

## 1. Shape: big batches, small waves

- **Order:** sev:high first, then sev:medium, then sev:low. A batch may pull in lower-severity issues that live on the
  same code path as its higher-severity ones (the whole bug class, `CLAUDE.md` §4), so the area is reviewed and
  tested once.
- **Batch:** one branch `bugfix/issue-batch-<n>-<topic>` off the latest `main`, one PR, as many issues as stay
  coherent. The expensive steps (full suite, sanitizers, whole-branch review, adversarial pass, docker e2e) run
  **once, at the end of the batch**.
- **Wave:** one code path inside a batch (the tables in §4). Waves run in sequence; within a wave, issues are fixed
  one commit each. Each wave ends with only a focused review and focused tests.

## 2. Lifecycle of a batch

1. **Open.** Comment "Work started (issue batch <n>, wave <w>)." on every issue of the batch. Merge `main` in.
2. **Per issue** (implementer, from the lead's recipe): failing `*Regression*` test first at the fastest layer → fix →
   green → mutation check (revert the fix, the test turns red). One commit per issue, with the session
   `Co-Authored-By:` trailer. Build and run only the test executables covering the touched code plus the new tests.
3. **End of wave — focused, not full:**
   - focused tests: the executables of the wave's area, and the new tests with `--repeat until-fail:20`;
   - focused review: one reviewer reads the wave's diff plus its area (module, callers, sibling entry points, their
     tests) and checks the interaction matrix. One fix round, re-checked by tests only. No adversarial pass, no full
     suite, no sanitizer build.
   - non-urgent findings become labeled issues and are appended to a later batch; urgent ones (hang, silent loss,
     security hole) are fixed in the wave.
4. **End of batch — the checkpoint, everything once:**
   - merge `main` in;
   - full `*Regression*` suite (`broker/scripts/run-regression.sh`), plus ASan+UBSan and TSan (broker and client; CI
     `sanitizers.yml` result may stand in);
   - whole-branch review by the most capable model, with the adversarial pass of `docs/adversarial-review.md` over
     the union of the batch's areas (each area once);
   - docker e2e and auth-e2e on the shared VM when `$RUNE_VM_HOST` is set (`CLAUDE.md` §3); otherwise add the batch to
     tracker #474;
   - one fix round, then re-run only the failed and affected tests. A checkpoint failure is bisected over the batch's
     commits. A flake that a rerun clears is filed (`flaky`) and does not block.
5. **Merge.** Merge commit; close each issue with
   `Fixed in <sha>: <one line> (regression: <test>)`; never delete branches.

## 3. Roles and limits

- The lead diagnoses and designs; diagnosis is never delegated. Implementers get an exact recipe (where, algorithm and
  invariant, every call site, exact tests and their mutations, don'ts). Sonnet for simple recipes, Opus for complex.
- Wave reviews: a fresh reviewer per wave; a mid-tier model is enough for a small, mechanical wave. Batch-end review:
  the most capable model, always.
- At most 3 agents at once, one agent per build dir, one build dir (`cmake -B build -DRUNE_TLS=OFF`; a separate
  client-cpp build with `-DBUILD_TESTING=ON` for client unit tests). Check `df` first (~18 GB). Scratch dirs
  `/tmp/rune_t_<batch>_*`, deleted by exact name. Never `conan create/export/upload`.

## 4. The batches

Built 2026-11-03 from all 458 open issues; every open issue is in exactly one row. Severity in bold is high/medium.
Issues filed later are appended to the wave that owns their code path, or to the next batch if that one has started.

### Batch 1 — sev:high and its bug classes (22)
Verdict pipeline (checked writes 04 on `main`) and view checkpoint recovery. `feature/checked-writes-05-*` merges this
batch in when it lands.

| Wave | Code path | # | Issues |
|---|---|---|---|
| 1A | judge activation/restore | 5 | **high** #791; **medium** #785 #788; low #787 #807 |
| 1B | verdict timeline fail-open | 2 | **high** #808; **medium** #809 |
| 1C | verdict config/hot path/client | 4 | low #802 #803 #804 #806 |
| 1D | checked-writes tests/docs | 6 | **medium** #797; low #798 #799 #800 #801 #805 |
| 1E | view checkpoint/recovery | 5 | **high** #590; low #75 #159 #163 #591 |

### Batch 2 — sev:medium (87)
2A goes first, so that the batch-end full suite is green. 2B takes over the unreviewed M28 commits of PR #619 (that PR
is then closed as superseded).

| Wave | Code path | # | Issues |
|---|---|---|---|
| 2A | red-on-main and load flakes | 10 | **medium** #456 #581 #777 #778 #783 #784 #793; low #69 #427 #781 |
| 2B | peer link (absorbs PR 619) | 9 | **medium** #274 #275 #543 #554; low #540 #545 #546 #683 #774 |
| 2C | namespaces | 39 | **medium** #643 #644 #645 #646 #652 #661 #662 #707 #709 #712 #724 #730; low #625 #626 #627 #637 #639 #640 #647 #648 #649 #650 #653 #663 #696 #697 #708 #710 #711 #713 #714 #715 #716 #721 #722 #723 #727 #728 #729 |
| 2D | client egress/transport/txn | 22 | **medium** #718 #732 #749 #767; low #733 #734 #735 #737 #738 #743 #744 #750 #751 #754 #755 #756 #762 #763 #764 #765 #766 #769 |
| 2E | views/storage/metrics/CI | 7 | **medium** #632 #658 #706 #726; low #629 #631 #796 |

### Batch 3 — sev:low, auth control plane (77)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 3A | login/throttle/session gate | 21 | low #52 #66 #133 #145 #181 #195 #216 #219 #227 #237 #238 #262 #263 #265 #266 #267 #268 #270 #477 #523 #594 |
| 3B | permissions shipper/mirror/__security | 23 | low #198 #199 #200 #201 #202 #204 #207 #208 #264 #269 #280 #281 #284 #285 #286 #411 #442 #444 #457 #465 #518 #608 #775 |
| 3C | DDL/catalog authorization | 33 | low #2 #40 #67 #83 #146 #147 #186 #190 #215 #217 #218 #220 #221 #223 #224 #229 #230 #234 #255 #289 #415 #424 #425 #500 #504 #505 #506 #524 #527 #536 #579 #586 #666 |

### Batch 4 — sev:low, crypto, peer link, ISR/consensus (50)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 4A | crypto and secret wiping | 14 | low #59 #174 #176 #177 #183 #193 #244 #250 #256 #258 #282 #283 #542 #677 |
| 4B | peer link transport | 16 | low #65 #139 #140 #243 #245 #246 #247 #248 #249 #251 #252 #253 #279 #287 #288 #541 |
| 4C | ISR/consensus | 20 | low #3 #7 #84 #85 #86 #87 #88 #89 #90 #91 #92 #94 #95 #96 #100 #101 #272 #391 #550 #602 |

### Batch 5 — sev:low, broker core (68)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 5A | gateway/sessions/egress | 20 | low #6 #179 #191 #312 #401 #417 #437 #502 #560 #615 #638 #752 #753 #757 #758 #759 #760 #761 #768 #773 |
| 5B | filters/subscriptions | 15 | low #412 #414 #416 #420 #421 #430 #447 #448 #449 #451 #452 #453 #463 #483 #484 |
| 5C | groups/txn/lineage | 11 | low #20 #164 #165 #166 #167 #168 #366 #563 #566 #568 #595 |
| 5D | catalog/DDL/storage/RC | 22 | low #74 #79 #98 #142 #144 #148 #171 #172 #173 #210 #292 #293 #294 #295 #297 #298 #299 #300 #558 #593 #614 #725 |

### Batch 6 — sev:low, codec, QL, views, protocol, client (40)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 6A | codec/schema | 12 | low #150 #151 #152 #153 #154 #319 #320 #321 #395 #396 #436 #598 |
| 6B | QL | 4 | low #58 #155 #316 #318 |
| 6C | view engine | 12 | low #160 #379 #380 #382 #383 #384 #385 #386 #387 #388 #492 #496 |
| 6D | protocol | 3 | low #394 #402 #628 |
| 6E | client | 9 | low #8 #335 #340 #377 #468 #688 #719 #739 #742 |

### Batch 7 — sev:low, test infrastructure, CI, flakes (23)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 7 | test infra, CI, flakes | 23 | low #39 #54 #55 #70 #271 #381 #390 #398 #400 #445 #455 #495 #533 #575 #576 #578 #599 #606 #621 #624 #695 #747 #748 |

### Batch 8 — VM, ops and bench (45)
Needs the shared Linux test VM (`$RUNE_VM_HOST`) and the clean bench box (`broker/bench/BENCHMARKING.md`). It runs
whenever the VM is available, and also clears tracker #474 (docker e2e owed by merged batches).

| Wave | Code path | # | Issues |
|---|---|---|---|
| 8 | VM, ops, bench | 45 | **medium** #16 #43 #71 #80 #81 #134 #135 #137 #260 #261 #474 #634 #686 #736; low #9 #42 #45 #46 #47 #48 #49 #50 #62 #63 #64 #72 #77 #78 #187 #336 #337 #389 #399 #439 #443 #462 #467 #470 #479 #489 #547 #630 #633 #635 #717 |

### Not batched

| Wave | Code path | # | Issues |
|---|---|---|---|
| D1 | design queue | 18 | **medium** #12 #41 #68 #361 #419 #532 #573 #612 #617 #745; low #99 #192 #507 #534 #539 #582 #592 #618 |
| R1 | roadmap enhancements | 28 | low #25 #26 #27 #28 #29 #30 #31 #32 #33 #34 #35 #36 #102 #103 #104 #105 #106 #107 #108 #156 #157 #158 #161 #482 #485 #488 #553 #636 |

- **D1 design queue:** each needs a design decision first (TLS, protocol changes, cross-node channels, the #460
  mechanism, prefix-scan completion). Once designed, an item joins the batch that owns its code path.
- **R1 roadmap:** product features (query engine, SQL/QL views, schema evolution tooling, replication hardening
  features). They are planned as features, not fixed as issues (`CLAUDE.md` §8).

## 5. State (2026-11-03)

- Labels fixed for the 16 issues that lacked a type, severity or area. #590 (rows dropped on a checkpoint upgrade) and
  #808 (fail-open verdict bound after failover) raised to sev:high.
- Medium batches M1–M27 merged. PR #619 (M28) is open and unreviewed; it is folded into wave 2B.
- Next: batch 1.
