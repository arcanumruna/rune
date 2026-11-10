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

Rebuilt 2026-11-10 from the 505 open issues: every open issue is in exactly one row. Severity in bold is high/medium;
"unlabeled" issues still need a type, severity and area (§5). Issues filed later are appended to the wave that owns
their code path, or to the next batch if that one has started. The first plan (2026-11-03, 458 issues) had batches
1–8; batches 1 and 2 are done, the old batches 3–8 are now 4–9 with the same waves, and batch 3 is new.

### Batch 1 — done (sev:high, verdict pipeline and view checkpoint recovery)
Merged 2026-11-04 as #851 (230ea43a). 16 of 22 issues closed. #791, #785 and #788 were handed to checked-writes Part 09
(merged 2026-11-10 as #992); they, #797, #800 and #805 move to wave 3D.

### Batch 2 — done (sev:medium)
Merged 2026-11-04 (#885, #888; branches `bugfix/issue-batch-2-*`). 76 of 87 issues closed; PR #619 (M28) was absorbed
by wave 2B. Still open and moved: #456 #778 #783 #69 → 3B; #709 (partly fixed) → 3E; #767 #734 (part 1) #769
(part 1) → 3F; #766 → D1 (needs a wire change); #632 (code fixed, Linux mutation + bench gate owed) → 9.

### Batch 3 — sev:high and sev:medium filed since the first plan, and their bug classes (102)
3A goes first so that the batch-end full suite is green. Starting points that already exist:
- **#960:** branch `bugfix/ci-standalone-suites` (4 commits, not merged) adds the standalone codec/ql/crypto suites to PR
  CI and fixes the `check_no_atomics` `atomic_ref` false positive (#947/#958 are duplicates of that one).
- **#990:** check whether `bugfix/789-follower-compaction-gap` (2 commits, not merged) is still needed after #792.
- **3D:** Part 09 (#992) implemented B1-1 (36350023, #788), the two-phase drop and the kVerdictPlan record (#785, #805)
  and the unnamed-member retire (#937). Verify each against its repro and close it before writing new code; #791
  needs a fresh stress run on `main`.
- #913 and #927 describe the same `FilterSchema.EmptyFixedStrHasItsOwnReason` failure.

| Wave | Code path | # | Issues |
|---|---|---|---|
| 3A | CI gates and red-on-main tests | 12 | **high** #960; low #947 #949 #948 #884 #860 #908; unlabeled #958 #925 #926 #913 #927 |
| 3B | load flakes and hangs | 24 | **high** #896; **medium** #456 #778 #783 #846 #863 #891 #897 #969 #970; low #69 #813 #829 #835 #855 #895 #965 #967 #1005 #1006; unlabeled #911 #914 #933 #959 |
| 3C | follower trim, damaged segments, producer state | 14 | **high** #990; **medium** #874 #880; low #830 #853 #872 #875 #876 #877 #878 #879 #882; unlabeled #955 #957 |
| 3D | checked writes: verdict judge, txn vote, autoinc | 30 | **high** #791; **medium** #785 #788 #797 #937; low #800 #805 #938 #939 #940 #941 #942 #943 #993 #994 #996 #905 #906 #907 #909 #944 #945 #946; unlabeled #951 #954 #956 #932 #929 #915 #931 |
| 3E | namespaces | 9 | **medium** #709 #864 #868; low #865 #866 #869 #870 #873 #988 |
| 3F | client transport and NAT egress | 13 | **medium** #767 #862 #893; low #734 #769 #832 #833 #834 #836 #850 #1004 #966 #997 |

### Batch 4 — sev:low, auth control plane (77)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 4A | login/throttle/session gate | 21 | low #52 #66 #133 #145 #181 #195 #216 #219 #227 #237 #238 #262 #263 #265 #266 #267 #268 #270 #477 #523 #594 |
| 4B | permissions shipper/mirror/__security | 23 | low #198 #199 #200 #201 #202 #204 #207 #208 #264 #269 #280 #281 #284 #285 #286 #411 #442 #444 #457 #465 #518 #608 #775 |
| 4C | DDL/catalog authorization | 33 | low #2 #40 #67 #83 #146 #147 #186 #190 #215 #217 #218 #220 #221 #223 #224 #229 #230 #234 #255 #289 #415 #424 #425 #500 #504 #505 #506 #524 #527 #536 #579 #586 #666 |

### Batch 5 — sev:low, crypto, peer link, ISR/consensus (56)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 5A | crypto and secret wiping | 14 | low #59 #174 #176 #177 #183 #193 #244 #250 #256 #258 #282 #283 #542 #677 |
| 5B | peer link transport | 23 | low #65 #139 #140 #243 #245 #246 #247 #248 #249 #251 #252 #253 #279 #287 #288 #541 #814 #815 #816 #859 #892 #898 #899 |
| 5C | ISR/consensus | 19 | low #3 #7 #84 #85 #86 #87 #88 #89 #90 #91 #92 #94 #95 #96 #100 #101 #272 #550 #602 |

### Batch 6 — sev:low, broker core (75)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 6A | gateway/sessions/egress | 22 | low #6 #179 #191 #312 #401 #417 #437 #502 #560 #615 #638 #752 #753 #757 #758 #759 #760 #761 #768 #773 #831 #987 |
| 6B | filters/subscriptions | 15 | low #412 #414 #416 #420 #421 #430 #447 #448 #449 #451 #452 #453 #463 #483 #484 |
| 6C | groups/txn/lineage | 11 | low #20 #164 #165 #166 #167 #168 #366 #563 #566 #568 #595 |
| 6D | catalog/DDL/storage/RC | 27 | low #74 #79 #98 #142 #144 #148 #171 #172 #173 #210 #292 #293 #294 #295 #297 #298 #299 #300 #558 #593 #614 #725 #900 #901 #902 #881; unlabeled #903 |

### Batch 7 — sev:low, codec, QL, views, protocol, client (59)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 7A | codec/schema | 13 | low #150 #151 #152 #153 #154 #319 #320 #321 #395 #396 #436 #598 #917 |
| 7B | QL | 6 | low #58 #155 #316 #318; unlabeled #928 #930 |
| 7C | view engine | 27 | low #160 #379 #380 #382 #383 #384 #385 #386 #387 #388 #492 #496 #818 #820 #821 #822 #823 #824 #825 #826 #828 #838 #918 #919 #921; unlabeled #950 #952 |
| 7D | protocol | 3 | low #394 #402 #628 |
| 7E | client | 10 | low #8 #335 #340 #377 #468 #688 #719 #739 #742 #920 |

### Batch 8 — sev:low, test infrastructure, CI, flakes (31)

| Wave | Code path | # | Issues |
|---|---|---|---|
| 8 | test infra, CI, flakes | 31 | **medium** #695; low #39 #54 #55 #70 #271 #381 #390 #398 #400 #445 #455 #495 #533 #575 #576 #578 #599 #606 #621 #624 #747 #748 #857 #852 #861 #986 #871 #856 #858; unlabeled #991 |

### Batch 9 — VM, ops and bench (56)
Needs the shared Linux test VM (`$RUNE_VM_HOST`) and the clean bench box (`broker/bench/BENCHMARKING.md`). It runs
whenever the VM is available, and also clears tracker #474 (docker e2e owed by merged batches).

| Wave | Code path | # | Issues |
|---|---|---|---|
| 9 | VM, ops, bench | 56 | **medium** #16 #43 #71 #80 #81 #134 #135 #137 #260 #261 #474 #634 #686 #736 #632; low #9 #42 #45 #46 #47 #48 #49 #50 #62 #63 #64 #72 #77 #78 #187 #336 #337 #389 #399 #439 #443 #462 #467 #470 #479 #489 #547 #630 #633 #635 #717 #827 #841 #842 #843 #844 #845 #910; unlabeled #904 #922 #923 |

### Not batched

| Wave | Code path | # | Issues |
|---|---|---|---|
| D1 | design queue | 20 | **medium** #12 #41 #68 #361 #419 #532 #573 #612 #617 #745 #995; low #99 #192 #507 #534 #539 #582 #592 #618 #766 |
| R1 | roadmap enhancements | 29 | low #25 #26 #27 #28 #29 #30 #31 #32 #33 #34 #35 #36 #102 #103 #104 #105 #106 #107 #108 #156 #157 #158 #161 #482 #485 #488 #553 #636 #916 |

- **D1 design queue:** each needs a design decision first (TLS, protocol changes, cross-node channels, the #460
  mechanism, prefix-scan completion, an authenticated WorkerBindAck (#766), distributed deadlock detection (#995)).
  Once designed, an item joins the batch that owns its code path.
- **R1 roadmap:** product features (query engine, SQL/QL views, schema evolution tooling, replication hardening
  features). They are planned as features, not fixed as issues (`CLAUDE.md` §8).

## 5. State (2026-11-10)

- Batches 1 and 2 merged (2026-11-04). Since then `main` took checked-writes Parts 09–11 (#992), lease topics (#1007)
  and flake fixes (#1000–#1003). 141 issues were filed after the first plan (#813–#1006); all are placed above.
- 27 open issues have no type/severity/area labels; label them before batch 3 opens: #903 #904 #911 #913 #914 #915 #922
  #923 #925 #926 #927 #928 #929 #930 #931 #932 #933 #950 #951 #952 #954 #955 #956 #957 #958 #959 #991. Their wave
  above is by title; move any whose severity comes out high or medium into batch 3.
- PR #619 is superseded by batch 2 and can be closed.
- Next: batch 3, wave 3A.
