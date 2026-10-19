# Review — D2 lows R2-L1..L4 (4bd7666 on abcbc13, branch fix/d2-lows, wt47)

**Verdict: APPROVE WITH CHANGES** — CRITICAL 0 / MAJOR 0 / MEDIUM 3 / LOW 4

The R2-L1 code is correct and closes a real double free. The commit's explanation of *why* there was no double free
is wrong against the linked Aeron (1.41.4). The R2-L2 warning is wrong for the default (fused) placement. R2-L4, as
the review asked for it, is not addressed.

## How this was checked
- Built `rune_node_tests` and `rune_embedded_driver_tests` in build-fast (ninja reported no work to do). All 6 new and
  adjacent tests pass: EnvIdleArgsMismatch (16 ms), MalformedIdle, DriverIdleArgsWithArgless, IdleMaxParkUs x2, and
  EffectiveConfigNamesTheControlPlaneIdle.
- Aeron source: the cached source `~/.conan2/p/aeron587c0be5e0d59/s/src` (1.41.4) is byte-identical to the build
  tree of the binary package the Mac build links (`~/.conan2/p/b/aeronb73a55f36892b`). I diffed
  `aeron_driver_context.c` and `aeron_agent.c`.
- Probe (`/tmp/aeronprobe/p.c`, outside the repo) linked against that `libaeron_driver_static.a`:
  1. Set `AERON_SHARED_IDLE_STRATEGY_INIT_ARGS=1-0-100us-2ms`.
  2. Call `aeron_driver_context_init`.
  3. Call `set_shared_idle_strategy("sleep-ns")`.
  4. Call `aeron_driver_context_close`.

  This is the pre-fix path. It was run with and without ASan.

## Aeron facts (verifying the commit-message claim)
- `aeron_driver_context_init` **does** set every `*_idle_strategy_name` to a heap string, at
  `aeron_driver_context.c:1013-1017`: `_context->shared_idle_strategy_name = aeron_strndup("backoff", AERON_MAX_PATH);`
  and likewise for conductor, shared_network, sender and receiver. The probe prints `name before set: 0x600001580070
  'backoff'`.
- `aeron_driver_context_set_*_idle_strategy` (`:2097-2117` for shared, and the same for the other roles):
  1. It frees `*_state` and `*_name`.
  2. It calls `aeron_idle_strategy_load(value, &state, env, context->*_init_args)`.
  3. On failure it returns -1 **without** resetting `*_name`. On failure it also does not reset `*_state` when the name
     lookup fails, because `aeron_agent.c:322` `*idle_strategy_state = NULL` runs only after a successful table or
     dlsym lookup.
- `aeron_driver_context_close` (`:1296-1310`) then frees both of them again.
- Result:
  - **Pre-fix base with an AERON_* args mismatch:** the setter frees `shared_idle_strategy_name`, then close frees it
    again. This is a double free.
    - ASan reports `bad-free ... aeron_driver_context_close aeron_driver_context.c:1303`, with the original allocation at
      `aeron_driver_context_init:1013`.
    - Without ASan, macOS malloc does not detect it: exit 0, "closed". That explains why the implementer's probe "saw no
      double free".
  - **Unknown name:** the stale state is also double freed. The non-ASan run aborts with exit 134.
- So the R2-L1 review prediction was right, and the commit message ("the name is NULL at the first set — refusal was
  already clean") is wrong. The fix is still correct: validation with the exact name and args now runs before any
  setter, so a failing setter is unreachable (barring OOM).

## Per-role validation (EmbeddedDriver.cpp:136-166)
- **Right args per role, in every mode:**
  - For each role the setter loads with `context->*_init_args`.
    - With explicit args, that is `strndup(idleArgs)` after `setArgs`, which is the same string `effective` validated.
    - Without explicit args, it is the context's own string, which is exactly `getArgs(ctx_)`.
  - Shared mode validates only the shared role and sets only the shared role.
  - Dedicated mode validates and sets conductor, then sender, then receiver, each with its own getter.
  - Invoker mode sets no idle strategy, which is correct: there is no agent idle loop. It matches the old
    `mode_ == Invoker ||` short-circuit.
- **Half-set state:**
  - A failing role is validated before its `setArgs` or setter runs. Earlier roles were fully and validly set. Then
    `close` runs and the constructor throws, so the context is never used half-configured.
  - The only residual window is `setArgs` OK but setter fails. That would need an allocation failure, or args longer
    than `AERON_MAX_PATH` that `strndup` truncates. Not realistic.
- **`usedArgs`:**
  - Explicit args: `usedArgs` = the explicit args, which is correct.
  - Env args, failing role: `usedArgs` = that role's env args, which is correct. The roles run in order and the code
    stops at the first failure.
  - Mislabel case: an earlier role has env args and a later role fails with NULL args. A NULL-args failure happens only
    for an unknown name, and an unknown name fails at the first role. So this cannot happen in practice.

## Findings

### MEDIUM
- **Md-1 The test comment and the commit message state a false Aeron mechanism ("no double free … the name is still NULL
  at the first set").**
  - **Where:** `libs/node/tests/node/EmbeddedDriverTest.cpp:255-257` and the 4bd7666 message.
  - **Why it matters:** the pre-validation in `EmbeddedDriver.cpp:145` is the only thing that keeps this path from being a
    double free. A maintainer who trusts the comment ("refusal was already clean") could drop the scratch validation as
    redundant. That would reintroduce UB that macOS hides and glibc aborts on (`free(): double free detected`).
  - **Concrete scenario:**
    1. Export `AERON_SHARED_IDLE_STRATEGY_INIT_ARGS=1-0-100us-2ms`.
    2. Start a legacy-mode node (driver idle `sleep-ns`).
    3. On abcbc13: setter fails, then close double-frees `shared_idle_strategy_name`. ASan: bad-free at
       `aeron_driver_context.c:1303`.
  - **Fix:**
    - Correct the test comment. It should say: "the setter frees name/state and does not reset them on failure, so a
      failing setter is a double free in close; validation with the setter's exact args must precede every set."
    - Record in the D2 findings or backlog that base abcbc13 had a real double free, not only a wrong message. This is
      also why the RED on base was only the message assertion: Mac malloc did not catch the double free.
- **Md-2 The R2-L2 warning and doc comment are wrong for the DEFAULT (fused) placement.**
  - **Where:** `HostDefaults.cpp:266-269` and `NodeConfig.h:280-281`.
  - **What is wrong:** in fused placement:
    - the runtime agent runs inside worker 0's pass (`BrokerNode.cpp:448-452`, `setFusedRuntime`);
    - the node driver is an **invoker** pumped by worker 0 (`BrokerNode.cpp:2974`);
    - the startup pump uses `makeBackoffIdle(config_.idleMaxParkUs)` (`BrokerNode.cpp:370`).

    So `RUNE_IDLE_MAX_PARK_US` *does* still cap the runtime and node-driver pumping there. Only the admin thread uses
    `RUNE_CONTROL_IDLE_MAX_PARK_US`.
  - **Scenario:** default config (shard + fused). The operator sets `RUNE_IDLE_MAX_PARK_US=50` and is told it "caps only
    the shard workers' park — the runtime, admin and node-driver threads use RUNE_CONTROL_IDLE_MAX_PARK_US". The operator
    then tunes the control knob expecting a runtime effect, and gets none. The regression test
    (`HostDefaultsTest.cpp` IdleMaxParkUsInShardModeWarnsRegression) runs on `threeNodeDefault`, which is fused, so it
    pins the wrong text.
  - **Fix:** make the text placement-aware:
    - Fused: "the admin thread uses RUNE_CONTROL_IDLE_MAX_PARK_US (the runtime and node driver ride worker 0)".
    - Thread placement: keep the current text.
    - Add a thread-placement test case.
- **Md-3 R2-L4 as written is not addressed. The commit redefined it as "args only where the strategy takes them".**
  - **Where:** `HostDefaults.cpp:444-451`.
  - **What the review asked** (d2-review.md:271-276):
    - **Fused placement:** print `invoker` for drvIdle, because the node driver has no idle loop. Today it prints
      `drvIdle=backoff{1-0-1000-2000000}`.
    - **Fused placement, ctlIdle:** ctlIdle should not claim a runtime thread shape when the runtime runs on worker 0.
    - **`RUNE_RUNTIME_IDLE=busyspin`:** print busyspin. Today it still prints the backoff ctlIdle.
  - **Scenario:** the default shard + fused node logs `drvIdle=backoff{1-0-1000-2000000} ctlIdle=10-0-...`. These are
    the proof-of-path fields, and both are false for that node.
  - **Fix:**
    - `drvIdle = cfg.fused() ? "invoker" : ...`.
    - For ctlIdle, print `worker0` (or the worker idle) when fused, and `busyspin` when
      `runtimeIdleStrategy == BusySpin` in thread placement.
    - Extend `EffectiveConfigNamesTheControlPlaneIdleRegression` to cover both cases.
    - Otherwise, explicitly re-scope R2-L4 in the backlog instead of claiming it fixed.

### LOW
- **L-1 The refusal blames the wrong knob, and can carry an empty reason.**
  - **Where:** `EmbeddedDriver.cpp:171-173`.
  - **Wrong knob:** when the bad args came from `AERON_<ROLE>_IDLE_STRATEGY_INIT_ARGS`, the text still points the
    operator at "RUNE_AERON_DRIVER_IDLE / RUNE_AERON_DRIVER_IDLE_ARGS" and names no role.
  - **Empty reason:** Aeron's sleeping init does not set an error on a parse failure. The probe printed `err=no error`,
    so `why` can be "no error" or stale text.
  - **Fix:** when `idleArgs == nullptr`, name the role and its env var. When `why` is empty or "no error", substitute
    "init args not parseable for '<strategy>'".
- **L-2 `driverIdleTakesArgs` misses Aeron's full symbol names.**
  - **Where:** `NodeConfig.h:735-738`.
  - **Gap:** `aeron_symbol_table_obj_scan` also matches the table's `name` column (`aeron_idle_strategy_backoff`,
    `aeron_idle_strategy_sleeping`).
  - **Scenario:** `RUNE_AERON_DRIVER_IDLE=aeron_idle_strategy_backoff` plus args causes a false "ignored" warning, and
    the summary hides args that are applied.
  - **Fix:** add the two symbol names, or note them. The table aliases (sleeping, sleep-ns, backoff take args; yield,
    spin, noop use `init_null`) are otherwise matched exactly.
- **L-3 The env-mismatch test covers only the first role of each mode.**
  - **Where:** `EmbeddedDriverTest.cpp:259-282`.
  - **Gap:** no case has conductor clean and sender or receiver bad. That case exercises the per-role getter and
    `usedArgs` on a later role in dedicated mode.
  - **Fix:** add `AERON_RECEIVER_IDLE_STRATEGY_INIT_ARGS` to the loop with Dedicated.
- **L-4 R2-L3 warns in fused placement as well.**
  - **Where:** `HostDefaults.cpp:263-265`.
  - **Behaviour:** in fused placement the node driver is an invoker, so both the strategy and its args are moot. The
    warning is still true ("ignored"), but it is incomplete.
  - **Fix:** fold this into Md-3 (print `invoker`). Optionally warn that `RUNE_AERON_DRIVER_IDLE*` has no effect in
    fused placement at all.

## Other checks (fine)
- **`idleMaxParkUsExplicit`** is set only inside `end != imp && us > 0` (`NodeConfig.cpp:1271`). That is exactly when
  the value is applied. An explicit `200` also warns, which is correct, since it was set explicitly.
- **R2-L3 condition:** the warning fires only when args are non-empty and the strategy is argless. This matches Aeron's
  table at `aeron_agent.c:286-294`.
- **RED without the fix:**
  - **EnvIdleArgsMismatch:** on base, validation with NULL args passes and the setter fails. The double free is silent
    on Mac, and the message says `args ''`, so the `find("1-0-100us-2ms")` assertion fails.
  - **DriverIdleArgsWithArgless:** there is no warning on base, and the base summary prints `drvIdle=yield{...}`.
  - **IdleMaxParkUs x2:** these do not compile on base, because the field is new.
- **No sleeps:** `aeron_is_driver_active` returns at once on a fresh dir, and the test ran in 16 ms.
- **Env hygiene:** `EnvGuard` restores or unsets the variable per iteration, including on throw (RAII). No env change
  leaks. `parseNodeConfig` tests use an injected getenv.
