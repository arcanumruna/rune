# Review: rune-fr 5fe5464 — `rune_fr_c_now` returns 0 while disabled (A8)

Reviewer: Claude (read-only).  Date: 2026-10-04.
Branch: `diag/fr-shard-waterfall` in `~/Projects/rune-fr`.

## Verdict: APPROVE (0 CRITICAL, 0 MAJOR, 0 MEDIUM, 4 LOW)

The change is correct and safe.  A start stamp of 0 already meant "no recorder" before this commit.  The Aeron hook
macro `RFR_NOW()` returns 0 when the weak symbol does not resolve, and `rune_fr_c_span` drops `start == 0`.  The
commit only adds a second way to get that 0.

## What was checked

### (1) Every caller handles 0
- **C API.**
  - `rune_fr_c_span` (src/Registry.cpp:217-221) returns early on `start == 0`, so no record and no duration.
  - `rune_fr_c_mark` (Registry.cpp:223-228) never uses `rune_fr_c_now`.  It reads its own TSC behind `mapSite`.
- **Aeron hook patch** (`aeron_fr_patch.py`, the scratchpad script the findings spec points to).
  - All 10 span wrappers follow one pattern: `uint64_t s = RFR_NOW(); r = f__rfr(...); RFR_SPAN(site, corr, s, r);`.
    They cover conductor/sender/receiver duty, do_send, pub send, pub_lmt, track_rebuild, dispatcher on_data,
    send syscall, recvmmsg and poller poll.
  - No wrapper does any arithmetic on `s`.  It goes straight into `rune_fr_c_span`.
  - Marks never call now().
- **Other callers.** The only other callers of `rune_fr_c_now` in the repo or under ~/Projects are the tests.  C++
  code uses `rune_fr::rdtscp()` / `Event` directly.
- **Turning the recorder on or off mid-span.**
  - Span begun while disabled, committed after enable: `s == 0`, so it is dropped.  It can never produce a huge
    `end - 0` duration.  This matches the C++ `Event`, which latches `on_` at begin and drops on commit
    (Event.h:25,33).
  - Span begun while enabled, committed after disable: `mapSite` returns false, so it is dropped.  This was already
    the behaviour before the commit.
- **Can the TSC legitimately read 0?**
  - It cannot happen in practice on x86 (rdtscp), ARM (cntvct_el0) or the CLOCK_MONOTONIC_RAW fallback (Tsc.h:19-31).
  - Even if it did, that one span would be dropped.  This was already true before the commit.
- **`#else` branch (RUNE_FR off).** It already returned 0 (Registry.cpp:230).  Unchanged.

### (2) Cost of the check and race safety
- `isEnabled()` is a `memory_order_relaxed` load of `std::atomic<bool> g_enabled` (Enable.h:23).
  - It is the same load that `enabled(Site)` (Enable.h:42) and every C++ site already do.
  - It adds no new atomic and no fence.  On x86 and arm64 it compiles to a plain load.
  - The a8 report's disassembly confirms the `g_enabled` test comes before rdtscp.
- **Races.** None.
  - `enable()` and `disable()` are relaxed stores.  The flag is advisory and guards no other data.
  - A thread that sees a stale value either drops one span (start 0) or takes one extra stamp that `mapSite` then
    drops.
  - This matches the existing enable semantics.

### (3) The regression test is RED without the fix
- Verified by building in a scratch copy: HEAD sources with the parent's `src/Registry.cpp` (865902f).
  - `HopPrimitivesRegression.CApiNowIsZeroWhileDisabledRegression` fails at tests/HopInstrumentationTest.cpp:226
    (`rune_fr_c_now()` = 55345216768357, expected 0).
- At HEAD it passes, and the full suite (`build-test/b/rune_fr_tests`) passes: 55/55.
- The test name carries `Regression`, so it is in the regression filter.

## Findings

- **LOW-1 — src/Registry.cpp:219** `if (!mapSite(c, s) || start == 0) return;` runs `mapSite` first.
  - `mapSite` does a function-local-static guard check for `aeronMask()`, a mask test and the `g_enabled` load.
  - While disabled, `start == 0` alone is enough.
  - Swapping the order to `if (start == 0 || !mapSite(c, s))` makes a disabled span cost one compare and one return.
  - Why it matters: the same profile measured `rune_fr_c_span` at 0.5% of a worker and 1.2% of the client.
    This is the same class of waste A8 targets.
  - This is a perf follow-up, not a correctness issue.
- **LOW-2 — src/Registry.cpp:215** While enabled, `rune_fr_c_now` still pays rdtscp for sites that `aeronMask()` or
  the category mask exclude.
  - The default mask leaves out all the per-duty-cycle sites (0,1,2,8,9,10,11,19,20,21), and those spans are then
    dropped.
  - The cost only applies in diag-enabled runs, and fixing it would need a site argument on `now()`, which is an ABI
    change to the Aeron patch.
  - Note it in the backlog only.
- **LOW-3 — tests/HopInstrumentationTest.cpp:227-228** The last assertion does not exercise the `start == 0` guard.
  - `rune_fr_c_span(..., rune_fr_c_now(), ...)` while disabled would record nothing even on the old code, because
    `mapSite` rejects it first.
  - The disabled-to-enabled case is not covered, and it is the case where the 0 sentinel matters.
  - Suggested addition: `disable(); auto t0 = rune_fr_c_now(); enable(); rune_fr_c_span(RUNE_FR_C_AERON_SEND_SYSCALL,
    1, t0, 2.0); EXPECT_EQ(drainedRecords().size(), 0u); disable();`
  - The RED/GREEN signal itself is fine: line 226 is the assertion that goes RED.
- **LOW-4 — include/rune_fr/capi.h:35-36** The header does not state the contract.
  - `rune_fr_c_now()` may return 0, meaning "recorder off".
  - Callers must pass that 0 straight to `rune_fr_c_span`, which drops it, and must not do arithmetic on it.
  - A one-line comment would protect future C hook authors from computing `rdtscp() - start`.
