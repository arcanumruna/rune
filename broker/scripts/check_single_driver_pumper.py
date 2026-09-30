#!/usr/bin/env python3
"""check_single_driver_pumper.py — F4 regression guard.

Spec rule (rune Sec 3.6): an Aeron media driver is driven by exactly ONE
thread. The F4 bug was every harness that starts a dedicated thread looping
aeron_driver_main_do_work(driver) and then ALSO pumps the SAME driver from
the caller thread in a "wait for CnC / driver ready" loop -- two threads
racing the same conductor.

The fix (see libs/node/src/EmbeddedDriver.cpp and the many test/benchmark
harnesses converted alongside it) is: aeron_driver_init()/aeron_driver_start()
already write cnc.dat and stamp the heartbeat synchronously, so nothing needs
to pump the driver to wait for it to become ready. Only the one dedicated
pump thread may ever call aeron_driver_main_do_work().

This script re-detects the exact antipattern (rather than banning the API
outright), because legitimate uses remain outside EmbeddedDriver.cpp:
  - the dedicated pump thread's own loop:
        thread = std::thread([...]{ while (running) aeron_driver_main_do_work(...); });
  - a post-join drain loop in stopDriver()/destructors, which runs strictly
    AFTER the dedicated thread has been joined (never concurrent with it, so
    it is not a second pumper)
  - a plain `fs::exists(cncPath)` readiness ASSERTION with no pumping at all

What is still forbidden everywhere outside EmbeddedDriver.cpp: a `for`/`while`
loop whose header (init/condition/increment) OR body calls
aeron_driver_main_do_work(...) AND ALSO references a cnc.dat readiness check
anywhere in that same loop (header or body, braced or not) -- the literal
"pump until cnc.dat appears" idiom that raced the dedicated pump thread. This
covers all of:
  for (...) { aeron_driver_main_do_work(...); if (fs::exists(cncPath)) break; }
  while (!fs::exists(cncPath)) aeron_driver_main_do_work(driver_);          // braceless
  while (aeron_driver_main_do_work(driver_) >= 0 && !fs::exists(cncPath)) {}  // pump in condition

Brace/paren-matching (not just line proximity, and not requiring a `{` body)
is used so that an unrelated later readiness assertion near a genuine
pump-thread loop is not flagged, while a single-statement (braceless) loop
body and a pump call embedded in the loop header are still caught.

This script self-tests its own detector (see run_selftest()) against small
synthetic fixtures before scanning the real tree, so a change to the
detection logic that reopens either a false-negative (bypass) or a
false-positive hole fails loudly on its own, not just on the real files.
"""
import re
import sys
from pathlib import Path

CALL_RE = re.compile(r"aeron_driver_main_do_work\s*\(")
CNC_RE = re.compile(r"cnc\.dat|cncPath|cncFile")
LOOP_RE = re.compile(r"\b(for|while)\s*\(")

# The one file allowed to pump the driver at all (it owns the one dedicated
# pump thread's implementation, and its own drain-after-close logic).
ALLOWLIST = {"libs/node/src/EmbeddedDriver.cpp"}

SOURCE_EXTS = {".cpp", ".h", ".hpp", ".cc", ".cxx"}
SKIP_DIR_NAMES = {
    "build", "build-fast", "build-tsan", "cmake-build-debug", "cmake-build-release",
    ".git", "_deps",
}


def iter_source_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in SOURCE_EXTS:
            continue
        if any(part in SKIP_DIR_NAMES for part in path.relative_to(root).parts):
            continue
        yield path


def strip_comments_and_strings(text: str) -> str:
    """Blank out //, /* */ comments and string/char literals so brace-matching
    and pattern search never trip on braces or keywords inside them, while
    preserving line numbers (newlines are kept)."""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            j = n if j == -1 else j
            out.append(" " * (j - i))
            i = j
        elif c == "/" and i + 1 < n and text[i + 1] == "*":
            j = text.find("*/", i + 2)
            j = n if j == -1 else j + 2
            seg = text[i:j]
            out.append("".join(ch if ch == "\n" else " " for ch in seg))
            i = j
        elif c in ('"', "'"):
            quote = c
            j = i + 1
            while j < n and text[j] != quote:
                if text[j] == "\\":
                    j += 1
                j += 1
            j = min(j + 1, n)
            seg = text[i:j]
            out.append("".join(ch if ch == "\n" else " " for ch in seg))
            i = j
        else:
            out.append(c)
            i += 1
    return "".join(out)


def _match_parens(text: str, open_paren_idx: int):
    """Return the index just past the ')' matching the '(' at open_paren_idx,
    or None if unbalanced."""
    depth = 0
    k = open_paren_idx
    n = len(text)
    while k < n:
        if text[k] == "(":
            depth += 1
        elif text[k] == ")":
            depth -= 1
            if depth == 0:
                return k + 1
        k += 1
    return None


def find_loop_units(clean_text: str):
    """Return the full [start, end) span of every for/while loop -- from the
    'for'/'while' keyword (so the header's init/condition/increment is
    included) through the end of its body, whether the body is brace-
    delimited or a single braceless statement terminated by ';'."""
    spans = []
    n = len(clean_text)
    for m in LOOP_RE.finditer(clean_text):
        kw_start = m.start()
        paren_start = m.end() - 1  # LOOP_RE consumes up to and including '('
        header_end = _match_parens(clean_text, paren_start)
        if header_end is None:
            continue

        b = header_end
        while b < n and clean_text[b] in " \t\r\n":
            b += 1

        if b < n and clean_text[b] == "{":
            # Braced body: match to the closing '}'.
            depth = 0
            e = b
            end = None
            while e < n:
                if clean_text[e] == "{":
                    depth += 1
                elif clean_text[e] == "}":
                    depth -= 1
                    if depth == 0:
                        end = e + 1
                        break
                e += 1
            if end is None:
                continue
            spans.append((kw_start, end))
        else:
            # Braceless single-statement body: scan to the next ';' that is
            # not nested inside (), [], or {} opened within the statement
            # itself (e.g. a lambda or nested call as the loop body/that the
            # statement's arguments contain).
            depth = 0
            e = b
            end = None
            while e < n:
                c = clean_text[e]
                if c in "([{":
                    depth += 1
                elif c in ")]}":
                    if depth == 0:
                        # The enclosing block closed before a ';' was seen
                        # (e.g. the braceless loop is the last statement
                        # before the function's closing '}'). Treat this
                        # point as the end of the (degenerate/empty) body.
                        end = e
                        break
                    depth -= 1
                elif c == ";" and depth == 0:
                    end = e + 1
                    break
                e += 1
            if end is None:
                continue
            spans.append((kw_start, end))
    return spans


def line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def check_text(rel: str, raw: str) -> list[str]:
    """Core detector: returns human-readable violation strings for one file's
    (or fixture's) source text, keyed by `rel` for the message."""
    if not CALL_RE.search(raw) or not CNC_RE.search(raw):
        return []  # fast path: needs both patterns somewhere at all
    clean = strip_comments_and_strings(raw)
    violations = []
    seen_lines = set()
    for start, end in find_loop_units(clean):
        unit = clean[start:end]
        call_m = CALL_RE.search(unit)
        cnc_m = CNC_RE.search(unit)
        if call_m and cnc_m:
            call_line = line_of(clean, start + call_m.start())
            if call_line in seen_lines:
                continue  # de-dup: nested loops both enclosing the same call
            seen_lines.add(call_line)
            violations.append(
                f"{rel}:{call_line}: a for/while loop (header and/or body) "
                f"pumps aeron_driver_main_do_work(...) AND checks a cnc.dat "
                f"readiness path in that same loop -- this is the caller-side "
                f"'wait for CnC by pumping' antipattern that races the "
                f"dedicated pump thread (F4, rune spec Sec 3.6)."
            )
    return violations


def check_file(path: Path, rel: str) -> list[str]:
    raw = path.read_text(errors="replace")
    return check_text(rel, raw)


# ---------------------------------------------------------------------------
# Self-test: pin both bypass-freedom (every known antipattern shape is
# caught) and false-positive-freedom (every known legitimate shape is not).
# ---------------------------------------------------------------------------
SELFTEST_CASES = [
    (
        "braced-antipattern",
        True,
        '''
        void startDriver() {
            const std::string cncPath = aeronDir + "/cnc.dat";
            for (int i = 0; i < 400; ++i) {
                aeron_driver_main_do_work(driver_);
                if (fs::exists(cncPath)) break;
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
            }
        }
        ''',
    ),
    (
        "braceless-antipattern",
        True,
        '''
        void startDriver() {
            const std::string cncPath = aeronDir + "/cnc.dat";
            while (!fs::exists(cncPath)) aeron_driver_main_do_work(driver_);
        }
        ''',
    ),
    (
        "pump-in-condition-antipattern",
        True,
        '''
        void startDriver() {
            const std::string cncPath = aeronDir + "/cnc.dat";
            while (aeron_driver_main_do_work(driver_) >= 0 && !fs::exists(cncPath)) {
                std::this_thread::sleep_for(std::chrono::microseconds{200});
            }
        }
        ''',
    ),
    (
        "pump-in-for-increment-antipattern",
        True,
        '''
        void startDriver() {
            const std::string cncPath = aeronDir + "/cnc.dat";
            for (int i = 0; i < 400 && !fs::exists(cncPath); aeron_driver_main_do_work(driver_))
                ++i;
        }
        ''',
    ),
    (
        "legitimate-single-pump-thread",
        False,
        '''
        void startDriver() {
            const std::string cncPath = aeronDir + "/cnc.dat";
            driverThread_ = std::thread([this]() {
                while (driverRunning_.load(std::memory_order_acquire)) {
                    aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));
                }
            });
            ASSERT_TRUE(fs::exists(cncPath)) << "Aeron CnC file not created";
        }
        ''',
    ),
    (
        "legitimate-post-join-drain",
        False,
        '''
        void stopDriver() {
            if (driverRunning_.exchange(false)) {
                if (driverThread_.joinable()) driverThread_.join();
            }
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{50};
            while (std::chrono::steady_clock::now() < deadline) {
                if (aeron_driver_main_do_work(driver_) == 0) {
                    aeron_driver_main_do_work(driver_);
                    break;
                }
            }
        }
        ''',
    ),
]


def run_selftest() -> list[str]:
    failures = []
    for name, expect_violation, snippet in SELFTEST_CASES:
        violations = check_text(f"<selftest:{name}>", snippet)
        got_violation = bool(violations)
        if got_violation != expect_violation:
            failures.append(
                f"selftest '{name}': expected "
                f"{'a violation' if expect_violation else 'NO violation'}, got "
                f"{violations if violations else 'none'}"
            )
    return failures


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    selftest_only = "--selftest" in sys.argv[1:]

    selftest_failures = run_selftest()
    if selftest_failures:
        print("check_single_driver_pumper: SELF-TEST FAILED (the guard itself is broken):")
        for f in selftest_failures:
            print("  " + f)
        return 1
    if selftest_only:
        print(f"check_single_driver_pumper: self-test OK ({len(SELFTEST_CASES)} fixtures)")
        return 0

    root = Path(args[0]) if args else Path(__file__).resolve().parents[1]
    root = root.resolve()
    violations: list[str] = []
    for path in iter_source_files(root):
        rel = str(path.relative_to(root))
        if rel in ALLOWLIST:
            continue
        violations.extend(check_file(path, rel))

    if violations:
        print("check_single_driver_pumper: FAIL -- caller-side driver pumping detected:")
        for v in violations:
            print("  " + v)
        print()
        print("Fix: delete the caller-side pump loop. aeron_driver_init()/")
        print("aeron_driver_start() already write cnc.dat and stamp the heartbeat")
        print("synchronously (aeron_driver.c:864, :911) -- a plain fs::exists() check")
        print("(no pumping) is enough, or use rune::node::EmbeddedDriver which handles")
        print("this correctly. See libs/node/include/rune/node/EmbeddedDriver.h.")
        return 1

    print(f"check_single_driver_pumper: OK (self-test {len(SELFTEST_CASES)} fixtures passed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
