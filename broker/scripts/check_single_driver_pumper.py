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
loop whose body calls BOTH aeron_driver_main_do_work(...) AND checks a cnc.dat
path in the same loop -- the literal "pump until cnc.dat appears" idiom that
raced the dedicated pump thread. Brace-matching (not just line proximity) is
used so that an unrelated later readiness assertion near a genuine pump-thread
loop is not flagged.
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
    and pattern search never trip on braces or keywords inside them."""
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


def find_loop_spans(clean_text: str):
    """Return (start_offset, end_offset) for every for(...)/while(...) BODY,
    i.e. from the '{' after the loop header to its matching '}'. Loops
    without a brace body (single-statement loops) can't host both calls on
    one line pair meaningfully for this antipattern, so they're skipped."""
    spans = []
    for m in LOOP_RE.finditer(clean_text):
        # Find the matching ')' for the loop header's '('.
        paren_start = clean_text.find("(", m.end() - 1)
        if paren_start == -1:
            continue
        depth = 0
        k = paren_start
        while k < len(clean_text):
            if clean_text[k] == "(":
                depth += 1
            elif clean_text[k] == ")":
                depth -= 1
                if depth == 0:
                    break
            k += 1
        else:
            continue
        # Skip whitespace to the body's opening brace.
        b = k + 1
        while b < len(clean_text) and clean_text[b] in " \t\r\n":
            b += 1
        if b >= len(clean_text) or clean_text[b] != "{":
            continue  # single-statement loop body; not the multi-call idiom
        depth = 0
        e = b
        while e < len(clean_text):
            if clean_text[e] == "{":
                depth += 1
            elif clean_text[e] == "}":
                depth -= 1
                if depth == 0:
                    break
            e += 1
        else:
            continue
        spans.append((b, e))
    return spans


def line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def check_file(path: Path, rel: str) -> list[str]:
    raw = path.read_text(errors="replace")
    if not CALL_RE.search(raw) or not CNC_RE.search(raw):
        return []  # fast path: needs both patterns somewhere at all
    clean = strip_comments_and_strings(raw)
    violations = []
    for start, end in find_loop_spans(clean):
        body = clean[start:end]
        call_m = CALL_RE.search(body)
        cnc_m = CNC_RE.search(body)
        if call_m and cnc_m:
            call_line = line_of(clean, start + call_m.start())
            violations.append(
                f"{rel}:{call_line}: a for/while loop pumps "
                f"aeron_driver_main_do_work(...) AND checks a cnc.dat readiness "
                f"path in the same loop body -- this is the caller-side "
                f"'wait for CnC by pumping' antipattern that races the dedicated "
                f"pump thread (F4, rune spec Sec 3.6)."
            )
    return violations


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1]
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

    print("check_single_driver_pumper: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
