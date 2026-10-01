#!/usr/bin/env python3
"""Single-driver-pumper guard (F4, rune spec Sec 3.6) -- INVERTED RULE.

An Aeron C media driver must be driven by exactly ONE thread.  Two threads
calling aeron_driver_main_do_work() on the same driver race the conductor,
sender and receiver state.  The previous version of this guard looked for one
antipattern shape (a loop that pumps AND checks cnc.dat) and so missed real
regressions: a pump in a test-body loop with no cnc check, a pump hidden in a
helper (`while (!ready) pumpOnce();`), readiness via aeron_is_driver_active(),
do{}while loops, ...

Rule now: `aeron_driver_main_do_work(` may appear ONLY in
libs/node/src/EmbeddedDriver.cpp (the one dedicated pump implementation) and
at the exact sites listed in scripts/driver_pump_allowlist.txt -- each entry
is `<file> | <normalized line text> | <count>`, and the scan must match the
allowlist EXACTLY (an extra occurrence fails, and so does an allowlisted site
that no longer exists, so the list can't silently go stale).  Every allowlisted
site must be inside a dedicated driver-thread lambda or a drain that runs after
that thread is joined; new harnesses should use rune::node::EmbeddedDriver.

Comments and string/char literals are stripped first (C++14 digit separators
such as 1'000'000 are NOT char literals).  The script self-tests its detector
and allowlist matcher on synthetic fixtures before scanning the tree.
"""
import re
import sys
from collections import Counter
from pathlib import Path

CALL_RE = re.compile(r"\baeron_driver_main_do_work\s*\(")
UNRESTRICTED = {"libs/node/src/EmbeddedDriver.cpp"}
SOURCE_EXTS = {".cpp", ".h", ".hpp", ".cc", ".cxx", ".ipp"}
SKIP_DIR_NAMES = {".git", "_deps", "node_modules"}
SKIP_DIR_PREFIXES = ("build", "cmake-build")
ALLOWLIST_FILE = Path(__file__).resolve().parent / "driver_pump_allowlist.txt"


def skip_dir(part: str) -> bool:
    return part in SKIP_DIR_NAMES or part.startswith(SKIP_DIR_PREFIXES)


def iter_source_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix not in SOURCE_EXTS:
            continue
        if any(skip_dir(p) for p in path.relative_to(root).parts[:-1]):
            continue
        yield path


def _is_digit_separator(text: str, i: int) -> bool:
    """A ' inside a numeric literal (1'000'000, 0xFF'FF) is a digit separator.
    Walk back over the token; it is numeric iff the token starts with a digit."""
    j = i - 1
    while j >= 0 and (text[j].isalnum() or text[j] in "'_."):
        j -= 1
    tok_start = j + 1
    return tok_start < i and text[tok_start].isdigit()


def strip_comments_and_strings(text: str) -> str:
    """Blank //, /* */ comments and string/char literals, keeping newlines so
    line numbers survive.  Raw strings R"(...)" are handled for the common
    delimiter-less form."""
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
            out.append("".join(ch if ch == "\n" else " " for ch in text[i:j]))
            i = j
        elif c == "R" and text.startswith('R"(', i):
            j = text.find(')"', i + 3)
            j = n if j == -1 else j + 2
            out.append("".join(ch if ch == "\n" else " " for ch in text[i:j]))
            i = j
        elif c == "'" and _is_digit_separator(text, i):
            out.append(c)
            i += 1
        elif c in ('"', "'"):
            quote = c
            j = i + 1
            while j < n and text[j] != quote and text[j] != "\n":
                if text[j] == "\\":
                    j += 1
                j += 1
            j = min(j + 1, n)
            out.append("".join(ch if ch == "\n" else " " for ch in text[i:j]))
            i = j
        else:
            out.append(c)
            i += 1
    return "".join(out)


def normalize(line: str) -> str:
    return " ".join(line.split())


def occurrences(raw: str) -> Counter:
    """Counter of normalized ORIGINAL line text for every pump call site
    (detected on the comment/string-stripped text)."""
    clean = strip_comments_and_strings(raw)
    raw_lines = raw.splitlines()
    found: Counter = Counter()
    for lineno, line in enumerate(clean.splitlines()):
        hits = len(CALL_RE.findall(line))
        if hits:
            found[normalize(raw_lines[lineno])] += hits
    return found


def load_allowlist(path: Path) -> dict:
    allowed: dict = {}
    if not path.exists():
        return allowed
    for n, line in enumerate(path.read_text().splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = [p.strip() for p in line.split(" | ")]
        if len(parts) != 3 or not parts[2].isdigit():
            raise SystemExit(f"{path}:{n}: malformed entry (want 'file | line | count')")
        allowed.setdefault(parts[0], Counter())[normalize(parts[1])] += int(parts[2])
    return allowed


def diff_file(rel: str, found: Counter, allowed: Counter) -> list:
    problems = []
    for text, cnt in sorted(found.items()):
        ok = allowed.get(text, 0)
        if cnt > ok:
            problems.append(f"{rel}: {cnt - ok} unallowlisted aeron_driver_main_do_work call(s): {text}")
    for text, cnt in sorted(allowed.items()):
        have = found.get(text, 0)
        if have < cnt:
            problems.append(f"{rel}: stale allowlist entry ({cnt - have} missing): {text}")
    return problems


def check_tree(root: Path, allowlist: dict) -> list:
    problems = []
    seen = set()
    for path in iter_source_files(root):
        rel = str(path.relative_to(root))
        if rel in UNRESTRICTED:
            continue
        raw = path.read_text(errors="replace")
        if "aeron_driver_main_do_work" not in raw and rel not in allowlist:
            continue
        seen.add(rel)
        problems += diff_file(rel, occurrences(raw), allowlist.get(rel, Counter()))
    for rel in sorted(set(allowlist) - seen):
        problems.append(f"{rel}: allowlisted file has no pump calls / no longer exists")
    return problems


# ---------------------------------------------------------------------------
# Self-test: every shape the old guard missed is now caught; allowlisted exact
# sites pass; stale entries fail; comments/strings/digit separators don't trip.
# ---------------------------------------------------------------------------
_THREAD = "aeron_driver_main_idle_strategy(driver_, aeron_driver_main_do_work(driver_));"
SELFTEST_CASES = [
    ("test-body-pump-no-cnc", "void pump() { while (!done()) { aeron_driver_main_do_work(driver_); gw_->doWork(); } }", {}, True),
    ("helper-pump", "void pumpOnce() { aeron_driver_main_do_work(driver_); }\nvoid wait() { while (!ready()) pumpOnce(); }", {}, True),
    ("is-driver-active-readiness", "while (!aeron_is_driver_active(dir, 1000, nullptr)) aeron_driver_main_do_work(d);", {}, True),
    ("do-while", "do { aeron_driver_main_do_work(d); } while (!exists(cnc));", {}, True),
    ("allowlisted-thread-lambda", "driverThread_ = std::thread([this] { while (run) { " + _THREAD + " } });",
     {normalize("driverThread_ = std::thread([this] { while (run) { " + _THREAD + " } });"): 1}, False),
    ("second-copy-of-allowlisted-line", _THREAD + "\n" + _THREAD, {normalize(_THREAD): 1}, True),
    ("stale-allowlist-entry", "int x = 1'000'000;", {normalize(_THREAD): 1}, True),
    ("comment-and-string-only", "// aeron_driver_main_do_work(d);\nconst char* s = \"aeron_driver_main_do_work(d)\";", {}, False),
    ("digit-separator-does-not-hide-code", "int x = 1'000; aeron_driver_main_do_work(d); int y = 2'000;", {}, True),
]


def run_selftest() -> list:
    failures = []
    for name, src, allowed, expect_fail in SELFTEST_CASES:
        problems = diff_file(f"<selftest:{name}>", occurrences(src), Counter(allowed))
        if bool(problems) != expect_fail:
            failures.append(f"selftest '{name}': expected {'a violation' if expect_fail else 'clean'}, got {problems or 'clean'}")
    return failures


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    failures = run_selftest()
    if failures:
        print("check_single_driver_pumper: SELF-TEST FAILED (the guard itself is broken):")
        for f in failures:
            print("  " + f)
        return 1
    if "--selftest" in sys.argv[1:]:
        print(f"check_single_driver_pumper: self-test OK ({len(SELFTEST_CASES)} fixtures)")
        return 0
    root = (Path(args[0]) if args else Path(__file__).resolve().parents[1]).resolve()
    problems = check_tree(root, load_allowlist(ALLOWLIST_FILE))
    if problems:
        print("check_single_driver_pumper: FAIL -- aeron_driver_main_do_work outside the single pumper:")
        for p in problems:
            print("  " + p)
        print("\nFix: use rune::node::EmbeddedDriver (one dedicated pump). A new dedicated")
        print("driver-thread lambda or post-join drain must be added to")
        print("scripts/driver_pump_allowlist.txt with its exact line text and count.")
        return 1
    print(f"check_single_driver_pumper: OK (self-test {len(SELFTEST_CASES)} fixtures passed)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
