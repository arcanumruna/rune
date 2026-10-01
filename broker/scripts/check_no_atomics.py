#!/usr/bin/env python3
"""check_no_atomics.py — F9a regression guard: NoAtomicsOrMutexesOutsideAllowlist.

USER LAW (2026-10-01): "there should be no atomic or mutex anywhere ... with the
exception of log cleaner/compaction".  Rune is strictly shared-nothing: every
piece of state is owned by ONE thread and everything that crosses threads is a
RING MESSAGE.  The only memory-ordering primitives allowed are:
  RING     — inside the ring-buffer implementations themselves (the M2O/O2O
             agrona copies, the io_uring SQ/CQ ring accessors, and the Log's
             hand-rolled SPSC ring indices);
  CLEANER  — the off-thread LogCleaner / compaction channel (the Log members it
             uses to hand work to / from the cleaner thread);
  SLICE-B  — TEMPORARY: sites owned by F9 slice B (PartitionReplica, ClientGateway
             minIsr_, ReplicationMetadataBridge observers).  Slice B deletes these
             entries as it converts them.
  UNSCOPED — TEMPORARY: sites found by this guard that belong to neither slice
             (reported for scheduling).  They must be converted, not grown.

What is scanned: every C/C++ source under libs/** and src/** EXCEPT test code
(any path component named "tests") — production code only.  Comments and
string literals are blanked first (no false positives from prose).

Forbidden tokens: std::atomic*, atomic_* / __atomic_* (fences, refs, free
functions, builtins), <atomic>/<mutex>/<shared_mutex>/<condition_variable>/
<semaphore>/<latch>/<barrier> includes, std::mutex, std::shared_mutex,
std::recursive_mutex, std::timed_mutex, std::condition_variable(_any),
std::lock_guard, std::unique_lock, std::shared_lock, std::scoped_lock,
std::counting_semaphore/binary_semaphore, std::latch, std::barrier,
std::once_flag/call_once, thread_local, pthread_mutex/cond/rwlock/spin, __sync_*.

Allowlist (scripts/atomics_allowlist.txt), one entry per line:
    <path> | <token> | <CATEGORY> | <reason>
A hit on line L of <path> is allowed iff an entry for <path> has token "*" or
<token> occurs on line L.  An entry that matches NO hit is STALE and fails the
guard too, so converted sites must be removed from the allowlist (that is how
slice B shrinks it).

Self-test: run_selftest() pins the detector (comment/string immunity, every
forbidden family, allowlist token matching, stale-entry detection) on synthetic
fixtures before scanning the tree.
"""
import re
import sys
from pathlib import Path



def strip_comments_and_strings(text: str) -> str:
    """Blank //, /* */ comments and string/char literals, preserving newlines.

    Unlike the F4 guard's stripper, a quote preceded by an alphanumeric
    character is a C++14 DIGIT SEPARATOR (1'000'000), not a char literal —
    otherwise a separator would open a bogus literal and blank real code
    (hiding forbidden tokens = a false negative)."""
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
        elif c == "'" and i > 0 and text[i - 1].isalnum() and i + 1 < n and text[i + 1].isalnum() \
                and not (text[i - 1] in "uUL8" and (i < 2 or not text[i - 2].isalnum())):
            out.append(c)   # digit separator
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

FORBIDDEN = [
    (r"\bstd::atomic\w*", "std::atomic*"),
    (r"atomic_\w+", "atomic_* / __atomic_*"),
    (r"#\s*include\s*<(atomic|mutex|shared_mutex|condition_variable|semaphore|latch|barrier|stop_token)>",
     "sync header include"),
    (r"\bstd::(recursive_|timed_|recursive_timed_|shared_|shared_timed_)?mutex\b", "std::*mutex"),
    (r"\bstd::condition_variable(_any)?\b", "std::condition_variable"),
    (r"\bstd::(lock_guard|unique_lock|shared_lock|scoped_lock)\b", "std lock"),
    (r"\bstd::(counting_semaphore|binary_semaphore|latch|barrier)\b", "std semaphore/latch/barrier"),
    (r"\bstd::(once_flag|call_once)\b", "std::once_flag/call_once"),
    (r"\bthread_local\b", "thread_local"),
    (r"\bpthread_(mutex|cond|rwlock|spin)_\w+", "pthread lock"),
    (r"\b__sync_\w+", "__sync builtin"),
]
FORBIDDEN_RE = [(re.compile(p), label) for p, label in FORBIDDEN]

CATEGORIES = {"RING", "CLEANER", "SLICE-B", "UNSCOPED"}
SOURCE_EXTS = {".cpp", ".h", ".hpp", ".cc", ".cxx", ".ipp", ".inl"}
SKIP_DIR_NAMES = {"tests", "build", "build-fast", "build-tsan", "build-release",
                  "cmake-build-debug", "cmake-build-release", ".git", "_deps"}
SCAN_ROOTS = ("libs", "src")


def find_hits(rel: str, raw: str):
    """Return [(line_no, label, original_line)] for forbidden tokens in code."""
    clean = strip_comments_and_strings(raw)
    # Includes: the angle-bracket header name survives comment stripping, but
    # strip_comments_and_strings blanks '"..."' only, so <atomic> stays intact.
    raw_lines = raw.splitlines()
    hits = []
    for i, line in enumerate(clean.splitlines(), start=1):
        for rx, label in FORBIDDEN_RE:
            if rx.search(line):
                hits.append((i, label, raw_lines[i - 1] if i - 1 < len(raw_lines) else line))
                break
    return hits


def parse_allowlist(text: str):
    entries, errors = [], []
    for n, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        parts = [p.strip() for p in s.split("|")]
        if len(parts) != 4 or not all(parts):
            errors.append(f"allowlist:{n}: expected '<path> | <token> | <CATEGORY> | <reason>'")
            continue
        path, token, cat, reason = parts
        if cat not in CATEGORIES:
            errors.append(f"allowlist:{n}: unknown category '{cat}' (allowed: {sorted(CATEGORIES)})")
            continue
        entries.append({"path": path, "token": token, "cat": cat, "reason": reason,
                        "lineno": n, "used": False})
    return entries, errors


def evaluate(files: dict, entries):
    """files: {rel: raw}.  Returns (violations, stale_entries)."""
    by_path = {}
    for e in entries:
        by_path.setdefault(e["path"], []).append(e)
    violations = []
    for rel, raw in sorted(files.items()):
        for line_no, label, line in find_hits(rel, raw):
            allowed = False
            for e in by_path.get(rel, []):
                if e["token"] == "*" or e["token"] in line:
                    e["used"] = True
                    allowed = True
            if not allowed:
                violations.append(f"{rel}:{line_no}: forbidden {label}: {line.strip()}")
    stale = [e for e in entries if not e["used"]]
    return violations, stale


# ---------------------------------------------------------------------------
# Self-test fixtures
# ---------------------------------------------------------------------------
SELFTEST_FILES = {
    "libs/a/Clean.h": '''
        // std::atomic<int> x;  thread_local in a comment, std::mutex m;
        /* std::lock_guard g(m); atomic_thread_fence */
        const char* s = "std::atomic<bool> in a string";
        int plain = 0;
    ''',
    "libs/a/Atomic.h": "struct S { std::atomic<int> x{0}; };\n",
    "libs/a/Fence.h": "void f() { std::atomic_thread_fence(std::memory_order_release); }\n",
    "libs/a/Builtin.h": "unsigned g(unsigned* p) { return __atomic_load_n(p, 0); }\n",
    "libs/a/Include.h": "#include <mutex>\n",
    "libs/a/Mutex.h": "std::mutex mu_;\n",
    "libs/a/Lock.h": "void h() { std::scoped_lock l(a, b); }\n",
    "libs/a/Cv.h": "std::condition_variable cv;\n",
    "libs/a/Tls.h": "thread_local int t = 0;\n",
    "libs/a/Pthread.h": "void k() { pthread_mutex_lock(&m); }\n",
    "libs/a/Sync.h": "int j(int* p) { return __sync_fetch_and_add(p, 1); }\n",
    "libs/a/AllowedTok.h": "std::atomic<int> allowedMember_{0};\nstd::atomic<int> notAllowed_{0};\n",
    "libs/a/AllowedStar.h": "std::atomic<int> a; std::mutex m;\n#include <atomic>\n",
    "libs/a/DigitSep.h": "constexpr long k = 1'000'000; std::atomic<int> hidden{0};\n",
}
SELFTEST_ALLOWLIST = """
# comment
libs/a/AllowedTok.h | allowedMember_ | SLICE-B | fixture
libs/a/AllowedStar.h | * | RING | fixture
libs/a/Gone.h | goneMember_ | UNSCOPED | fixture (stale)
"""
SELFTEST_EXPECT_VIOLATION_PATHS = {
    "libs/a/Atomic.h", "libs/a/Fence.h", "libs/a/Builtin.h", "libs/a/Include.h",
    "libs/a/Mutex.h", "libs/a/Lock.h", "libs/a/Cv.h", "libs/a/Tls.h",
    "libs/a/Pthread.h", "libs/a/Sync.h", "libs/a/AllowedTok.h", "libs/a/DigitSep.h",
}


def run_selftest():
    failures = []
    entries, errors = parse_allowlist(SELFTEST_ALLOWLIST)
    if errors:
        failures.append(f"fixture allowlist failed to parse: {errors}")
    violations, stale = evaluate(SELFTEST_FILES, entries)
    got_paths = {v.split(":", 1)[0] for v in violations}
    if got_paths != SELFTEST_EXPECT_VIOLATION_PATHS:
        failures.append(f"violation paths: expected {sorted(SELFTEST_EXPECT_VIOLATION_PATHS)}, "
                        f"got {sorted(got_paths)}")
    tok_hits = [v for v in violations if v.startswith("libs/a/AllowedTok.h")]
    if len(tok_hits) != 1 or "notAllowed_" not in tok_hits[0]:
        failures.append(f"token allowlisting must allow only the matching line: {tok_hits}")
    if [e["path"] for e in stale] != ["libs/a/Gone.h"]:
        failures.append(f"stale-entry detection: expected ['libs/a/Gone.h'], got "
                        f"{[e['path'] for e in stale]}")
    _, bad = parse_allowlist("libs/x.h | tok | MAYBE | reason\nlibs/y.h | tok\n")
    if len(bad) != 2:
        failures.append(f"malformed/unknown-category allowlist lines must be rejected: {bad}")
    return failures


def iter_sources(root: Path):
    for top in SCAN_ROOTS:
        base = root / top
        if not base.is_dir():
            continue
        for path in base.rglob("*"):
            if not path.is_file() or path.suffix not in SOURCE_EXTS:
                continue
            if any(part in SKIP_DIR_NAMES for part in path.relative_to(root).parts):
                continue
            yield path


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    failures = run_selftest()
    if failures:
        print("check_no_atomics: SELF-TEST FAILED (the guard itself is broken):")
        for f in failures:
            print("  " + f)
        return 1
    if "--selftest" in sys.argv[1:]:
        print(f"check_no_atomics: self-test OK ({len(SELFTEST_FILES)} fixtures)")
        return 0

    root = (Path(args[0]) if args else Path(__file__).resolve().parents[1]).resolve()
    allow_path = Path(args[1]) if len(args) > 1 else root / "scripts" / "atomics_allowlist.txt"
    entries, errors = parse_allowlist(allow_path.read_text())
    files = {str(p.relative_to(root)): p.read_text(errors="replace") for p in iter_sources(root)}
    violations, stale = evaluate(files, entries)

    rc = 0
    if errors:
        rc = 1
        print("check_no_atomics: FAIL -- malformed allowlist:")
        for e in errors:
            print("  " + e)
    if violations:
        rc = 1
        print("check_no_atomics: FAIL -- atomics / locks outside the allowlist "
              "(shared-nothing law: own the state on one thread, cross threads by ring):")
        for v in violations:
            print("  " + v)
    if stale:
        rc = 1
        print("check_no_atomics: FAIL -- STALE allowlist entries (the site was converted; "
              "delete the entry):")
        for e in stale:
            print(f"  allowlist:{e['lineno']}: {e['path']} | {e['token']} | {e['cat']}")
    if rc == 0:
        by_cat = {}
        for e in entries:
            by_cat[e["cat"]] = by_cat.get(e["cat"], 0) + 1
        print(f"check_no_atomics: OK ({len(files)} files; allowlist {by_cat}; "
              f"self-test {len(SELFTEST_FILES)} fixtures passed)")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
