#!/usr/bin/env python3
"""check_no_atomics_or_mutexes.py — F8a shared-nothing guard (rune-client-cpp).

USER LAW (2026-10-01): there must be no std::atomic and no mutex anywhere in
Rune.  Every piece of state is owned by exactly one thread; all cross-thread
communication goes through ring buffers.  In the client, the ONLY allowed
atomics are the memory-ordering operations INSIDE the ring implementation
(include/rune/client/detail/M2ORingBuffer.h).

This guard scans src/** and include/** (comments and string literals blanked
first, line numbers preserved) for every synchronisation primitive that could
smuggle shared state back in:

  std::atomic*, atomic_* free functions (atomic_thread_fence, ...), bare
  atomic<...>, __atomic_* / __sync_* builtins, any *mutex type, lock_guard /
  unique_lock / scoped_lock / shared_lock, condition_variable*, counting /
  binary semaphores, latch, barrier, call_once / once_flag, thread_local, and
  #include of <atomic> <mutex> <shared_mutex> <condition_variable> <semaphore>
  <latch> <barrier>.

ALLOWLIST (path relative to the repo root):
  * RING     — a ring implementation: atomics-family constructs allowed (any
               count); a mutex / lock / condition_variable / thread_local there
               is still a violation.
  * SLICE-B  — TEMPORARY: sites owned by F8a slice B (Client::mtx_ and the
               Client/ClientAgent/AeronTransport lifecycle atomics, the
               TransactionalProducerImpl mutexes, nextCorrId_, ProducerImpl's
               globalFlushPending_).  Each entry is a CAP on the number of
               matches in that file: a new site fails the guard; a removed site
               prints a "ratchet" note asking to lower the cap.  Slice B deletes
               these entries.

The script self-tests its detector and the allowlist logic against synthetic
fixtures before scanning (--selftest runs only that), so a regex change that
opens a bypass or a false positive fails loudly on its own.
"""
import re
import sys
from pathlib import Path

SCAN_DIRS = ("src", "include")
SOURCE_EXTS = {".cpp", ".h", ".hpp", ".cc", ".cxx", ".ipp", ".inl"}

# kind -> regex.  Matched on comment/string-stripped text.  Order matters only
# for which kind a position is reported as (matches are de-duplicated by start).
PATTERNS = [
    ("include",   r"#\s*include\s*<\s*(?:atomic|mutex|shared_mutex|condition_variable|semaphore|latch|barrier)\s*>"),
    ("atomic",    r"\bstd\s*::\s*atomic\w*"),
    ("atomic",    r"(?<![\w:.])(?<!->)atomic_\w+"),
    ("atomic",    r"(?<![\w:])atomic\s*<"),
    ("atomic",    r"\b__(?:atomic|sync)_\w+"),
    ("mutex",     r"\bstd\s*::\s*\w*mutex\b"),
    ("mutex",     r"(?<![\w:])(?:recursive_|timed_|recursive_timed_|shared_|shared_timed_)?mutex\b"),
    ("lock",      r"(?<![\w])(?:std\s*::\s*)?(?:lock_guard|unique_lock|scoped_lock|shared_lock)\b"),
    ("condvar",   r"(?<![\w])(?:std\s*::\s*)?condition_variable\w*"),
    ("sync",      r"(?<![\w])(?:std\s*::\s*)?(?:counting_semaphore|binary_semaphore|latch|barrier|call_once|once_flag)\b"),
    ("tls",       r"\bthread_local\b"),
]
COMPILED = [(k, re.compile(p)) for k, p in PATTERNS]
ATOMIC_KINDS = {"atomic", "include"}  # what a RING entry may contain

# path -> (tag, cap or None, reason)
ALLOWLIST = {
    "include/rune/client/detail/M2ORingBuffer.h": (
        "RING", None,
        "the ring implementation's own head/tail/length/heartbeat/closed memory ordering"),
    # ---- SLICE-B (temporary; removed by F8a slice B) --------------------------
    "include/rune/client/Client.h": (
        "SLICE-B", 9,
        "Client::mtx_ + openTopic lock, stopped_/connectCalled_/disconnectedFired_, nextCorrId_"),
    "src/Client.cpp": (
        "SLICE-B", 36,
        "Client::mtx_ lock sites (pending connect/ping/define/get maps, knownTopics_)"),
    "include/rune/client/producer/TransactionalProducerImpl.h": (
        "SLICE-B", 9,
        "cmdMtx_/pendingSendsMtx_, producerId_/producerEpoch_/txnActive_, nextCorrId_ ref"),
    "src/producer/TransactionalProducerImpl.cpp": (
        "SLICE-B", 25,
        "cmdMtx_/pendingSendsMtx_ lock sites, nextCorrId_ ref"),
    "include/rune/client/producer/ProducerImpl.h": (
        "SLICE-B", 4,
        "globalFlushPending_, nextCorrId_ ref"),
    "src/producer/ProducerImpl.cpp": (
        "SLICE-B", 1,
        "nextCorrId_ ref"),
    "include/rune/client/agent/ClientAgent.h": (
        "SLICE-B", 4,
        "ClientAgent started_/stopped_/snapshot_ lifecycle atomics"),
    "include/rune/client/transport/AeronTransport.h": (
        "SLICE-B", 4,
        "AeronTransport driverRunning_/started_/stopped_ lifecycle atomics"),
}


def strip_comments_and_strings(text: str) -> str:
    """Blank out //, /* */ comments and string/char literals (newlines kept)."""
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
        elif c == "R" and text.startswith('R"', i):
            # raw string literal R"delim( ... )delim"
            k = text.find("(", i + 2)
            if k == -1:
                out.append(c)
                i += 1
                continue
            delim = text[i + 2:k]
            end = text.find(")" + delim + '"', k + 1)
            j = n if end == -1 else end + len(delim) + 2
            out.append("".join(ch if ch == "\n" else " " for ch in text[i:j]))
            i = j
        elif c in ('"', "'"):
            # Digit separators (1'000'000) are not char literals.
            if c == "'" and i > 0 and text[i - 1].isalnum():
                out.append(c)
                i += 1
                continue
            j = i + 1
            while j < n and text[j] != c and text[j] != "\n":
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


def find_matches(raw: str):
    """[(line, kind, snippet)] for every banned construct, de-duplicated."""
    clean = strip_comments_and_strings(raw)
    seen = {}
    for kind, rx in COMPILED:
        for m in rx.finditer(clean):
            # De-duplicate overlapping reports of one construct (e.g.
            # std::atomic_thread_fence hits two atomic patterns).
            if any(s <= m.start() < e for (s, e) in seen):
                continue
            seen[(m.start(), m.end())] = kind
    out = []
    for (s, e), kind in sorted(seen.items()):
        line = clean.count("\n", 0, s) + 1
        out.append((line, kind, raw[s:e].strip()))
    return out


def evaluate(files: dict, allowlist: dict):
    """files: {relpath: text}.  Returns (violations, notes)."""
    violations, notes = [], []
    for rel in sorted(files):
        matches = find_matches(files[rel])
        entry = allowlist.get(rel)
        if entry is None:
            for line, kind, snip in matches:
                violations.append(f"{rel}:{line}: banned {kind} construct `{snip}` "
                                  f"(shared-nothing law: own the state on one thread, "
                                  f"talk through a ring)")
            continue
        tag, cap, _reason = entry
        if tag == "RING":
            for line, kind, snip in matches:
                if kind not in ATOMIC_KINDS:
                    violations.append(f"{rel}:{line}: banned {kind} construct `{snip}` "
                                      f"(ring impl may only use memory-ordering atomics)")
            continue
        n = len(matches)
        if cap is not None and n > cap:
            listing = "; ".join(f"{l}:{k}:`{s}`" for l, k, s in matches)
            violations.append(f"{rel}: {n} banned constructs > {tag} cap {cap} — a NEW "
                              f"site was added ({listing})")
        elif cap is not None and n < cap:
            notes.append(f"{rel}: {n} < {tag} cap {cap} — ratchet the cap down to {n}")
    for rel in sorted(allowlist):
        if rel not in files:
            violations.append(f"{rel}: allowlisted but not found (stale allowlist entry)")
    return violations, notes


# ---- self-test ---------------------------------------------------------------
SELFTEST_DETECT = [
    # (name, snippet, expected number of matches)
    ("std_atomic_member", "struct S { std::atomic<int> x{0}; };", 1),
    ("std_atomic_spaced", "std :: atomic<bool> b;", 1),
    ("atomic_flag", "std::atomic_flag f;", 1),
    ("fence", "std::atomic_thread_fence(std::memory_order_release);", 1),
    ("bare_atomic_using_ns", "using namespace std; atomic<int> a;", 1),
    ("builtin", "__atomic_load_n(&x, __ATOMIC_ACQUIRE);", 1),
    ("sync_builtin", "__sync_fetch_and_add(&x, 1);", 1),
    ("mutex", "std::mutex m;", 1),
    ("shared_mutex", "mutable std::shared_mutex m;", 1),
    ("recursive_mutex_bare", "using namespace std; recursive_mutex m;", 1),
    ("lock_guard", "std::lock_guard<std::mutex> lk(m);", 2),
    ("unique_lock", "std::unique_lock lk(m_);", 1),
    ("scoped_lock", "std::scoped_lock lk(a, b);", 1),
    ("condvar", "std::condition_variable_any cv;", 1),
    ("semaphore", "std::counting_semaphore<4> s{0};", 1),
    ("latch", "std::latch l{1};", 1),
    ("call_once", "std::once_flag f; std::call_once(f, []{});", 2),
    ("thread_local", "thread_local int x = 0;", 1),
    ("include_atomic", "#include <atomic>", 1),
    ("include_mutex", "#include <mutex>\n#include <vector>", 1),
    # false-positive guards
    ("comment_line", "// std::mutex and std::atomic are banned here", 0),
    ("comment_block", "/* std::lock_guard<std::mutex> */ int x;", 0),
    ("string_literal", 'const char* s = "std::atomic<int> thread_local";', 0),
    ("raw_string", 'auto s = R"x(std::mutex m;)x";', 0),
    ("identifiers", "int mtx_ = 0; int cmdMtx_; bool nonatomic = false; int mutexCount;", 0),
    ("member_named_atomic_ish", "x.atomicity = 1; foo_atomic_bar(); obj.atomic_view(); p->atomic_n;", 0),
    ("free_atomic_fn", "atomic_store(&a, 1); std::atomic_load(&a);", 2),
    ("digit_separator", "int64_t n = 1'000'000; std::mutex m;", 1),
    ("include_other", "#include <future>\n#include <thread>", 0),
    ("ring_word_names", "storeRelease64_(idx, v); loadAcquire32_(idx);", 0),
]

SELFTEST_ALLOW = [
    # (name, files, allowlist, expected violations, expected notes)
    ("unlisted_file_fails",
     {"src/a.cpp": "std::mutex m;"}, {}, 1, 0),
    ("ring_allows_atomics",
     {"include/r.h": "#include <atomic>\nstd::atomic_thread_fence(std::memory_order_acquire);"},
     {"include/r.h": ("RING", None, "")}, 0, 0),
    ("ring_rejects_mutex",
     {"include/r.h": "std::mutex m;"},
     {"include/r.h": ("RING", None, "")}, 1, 0),
    ("sliceb_at_cap_ok",
     {"src/c.cpp": "std::mutex m; std::atomic<int> a;"},
     {"src/c.cpp": ("SLICE-B", 2, "")}, 0, 0),
    ("sliceb_over_cap_fails",
     {"src/c.cpp": "std::mutex m; std::atomic<int> a; std::atomic<int> b;"},
     {"src/c.cpp": ("SLICE-B", 2, "")}, 1, 0),
    ("sliceb_under_cap_ratchet_note",
     {"src/c.cpp": "std::mutex m;"},
     {"src/c.cpp": ("SLICE-B", 2, "")}, 0, 1),
    ("stale_allowlist_fails",
     {}, {"src/gone.cpp": ("SLICE-B", 1, "")}, 1, 0),
]


def run_selftest():
    failures = []
    for name, snippet, want in SELFTEST_DETECT:
        got = len(find_matches(snippet))
        if got != want:
            failures.append(f"detect '{name}': expected {want} match(es), got {got}: "
                            f"{find_matches(snippet)}")
    for name, files, allow, want_v, want_n in SELFTEST_ALLOW:
        v, n = evaluate(files, allow)
        if len(v) != want_v or len(n) != want_n:
            failures.append(f"allowlist '{name}': expected {want_v} violation(s)/{want_n} "
                            f"note(s), got {len(v)}/{len(n)}: {v} {n}")
    return failures


def collect(root: Path):
    files = {}
    for d in SCAN_DIRS:
        base = root / d
        if not base.is_dir():
            continue
        for p in base.rglob("*"):
            if p.is_file() and p.suffix in SOURCE_EXTS:
                files[p.relative_to(root).as_posix()] = p.read_text(encoding="utf-8",
                                                                     errors="replace")
    return files


def main(argv):
    failures = run_selftest()
    if failures:
        print("check_no_atomics_or_mutexes: SELF-TEST FAILED")
        for f in failures:
            print("  " + f)
        return 2
    if "--selftest" in argv:
        print(f"check_no_atomics_or_mutexes: self-test OK "
              f"({len(SELFTEST_DETECT)} detector + {len(SELFTEST_ALLOW)} allowlist fixtures)")
        return 0
    args = [a for a in argv if not a.startswith("--")]
    root = Path(args[0]) if args else Path(__file__).resolve().parent.parent
    files = collect(root)
    violations, notes = evaluate(files, ALLOWLIST)
    for n in notes:
        print("note: " + n)
    if violations:
        print(f"check_no_atomics_or_mutexes: {len(violations)} violation(s)")
        for v in violations:
            print("  " + v)
        return 1
    print(f"check_no_atomics_or_mutexes: OK ({len(files)} files; "
          f"{sum(1 for e in ALLOWLIST.values() if e[0] == 'SLICE-B')} SLICE-B entries pending)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
