#!/usr/bin/env python3
"""check_no_atomics.py — F9a regression guard: NoAtomicsOrMutexesOutsideAllowlist.

USER LAW (2026-10-01): "there should be no atomic or mutex anywhere ... with the
exception of log cleaner/compaction".  Rune is strictly shared-nothing: every
piece of state is owned by ONE thread and everything that crosses threads is a
RING MESSAGE.  Memory-ordering / blocking primitives are allowed ONLY at the
exact sites listed in scripts/atomics_allowlist.txt:
  RING        — inside a ring-buffer implementation (agrona M2O/O2O copies,
                the kernel-shared io_uring SQ/CQ rings);
  CLEANER     — the off-thread LogCleaner / compaction channel;
  CRASH-ORDER — a compiler-only barrier ordering stores into a single-writer
                mmap'd file (not shared state, not an atomic, not a mutex);
  CPU-HINT    — a spin-wait CPU hint (pause/yield) in the busy-spin idle
                strategy — no shared state, no ordering contract;
  CYCLE-COUNTER — a read of the CPU's own cycle counter (rdtsc / cntvct_el0)
                for a single-thread cost measurement — no shared state;
  SLICE-B     — TEMPORARY: F9 slice B converts these, then deletes the entry;
  UNSCOPED    — TEMPORARY: found by this guard, owned by neither slice yet;
  DEBUG-ONLY  — a debug-build test record compiled out of release builds (no
                production coordination; e.g. the D58 metric owner tags).

Scope: every C/C++ source under libs/** and src/** EXCEPT test code (any path
component named "tests").  Production goes to zero first; tests are converted
in a later slice.  Comments, string/char literals and digit separators are
neutralised first.

DETECTION (a line is a hit if any family matches its code text):
  * std facilities under ANY spelling of the std namespace — std::, ::std::,
    every `namespace X = std;` alias, and BARE names in files with
    `using namespace std;`:  atomic*, memory_order*, kill_dependency, *mutex,
    condition_variable(_any), lock_guard / unique_lock / shared_lock /
    scoped_lock, counting/binary_semaphore, latch, barrier, once_flag /
    call_once, jthread, stop_source / stop_token / stop_callback, promise /
    future / shared_future / async / packaged_task;
  * builtins & C: atomic_* / __atomic_* / __c11_atomic_* / __ATOMIC_*, _Atomic,
    __sync_*, thread_local, sem_*(), pthread_{mutex,spin,rwlock,barrier,cond}*,
    pthread_once, os_unfair_lock*, dispatch_semaphore*, OSAtomic*;
  * inline asm (asm / __asm / __asm__ — e.g. a compiler barrier);
  * sync header includes: <atomic> <mutex> <shared_mutex> <condition_variable>
    <semaphore> <latch> <barrier> <stop_token> <future> <stdatomic.h>
    <semaphore.h>;
  * Aeron memory-ordering API: AtomicBuffer *Ordered / *Volatile accessors,
    compareAndSet*, getAndAdd*, getAndSet*, addAndGet*, aeron::concurrent::atomic::,
    AtomicCounter;
  * any USE of an identifier DECLARED atomic anywhere in the scanned tree
    (x.load(), x.fetch_add(), x++ ...), incl. decltype(x): an allowlisted
    atomic member can neither gain new access sites nor leak its type.

ALLOWLIST (exact, per site — no wildcards):
    <path> | <normalized line> | <count> | <CATEGORY> | <reason>
The normalized line is the hit line's code (trailing comment removed, runs of
whitespace collapsed to one space).  For each (path, normalized line) the
number of hit lines must EQUAL <count>: an extra occurrence (copy-paste of an
allowlisted line) or a missing one (site converted/changed — delete or update
the entry) both FAIL.  A ring implementation lists its exact multiset of lines.

Self-test: run_selftest() pins every detection family, the spelling variants,
decltype leakage, comment/string/digit-separator immunity, exact counts and
malformed-allowlist rejection on synthetic fixtures before scanning the tree.
"""
import re
import sys
from collections import Counter
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


STD_NAMES = (
    r"atomic\w*|memory_order\w*|kill_dependency"
    r"|(?:recursive_|timed_|recursive_timed_|shared_|shared_timed_)?mutex|lock|try_lock"
    r"|condition_variable(?:_any)?|lock_guard|unique_lock|shared_lock|scoped_lock"
    r"|counting_semaphore|binary_semaphore|latch|barrier|once_flag|call_once"
    r"|jthread|stop_source|stop_token|stop_callback"
    r"|promise|future|shared_future|async|packaged_task"
)

PLAIN_FAMILIES = [
    (r"(?:__c11_|__)?atomic_\w+", "atomic_* builtin/free function"),
    (r"\b__ATOMIC_\w+", "__ATOMIC_* order"),
    (r"\b_Atomic\b", "_Atomic"),
    (r"\b__sync_\w+", "__sync builtin"),
    (r"\bthread_local\b", "thread_local"),
    (r"\bsem_\w+\s*\(", "POSIX semaphore"),
    (r"\bpthread_(?:mutex|spin|rwlock|barrier|cond)\w*", "pthread lock/cond/barrier"),
    (r"\bpthread_once\b", "pthread_once"),
    (r"\bos_unfair_lock\w*", "os_unfair_lock"),
    (r"\bdispatch_semaphore\w*", "dispatch_semaphore"),
    (r"\bOSAtomic\w*", "OSAtomic"),
    (r"(?<![\w])(?:asm|__asm|__asm__)\b", "inline asm"),
    (r"#\s*include\s*<(?:atomic|mutex|shared_mutex|condition_variable|semaphore|latch|barrier"
     r"|stop_token|future|stdatomic\.h|semaphore\.h)>", "sync header include"),
    (r"\b\w*(?:Ordered|Volatile|Release|Acquire)\s*\(", "Aeron ordered/volatile/release/acquire accessor"),
    (r"\bproposeM(?:ax|in)(?:Ordered|Release)?\s*\(", "Aeron propose* accessor"),
    (r"\bAtomicArrayUpdater\b", "Aeron AtomicArrayUpdater"),
    (r"\b(?:compareAndSet|getAndAdd|getAndSet|addAndGet)\w*\s*\(", "Aeron CAS/RMW accessor"),
    (r"\baeron::concurrent::atomic::", "aeron::concurrent::atomic"),
    (r"\bAtomicCounter\b", "Aeron AtomicCounter"),
]
PLAIN_RE = [(re.compile(p), label) for p, label in PLAIN_FAMILIES]

USING_STD_RE = re.compile(r"\busing\s+namespace\s+(?:::)?std\s*;")
ALIAS_RE = re.compile(r"\bnamespace\s+(\w+)\s*=\s*(?:::)?std\s*;")
AERON_NS_RE = re.compile(r"\busing\s+namespace\s+(?:::)?aeron::concurrent(?:::atomic)?\s*;"
                         r"|\bnamespace\s+\w+\s*=\s*(?:::)?aeron::concurrent(?:::atomic)?\s*;")
AERON_ALIAS_RE = re.compile(r"\bnamespace\s+(\w+)\s*=\s*(?:::)?aeron::concurrent(?:::atomic)?\s*;")
AERON_OPS = r"(?:cmpxchg|xchg|thread_fence|fence|acquire|release|loadFence|storeFence|getAndAddInt32|getAndAddInt64)"
# Mutex / condvar / once_flag declarations: their identifiers are tracked like
# atomics so a new `mu_.lock()` critical section on an allowlisted mutex is caught.
MUTEX_DECL_RE = re.compile(
    r"\b(?:\w+::)*(?:(?:recursive_|timed_|recursive_timed_|shared_|shared_timed_)?mutex"
    r"|condition_variable(?:_any)?|once_flag)\s+(\w+)\s*[{=;\[(]")
ATOMIC_DECL_RE = re.compile(
    r"(?:\b(?:\w+::)*atomic(?:_ref)?\s*<[^;{}()]*?>|\b_Atomic\s*\([^)]*\)|\b_Atomic\s+[\w:\s*]+?)"
    r"\s*[*&]?\s*(\w+)\s*(?:[{=;\[]|\()")

CATEGORIES = {"RING", "CLEANER", "CRASH-ORDER", "CPU-HINT", "CYCLE-COUNTER", "SLICE-B", "UNSCOPED", "DEBUG-ONLY"}
SOURCE_EXTS = {".cpp", ".h", ".hpp", ".cc", ".cxx", ".ipp", ".inl"}
SKIP_DIR_NAMES = {"tests", "build", "build-fast", "build-tsan", "build-release",
                  "cmake-build-debug", "cmake-build-release", ".git", "_deps"}
SCAN_ROOTS = ("libs", "src")


def std_family_re(aliases, using_std):
    prefixes = ["std"] + sorted(aliases)
    pref = "|".join(re.escape(a) for a in prefixes)
    rx = [re.compile(r"(?<![\w])(?:::)?(?:" + pref + r")\s*::\s*(?:" + STD_NAMES + r")\b")]
    if using_std:
        rx.append(re.compile(r"(?<![\w:.>])(?:" + STD_NAMES + r")\b"))
    return rx


def normalize(raw_line: str, clean_line: str) -> str:
    """Code part of the raw line (up to the last non-blank char of the
    comment/string-blanked line, so string contents survive), whitespace
    collapsed."""
    last = len(clean_line.rstrip())
    return " ".join(raw_line[:last].split())


def stem(rel: str) -> str:
    return Path(rel).stem


def collect_atomic_names(cleans: dict):
    """{file stem: identifiers declared atomic in Stem.h / Stem.cpp}."""
    names = {}
    for rel, clean in cleans.items():
        for m in ATOMIC_DECL_RE.finditer(clean):
            names.setdefault(stem(rel), set()).add(m.group(1))
        for m in MUTEX_DECL_RE.finditer(clean):
            names.setdefault(stem(rel), set()).add(m.group(1))
    return names


def find_hits(raw: str, clean: str, atomic_names, rel: str = ""):
    """Return [(line_no, label, normalized_line)] for every forbidden line."""
    using_std = bool(USING_STD_RE.search(clean))
    aliases = set(ALIAS_RE.findall(clean))
    std_rx = std_family_re(aliases, using_std)
    # decltype(x) of ANY atomic-declared identifier is flagged tree-wide; plain
    # uses are flagged in the declaring class's own files (same stem: Foo.h /
    # Foo.cpp) — atomic members are private, and unrelated classes may reuse a
    # name (MetadataService::minIsr_ is a plain int).
    all_names = set().union(*atomic_names.values()) if atomic_names else set()
    own = atomic_names.get(stem(rel), set()) if atomic_names else set()
    decl_rx = use_rx = member_rx = aeron_rx = None
    if AERON_NS_RE.search(clean):
        apre = "|".join(re.escape(a) for a in sorted(set(AERON_ALIAS_RE.findall(clean)) | {"atomic"}))
        aeron_rx = re.compile(r"(?<![\w])(?:(?:" + apre + r")\s*::\s*)?" + AERON_OPS + r"\s*\(")
    if all_names:
        decl_rx = re.compile(r"\bdecltype\s*\([^;]*?\b(?:" +
                             "|".join(re.escape(n) for n in sorted(all_names)) + r")\b")
    if all_names:
        # m1: member access to ANY atomic/mutex-declared identifier from ANY file
        # (a split implementation file, friend or derived class) is flagged.
        member_rx = re.compile(r"(?:\.|->)\s*(?:" +
                               "|".join(re.escape(n) for n in sorted(all_names)) + r")\b")
    if own:
        use_rx = re.compile(r"(?<![\w])(?:" + "|".join(re.escape(n) for n in sorted(own)) + r")\b")
    raw_lines = raw.splitlines()
    hits = []
    for i, line in enumerate(clean.splitlines(), start=1):
        label = None
        if any(r.search(line) for r in std_rx):
            label = "std sync facility"
        else:
            for rx, lab in PLAIN_RE:
                if rx.search(line):
                    label = lab
                    break
        if label is None and aeron_rx is not None and aeron_rx.search(line):
            label = "Aeron atomic op via namespace using/alias"
        if label is None and member_rx is not None and member_rx.search(line):
            label = "member access to an atomic/mutex-declared identifier"
        if label is None and decl_rx is not None and decl_rx.search(line):
            label = "decltype of an atomic"
        if label is None and use_rx is not None and use_rx.search(line):
            label = "use of an atomic-declared identifier"
        if label is not None:
            raw_line = raw_lines[i - 1] if i - 1 < len(raw_lines) else line
            hits.append((i, label, normalize(raw_line, line)))
    return hits


def parse_allowlist(text: str):
    entries, errors = [], []
    for n, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        parts = [p.strip() for p in s.split(" | ")]
        if len(parts) != 5 or not all(parts):
            errors.append(f"allowlist:{n}: expected '<path> | <normalized line> | <count> | "
                          f"<CATEGORY> | <reason>'")
            continue
        path, text_, count, cat, reason = parts
        if text_ == "*":
            errors.append(f"allowlist:{n}: wildcard entries are not allowed (list each site)")
            continue
        if not count.isdigit() or int(count) < 1:
            errors.append(f"allowlist:{n}: count must be a positive integer, got '{count}'")
            continue
        if cat not in CATEGORIES:
            errors.append(f"allowlist:{n}: unknown category '{cat}' (allowed: {sorted(CATEGORIES)})")
            continue
        entries.append({"path": path, "text": " ".join(text_.split()), "count": int(count),
                        "cat": cat, "reason": reason, "lineno": n})
    keys = Counter((e["path"], e["text"]) for e in entries)
    for (path, text_), c in keys.items():
        if c > 1:
            errors.append(f"allowlist: duplicate entry for {path} | {text_} (merge into one count)")
    return entries, errors


def evaluate(files: dict, entries):
    """files: {rel: raw}.  Returns (unexpected, mismatched_entries)."""
    cleans = {rel: strip_comments_and_strings(raw) for rel, raw in files.items()}
    atomic_names = collect_atomic_names(cleans)
    actual = {}       # (path, text) -> [line numbers]
    labels = {}
    for rel in sorted(files):
        for line_no, label, text_ in find_hits(files[rel], cleans[rel], atomic_names, rel):
            actual.setdefault((rel, text_), []).append(line_no)
            labels[(rel, text_)] = label
    expected = {(e["path"], e["text"]): e for e in entries}
    unexpected = []
    for key, lines in sorted(actual.items()):
        e = expected.get(key)
        if e is None:
            for ln in lines:
                unexpected.append(f"{key[0]}:{ln}: forbidden {labels[key]}: {key[1]}")
        elif len(lines) > e["count"]:
            unexpected.append(f"{key[0]}:{lines}: {len(lines)} occurrences of an allowlisted line "
                              f"(allowlist:{e['lineno']} expects {e['count']}): {key[1]}")
    mismatched = []
    for key, e in expected.items():
        n = len(actual.get(key, []))
        if n < e["count"]:
            mismatched.append((e, n))
    return unexpected, mismatched


# ---------------------------------------------------------------------------
# Self-test fixtures
# ---------------------------------------------------------------------------
SELFTEST_FILES = {
    "libs/a/Clean.h": """
        // std::atomic<int> x;  thread_local in a comment, std::mutex m;
        /* std::lock_guard g(m); atomic_thread_fence; asm volatile("" ::: "memory") */
        const char* s = "std::atomic<bool> in a string";
        int plain = 0; int mutex_count = 0; struct future_cfg {}; int sem = 0;
        long big = 1'000'000; std::thread t; std::vector<int> v;
    """,
    "libs/a/Atomic.h": "struct S { std::atomic<int> x{0}; };\n",
    "libs/a/GlobalStd.h": "::std::atomic_flag f;\n",
    "libs/a/Fence.h": "void f() { std::atomic_thread_fence(std::memory_order_release); }\n",
    "libs/a/MemOrder.h": "constexpr auto kOrd = std::memory_order_acquire;\n",
    "libs/a/Builtin.h": "unsigned g(unsigned* p) { return __atomic_load_n(p, 0); }\n",
    "libs/a/C11.h": "int g2(int* p) { return __c11_atomic_load(p, 5); }\n",
    "libs/a/CAtomic.h": "_Atomic int ca;\n",
    "libs/a/Include.h": "#include <mutex>\n",
    "libs/a/IncludeFuture.h": "#include <future>\n",
    "libs/a/Mutex.h": "std::mutex mu_;\n",
    "libs/a/Lock.h": "void h() { std::scoped_lock l(a, b); }\n",
    "libs/a/Cv.h": "std::condition_variable cv;\n",
    "libs/a/Tls.h": "thread_local int t = 0;\n",
    "libs/a/Pthread.h": "void k() { pthread_mutex_lock(&m); }\n",
    "libs/a/PthreadSpin.h": "void k2() { pthread_spin_lock(&s); }\n",
    "libs/a/PthreadBarrier.h": "pthread_barrier_t b;\n",
    "libs/a/PthreadOnce.h": "void k3() { pthread_once(&o, init); }\n",
    "libs/a/Sem.h": "void k4() { sem_wait(&s); }\n",
    "libs/a/Unfair.h": "os_unfair_lock ul = OS_UNFAIR_LOCK_INIT;\n",
    "libs/a/Dispatch.h": "void k5() { dispatch_semaphore_signal(ds); }\n",
    "libs/a/Sync.h": "int j(int* p) { return __sync_fetch_and_add(p, 1); }\n",
    "libs/a/Asm.h": "void b() { asm volatile(\"\" ::: \"memory\"); }\n",
    "libs/a/UsingStd.h": "using namespace std;\nmutex m2;\n",
    "libs/a/UsingStdAtomic.h": "using namespace std;\natomic<int> a2{0};\n",
    "libs/a/UsingStdLock.h": "using namespace std;\nvoid q() { lock_guard<M> g(m); unique_lock<M> u(m); }\n",
    "libs/a/UsingStdCv.h": "using namespace std;\ncondition_variable cv2;\n",
    "libs/a/Alias.h": "namespace S = std;\nS::atomic<int> a3;\n",
    "libs/a/AliasMutex.h": "namespace s2 = ::std;\nvoid r() { s2::scoped_lock l(m); }\n",
    "libs/a/Jthread.h": "std::jthread jt;\n",
    "libs/a/StopToken.h": "void w(std::stop_token st);\n",
    "libs/a/Promise.h": "std::promise<int> pr;\n",
    "libs/a/Future.h": "std::shared_future<void> sf;\n",
    "libs/a/Async.h": "auto fu = std::async([] {});\n",
    "libs/a/Packaged.h": "std::packaged_task<int()> pt;\n",
    "libs/a/AeronOrdered.h": "void o(B& b) { b.putInt64Ordered(0, 1); }\n",
    "libs/a/AeronVolatile.h": "long o2(B& b) { return b.getInt32Volatile(0); }\n",
    "libs/a/AeronCas.h": "bool o3(B& b) { return b.compareAndSetInt64(0, 1, 2); }\n",
    "libs/a/AeronRmw.h": "long o4(B& b) { return b.getAndAddInt64(0, 1); }\n",
    "libs/a/AeronNs.h": "using X = aeron::concurrent::atomic::Foo;\n",
    "libs/a/AeronCounter.h": "aeron::concurrent::AtomicCounter* c;\n",
    "libs/a/AllowedTok.h": "std::atomic<int> allowedMember_{0};\nstd::atomic<int> notAllowed_{0};\n",
    "libs/a/Decltype.h": "decltype(allowedMember_) leaked;\n",
    "libs/a/AllowedTok.cpp": "int u() { return allowedMember_.load(); }\n",
    "libs/a/Other.h": "struct O { int allowedMember_ = 0; };\n",
    "libs/a/CountTwice.h": "std::atomic<int> twice_{0}; // a\nstd::atomic<int> twice_{0}; // b\n",
    "libs/a/CountExact.h": "std::atomic_thread_fence(std::memory_order_release);\n"
                           "std::atomic_thread_fence(std::memory_order_release);\n",
    "libs/a/DigitSep.h": "constexpr long k = 1'000'000; std::atomic<int> hidden{0};\n",
    "libs/a/AeronSetOrdered.h": "void o5(C& c) { c.setOrdered(1); }\n",
    "libs/a/AeronSetRelease.h": "void o6(C& c) { c.setRelease(1); }\n",
    "libs/a/AeronPropose.h": "void o7(C& c) { c.proposeMaxOrdered(1); c.proposeMax(2); }\n",
    "libs/a/AeronAddOrdered.h": "void o8(B& b) { b.addInt64Ordered(0, 1); }\n",
    "libs/a/AeronUpdater.h": "AtomicArrayUpdater<int*> up;\n",
    "libs/a/AeronAlias.h": "namespace atm = aeron::concurrent::atomic;\nvoid o9() { atm::release(); atm::fence(); }\n",
    "libs/a/AeronUsingConc.h": "using namespace aeron::concurrent;\nvoid o10(long* p) { atomic::cmpxchg(p, 0, 1); }\n",
    "libs/a/AeronUsingAtomic.h": "using namespace aeron::concurrent::atomic;\nvoid o11(long* p) { xchg(p, 1); thread_fence(); }\n",
    "libs/a/MuDecl.h": "struct M { std::mutex muX_; };\n",
    "libs/a/MuDecl.cpp": "void M2() { muX_.lock(); muX_.unlock(); }\n",
    "libs/a/StdLock.h": "void l2() { std::lock(a, b); std::try_lock(a, b); }\n",
    "libs/a/SplitImpl.cpp": "int v(AllowedTok& t) { return t.allowedMember_.load(std::memory_order_relaxed) + t.allowedMember_++; }\n",
}
SELFTEST_ALLOWLIST = """
# comment
libs/a/AllowedTok.h | std::atomic<int> allowedMember_{0}; | 1 | SLICE-B | fixture
libs/a/CountTwice.h | std::atomic<int> twice_{0}; | 1 | UNSCOPED | fixture: 2 copies vs count 1
libs/a/CountExact.h | std::atomic_thread_fence(std::memory_order_release); | 2 | RING | fixture: exact multiset
libs/a/Gone.h | std::atomic<int> goneMember_{0}; | 1 | UNSCOPED | fixture (missing)
libs/a/Asm.h | void b() { asm volatile("" ::: "memory"); } | 2 | CRASH-ORDER | fixture: 1 present vs count 2
"""
SELFTEST_EXPECT_VIOLATION_PATHS = {
    "libs/a/Atomic.h", "libs/a/GlobalStd.h", "libs/a/Fence.h", "libs/a/MemOrder.h",
    "libs/a/Builtin.h", "libs/a/C11.h", "libs/a/CAtomic.h", "libs/a/Include.h",
    "libs/a/IncludeFuture.h", "libs/a/Mutex.h", "libs/a/Lock.h", "libs/a/Cv.h", "libs/a/Tls.h",
    "libs/a/Pthread.h", "libs/a/PthreadSpin.h", "libs/a/PthreadBarrier.h", "libs/a/PthreadOnce.h",
    "libs/a/Sem.h", "libs/a/Unfair.h", "libs/a/Dispatch.h", "libs/a/Sync.h",
    "libs/a/UsingStd.h", "libs/a/UsingStdAtomic.h", "libs/a/UsingStdLock.h", "libs/a/UsingStdCv.h",
    "libs/a/Alias.h", "libs/a/AliasMutex.h", "libs/a/Jthread.h", "libs/a/StopToken.h",
    "libs/a/Promise.h", "libs/a/Future.h", "libs/a/Async.h", "libs/a/Packaged.h",
    "libs/a/AeronOrdered.h", "libs/a/AeronVolatile.h", "libs/a/AeronCas.h", "libs/a/AeronRmw.h",
    "libs/a/AeronNs.h", "libs/a/AeronCounter.h", "libs/a/AllowedTok.h", "libs/a/Decltype.h", "libs/a/AllowedTok.cpp",
    "libs/a/CountTwice.h", "libs/a/DigitSep.h",
    "libs/a/AeronSetOrdered.h", "libs/a/AeronSetRelease.h", "libs/a/AeronPropose.h",
    "libs/a/AeronAddOrdered.h", "libs/a/AeronUpdater.h", "libs/a/AeronAlias.h",
    "libs/a/AeronUsingConc.h", "libs/a/AeronUsingAtomic.h", "libs/a/MuDecl.h",
    "libs/a/MuDecl.cpp", "libs/a/StdLock.h", "libs/a/SplitImpl.cpp",
}


def run_selftest():
    failures = []
    entries, errors = parse_allowlist(SELFTEST_ALLOWLIST)
    if errors:
        failures.append(f"fixture allowlist failed to parse: {errors}")
    unexpected, mismatched = evaluate(SELFTEST_FILES, entries)
    got = {v.split(":", 1)[0] for v in unexpected}
    if got != SELFTEST_EXPECT_VIOLATION_PATHS:
        failures.append(f"violations: missing {sorted(SELFTEST_EXPECT_VIOLATION_PATHS - got)}, "
                        f"unexpected {sorted(got - SELFTEST_EXPECT_VIOLATION_PATHS)}")
    tok = [v for v in unexpected if v.startswith("libs/a/AllowedTok.h")]
    if len(tok) != 1 or "notAllowed_" not in tok[0]:
        failures.append(f"exact-line allowlisting must allow only the listed line: {tok}")
    if any(v.startswith("libs/a/CountExact.h") for v in unexpected):
        failures.append("an exact multiset (count 2, 2 present) must pass")
    mm = sorted(e["path"] for e, _ in mismatched)
    if mm != ["libs/a/Asm.h", "libs/a/Gone.h"]:
        failures.append(f"missing-occurrence detection: expected Asm.h + Gone.h, got {mm}")
    _, bad = parse_allowlist(
        "libs/x.h | x; | 1 | MAYBE | reason\n"            # unknown category
        "libs/y.h | x;\n"                                 # malformed
        "libs/z.h | * | 1 | RING | wildcard\n"            # wildcard banned
        "libs/w.h | x; | zero | RING | bad count\n"       # bad count
        "libs/v.h | x; | 1 | RING | dup\nlibs/v.h | x; | 1 | RING | dup\n")
    if len(bad) != 5:
        failures.append(f"allowlist validation must reject 5 bad lines, got {len(bad)}: {bad}")
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
    unexpected, mismatched = evaluate(files, entries)

    rc = 0
    if errors:
        rc = 1
        print("check_no_atomics: FAIL -- malformed allowlist:")
        for e in errors:
            print("  " + e)
    if unexpected:
        rc = 1
        print("check_no_atomics: FAIL -- atomics / locks / ordering outside the allowlist "
              "(shared-nothing law: own the state on one thread, cross threads by ring):")
        for v in unexpected:
            print("  " + v)
    if mismatched:
        rc = 1
        print("check_no_atomics: FAIL -- allowlisted sites MISSING (converted or changed: "
              "delete / update the entry):")
        for e, n in mismatched:
            print(f"  allowlist:{e['lineno']}: {e['path']} | {e['text']} | expected {e['count']}, "
                  f"found {n}")
    if rc == 0:
        by_cat = Counter()
        for e in entries:
            by_cat[e["cat"]] += e["count"]
        print(f"check_no_atomics: OK ({len(files)} files; allowlisted sites {dict(by_cat)}; "
              f"self-test {len(SELFTEST_FILES)} fixtures passed)")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
