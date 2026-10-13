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

F8c C9 adds the FUTURE kind: std::future / shared_future / promise /
packaged_task / async / launch (and future_status / future_error ...,
std::experimental:: forms, bare future< / promise< / shared_future /
packaged_task) and #include <future>.  It is banned in src/** and include/**
(ring files included) and ALSO in examples/** bench/** tools/** (FUTURE kind
only there).  `--futures` checks just that kind across all five trees
(ctest NoFuturesOrPromisesGuardRegression).

ALLOWLIST (path relative to the repo root):
  * RING     — a ring implementation: atomics-family constructs allowed (any
               count); a mutex / lock / condition_variable / thread_local there
               is still a violation.
  * SLICE-B  — TEMPORARY: sites owned by F8a slice B (Client::mtx_ and the
               Client/ClientAgent/AeronTransport lifecycle atomics, the
               TransactionalProducerImpl mutexes, nextCorrId_).  Each entry is an EXACT MULTISET of
               (kind, normalized snippet) -> count for that file: ANY difference
               fails — a new site, and also a removed one (which must then be
               removed from the allowlist).  EMPTY since F8c C5 (the last
               entries, TransactionalProducerImpl, were removed).

The script self-tests its detector and the allowlist logic against synthetic
fixtures before scanning (--selftest runs only that), so a regex change that
opens a bypass or a false positive fails loudly on its own.
"""
import re
import sys
from pathlib import Path

SCAN_DIRS = ("src", "include")
# F8c C9: the examples, benchmarks and tools were migrated to the completion-
# token API too; only the FUTURE kind is enforced there (their harness code may
# still time / count with other constructs — the shared-nothing law for the
# library itself is src/ + include/).  tests/e2e/ holds client apps shipped in
# Docker images (rune_ql_probe), so it is scanned like examples/; the rest of
# tests/ is not.
FUTURE_ONLY_DIRS = ("examples", "bench", "tools", "tests/e2e")  # tests/e2e: shipped probe apps (C9 fix1)
SOURCE_EXTS = {".cpp", ".h", ".hpp", ".cc", ".cxx", ".ipp", ".inl"}

# kind -> regex.  Matched on comment/string-stripped text.  Order matters only
# for which kind a position is reported as (matches are de-duplicated by start).
PATTERNS = [
    ("include",   r"#\s*include\s*<\s*(?:atomic|stdatomic\.h|mutex|shared_mutex|condition_variable|semaphore|latch|barrier|stop_token)\s*>"),
    ("atomic",    r"\bstd\s*::\s*atomic\w*"),
    ("atomic",    r"(?<![\w:.])(?<!->)atomic_\w+"),
    # Bare / alias-qualified (s::atomic<, after `namespace s = std`); std::atomic
    # is reported by the pattern above (same start: de-duplicated).
    ("atomic",    r"(?<![\w.])(?<!->)atomic\s*<"),
    ("atomic",    r"\b__(?:atomic|sync|c11_atomic)_\w+"),
    ("atomic",    r"\b_Atomic\b"),
    # Aeron's atomics APIs (AtomicBuffer ordered/volatile accessors, CAS, RMW,
    # and the aeron::concurrent::atomic namespace).
    ("atomic",    r"\baeron\s*::\s*concurrent\s*::\s*atomic\s*::"),
    ("atomic",    r"\b(?:put|get|add)\w*(?:Ordered|Volatile)\b"),
    ("atomic",    r"\b(?:compareAndSet|getAndAdd|getAndSet|addAndGet)\w*"),
    ("mutex",     r"\bstd\s*::\s*\w*mutex\b"),
    ("mutex",     r"(?<![\w.])(?<!->)(?:recursive_|timed_|recursive_timed_|shared_|shared_timed_)?mutex\b"),
    ("mutex",     r"\bpthread_(?:mutex|spin|rwlock|cond)\w*"),
    ("mutex",     r"\bos_unfair_lock\w*"),
    ("lock",      r"(?<![\w])(?:std\s*::\s*)?(?:lock_guard|unique_lock|scoped_lock|shared_lock)\b"),
    ("condvar",   r"(?<![\w])(?:std\s*::\s*)?condition_variable\w*"),
    ("sync",      r"(?<![\w])(?:std\s*::\s*)?(?:counting_semaphore|binary_semaphore|latch|barrier|call_once|once_flag)\b"),
    ("sync",      r"(?<![\w])(?:std\s*::\s*)?(?:jthread|stop_source|stop_token|stop_callback)\b"),
    ("tls",       r"\bthread_local\b"),
    # F8c C9: futures / promises / packaged tasks / std::async (and their
    # status / error / launch companions).  Every async op completes through a
    # Token into Completions or a CompletionHandler instead.
    ("future",    r"#\s*include\s*<\s*(?:experimental\s*/\s*)?future\s*>"),
    ("future",    r"\bstd\s*::\s*(?:experimental\s*::\s*)?(?:future|shared_future|promise|packaged_task|async|launch)\w*"),
    # Bare (using-directive), alias-qualified (s::) or foreign (boost::) forms.
    # `.`/`->` excluded: a member named future/async is not the std facility.
    ("future",    r"(?<![\w.])(?<!->)(?:shared_future|packaged_task)\b"),
    # `<` then a type-ish token (letter, _, ::, or `>`): `future < 3` is a compare.
    ("future",    r"(?<![\w.])(?<!->)(?:future|promise)\s*<\s*(?=[A-Za-z_:>])"),
    ("future",    r"(?<![\w.])(?<!->)async\s*\("),
    ("future",    r"(?<![\w.])(?<!->)launch\s*::"),
    # C9 fix round 1: namespace escapes that would let any bare / aliased banned
    # construct through.  `using namespace std::chrono_literals` (a sub-
    # namespace) and aliases of sub-namespaces (std::chrono) stay allowed.
    ("stdns",     r"\busing\s+namespace\s+(?:::\s*)?std\s*;"),
    ("stdns",     r"\bnamespace\s+\w+\s*=\s*(?:::\s*)?std\s*(?:::\s*experimental\s*)?;"),
]
# Kinds enforced in FUTURE_ONLY_DIRS too (a namespace escape there would hide a future).
FUTURE_SCOPE_KINDS = {"future", "stdns"}
COMPILED = [(k, re.compile(p)) for k, p in PATTERNS]
ATOMIC_KINDS = {"atomic", "include"}  # what a RING entry may contain

RING_FILES = {
    # The ring implementations' own head/tail/length/heartbeat/closed memory
    # ordering — atomics-family constructs only (a mutex there still fails).
    "include/rune/client/detail/M2ORingBuffer.h",
    "include/rune/client/detail/OneToOneRingBuffer.h",
}

# path -> {(kind, normalized snippet): count}.  Regenerate a file's entry with
# `--dump PATH` after a deliberate slice-B change (the reviewer sees the diff).
SLICE_B = {
    # F8c C2/C3 removed every Client / ClientAgent entry and F8c C5 the last
    # two (TransactionalProducerImpl's atomics, mutexes and lock_guards: its
    # state is duty-owned, commands arrive on the agent command ring).  The
    # allowlist stays as a mechanism (empty: any new site fails).
}

# thread_local is BANNED outright in src/** and include/** (CLAUDE.md hard
# rule 1: "no thread_local"; catalog history Task 5 fix round 0 removed the
# former per-thread allowance and its two Client.cpp sites).  There is no
# per-thread allowlist: any thread_local is a violation.

CHAR_PREFIXES = {"L", "u", "U", "u8"}


def strip_comments_and_strings(text: str) -> str:
    """Blank out //, /* */ comments and string/char literals (newlines kept).

    Char literals may carry an encoding prefix (L'x', u'x', U'x', u8'x'); a
    quote preceded by any OTHER identifier/number token is a digit separator
    (1'000'000, 0xFF'FF) and left alone."""
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
        elif c == "R" and text.startswith('R"', i) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")
                                                     or text[max(0, i - 2):i] in ("u8",) or text[i - 1] in "LuU"):
            # raw string literal [prefix]R"delim( ... )delim"
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
            if c == "'":
                # The identifier/number token immediately before the quote.
                k = i
                while k > 0 and (text[k - 1].isalnum() or text[k - 1] == "_"):
                    k -= 1
                token = text[k:i]
                if token and token not in CHAR_PREFIXES:
                    out.append(c)  # digit separator
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


def normalize(snip: str) -> str:
    s = re.sub(r"\s*::\s*", "::", snip.strip())
    return re.sub(r"\s+", " ", s)


def find_matches(raw: str):
    """[(line, kind, normalized snippet)] for every banned construct, de-duplicated."""
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
        out.append((line, kind, normalize(raw[s:e])))
    return out


def multiset(matches):
    ms = {}
    for _line, kind, snip in matches:
        ms[(kind, snip)] = ms.get((kind, snip), 0) + 1
    return ms


def evaluate(files: dict, ring_files: set, slice_b: dict):
    """files: {relpath: text}.  Returns a list of violations."""
    violations = []
    for rel in sorted(files):
        matches = find_matches(files[rel])
        if rel in ring_files:
            for line, kind, snip in matches:
                if kind not in ATOMIC_KINDS:
                    violations.append(f"{rel}:{line}: banned {kind} construct `{snip}` "
                                      f"(ring impl may only use memory-ordering atomics)")
            continue
        allowed = slice_b.get(rel)
        if allowed is None:
            for line, kind, snip in matches:
                violations.append(f"{rel}:{line}: banned {kind} construct `{snip}` "
                                  f"(shared-nothing law: own the state on one thread, "
                                  f"talk through a ring)")
            continue
        got = multiset(matches)
        for key in sorted(set(got) | set(allowed)):
            g, a = got.get(key, 0), allowed.get(key, 0)
            if g > a:
                lines = ", ".join(str(l) for l, k, sn in matches if (k, sn) == key)
                violations.append(f"{rel}: NEW SLICE-B-file site {key} x{g - a} "
                                  f"(lines {lines}) — not allowlisted")
            elif g < a:
                violations.append(f"{rel}: allowlisted site {key} x{a - g} is GONE — "
                                  f"remove it from SLICE_B (the allowlist must match exactly)")
    for rel in sorted(set(slice_b) | ring_files):
        if rel not in files:
            violations.append(f"{rel}: allowlisted but not found (stale allowlist entry)")
    return violations


def evaluate_futures(files: dict):
    """Only the FUTURE kind, no allowlist (FUTURE_ONLY_DIRS and --futures)."""
    violations = []
    for rel in sorted(files):
        for line, kind, snip in find_matches(files[rel]):
            if kind in FUTURE_SCOPE_KINDS:
                violations.append(f"{rel}:{line}: banned future construct `{snip}` (F8c: issue the op "
                                  f"with a Token into Completions / a CompletionHandler, or *AndWait)")
    return violations


# ---- self-test ---------------------------------------------------------------
SELFTEST_DETECT = [
    # (name, snippet, expected number of matches)
    ("std_atomic_member", "struct S { std::atomic<int> x{0}; };", 1),
    ("std_atomic_spaced", "std :: atomic<bool> b;", 1),
    ("atomic_flag", "std::atomic_flag f;", 1),
    ("fence", "std::atomic_thread_fence(std::memory_order_release);", 1),
    ("bare_atomic_using_ns", "using namespace std; atomic<int> a;", 2),  # + the using-directive (C9 fix1)
    ("free_atomic_fn", "atomic_store(&a, 1); std::atomic_load(&a);", 2),
    ("builtin", "__atomic_load_n(&x, __ATOMIC_ACQUIRE);", 1),
    ("sync_builtin", "__sync_fetch_and_add(&x, 1);", 1),
    ("c11_builtin", "__c11_atomic_load(&x, 2);", 1),
    ("c_Atomic", "_Atomic int x;", 1),
    ("stdatomic_h", "#include <stdatomic.h>", 1),
    ("aeron_atomic_ns", "aeron::concurrent::atomic::getAndAddInt64(p, 1);", 2),
    ("aeron_ordered", "buf.putInt64Ordered(i, v); buf.putIntOrdered(i, v);", 2),
    ("aeron_volatile", "buf.getInt64Volatile(i); buf.getIntVolatile(i);", 2),
    ("aeron_cas_rmw", "buf.compareAndSetInt64(i, a, b); buf.getAndAddInt64(i, 1); buf.addInt64Ordered(i, 1);", 3),
    ("mutex", "std::mutex m;", 1),
    ("shared_mutex", "mutable std::shared_mutex m;", 1),
    ("recursive_mutex_bare", "using namespace std; recursive_mutex m;", 2),
    ("pthread_mutex", "pthread_mutex_t m; pthread_mutex_lock(&m);", 2),
    ("pthread_spin_rwlock", "pthread_spin_lock(&s); pthread_rwlock_rdlock(&r);", 2),
    ("pthread_cond", "pthread_cond_wait(&c, &m);", 1),
    ("os_unfair_lock", "os_unfair_lock l = OS_UNFAIR_LOCK_INIT; os_unfair_lock_lock(&l);", 2),
    ("lock_guard", "std::lock_guard<std::mutex> lk(m);", 2),
    ("unique_lock", "std::unique_lock lk(m_);", 1),
    ("scoped_lock", "std::scoped_lock lk(a, b);", 1),
    ("condvar", "std::condition_variable_any cv;", 1),
    ("semaphore", "std::counting_semaphore<4> s{0};", 1),
    ("latch", "std::latch l{1};", 1),
    ("call_once", "std::once_flag f; std::call_once(f, []{});", 2),
    ("jthread", "std::jthread t([](std::stop_token st){});", 2),
    ("stop_source", "std::stop_source src;", 1),
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
    ("digit_separator", "int64_t n = 1'000'000; std::mutex m;", 1),
    ("hex_digit_separator", "auto h = 0xFF'FF; std::mutex m;", 1),
    ("include_other", "#include <thread>\n#include <functional>", 0),  # <future> is banned since F8c C9
    ("ring_word_names", "storeRelease64_(idx, v); loadAcquire32_(idx);", 0),
    ("pthread_affinity_ok", "#include <pthread.h>\npthread_setaffinity_np(pthread_self(), n, &s);", 0),
    # char-literal prefixes: the quote inside must not open a fake string that
    # swallows (hides) the real construct after it, nor leak one.
    ("wide_char_quote", "wchar_t q = L'\"'; std::mutex m;", 1),
    ("u8_char_quote", "auto q = u8'\"'; std::mutex m;", 1),
    ("u16_char_quote", "char16_t q = u'\"'; std::mutex m;", 1),
    ("u32_char_quote", "char32_t q = U'\"'; std::mutex m;", 1),
    ("prefixed_char_hides_nothing", "auto q = L'x'; /* std::mutex */", 0),
    # F8c C9: futures / promises / packaged tasks / std::async are banned (the
    # completion-token API replaced every one of them; spec §5 C9).
    ("include_future", "#include <future>", 1),
    ("include_future_spaced", "#  include < future >", 1),
    ("std_future", "std::future<int> f;", 1),
    ("std_future_spaced", "std :: future<void> f;", 1),
    ("std_promise", "std::promise<void> p;", 1),
    ("std_shared_future", "std::shared_future<int> s;", 1),
    ("std_packaged_task", "std::packaged_task<int()> t;", 1),
    ("std_async_launch", "auto f = std::async(std::launch::async, fn);", 2),
    ("std_async_plain", "auto f = std::async(fn);", 1),
    ("future_status_error", "f.wait_for(0s) == std::future_status::ready; catch (std::future_error&) {}", 2),
    ("using_decl_future", "using std::future; using std::promise;", 2),
    ("experimental_future", "std::experimental::future<int> f;", 1),
    ("bare_future_promise", "using namespace std; future<int> f; promise<int> p;", 3),
    ("bare_shared_future_task", "using namespace std; shared_future<int> s; packaged_task<void()> t;", 3),
    # false positives: prose, strings, look-alike identifiers
    ("future_comment", "// std::future / std::promise / std::async are banned", 0),
    ("future_string", 'const char* s = "std::promise<int> #include <future>";', 0),
    ("future_identifiers", "int futureCount; bool promised; void asyncSend(); bool async = true; obj.future = 1;", 0),
    ("future_compare", "if (future < 3) {} launch(x);", 0),
    # C9 fix round 1 (review finding 1): namespace escapes and spelling variants.
    ("using_namespace_std_banned", "using namespace std;", 1),
    ("using_namespace_std_spaced", "using  namespace   std ;", 1),
    ("using_namespace_global_std", "using namespace ::std;", 1),
    ("chrono_literals_ok", "using namespace std::chrono_literals; using namespace std::literals;", 0),
    ("alias_std_banned", "namespace s = std;", 1),
    ("alias_std_experimental_banned", "namespace ex = std::experimental;", 1),
    ("alias_std_chrono_ok", "namespace chr = std::chrono;", 0),
    ("alias_std_future",
     "namespace s = std; s::promise<int> p; s::future<int> f; s::async(fn); s::shared_future<int> x;", 5),
    ("alias_std_atomic_mutex", "namespace s = std; s::atomic<int> a; s::mutex m;", 3),
    ("bare_future_space_lt", "using namespace std; promise <int> p; future < int > f;", 3),
    ("bare_async_call", "using namespace std; auto f = async(launch::async, fn);", 3),
    ("bare_async_member_ok", "obj.async(1); p->async(2); runAsync(3); bool async = true;", 0),
    ("experimental_include", "#include <experimental/future>", 1),
    ("boost_future", "boost::future<int> f; boost::promise<int> p; boost::async(fn);", 3),
]

SELFTEST_ALLOW = [
    # (name, files, ring_files, slice_b, expected violations)
    ("unlisted_file_fails", {"src/a.cpp": "std::mutex m;"}, set(), {}, 1),
    ("ring_allows_atomics",
     {"include/r.h": "#include <atomic>\nstd::atomic_thread_fence(std::memory_order_acquire);"},
     {"include/r.h"}, {}, 0),
    ("ring_allows_aeron_style_cas",
     {"include/r.h": "compareAndSet64_(i, a, b);"}, {"include/r.h"}, {}, 0),
    ("ring_rejects_mutex", {"include/r.h": "std::mutex m;"}, {"include/r.h"}, {}, 1),
    ("sliceb_exact_ok",
     {"src/c.cpp": "std::mutex m; std::atomic<int> a;"}, set(),
     {"src/c.cpp": {("mutex", "std::mutex"): 1, ("atomic", "std::atomic"): 1}}, 0),
    ("sliceb_new_site_fails",
     {"src/c.cpp": "std::mutex m; std::atomic<int> a; std::atomic<int> b;"}, set(),
     {"src/c.cpp": {("mutex", "std::mutex"): 1, ("atomic", "std::atomic"): 1}}, 1),
    ("sliceb_swapped_kind_fails",
     {"src/c.cpp": "std::mutex m; std::lock_guard<std::mutex> g(m);"}, set(),
     {"src/c.cpp": {("mutex", "std::mutex"): 1, ("atomic", "std::atomic"): 1}}, 3),
    ("sliceb_removed_site_fails",
     {"src/c.cpp": "std::mutex m;"}, set(),
     {"src/c.cpp": {("mutex", "std::mutex"): 1, ("atomic", "std::atomic"): 1}}, 1),
    ("stale_allowlist_fails", {}, set(), {"src/gone.cpp": {("mutex", "std::mutex"): 1}}, 1),
    # F8c C9: a ring file may hold memory-ordering atomics, never a future.
    ("ring_rejects_future",
     {"include/r.h": "#include <future>\nstd::promise<int> p;"}, {"include/r.h"}, {}, 2),
    ("unlisted_future_fails", {"src/a.cpp": "auto f = std::async(fn);"}, set(), {}, 1),
]

SELFTEST_FUTURES = [
    # (name, files, expected violations) for evaluate_futures (FUTURE_ONLY_DIRS)
    ("example_future_fails", {"examples/e.cpp": "#include <future>\nauto f = c.connect(); f.get();\n"
                                                "std::future<void> g;"}, 2),
    ("bench_other_kinds_ok", {"bench/b.cpp": "std::atomic<int> n{0}; std::mutex m;"}, 0),
    ("bench_promise_fails", {"bench/b.cpp": "std::atomic<int> n{0}; std::promise<int> p;"}, 1),
    ("example_using_namespace_std_fails", {"examples/e.cpp": "using namespace std;"}, 1),
    ("example_alias_std_fails", {"tests/e2e/p.cpp": "namespace s = std; s::future<int> f;"}, 2),
    ("prose_ok", {"tools/t.cpp": "// no std::future here\nconst char* s = \"std::async\";"}, 0),
]


def run_selftest():
    failures = []
    for name, snippet, want in SELFTEST_DETECT:
        got = len(find_matches(snippet))
        if got != want:
            failures.append(f"detect '{name}': expected {want} match(es), got {got}: "
                            f"{find_matches(snippet)}")
    for name, files, ring, sliceb, want_v in SELFTEST_ALLOW:
        v = evaluate(files, ring, sliceb)
        if len(v) != want_v:
            failures.append(f"allowlist '{name}': expected {want_v} violation(s), got {len(v)}: {v}")
    for name, files, want_v in SELFTEST_FUTURES:
        v = evaluate_futures(files)
        if len(v) != want_v:
            failures.append(f"futures '{name}': expected {want_v} violation(s), got {len(v)}: {v}")
    return failures


def collect(root: Path, dirs=SCAN_DIRS):
    files = {}
    for d in dirs:
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
              f"({len(SELFTEST_DETECT)} detector + {len(SELFTEST_ALLOW)} allowlist + "
              f"{len(SELFTEST_FUTURES)} futures-scope fixtures)")
        return 0
    args = [a for a in argv if not a.startswith("--")]
    root = Path(args[0]) if args else Path(__file__).resolve().parent.parent
    files = collect(root)
    # T15b: --expect-covered=a,b,...  every listed path must be among the scanned
    # files (proves a subsystem — e.g. src/shard/** — is under the guard).
    for a in argv:
        if a.startswith("--expect-covered="):
            missing = [rel for rel in a.split("=", 1)[1].split(",") if rel and rel not in files]
            if missing:
                print("check_no_atomics_or_mutexes: NOT SCANNED: " + ", ".join(missing))
                return 1
    if "--dump" in argv:
        for rel in args[1:]:
            print(rel, dict(sorted(multiset(find_matches(files[rel])).items())))
        return 0
    if "--futures" in argv:
        # NoFuturesOrPromisesGuardRegression: the FUTURE kind only, everywhere
        # it is enforced (src/ include/ examples/ bench/ tools/), no allowlist.
        all_files = {**files, **collect(root, FUTURE_ONLY_DIRS)}
        violations = evaluate_futures(all_files)
        if violations:
            print(f"check_no_atomics_or_mutexes --futures: {len(violations)} violation(s)")
            for v in violations:
                print("  " + v)
            return 1
        print(f"check_no_atomics_or_mutexes --futures: OK ({len(all_files)} files, no future/promise/"
              f"packaged_task/async)")
        return 0
    violations = evaluate(files, RING_FILES, SLICE_B)
    violations += evaluate_futures(collect(root, FUTURE_ONLY_DIRS))
    if violations:
        print(f"check_no_atomics_or_mutexes: {len(violations)} violation(s)")
        for v in violations:
            print("  " + v)
        return 1
    print(f"check_no_atomics_or_mutexes: OK ({len(files)} files; "
          f"{len(SLICE_B)} SLICE-B files pending)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
