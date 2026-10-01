#!/usr/bin/env python3
"""check_no_shared_ptr_handles.py — F8c C7 guard (rune-client-cpp).

Spec F8c §3.1 / §5 C7: the client's handles (Producer, TransactionalProducer,
Subscription, GroupConsumer, BatchBuilder, TypedTopic) are MOVE-ONLY and own
their impls through the §3.3 release protocol — no std::shared_ptr, no
refcount, no "last owner frees" decided by an atomic RMW race.  The hubs hold
owning raw pointers (the duty side's share) and hand them over by ring order.

This guard scans src/** and include/** (comments and string literals blanked,
line numbers preserved — the same stripper as check_no_atomics_or_mutexes.py)
for every shared-ownership construct:

  std::shared_ptr, std::weak_ptr, std::make_shared, std::allocate_shared,
  std::enable_shared_from_this (qualified or not).

ALLOWLIST (path relative to the repo root) — an EXACT multiset of
(normalized snippet) -> count per file: a new site fails, and so does a removed
one (which must then be removed from the allowlist):

  * TRANSPORT — the Aeron C++ client's own object model (Aeron, Publication,
    Subscription are shared_ptrs created by the library; spec §1.3 S5): they
    are created on the constructing thread before the runner starts and
    released after the join — never shared across live threads by us.

The script self-tests its detector and the allowlist logic against synthetic
fixtures before scanning (--selftest runs only that).
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_no_atomics_or_mutexes as base  # noqa: E402  (shared stripper / collector)

PATTERN = re.compile(
    r"(?<![\w])(?:std\s*::\s*)?(?:shared_ptr|weak_ptr|make_shared|allocate_shared|enable_shared_from_this)\b")

TRANSPORT = {
    "include/rune/client/transport/AeronTransport.h": {"std::shared_ptr": 3},
    "include/rune/client/transport/Resolution.h": {"std::shared_ptr": 3},
    "src/transport/AeronTransport.cpp": {"std::shared_ptr": 2},
}


def find_matches(raw: str):
    clean = base.strip_comments_and_strings(raw)
    out = []
    for m in PATTERN.finditer(clean):
        line = clean.count("\n", 0, m.start()) + 1
        out.append((line, base.normalize(raw[m.start():m.end()])))
    return out


def evaluate(files: dict, allow: dict):
    violations = []
    for rel in sorted(files):
        matches = find_matches(files[rel])
        allowed = allow.get(rel)
        if allowed is None:
            for line, snip in matches:
                violations.append(f"{rel}:{line}: `{snip}` — shared ownership is banned (F8c C7: "
                                  f"move-only handles, owning raw pointers handed over by ring order)")
            continue
        got = {}
        for _line, snip in matches:
            got[snip] = got.get(snip, 0) + 1
        for key in sorted(set(got) | set(allowed)):
            g, a = got.get(key, 0), allowed.get(key, 0)
            if g > a:
                lines = ", ".join(str(l) for l, sn in matches if sn == key)
                violations.append(f"{rel}: NEW `{key}` x{g - a} (lines {lines}) — not allowlisted")
            elif g < a:
                violations.append(f"{rel}: allowlisted `{key}` x{a - g} is GONE — remove it from TRANSPORT")
    for rel in sorted(allow):
        if rel not in files:
            violations.append(f"{rel}: allowlisted but not found (stale allowlist entry)")
    return violations


SELFTEST_DETECT = [
    ("member", "std::shared_ptr<Impl> impl_;", 1),
    ("spaced", "std :: shared_ptr<Impl> p;", 1),
    ("make_shared", "auto p = std::make_shared<Impl>();", 1),
    ("weak", "std::weak_ptr<Impl> w;", 1),
    ("allocate_shared", "auto p = std::allocate_shared<T>(a);", 1),
    ("esft", "struct X : std::enable_shared_from_this<X> {};", 1),
    ("unqualified", "using namespace std; shared_ptr<int> p; make_shared<int>();", 2),
    ("two_in_one", "std::shared_ptr<A> a = std::make_shared<A>();", 2),
    # false positives
    ("comment", "// std::shared_ptr is banned", 0),
    ("block_comment", "/* std::make_shared<T>() */ int x;", 0),
    ("string", 'const char* s = "std::shared_ptr";', 0),
    ("unique", "std::unique_ptr<Impl> p = std::make_unique<Impl>();", 0),
    ("identifiers", "int shared_ptr_count; void my_make_shared(); bool weak_ptrs;", 0),
]

SELFTEST_ALLOW = [
    ("unlisted_fails", {"src/a.cpp": "std::shared_ptr<int> p;"}, {}, 1),
    ("exact_ok", {"src/t.cpp": "std::shared_ptr<A> a; std::shared_ptr<B> b;"},
     {"src/t.cpp": {"std::shared_ptr": 2}}, 0),
    ("new_site_fails", {"src/t.cpp": "std::shared_ptr<A> a; std::shared_ptr<B> b; std::shared_ptr<C> c;"},
     {"src/t.cpp": {"std::shared_ptr": 2}}, 1),
    ("removed_site_fails", {"src/t.cpp": "std::shared_ptr<A> a;"}, {"src/t.cpp": {"std::shared_ptr": 2}}, 1),
    ("other_kind_fails", {"src/t.cpp": "std::shared_ptr<A> a; std::shared_ptr<B> b; auto c = std::make_shared<C>();"},
     {"src/t.cpp": {"std::shared_ptr": 2}}, 1),
    ("stale_fails", {}, {"src/gone.cpp": {"std::shared_ptr": 1}}, 1),
]


def run_selftest():
    failures = []
    for name, snippet, want in SELFTEST_DETECT:
        got = len(find_matches(snippet))
        if got != want:
            failures.append(f"detect '{name}': expected {want}, got {got}: {find_matches(snippet)}")
    for name, files, allow, want in SELFTEST_ALLOW:
        v = evaluate(files, allow)
        if len(v) != want:
            failures.append(f"allowlist '{name}': expected {want} violation(s), got {len(v)}: {v}")
    return failures


def main(argv):
    failures = run_selftest()
    if failures:
        print("check_no_shared_ptr_handles: SELF-TEST FAILED")
        for f in failures:
            print("  " + f)
        return 2
    if "--selftest" in argv:
        print(f"check_no_shared_ptr_handles: self-test OK "
              f"({len(SELFTEST_DETECT)} detector + {len(SELFTEST_ALLOW)} allowlist fixtures)")
        return 0
    args = [a for a in argv if not a.startswith("--")]
    root = Path(args[0]) if args else Path(__file__).resolve().parent.parent
    files = base.collect(root)
    violations = evaluate(files, TRANSPORT)
    if violations:
        print(f"check_no_shared_ptr_handles: {len(violations)} violation(s)")
        for v in violations:
            print("  " + v)
        return 1
    print(f"check_no_shared_ptr_handles: OK ({len(files)} files; transport allowlist "
          f"{sum(sum(m.values()) for m in TRANSPORT.values())} sites)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
