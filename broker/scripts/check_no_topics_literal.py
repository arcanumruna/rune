#!/usr/bin/env python3
"""Source guard (namespaces plan Task 6, D14): no broker source spells the "topics" directory.

Every disk path to a topic is built by rune/node/NamespacePaths.h (<dataDir>/ns-<nsId>/topics/...), so a
"topics" string literal in any C++ source under libs/ (tests included) is a path that bypasses the namespace
root.  Scans every tracked and untracked-not-ignored libs/**.cpp except NamespacePaths itself.  Exit 1 listing
every hit; exit 2 when the root holds no libs/**.cpp (a wrong root would otherwise pass by checking nothing,
#884); exit 0 when clean.  Usage: check_no_topics_literal.py <broker dir>
"""
import subprocess, sys, pathlib

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
ls = subprocess.run(["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                    capture_output=True, check=True).stdout
files = sorted({p.decode("utf-8", "surrogateescape") for p in ls.split(b"\0") if p})
needle = '"topics"'
sources = [f for f in files if f.startswith("libs/") and f.endswith(".cpp")]
if not sources:
    print(f"error: {root} holds no libs/**.cpp; pass the broker directory as the root", file=sys.stderr)
    sys.exit(2)
hits = []
for f in sources:
    if "NamespacePaths" in f:
        continue
    try:
        text = (root / f).read_text(errors="ignore")
    except (IsADirectoryError, FileNotFoundError):
        continue
    hits += [f"{f}:{n}: {l.strip()}" for n, l in enumerate(text.splitlines(), 1) if needle in l]
if hits:
    print('"topics" must not appear in libs/ sources (build topic paths with rune/node/NamespacePaths.h):')
    print("\n".join(hits))
    sys.exit(1)
print('ok: no "topics" literal in libs/ sources outside NamespacePaths')
