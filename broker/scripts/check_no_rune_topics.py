#!/usr/bin/env python3
"""Grep gate (catalog history spec §8): no RUNE_TOPICS mention may remain.

Scans every tracked and untracked-not-ignored file except historical design records (docs/superpowers/**,
which describe what existed when they were written) and the gate's own two files.
The only other mentions allowed are listed — path and EXACT count — in
scripts/rune_topics_allowlist.txt (F5): the config loader's presence refusal, its
Regression test, and the release note naming the removal.  A tracked binary that
contains the string (e.g. committed build output) is a hit too.  Exit 1 listing
every hit; exit 0 when clean.
"""
import subprocess, sys, pathlib

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
ls = subprocess.run(["git", "-C", str(root), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
                    capture_output=True, check=True).stdout
files = sorted({p.decode("utf-8", "surrogateescape") for p in ls.split(b"\0") if p})
allow = {}
for raw in (root / "scripts" / "rune_topics_allowlist.txt").read_text().splitlines():
    entry = raw.split("#", 1)[0].split()
    if entry:
        allow[entry[0]] = int(entry[1])
own = {"scripts/check_no_rune_topics.py", "scripts/rune_topics_allowlist.txt"}
hits = []
for f in files:
    if f.startswith("docs/superpowers/") or f in own:
        continue
    try:
        data = (root / f).read_bytes()
    except (IsADirectoryError, FileNotFoundError):
        continue
    if b"\0" in data[:8192]:
        if b"RUNE_TOPICS" in data:
            hits.append(f"{f}: tracked binary contains RUNE_TOPICS (build output must not be tracked)")
        continue
    lines = [(n, l) for n, l in enumerate(data.decode(errors="ignore").splitlines(), 1) if "RUNE_TOPICS" in l]
    mentions = sum(l.count("RUNE_TOPICS") for _, l in lines)
    if f in allow:
        if mentions != allow[f]:
            hits += [f"{f}:{n}: {l.strip()}" for n, l in lines]
            hits.append(f"{f}: {mentions} mentions, allowlist says exactly {allow[f]} (update scripts/rune_topics_allowlist.txt)")
        continue
    if mentions == 0:
        continue
    hits += [f"{f}:{n}: {l.strip()}" for n, l in lines]
if hits:
    print("RUNE_TOPICS must not appear (topics are created by DDL):")
    print("\n".join(hits))
    sys.exit(1)
print("ok: no RUNE_TOPICS mention outside scripts/rune_topics_allowlist.txt")
