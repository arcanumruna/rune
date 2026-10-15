#!/usr/bin/env python3
"""Self-test of scripts/check_no_rune_topics.py: every way the gate could silently pass.

Each case builds a scratch git repo holding copies of the gate and an allowlist, then runs the
gate against it.  Exits 1 on the first mismatch, naming the case.
"""
import pathlib, shutil, subprocess, sys, tempfile

HERE = pathlib.Path(__file__).resolve().parent
GATE = HERE / "check_no_rune_topics.py"
ALLOW_HEADER = "# test allowlist\n"
M = "RUNE_" + "TOPICS"  # never spell the literal in this file's own source


def git(repo, *args):
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=x", "-c", "user.email=x@x", *args],
                   check=True, capture_output=True)


def run_case(name, expected, tracked=None, untracked=None, allow="", gitignore=None):
    repo = pathlib.Path(tempfile.mkdtemp(prefix="gate-selftest-"))
    try:
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        (repo / "scripts").mkdir()
        shutil.copy(GATE, repo / "scripts" / "check_no_rune_topics.py")
        (repo / "scripts" / "rune_topics_allowlist.txt").write_text(ALLOW_HEADER + allow)
        if gitignore is not None:
            (repo / ".gitignore").write_text(gitignore)
        for rel, data in (tracked or {}).items():
            (repo / rel).write_bytes(data)
        git(repo, "add", "-A")
        git(repo, "commit", "-qm", "x")
        for rel, data in (untracked or {}).items():
            p = repo / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        r = subprocess.run([sys.executable, str(repo / "scripts" / "check_no_rune_topics.py"), str(repo)],
                           capture_output=True, text=True)
        if r.returncode != expected:
            print(f"FAIL case {name}: exit {r.returncode}, expected {expected}\n{r.stdout}{r.stderr}")
            sys.exit(1)
        print(f"ok case {name}")
    finally:
        shutil.rmtree(repo, ignore_errors=True)


hit = f"{M}=x\n".encode()
run_case("a clean repo", 0, tracked={"ok.sh": b"echo hi\n"})
run_case("b tracked path with a space", 1, tracked={"a b.sh": hit})
run_case("c tracked non-ascii path", 1, tracked={"é.sh": hit})
run_case("d untracked file", 1, tracked={"ok.sh": b"x\n"}, untracked={"new.sh": hit})
run_case("e ignored directory", 0, tracked={"ok.sh": b"x\n"}, gitignore="/build-*/\n",
         untracked={"build-x/f.txt": hit})
run_case("f allowlist exact: fewer mentions than the row", 1, tracked={"x.cpp": hit}, allow="x.cpp 2\n")
run_case("g allowlist exact: more mentions than the row", 1, tracked={"x.cpp": hit * 3}, allow="x.cpp 2\n")
run_case("h tracked binary", 1, tracked={"bin.dat": b"\0" + M.encode()})
run_case("i allowlist row for an absent path", 0, tracked={"ok.sh": b"x\n"}, allow="absent.md 2\n")
run_case("j allowlist exact: matching count passes", 0, tracked={"x.cpp": hit * 2}, allow="x.cpp 2\n")
print("ok: gate self-test")
