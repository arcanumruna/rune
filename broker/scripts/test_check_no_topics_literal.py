#!/usr/bin/env python3
"""Self-test of scripts/check_no_topics_literal.py: every way the gate could silently pass (#884).

Each case builds a scratch git repo holding a copy of the gate, then runs the gate against it.
Exits 1 on the first mismatch, naming the case.
"""
import pathlib, shutil, subprocess, sys, tempfile

HERE = pathlib.Path(__file__).resolve().parent
GATE = HERE / "check_no_topics_literal.py"
LIT = '"top' + 'ics"'  # never spell the literal in this file's own source


def git(repo, *args):
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=x", "-c", "user.email=x@x", *args],
                   check=True, capture_output=True)


def run_case(name, expected, tracked=None, untracked=None, arg=None):
    repo = pathlib.Path(tempfile.mkdtemp(prefix="topics-gate-selftest-"))
    try:
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        (repo / "scripts").mkdir()
        shutil.copy(GATE, repo / "scripts" / "check_no_topics_literal.py")
        for rel, data in (tracked or {"README": "x\n"}).items():
            p = repo / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(data)
        git(repo, "add", "-A")
        git(repo, "commit", "-qm", "x")
        for rel, data in (untracked or {}).items():
            p = repo / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(data)
        target = repo if arg is None else repo / arg
        r = subprocess.run([sys.executable, str(repo / "scripts" / "check_no_topics_literal.py"), str(target)],
                           capture_output=True, text=True)
        if r.returncode != expected:
            print(f"FAIL case {name}: exit {r.returncode}, expected {expected}\n{r.stdout}{r.stderr}")
            sys.exit(1)
        print(f"ok case {name}")
    finally:
        shutil.rmtree(repo, ignore_errors=True)


hit = f"auto p = root / {LIT};\n"
clean = "int x = 0;\n"
run_case("a clean libs tree", 0, tracked={"libs/a/x.cpp": clean})
run_case("b literal in a source", 1, tracked={"libs/a/x.cpp": hit})
run_case("c literal in a test", 1, tracked={"libs/a/tests/t/xTest.cpp": hit})
run_case("d NamespacePaths is the one owner", 0, tracked={"libs/node/src/NamespacePaths.cpp": hit})
run_case("e untracked source", 1, tracked={"libs/a/x.cpp": clean}, untracked={"libs/a/y.cpp": hit})
run_case("f root without libs/ checks nothing and must fail", 2, tracked={"README": "x\n"})
run_case("g libs/ present but holds no C++ source", 2, tracked={"libs/README": "x\n"})
run_case("h literal outside libs/ is not this gate's business", 0,
         tracked={"libs/a/x.cpp": clean, "tools/t.cpp": hit})
print("ok: topics-literal gate self-test")
