#!/usr/bin/env python3
"""check_pr_workflow.py -- #706 guard: the pull request workflow keeps running the client suite.

Before .github/workflows/pr.yml no check ran on a pull request, so a PR merged with none.  This guard is a text parse
(no YAML dependency) of that file and fails when it is removed or weakened:

  (a) `pull_request:` is a trigger under the top-level `on:`;
  (b) the client is configured (cmake -S client-cpp) with -DBUILD_TESTING=ON and -DBUILD_EXAMPLES=ON, so every example
      compiles and the client tests exist;
  (c) a ctest run on the client build (--test-dir ... client ...) selects `-R Regression` and `-LE latency`;
  (d) nothing selects `-L latency`: latency gates need an idle host and never run in CI.

Comments are ignored; shell line continuations (a trailing backslash) are joined first.

usage: check_pr_workflow.py [path-to-pr.yml]     (default: ../.github/workflows/pr.yml relative to this script's tree)
       check_pr_workflow.py --selftest           (a good fixture plus one mutant per rule)
exit 0 = ok, 1 = a rule is broken (each is listed), 2 = usage / unreadable file.
"""
import pathlib
import re
import sys


def logical_lines(text):
    """Non-comment lines with trailing-backslash continuations joined."""
    out, cur = [], ""
    for raw in text.splitlines():
        line = raw.rstrip()
        if not cur and line.lstrip().startswith("#"):
            continue
        if line.endswith("\\"):
            cur += line[:-1] + " "
            continue
        out.append(cur + line)
        cur = ""
    if cur:
        out.append(cur)
    return out


def trigger_block(lines):
    """The indented lines under the top-level `on:` key."""
    block, inside = [], False
    for line in lines:
        if re.match(r"^on\s*:", line):
            inside = True
            continue
        if inside:
            if line and not line[0].isspace():
                break
            block.append(line)
    return block


def check(text):
    """Return the list of broken rules (empty = ok)."""
    lines = logical_lines(text)
    errors = []

    if not any(re.match(r"^\s+pull_request\s*:", l) for l in trigger_block(lines)):
        errors.append("(a) `pull_request:` is not a trigger under `on:`")

    cfg = [l for l in lines if re.search(r"\bcmake\b.*-S\s+client-cpp\b", l)]
    if not cfg:
        errors.append("(b) no `cmake -S client-cpp` configure step")
    for flag in ("-DBUILD_TESTING=ON", "-DBUILD_EXAMPLES=ON"):
        if cfg and not any(flag in l for l in cfg):
            errors.append(f"(b) the client configure step lacks {flag}")

    runs = [l for l in lines if re.search(r"\bctest\b", l) and re.search(r"--test-dir\s+\S*client", l)]
    if not runs:
        errors.append("(c) no `ctest --test-dir <client build>` step")
    elif not any(re.search(r"(^|\s)-R\s+[\"']?Regression", l) and re.search(r"(^|\s)-LE\s+[\"']?latency", l)
                 for l in runs):
        errors.append("(c) the client ctest step lacks `-R Regression` together with `-LE latency`")

    for l in lines:
        if re.search(r"(^|\s)-L\s+[\"']?latency", l):
            errors.append("(d) a step selects `-L latency` (latency gates never run in CI): " + l.strip())
    return errors


GOOD = """\
name: pr
on:
  pull_request:
    branches: [main]
  push:
    branches: [main]
jobs:
  client:
    steps:
      - name: Configure
        run: |
          cmake -S client-cpp -B build-client/cmake -G Ninja \\
            -DBUILD_TESTING=ON -DBUILD_EXAMPLES=ON
      # ctest --test-dir x -L latency  (a comment is ignored)
      - name: Regression suite (client)
        run: |
          ctest --test-dir build-client/cmake -R Regression -LE latency -j4 \\
            --output-on-failure
"""


def selftest():
    failures = []
    if check(GOOD):
        failures.append("the good fixture was refused: " + "; ".join(check(GOOD)))
    mutants = {
        "a": GOOD.replace("  pull_request:\n    branches: [main]\n", ""),
        "b": GOOD.replace("-DBUILD_EXAMPLES=ON", ""),
        "c": GOOD.replace("-R Regression", "-R Smoke"),
        "d": GOOD.replace("-LE latency", "-L latency"),
        "b2": GOOD.replace("-DBUILD_TESTING=ON", ""),
        "c2": GOOD.replace("-LE latency", ""),
        "d2": GOOD.replace("-j4 \\\n", "-j4 -L latency \\\n"),
        "a2": GOOD.replace("pull_request:", "push_request:"),
        "c3": GOOD.replace("--test-dir build-client/cmake", "--test-dir build-other/cmake"),
    }
    for rule, text in mutants.items():
        errs = check(text)
        if not any(e.startswith("(" + rule[0] + ")") for e in errs):
            failures.append(f"mutant {rule} was not caught (got: {errs})")
    # a pull_request key outside `on:` does not count
    outside = GOOD.replace("on:\n  pull_request:\n    branches: [main]\n", "on:\n") + "x:\n  pull_request:\n"
    if not any(e.startswith("(a)") for e in check(outside)):
        failures.append("a pull_request key outside `on:` was accepted")
    if failures:
        print("check_pr_workflow selftest FAILED:\n  " + "\n  ".join(failures))
        return 1
    print(f"check_pr_workflow selftest: OK (good fixture + {len(mutants) + 1} mutants)")
    return 0


def main(argv):
    if len(argv) == 2 and argv[1] == "--selftest":
        return selftest()
    if len(argv) > 2:
        print(__doc__)
        return 2
    path = pathlib.Path(argv[1]) if len(argv) == 2 else \
        pathlib.Path(__file__).resolve().parent.parent.parent / ".github" / "workflows" / "pr.yml"
    try:
        text = path.read_text()
    except OSError as e:
        print(f"check_pr_workflow: FAIL -- cannot read {path.name}: {e} (the PR workflow was removed?)")
        return 1
    errors = check(text)
    if errors:
        print("check_pr_workflow: FAIL -- pr.yml no longer runs the client suite on pull requests:\n  " + "\n  ".join(errors))
        return 1
    print("check_pr_workflow: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
