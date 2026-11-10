#!/usr/bin/env python3
"""check_pr_workflow.py -- #706 guard: the pull request workflow keeps running the client suite.

Before .github/workflows/pr.yml no check ran on a pull request, so a PR merged with none.  This guard is a text parse
(no YAML dependency) of that file and fails when it is removed or weakened:

  (a) `pull_request:` is a trigger under the top-level `on:`;
  (b) the client is configured (cmake -S client-cpp) with -DBUILD_TESTING=ON and -DBUILD_EXAMPLES=ON, so every example
      compiles and the client tests exist;
  (c) a ctest run on the client build (--test-dir ... client ...) selects `-R Regression` and `-LE latency`;
  (d) nothing selects `-L latency`: latency gates need an idle host and never run in CI;
  (e) codec, ql and crypto each build standalone with -DBUILD_TESTING=ON and have their suite run with `-LE latency`
      (#960: the root build registers none of their tests). A `matrix.lib` job counts when its `lib:` list names
      the library; a literal `cmake -S <lib>` / `ctest --test-dir build-<lib>...` counts too;
  (f) the broker is configured from the root (cmake -S . -B build-broker) with -DBUILD_TESTING=ON, a ctest run on
      build-broker selects `-R Regression` and excludes `latency` with -LE, and its shard stride (-I k,total,N)
      matches a full plan `shards=[0,...,N-1]` (#860);
  (g) every Python guard invocation broker/CMakeLists.txt registers as a ctest (script plus `--selftest` or the
      broker tree) also runs in pr.yml (#860).

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


def check(text, broker_guards=()):
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

    matrix_libs = set()
    for l in lines:
        m = re.match(r"^\s+lib\s*:\s*\[([^\]]*)\]", l)
        if m:
            matrix_libs |= {x.strip().strip("'\"") for x in m.group(1).split(",")}
    for lib in STANDALONE_LIBS:
        ref = r"(?:\$\{\{\s*matrix\.lib\s*\}\}|" + lib + ")" if lib in matrix_libs else lib
        cfg = [l for l in lines if re.search(r"\bcmake\b.*-S\s+" + ref + r"(\s|$)", l)]
        if not cfg or not any("-DBUILD_TESTING=ON" in l for l in cfg):
            errors.append(f"(e) no `cmake -S {lib}` configure step with -DBUILD_TESTING=ON")
        runs = [l for l in lines if re.search(r"\bctest\b", l) and re.search(r"--test-dir\s+build-" + ref + r"(?=[/\s]|$)", l)]
        if not any(re.search(r"(^|\s)-LE\s+[\"']?latency", l) for l in runs):
            errors.append(f"(e) no `ctest --test-dir build-{lib}...` step with `-LE latency`")

    cfg = [l for l in lines if re.search(r"\bcmake\b.*-S\s+\.(\s|$)", l) and re.search(r"-B\s+build-broker\b", l)]
    if not cfg or not any("-DBUILD_TESTING=ON" in l for l in cfg):
        errors.append("(f) no root `cmake -S . -B build-broker` configure step with -DBUILD_TESTING=ON")
    runs = [l for l in lines if re.search(r"\bctest\b", l) and re.search(r"--test-dir\s+build-broker(?=[/\s]|$)", l)
            and re.search(r"(^|\s)-R\s+[\"']?Regression", l)]
    if not runs:
        errors.append("(f) no `ctest --test-dir build-broker -R Regression` step")
    elif not any(re.search(r"(^|\s)-LE\s+[\"']?([^\s\"']*\|)?latency(\||[\"'\s]|$)", l) for l in runs):
        errors.append("(f) the broker ctest step does not exclude `latency` with -LE")
    # Shard k runs every Nth test (ctest -I k+1,total,N); the full plan must list exactly shards 0..N-1, or a push to
    # main silently skips (or double-runs) a residue class of the suite.
    strides = {int(m.group(1)) for l in runs for m in [re.search(r"-I\s+[\"']?[^,\s]+,[^,\s]+,(\d+)", l)] if m}
    plans = [m.group(1) for l in lines for m in [re.search(r"shards=\[([0-9,\s]+)\]", l)] if m]
    if runs and strides:
        if len(strides) != 1:
            errors.append(f"(f) the broker ctest steps disagree on the shard stride: {sorted(strides)}")
        else:
            n = strides.pop()
            if not any([int(x) for x in p.split(",")] == list(range(n)) for p in plans):
                errors.append(f"(f) no full shard plan `shards=[0..{n - 1}]` matching the ctest -I stride {n}")

    ran = set()
    for l in lines:
        for m in re.finditer(r"python3\s+broker/scripts/(\w+\.py)(?:[ \t]+([^\s;&|]+))?", l):
            ran.add(_guard_run(m.group(1), m.group(2)))
    for g in broker_guards:
        if g not in ran:
            errors.append(f"(g) broker guard `{g}` is a ctest in broker/CMakeLists.txt but pr.yml never runs it")
    return errors


def _guard_run(script, arg):
    """One guard invocation as `<script>`, `<script> --selftest` or `<script> broker` (the broker tree as root)."""
    if not arg:
        return script
    if arg == "--selftest":
        return script + " --selftest"
    if arg in ("broker", "${CMAKE_CURRENT_SOURCE_DIR}"):
        return script + " broker"
    return script + " " + arg


def registered_broker_guards(cmakelists):
    """Every Python guard invocation broker/CMakeLists.txt registers as a ctest, as _guard_run spells it."""
    try:
        text = cmakelists.read_text()
    except OSError:
        return None
    pat = r"\$\{CMAKE_CURRENT_SOURCE_DIR\}/scripts/(\w+\.py)(?:\s+(--selftest|\$\{CMAKE_CURRENT_SOURCE_DIR\}))?"
    return tuple(sorted({_guard_run(m.group(1), m.group(2)) for m in re.finditer(pat, text)}))


STANDALONE_LIBS = ("codec", "ql", "crypto")


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
  libs:
    strategy:
      matrix:
        lib: [codec, ql, crypto]
    steps:
      - name: Configure
        run: cmake -S ${{ matrix.lib }} -B build-${{ matrix.lib }}/cmake -DBUILD_TESTING=ON
      - name: Test suite
        run: ctest --test-dir build-${{ matrix.lib }}/cmake -LE latency -j4
  plan:
    steps:
      - run: |
          if [[ $EVENT == pull_request ]]; then
            echo "shards=[$((PR % 6))]" >> "$GITHUB_OUTPUT"
          else
            echo 'shards=[0,1,2,3,4,5]' >> "$GITHUB_OUTPUT"
          fi
  broker-ratchets:
    steps:
      - run: |
          python3 broker/scripts/check_no_atomics.py --selftest
          python3 broker/scripts/check_no_atomics.py broker
          python3 broker/scripts/test_check_gate.py
  broker:
    steps:
      - name: Configure
        run: |
          cmake -S . -B build-broker -G Ninja \
            -DBUILD_TESTING=ON
      - name: Regression suite (broker)
        run: |
          ctest --test-dir build-broker -R Regression -LE "latency|stress" \
            -I "$((SHARD+1)),$total,6" -j2
"""

GOOD_GUARDS = ("check_no_atomics.py --selftest", "check_no_atomics.py broker", "test_check_gate.py")


def selftest():
    failures = []
    if check(GOOD, GOOD_GUARDS):
        failures.append("the good fixture was refused: " + "; ".join(check(GOOD, GOOD_GUARDS)))
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
        "e": GOOD.replace("lib: [codec, ql, crypto]", "lib: [codec, crypto]"),
        "e2": GOOD.replace("-B build-${{ matrix.lib }}/cmake -DBUILD_TESTING=ON", "-B build-${{ matrix.lib }}/cmake"),
        "e3": GOOD.replace("-LE latency -j4\n", "-j4\n"),
        "e4": GOOD.replace("cmake -S ${{ matrix.lib }}", "cmake -S other"),
        # #860: the broker job and the broker source guards are covered too, not only the client job.
        "f": GOOD.replace("cmake -S . -B build-broker", "cmake -S broker -B build-broker"),
        "f2": GOOD.replace("            -DBUILD_TESTING=ON\n      - name: Regression suite (broker)",
                           "            -DBUILD_TESTING=OFF\n      - name: Regression suite (broker)"),
        "f3": GOOD.replace('ctest --test-dir build-broker -R Regression', 'ctest --test-dir build-broker -R Smoke'),
        "f4": GOOD.replace('-LE "latency|stress"', '-LE "stress"'),
        "f5": GOOD.replace(',$total,6"', ',$total,7"'),
        "f6": GOOD.replace("'shards=[0,1,2,3,4,5]'", "'shards=[0,1,2,3,4]'"),
        "f7": GOOD.replace("ctest --test-dir build-broker", "ctest --test-dir build-other"),
        "g": GOOD.replace("          python3 broker/scripts/test_check_gate.py\n", ""),
        "g2": GOOD.replace("python3 broker/scripts/check_no_atomics.py broker", "true"),
        "g3": GOOD.replace("          python3 broker/scripts/test_check_gate.py\n",
                           "          # python3 broker/scripts/test_check_gate.py\n"),
    }
    for rule, text in mutants.items():
        errs = check(text, GOOD_GUARDS)
        if not any(e.startswith("(" + rule[0] + ")") for e in errs):
            failures.append(f"mutant {rule} was not caught (got: {errs})")
    # a pull_request key outside `on:` does not count
    outside = GOOD.replace("on:\n  pull_request:\n    branches: [main]\n", "on:\n") + "x:\n  pull_request:\n"
    if not any(e.startswith("(a)") for e in check(outside, GOOD_GUARDS)):
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
    guards = registered_broker_guards(path.resolve().parent.parent.parent / "broker" / "CMakeLists.txt")
    if guards is None or not guards:
        print(f"check_pr_workflow: FAIL -- no broker guard ctests found next to {path.name} (wrong path?)")
        return 1
    errors = check(text, guards)
    if errors:
        print("check_pr_workflow: FAIL -- pr.yml no longer runs the client, library and broker suites on pull requests:\n  " + "\n  ".join(errors))
        return 1
    print("check_pr_workflow: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
