#!/usr/bin/env python3
"""PreToolUse guard for Bash commands (see the root CLAUDE.md §5 and §6).

- `gh issue create` must carry one type label, one sev:* label and at least one area:* label.
- `git push` always asks the owner first.
"""
import json
import re
import shlex
import sys

TYPES = {"bug", "enhancement", "tech-debt", "test-gap", "flaky", "docs"}


def labels_of(argv):
    out = []
    for i, a in enumerate(argv):
        if a in ("--label", "-l") and i + 1 < len(argv):
            out += argv[i + 1].split(",")
        elif a.startswith("--label="):
            out += a[len("--label="):].split(",")
    return [x.strip() for x in out if x.strip()]


def segments(cmd):
    # Split on shell separators so `cd x && gh issue create ...` is still checked.
    for part in re.split(r"&&|\|\||;|\|", cmd):
        try:
            yield shlex.split(part)
        except ValueError:
            yield part.split()


def git_subcommand(argv):
    i = 1
    while i < len(argv) and argv[i].startswith("-"):
        i += 2 if argv[i] in ("-C", "-c", "--git-dir", "--work-tree") else 1
    return argv[i] if i < len(argv) else ""


def deny(reason):
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
                                             "permissionDecision": "deny",
                                             "permissionDecisionReason": reason}}))
    sys.exit(0)


def ask(reason):
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse",
                                             "permissionDecision": "ask",
                                             "permissionDecisionReason": reason}}))
    sys.exit(0)


def main():
    data = json.load(sys.stdin)
    cmd = (data.get("tool_input") or {}).get("command", "")
    for argv in segments(cmd):
        if len(argv) >= 3 and argv[0] == "gh" and argv[1] == "issue" and argv[2] == "create":
            labels = labels_of(argv)
            types = [l for l in labels if l in TYPES]
            sevs = [l for l in labels if l.startswith("sev:")]
            areas = [l for l in labels if l.startswith("area:")]
            if len(types) != 1 or len(sevs) != 1 or not areas:
                deny("gh issue create needs exactly one type label (bug|enhancement|tech-debt|test-gap|flaky|docs), "
                     "exactly one sev:low|medium|high and at least one area:* label (root CLAUDE.md §5). "
                     f"Got: {labels or 'none'}")


if __name__ == "__main__":
    main()
