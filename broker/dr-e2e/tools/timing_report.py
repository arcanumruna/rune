#!/usr/bin/env python3
"""Summarise a DR_TIMING_LOG: exclusive main-thread seconds per phase kind, and the top sleeps / polls.

    python3 tools/timing_report.py timing.jsonl [--by-detail KIND ...] [--per-test]

Exclusive time = a phase's duration minus its direct main-thread children, so the per-kind
column sums to the total wall time of the tests (test.setup/call/teardown exclusive = python
time not covered by any instrumented phase).
"""
from __future__ import annotations

import argparse
import collections
import json


def load(path):
    with open(path) as fh:
        return [json.loads(l) for l in fh if l.strip()]


def exclusive(events):
    # One phase tree per process (xdist worker); trees of different workers overlap in time.
    main = sorted((e for e in events if e["main"]), key=lambda e: (e.get("worker", ""), e["t0"], e["depth"]))
    for e in main:
        e["excl"] = e["dur"]
    stack = []
    for e in main:
        while stack and (stack[-1]["depth"] >= e["depth"] or stack[-1].get("worker", "") != e.get("worker", "")):
            stack.pop()
        if stack:
            stack[-1]["excl"] -= e["dur"]
        stack.append(e)
    return main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("log")
    ap.add_argument("--by-detail", nargs="*", default=["sleep"])
    ap.add_argument("--per-test", action="store_true")
    a = ap.parse_args()
    ev = load(a.log)
    main_ev = exclusive(ev)
    total = sum(e["dur"] for e in main_ev if e["kind"].startswith("test.")) + \
        sum(e["dur"] for e in main_ev if e["depth"] == 0 and not e["kind"].startswith("test."))
    by = collections.defaultdict(lambda: [0.0, 0])
    for e in main_ev:
        by[e["kind"]][0] += e["excl"]
        by[e["kind"]][1] += 1
    tests = {e["test"] for e in ev if e["kind"] == "test.call"}
    print(f"tests: {len(tests)}   instrumented wall: {total:.0f}s   per test: {total / max(1, len(tests)):.1f}s\n")
    print(f"| phase | count | exclusive s | share |\n|---|---:|---:|---:|")
    for k, (s, n) in sorted(by.items(), key=lambda kv: -kv[1][0]):
        print(f"| {k} | {n} | {s:.1f} | {100 * s / max(total, 1e-9):.1f}% |")
    for kind in a.by_detail:
        d = collections.defaultdict(lambda: [0.0, 0])
        for e in main_ev:
            if e["kind"] == kind:
                d[e["detail"]][0] += e["dur"]
                d[e["detail"]][1] += 1
        print(f"\n{kind} by detail:\n| detail | count | s |\n|---|---:|---:|")
        for k, (s, n) in sorted(d.items(), key=lambda kv: -kv[1][0])[:40]:
            print(f"| {k} | {n} | {s:.1f} |")
    cont = collections.defaultdict(lambda: [0.0, 0, 0.0])
    for e in ev:
        if e["kind"].startswith("container."):
            c = cont[e["kind"]]
            c[0] += e["dur"]; c[1] += 1; c[2] = max(c[2], e["dur"])
    print("\ncontainer runs (all threads, overlapping):\n| kind | count | sum s | max s |\n|---|---:|---:|---:|")
    for k, (s, n, m) in sorted(cont.items(), key=lambda kv: -kv[1][0]):
        print(f"| {k} | {n} | {s:.1f} | {m:.1f} |")
    if a.per_test:
        per = collections.defaultdict(float)
        for e in main_ev:
            if e["kind"].startswith("test."):
                per[e["test"]] += e["dur"]
        print("\nper test:")
        for t, s in sorted(per.items(), key=lambda kv: -kv[1]):
            print(f"{s:8.1f}  {t}")


if __name__ == "__main__":
    main()
