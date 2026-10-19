#!/usr/bin/env python3
"""Merge results.jsonl into results.json, aggregating repeats by median+spread.

A "cell" is identified by every field except the metric values and repeat index.
Metrics (throughput_*, p50_us, p99_us, p999_us, dr_*) are aggregated across the
matching raw rows: median/min/max for throughput, median for latency.

Usage: collect.py [results.jsonl] [results.json]
"""
import json, sys, statistics
from collections import defaultdict

RAW = sys.argv[1] if len(sys.argv) > 1 else "results.jsonl"
OUT = sys.argv[2] if len(sys.argv) > 2 else "results.json"

KEY_FIELDS = ("tool", "scenario", "tier", "record_bytes", "partitions",
              "cores", "workers", "consumers", "persistence", "nodes", "idle")
THRPT = ("throughput_msgs_per_sec", "throughput_mb_per_sec")
LAT = ("p50_us", "p99_us", "p999_us")
DR = ("elect_ms", "resume_ms", "acked_lost", "restart_s")

rows = []
with open(RAW) as f:
    for ln in f:
        ln = ln.strip()
        if ln:
            rows.append(json.loads(ln))

groups = defaultdict(list)
for r in rows:
    groups[tuple(r.get(k) for k in KEY_FIELDS)].append(r)

def agg(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {"median": statistics.median(vals), "min": min(vals), "max": max(vals)}

cells = []
for key, rs in sorted(groups.items(), key=lambda kv: [str(x) for x in kv[0]]):
    cell = dict(zip(KEY_FIELDS, key))
    cell["repeats"] = len(rs)
    for m in THRPT:
        vals = [r[m] for r in rs if m in r]
        if vals:
            cell[m] = agg(vals)
    for m in LAT:
        vals = [r[m] for r in rs if m in r]
        if vals:
            cell[m] = round(statistics.median(vals), 2)
    for m in DR:
        vals = [r[m] for r in rs if m in r]
        if vals:
            cell[m] = statistics.median(vals)
    cells.append(cell)

with open(OUT, "w") as f:
    json.dump({"cells": cells}, f, indent=2)
print(f"collected {len(rows)} raw rows -> {len(cells)} cells -> {OUT}")
