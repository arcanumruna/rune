#!/usr/bin/env python3
"""Parse Kafka perf-tool output into key=value lines for the bash harness.

Usage: parse_kafka.py {producer|consumer|e2e} < tool_output
Latencies are normalised to microseconds (Kafka reports ms).
"""
import re, sys


def producer(text):
    # Final summary line, e.g.:
    # "1000000 records sent, 500000.5 records/sec (122.07 MB/sec), 5.00 ms avg
    #  latency, 250.00 ms max latency, 4 ms 50th, 10 ms 95th, 15 ms 99th, 20 ms 99.9th."
    line = ""
    for ln in text.splitlines():
        if "records sent" in ln and "records/sec" in ln:
            line = ln  # keep the last (final summary)
    out = {"rps": 0, "mbps": 0, "p50": 0, "p99": 0, "p999": 0}
    m = re.search(r"([\d.]+)\s+records/sec\s+\(([\d.]+)\s+MB/sec\)", line)
    if m:
        out["rps"], out["mbps"] = float(m.group(1)), float(m.group(2))
    for pct, key in ((r"50th", "p50"), (r"99th(?!\.)", "p99"), (r"99\.9th", "p999")):
        m = re.search(r"([\d.]+)\s*ms\s*" + pct, line)
        if m:
            out[key] = float(m.group(1)) * 1000.0  # ms -> us
    return out


def consumer(text):
    # CSV: header then data row; nMsg.sec is column index 5, MB.sec index 3.
    rows = [r for r in text.splitlines() if r.strip() and "," in r]
    out = {"rps": 0, "mbps": 0}
    if len(rows) >= 2:
        cols = [c.strip() for c in rows[-1].split(",")]
        try:
            out["mbps"] = float(cols[3])
            out["rps"] = float(cols[5])
        except (IndexError, ValueError):
            pass
    return out


def e2e(text):
    # "Percentiles: 50th = 1.00, 99th = 3.00, 99.9th = 5.00" (ms)
    out = {"p50": 0, "p99": 0, "p999": 0}
    for pct, key in ((r"50th", "p50"), (r"99th", "p99"), (r"99\.9th", "p999")):
        m = re.search(pct + r"\s*=\s*([\d.]+)", text)
        if m:
            out[key] = float(m.group(1)) * 1000.0  # ms -> us
    return out


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else ""
    text = sys.stdin.read()
    fn = {"producer": producer, "consumer": consumer, "e2e": e2e}.get(kind)
    if not fn:
        sys.exit("unknown kind: " + kind)
    print(" ".join(f"{k}={v}" for k, v in fn(text).items()))
