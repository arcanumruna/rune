"""Multi-client driver: launch N rune-dr-client containers + parse their output.

The dr_client binary (rune-client-cpp/examples/dr_client.cpp, image
`rune-dr-client:dev`) is fully env-driven and emits machine-parseable lines:

    PRODUCED  cid=<c> key=<k> seq=<s> offset=<o>
    DELIVERED cid=<c> key=<k> seq=<s> offset=<o>
    SUMMARY   produced=<np> delivered=<nd> mode=<m> rc=<rc>

`run_clients(n, ...)` runs n such containers concurrently against the cluster,
collects every line, and returns a `Results` object the invariant assertions
consume.  Each client gets a disjoint client id (0..n-1) and its own egress
port so their Aeron UDP endpoints never collide.
"""

from __future__ import annotations

import concurrent.futures
import re
import time
import uuid
from dataclasses import dataclass, field

from .dockerutil import run

DR_IMAGE = "rune-dr-client:dev"

_PRODUCED_RE = re.compile(r"PRODUCED cid=(-?\d+) key=(-?\d+) seq=(-?\d+) offset=(-?\d+)")
_DELIVERED_RE = re.compile(r"DELIVERED cid=(-?\d+) key=(-?\d+) seq=(-?\d+) offset=(-?\d+)")
_SUMMARY_RE = re.compile(r"SUMMARY produced=(\d+) delivered=(\d+) mode=(\S+) rc=(\d+)")


@dataclass
class Msg:
    cid: int
    key: int
    seq: int
    offset: int


@dataclass
class ClientResult:
    cid: int
    rc: int
    produced: list[Msg] = field(default_factory=list)
    delivered: list[Msg] = field(default_factory=list)
    exit_code: int = 0
    raw: str = ""

    @property
    def summary_produced(self) -> int:
        return len(self.produced)

    @property
    def summary_delivered(self) -> int:
        return len(self.delivered)


@dataclass
class Results:
    clients: list[ClientResult] = field(default_factory=list)

    @property
    def all_produced(self) -> list[Msg]:
        out: list[Msg] = []
        for c in self.clients:
            out.extend(c.produced)
        return out

    @property
    def all_delivered(self) -> list[Msg]:
        out: list[Msg] = []
        for c in self.clients:
            out.extend(c.delivered)
        return out

    @property
    def ok(self) -> bool:
        return all(c.exit_code == 0 and c.rc == 0 for c in self.clients)

    def by_cid(self, cid: int) -> ClientResult:
        for c in self.clients:
            if c.cid == cid:
                return c
        raise KeyError(cid)


def _parse_client_output(cid: int, exit_code: int, out: str) -> ClientResult:
    res = ClientResult(cid=cid, rc=1, exit_code=exit_code, raw=out)
    for m in _PRODUCED_RE.finditer(out):
        res.produced.append(Msg(int(m[1]), int(m[2]), int(m[3]), int(m[4])))
    for m in _DELIVERED_RE.finditer(out):
        res.delivered.append(Msg(int(m[1]), int(m[2]), int(m[3]), int(m[4])))
    sm = _SUMMARY_RE.search(out)
    if sm:
        res.rc = int(sm[4])
    return res


def _run_one_client(
    *,
    network: str,
    cid: int,
    target_service: str,
    egress_port: int,
    cfg: dict,
    timeout_s: int,
) -> ClientResult:
    name = f"dr-client-{cid}-{uuid.uuid4().hex[:8]}"
    # RUNE_EGRESS host is set per-attempt below (each retry uses a fresh
    # hostname so the Aeron egress endpoint never collides on retry).
    env = {
        "RUNE_TARGET": f"aeron:udp?endpoint={target_service}:40200",
        "RUNE_TARGET_STREAM": "1",
        "RUNE_EGRESS": f"aeron:udp?endpoint={name}:{egress_port}",  # overridden per-attempt
        "RUNE_EGRESS_STREAM": "2",
        "RUNE_DATA_DIR": "/var/lib/rune-client",
        "RUNE_TOPICS": cfg.get("topics", "events:0:1"),
        "RUNE_CLIENT_ID": str(cid),
        "RUNE_MODE": cfg.get("mode", "produce_subscribe"),
        "RUNE_RECORDS": str(cfg.get("records", 20)),
        "RUNE_KEYS": str(cfg.get("keys", 1)),
        "RUNE_RATE_HZ": str(cfg.get("rate_hz", 0)),
        "RUNE_FROM": cfg.get("from", "earliest"),
        "RUNE_TIMEOUT_MS": str(cfg.get("timeout_ms", 10000)),
        "RUNE_SUBSCRIBE_MS": str(cfg.get("subscribe_ms", 30000)),
        "RUNE_SLOW_SUBSCRIBER_MS": str(cfg.get("slow_subscriber_ms", 0)),
    }
    if "expect" in cfg:
        env["RUNE_EXPECT"] = str(cfg["expect"])
    if "expect_cid_min" in cfg:
        env["RUNE_EXPECT_CID_MIN"] = str(cfg["expect_cid_min"])
    if cfg.get("expect_own"):
        # count only THIS client's own records toward `expect` — for multi-client
        # runs where each subscriber verifies its own committed set without racing
        # the other producers.
        env["RUNE_EXPECT_CID_EXACT"] = str(cid)

    # Bounded retry for TRANSIENT client-startup faults that leave NO produced
    # records: a connect() timeout (rc==2) or a SIGSEGV (exit 139) in the
    # connect/first-produce handshake.  The current rune-client-cpp build has a
    # race there (documented in the DR-1 report); retrying from a clean state is
    # safe because nothing was produced yet.  We NEVER retry once any record was
    # produced (would risk double-produce) or on a genuine workload failure.
    connect_retries = int(cfg.get("connect_retries", 5))
    last: ClientResult | None = None
    for attempt in range(connect_retries + 1):
        run_name = f"{name}-a{attempt}"
        run_host = run_name
        env["RUNE_EGRESS"] = f"aeron:udp?endpoint={run_host}:{egress_port}"
        args = [
            "docker", "run", "--rm",
            "--name", run_name,
            "--platform", "linux/amd64",
            "--network", network,
            "--hostname", run_host,
            "--shm-size=256m",
        ]
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
        args.append(DR_IMAGE)

        res = run(args, timeout=timeout_s)
        cr = _parse_client_output(cid, res.returncode, res.stdout + "\n" + res.stderr)
        last = cr
        nothing_produced = len(cr.produced) == 0
        startup_crash = (res.returncode == 139) or (
            cr.rc == 2 and "connect() timed out" in cr.raw
        )
        transient = startup_crash and nothing_produced
        if not transient:
            return cr
    return last  # exhausted retries — return the last (still-failed) result


def run_clients(
    n: int,
    *,
    network: str,
    target_service: str,
    egress_port_base: int = 40500,
    cid_base: int = 0,
    timeout_s: int = 180,
    **cfg,
) -> Results:
    """Run `n` dr_client containers concurrently. Returns parsed Results.

    `cid_base` offsets every client's logical id (RUNE_CLIENT_ID = cid_base+i).
    Because dr_client disjoints its key space by client id, a unique cid_base per
    test run keeps that run's records from colliding with records left on a
    shared/persistent partition by earlier runs — so per-key ordering and
    exactly-once stay attributable per run.

    cfg keys (all optional): mode, records, keys, rate_hz, from, topics,
    timeout_ms, subscribe_ms, slow_subscriber_ms, expect.
    """
    results = Results()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, n)) as pool:
        futs = [
            pool.submit(
                _run_one_client,
                network=network,
                cid=cid_base + i,
                target_service=target_service,
                egress_port=egress_port_base + i,
                cfg=cfg,
                timeout_s=timeout_s,
            )
            for i in range(n)
        ]
        for f in concurrent.futures.as_completed(futs, timeout=timeout_s + 30):
            results.clients.append(f.result())
    results.clients.sort(key=lambda c: c.cid)
    return results
