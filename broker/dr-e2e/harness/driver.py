"""Multi-client driver: launch N rune-dr-client containers + parse their output.

The dr_client binary (rune-client-cpp/examples/dr_client.cpp, image
`rune-dr-client:dev`) is fully env-driven and emits machine-parseable lines:

    PRODUCED  cid=<c> key=<k> seq=<s> part=<p> offset=<o>
    DELIVERED cid=<c> key=<k> seq=<s> part=<p> offset=<o>
    SUMMARY   produced=<np> delivered=<nd> mode=<m> rc=<rc>

Group mode (RUNE_MODE=group) additionally emits:
    ASSIGN       cid=<c> parts=<p0,p1,...>
    REVOKE       cid=<c> parts=<p0,p1,...>
    GROUPREC     cid=<c> topic=<t> partition=<p> offset=<o> rec_cid=<rc> rec_key=<rk> rec_seq=<rs>
    GROUPSUMMARY cid=<c> consumed=<n> rc=<rc>

`run_clients(n, ...)` runs n such containers concurrently against the cluster,
collects every line, and returns a `Results` object the invariant assertions
consume.  Each client gets a disjoint client id (0..n-1) and its own egress
port so their Aeron UDP endpoints never collide.

`run_group(members, producer_n, ...)` launches producer_n produce-mode clients
plus `members` group-mode consumers in the same group, collects all parsed
output, and returns a `GroupResults` the group-invariant assertions consume.
"""

from __future__ import annotations

import concurrent.futures
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from .dockerutil import run

# Client image tag is overridable so each work-context uses a UNIQUE tag (never a
# generic :dev/:latest that silently overwrites a baseline).  Set DR_CLIENT_IMAGE.
DR_IMAGE = os.environ.get("DR_CLIENT_IMAGE", "rune-dr-client:dev")
# The client image's platform: unset = the image's own (native arm64 on Apple Silicon, amd64 on the bench box);
# DR_CLIENT_PLATFORM=linux/amd64 forces emulation for an amd64-only image.
_PLATFORM_ARGS = (["--platform", os.environ["DR_CLIENT_PLATFORM"]] if os.environ.get("DR_CLIENT_PLATFORM") else [])

_PRODUCED_RE = re.compile(r"PRODUCED cid=(-?\d+) key=(-?\d+) seq=(-?\d+) part=(-?\d+) offset=(-?\d+)")
_DELIVERED_RE = re.compile(r"DELIVERED cid=(-?\d+) key=(-?\d+) seq=(-?\d+) part=(-?\d+) offset=(-?\d+)")
_SUMMARY_RE = re.compile(r"SUMMARY produced=(\d+) delivered=(\d+) mode=(\S+) rc=(\d+)")

# TX-5: txn mode regexes.
_TXNPRODUCE_RE = re.compile(r"TXNPRODUCE cid=(-?\d+) key=(-?\d+) seq=(-?\d+) part=(-?\d+)")
_TXNRESULT_RE = re.compile(r"TXNRESULT txn_id=(\S+) cid=(-?\d+) outcome=(\S+)")
_TXNCONSUME_SUMMARY_RE = re.compile(r"TXNCONSUME_SUMMARY cid=(-?\d+) consumed=(\d+) isolation=(\S+)")

# Group-mode regexes.
# gen=<g> is emitted by dr_client (CG-6 fix2) so single_owner can be
# generation-aware.  It is optional in the regex for backward compatibility with
# older client images that predate the gen= field (parsed as generation -1).
_ASSIGN_RE = re.compile(r"ASSIGN cid=(-?\d+)(?: gen=(-?\d+))? parts=([\d,]*)")
_REVOKE_RE = re.compile(r"REVOKE cid=(-?\d+)(?: gen=(-?\d+))? parts=([\d,]*)")
_GROUPREC_RE = re.compile(
    r"GROUPREC cid=(-?\d+)(?: gen=(-?\d+))? topic=(\S+) partition=(-?\d+) offset=(-?\d+)"
    r" rec_cid=(-?\d+) rec_key=(-?\d+) rec_seq=(-?\d+)"
)
_GROUPSUMMARY_RE = re.compile(r"GROUPSUMMARY cid=(-?\d+) consumed=(\d+) rc=(\d+)")


@dataclass
class Msg:
    cid: int
    key: int
    seq: int
    partition: int
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


# ---------------------------------------------------------------------------
# Group-consumer data structures
# ---------------------------------------------------------------------------

@dataclass
class GroupRec:
    """A single record received by a group-consumer member."""
    member_cid: int    # the consumer-group member's client id
    topic: str
    partition: int
    offset: int
    rec_cid: int       # cid encoded in the record value payload
    rec_key: int       # key encoded in the payload
    rec_seq: int       # seq encoded in the payload
    generation: int = -1  # assignment generation at delivery (-1 = pre-gen client)


@dataclass
class GroupMemberResult:
    """Parsed output of one group-consumer member container."""
    cid: int
    exit_code: int
    rc: int            # from GROUPSUMMARY rc= field (0=success)
    consumed: int      # total records consumed (all assigned partitions)
    records: list[GroupRec] = field(default_factory=list)
    assigned_partitions: list[list[int]] = field(default_factory=list)  # list of assignment events
    revoked_partitions: list[list[int]] = field(default_factory=list)
    raw: str = ""


@dataclass
class GroupResults:
    """Collected output from a run_group() call.

    ``produce_results`` is the Results from the producer containers.
    ``member_results`` is a list of GroupMemberResult, one per consumer member.
    """
    produce_results: Results
    member_results: list[GroupMemberResult] = field(default_factory=list)

    @property
    def all_produced(self) -> list[Msg]:
        return self.produce_results.all_produced

    @property
    def all_group_records(self) -> list[GroupRec]:
        out: list[GroupRec] = []
        for m in self.member_results:
            out.extend(m.records)
        return out

    @property
    def ok(self) -> bool:
        prod_ok = self.produce_results.ok
        members_ok = all(
            m.exit_code == 0 and m.rc == 0 for m in self.member_results
        )
        return prod_ok and members_ok


# ---------------------------------------------------------------------------
# TX-5: Transactional producer/consumer data structures + run_txn() helper
# ---------------------------------------------------------------------------

@dataclass
class TxnMsg:
    """A single record sent inside a transaction (from TXNPRODUCE output)."""
    cid: int
    key: int
    seq: int
    partition: int


@dataclass
class TxnResult:
    """Parsed output of one txn_produce or txn_consume container."""
    cid: int
    txn_id: str
    outcome: str           # "committed" | "aborted" | "" (consume mode)
    sent: list[TxnMsg] = field(default_factory=list)    # TXNPRODUCE lines
    delivered: list[Msg] = field(default_factory=list)  # DELIVERED lines (txn_consume)
    consumed: int = 0
    isolation: str = ""
    exit_code: int = 0
    rc: int = 1
    raw: str = ""

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and self.rc == 0


def _parse_txn_output(cid: int, exit_code: int, out: str) -> TxnResult:
    """Parse txn_produce or txn_consume container stdout+stderr."""
    res = TxnResult(cid=cid, txn_id="", outcome="", exit_code=exit_code, raw=out)
    for m in _TXNPRODUCE_RE.finditer(out):
        res.sent.append(TxnMsg(int(m[1]), int(m[2]), int(m[3]), int(m[4])))
    for m in _DELIVERED_RE.finditer(out):
        res.delivered.append(Msg(int(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5])))
    rm = _TXNRESULT_RE.search(out)
    if rm:
        res.txn_id = rm[1]
        res.outcome = rm[3]
    cs = _TXNCONSUME_SUMMARY_RE.search(out)
    if cs:
        res.consumed = int(cs[2])
        res.isolation = cs[3]
    sm = _SUMMARY_RE.search(out)
    if sm:
        res.rc = int(sm[4])
    return res


def run_txn(
    *,
    network: str,
    target_service: str,
    cid: int = 0,
    txn_id: str = "test-txn",
    commit: bool = True,
    topics: str = "events:0:1",
    records: int = 10,
    keys: int = 1,
    timeout_ms: int = 15000,
    timeout_s: int = 120,
    egress_port: int = 40800,
) -> TxnResult:
    """Run one txn_produce container and return the parsed TxnResult.

    The txn_produce mode runs init→begin→send(records)→commit/abort and emits
    TXNPRODUCE + TXNRESULT + SUMMARY lines that _parse_txn_output decodes.
    """
    name = f"dr-txn-{cid}-{uuid.uuid4().hex[:8]}"
    env = {
        "RUNE_TARGET": f"aeron:udp?endpoint={target_service}:40200",
        "RUNE_TARGET_STREAM": "1",
        "RUNE_EGRESS": f"aeron:udp?endpoint={name}:{egress_port}",
        "RUNE_EGRESS_STREAM": "2",
        "RUNE_DATA_DIR": "/var/lib/rune-client",
        "RUNE_TOPICS": topics,
        "RUNE_CLIENT_ID": str(cid),
        "RUNE_MODE": "txn_produce",
        "RUNE_TXN_ID": txn_id,
        "RUNE_TXN_COMMIT": "1" if commit else "0",
        "RUNE_RECORDS": str(records),
        "RUNE_KEYS": str(keys),
        "RUNE_TIMEOUT_MS": str(timeout_ms),
    }
    args = [
        "docker", "run", "--rm",
        "--name", name,
        *_PLATFORM_ARGS,
        "--network", network,
        "--hostname", name,
        "--shm-size=64m",
    ]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    args.append(DR_IMAGE)
    res = run(args, timeout=timeout_s)
    return _parse_txn_output(cid, res.returncode, res.stdout + "\n" + res.stderr)


def run_txn_consume(
    *,
    network: str,
    target_service: str,
    group_id: str,
    cid: int = 0,
    topics: str = "events:0:1",
    isolation: str = "read_committed",
    subscribe_ms: int = 20000,
    autocommit_ms: int = 1000,
    timeout_ms: int = 15000,
    timeout_s: int = 90,
    egress_port: int = 40850,
) -> TxnResult:
    """Run one txn_consume container with the given isolation level.

    Returns a TxnResult with .delivered = list of DELIVERED records seen
    and .isolation = the isolation string used.
    """
    name = f"dr-txnc-{cid}-{uuid.uuid4().hex[:8]}"
    env = {
        "RUNE_TARGET": f"aeron:udp?endpoint={target_service}:40200",
        "RUNE_TARGET_STREAM": "1",
        "RUNE_EGRESS": f"aeron:udp?endpoint={name}:{egress_port}",
        "RUNE_EGRESS_STREAM": "2",
        "RUNE_DATA_DIR": "/var/lib/rune-client",
        "RUNE_TOPICS": topics,
        "RUNE_CLIENT_ID": str(cid),
        "RUNE_MODE": "txn_consume",
        "RUNE_GROUP_ID": group_id,
        "RUNE_ISOLATION": isolation,
        "RUNE_SUBSCRIBE_MS": str(subscribe_ms),
        "RUNE_AUTOCOMMIT_MS": str(autocommit_ms),
        "RUNE_TIMEOUT_MS": str(timeout_ms),
    }
    args = [
        "docker", "run", "--rm",
        "--name", name,
        *_PLATFORM_ARGS,
        "--network", network,
        "--hostname", name,
        "--shm-size=64m",
    ]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    args.append(DR_IMAGE)
    res = run(args, timeout=timeout_s)
    return _parse_txn_output(cid, res.returncode, res.stdout + "\n" + res.stderr)


def _parse_group_member_output(cid: int, exit_code: int, out: str) -> GroupMemberResult:
    """Parse a group-consumer member's stdout+stderr into a GroupMemberResult."""
    res = GroupMemberResult(cid=cid, exit_code=exit_code, rc=1, consumed=0, raw=out)
    for m in _ASSIGN_RE.finditer(out):
        parts_str = m[3].strip()
        parts = [int(p) for p in parts_str.split(",") if p] if parts_str else []
        res.assigned_partitions.append(parts)
    for m in _REVOKE_RE.finditer(out):
        parts_str = m[3].strip()
        parts = [int(p) for p in parts_str.split(",") if p] if parts_str else []
        res.revoked_partitions.append(parts)
    for m in _GROUPREC_RE.finditer(out):
        res.records.append(GroupRec(
            member_cid=int(m[1]),
            generation=int(m[2]) if m[2] is not None else -1,
            topic=m[3],
            partition=int(m[4]),
            offset=int(m[5]),
            rec_cid=int(m[6]),
            rec_key=int(m[7]),
            rec_seq=int(m[8]),
        ))
    sm = _GROUPSUMMARY_RE.search(out)
    if sm:
        res.consumed = int(sm[2])
        res.rc = int(sm[3])
    # Fallback to SUMMARY rc if GROUPSUMMARY not found (shouldn't happen).
    elif _SUMMARY_RE.search(out):
        sm2 = _SUMMARY_RE.search(out)
        res.rc = int(sm2[4])  # type: ignore[index]
    return res


def _run_one_group_member(
    *,
    network: str,
    cid: int,
    target_service: str,
    egress_port: int,
    cfg: dict,
    timeout_s: int,
) -> GroupMemberResult:
    """Run a single group-consumer container and parse its output."""
    name = f"dr-group-member-{cid}-{uuid.uuid4().hex[:8]}"
    group_id = cfg.get("group_id", "test-group")
    topics = cfg.get("topics", "events:0:1")
    subscribe_ms = cfg.get("subscribe_ms", 30000)
    autocommit_ms = cfg.get("autocommit_ms", 1000)
    timeout_ms = cfg.get("timeout_ms", 15000)

    env = {
        "RUNE_TARGET": f"aeron:udp?endpoint={target_service}:40200",
        "RUNE_TARGET_STREAM": "1",
        "RUNE_EGRESS": f"aeron:udp?endpoint={name}:{egress_port}",
        "RUNE_EGRESS_STREAM": "2",
        "RUNE_DATA_DIR": "/var/lib/rune-client",
        "RUNE_TOPICS": topics,
        "RUNE_CLIENT_ID": str(cid),
        "RUNE_MODE": "group",
        "RUNE_GROUP_ID": group_id,
        "RUNE_SUBSCRIBE_MS": str(subscribe_ms),
        "RUNE_AUTOCOMMIT_MS": str(autocommit_ms),
        "RUNE_TIMEOUT_MS": str(timeout_ms),
    }

    args = [
        "docker", "run", "--rm",
        "--name", name,
        *_PLATFORM_ARGS,
        "--network", network,
        "--hostname", name,
        "--shm-size=64m",
    ]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    args.append(DR_IMAGE)

    res = run(args, timeout=timeout_s)
    return _parse_group_member_output(cid, res.returncode, res.stdout + "\n" + res.stderr)


def run_group(
    members: int,
    *,
    network: str,
    target_service: str,
    group_id: str,
    topics: str = "events:0:1",
    records: int = 50,
    keys: int = 1,
    partitions: int = 2,
    cid_base: int = 0,
    subscribe_ms: int = 30000,
    autocommit_ms: int = 1000,
    timeout_ms: int = 15000,
    timeout_s: int = 240,
    start_stagger_ms: int = 400,
    acks: str = "all",
    producer_cid_base: Optional[int] = None,
    produce_after_members_ms: int = 0,
) -> GroupResults:
    """Run a producer and M group-consumer members and return GroupResults.

    Launches one produce-only container (cid_base) and `members` group-consumer
    containers (cids: cid_base+1 .. cid_base+members).  The producer uses
    cid_base+0 by default but can be overridden via producer_cid_base.

    Ordering:
      * ``produce_after_members_ms == 0`` (default): produce FIRST, then launch
        members.  Members join sequentially against an already-populated log; the
        ramp-up rebalances can cross-generation-re-deliver low offsets.  Used by
        the failover / at-least-once tests, which explicitly tolerate that.
      * ``produce_after_members_ms > 0``: launch all members FIRST, wait this many
        ms for every member to join AND the group to converge to its FINAL stable
        membership (no more joins are coming), THEN produce.  All records are then
        delivered within a SINGLE assignment generation, making the stable test
        deterministic and single_owner strict.  The ramp-up rebalances happen
        while the log is empty, so no real record is re-read across generations.

    Args:
        members:           Number of consumer-group members to launch.
        network:           Docker network name (from cluster.compose.network_name()).
        target_service:    Broker service name to connect to.
        group_id:          Consumer group id string.
        topics:            Topic spec "name:id:partitions,...".
        records:           Records for the producer to produce (acks=all).
        keys:              Distinct keys the producer uses.
        partitions:        Number of topic partitions (for sub_partitions).
        cid_base:          Base client-id; producer gets cid_base, members get
                           cid_base+1..cid_base+members.
        subscribe_ms:      How long each member consumes (window).
        autocommit_ms:     Auto-commit interval for group consumers.
        timeout_ms:        Connection timeout for both producers and consumers.
        timeout_s:         Wall-clock budget per container.
        start_stagger_ms:  Delay between launching member containers.
        acks:              Producer acks policy ("all" default — majority-durable).
        producer_cid_base: Override producer cid (defaults to cid_base).
        produce_after_members_ms: If > 0, produce only after members are up and
                           the group has converged (see Ordering above).
    """
    if producer_cid_base is None:
        producer_cid_base = cid_base
    member_cid_base = cid_base + 1

    def _produce() -> Results:
        # Produce records with acks=all so every PRODUCED = majority-committed.
        return run_clients(
            1,
            network=network,
            target_service=target_service,
            cid_base=producer_cid_base,
            timeout_s=timeout_s,
            mode="produce",
            topics=topics,
            records=records,
            keys=max(keys, partitions),   # spread across partitions
            acks=acks,
            timeout_ms=timeout_ms,
            connect_retries=0,
            start_stagger_ms=0,
        )

    prod_results: Optional[Results] = None
    if produce_after_members_ms <= 0:
        # Default ordering: produce first, then consume.
        prod_results = _produce()

    # Run the consumer-group members concurrently.
    member_results: list[GroupMemberResult] = []
    member_cfg = {
        "group_id": group_id,
        "topics": topics,
        "subscribe_ms": subscribe_ms,
        "autocommit_ms": autocommit_ms,
        "timeout_ms": timeout_ms,
    }
    egress_port_base = 40700  # disjoint from run_clients base (40500)

    # When producing after members, kick off a deferred producer on a side thread:
    # it sleeps until all members have joined + the group has reached its FINAL
    # stable membership, then produces into the single stable generation.
    deferred_producer: Optional[concurrent.futures.Future] = None
    producer_pool: Optional[concurrent.futures.ThreadPoolExecutor] = None
    if produce_after_members_ms > 0:
        producer_pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        def _deferred_produce() -> Results:
            time.sleep(produce_after_members_ms / 1000.0)
            return _produce()

        deferred_producer = producer_pool.submit(_deferred_produce)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, members)) as pool:
        futs = []
        for i in range(members):
            if i > 0 and start_stagger_ms > 0:
                time.sleep(start_stagger_ms / 1000.0)
            futs.append(pool.submit(
                _run_one_group_member,
                network=network,
                cid=member_cid_base + i,
                target_service=target_service,
                egress_port=egress_port_base + i,
                cfg=member_cfg,
                timeout_s=timeout_s,
            ))
        for f in concurrent.futures.as_completed(futs, timeout=timeout_s + 30):
            member_results.append(f.result())

    if deferred_producer is not None:
        prod_results = deferred_producer.result(timeout=timeout_s + 30)
    if producer_pool is not None:
        producer_pool.shutdown(wait=True)

    assert prod_results is not None
    member_results.sort(key=lambda m: m.cid)
    return GroupResults(produce_results=prod_results, member_results=member_results)


def _parse_client_output(cid: int, exit_code: int, out: str) -> ClientResult:
    # DR_CLIENT_LOG_DIR: keep every client's full output (assertions show only a tail).
    log_dir = os.environ.get("DR_CLIENT_LOG_DIR")
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, f"{cid}.log"), "w") as fh:
            fh.write(out)
    res = ClientResult(cid=cid, rc=1, exit_code=exit_code, raw=out)
    for m in _PRODUCED_RE.finditer(out):
        res.produced.append(Msg(int(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5])))
    for m in _DELIVERED_RE.finditer(out):
        res.delivered.append(Msg(int(m[1]), int(m[2]), int(m[3]), int(m[4]), int(m[5])))
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
        "RUNE_SUB_PARTITIONS": str(cfg.get("sub_partitions", 1)),
        "RUNE_SUB_PARTITION_BASE": str(cfg.get("sub_partition_base", 0)),
        "RUNE_ACKS": cfg.get("acks", "leader"),
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

    # DR-BUG-1 (e56a527) fixed the startup SIGSEGV race — the exit-139 retry
    # branch is removed.  A resurfaced SIGSEGV must FAIL loudly so regressions
    # are not silently swallowed.
    #
    # DR-BUG-2 (connect-under-load stall) is fixed in both the broker
    # (stash-and-retry ConnectResponse) and the client (fast-retry ConnectRequest
    # when ingress pub NOT_CONNECTED).  The baseline is now expected to pass
    # first-try with connect_retries=0.  Fault tickets may opt into retries
    # explicitly when their failure injection legitimately causes connect timeouts.
    connect_retries = int(cfg.get("connect_retries", 0))
    last: ClientResult | None = None
    for attempt in range(connect_retries + 1):
        run_name = f"{name}-a{attempt}"
        run_host = run_name
        # Vary egress port per retry attempt: on retry the old container is
        # gone (--rm) but the Aeron media driver's cnc.dat may linger briefly
        # in /dev/shm on the Docker network.  Using a distinct port per attempt
        # ensures the new container's egress subscription never collides with
        # a stale endpoint from a previous attempt on the same node.
        attempt_port = egress_port + attempt * 100
        env["RUNE_EGRESS"] = f"aeron:udp?endpoint={run_host}:{attempt_port}"
        args = [
            "docker", "run", "--rm",
            "--name", run_name,
            *_PLATFORM_ARGS,
            "--network", network,
            "--hostname", run_host,
            "--shm-size=64m",
        ]
        for k, v in env.items():
            args += ["-e", f"{k}={v}"]
        args.append(DR_IMAGE)

        res = run(args, timeout=timeout_s)
        cr = _parse_client_output(cid, res.returncode, res.stdout + "\n" + res.stderr)
        last = cr
        # Retry on transient startup failures with no produced records:
        #   * connect-timeout ("connect() timed out") — broker not yet ready
        #   * DNS resolution failure ("unknown host" / "Name or service not known")
        #     which can occur on Docker Desktop when a recently-unpaused container's
        #     hostname hasn't propagated to the embedded DNS resolver yet.
        # Any other failure (genuine workload error, crash) must fail immediately.
        nothing_produced = len(cr.produced) == 0
        connect_timeout = (cr.rc == 2 and "connect() timed out" in cr.raw)
        dns_failure = (cr.rc == 2 and (
            "unknown host" in cr.raw or "Name or service not known" in cr.raw
        ))
        transient_startup = (connect_timeout or dns_failure) and nothing_produced
        if not (transient_startup and attempt < connect_retries):
            return cr
        # Brief back-off before retry: give Docker Desktop's embedded DNS proxy
        # time to recover after Docker network operations (pause/unpause cycles).
        time.sleep(3.0)
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
    timeout_ms, subscribe_ms, slow_subscriber_ms, expect, sub_partitions,
    sub_partition_base (default 0 — first partition index; combined with
    sub_partitions=K subscribes partitions [base..base+K-1]),
    acks ("leader" default | "all" for majority-durable; maps to RUNE_ACKS),
    connect_retries (default 0 — first-try clean; fault tickets may set > 0),
    start_stagger_ms (default 400 — delay between launching each client so N
    embedded Aeron media drivers don't cold-start in the same instant).
    """
    # start_stagger_ms: launching N dr_client containers at the *exact* same
    # moment makes N embedded media drivers + ingress UDP publications warm up
    # simultaneously on the Docker-Desktop VM, a thundering herd whose contention
    # tail occasionally exceeds even a generous connect deadline (a pure env
    # artifact — real clients never cold-start in the same millisecond).  A small
    # stagger removes that peak without weakening any invariant.  Fault tickets
    # (reconnect storms) may set it to 0 when the simultaneity IS the test.
    start_stagger_ms = int(cfg.pop("start_stagger_ms", 400))

    results = Results()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, n)) as pool:
        futs = []
        for i in range(n):
            if i > 0 and start_stagger_ms > 0:
                time.sleep(start_stagger_ms / 1000.0)
            futs.append(pool.submit(
                _run_one_client,
                network=network,
                cid=cid_base + i,
                target_service=target_service,
                egress_port=egress_port_base + i,
                cfg=cfg,
                timeout_s=timeout_s,
            ))
        for f in concurrent.futures.as_completed(futs, timeout=timeout_s + 30):
            results.clients.append(f.result())
    results.clients.sort(key=lambda c: c.cid)
    return results
