"""CG-6: Consumer-group at-least-once delivery under member crash.

Proves the at-least-once guarantee: if a member crashes mid-consume (SIGKILL
of its container while it actively owns partitions), its partitions are
reassigned to surviving members once the coordinator detects the session timeout,
and the union of all consumed records still covers every produced record (no
record missed by the group).

Scenarios
---------
test_cg_atleastonce_member_crash
    Start 2 members concurrently.  After a brief consume window (records are
    accumulating but not fully committed), SIGKILL the FIRST member's container
    while it is still running.  The second member must survive the full window
    and, after the coordinator detects the session timeout and triggers a
    rebalance, inherit the dead member's partitions.  The union of consumed
    records across both members (even though the first died) must cover ALL
    produced records.

test_cg_atleastonce_multiple_survivors
    3 members → 1 crashes → 2 survivors cover the full produced set.
    Same at-least-once proof but with 3 initial members and 2 survivors, which
    exercises a more realistic rebalance (one-third of partitions orphaned and
    redistributed, not a full handover).

CID window: 2_200_000+ — within CG-6's 2_000_000+ window.
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
import uuid

import pytest

from harness import faults
from harness.cluster import node_id_to_service
from harness.driver import (
    GroupMemberResult,
    GroupResults,
    Results,
    _parse_group_member_output,
    _run_one_group_member,
    run_clients,
    dr_image,
)
from harness.dockerutil import run as docker_run
from harness.invariants import (
    assert_group_bounded_duplicates,
    assert_group_no_committed_loss,
    assert_group_single_owner,
    assert_single_leader,
)

_CID_STRIDE = 5000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 2_200_000 + _run_counter["n"] * _CID_STRIDE


_RECORDS = 80
_KILL_DELAY_S = 8.0     # seconds after member start before SIGKILL (mid-consume)
_LONG_MS = 70000        # survive window: long enough for session timeout + rebalance + drain
_TIMEOUT_MS = 20000
# "cg" topic: topicId=2, 4 partitions.  Must match broker RUNE_TOPICS="events:1,multi:3,cg:4".
_TOPIC = "cg:2:4"
_GROUP_ID = "cg-alo-group"


def _run_group_member_named(
    *,
    network: str,
    cid: int,
    target_service: str,
    egress_port: int,
    group_id: str,
    topics: str,
    subscribe_ms: int,
    autocommit_ms: int,
    timeout_ms: int,
    timeout_s: int,
    container_name: str | None = None,
) -> tuple[str, GroupMemberResult]:
    """Run one group-consumer container.

    Returns (container_name, GroupMemberResult).  The container name is either
    the supplied `container_name` or an auto-generated one.  Callers that need
    to kill the container mid-run can supply the name ahead of time and use it
    with faults.kill_client_container().
    """
    name = container_name or f"dr-group-member-{cid}-{uuid.uuid4().hex[:8]}"

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
        "--platform", "linux/amd64",
        "--network", network,
        "--hostname", name,
        "--shm-size=64m",
    ]
    for k, v in env.items():
        args += ["-e", f"{k}={v}"]
    args.append(dr_image())

    res = docker_run(args, timeout=timeout_s)
    return name, _parse_group_member_output(cid, res.returncode, res.stdout + "\n" + res.stderr)


@pytest.mark.fault
@pytest.mark.timeout(700)
def test_cg_atleastonce_member_crash(fresh_cluster):
    """Member container SIGKILL mid-consume → partitions reassigned → no loss.

    Scenario:
      1. Produce _RECORDS records (acks=all) — all majority-durable.
      2. Launch 2 group-member containers concurrently; both start consuming.
      3. After _KILL_DELAY_S seconds (mid-consume, partitions actively owned),
         SIGKILL the FIRST member's container.
      4. The second member keeps running for _LONG_MS total.  The coordinator
         detects the session timeout of the killed member and triggers a
         rebalance, reassigning its partitions to the survivor.
      5. Assert:
           - group_no_committed_loss: union of both members covers all produced.
             (The killed member may have consumed some; the survivor must cover
             the rest after rebalance.)
           - bounded_duplicates: re-delivery bounded to uncommitted window, not
             a restart from 0.
           - single_owner (within each member's records): no partition served
             to two members simultaneously.
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)
    _, leader_svc = cluster.identify_leader()

    cid_base = _next_cid_base()
    network = cluster.compose.network_name()

    # Produce the committed set (majority-durable acks=all).
    prod = run_clients(
        1,
        network=network,
        target_service=leader_svc,
        cid_base=cid_base,
        timeout_s=180,
        mode="produce",
        topics=_TOPIC,
        records=_RECORDS,
        keys=4,
        acks="all",
        timeout_ms=_TIMEOUT_MS,
        connect_retries=0,
        start_stagger_ms=0,
    )
    assert prod.ok, "produce failed:\n" + prod.clients[0].raw[-800:]
    assert len(prod.all_produced) == _RECORDS

    # Member 1 (victim): long enough window so it is mid-consume when killed.
    # Member 2 (survivor): long window to absorb rebalance + drain.
    m1_cid = cid_base + 1
    m2_cid = cid_base + 2
    m1_name = f"dr-group-member-alo-m1-{uuid.uuid4().hex[:8]}"
    m1_port = 40750
    m2_port = 40751

    common_cfg = dict(
        network=network,
        target_service=leader_svc,
        group_id=_GROUP_ID,
        topics=_TOPIC,
        autocommit_ms=800,
        timeout_ms=_TIMEOUT_MS,
        timeout_s=600,
    )

    result_holder: dict[str, GroupMemberResult] = {}
    exception_holder: dict[str, Exception] = {}

    def run_m1():
        try:
            _, r = _run_group_member_named(
                cid=m1_cid,
                egress_port=m1_port,
                subscribe_ms=int(_LONG_MS * 2),   # would run forever if not killed
                container_name=m1_name,
                **common_cfg,
            )
            result_holder["m1"] = r
        except Exception as e:
            exception_holder["m1"] = e
            # Create a sentinel result so assertions can proceed even if the
            # container was killed (non-zero exit code is expected).
            result_holder["m1"] = GroupMemberResult(
                cid=m1_cid, exit_code=137, rc=0, consumed=0, raw=str(e)
            )

    def run_m2():
        try:
            r = _run_one_group_member(
                network=network,
                cid=m2_cid,
                target_service=leader_svc,
                egress_port=m2_port,
                cfg={
                    "group_id": _GROUP_ID,
                    "topics": _TOPIC,
                    "subscribe_ms": _LONG_MS,
                    "autocommit_ms": 800,
                    "timeout_ms": _TIMEOUT_MS,
                },
                timeout_s=600,
            )
            result_holder["m2"] = r
        except Exception as e:
            exception_holder["m2"] = e
            result_holder["m2"] = GroupMemberResult(
                cid=m2_cid, exit_code=1, rc=1, consumed=0, raw=str(e)
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(run_m1)
        f2 = pool.submit(run_m2)

        # Let both members come up and start consuming, then kill member 1.
        time.sleep(_KILL_DELAY_S)
        try:
            faults.kill_client_container(m1_name, sig="KILL")
        except RuntimeError as exc:
            # Container may have already exited (very fast subscribe_ms or
            # SIGKILL already delivered).  Log and continue — the assertion
            # will still verify at-least-once on whatever m1 managed to do.
            import warnings
            warnings.warn(f"kill_client_container for m1 failed (may have exited): {exc}")

        # Wait for both containers to finish.
        f1.result(timeout=700)
        f2.result(timeout=700)

    m1 = result_holder["m1"]
    m2 = result_holder["m2"]

    # Single-owner check: both members together.
    # Note: m1 may have non-zero exit_code (killed), which is expected.
    # We treat its consumed records as valid (they were received before the kill).
    all_members = GroupResults(
        produce_results=prod,
        member_results=[m1, m2],
    )
    # single_owner check is strictly about simultaneous dual ownership — it is
    # valid even if m1 was killed (its records were received in disjoint time).
    assert_group_single_owner(all_members)

    # At-least-once: the UNION of m1 + m2 consumed records must cover all produced.
    # m1 consumed some before dying; m2 inherited the rest after rebalance.
    assert_group_no_committed_loss(all_members)
    assert_group_bounded_duplicates(all_members)
    assert_single_leader(cluster)


@pytest.mark.fault
@pytest.mark.timeout(700)
def test_cg_atleastonce_multiple_survivors(fresh_cluster):
    """3 members → 1 crashes → 2 survivors cover the full produced set.

    Same at-least-once proof but with 3 initial members and 2 survivors, which
    exercises a more realistic rebalance (one-third of partitions orphaned and
    redistributed, not a full handover).

    The crash is simulated by SIGKILL of the first member's container mid-consume.
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)
    _, leader_svc = cluster.identify_leader()

    cid_base = _next_cid_base()
    network = cluster.compose.network_name()
    topic = _TOPIC
    group_id = "cg-alo3-group"

    prod = run_clients(
        1,
        network=network,
        target_service=leader_svc,
        cid_base=cid_base,
        timeout_s=180,
        mode="produce",
        topics=topic,
        records=_RECORDS,
        keys=8,
        acks="all",
        timeout_ms=_TIMEOUT_MS,
        connect_retries=0,
        start_stagger_ms=0,
    )
    assert prod.ok, "produce failed:\n" + prod.clients[0].raw[-800:]
    assert len(prod.all_produced) == _RECORDS

    m_cids = [cid_base + 1, cid_base + 2, cid_base + 3]
    m_ports = [40760, 40761, 40762]
    # Only the FIRST member (index 0) will be killed mid-run.
    victim_name = f"dr-group-member-alo3-m0-{uuid.uuid4().hex[:8]}"

    result_holder: dict[int, GroupMemberResult] = {}

    def _run_member(idx: int):
        cid = m_cids[idx]
        port = m_ports[idx]
        name = victim_name if idx == 0 else None
        subscribe_ms = int(_LONG_MS * 2) if idx == 0 else _LONG_MS
        try:
            _, r = _run_group_member_named(
                network=network,
                cid=cid,
                target_service=leader_svc,
                egress_port=port,
                group_id=group_id,
                topics=topic,
                subscribe_ms=subscribe_ms,
                autocommit_ms=800,
                timeout_ms=_TIMEOUT_MS,
                timeout_s=600,
                container_name=name,
            )
            result_holder[idx] = r
        except Exception as e:
            result_holder[idx] = GroupMemberResult(
                cid=cid, exit_code=137, rc=0, consumed=0, raw=str(e)
            )

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futs = [pool.submit(_run_member, i) for i in range(3)]

        time.sleep(_KILL_DELAY_S)
        try:
            faults.kill_client_container(victim_name, sig="KILL")
        except RuntimeError as exc:
            import warnings
            warnings.warn(f"kill_client_container for victim m0 failed: {exc}")

        for f in concurrent.futures.as_completed(futs, timeout=700):
            f.result()

    all_results = GroupResults(
        produce_results=prod,
        member_results=[result_holder[i] for i in range(3)],
    )
    assert_group_single_owner(all_results)
    assert_group_no_committed_loss(all_results)
    assert_group_bounded_duplicates(all_results)
    assert_single_leader(cluster)
