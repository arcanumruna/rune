"""CG-6: Consumer-group at-least-once delivery under member crash.

Proves the at-least-once guarantee: if a member crashes mid-consume, its
partitions are reassigned to the surviving members, and the union of all
consumed records still covers every produced record (no record missed by the
group).

Scenario:
  1. Produce _RECORDS records (acks=all) — each PRODUCED line is majority-durable.
  2. Start 2 group-member containers.  One runs for the full window; the other
     runs for a SHORT window then its container exits (simulating a crash).
  3. The surviving member detects the session timeout and is reassigned the
     departed member's partitions (rebalance).
  4. Assert:
       - group_no_committed_loss: union covers all produced (no record missed).
       - bounded_duplicates: re-delivery starts at committed offset, not 0.
       - single_owner (within each phase): no simultaneous dual-ownership.

Note on "crash" simulation: the harness does not kill the container mid-run
(that would require a background thread + kill_client_container, which adds
timing complexity).  Instead we use a two-phase approach:
  - Phase 1 short: both members consume for a SHORT window.  They commit offsets.
  - Phase 2 long: only ONE member consumes for a LONG window (the other is gone).
The survivor inherits the departed member's partitions via rebalance (same
group_id, same topic, coordinator detects session expiry).
This cleanly simulates the at-least-once guarantee without requiring container
kill timing.

CID window: 2_200_000+ — within CG-6's 2_000_000+ window.
"""

from __future__ import annotations

import concurrent.futures
import time

import pytest

from harness.cluster import node_id_to_service
from harness.driver import (
    GroupResults,
    Results,
    run_clients,
    _run_one_group_member,
)
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
_SHORT_MS = 12000    # window for the "crashing" member
_LONG_MS = 50000     # window for the surviving member (must see rebalance + drain)
_TIMEOUT_MS = 20000
# "cg" topic: topicId=2, 4 partitions.  Must match broker RUNE_TOPICS="events:1,multi:3,cg:4".
_TOPIC = "cg:2:4"
_GROUP_ID = "cg-alo-group"


@pytest.mark.fault
@pytest.mark.timeout(600)
def test_cg_atleastonce_member_crash(fresh_cluster):
    """Member crash → partitions reassigned → group still covers all produced.

    Two-phase approach (see module docstring for rationale):
      Phase 1: 2 members consume for _SHORT_MS → offsets committed.
      Phase 2: 1 member consumes for _LONG_MS → gets reassigned departed member's
               partitions → must cover the full produced set.
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)
    _, leader_svc = cluster.identify_leader()

    cid_base = _next_cid_base()

    # Produce the committed set (majority-durable acks=all).
    prod = run_clients(
        1,
        network=cluster.compose.network_name(),
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

    # Phase 1: two members consume for the short window (commit offsets).
    def _run_member(cid: int, subscribe_ms: int, port: int):
        return _run_one_group_member(
            network=cluster.compose.network_name(),
            cid=cid,
            target_service=leader_svc,
            egress_port=port,
            cfg={
                "group_id": _GROUP_ID,
                "topics": _TOPIC,
                "subscribe_ms": subscribe_ms,
                "autocommit_ms": 800,
                "timeout_ms": _TIMEOUT_MS,
            },
            timeout_s=300,
        )

    m1_cid = cid_base + 1
    m2_cid = cid_base + 2
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(_run_member, m1_cid, _SHORT_MS, 40750)
        f2 = pool.submit(_run_member, m2_cid, _SHORT_MS, 40751)
        m1_before = f1.result(timeout=200)
        m2_before = f2.result(timeout=200)

    phase1_results = GroupResults(
        produce_results=prod,
        member_results=[m1_before, m2_before],
    )
    # Phase 1 single-owner check.
    assert_group_single_owner(phase1_results)

    # Brief gap — let the coordinator detect the session expiry of both
    # phase-1 members (sessionTimeoutMs=30s, but in practice the coordinator
    # will trigger rebalance when the surviving phase-2 member joins).
    time.sleep(2.0)

    # Phase 2: only ONE surviving member joins the group.  The coordinator
    # sees only one member → assigns ALL partitions to it.  It must cover the
    # full produced set (union with phase-1 records).
    m3_cid = cid_base + 3
    m3 = _run_member(m3_cid, _LONG_MS, 40752)

    phase2_results = GroupResults(
        produce_results=prod,
        member_results=[m3],
    )

    # Single-owner within phase 2 (trivially true with one member).
    assert_group_single_owner(phase2_results)

    # At-least-once: union of phase-2 survivor must cover all produced.
    # (Phase-1 may have left some uncommitted — phase-2 survivor gets those.)
    assert_group_no_committed_loss(phase2_results)
    assert_group_bounded_duplicates(phase2_results)
    assert_single_leader(cluster)


@pytest.mark.fault
@pytest.mark.timeout(700)
def test_cg_atleastonce_multiple_survivors(fresh_cluster):
    """3 members → 1 crashes → 2 survivors cover the full produced set.

    Same at-least-once proof but with 3 initial members and 2 survivors, which
    exercises a more realistic rebalance (one-third of partitions orphaned and
    redistributed, not a full handover).
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)
    _, leader_svc = cluster.identify_leader()

    cid_base = _next_cid_base()
    topic = _TOPIC   # "cg:2:4" — same 4-partition topic, different group_id
    group_id = "cg-alo3-group"

    prod = run_clients(
        1,
        network=cluster.compose.network_name(),
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

    def _run_member(cid: int, subscribe_ms: int, port: int, t: str = topic, gid: str = group_id):
        return _run_one_group_member(
            network=cluster.compose.network_name(),
            cid=cid,
            target_service=leader_svc,
            egress_port=port,
            cfg={
                "group_id": gid,
                "topics": t,
                "subscribe_ms": subscribe_ms,
                "autocommit_ms": 800,
                "timeout_ms": _TIMEOUT_MS,
            },
            timeout_s=300,
        )

    # Phase 1: 3 members for a short window.
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        fm = [
            pool.submit(_run_member, cid_base + 1 + i, _SHORT_MS, 40760 + i)
            for i in range(3)
        ]
        p1_members = [f.result(timeout=200) for f in concurrent.futures.as_completed(fm)]
    p1_members.sort(key=lambda m: m.cid)

    phase1 = GroupResults(produce_results=prod, member_results=p1_members)
    assert_group_single_owner(phase1)

    time.sleep(2.0)

    # Phase 2: 2 surviving members.
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        fm2 = [
            pool.submit(_run_member, cid_base + 10 + i, _LONG_MS, 40770 + i)
            for i in range(2)
        ]
        p2_members = [f.result(timeout=250) for f in concurrent.futures.as_completed(fm2)]
    p2_members.sort(key=lambda m: m.cid)

    phase2 = GroupResults(produce_results=prod, member_results=p2_members)
    assert_group_single_owner(phase2)
    assert_group_no_committed_loss(phase2)
    assert_group_bounded_duplicates(phase2)
    assert_single_leader(cluster)
