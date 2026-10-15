"""CG-6: Consumer-group rebalance scenarios.

Proves that consumer groups correctly assign partitions to members and that the
single_owner and no_committed_loss invariants hold across the full lifecycle:

  Phase 1 (stable):  M members join → partitions assigned one-per-member →
                     single_owner + union(consumed) == produced.
  Phase 2 (leave):   One member stops → remaining members get reassigned →
                     still single_owner + no committed loss (the orphaned
                     partitions are handed to the surviving members).

All tests use the "cg" topic (topicId=2, 4 partitions) configured in the
topics Cluster.up creates by DDL (DEFAULT_TOPICS: events:1, multi:3, cg:4).

Parametrized matrix (per the brief: members ∈ {2, 3, 4}):
  - test_cg_rebalance_stable:      members ∈ {2, 3, 4}
  - test_cg_rebalance_member_leave: members ∈ {2, 3, 4}

Small-subset validation: ``-k "members2"`` selects both lightest cases.

CID window: 2_000_000+ — fully disjoint from all prior DR tests.
"""

from __future__ import annotations

import concurrent.futures
import time

import pytest

from harness.driver import (
    GroupResults,
    run_group,
    _run_one_group_member,
)
from harness.invariants import (
    assert_group_bounded_duplicates,
    assert_group_no_committed_loss,
    assert_group_single_owner,
    assert_single_leader,
)

# CID window: 2_000_000+ (disjoint from all existing DR tests).
_CID_STRIDE = 5000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 2_000_000 + _run_counter["n"] * _CID_STRIDE


# "cg" topic: topicId=2, 4 partitions.  Must match the topics Cluster.up creates (DEFAULT_TOPICS).
_TOPIC = "cg:2:4"
_RECORDS = 80          # records to produce per test case
_SUBSCRIBE_MS = 40000  # member consume window (ms)
_SHORT_MS = 15000      # short window for phase-1 in member-leave test
_TIMEOUT_MS = 15000    # connection timeout (ms)


def _run_members(
    cluster,
    leader_svc: str,
    group_id: str,
    cid_base: int,
    n: int,
    subscribe_ms: int,
    port_base: int = 40800,
) -> list:
    """Launch n group-consumer containers concurrently and return their results."""
    cfg = {
        "group_id": group_id,
        "topics": _TOPIC,
        "subscribe_ms": subscribe_ms,
        "autocommit_ms": 800,
        "timeout_ms": _TIMEOUT_MS,
    }
    results = []
    budget_s = subscribe_ms // 1000 + 90
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, n)) as pool:
        futs = [
            pool.submit(
                _run_one_group_member,
                network=cluster.compose.network_name(),
                cid=cid_base + i,
                target_service=leader_svc,
                egress_port=port_base + i,
                cfg=cfg,
                timeout_s=budget_s,
            )
            for i in range(n)
        ]
        for f in concurrent.futures.as_completed(futs, timeout=budget_s + 30):
            results.append(f.result())
    results.sort(key=lambda m: m.cid)
    return results


@pytest.mark.fault
@pytest.mark.timeout(600)
@pytest.mark.parametrize("members", [2, 3, 4])
def test_cg_rebalance_stable(fresh_cluster, members):
    """All members join → single_owner + no_committed_loss in a stable generation.

    Uses run_group which:
      1. Produces _RECORDS records (acks=all) from a dedicated producer.
      2. Launches `members` group-consumer containers sharing the same group-id.
      3. Waits _SUBSCRIBE_MS for records to flow.

    Determinism: this is the STABLE case, so we assert a STRICT single-owner
    guarantee within ONE generation.  To get there deterministically we launch
    all members FIRST, let the group converge to its FINAL stable membership
    (``produce_after_members_ms``), and only THEN produce.  Every record is thus
    delivered inside a single assignment generation; the ramp-up rebalances
    happen while the log is empty, so there is no legitimate cross-generation
    re-delivery to reason about.  A REAL same-generation split would still be
    caught by the (now generation-aware) single_owner invariant.

    Asserts:
      - single_owner: no (partition, offset) delivered to two members in the
        same generation.
      - group_no_committed_loss: union of consumed == produced.
      - bounded_duplicates: no runaway re-delivery.
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)

    _, leader_svc = cluster.identify_leader()
    cid_base = _next_cid_base()
    group_id = f"cg-stable-{members}m"

    # Stabilization budget before producing: cover the member-join stagger
    # (start_stagger_ms per extra member) plus join/sync round-trips so the group
    # is at its FINAL membership before any record exists.
    stabilize_ms = 8000

    gr = run_group(
        members,
        network=cluster.compose.network_name(),
        target_service=leader_svc,
        group_id=group_id,
        topics=_TOPIC,
        records=_RECORDS,
        keys=8,          # spread across 4 partitions
        partitions=4,
        cid_base=cid_base,
        subscribe_ms=_SUBSCRIBE_MS,
        autocommit_ms=1000,
        timeout_ms=_TIMEOUT_MS,
        timeout_s=300,
        produce_after_members_ms=stabilize_ms,
    )

    assert gr.produce_results.ok, (
        "producer failed:\n" + "\n".join(
            f"[cid={c.cid} rc={c.rc}]\n{c.raw[-800:]}"
            for c in gr.produce_results.clients
            if c.rc != 0 or c.exit_code != 0
        )
    )
    produced_count = len(gr.all_produced)
    assert produced_count == _RECORDS, f"produced {produced_count} != {_RECORDS}"

    assert_group_single_owner(gr)
    assert_group_no_committed_loss(gr)
    assert_group_bounded_duplicates(gr)
    assert_single_leader(cluster)


@pytest.mark.fault
@pytest.mark.timeout(700)
@pytest.mark.parametrize("members", [2, 3, 4])
def test_cg_rebalance_member_leave(fresh_cluster, members):
    """One member stops mid-consume → reassignment → still single_owner + no loss.

    Phase 1: M members consume for _SHORT_MS (offsets committed).
    Phase 2: (M-1) surviving members join the same group for _SUBSCRIBE_MS
             (coordinator sees one fewer member → rebalances → survivors get
             the departed member's partitions).

    The surviving members must cover the full produced set.
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)

    _, leader_svc = cluster.identify_leader()
    cid_base = _next_cid_base()
    group_id = f"cg-leave-{members}m"

    # Phase 1: produce once; M members consume for a short window.
    gr1 = run_group(
        members,
        network=cluster.compose.network_name(),
        target_service=leader_svc,
        group_id=group_id,
        topics=_TOPIC,
        records=_RECORDS,
        keys=8,
        partitions=4,
        cid_base=cid_base,
        subscribe_ms=_SHORT_MS,
        autocommit_ms=800,
        timeout_ms=_TIMEOUT_MS,
        timeout_s=240,
    )

    assert gr1.produce_results.ok, (
        "phase-1 producer failed:\n" + "\n".join(
            f"[cid={c.cid} rc={c.rc}]\n{c.raw[-600:]}"
            for c in gr1.produce_results.clients
            if c.rc != 0 or c.exit_code != 0
        )
    )

    # Brief gap so phase-1 members fully exit (triggers session expiry / rebalance).
    time.sleep(3.0)   # KEPT: scenario - phase-1 members must have exited (session expiry) before phase 2

    # Phase 2: (M-1) surviving members re-join — coordinator rebalances all
    # partitions to the smaller group.
    surviving = max(1, members - 1)
    phase2_cid_base = cid_base + 100  # distinct cid range
    p2_members = _run_members(
        cluster, leader_svc, group_id,
        cid_base=phase2_cid_base,
        n=surviving,
        subscribe_ms=_SUBSCRIBE_MS,
        port_base=40820,
    )

    gr2 = GroupResults(
        produce_results=gr1.produce_results,  # same produced set
        member_results=p2_members,
    )

    # no_committed_loss: union of BOTH phases must cover the produced set.
    # Phase-1 members consume and commit; phase-2 picks up from committed offsets.
    # Together they must cover every produced record.
    gr_combined = GroupResults(
        produce_results=gr1.produce_results,
        member_results=gr1.member_results + p2_members,
    )

    assert_group_single_owner(gr2)
    assert_group_no_committed_loss(gr_combined)
    assert_group_bounded_duplicates(gr2)
    assert_single_leader(cluster)


@pytest.mark.fault
@pytest.mark.timeout(600)
def test_cg_members_on_a_follower_find_the_coordinator(fresh_cluster):
    """D4 (backlog) "group subscribe redirect": members bootstrapped against a
    FOLLOWER (not the group coordinator / cluster leader).  The follower answers
    their coordinator requests ErrorResponse{NotCoordinator, leader endpoint};
    the client moves its seed session to the coordinator, re-joins there and
    follows each partition's leader (Subscribe Redirect), then consumes
    everything exactly as on the leader.  The producer also targets the follower
    (produce redirect).  Before the fix: 0 of 80 consumed.  (The join latency
    bound lives in the client's in-process GroupRoutingRegression.
    MemberOnAFollowerJoinsTheCoordinatorRegression.)"""
    cluster = fresh_cluster
    assert_single_leader(cluster)
    follower_svc = cluster.follower_service()
    _, leader_svc = cluster.identify_leader()
    assert follower_svc != leader_svc

    gr = run_group(
        2,
        network=cluster.compose.network_name(),
        target_service=follower_svc,
        group_id="cg-on-follower",
        topics=_TOPIC,
        records=_RECORDS,
        keys=8,
        partitions=4,
        cid_base=_next_cid_base(),
        subscribe_ms=_SUBSCRIBE_MS,
        autocommit_ms=1000,
        timeout_ms=_TIMEOUT_MS,
        timeout_s=300,
        produce_after_members_ms=8000,
    )
    assert gr.produce_results.ok, (
        "producer via follower failed:\n" + "\n".join(
            f"[cid={c.cid} rc={c.rc}]\n{c.raw[-800:]}"
            for c in gr.produce_results.clients if c.rc != 0 or c.exit_code != 0))
    assert len(gr.all_produced) == _RECORDS
    assert_group_single_owner(gr)
    assert_group_no_committed_loss(gr)
    assert_group_bounded_duplicates(gr)
    assert_single_leader(cluster)
