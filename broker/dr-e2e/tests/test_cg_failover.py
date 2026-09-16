"""CG-6: Consumer-group coordinator (leader) failover scenarios.

Verifies that committed offsets survive a coordinator failover (the coordinator
IS the Raft leader — killing it forces re-election and the new leader becomes
the new group coordinator):

  1. Produce + group-consume for a short window (offsets get committed).
  2. SIGKILL the coordinator / cluster leader.
  3. New leader elected → members rejoin new coordinator.
  4. Members resume consuming from the committed offset (not 0).

Invariants checked:
  - offset_resume: after failover, members resume from committed offset.
  - group_no_committed_loss: union of consumed across both phases == produced.
  - bounded_duplicates: re-delivery starts at committed offset, not from 0.
  - single_owner: no partition served to two members simultaneously.
  - assert_single_leader: no split-brain.

Parametrized:
  - test_cg_failover_leader_kill: baseline (no netem).
  - test_cg_failover_leader_kill_netem: leader-kill + 10% follower packet loss
    (mirrors DR-8 S1 test; marked xfail on the same DR-BUG-6 grounds if it
    fails the committed-loss check under combined fault, but offset_resume and
    single_owner are still asserted).

CID window: 2_100_000+ — within CG-6's 2_000_000+ window.
"""

from __future__ import annotations

import concurrent.futures
import signal
import time

import pytest

from harness import faults
from harness.cluster import node_id_to_service
from harness.driver import (
    GroupResults,
    GroupMemberResult,
    Results,
    run_clients,
    run_group,
    _run_one_group_member,
)
from harness.invariants import (
    assert_group_bounded_duplicates,
    assert_group_no_committed_loss,
    assert_group_offset_resume,
    assert_group_single_owner,
    assert_single_leader,
)

_CID_STRIDE = 5000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 2_100_000 + _run_counter["n"] * _CID_STRIDE


_RECORDS = 80
_SHORT_MS = 12000   # consume window before kill (let offsets get committed)
_LONG_MS = 40000    # consume window after failover
_TIMEOUT_MS = 20000
# "cg" topic: topicId=2, 4 partitions.  Must match broker RUNE_TOPICS="events:1,multi:3,cg:4".
_TOPIC = "cg:2:4"
_GROUP_ID = "cg-failover-group"


def _run_members(
    cluster,
    leader_svc: str,
    cid_base: int,
    n_members: int,
    subscribe_ms: int,
    port_base: int = 40700,
) -> list[GroupMemberResult]:
    """Launch n_members group-consumer containers concurrently."""
    results: list[GroupMemberResult] = []
    cfg = {
        "group_id": _GROUP_ID,
        "topics": _TOPIC,
        "subscribe_ms": subscribe_ms,
        "autocommit_ms": 1000,
        "timeout_ms": _TIMEOUT_MS,
    }
    with concurrent.futures.ThreadPoolExecutor(max_workers=n_members) as pool:
        futs = [
            pool.submit(
                _run_one_group_member,
                network=cluster.compose.network_name(),
                cid=cid_base + i,
                target_service=leader_svc,
                egress_port=port_base + i,
                cfg=cfg,
                timeout_s=300,
            )
            for i in range(n_members)
        ]
        for f in concurrent.futures.as_completed(futs, timeout=330):
            results.append(f.result())
    results.sort(key=lambda m: m.cid)
    return results


@pytest.mark.fault
@pytest.mark.timeout(700)
def test_cg_failover_leader_kill(fresh_cluster):
    """Committed offsets survive coordinator (leader) SIGKILL.

    Steps:
      1. Produce _RECORDS records (acks=all).
      2. Start 2 group-member containers for _SHORT_MS (offsets committed ≥ once).
      3. SIGKILL the cluster leader (= group coordinator).
      4. Wait for new leader to elect.
      5. Start 2 fresh group-member containers for _LONG_MS.
      6. Assert:
           - offset_resume (after-members start near committed offset, not 0)
           - group_no_committed_loss (union covers all produced)
           - bounded_duplicates
           - single_owner (no simultaneous dual ownership)
           - assert_single_leader (no split-brain)
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)
    lid, leader_svc = cluster.identify_leader()

    cid_base = _next_cid_base()

    # Phase 1: produce.
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
    assert prod.ok, (
        "pre-failover produce failed:\n" + prod.clients[0].raw[-800:]
    )
    assert len(prod.all_produced) == _RECORDS

    # Phase 1b: consume for _SHORT_MS to commit offsets.
    members_before = _run_members(
        cluster, leader_svc, cid_base + 1, n_members=2,
        subscribe_ms=_SHORT_MS, port_base=40700,
    )
    gr_before = GroupResults(
        produce_results=prod,
        member_results=members_before,
    )

    # Phase 2: kill the coordinator (cluster leader).
    faults.kill_node(cluster, leader_svc, signal.SIGKILL)

    # Wait for new leader.
    new_lid = cluster.wait_for_new_leader(leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != leader_svc, "leader did not change after kill"

    # Brief settle for the new coordinator to be ready.
    time.sleep(3.0)

    # Phase 3: run members against the new coordinator.
    cid_base2 = cid_base + 50
    members_after = _run_members(
        cluster, new_leader_svc, cid_base2, n_members=2,
        subscribe_ms=_LONG_MS, port_base=40710,
    )
    gr_after = GroupResults(
        produce_results=prod,
        member_results=members_after,
    )

    # Invariants.
    assert_single_leader(cluster)
    assert_group_single_owner(gr_before)
    assert_group_single_owner(gr_after)

    # no_committed_loss: union of BOTH phases must cover all produced records.
    # Phase-1 members consume and commit; after failover phase-2 resumes from
    # committed offsets.  Together they cover the full produced set.
    gr_combined = GroupResults(
        produce_results=prod,
        member_results=gr_before.member_results + members_after,
    )
    assert_group_no_committed_loss(gr_combined)
    assert_group_bounded_duplicates(gr_after)
    assert_group_offset_resume(gr_before, gr_after)


@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(700)
def test_cg_failover_leader_kill_netem(fresh_cluster, netem_ready):
    """Leader-kill + follower 10% packet loss: offset_resume and single_owner hold.

    Mirrors DR-8 S1 pattern: apply netem loss to one follower, then SIGKILL the
    leader.  The combined fault (kill + loss) is the known DR-BUG-6 condition
    for committed-loss on acks=All in the base subscribe path.  For group
    consumers the assert_group_no_committed_loss check is marked xfail via the
    strict=False marker so a single combined-fault failure does not block the
    full CG-6 suite.  offset_resume and single_owner are always hard-asserted.
    """
    cluster = fresh_cluster
    assert_single_leader(cluster)
    lid, leader_svc = cluster.identify_leader()

    # Find a follower to apply netem loss.
    follower_svc = cluster.follower_service()

    cid_base = _next_cid_base()

    # Produce before fault.
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
    assert prod.ok, "pre-netem produce failed:\n" + prod.clients[0].raw[-800:]
    assert len(prod.all_produced) == _RECORDS

    # Apply netem on follower, consume for a short window, then kill leader.
    faults.netem(cluster, follower_svc, loss=10)

    members_before = _run_members(
        cluster, leader_svc, cid_base + 1, n_members=2,
        subscribe_ms=_SHORT_MS, port_base=40720,
    )
    gr_before = GroupResults(produce_results=prod, member_results=members_before)

    faults.kill_node(cluster, leader_svc, signal.SIGKILL)
    new_lid = cluster.wait_for_new_leader(leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    faults.clear_netem(cluster, follower_svc)

    time.sleep(3.0)

    cid_base2 = cid_base + 50
    members_after = _run_members(
        cluster, new_leader_svc, cid_base2, n_members=2,
        subscribe_ms=_LONG_MS, port_base=40730,
    )
    gr_after = GroupResults(produce_results=prod, member_results=members_after)

    assert_single_leader(cluster)
    # Hard invariants — always must hold.
    assert_group_single_owner(gr_before)
    assert_group_single_owner(gr_after)
    assert_group_bounded_duplicates(gr_after)
    assert_group_offset_resume(gr_before, gr_after)

    # Soft invariant — may fail on DR-BUG-6 (combined kill+loss can lose
    # committed tail on the base replication path).  Mark xfail consistent
    # with the existing test_dr8_leader_kill_netem xfail.
    # Union of both phases must cover all produced records.
    gr_combined_netem = GroupResults(
        produce_results=prod,
        member_results=gr_before.member_results + members_after,
    )
    try:
        assert_group_no_committed_loss(gr_combined_netem)
    except AssertionError as exc:
        pytest.xfail(
            f"group-no-committed-loss under combined kill+netem (DR-BUG-6): {exc}"
        )
