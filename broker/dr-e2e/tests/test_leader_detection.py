"""Leader detection via the node-role metric (not log scraping).

These tests prove the machine-readable leadership signal end-to-end in Docker:
  * rune_role reads the node-role + leadership-term metrics out of each broker's
    metrics file (RUNE_DATA_DIR/metrics/rune-metrics.dat) and prints a parseable ROLE line.
  * The harness (cluster._read_role / _scan_leader / identify_leader /
    assert_single_leader) detects the leader from those counters.
  * Detection is INDEPENDENT of RUNE_LOG_LEVEL — the regression that motivated
    this work (setting the level to "warning" suppressed the "is LEADER" INFO
    line and broke the old log-scraping detector).

They are fast (single fresh cluster, no fault injection).
"""

from __future__ import annotations

import pytest

from harness.cluster import SERVICES, node_id_to_service
from harness.invariants import assert_single_leader


@pytest.mark.timeout(200)
def test_rune_role_reports_roles_for_all_nodes(fresh_cluster):
    """Every running node answers rune_role with a valid role; exactly one LEADER."""
    cluster = fresh_cluster
    # Ensure a leader exists (counter-based wait).
    lid, leader_svc = cluster.identify_leader(timeout_s=120)
    assert leader_svc in SERVICES

    roles: dict[str, tuple[str, int, int]] = {}
    for svc in cluster.running_services():
        reading = cluster._read_role(svc)
        assert reading is not None, f"rune_role produced no reading for {svc}"
        role, node_id, term = reading
        assert role in ("LEADER", "FOLLOWER", "CANDIDATE", "NONE"), role
        assert node_id_to_service(node_id) == svc, (
            f"{svc} role counter reports node id {node_id} "
            f"(-> {node_id_to_service(node_id)}), expected {svc}"
        )
        roles[svc] = reading

    leaders = [svc for svc, (role, _n, _t) in roles.items() if role == "LEADER"]
    assert len(leaders) == 1, f"expected exactly one LEADER, got {leaders} ({roles})"
    assert leaders[0] == leader_svc, (
        f"identify_leader said {leader_svc} but rune_role says {leaders[0]}"
    )
    # The leader's counter must report its own node id.
    lrole, lnode, lterm = roles[leader_svc]
    assert node_id_to_service(lnode) == leader_svc
    assert lterm >= 0, "leader should report a non-negative leadership term"


@pytest.mark.timeout(200)
def test_assert_single_leader_via_counter(fresh_cluster):
    """assert_single_leader agrees with identify_leader (both counter-based)."""
    cluster = fresh_cluster
    lid, leader_svc = cluster.identify_leader(timeout_s=120)
    agreed = assert_single_leader(cluster, timeout_s=60)
    assert agreed == lid, (
        f"assert_single_leader returned {agreed}, identify_leader returned {lid}"
    )


@pytest.mark.timeout(240)
def test_leader_detection_independent_of_log_level(fresh_cluster):
    """Detection must NOT depend on RUNE_LOG_LEVEL.

    The bring-up already succeeded (fresh_cluster waits for a leader via the
    counter).  We additionally assert that the "is LEADER" log line is NOT what
    detection relies on: even if we ignore logs entirely, rune_role still yields
    the leader.  This is the direct regression guard for the warning-level break.
    """
    cluster = fresh_cluster
    lid, leader_svc = cluster.identify_leader(timeout_s=120)

    # Detection path uses only the counter (docker exec rune_role); confirm it
    # returns a stable leader without consulting compose.logs at all.
    scanned = cluster._scan_leader(list(cluster.running_services()))
    assert scanned == lid, f"_scan_leader (counter) = {scanned}, expected {lid}"

    # Sanity: the counter reading for the leader is LEADER regardless of logs.
    reading = cluster._read_role(leader_svc)
    assert reading is not None
    assert reading[0] == "LEADER"


def test_rune_role_two_line_output_parses():
    """ISR v3: rune_role prints a second STORAGE line; the ROLE parse is unchanged by it (no cluster needed)."""
    from harness.cluster import _ROLE_RE, parse_rune_role_storage

    out = (
        "ROLE nodeId=1 role=FOLLOWER term=7\n"
        "STORAGE nodeId=1 state=UNCONFIRMED heldLeaderRoles=3 pendingNodes=0 outOfSync=2 lostCommittedReports=1\n"
    )
    role = _ROLE_RE.search(out)
    assert role and (role.group(1), role.group(2), role.group(3)) == ("1", "FOLLOWER", "7")
    st = parse_rune_role_storage(out)
    assert st == {
        "node_id": 1, "state": "UNCONFIRMED", "held_leader_roles": 3, "pending_nodes": 0,
        "out_of_sync": 2, "lost_committed_reports": 1,
    }
    # An absent counter: UNKNOWN state and -1 values; an older image: no STORAGE line at all.
    unk = parse_rune_role_storage(
        "ROLE nodeId=0 role=NONE term=-1\n"
        "STORAGE nodeId=0 state=UNKNOWN heldLeaderRoles=-1 pendingNodes=-1 outOfSync=-1 lostCommittedReports=-1\n")
    assert unk and unk["state"] == "UNKNOWN" and unk["held_leader_roles"] == -1
    assert parse_rune_role_storage("ROLE nodeId=0 role=NONE term=-1\n") is None
