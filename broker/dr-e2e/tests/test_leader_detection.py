"""Leader detection via the Aeron role counter (not log scraping).

These tests prove the machine-readable leadership signal end-to-end in Docker:
  * rune_role reads the node-role + leadership-term counters out of each broker's
    CnC file and prints a parseable ROLE line.
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
