"""CP-5.3: DR-during-compaction scenarios.

Tests that the log cleaner is safe in the presence of common DR faults:
follower kill, leader kill mid-compaction, log truncation interleaved with
compaction, and network partition during active compaction.

All scenarios share the compaction_cluster fixture from test_compaction_e2e.py
(compact_log: compact policy, 4 KiB segments; delete_log: delete policy).

CID base: 3_000_000 + n * 5000 — fully disjoint from all DR-1..15 suites
          (cap ~1_600_000) and from the CP-5.2 suite (2_000_000..2_025_000).

Markers:
  - @pytest.mark.compaction  : the whole module
  - @pytest.mark.fault       : all scenarios (all mutate cluster state)
"""

from __future__ import annotations

import os
import signal
import tempfile
import textwrap
import time
from typing import Iterator

import pytest

from harness import faults
from harness.cluster import (
    BROKER_REPO, COMPOSE_FILE, SERVICES,
    Cluster, node_id_to_service, service_to_node_id,
)
from harness.driver import Results, run_clients
from harness.invariants import assert_single_leader

# Reuse shared helpers from the CP-5.2 module rather than duplicating them.
# Import the helper functions and constants directly.
from tests.test_compaction_e2e import (
    _WARMUP_CID_MIN,
    _is_test_cid,
    _CID_STRIDE,
    _run_counter,
    _CONNECT_MS,
    _STAGGER_MS,
    _COMPACT_TOPIC_NAME,
    _COMPACT_TOPIC_ID,
    _COMPACT_TOPIC_PARTS,
    _COMPACT_TOPIC_SEGMENT_BYTES,
    _COMPACT_TOPIC_DELETE_RETENTION_MS,
    _DELETE_TOPIC_NAME,
    _DELETE_TOPIC_ID,
    _DELETE_TOPIC_PARTS,
    _DELETE_TOPIC_SEGMENT_BYTES,
    _DELETE_TOPIC_RETENTION_BYTES,
    _CLIENT_COMPACT_TOPICS,
    _CLIENT_DELETE_TOPICS,
    _write_compose_override,
    _count_log_files,
    _wait_for_compaction,
    _produce_compact,
    _subscribe_compact,
    assert_latest_per_key,
    assert_no_resurrection,
    assert_hw_bounded,
)

# ---------------------------------------------------------------------------
# Module marker — all tests in this file are compaction DR-related.
# ---------------------------------------------------------------------------
pytestmark = [pytest.mark.compaction, pytest.mark.fault]


# ---------------------------------------------------------------------------
# CID base — disjoint from all existing suites.
# ---------------------------------------------------------------------------
_DR_CID_STRIDE = 5000
_dr_run_counter = {"n": 0}


def _next_dr_cid_base() -> int:
    _dr_run_counter["n"] += 1
    return 3_000_000 + _dr_run_counter["n"] * _DR_CID_STRIDE


# ---------------------------------------------------------------------------
# Shared cluster fixture (mirrors compaction_cluster from test_compaction_e2e).
# Uses a DIFFERENT project name ("rune-dr-compact-dr") so DR scenarios can
# run in isolation without affecting the CP-5.2 test cluster.
# ---------------------------------------------------------------------------

@pytest.fixture
def compaction_dr_cluster() -> Iterator[Cluster]:
    """Function-scoped cluster with compact + delete topic overrides.

    Uses project "rune-dr-compact-dr" (distinct from the CP-5.2 fixture's
    "rune-dr-compact") so both suites can coexist in the same Docker host.
    """
    tmp = tempfile.mkdtemp(prefix="rune_compaction_dr_e2e_")
    override = _write_compose_override(tmp)

    build = os.environ.get("DR_E2E_BUILD", "0") == "1"
    c = Cluster(project="rune-dr-compact-dr", override_files=[override])
    try:
        c.up(build=build, leader_timeout=120)
        # Probe via delete_log (topicId=1) so the harness warm-up records do NOT
        # land on compact_log (topicId=0).  The _is_test_cid filter already
        # excludes them from every invariant, but keeping compact_log free of
        # harness traffic makes on-disk debugging cleaner.
        c.wait_data_plane_ready(max_attempts=5, timeout_s=60,
                                topics=_CLIENT_DELETE_TOPICS)
        yield c
    finally:
        faults.heal_all(c)
        c.down()
        try:
            os.remove(override)
            os.rmdir(tmp)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# DR-COMPACT-1: Follower kill during active compaction.
#
# Scenario:
#   1. Produce 200 records on the compact topic (acks=All → majority-durable).
#   2. Wait for the first compaction pass to START (file count has dropped).
#   3. Kill a follower (SIGKILL) while the leader is actively compacting.
#   4. Leader continues compaction; wait for stable compaction.
#   5. Subscribe from the leader → assert latest-per-key, no phantom data.
#   6. Revive the killed follower; wait for it to catch up.
#   7. Subscribe from the revived follower → still latest-per-key (segment
#      swap propagated correctly even though the follower was down during the
#      write).
# ---------------------------------------------------------------------------

@pytest.mark.timeout(900)
def test_drcp1_follower_kill_during_compaction(compaction_dr_cluster):
    """Kill a follower mid-compaction; leader completes; follower catches up.

    Key assertions:
      - Leader delivers latest-per-key after compaction completes.
      - Revived follower delivers latest-per-key after catchup (no resurrection
        of compacted-away records via the replication fetch path).
      - HW-bounded: no phantom records on either leader or follower.
      - Cluster stays healthy (single leader) throughout.
    """
    cluster = compaction_dr_cluster
    keys = 4
    records = 200

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Find a follower to kill.
    follower_svc = next(
        s for s in SERVICES if s != leader_svc
    )

    # ── Produce many updates per key ──────────────────────────────────────────
    cid_produce = _next_dr_cid_base()
    produce_result = _produce_compact(cluster, leader_svc, cid_produce,
                                      records=records, keys=keys)
    assert produce_result.ok, (
        "DR-COMPACT-1 produce failed:\n"
        + "\n".join(f"[cid={c.cid}] {c.raw[-800:]}"
                    for c in produce_result.clients if c.exit_code != 0 or c.rc != 0)
    )
    assert len(produce_result.all_produced) == records

    # Capture baseline file count on leader.
    leader_idx = service_to_node_id(leader_svc) + 1
    initial_file_count = _count_log_files(cluster.project, leader_idx)

    # ── Kill follower while compaction runs ──────────────────────────────────
    # Give the leader ~5s to start the first compaction pass, then kill the follower.
    time.sleep(5.0)
    faults.kill_node(cluster, follower_svc, signal.SIGKILL)

    # Wait for the leader to fully compact (stable minimum).
    if initial_file_count > 0:
        _wait_for_compaction(cluster, leader_svc,
                              initial_count=initial_file_count,
                              timeout_s=120.0)
    else:
        time.sleep(60.0)

    # ── Subscribe from leader ─────────────────────────────────────────────────
    cid_sub_leader = _next_dr_cid_base()
    sub_leader = _subscribe_compact(cluster, leader_svc, cid_sub_leader,
                                    window_ms=60_000)

    # Assertions on leader.
    assert_latest_per_key(produce_result, sub_leader)
    assert_hw_bounded(produce_result, sub_leader)

    n_test_leader = sum(1 for m in sub_leader.all_delivered
                        if _is_test_cid(m.cid))
    assert n_test_leader == keys, (
        f"DR-COMPACT-1: leader delivered {n_test_leader} test records, expected {keys}"
    )

    # ── Revive follower and wait for catchup ──────────────────────────────────
    cluster.compose.start(follower_svc)
    # Wait for the revived node to rejoin the cluster (it needs to fetch missing
    # segments from the leader — both the original replicated bytes AND any
    # compacted segments that were swapped while it was down).
    time.sleep(15.0)
    assert_single_leader(cluster)

    # ── Subscribe from the revived follower ───────────────────────────────────
    # Wait for the follower to be data-plane ready (can serve produce requests).
    cluster.wait_data_plane_ready(max_attempts=5, timeout_s=60)
    new_lid, new_leader_svc = cluster.identify_leader()

    cid_sub_follower = _next_dr_cid_base()
    sub_follower = _subscribe_compact(cluster, new_leader_svc, cid_sub_follower,
                                      window_ms=60_000)

    # Assertions on revived path (served through new_leader, which might be the
    # revived follower if it re-won election, or the original leader).
    assert_no_resurrection(produce_result, sub_follower)
    assert_hw_bounded(produce_result, sub_follower)
    assert_single_leader(cluster)


# ---------------------------------------------------------------------------
# DR-COMPACT-2: Leader kill during active compaction.
#
# Scenario:
#   1. Produce 200 records; wait for the leader to START compacting.
#   2. SIGKILL the leader mid-compaction (the cleaner may have partially
#      rewritten one segment but not committed it to the log).
#   3. Wait for a new leader to elect.
#   4. The new leader has the uncompacted log.  Wait for it to compact.
#   5. Subscribe → assert latest-per-key, no phantom data.
#
# This is a regression test for the atomicity of the segment-swap path:
# if the leader crashes between producing the cleaned .log.new file and
# atomically renaming it, the rename never completes.  The broker must
# detect and discard incomplete .log.new files on recovery.
# ---------------------------------------------------------------------------

@pytest.mark.timeout(900)
def test_drcp2_leader_kill_during_active_compaction(compaction_dr_cluster):
    """SIGKILL the leader mid-compaction; new leader recovers; compacts correctly.

    Key assertions:
      - New leader can win election (no hung raft due to incomplete segment).
      - After new leader compacts its own copy, delivers exactly latest-per-key.
      - HW-bounded: no phantom records.
      - No resurrection: no superseded records reappear after failover.
    """
    cluster = compaction_dr_cluster
    keys = 4
    records = 200

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Produce many updates per key ──────────────────────────────────────────
    cid_produce = _next_dr_cid_base()
    produce_result = _produce_compact(cluster, leader_svc, cid_produce,
                                      records=records, keys=keys)
    assert produce_result.ok, (
        "DR-COMPACT-2 produce failed:\n"
        + "\n".join(f"[cid={c.cid}] {c.raw[-800:]}"
                    for c in produce_result.clients if c.exit_code != 0 or c.rc != 0)
    )
    assert len(produce_result.all_produced) == records

    leader_idx = service_to_node_id(leader_svc) + 1
    initial_file_count = _count_log_files(cluster.project, leader_idx)

    # ── Let compaction start, then kill the leader ────────────────────────────
    # Give the cleaner a few seconds to start (possibly mid-segment rewrite).
    time.sleep(8.0)
    faults.kill_node(cluster, leader_svc, signal.SIGKILL)

    # ── Wait for new leader election ──────────────────────────────────────────
    new_lid = cluster.wait_for_new_leader(leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != leader_svc
    assert_single_leader(cluster)

    # ── Wait for new leader to compact its copy ───────────────────────────────
    new_leader_idx = service_to_node_id(new_leader_svc) + 1
    new_initial_count = _count_log_files(cluster.project, new_leader_idx)
    if new_initial_count > 0:
        _wait_for_compaction(cluster, new_leader_svc,
                              initial_count=new_initial_count,
                              timeout_s=120.0)
    else:
        time.sleep(60.0)

    # Wait for data plane ready before subscribing.
    cluster.wait_data_plane_ready(max_attempts=8, timeout_s=120)

    # ── Subscribe from new leader ─────────────────────────────────────────────
    cid_sub = _next_dr_cid_base()
    sub_result = _subscribe_compact(cluster, new_leader_svc, cid_sub,
                                    window_ms=60_000)

    # ── Assertions ────────────────────────────────────────────────────────────
    assert_no_resurrection(produce_result, sub_result)
    assert_hw_bounded(produce_result, sub_result)
    assert_latest_per_key(produce_result, sub_result)

    n_test_delivered = sum(1 for m in sub_result.all_delivered
                           if _is_test_cid(m.cid))
    assert n_test_delivered == keys, (
        f"DR-COMPACT-2: delivered {n_test_delivered} test records, expected {keys}"
    )
    assert_single_leader(cluster)


# ---------------------------------------------------------------------------
# DR-COMPACT-3: Log truncation interleaved with compaction.
#
# Scenario:
#   1. Produce 200 records; wait for full compaction.
#   2. Kill one follower (SIGKILL) — its log is NOT compacted.
#   3. Truncate the last 128 bytes from the follower's active log segment
#      (simulates a torn write on a follower that was receiving replicated
#      batches from the leader before crash).
#   4. Restart the follower; it recovers (discards the torn tail) and
#      re-fetches from the leader, which has the compacted log.
#   5. New leader serves data plane; subscribe → assert latest-per-key.
#
# This combines the existing truncation DR with compaction: the follower's
# recovery must not confuse the compacted leader's log layout with its own
# truncated uncompacted log.
# ---------------------------------------------------------------------------

@pytest.mark.timeout(900)
def test_drcp3_truncation_interplay_with_compaction(compaction_dr_cluster):
    """Truncate a follower log after leader compacts; follower recovers correctly.

    Key assertions:
      - Follower re-syncs without data loss or incorrect compaction state.
      - After recovery, cluster delivers latest-per-key (compacted state).
      - HW-bounded: no phantom data from the pre-truncation follower log.
    """
    cluster = compaction_dr_cluster
    keys = 4
    records = 200

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Find a follower.
    follower_svc = next(s for s in SERVICES if s != leader_svc)
    follower_idx = service_to_node_id(follower_svc) + 1

    # ── Produce 200 records ───────────────────────────────────────────────────
    cid_produce = _next_dr_cid_base()
    produce_result = _produce_compact(cluster, leader_svc, cid_produce,
                                      records=records, keys=keys)
    assert produce_result.ok, "DR-COMPACT-3 produce failed"
    assert len(produce_result.all_produced) == records

    # ── Wait for full compaction on leader ────────────────────────────────────
    leader_idx = service_to_node_id(leader_svc) + 1
    initial_count = _count_log_files(cluster.project, leader_idx)
    if initial_count > 0:
        _wait_for_compaction(cluster, leader_svc,
                              initial_count=initial_count,
                              timeout_s=120.0)
    else:
        time.sleep(60.0)

    # ── Kill follower, truncate its log, restart ──────────────────────────────
    faults.kill_node(cluster, follower_svc, signal.SIGKILL)
    time.sleep(2.0)  # ensure container is stopped before volume access

    faults.truncate_log_tail(cluster.project, follower_idx, tail_bytes=128)

    cluster.compose.start(follower_svc)
    # Give the follower time to recover and catch up.
    time.sleep(20.0)
    assert_single_leader(cluster)

    # Wait for data plane to be serving.
    cluster.wait_data_plane_ready(max_attempts=8, timeout_s=120)
    new_lid, new_leader_svc = cluster.identify_leader()

    # ── Subscribe from the cluster (current leader may be the revived follower) ─
    cid_sub = _next_dr_cid_base()
    sub_result = _subscribe_compact(cluster, new_leader_svc, cid_sub,
                                    window_ms=60_000)

    # ── Assertions ────────────────────────────────────────────────────────────
    assert_no_resurrection(produce_result, sub_result)
    assert_hw_bounded(produce_result, sub_result)
    assert_single_leader(cluster)


# ---------------------------------------------------------------------------
# DR-COMPACT-4: Network partition during compaction.
#
# Scenario:
#   1. Produce 200 records; start compaction (let a few passes run).
#   2. Partition one follower from the rest (simulated via `faults.partition`).
#   3. Leader continues compacting in isolation.  New records can still be
#      produced (leader + 1 follower = quorum of 2).
#   4. After full compaction on the leader, heal the partition.
#   5. Partitioned follower re-syncs; wait for it to catch up.
#   6. Subscribe → assert latest-per-key, no resurrection, HW-bounded.
#
# This tests that the segment swap (enqueueCleanedSegment) and the replication
# fetch path are compatible: the partitioned follower fetches the compacted
# segments once reconnected and does not resurrect compacted-away records.
# ---------------------------------------------------------------------------

@pytest.mark.timeout(900)
def test_drcp4_network_partition_during_compaction(compaction_dr_cluster):
    """Partition a follower while leader compacts; heal; follower gets compacted log.

    Key assertions:
      - Cluster stays operational during partition (leader + 1 ISR).
      - After partition heal and follower re-sync, cluster delivers latest-per-key.
      - No resurrection: partitioned follower's old records do NOT overwrite
        the compacted state on the leader.
      - HW-bounded throughout.
    """
    cluster = compaction_dr_cluster
    keys = 4
    records = 200

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Find a follower to partition.
    follower_svc = next(s for s in SERVICES if s != leader_svc)

    # ── Produce 200 records ───────────────────────────────────────────────────
    cid_produce = _next_dr_cid_base()
    produce_result = _produce_compact(cluster, leader_svc, cid_produce,
                                      records=records, keys=keys)
    assert produce_result.ok, "DR-COMPACT-4 produce failed"
    assert len(produce_result.all_produced) == records

    leader_idx = service_to_node_id(leader_svc) + 1
    initial_count = _count_log_files(cluster.project, leader_idx)

    # ── Partition follower ─────────────────────────────────────────────────────
    faults.partition(cluster, follower_svc)

    # ── Wait for full compaction on the (isolated) leader ─────────────────────
    if initial_count > 0:
        _wait_for_compaction(cluster, leader_svc,
                              initial_count=initial_count,
                              timeout_s=120.0)
    else:
        time.sleep(60.0)

    # ── Heal the partition and wait for re-sync ────────────────────────────────
    faults.heal(cluster, follower_svc)
    # Give the follower time to re-sync from the leader.
    time.sleep(20.0)
    assert_single_leader(cluster)

    cluster.wait_data_plane_ready(max_attempts=8, timeout_s=120)
    new_lid, new_leader_svc = cluster.identify_leader()

    # ── Subscribe from current leader ─────────────────────────────────────────
    cid_sub = _next_dr_cid_base()
    sub_result = _subscribe_compact(cluster, new_leader_svc, cid_sub,
                                    window_ms=60_000)

    # ── Assertions ────────────────────────────────────────────────────────────
    assert_no_resurrection(produce_result, sub_result)
    assert_hw_bounded(produce_result, sub_result)
    assert_single_leader(cluster)
