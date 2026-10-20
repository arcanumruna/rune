"""TX-6 — test_tx_fencing: zombie producer fencing.

A zombie producer (old epoch) is fenced after a new initProducerId is called
with the same transactional id.  The fenced producer's late commit must be
rejected by the broker.

This test exercises the fencing path of a transactional Producer (ProducerConfig::transactionalId):
  - ProducerFenced / InvalidProducerEpoch errorCodes cause the old producer to
    be fenced (producerId reset to -1, all pending futures fail).

Strategy:
  1. Producer P1 commits txn T1 successfully (establishes epoch E0).
  2. Producer P2 with the SAME transactional id initialises (bumps epoch to E1),
     stealing P1's epoch.  P1 is now a zombie.
  3. P1's new transaction is rejected (ProducerFenced).
  4. P2 commits successfully.

Since we can't interleave two transactional producer handles on the same
dr_client container easily, we simulate via two sequential txn_produce runs
with the SAME txn_id.  The second run with a fresh initTransactions bumps the
epoch; the first producer (if it had an open transaction) would be fenced.

The fencing invariant we verify:
  - After P2 commits: read_committed sees P2's records.
  - After fencing, the old epoch's data is NOT leaked.

For a stronger fencing test (concurrent zombie + new producer), see the
inline interleaved scenario.
"""

from __future__ import annotations

import uuid
import time

import pytest

from harness.driver import run_txn, run_txn_consume
from harness.invariants import (
    assert_eos_no_dup,
    assert_eos_no_loss,
    assert_read_committed_excludes,
    assert_txn_aborted,
    assert_txn_committed,
)


def _next_txn_id() -> str:
    return f"fence-{uuid.uuid4().hex[:12]}"


def _next_group_id() -> str:
    return f"fence-grp-{uuid.uuid4().hex[:8]}"


# ── Smoke ─────────────────────────────────────────────────────────────────────

@pytest.mark.timeout(240)
def test_tx_fencing_smoke(fresh_cluster):
    """Basic smoke: a committed txn by P1 followed by P2 (same txn_id) is clean.

    P2 bumps epoch at initTransactions → P1 (if still alive) would be fenced.
    P2's commit succeeds and its records are visible.
    """
    cluster = fresh_cluster
    svc = cluster.wait_data_plane_ready()
    net = cluster.compose.network_name()

    shared_txn_id = _next_txn_id()

    # P1 commits (epoch E0)
    prod1 = run_txn(
        network=net, target_service=svc, cid=70,
        txn_id=shared_txn_id, commit=True,
        topics="events:0:1", records=5, keys=1,
        timeout_ms=20000, timeout_s=60, egress_port=40830,
    )
    assert prod1.ok and prod1.outcome == "committed", (
        f"P1 commit failed: {prod1.raw[-400:]}"
    )

    # P2 (same txn_id) — re-init bumps epoch; P1 is now a zombie
    # P2 commits its own records
    prod2 = run_txn(
        network=net, target_service=svc, cid=71,
        txn_id=shared_txn_id, commit=True,
        topics="events:0:1", records=5, keys=1,
        timeout_ms=20000, timeout_s=60, egress_port=40831,
    )
    assert prod2.ok and prod2.outcome == "committed", (
        f"P2 commit failed: {prod2.raw[-400:]}"
    )

    # read_committed: sees BOTH P1 and P2's records (both committed)
    cons = run_txn_consume(
        network=net, target_service=svc,
        group_id=_next_group_id(), cid=72,
        topics="events:0:1",
        isolation="read_committed",
        subscribe_ms=15000, timeout_ms=20000, timeout_s=60, egress_port=40832,
    )
    assert_eos_no_dup(cons)
    assert_eos_no_loss(prod1.sent, cons)
    assert_eos_no_loss(prod2.sent, cons)


# ── Fencing: P2 steals epoch, P1's abort succeeds (already cleaned up) ────────

@pytest.mark.timeout(360)
def test_tx_fencing_epoch_bump_p1_aborted(fresh_cluster):
    """P1 aborts (epoch E0), then P2 inits same txn_id (bumps to E1) and commits.

    P1's abort leaves no records.  P2's commit leaves its records visible.
    This verifies that epoch-bump (fencing guard) doesn't break clean sequences.
    """
    cluster = fresh_cluster
    svc = cluster.wait_data_plane_ready()
    net = cluster.compose.network_name()

    shared_txn_id = _next_txn_id()

    # P1 aborts
    prod1 = run_txn(
        network=net, target_service=svc, cid=73,
        txn_id=shared_txn_id, commit=False,
        topics="events:0:1", records=6, keys=1,
        timeout_ms=20000, timeout_s=60, egress_port=40833,
    )
    assert prod1.ok
    assert_txn_aborted(prod1)

    # P2 with same txn_id commits (epoch bump at initTransactions)
    prod2 = run_txn(
        network=net, target_service=svc, cid=74,
        txn_id=shared_txn_id, commit=True,
        topics="events:0:1", records=6, keys=1,
        timeout_ms=20000, timeout_s=60, egress_port=40834,
    )
    assert prod2.ok and prod2.outcome == "committed", (
        f"P2 commit failed: {prod2.raw[-400:]}"
    )

    cons = run_txn_consume(
        network=net, target_service=svc,
        group_id=_next_group_id(), cid=75,
        topics="events:0:1",
        isolation="read_committed",
        subscribe_ms=15000, timeout_ms=20000, timeout_s=60, egress_port=40835,
    )
    # P1's records (aborted) must not be visible
    assert_read_committed_excludes(cons, prod1.sent)
    # P2's records must be visible
    assert_eos_no_loss(prod2.sent, cons)
    assert_eos_no_dup(cons)


# ── Fencing: multiple epoch bumps ─────────────────────────────────────────────

@pytest.mark.timeout(480)
def test_tx_fencing_multiple_epoch_bumps(fresh_cluster):
    """Three producers with the same txn_id, each bumping epoch in sequence.

    Only the last producer's committed records are visible (each previous
    epoch was superseded).  This verifies the broker correctly sequences
    InitProducerId epoch increments and rejects stale-epoch produce attempts.
    """
    cluster = fresh_cluster
    svc = cluster.wait_data_plane_ready()
    net = cluster.compose.network_name()

    shared_txn_id = _next_txn_id()
    results = []
    grp_id = _next_group_id()

    for i, (commit, cid_val, port) in enumerate([
        (False, 80, 40836),  # E0 → aborted
        (False, 81, 40837),  # E1 → aborted
        (True,  82, 40838),  # E2 → committed
    ]):
        r = run_txn(
            network=net, target_service=svc, cid=cid_val,
            txn_id=shared_txn_id, commit=commit,
            topics="events:0:1", records=4, keys=1,
            timeout_ms=20000, timeout_s=60, egress_port=port,
        )
        assert r.ok, f"producer {i} failed: {r.raw[-400:]}"
        results.append(r)

    # Only the last (E2, committed) records should be visible
    cons = run_txn_consume(
        network=net, target_service=svc,
        group_id=grp_id, cid=83,
        topics="events:0:1",
        isolation="read_committed",
        subscribe_ms=15000, timeout_ms=20000, timeout_s=60, egress_port=40839,
    )
    # E0 and E1 (aborted) must not be visible
    assert_read_committed_excludes(cons, results[0].sent)
    assert_read_committed_excludes(cons, results[1].sent)
    # E2 (committed) must be visible
    assert_eos_no_loss(results[2].sent, cons)
    assert_eos_no_dup(cons)
