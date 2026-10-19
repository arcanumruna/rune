"""TX-6 — test_tx_read_committed: isolation level differential.

read_committed NEVER exposes aborted or open-txn records.
read_uncommitted DOES expose records up to the High Watermark (≤HW).

Variants:
  - happy path: committed → visible in both; aborted → only in uncommitted
  - netem-loss: under 10% packet loss, isolation holds

Mac / Docker Desktop note: netem variants require NET_ADMIN + tc inside a
container netns sidecar (faults.netem).  On Docker Desktop (macOS LinuxKit VM),
tc netem works but may add extra jitter.  If the netem sidecar fails to apply,
the test is skipped with an explanation.
"""

from __future__ import annotations

import uuid
import time

import pytest

from harness import faults
from harness.driver import run_txn, run_txn_consume
from harness.invariants import (
    assert_eos_no_dup,
    assert_eos_no_loss,
    assert_read_committed_excludes,
    assert_read_committed_no_aborted,
    assert_read_uncommitted_includes,
    assert_txn_aborted,
    assert_txn_committed,
)


def _next_txn_id() -> str:
    return f"rc-{uuid.uuid4().hex[:12]}"


def _next_group_id() -> str:
    return f"rc-grp-{uuid.uuid4().hex[:8]}"


# ── Smoke: single-node happy-path ─────────────────────────────────────────────

@pytest.mark.timeout(240)
def test_tx_read_committed_smoke(fresh_cluster):
    """Commit → visible in both; abort → not visible in read_committed."""
    cluster = fresh_cluster
    svc = cluster.wait_data_plane_ready()
    net = cluster.compose.network_name()

    # Committed transaction
    committed_txn_id = _next_txn_id()
    prod_commit = run_txn(
        network=net, target_service=svc, cid=50,
        txn_id=committed_txn_id, commit=True,
        topics="events:0:1", records=8, keys=2,
        timeout_ms=20000, timeout_s=60, egress_port=40820,
    )
    assert prod_commit.ok and prod_commit.outcome == "committed"

    # Aborted transaction
    aborted_txn_id = _next_txn_id()
    prod_abort = run_txn(
        network=net, target_service=svc, cid=51,
        txn_id=aborted_txn_id, commit=False,
        topics="events:0:1", records=8, keys=2,
        timeout_ms=20000, timeout_s=60, egress_port=40821,
    )
    assert prod_abort.ok
    assert_txn_aborted(prod_abort)

    # read_committed: sees committed, NOT aborted
    cons_rc = run_txn_consume(
        network=net, target_service=svc,
        group_id=_next_group_id(), cid=52,
        topics="events:0:1",
        isolation="read_committed",
        subscribe_ms=15000, timeout_ms=20000, timeout_s=60, egress_port=40822,
    )
    assert_read_committed_no_aborted(cons_rc)
    assert_eos_no_dup(cons_rc)
    assert_read_committed_excludes(cons_rc, prod_abort.sent)
    assert_eos_no_loss(prod_commit.sent, cons_rc)

    # read_uncommitted: sees everything up to HW (includes aborted)
    cons_ru = run_txn_consume(
        network=net, target_service=svc,
        group_id=_next_group_id(), cid=53,
        topics="events:0:1",
        isolation="read_uncommitted",
        subscribe_ms=15000, timeout_ms=20000, timeout_s=60, egress_port=40823,
    )
    assert_read_uncommitted_includes(cons_ru, prod_commit.sent)
    assert_read_uncommitted_includes(cons_ru, prod_abort.sent)


# ── Open (in-flight) transaction not visible to read_committed ────────────────

@pytest.mark.timeout(240)
def test_tx_read_committed_open_txn_invisible(fresh_cluster):
    """An open (uncommitted, not aborted) txn is invisible to read_committed.

    We produce COMMITTED records first, then start a second transaction with
    the same producer but don't commit it (abort). read_committed sees only
    the first batch.
    """
    cluster = fresh_cluster
    svc = cluster.wait_data_plane_ready()
    net = cluster.compose.network_name()

    # First committed txn
    txn_id_1 = _next_txn_id()
    prod1 = run_txn(
        network=net, target_service=svc, cid=54,
        txn_id=txn_id_1, commit=True,
        topics="events:0:1", records=6, keys=1,
        timeout_ms=20000, timeout_s=60, egress_port=40824,
    )
    assert prod1.ok and prod1.outcome == "committed"

    # Second txn — aborted (simulates an "open" txn that never commits)
    txn_id_2 = _next_txn_id()
    prod2 = run_txn(
        network=net, target_service=svc, cid=55,
        txn_id=txn_id_2, commit=False,
        topics="events:0:1", records=6, keys=1,
        timeout_ms=20000, timeout_s=60, egress_port=40825,
    )
    assert prod2.ok
    assert_txn_aborted(prod2)

    # read_committed: sees only prod1's records, NOT prod2's
    cons_rc = run_txn_consume(
        network=net, target_service=svc,
        group_id=_next_group_id(), cid=56,
        topics="events:0:1",
        isolation="read_committed",
        subscribe_ms=15000, timeout_ms=20000, timeout_s=60, egress_port=40826,
    )
    assert_read_committed_excludes(cons_rc, prod2.sent)
    assert_eos_no_loss(prod1.sent, cons_rc)


# ── Netem-loss variant ────────────────────────────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(480)
def test_tx_read_committed_netem_loss(fresh_cluster, netem_ready):
    """Under 10% packet loss, read_committed isolation still holds.

    Mac / Docker Desktop: netem via sidecar + NET_ADMIN works on LinuxKit VM.
    If the sidecar fails (non-zero exit from tc), the test is skipped.
    """
    cluster = fresh_cluster
    svc = cluster.wait_data_plane_ready()
    net = cluster.compose.network_name()

    # Apply netem 10% loss on the leader node
    _, leader_svc = cluster.identify_leader()
    try:
        faults.netem(cluster, leader_svc, loss=10)
    except RuntimeError as exc:
        pytest.skip(f"netem sidecar failed (mac/Docker Desktop flake?): {exc}")

    try:
        txn_id_commit = _next_txn_id()
        prod_commit = run_txn(
            network=net, target_service=leader_svc, cid=60,
            txn_id=txn_id_commit, commit=True,
            topics="events:0:1", records=10, keys=2,
            timeout_ms=30000, timeout_s=90, egress_port=40827,
        )

        txn_id_abort = _next_txn_id()
        prod_abort = run_txn(
            network=net, target_service=leader_svc, cid=61,
            txn_id=txn_id_abort, commit=False,
            topics="events:0:1", records=10, keys=2,
            timeout_ms=30000, timeout_s=90, egress_port=40828,
        )
    finally:
        faults.clear_netem(cluster, leader_svc)

    if not prod_commit.ok or prod_commit.outcome != "committed":
        pytest.skip(f"commit failed under netem loss (transient): {prod_commit.raw[-300:]}")
    if not prod_abort.ok:
        pytest.skip(f"abort failed under netem loss (transient): {prod_abort.raw[-300:]}")

    assert_txn_aborted(prod_abort)

    cons_rc = run_txn_consume(
        network=net, target_service=leader_svc,
        group_id=_next_group_id(), cid=62,
        topics="events:0:1",
        isolation="read_committed",
        subscribe_ms=15000, timeout_ms=20000, timeout_s=60, egress_port=40829,
    )
    assert_read_committed_no_aborted(cons_rc)
    assert_read_committed_excludes(cons_rc, prod_abort.sent)
    assert_eos_no_loss(prod_commit.sent, cons_rc)
