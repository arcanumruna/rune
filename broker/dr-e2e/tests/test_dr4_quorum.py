"""DR-4: follower-kill and quorum scenarios.

Tests the cluster's behaviour under two quorum conditions:

  1. **Quorum holds (kill 1 follower):** produce acks=All to the leader; kill ONE
     follower (non-leader).  Cluster has 2/3 = majority → ISR shrinks to 2 which
     still satisfies minIsr(2).  Assert the leader KEEPS SERVING: a further produce
     acks=All + subscribe succeed, no committed loss, ordering, exactly-once, and
     single-leader still holds.

  2. **Quorum lost (kill 2/3):** commit a batch acks=All first (record the committed
     set).  Kill the leader + one follower (2 of 3 dead → 1 survivor, no quorum).
     Assert SAFETY:
     - The lone survivor must NOT serve writes (produce must fail/timeout, NOT ack).
     - The survivor must NOT declare itself leader for a new term (no split-brain in
       a minority — there must be AT MOST one leader announced across running nodes,
       and it must not be a NEW leader serving on a minority).
     RESTORE quorum (restart the 2 killed nodes).  Assert RECOVERY:
     - Cluster re-elects a single leader.
     - Pre-quorum-loss committed set SURVIVES subscribe-from-earliest.
     - assert_single_leader holds after recovery.

NOTE on Milestone-C durability: producers use acks=All so every PRODUCED line is
majority-durable before the ack arrives.  The no-committed-loss assertion is
therefore iron-clad: acked records MUST survive any minority failure.
"""

from __future__ import annotations

import re
import signal
import time

import pytest

from harness import faults
from harness.cluster import SERVICES, node_id_to_service
from harness.driver import DRAIN_QUIET_MS, Results, run_clients
from harness.invariants import (
    assert_exactly_once,
    assert_no_committed_loss,
    assert_per_key_ordering,
    assert_single_leader,
)

# ── Per-run disjoint cid windows ──────────────────────────────────────────────
# DR-1: n*1000, DR-2: 100_000+n*2000, DR-3: 300_000+n*3000
# DR-4: 400_000+ window — disjoint from all previous tests.
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 400_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + client-start stagger (see DR-2 / DR-BUG-2 notes in DR-3).
_CONNECT_MS = 40000
_STAGGER_MS = 400


# ── Shared helpers (mirror DR-3) ──────────────────────────────────────────────

def _produce(cluster, target_svc: str, n: int, cid_base: int, records: int,
             keys: int, rate_hz: int = 0) -> Results:
    """Run `n` producers against target_svc with acks=All (majority-durable)."""
    return run_clients(
        n,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=240,
        mode="produce",
        topics="multi:1:3",
        records=records,
        keys=max(keys, 18),        # >= partitions so records span all 3 partitions
        rate_hz=rate_hz,
        acks="all",                # GR-9: majority-durable ack
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 60000, drain_quiet_ms: int = DRAIN_QUIET_MS) -> Results:
    """Subscribe-from-earliest and drain the whole partition set."""
    return run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=180,
        mode="subscribe",
        topics="multi:1:3",
        sub_partitions=3,
        keys=keys,
        expect=0,                  # drain the window; don't require a fixed count
        from_="earliest",
        subscribe_ms=window_ms,
        drain_quiet_ms=drain_quiet_ms,   # stop once the static log is fully read (driver._drain_complete)
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


# ── Safety helpers for quorum-loss window ────────────────────────────────────

def assert_no_serving_leader_in_minority(cluster, running_services: list[str]) -> None:
    """Assert that the minority survivor is NOT serving writes and that there is
    AT MOST one leader announced among running nodes (no split-brain).

    Two checks:
      1. Produce safety: attempt a produce acks=All to every running service;
         none must succeed (all must fail — timeout, rc!=0, or exit_code!=0).
         A successful produce would mean the minority accepted a write, which
         violates the minIsr gate and is a split-brain/data-loss risk.

      2. No concurrent-term leadership: scan each running node's logs for the
         LATEST leadership announcement. There must be AT MOST one distinct leader
         id. If multiple distinct ids appear the cluster has two serving leaders
         (split-brain blocker).  We do NOT assert that a leader EXISTS here —
         the minority is expected to have no serving leader.
    """
    # --- produce safety: every write attempt must fail ---
    for svc in running_services:
        try:
            probe = run_clients(
                1,
                network=cluster.compose.network_name(),
                target_service=svc,
                cid_base=_next_cid_base(),
                # E3 report 2026-10-04: the client fails the write itself after its
                # delivery timeout (30 s) + connect — 45 s cut it off mid-failure.
                timeout_s=90,
                mode="produce",
                topics="multi:1:3",
                records=5,
                keys=18,
                acks="all",            # acks=All; must be refused if ISR < minIsr(2)
                expect_own=True,
                timeout_ms=15000,      # short timeout — we WANT this to fail fast
                connect_retries=0,
                start_stagger_ms=0,
            )
        except TimeoutError as exc:
            # The probe never finished: the write was not acknowledged — unless
            # an ack (PRODUCED line) is already in its partial output.
            assert "PRODUCED cid=" not in str(exc), (
                f"SPLIT-BRAIN / MIN-ISR VIOLATION [BLOCKER]: minority survivor {svc!r} "
                f"acknowledged an acks=All produce before the probe timed out"
            )
            continue
        # A probe that HUNG past timeout_s now comes back as a failed result carrying its partial output
        # (driver: exit 124) instead of a TimeoutError - the same rule as the except branch above applies to it.
        if any(c.exit_code == 124 for c in probe.clients):
            assert not probe.all_produced, (
                f"SPLIT-BRAIN / MIN-ISR VIOLATION [BLOCKER]: minority survivor {svc!r} "
                f"acknowledged an acks=All produce before the probe timed out"
            )
            continue
        serving = probe.ok and len(probe.all_produced) > 0
        assert not serving, (
            f"SPLIT-BRAIN / MIN-ISR VIOLATION [BLOCKER]: minority survivor {svc!r} "
            f"successfully accepted an acks=All produce with only {len(running_services)}/3 "
            f"nodes running (ISR < minIsr(2) = quorum lost). "
            f"produced={len(probe.all_produced)} rc={probe.clients[0].rc if probe.clients else '?'} "
            f"exit={probe.clients[0].exit_code if probe.clients else '?'}"
        )

    # --- no-split-brain: at most one leader id across running nodes ---
    leader_re = re.compile(r"node (\d+) is LEADER")
    claims: dict[str, int] = {}
    for svc in running_services:
        logs = cluster.compose.logs(svc, timeout=30)
        ids = leader_re.findall(logs)
        if ids:
            claims[svc] = int(ids[-1])
    distinct = set(claims.values())
    assert len(distinct) <= 1, (
        f"SPLIT-BRAIN [BLOCKER]: running minority nodes disagree on leader: {claims} "
        f"(distinct leader ids: {sorted(distinct)})"
    )
    # If there IS a claimed leader in the minority, that is acceptable only if
    # it was the pre-quorum-loss leader (the log entry predates the kill).
    # The produce-safety check above already guards against it actually SERVING.


# ── Test 1: quorum holds — kill 1 follower ───────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(600)
@pytest.mark.parametrize("n_clients", [1, 3])
def test_dr4_quorum_holds_kill_follower(fresh_cluster, n_clients):
    """Kill ONE follower (2/3 survive = majority).  The leader must keep serving:
    acks=All remains satisfiable because ISR=2 = minIsr(2).  Assert continuity:
    no committed loss, ordering, exactly-once, single-leader throughout.
    """
    cluster = fresh_cluster
    keys = 4
    records = 150

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Pre-kill batch: commit records acks=All.
    cid_pre = _next_cid_base()
    committed = _produce(cluster, leader_svc, n_clients, cid_pre, records, keys)
    assert committed.ok, (
        "pre-kill produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in committed.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )

    # Identify and kill a follower (non-leader).
    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc, "follower_service() returned the leader"
    faults.kill_node(cluster, follower_svc, signal.SIGKILL)

    # Cluster still has 2/3 = majority.  The leader must keep serving.
    # Brief wait for leader to detect ISR shrinkage.
    # KEPT fixed: "no spurious re-election" below is checked after an observation window.
    time.sleep(3.0)

    # Verify the leader is still the same (no spurious re-election).
    running = cluster.running_services()
    assert leader_svc in running, f"leader {leader_svc} unexpectedly went down"
    assert_single_leader(cluster)

    # Post-kill produce + subscribe: must succeed on the same leader.
    cid_post = _next_cid_base()
    post_produce = _produce(cluster, leader_svc, 1, cid_post, 50, keys)
    assert post_produce.ok, (
        "post-kill produce (acks=All with ISR=2) failed — leader refused to serve "
        "when quorum is still intact:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in post_produce.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post_produce.all_produced) == 50, (
        f"post-kill produced {len(post_produce.all_produced)} != 50"
    )

    # Subscribe-from-earliest to check the full committed set.
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, leader_svc, cid_sub, keys)

    # Merge pre+post produce and the subscriber results for invariant checks.
    merged = Results(clients=committed.clients + post_produce.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)


# ── Test 2: quorum lost — kill 2/3, safety + recovery ────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(900)
def test_dr4_quorum_lost_safety_and_recovery(fresh_cluster):
    """Kill leader + 1 follower (2/3 dead → minority survivor).

    Safety window: the lone survivor MUST NOT serve writes (acks=All refused
    because ISR(1) < minIsr(2)).  No split-brain.

    Recovery: restart both killed nodes; cluster re-elects; committed set survives.
    """
    cluster = fresh_cluster
    keys = 4
    records = 150

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Pre-kill batch: commit records acks=All (these MUST survive the outage).
    cid_pre = _next_cid_base()
    committed = _produce(cluster, leader_svc, 1, cid_pre, records, keys)
    assert committed.ok, (
        "pre-kill produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in committed.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )
    pre_produced_count = len(committed.all_produced)
    assert pre_produced_count > 0, "pre-kill produce returned 0 records"

    # Identify one follower to kill alongside the leader.
    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc

    # Determine the sole survivor (the remaining follower).
    survivor_svc = next(s for s in SERVICES if s not in (leader_svc, follower_svc))

    # Kill leader first, then follower — 2/3 dead.
    faults.kill_node(cluster, leader_svc, signal.SIGKILL)
    time.sleep(1.0)   # KEPT: the gap between the two kills is part of the scenario
    faults.kill_node(cluster, follower_svc, signal.SIGKILL)

    # Give the survivor a moment to recognise quorum loss.
    # KEPT fixed: the quorum-loss window the minority-safety checks below observe (no serving leader).
    time.sleep(5.0)

    running = cluster.running_services()
    assert survivor_svc in running, f"survivor {survivor_svc} is not running"
    assert len(running) == 1, f"expected 1 running node, got {running}"

    # ── SAFETY ASSERTIONS (quorum-loss window) ────────────────────────────────
    # assert_single_leader() requires a leader to exist — we do NOT call it here
    # because no quorum means no valid leader should be serving.  Use the
    # minority-specific safety helper instead.
    assert_no_serving_leader_in_minority(cluster, running)

    # ── RECOVERY: restart the 2 killed nodes ─────────────────────────────────
    faults.restart(cluster, leader_svc)
    faults.restart(cluster, follower_svc)

    # Wait for a new leader among the now-restored 3-node cluster.
    new_lid = cluster.wait_for_leader(timeout_s=120)
    new_leader_svc = node_id_to_service(new_lid)

    # Cluster must re-elect EXACTLY ONE leader.
    assert_single_leader(cluster)

    # ── DURABILITY: pre-quorum-loss committed set must survive ────────────────
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, new_leader_svc, cid_sub, keys, window_ms=90000)

    merged = Results(clients=committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)

    # Post-recovery usability: fresh produce+subscribe must work end-to-end.
    cid_recovery = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=new_leader_svc,
        cid_base=cid_recovery,
        timeout_s=180,
        mode="produce_subscribe",
        topics="multi:1:3",
        sub_partitions=3,
        records=30,
        keys=18,
        expect=30,
        acks="all",
        expect_own=True,
        from_="earliest",
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        subscribe_ms=60000,
        start_stagger_ms=_STAGGER_MS,
    )
    assert post.ok, (
        "post-recovery produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"post-recovery produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)
