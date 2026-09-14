"""DR-3: leader-kill scenarios.

Kill the consensus LEADER and assert the cluster + clients survive:
  * re-election — a DIFFERENT node becomes leader (single leader, no split-brain);
  * no committed loss — every record the producer got an ack for BEFORE the kill
    is still delivered to a subscriber AFTER recovery (durable across failover);
  * ordering + exactly-once preserved for the surviving records;
  * post-recovery usability — a fresh produce+subscribe works against the new
    leader (the cluster is fully serving again).

Variants (parametrized):
  * idle        — kill the leader with no client in flight, then verify the
                  previously-committed set survives + the cluster recovers.
  * mid_produce — kill the leader WHILE rate-limited producers are running; some
                  in-flight produces may fail (the client bails on a produce
                  error — that is acceptable), but every ACKED record must survive
                  and the cluster must re-elect + serve again.
  * rolling     — kill the leader, wait for a new leader, restart the killed node
                  so it rejoins (quorum preserved), repeat N times; then verify a
                  produce+subscribe works and there is a single leader.

NOTE on durability (GR-9 / Milestone C): producers now use acks=All, so a
PRODUCED line means the batch has been durably replicated to a majority of ISR
nodes before the ProduceResponse was sent.  This makes the no-committed-loss
assertion iron-clad: any record that appears in `committed` MUST survive a leader
kill because it was majority-flushed before the ack.  The prior acks=Leader
caveat (DR-15 tail window) no longer applies to these test scenarios.
"""

from __future__ import annotations

import signal
import threading

import pytest

from harness import faults
from harness.driver import Results, run_clients
from harness.invariants import (
    assert_exactly_once,
    assert_no_committed_loss,
    assert_per_key_ordering,
    assert_single_leader,
)

# ── Per-run disjoint cid windows (log persists across the session cluster) ─────
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    # 300_000+ window — disjoint from DR-1 (n*1000) and DR-2 (100_000+n*2000).
    return 300_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + client-start stagger — Docker Desktop connect
# establishment tail (see DR-2 / DR-BUG-2), NOT a workload mask.
_CONNECT_MS = 40000
_STAGGER_MS = 400


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 60000) -> Results:
    """Subscribe-from-earliest on `target_svc` and drain the whole partition set.

    Used to verify the committed set survived: a single subscriber reads every
    record on the (multi-partition) log from the beginning.
    """
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
        expect=0,                 # drain the window; don't require a fixed count
        from_="earliest",
        subscribe_ms=window_ms,
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


def _produce(cluster, target_svc: str, n: int, cid_base: int, records: int,
             keys: int, rate_hz: int = 0) -> Results:
    """Run `n` producers (produce-only) against target_svc. Returns Results.

    Uses acks=all so every PRODUCED line represents a majority-committed record.
    This is the correct basis for no-committed-loss assertions across failover.
    """
    cfg = dict(
        mode="produce",
        topics="multi:1:3",
        records=records,
        keys=max(keys, 18),       # ≥ partitions so records span all 3 partitions
        rate_hz=rate_hz,
        acks="all",               # GR-9: majority-durable ack before counting as committed
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )
    return run_clients(
        n,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=240,
        **cfg,
    )


def _assert_recovery(cluster, new_leader_svc: str, committed: Results,
                     keys: int) -> None:
    """Assert every ACKED record in `committed` survives + is delivered post-kill,
    with ordering + exactly-once, and the cluster is singular + usable again."""
    # Re-election already asserted by the caller (wait_for_new_leader).
    assert_single_leader(cluster)

    # Durability + no committed loss: a fresh subscribe-from-earliest must deliver
    # every record the producer got an ack for before/around the kill.
    sub = _subscribe_all(cluster, new_leader_svc, _next_cid_base(), keys)
    merged = Results(clients=committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)

    # Post-recovery usability: a fresh produce+subscribe round-trips on the new
    # leader (cluster fully serving again).  Use acks=all so the produce side of
    # this round-trip is also majority-durable (not just leader-page-cache).
    cid = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=new_leader_svc,
        cid_base=cid,
        timeout_s=180,
        mode="produce_subscribe",
        topics="multi:1:3",
        sub_partitions=3,
        records=30,
        keys=max(keys, 18),
        expect=30,
        acks="all",               # GR-9: post-recovery produce must also be majority-durable
        expect_own=True,
        from_="earliest",
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        subscribe_ms=60000,
        start_stagger_ms=_STAGGER_MS,
    )
    assert post.ok, "post-recovery produce+subscribe failed:\n" + "\n\n".join(
        f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
        for c in post.clients if c.exit_code != 0 or c.rc != 0
    )
    assert len(post.all_produced) == 30, (
        f"post-recovery produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)


# ── idle + mid_produce leader-kill ────────────────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(600)
@pytest.mark.parametrize("n_clients", [1, 3])
@pytest.mark.parametrize("timing", ["idle", "mid_produce"])
@pytest.mark.parametrize("rep", [0, 1])
def test_dr3_leader_kill(fresh_cluster, n_clients, timing, rep):
    cluster = fresh_cluster
    keys = 4
    records = 200
    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    if timing == "idle":
        # Commit a batch, THEN kill the (idle) leader.
        committed = _produce(cluster, leader_svc, n_clients, _next_cid_base(),
                             records, keys, rate_hz=0)
        assert committed.ok, "pre-kill produce failed:\n" + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in committed.clients if c.exit_code != 0 or c.rc != 0
        )
        faults.kill_node(cluster, leader_svc, signal.SIGKILL)
    else:  # mid_produce — kill WHILE producing (rate-limited to open a window)
        cid = _next_cid_base()
        holder: dict[str, Results] = {}

        def _run():
            # rate_hz keeps production spanning several seconds so the kill lands
            # mid-flight.  Producers may bail on the failover (acceptable).
            holder["r"] = _produce(cluster, leader_svc, n_clients, cid,
                                   records, keys, rate_hz=40)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        # Let production get going, then kill the leader mid-flight.
        import time as _t
        _t.sleep(2.5)
        faults.kill_node(cluster, leader_svc, signal.SIGKILL)
        t.join(timeout=300)
        committed = holder.get("r", Results())

    # Re-election: a DIFFERENT node must lead.
    # wait_for_new_leader already excludes old-leader logs; use its result
    # directly to derive new_leader_svc — avoids identify_leader() re-scanning
    # all logs (including the dead node's stale "is LEADER" entry) and returning
    # the old leader again (harness-log scan artefact, not a real election bug).
    from harness.cluster import node_id_to_service
    new_lid = cluster.wait_for_new_leader(leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != leader_svc, "leader did not change after kill"

    _assert_recovery(cluster, new_leader_svc, committed, keys)


# ── rolling leader-kill (kill → re-elect → rejoin, ×N) ────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(600)
@pytest.mark.parametrize("rounds", [2, 3])
def test_dr3_rolling_leader_kill(fresh_cluster, rounds):
    """Kill the leader, wait for a new leader, restart the killed node so it
    rejoins (quorum preserved), repeat `rounds` times.  Then a produce+subscribe
    must work and there must be exactly one leader."""
    cluster = fresh_cluster
    assert_single_leader(cluster)

    for i in range(rounds):
        _, leader_svc = cluster.identify_leader()
        faults.kill_node(cluster, leader_svc, signal.SIGKILL)
        cluster.wait_for_new_leader(leader_svc)
        # Restart the killed node so quorum is preserved for the next round.
        faults.restart(cluster, leader_svc)
        # Give the rejoined node a moment to catch up before the next kill.
        import time as _t
        _t.sleep(3.0)
        assert_single_leader(cluster)

    # Cluster must still serve after N rolling failovers.
    _, leader_svc = cluster.identify_leader()
    cid = _next_cid_base()
    res = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=leader_svc,
        cid_base=cid,
        timeout_s=180,
        mode="produce_subscribe",
        topics="multi:1:3",
        sub_partitions=3,
        records=50,
        keys=18,
        expect=50,
        expect_own=True,
        from_="earliest",
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        subscribe_ms=60000,
        start_stagger_ms=_STAGGER_MS,
    )
    assert res.ok, "post-rolling produce+subscribe failed:\n" + "\n\n".join(
        f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
        for c in res.clients if c.exit_code != 0 or c.rc != 0
    )
    assert len(res.all_produced) == 50, f"produced {len(res.all_produced)} != 50"
    assert_per_key_ordering(res)
    assert_exactly_once(res)
    assert_single_leader(cluster)
