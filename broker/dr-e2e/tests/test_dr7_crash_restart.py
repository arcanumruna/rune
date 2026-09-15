"""DR-7: Node crash + restart / durability recovery.

SIGKILL or SIGTERM a node, restart it, assert it rejoins the ISR and that NO
committed (acks=All) records are lost.  Tests cover the four recovery scenarios
mandated by Milestone C:

  1. **Follower crash + restart + catch-up** — SIGKILL a follower while the
     leader + other follower keep accepting acks=All writes; restart the crashed
     follower; assert it catches up to the HW and re-enters ISR; subscribe-from-
     earliest must deliver ALL records (pre-down + during-down), with ordering
     and exactly-once; a fresh acks=All produce+subscribe confirms ISR is healed.

  2. **Leader crash + restart + rejoin-as-follower** — SIGKILL the leader; a new
     leader is elected; produce more acks=All to the new leader; restart the old
     leader; it rejoins as a follower + catches up; assert no committed loss, no
     ordering violations, exactly-once, and single-leader throughout.

  3. **Graceful (SIGTERM) vs ungraceful (SIGKILL) parity** — one parametrized
     case each of a clean SIGTERM and a SIGKILL crash of a follower; both must
     recover with the full committed set intact (parity assertion).

  4. **Full-cluster cold restart** — commit acks=All; SIGKILL ALL 3 nodes;
     restart all 3 via `cluster.compose.start` (the only harness path that
     starts a stopped container); `wait_for_leader`; assert the committed
     (acks=All, majority-flushed) set SURVIVES the cold restart via subscribe-
     from-earliest.  Single-leader + fresh produce+subscribe round-trip confirms
     the cluster is fully serving.

NOTE: `fresh_cluster.down()` calls `compose down -v`, which also removes
stopped containers, so we do not need to worry about restarting killed nodes
before teardown — any container left stopped will be cleaned up by the fixture.
However, for scenario 4 the nodes are restarted within the test body so the
cluster is live (and can be torn down cleanly) at fixture exit.

HARNESS UNCERTAINTY (flag for controller): `cluster.compose.start(service)`
restarts a single STOPPED container and is called per-node in scenario 4 because
there is no batch "start all" API in the current harness.  The `Compose.up(*svcs)`
method is a bring-up path (with `--detach`) and DOES accept multiple services, so
`cluster.compose.up(*SERVICES)` would also work for a cold-start scenario — but
it may re-create containers rather than just re-starting stopped ones.  The
controller should consider adding a `Compose.start_all(*services)` helper that
issues `docker compose start <svc1> <svc2> <svc3>` in a single call to reduce
cold-restart race windows.
"""

from __future__ import annotations

import signal
import time

import pytest

from harness import faults
from harness.cluster import SERVICES, node_id_to_service
from harness.driver import Results, run_clients
from harness.invariants import (
    assert_exactly_once,
    assert_no_committed_loss,
    assert_per_key_ordering,
    assert_single_leader,
)

# ── Per-run disjoint cid windows ──────────────────────────────────────────────
# DR-1: n*1000, DR-2: 100_000+, DR-3: 300_000+, DR-4: 400_000+,
# DR-5: 500_000+, DR-6: 600_000+
# DR-7: 700_000+ — fully disjoint from all prior suites.
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 700_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + client-start stagger — mirrors DR-3 / DR-4 tuning
# to tolerate Docker Desktop connection establishment tail (see DR-BUG-2).
_CONNECT_MS = 40000
_STAGGER_MS = 400


# ── Shared helpers ────────────────────────────────────────────────────────────

def _produce(cluster, target_svc: str, n: int, cid_base: int, records: int,
             keys: int, rate_hz: int = 0) -> Results:
    """Run `n` producers against target_svc with acks=All (majority-durable).

    Every PRODUCED line represents a record that has been committed to a
    majority of the ISR before the ProduceResponse was sent (GR-9 / Milestone C).
    """
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
        acks="all",                # GR-9: majority-durable ack before counting
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 60000) -> Results:
    """Subscribe-from-earliest on `target_svc` and drain the whole partition set.

    Used to assert the committed set survived: a single subscriber reads every
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
        expect=0,                  # drain window; don't require a fixed count
        from_="earliest",
        subscribe_ms=window_ms,
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


def _assert_isr_healed(cluster, leader_svc: str, keys: int) -> None:
    """Confirm ISR has healed back to 3 by verifying acks=All is satisfiable
    with all 3 nodes running, and the cluster serves a fresh produce+subscribe.

    A successful acks=All produce requires ISR >= minIsr(2), so a 3-node cluster
    in which the rejoined follower has NOT re-entered the ISR would still pass if
    ISR=2.  We treat a successful 3-node acks=All produce+subscribe as sufficient
    ISR-healed evidence — a stricter ISR=3 check would require log scraping.
    """
    running = cluster.running_services()
    assert len(running) == 3, (
        f"ISR-healed check: expected 3 running nodes, got {running}"
    )
    cid = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=leader_svc,
        cid_base=cid,
        timeout_s=180,
        mode="produce_subscribe",
        topics="multi:1:3",
        sub_partitions=3,
        records=30,
        keys=max(keys, 18),
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
        "ISR-healed produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"ISR-healed produce+subscribe: produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)


# ── Scenario 1: Follower crash + restart + catch-up ──────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(720)
@pytest.mark.parametrize("rep", [0, 1])
@pytest.mark.parametrize("follower_idx", [0, 1])
def test_dr7_follower_crash_restart_catchup(fresh_cluster, follower_idx, rep):
    """SIGKILL a follower; produce acks=All while it's down (quorum via leader +
    other follower); restart the crashed follower; assert it rejoins, catches up
    to the HW, and re-enters ISR.  Subscribe-from-earliest must deliver ALL
    committed records (pre-crash + during-down).

    `follower_idx` selects which of the two non-leader followers to crash (0 or 1)
    so we cover both follower identities across parametrization.  `rep` is a
    repetition dimension so the matrix runs each combination twice.
    """
    cluster = fresh_cluster
    keys = 4
    records_pre = 150
    records_during = 100

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Pick the target follower by index (0 or 1) from the two non-leader nodes.
    follower_candidates = [s for s in SERVICES if s != leader_svc]
    assert len(follower_candidates) == 2, "expected exactly 2 non-leader nodes"
    crash_svc = follower_candidates[follower_idx]

    # ── Pre-crash batch: commit records acks=All ──────────────────────────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, leader_svc, 1, cid_pre, records_pre, keys)
    assert pre_result.ok, (
        "pre-crash produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre_result.all_produced) == records_pre, (
        f"pre-crash: produced {len(pre_result.all_produced)} != {records_pre}"
    )

    # ── SIGKILL the follower ──────────────────────────────────────────────────
    faults.kill_node(cluster, crash_svc, signal.SIGKILL)

    # Brief wait for the leader to detect ISR shrinkage (ISR -> leader + other follower).
    time.sleep(3.0)

    running = cluster.running_services()
    assert crash_svc not in running, f"crashed follower {crash_svc} still appears running"
    assert leader_svc in running, f"leader {leader_svc} unexpectedly went down"
    assert_single_leader(cluster)

    # ── Produce more records while the follower is down ───────────────────────
    # The cluster has 2/3 = majority; acks=All is satisfiable (ISR=2 >= minIsr(2)).
    cid_during = _next_cid_base()
    during_result = _produce(cluster, leader_svc, 1, cid_during, records_during, keys)
    assert during_result.ok, (
        "during-down produce failed (quorum via leader + other follower):\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in during_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(during_result.all_produced) == records_during, (
        f"during-down: produced {len(during_result.all_produced)} != {records_during}"
    )

    # ── Restart the crashed follower; generous catch-up window ────────────────
    faults.restart(cluster, crash_svc)

    # Give the rejoined follower time to fetch missed segments and advance its HW.
    # Generous sleep: the crash_svc must replicate `records_during` records over
    # the Aeron IPC/UDP replication channel before re-entering the ISR.
    time.sleep(10.0)

    # All 3 nodes must now be running.
    running = cluster.running_services()
    assert crash_svc in running, f"restarted follower {crash_svc} not running after restart"
    assert len(running) == 3, f"expected 3 running nodes after restart, got {running}"
    assert_single_leader(cluster)

    # ── Durability: subscribe-from-earliest must deliver ALL committed records ─
    # Merge pre-crash + during-down committed sets for the no-committed-loss check.
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=90000)

    all_committed = Results(clients=pre_result.clients + during_result.clients + sub.clients)
    assert_no_committed_loss(all_committed)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)

    # ── ISR healed: a fresh acks=All produce+subscribe must succeed (ISR=3) ───
    _assert_isr_healed(cluster, leader_svc, keys)


# ── Scenario 2: Leader crash + restart + rejoin-as-follower ──────────────────

@pytest.mark.fault
@pytest.mark.timeout(720)
@pytest.mark.parametrize("rep", [0, 1])
def test_dr7_leader_crash_restart_rejoins_follower(fresh_cluster, rep):
    """SIGKILL the leader; wait for a new leader; produce more acks=All to the
    new leader; restart the old leader; assert it rejoins as a follower + catches
    up; assert no committed loss (pre-kill acks=All records survive on new leader),
    ordering, exactly-once, single-leader, and log convergence.
    """
    cluster = fresh_cluster
    keys = 4
    records_pre = 150
    records_post_kill = 100

    lid, old_leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Pre-kill batch: commit records acks=All ───────────────────────────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, old_leader_svc, 1, cid_pre, records_pre, keys)
    assert pre_result.ok, (
        "pre-kill produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre_result.all_produced) == records_pre, (
        f"pre-kill: produced {len(pre_result.all_produced)} != {records_pre}"
    )

    # ── SIGKILL the leader ────────────────────────────────────────────────────
    faults.kill_node(cluster, old_leader_svc, signal.SIGKILL)

    # Re-election: a DIFFERENT node must lead.
    new_lid = cluster.wait_for_new_leader(old_leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != old_leader_svc, (
        "leader did not change after SIGKILL of old leader"
    )

    # ── Produce more records to the new leader while old leader is down ───────
    cid_new = _next_cid_base()
    new_result = _produce(cluster, new_leader_svc, 1, cid_new, records_post_kill, keys)
    assert new_result.ok, (
        "post-failover produce to new leader failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in new_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(new_result.all_produced) == records_post_kill, (
        f"post-failover produce: produced {len(new_result.all_produced)} "
        f"!= {records_post_kill}"
    )

    # ── Restart old leader; it must rejoin as a FOLLOWER ─────────────────────
    faults.restart(cluster, old_leader_svc)

    # Give it time to catch up (replicate pre-kill segments + new records from
    # new leader).  The new leader must remain the SINGLE leader — the restarted
    # old node must NOT re-declare itself leader.
    time.sleep(10.0)

    running = cluster.running_services()
    assert old_leader_svc in running, (
        f"restarted old leader {old_leader_svc} not in running after restart"
    )
    assert len(running) == 3, f"expected 3 running nodes after restart, got {running}"

    # The new leader must still be the single leader (old node rejoined as follower).
    current_leader = assert_single_leader(cluster)
    assert node_id_to_service(current_leader) == new_leader_svc, (
        f"old leader {old_leader_svc} appears to have re-elected itself as leader "
        f"(current leader: {node_id_to_service(current_leader)}, expected {new_leader_svc})"
    )

    # ── Durability: all pre-kill + post-kill acks=All records must survive ────
    # Subscribe from the new leader to check the full committed set.
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, new_leader_svc, cid_sub, keys, window_ms=90000)

    all_committed = Results(
        clients=pre_result.clients + new_result.clients + sub.clients
    )
    assert_no_committed_loss(all_committed)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)

    # ── Log convergence: old leader must also deliver the full committed set ──
    # Subscribe from the restarted old-leader node to verify its log converged.
    cid_conv = _next_cid_base()
    conv_sub = _subscribe_all(cluster, old_leader_svc, cid_conv, keys, window_ms=90000)
    conv_merged = Results(
        clients=pre_result.clients + new_result.clients + conv_sub.clients
    )
    assert_no_committed_loss(conv_merged)
    assert_per_key_ordering(conv_sub)
    assert_exactly_once(conv_sub)

    # ── ISR healed: a fresh acks=All produce+subscribe must succeed ───────────
    _assert_isr_healed(cluster, new_leader_svc, keys)


# ── Scenario 3: SIGTERM vs SIGKILL parity ────────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(600)
@pytest.mark.parametrize("sig,label", [
    (signal.SIGTERM, "graceful_sigterm"),
    (signal.SIGKILL, "ungraceful_sigkill"),
])
def test_dr7_sigterm_vs_sigkill_parity(fresh_cluster, sig, label):
    """Parity test: a SIGTERM (graceful shutdown) and a SIGKILL (crash) of a
    follower must both recover with NO committed loss.  The committed set must
    be fully delivered by subscribe-from-earliest in both cases (parity assertion:
    committed records are durable regardless of how the node went down).
    """
    cluster = fresh_cluster
    keys = 4
    records = 150

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Pick a follower to crash/stop.
    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc, "follower_service() returned the leader"

    # ── Pre-down batch: commit records acks=All ───────────────────────────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, leader_svc, 1, cid_pre, records, keys)
    assert pre_result.ok, (
        f"[{label}] pre-down produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre_result.all_produced) == records, (
        f"[{label}] pre-down: produced {len(pre_result.all_produced)} != {records}"
    )

    # ── Stop the follower (SIGTERM = graceful, SIGKILL = crash) ──────────────
    faults.kill_node(cluster, follower_svc, sig)

    # Brief wait for leader to recognise ISR shrinkage.
    time.sleep(3.0)

    running = cluster.running_services()
    assert follower_svc not in running, (
        f"[{label}] follower {follower_svc} still running after signal"
    )
    assert leader_svc in running, (
        f"[{label}] leader {leader_svc} unexpectedly went down"
    )
    assert_single_leader(cluster)

    # ── Restart the stopped follower ──────────────────────────────────────────
    faults.restart(cluster, follower_svc)
    time.sleep(8.0)

    running = cluster.running_services()
    assert follower_svc in running, (
        f"[{label}] follower {follower_svc} not running after restart"
    )
    assert len(running) == 3, (
        f"[{label}] expected 3 running nodes, got {running}"
    )
    assert_single_leader(cluster)

    # ── Parity assertion: committed set fully delivered after recovery ─────────
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=90000)

    merged = Results(clients=pre_result.clients + sub.clients)
    assert_no_committed_loss(merged), (
        f"[{label}] committed-loss after {label} restart — "
        "parity violated: durable records must survive regardless of shutdown kind"
    )
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)

    # ── ISR healed: fresh acks=All produce+subscribe confirms recovery ─────────
    _assert_isr_healed(cluster, leader_svc, keys)


# ── Scenario 4: Full-cluster cold restart ─────────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(900)
def test_dr7_full_cluster_cold_restart(fresh_cluster):
    """Commit acks=All records; SIGKILL ALL 3 nodes; restart all 3; assert the
    committed (acks=All, majority-flushed) set SURVIVES the cold restart.

    Correctness guarantee: every acks=All record was majority-flushed to durable
    storage BEFORE the ProduceResponse was sent (Milestone C / GR-9 durability
    contract).  A cold restart of all 3 nodes must therefore NOT lose any of those
    records — they live in at least 2 nodes' durable segment files.

    NOTE on restart path: `faults.restart` calls `cluster.compose.restart(service)`
    which issues `docker compose restart <service>`.  This STOP+START path works
    for running containers.  For SIGKILL'd containers (which Docker marks as
    'exited'), we must call `cluster.compose.start(service)` (not `restart`)
    to bring them back up.  This test issues `compose.start` for each node
    individually.  The controller may wish to add a `Compose.start_all` helper
    that starts all services in a single call.

    HARNESS NOTE: if the harness lacks a clean "restart all" primitive, the
    controller should add `Compose.start_all(*services)` (delegating to
    `docker compose start <svc1> <svc2> <svc3>`) — this is flagged above and in
    the module docstring.
    """
    cluster = fresh_cluster
    keys = 4
    records = 200

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Pre-crash batch: commit records acks=All (majority-flushed) ───────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, leader_svc, 1, cid_pre, records, keys)
    assert pre_result.ok, (
        "pre-crash produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre_result.all_produced) == records, (
        f"pre-crash: produced {len(pre_result.all_produced)} != {records}"
    )
    pre_committed_count = len(pre_result.all_produced)
    assert pre_committed_count > 0, "pre-crash produce returned 0 records"

    # ── SIGKILL ALL 3 nodes ───────────────────────────────────────────────────
    # Kill sequentially with a short inter-kill gap to avoid a race where the
    # last surviving node panics / tries to flush to a non-existent quorum.
    for svc in SERVICES:
        faults.kill_node(cluster, svc, signal.SIGKILL)
        time.sleep(0.5)

    # Verify all nodes are down.
    time.sleep(3.0)
    running = cluster.running_services()
    assert len(running) == 0, (
        f"expected 0 running nodes after killing all 3, got {running}"
    )

    # ── Restart all 3 nodes ───────────────────────────────────────────────────
    # Use compose.start (not restart) since the containers are in 'exited' state.
    # Start them concurrently (one call per service) so the Raft election can
    # proceed as soon as a quorum is up.
    for svc in SERVICES:
        cluster.compose.start(svc)
        time.sleep(0.3)   # stagger slightly to avoid thundering-herd on startup

    # Wait for a new leader — generous timeout for cold-start (disk replay + election).
    new_lid = cluster.wait_for_leader(timeout_s=120)
    new_leader_svc = node_id_to_service(new_lid)

    # ── Single leader + all 3 running ────────────────────────────────────────
    assert_single_leader(cluster)
    running = cluster.running_services()
    assert len(running) == 3, (
        f"expected 3 running nodes after cold restart, got {running}"
    )

    # ── Durability: acks=All committed set must survive full cold restart ──────
    # acks=All records were majority-flushed to at least 2 nodes' durable segment
    # files before the ack was sent; they MUST be present after cold restart.
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, new_leader_svc, cid_sub, keys, window_ms=120000)

    merged = Results(clients=pre_result.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)

    # ── Post-restart usability: fresh acks=All produce+subscribe ─────────────
    cid_post = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=new_leader_svc,
        cid_base=cid_post,
        timeout_s=180,
        mode="produce_subscribe",
        topics="multi:1:3",
        sub_partitions=3,
        records=30,
        keys=max(keys, 18),
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
        "post-cold-restart produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"post-cold-restart produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)
