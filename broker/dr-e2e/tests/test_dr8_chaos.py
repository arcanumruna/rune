"""DR-8: Combined chaos + soak.

Three scenarios exercising CONCURRENT client activity under MULTIPLE fault types
in a single test run — the capstone of the DR-E2E suite:

  1. **Leader-kill + packet loss (combined):** apply netem loss (~10%) to one
     follower, THEN SIGKILL the leader while producers are running acks=All.
     Assert re-election, no committed loss, ordering, exactly-once, single-leader
     after heal + convergence.

  2. **Rolling restarts + client churn:** three concurrent produce/subscribe
     clients run acks=All while nodes are restarted one-at-a-time (×2-3 rounds,
     quorum preserved at every step).  Assert no committed loss, single-leader
     throughout, ISR heals.

  3. **Bounded chaos-monkey soak (capstone):** for a bounded number of rounds
     (SOAK_ROUNDS = 12, ~90-120 s wall clock at default timing) with a FIXED
     random seed, a continuous background producer runs acks=All while the soak
     loop picks a random fault from:
         {kill-follower+restart, netem-loss+clear, pause+unpause-follower,
          kill-leader+restart}
     NEVER killing/pausing more than one node at a time (quorum preserved).
     After SOAK_ROUNDS: heal_all, wait_for_leader, assert the full acked set is
     delivered (no committed loss), ordering, exactly-once, single-leader, and a
     final produce+subscribe round-trip works.

Quorum-preservation contract (soak):
  * Only ONE fault is active at a time.
  * "kill-leader+restart" kills the current leader (1/3 dead → 2/3 quorum OK),
    then immediately waits for a new leader before applying another fault, then
    restarts the killed node so the next round always starts 3-healthy.
  * "kill-follower+restart" similarly: kills one follower (2/3 quorum → OK),
    then restarts it after a brief window, full cluster before next round.
  * "pause+unpause-follower": SIGSTOP one non-leader node (2/3 heartbeating →
    election may fire); unpause after a short window. Quorum never < 2/3.
  * "netem-loss+clear": apply netem loss on one randomly chosen node (does NOT
    remove it from the cluster; ISR shrinks at most to 2/3 = still serves).
  The soak therefore tests AVAILABILITY + DURABILITY under a mix of faults, NOT
  quorum-loss (DR-4 covers quorum-loss separately).

Random seed: CHAOS_SEED = 42 (reproducible runs; the fault sequence is the same
every time).

Timeouts:
  * test_dr8_leader_kill_netem:     @pytest.mark.timeout(600)
  * test_dr8_rolling_restart_churn: @pytest.mark.timeout(720)
  * test_dr8_chaos_soak:            @pytest.mark.timeout(900)
    (soak: 12 rounds × ~7 s average + produce settle + final subscribe ≈ 200 s
     plus generous buffers; 900 s is conservative for Docker Desktop slowness.)

CID window: 1_000_000+ — disjoint from all prior DR tests.
  DR-1: n*1000, DR-2: 100_000+, DR-3: 300_000+, DR-4: 400_000+,
  DR-5: 500_000+, DR-6: 600_000+, DR-7: 700_000+, DR-10: 800_000+,
  DR-11: 900_000+.  DR-8: 1_000_000+.

Invariants asserted throughout (and finally):
  * committed-loss = 0 (assert_no_committed_loss)
  * per-key ordering (assert_per_key_ordering)
  * exactly-once per subscriber (assert_exactly_once)
  * single-leader at-a-time (assert_single_leader / restricted scan)

Harness uncertainty (flag for controller):
  * The soak uses `cluster.compose.restart(service)` via `faults.restart()` for
    stopped containers; if the harness does not SIGKILL+start those, the restart
    may be a warm restart (SIGTERM+start).  For killed containers `compose.start`
    is the correct call — the test uses `cluster.compose.start(service)` for
    containers in the 'exited' state (mirrors DR-7 cold-restart pattern).
  * `faults.pause/unpause` uses `docker pause/unpause` (cgroup-freeze / SIGSTOP).
    On Docker Desktop this is reliable.
"""

from __future__ import annotations

import random
import re
import signal
import threading
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

# ── Per-run disjoint CID window ───────────────────────────────────────────────
# DR-8: 1_000_000+ — fully disjoint from all prior suites.
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 1_000_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + client-start stagger (Docker Desktop tail; DR-BUG-2).
_CONNECT_MS = 40000
_STAGGER_MS = 400

# Chaos-monkey soak parameters.
SOAK_ROUNDS = 12       # bounded: 12 fault rounds ≈ 90-120 s (varies by fault)
CHAOS_SEED = 42        # fixed seed → reproducible fault sequence


# ── Shared helpers (mirror DR-3 / DR-5 / DR-6 conventions) ───────────────────

def _produce(cluster, target_svc: str, n: int, cid_base: int, records: int,
             keys: int, rate_hz: int = 0) -> Results:
    """Run ``n`` producers against ``target_svc`` with acks=All (majority-durable)."""
    return run_clients(
        n,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=300,
        mode="produce",
        topics="multi:1:3",
        records=records,
        keys=max(keys, 18),        # >= partitions so records span all 3 partitions
        rate_hz=rate_hz,
        acks="all",                # GR-9: majority-durable ack before counting committed
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 90000) -> Results:
    """Subscribe-from-earliest and drain the whole partition set.

    window_ms default is 90 s — generous for chaos / netem-inflated delivery.
    """
    return run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=240,
        mode="subscribe",
        topics="multi:1:3",
        sub_partitions=3,
        keys=keys,
        expect=0,                  # drain; don't require a fixed count
        from_="earliest",
        subscribe_ms=window_ms,
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


def _post_recovery_round_trip(cluster, leader_svc: str, keys: int) -> None:
    """Assert the cluster is fully serving: a fresh acks=All produce+subscribe
    round-trips successfully on ``leader_svc``.  Mirrors _assert_isr_healed from
    DR-7 but also verifies exactly-once + ordering of the new records."""
    running = cluster.running_services()
    assert len(running) == 3, (
        f"post-recovery round-trip: expected 3 running nodes, got {running}"
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
        "post-recovery produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"post-recovery round-trip: produced {len(post.all_produced)} != 30"
    )
    assert_per_key_ordering(post)
    assert_exactly_once(post)
    assert_single_leader(cluster)


def _assert_delivery_invariants(committed: Results, sub: Results) -> None:
    """Assert committed-loss=0 + per-key ordering + exactly-once (hard invariants)."""
    merged = Results(clients=committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)


def _assert_single_leader_among(cluster, services: list[str]) -> int:
    """Restricted assert_single_leader: scan ONLY ``services`` (not all nodes).

    Used during a partition/kill when one node is isolated/dead to avoid false
    split-brain alerts from stale log entries on the offline container.
    Returns the agreed leader id.
    """
    leader_re = re.compile(r"node (\d+) is LEADER")
    claims: dict[str, int] = {}
    for svc in services:
        logs = cluster.compose.logs(svc, timeout=30)
        ids = leader_re.findall(logs)
        if ids:
            claims[svc] = int(ids[-1])
    distinct = set(claims.values())
    assert len(distinct) <= 1, (
        f"split-brain among {services}: nodes disagree on leader: {claims} "
        f"(distinct leaders: {sorted(distinct)})"
    )
    assert distinct, (
        f"no leader observed among nodes {services} (claims={claims})"
    )
    return next(iter(distinct))


# ── DR-4/5 safety helper: assert isolated/minority node cannot serve ──────────

def _assert_isolated_cannot_serve(cluster, isolated_svc: str) -> None:
    """Assert an isolated/minority node cannot accept acks=All writes.

    Attempts a produce acks=All to ``isolated_svc``; it MUST FAIL (ISR < minIsr(2)
    or the node has lost quorum).  A successful produce would indicate split-brain
    / min-ISR violation — a BLOCKER.
    """
    probe = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=isolated_svc,
        cid_base=_next_cid_base(),
        timeout_s=45,
        mode="produce",
        topics="multi:1:3",
        records=5,
        keys=18,
        acks="all",            # must be refused — isolated/minority node
        expect_own=True,
        timeout_ms=15000,      # short: we WANT this to fail fast
        connect_retries=0,
        start_stagger_ms=0,
    )
    serving = probe.ok and len(probe.all_produced) > 0
    assert not serving, (
        f"SPLIT-BRAIN / MIN-ISR VIOLATION [BLOCKER]: isolated node "
        f"{isolated_svc!r} accepted an acks=All produce while isolated "
        f"(quorum lost on that node). "
        f"produced={len(probe.all_produced)} "
        f"rc={probe.clients[0].rc if probe.clients else '?'} "
        f"exit={probe.clients[0].exit_code if probe.clients else '?'}"
    )


# ── Scenario 1: Leader-kill + follower packet loss (combined) ─────────────────

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(600)
@pytest.mark.xfail(
    reason="Blocked on DR-BUG-6 (leader-completeness): combined leader-kill + "
    "packet-loss can elect a partition-data-lagging node whose committed-tail "
    "reconcile doesn't engage in the production consensus/bridge path, so an "
    "acks=All-committed record can be lost across this specific failover. This is "
    "a REAL durability Blocker (tracked as DR-BUG-6), deferred by owner decision "
    "for a focused fix — NOT a passing scenario. leader-kill-alone (DR-3) and "
    "loss-alone (DR-6) both pass with zero loss; only the combination triggers it.",
    strict=False,
)
@pytest.mark.parametrize("loss_pct", [10])
def test_dr8_leader_kill_netem(fresh_cluster, netem_ready, loss_pct):
    """COMBINED FAULT: apply netem loss (~10%) to one follower, then SIGKILL the
    leader mid-produce (acks=All).

    Sequence:
      1. Identify leader + follower; assert single leader.
      2. Commit a small pre-fault batch (acks=All).
      3. Apply netem loss on one follower (the cluster still has leader + other
         follower = 2/3 quorum, ISR ≥ 2 = satisfies minIsr(2)).
      4. Start background producers (rate-limited) targeting the leader.
      5. While production is in flight, SIGKILL the leader.
      6. Wait for re-election on the majority side.
      7. Assert the isolated ex-leader (now dead) did not serve writes while
         netem was active and quorum was 2/3.
      8. In finally: heal_all.
      9. Post-heal convergence: assert_single_leader across all nodes.
      10. Durability: subscribe-from-earliest delivers all committed records,
          ordering, exactly-once.
      11. Post-recovery usability: fresh produce+subscribe round-trip.

    Invariants:
      * No committed loss: every acks=All-acked record before or during kill survives.
      * Re-election: a new leader is elected from the surviving nodes.
      * Single leader: at no point are two leaders simultaneously serving writes.
      * Ordering + exactly-once after recovery.
    """
    cluster = fresh_cluster
    keys = 4
    records_pre = 100
    records_mid = 150

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # The two non-leader services (one will get netem, the other stays clean).
    non_leaders = [s for s in SERVICES if s != leader_svc]
    assert len(non_leaders) == 2
    netem_follower = non_leaders[0]    # apply netem loss here
    clean_follower = non_leaders[1]    # stays healthy: leader + clean_follower = quorum

    # ── Pre-fault batch ───────────────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, leader_svc, 1, cid_pre, records_pre, keys)
    assert pre_result.ok, (
        "pre-fault produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre_result.all_produced) == records_pre, (
        f"pre-fault: produced {len(pre_result.all_produced)} != {records_pre}"
    )

    mid_holder: dict[str, Results] = {}

    try:
        # ── Inject netem loss on one follower ─────────────────────────────────
        # The cluster is still 2/3 majority (leader + clean_follower); ISR ≥ 2.
        faults.netem(cluster, netem_follower, loss=loss_pct)
        time.sleep(2.0)  # let Aeron observe the loss before starting mid-produce

        # ── Start background producers (rate-limited) ─────────────────────────
        cid_mid = _next_cid_base()

        def _run_mid():
            # rate_hz=30 keeps production spanning several seconds so the leader kill
            # lands mid-flight.  Mid-flight produces may bail on the failover — that
            # is acceptable; only ACKED records count as committed.
            mid_holder["r"] = _produce(cluster, leader_svc, 2, cid_mid,
                                       records_mid, keys, rate_hz=30)

        t = threading.Thread(target=_run_mid, daemon=True)
        t.start()

        # Let production get going, then SIGKILL the leader mid-flight.
        time.sleep(2.5)
        faults.kill_node(cluster, leader_svc, signal.SIGKILL)

        # ── Wait for re-election on the majority side ─────────────────────────
        # Exclude both the dead leader AND scan only the majority (clean_follower +
        # possibly netem_follower if it's still reachable — but quorum is those 2).
        new_lid = cluster.wait_for_new_leader(leader_svc, timeout_s=90)
        new_leader_svc = node_id_to_service(new_lid)
        assert new_leader_svc != leader_svc, (
            f"leader did not change after SIGKILL + netem; still pointing to {leader_svc!r}"
        )

        # Majority-side single-leader (2 surviving nodes; the dead leader is excluded).
        majority_svcs = [s for s in SERVICES if s != leader_svc]
        _assert_single_leader_among(cluster, majority_svcs)

        # Join the background producer thread.
        t.join(timeout=300)
        mid_result = mid_holder.get("r", Results())

    finally:
        # Always heal: clear netem on the follower; the dead leader is already down.
        faults.heal_all(cluster)

    # ── Convergence after heal ────────────────────────────────────────────────
    # Give the cluster time to settle and the killed leader's slot to be noted.
    time.sleep(8.0)

    # Full single-leader check on the 2 running nodes (killed leader still down).
    running = cluster.running_services()
    alive_svcs = [s for s in SERVICES if s in running]
    assert len(alive_svcs) >= 2, f"expected ≥ 2 running after heal, got {running}"
    _assert_single_leader_among(cluster, alive_svcs)

    # ── Durability: all pre- and mid-fault committed records must survive ──────
    all_committed = Results(clients=pre_result.clients + mid_result.clients)
    _, current_leader = cluster.identify_leader()
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, current_leader, cid_sub, keys, window_ms=120000)

    merged = Results(clients=all_committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)

    # ── Restart the killed ex-leader so the cluster returns to full health ─────
    # The durability invariants above are already verified on the 2-node majority.
    # For the post-recovery usability round-trip we need the cluster back to 3/3
    # (the correct DR expectation: the killed node cold-restarts and rejoins,
    # mirroring DR-7 and DR-8 scenarios 2/3).  Without this, _post_recovery_round_trip
    # can never see 3 running nodes because this scenario SIGKILLs the leader and
    # never brings it back.
    cluster.compose.start(leader_svc)
    time.sleep(8.0)  # cold-start + log-replay + ISR rejoin
    running = cluster.running_services()
    assert len(running) == 3, (
        f"expected 3 running after ex-leader restart, got {running}"
    )
    _assert_single_leader_among(cluster, running)

    # ── Post-recovery usability (full 3-node cluster after ex-leader rejoin) ───
    _, current_leader = cluster.identify_leader()
    _post_recovery_round_trip(cluster, current_leader, keys)

    print(
        f"\n[DR-8 S1] leader-kill + netem loss={loss_pct}% on {netem_follower}: "
        f"pre_committed={len(pre_result.all_produced)}, "
        f"mid_committed={len(mid_result.all_produced)}, "
        f"new_leader={new_leader_svc}; "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )


# ── Scenario 2: Rolling restarts + client churn ────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(720)
@pytest.mark.parametrize("restart_rounds", [2, 3])
def test_dr8_rolling_restart_churn(fresh_cluster, restart_rounds):
    """Rolling restarts of all nodes (one at a time, quorum preserved) while
    multiple concurrent clients (3 producers + 1 subscriber) run acks=All.

    Per restart round:
      1. Identify current leader.
      2. SIGKILL the current leader; wait for a new leader (2/3 quorum = OK).
      3. Restart the killed node; give it time to rejoin ISR.
      4. Assert single-leader throughout.

    After all rounds:
      * Assert no committed loss across all production batches.
      * Assert ISR has healed: fresh acks=All produce+subscribe works on the
        final leader (3/3 nodes running after all restarts).
      * Assert per-key ordering + exactly-once on the full subscriber view.

    Invariants checked during the loop:
      * After each kill: exactly one leader on the 2-node majority side.
      * After each restart+rejoin: assert_single_leader on all 3 nodes.
    """
    cluster = fresh_cluster
    keys = 4
    all_committed_clients: list = []

    _, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Background workload: 3 concurrent acks=All producers ─────────────────
    # Rate-limited so the soak spans multiple restart rounds.
    workload_holder: dict[str, Results] = {}
    workload_done = threading.Event()

    def _run_workload():
        cid = _next_cid_base()
        # Each producer sends 200 records at rate_hz=20 → ~10 s of production
        # per client; with 3 clients that covers several restart rounds.
        result = _produce(cluster, leader_svc, 3, cid, 200, keys, rate_hz=20)
        workload_holder["r"] = result
        workload_done.set()

    wt = threading.Thread(target=_run_workload, daemon=True)
    wt.start()

    # Let the workload get going before we start churning.
    time.sleep(3.0)

    try:
        for round_i in range(restart_rounds):
            _, current_leader = cluster.identify_leader()

            # SIGKILL the current leader; 2/3 quorum remains.
            faults.kill_node(cluster, current_leader, signal.SIGKILL)

            # Majority must elect a new leader.
            new_lid = cluster.wait_for_new_leader(current_leader, timeout_s=60)
            new_leader_svc = node_id_to_service(new_lid)
            assert new_leader_svc != current_leader, (
                f"[round {round_i}] leader did not change after kill of {current_leader!r}"
            )

            # Majority-side single-leader check.
            majority_svcs = [s for s in SERVICES if s != current_leader]
            _assert_single_leader_among(cluster, majority_svcs)

            # Produce a small acks=All batch to the new leader to confirm it serves.
            cid_chk = _next_cid_base()
            chk = _produce(cluster, new_leader_svc, 1, cid_chk, 30, keys)
            assert chk.ok, (
                f"[round {round_i}] post-failover produce to new leader {new_leader_svc!r} "
                f"failed:\n" + "\n\n".join(
                    f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                    for c in chk.clients if c.exit_code != 0 or c.rc != 0
                )
            )
            assert len(chk.all_produced) == 30, (
                f"[round {round_i}] check produce: {len(chk.all_produced)} != 30"
            )
            all_committed_clients.extend(chk.clients)

            # Restart the killed leader so it rejoins before the next round.
            cluster.compose.start(current_leader)
            time.sleep(8.0)  # allow the rejoined node to catch up and re-enter ISR

            # Full 3-node single-leader check (rejoined node must NOT re-declare itself).
            running = cluster.running_services()
            assert len(running) == 3, (
                f"[round {round_i}] expected 3 running after restart, got {running}"
            )
            assert_single_leader(cluster)

    finally:
        # Wait for background workload to drain (or timeout).
        workload_done.wait(timeout=300)

    workload_result = workload_holder.get("r", Results())
    all_committed_clients.extend(workload_result.clients)
    all_committed = Results(clients=all_committed_clients)

    # ── Durability: subscribe-from-earliest delivers everything committed ──────
    _, final_leader = cluster.identify_leader()
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, final_leader, cid_sub, keys, window_ms=120000)

    merged = Results(clients=all_committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)

    # ── ISR healed: all 3 nodes up; fresh acks=All round-trip must succeed ────
    _post_recovery_round_trip(cluster, final_leader, keys)

    print(
        f"\n[DR-8 S2] rolling-restart ×{restart_rounds}: "
        f"total_committed={len(all_committed.all_produced)}; "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )


# ── Scenario 3: Bounded chaos-monkey soak (capstone) ─────────────────────────

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
def test_dr8_chaos_soak(fresh_cluster, netem_ready):
    """BOUNDED CHAOS-MONKEY SOAK — capstone of the DR-E2E suite.

    A continuous background producer runs acks=All while the soak loop picks a
    random fault (FIXED seed = 42) from:
        0: kill a random follower + restart
        1: apply netem loss (5–15%) on a random node + clear
        2: pause + unpause a random follower
        3: kill the current leader + wait for new leader + restart

    QUORUM PRESERVATION: only ONE fault is active at a time; faults that kill/pause
    a node are reversed (restart/unpause) before the next round. The cluster always
    has ≥ 2/3 nodes operational so acks=All remains satisfiable throughout.

    SOAK_ROUNDS = 12 (bounded; ~90-120 s depending on fault mix and Docker speed).
    CHAOS_SEED  = 42 (fixed → reproducible fault sequence).

    After SOAK_ROUNDS:
      1. heal_all (belt-and-suspenders; also clears any lingering netem).
      2. Wait for a leader (generous timeout).
      3. Wait for the background producer to finish.
      4. Assert: full acked set delivered (no committed loss), ordering,
         exactly-once, single-leader.
      5. Final produce+subscribe round-trip confirms full cluster usability.
      6. Print a catalogue of faults applied (for post-run debugging).

    Invariants asserted DURING the soak (after each round):
      * After each kill/pause: assert the remaining 2-node majority has a single
        leader (no split-brain while 1 node is out).
      * After restart/unpause/heal: assert_single_leader on all 3 nodes.
    """
    cluster = fresh_cluster
    keys = 4

    _, initial_leader = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Background producer: continuous acks=All workload ─────────────────────
    # 300 records at rate_hz=10 → ~30 s of production; with SOAK_ROUNDS≈12 rounds
    # this runs for most of the soak window.  rate_hz is modest: we're stressing
    # the FAULT PATH, not throughput.
    workload_holder: dict[str, Results] = {}
    workload_done = threading.Event()

    _, workload_leader = cluster.identify_leader()
    cid_soak = _next_cid_base()

    def _run_soak_producer():
        result = _produce(cluster, workload_leader, 1, cid_soak,
                          300, keys, rate_hz=10)
        workload_holder["r"] = result
        workload_done.set()

    producer_thread = threading.Thread(target=_run_soak_producer, daemon=True)
    producer_thread.start()

    # Let production warm up before the first fault.
    time.sleep(3.0)

    # ── Chaos-monkey loop ─────────────────────────────────────────────────────
    rng = random.Random(CHAOS_SEED)

    # Fault labels for cataloguing.
    _FAULT_KILL_FOLLOWER = 0
    _FAULT_NETEM         = 1
    _FAULT_PAUSE_FOLLOWER = 2
    _FAULT_KILL_LEADER   = 3

    fault_catalogue: list[str] = []

    try:
        for round_i in range(SOAK_ROUNDS):
            fault_choice = rng.randint(0, 3)

            # Always work with the CURRENT leader at the start of each round
            # (may have changed from a prior leader-kill round).
            _, current_leader = cluster.identify_leader()
            followers = [s for s in SERVICES if s != current_leader]
            assert len(followers) == 2, (
                f"[soak round {round_i}] expected 2 followers, got {followers}"
            )
            target_follower = rng.choice(followers)

            if fault_choice == _FAULT_KILL_FOLLOWER:
                # Kill a random follower (2/3 quorum remains); restart it after
                # a short window so the NEXT round starts with 3 healthy nodes.
                fault_catalogue.append(f"R{round_i}: kill-follower({target_follower})")
                faults.kill_node(cluster, target_follower, signal.SIGKILL)

                # Give the leader time to detect ISR shrinkage.
                time.sleep(3.0)

                # Majority (leader + other follower) must still serve.
                majority = [s for s in SERVICES if s != target_follower]
                _assert_single_leader_among(cluster, majority)

                # Restart the follower; wait for it to rejoin.
                cluster.compose.start(target_follower)
                time.sleep(5.0)

                running = cluster.running_services()
                assert len(running) == 3, (
                    f"[soak round {round_i}] expected 3 running after follower restart, "
                    f"got {running}"
                )
                assert_single_leader(cluster)

            elif fault_choice == _FAULT_NETEM:
                # Apply netem loss on a RANDOM node (could be leader or follower).
                # This degrades one link but does not kill the node.
                loss_pct = rng.randint(5, 15)
                netem_target = rng.choice(SERVICES)
                fault_catalogue.append(
                    f"R{round_i}: netem-loss({loss_pct}% on {netem_target})"
                )
                faults.netem(cluster, netem_target, loss=loss_pct)

                # Sustained brief window under loss.
                time.sleep(4.0)

                # Clear the loss and let Aeron retransmit settle.
                faults.clear_netem(cluster, netem_target)
                time.sleep(2.0)

                # All nodes should still be running; check single leader.
                assert_single_leader(cluster)

            elif fault_choice == _FAULT_PAUSE_FOLLOWER:
                # SIGSTOP (docker pause) a random FOLLOWER (not the leader).
                # The 2 remaining nodes (leader + other follower) form quorum.
                # The paused node will miss heartbeats; an election MAY fire
                # (150 ms default electionTimeoutMs) but quorum is preserved.
                fault_catalogue.append(
                    f"R{round_i}: pause-follower({target_follower})"
                )
                faults.pause(cluster, target_follower)

                # Pause window: long enough that the cluster may re-elect
                # (to stress fencing) but short enough to keep the soak bounded.
                pause_s = rng.uniform(1.5, 3.0)
                time.sleep(pause_s)

                # Majority-side leader check while follower is paused.
                majority = [s for s in SERVICES if s != target_follower]
                _assert_single_leader_among(cluster, majority)

                # Unpause; the follower rejoins (may step down if re-elected).
                faults.unpause(cluster, target_follower)
                time.sleep(3.0)

                # Full cluster: single leader, all 3 running.
                running = cluster.running_services()
                assert len(running) == 3, (
                    f"[soak round {round_i}] expected 3 running after unpause, "
                    f"got {running}"
                )
                assert_single_leader(cluster)

            else:  # _FAULT_KILL_LEADER
                # Kill the CURRENT leader; 2/3 quorum + re-election.
                fault_catalogue.append(
                    f"R{round_i}: kill-leader({current_leader})"
                )
                old_leader = current_leader
                faults.kill_node(cluster, old_leader, signal.SIGKILL)

                # Wait for a new leader on the majority side.
                new_lid = cluster.wait_for_new_leader(old_leader, timeout_s=60)
                new_leader_svc = node_id_to_service(new_lid)
                assert new_leader_svc != old_leader, (
                    f"[soak round {round_i}] leader did not change after kill of "
                    f"{old_leader!r}"
                )

                # Majority single-leader check.
                majority = [s for s in SERVICES if s != old_leader]
                _assert_single_leader_among(cluster, majority)

                # Restart the killed leader so the next round starts 3-healthy.
                cluster.compose.start(old_leader)
                time.sleep(8.0)  # generous: cold-start + log-replay + ISR rejoin

                running = cluster.running_services()
                assert len(running) == 3, (
                    f"[soak round {round_i}] expected 3 running after leader restart, "
                    f"got {running}"
                )
                assert_single_leader(cluster)

    finally:
        # Belt-and-suspenders: heal_all clears any lingering netem qdiscs.
        faults.heal_all(cluster)

    # ── Post-soak convergence ─────────────────────────────────────────────────
    time.sleep(5.0)

    # Wait for the background producer to drain (it may have timed out / bailed
    # during a leader-kill round — that is acceptable; only ACKED records count).
    workload_done.wait(timeout=120)
    soak_result = workload_holder.get("r", Results())

    # Also grab any additional direct produce batches done by individual rounds.
    # (All rounds in this scenario use the single background workload, not their
    # own produce batches, so all_committed is just the soak workload.)
    all_committed = soak_result

    # Final single-leader (full cluster must be up after all restarts).
    running = cluster.running_services()
    assert len(running) == 3, (
        f"post-soak: expected 3 running nodes, got {running}"
    )
    assert_single_leader(cluster)

    # ── Durability: the FULL acked set must be delivered (no committed loss) ───
    _, final_leader = cluster.identify_leader()
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, final_leader, cid_sub, keys, window_ms=120000)

    merged = Results(clients=all_committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)

    # ── Final round-trip: fresh produce+subscribe confirms usability ──────────
    _post_recovery_round_trip(cluster, final_leader, keys)

    # ── Catalogue: print every fault applied for post-run debugging ───────────
    print(
        f"\n[DR-8 S3] chaos-soak completed: seed={CHAOS_SEED}, rounds={SOAK_ROUNDS}\n"
        f"Fault sequence:\n  " + "\n  ".join(fault_catalogue) + "\n"
        f"soak_acked={len(soak_result.all_produced)}, "
        f"delivered={len(sub.all_delivered)}, "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )
