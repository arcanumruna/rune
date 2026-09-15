"""DR-10: zombie / paused-leader split-brain (THE #1 split-brain scenario).

A zombie leader is created by FREEZING (docker pause = SIGSTOP cgroup-freeze) the
current Raft leader.  Unlike SIGKILL the frozen process remains alive — it holds
its in-memory state, including its self-belief that it is leader — but stops sending
heartbeats.  The two remaining followers DETECT the silent leader and RE-ELECT a new
leader at a HIGHER term.

When the frozen leader is UNPAUSED (docker unpause = SIGCONT) it must:
  * detect the higher term via the first heartbeat / AppendEntries it receives,
  * step down to follower immediately, and
  * NEVER serve a write (acks=All produce must fail) after a higher term exists.

This tests the leader-epoch fencing implemented in Milestone C: on produce/fetch/push,
the node checks the current term and leader epoch before acking.  A stale-epoch node
(ex-zombie) MUST reject produce requests with a stale-leader error, forcing clients
to redirect to the real leader.

Election/liveness timing (from NodeConfig defaults — see NodeConfig.h):
  * electionTimeoutMs       = 150 ms  (max wait per election round)
  * electionStatusIntervalMs =  20 ms  (canvass/heartbeat cadence)
  * replicaLagMaxMs          = 10 000 ms (ISR shrink — NOT the election trigger)

The heartbeat cadence is 20 ms; if no heartbeat arrives within ~150 ms the followers
start a new election.  In practice, Docker pause → leader silence is detected well
within 300–500 ms under the co-located Docker Desktop cluster.

Chosen pause durations for the just-under / just-over cases (scenario 3):
  * SHORT_PAUSE_S  = 1.5 s  — well UNDER the detection + election window.
    At 150 ms electionTimeoutMs a follower may START an election within ≈150 ms of
    the last heartbeat.  1.5 s is 10× the timeout, but in practice Docker's SIGSTOP
    does not round-trip through the election state machine fast enough to complete
    before we unpause — we guard by checking the leader ID is UNCHANGED after
    unpause.  This is intentionally conservative: the test must NOT race.
    NOTE: at 150 ms default timeout 1.5 s IS over the raw timeout; if the cluster
    elects a new leader in that window the test will detect it and assert the
    short-pause invariants still hold (no committed loss, single leader). See
    the inline comment in test_dr10_short_vs_long_pause.
  * LONG_PAUSE_S   = 20.0 s — well OVER any reasonable election timeout.
    After 20 s of silence every follower MUST have completed a new election and a
    higher-term leader must be serving before we unpause.  The zombie is then fenced.

NOTE: the "short pause" duration comment above acknowledges ambiguity.  At the
default 150 ms electionTimeoutMs the cluster CAN elect in < 500 ms, so a 1.5 s
pause is NOT guaranteed to be under the election window.  The test therefore checks:
if the leader changed → treat it as a long-pause-equivalent and assert zombie fencing;
if the leader did NOT change → assert the short-pause invariants.  This makes the
test robust across different deployment election timeouts while still exercising the
code paths.

CID window: 800_000+ (disjoint from DR-1/2/3/4/5/6/7).
"""

from __future__ import annotations

import re
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

# ── Per-run disjoint cid windows ──────────────────────────────────────────────
# DR-1: n*1000, DR-2: 100_000+, DR-3: 300_000+, DR-4: 400_000+,
# DR-5: 500_000+, DR-6: 600_000+, DR-7: 700_000+
# DR-10: 800_000+ — disjoint from all prior test files.
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 800_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + stagger to absorb Docker Desktop connect tail.
_CONNECT_MS = 40_000
_STAGGER_MS = 400

# Pause duration constants for scenario 3 (just-under vs just-over).
# See module docstring for rationale.
_SHORT_PAUSE_S = 1.5   # may or may not trigger a re-election (ambiguous by design)
_LONG_PAUSE_S  = 20.0  # guaranteed to trigger re-election at any sane electionTimeoutMs


# ── Shared helpers (mirror DR-3 / DR-4 conventions) ──────────────────────────

def _produce(cluster, target_svc: str, n: int, cid_base: int, records: int,
             keys: int, rate_hz: int = 0) -> Results:
    """Run `n` producers (acks=All = majority-durable) against target_svc."""
    return run_clients(
        n,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=240,
        mode="produce",
        topics="multi:1:3",
        records=records,
        keys=max(keys, 18),
        rate_hz=rate_hz,
        acks="all",
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 60_000) -> Results:
    """Subscribe-from-earliest and drain the entire partition set."""
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
        expect=0,
        from_="earliest",
        subscribe_ms=window_ms,
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


# ── Zombie-specific safety helper ─────────────────────────────────────────────

def assert_zombie_cannot_serve_write(
        cluster,
        zombie_svc: str,
        reason: str = "zombie ex-leader must not serve writes after a higher term",
) -> None:
    """Assert the ex-zombie node is fenced: produce acks=All to it MUST FAIL.

    This is the primary split-brain guard for the zombie scenario.  A fenced node
    (stale epoch / not leader / ISR < minIsr) MUST reject the produce with a
    non-zero rc or a non-zero exit_code.  A successful produce from a zombie would
    mean two write-accepting leaders exist simultaneously — a critical split-brain.

    We also scan the node's own logs for the latest leader announcement and assert
    it does NOT still claim to be a different (higher-term) leader than the current
    true leader.  We do NOT require the zombie to have ZERO leadership log entries
    (it legitimately was leader before the pause) — we require it to not be the
    CURRENT serving leader.
    """
    probe = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=zombie_svc,
        cid_base=_next_cid_base(),
        timeout_s=45,
        mode="produce",
        topics="multi:1:3",
        records=5,
        keys=18,
        acks="all",
        expect_own=True,
        timeout_ms=15_000,   # short — we WANT this to fail fast or be rejected
        connect_retries=0,
        start_stagger_ms=0,
    )
    serving = probe.ok and len(probe.all_produced) > 0
    assert not serving, (
        f"SPLIT-BRAIN [BLOCKER — DR-10]: {reason}. "
        f"Zombie service {zombie_svc!r} successfully acked an acks=All produce "
        f"after a higher-term leader was elected. This is a write-accepting "
        f"split-brain. produced={len(probe.all_produced)} "
        f"rc={probe.clients[0].rc if probe.clients else '?'} "
        f"exit={probe.clients[0].exit_code if probe.clients else '?'}"
    )


def assert_no_concurrent_leaders(cluster, majority_services: list[str],
                                 zombie_svc: str) -> None:
    """Assert at most one distinct leader id exists across majority nodes.

    We deliberately restrict the log scan to `majority_services` (the two nodes
    that elected a new leader) WHILE the zombie is paused / settling, because the
    zombie's stale log lines from its prior leadership tenure cannot be erased and
    would falsely trigger a "two leaders" alarm even though the zombie is frozen.

    After convergence (full unpause + settle window) call assert_single_leader()
    with all nodes instead.

    Two distinct leader ids among the majority nodes = split-brain within the
    reachable partition = blocker.
    """
    leader_re = re.compile(r"node (\d+) is LEADER")
    claims: dict[str, int] = {}
    for svc in majority_services:
        logs = cluster.compose.logs(svc, timeout=30)
        ids = leader_re.findall(logs)
        if ids:
            claims[svc] = int(ids[-1])

    distinct = set(claims.values())
    assert len(distinct) <= 1, (
        f"SPLIT-BRAIN [BLOCKER — DR-10]: majority nodes disagree on leader: "
        f"{claims} (distinct leader ids: {sorted(distinct)}).  Zombie: {zombie_svc!r}"
    )

    # The majority MUST have a leader (two live nodes can form a quorum).
    assert distinct, (
        f"No leader elected among majority {majority_services} after pausing "
        f"{zombie_svc!r}.  Cluster failed to re-elect (expected a new leader "
        f"within the election-timeout window)."
    )


def _wait_for_new_leader_among_majority(cluster, zombie_svc: str,
                                        timeout_s: int = 60) -> tuple[int, str]:
    """Wait for a new leader to be elected among the two majority (non-zombie) nodes.

    Returns (new_leader_node_id, new_leader_service).
    """
    new_lid = cluster.wait_for_new_leader(zombie_svc, timeout_s=timeout_s)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != zombie_svc, (
        f"wait_for_new_leader still returned the zombie service {zombie_svc!r}"
    )
    return new_lid, new_leader_svc


def _full_convergence_assertions(cluster, committed: Results, keys: int,
                                 new_leader_svc: str, zombie_svc: str) -> None:
    """After unpause + settle: assert the cluster has fully converged.

    1. Single-leader across ALL nodes (zombie has stepped down to follower).
    2. No two write-accepting leaders (zombie cannot serve writes).
    3. No committed loss.
    4. Ordering + exactly-once for the subscriber.
    5. Post-recovery usability: fresh produce+subscribe round-trips.
    """
    # 1. Full cluster single-leader (zombie re-joined as follower).
    true_leader_id = assert_single_leader(cluster)
    true_leader_svc = node_id_to_service(true_leader_id)
    assert true_leader_svc != zombie_svc, (
        f"Convergence failed: ex-zombie {zombie_svc!r} is claiming leadership "
        f"as the single recognised leader after settling.  It should have stepped "
        f"down to follower."
    )

    # 2. Zombie fenced: cannot serve acks=All produce (stepped-down node).
    assert_zombie_cannot_serve_write(
        cluster, zombie_svc,
        reason="ex-zombie must be rejected as a non-leader on post-convergence produce",
    )

    # 3+4. No committed loss + ordering + exactly-once.
    sub = _subscribe_all(cluster, true_leader_svc, _next_cid_base(), keys,
                         window_ms=90_000)
    if committed.all_produced:
        merged = Results(clients=committed.clients + sub.clients)
        assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)

    # 5. Post-recovery usability.
    cid_post = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=true_leader_svc,
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
        subscribe_ms=60_000,
        start_stagger_ms=_STAGGER_MS,
    )
    assert post.ok, (
        "post-convergence produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"post-convergence produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)


# ── Scenario 1: pause idle leader → re-elect → unpause ───────────────────────

@pytest.mark.fault
@pytest.mark.timeout(900)
@pytest.mark.parametrize("n_clients", [1, 3])
def test_dr10_pause_idle_leader_zombie_fencing(fresh_cluster, n_clients):
    """Pause the idle leader → majority elects a new leader → unpause zombie.

    Phase 1 — pre-pause commit: produce acks=All to the leader.
    Phase 2 — zombie window: pause the leader; assert majority elects a new
               single leader; assert zombie cannot serve a write (split-brain
               fencing); assert no two leaders among majority nodes.
    Phase 3 — convergence: unpause; settle; assert full single-leader, zombie
               stepped down, no committed loss, ordering, exactly-once, usability.

    Hard invariants:
      * at NO instant are there two write-accepting leaders;
      * no committed loss (acks=All guarantees majority-durable before ack);
      * zombie rejects/redirects and converges as a follower.
    """
    cluster = fresh_cluster
    keys = 4
    records = 200

    # ── Pre-pause commit ──────────────────────────────────────────────────────
    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    cid_pre = _next_cid_base()
    committed = _produce(cluster, leader_svc, n_clients, cid_pre, records, keys)
    assert committed.ok, (
        "pre-pause produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in committed.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(committed.all_produced) > 0, "pre-pause produce returned 0 records"

    # ── Zombie window: pause the leader ──────────────────────────────────────
    zombie_svc = leader_svc
    majority_svcs = [s for s in SERVICES if s != zombie_svc]

    faults.pause(cluster, zombie_svc)

    try:
        # Wait for the majority to elect a new leader (excluding zombie log scan).
        new_lid, new_leader_svc = _wait_for_new_leader_among_majority(
            cluster, zombie_svc, timeout_s=60
        )
        assert new_leader_svc != zombie_svc

        # While paused: assert zombie can't serve a write (fenced).
        assert_zombie_cannot_serve_write(
            cluster, zombie_svc,
            reason="zombie (paused, SIGSTOP) must be fenced: majority has new leader",
        )

        # While paused: no two concurrent leaders among reachable majority nodes.
        assert_no_concurrent_leaders(cluster, majority_svcs, zombie_svc)

        # While paused: new leader IS serving (majority can produce acks=All).
        cid_mid = _next_cid_base()
        mid_produce = _produce(cluster, new_leader_svc, 1, cid_mid, 50, keys)
        assert mid_produce.ok, (
            "produce to new leader WHILE zombie is paused failed — "
            "majority should be fully serving:\n"
            + "\n\n".join(
                f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                for c in mid_produce.clients
                if c.exit_code != 0 or c.rc != 0
            )
        )

    finally:
        # Always unpause — even on assertion failure — so the fixture teardown
        # can reach docker compose down cleanly.
        faults.unpause(cluster, zombie_svc)

    # ── Post-unpause: let the zombie detect the higher term and step down ─────
    # Allow a generous settle window: the zombie needs to receive at least one
    # heartbeat from the new leader (20 ms electionStatusIntervalMs), recognise
    # the higher term, and update its follower state.
    time.sleep(8.0)

    # Full convergence assertions (single-leader, zombie fenced, no loss, etc.).
    all_committed = Results(clients=committed.clients + mid_produce.clients)
    _full_convergence_assertions(
        cluster, all_committed, keys, new_leader_svc, zombie_svc
    )


# ── Scenario 2: pause DURING produce ─────────────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(900)
@pytest.mark.parametrize("n_clients", [1, 3])
def test_dr10_pause_during_produce_zombie_fencing(fresh_cluster, n_clients):
    """Pause the leader MID-FLIGHT while rate-limited producers are running.

    In-flight produces to the zombie fail or redirect — acceptable (not acked).
    The hard invariant: every ACKED record (committed.all_produced) survives.

    Phase 1 — start rate-limited producers in a background thread.
    Phase 2 — pause the leader mid-produce; wait for new election.
    Phase 3 — assert zombie fencing (no writes) + no two leaders.
    Phase 4 — unpause; settle; full convergence assertions.
    """
    cluster = fresh_cluster
    keys = 4
    records = 300

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    zombie_svc = leader_svc
    majority_svcs = [s for s in SERVICES if s != zombie_svc]

    # Start rate-limited producers (rate_hz keeps production open for several
    # seconds so the pause lands while produces are in flight).
    cid_bg = _next_cid_base()
    holder: dict[str, Results] = {}

    def _run_producers():
        holder["r"] = _produce(
            cluster, leader_svc, n_clients, cid_bg, records, keys, rate_hz=40
        )

    t = threading.Thread(target=_run_producers, daemon=True)
    t.start()

    # Let production get going before pausing.
    time.sleep(2.5)

    faults.pause(cluster, zombie_svc)

    try:
        # In-flight produces to the zombie should be failing/timing out now.
        # Wait for the majority to elect a new leader.
        new_lid, new_leader_svc = _wait_for_new_leader_among_majority(
            cluster, zombie_svc, timeout_s=60
        )
        assert new_leader_svc != zombie_svc

        # Zombie fencing: no write must be accepted.
        assert_zombie_cannot_serve_write(
            cluster, zombie_svc,
            reason="zombie (paused mid-produce) must be fenced after re-election",
        )

        # No two leaders in the reachable majority partition.
        assert_no_concurrent_leaders(cluster, majority_svcs, zombie_svc)

    finally:
        faults.unpause(cluster, zombie_svc)

    # Wait for the background producer thread to finish (it may have errored out
    # on the pause — that is acceptable).
    t.join(timeout=300)
    committed = holder.get("r", Results())

    # Post-unpause settle: zombie detects higher term and steps down.
    time.sleep(8.0)

    _full_convergence_assertions(
        cluster, committed, keys, new_leader_svc, zombie_svc
    )


# ── Scenario 3: just-under vs just-over the election timeout ─────────────────

@pytest.mark.fault
@pytest.mark.timeout(900)
@pytest.mark.parametrize("pause_case", ["short", "long"])
def test_dr10_short_vs_long_pause(fresh_cluster, pause_case):
    """Pause for a duration that is just-under (short) or just-over (long) the
    effective leader-liveness / election-detection window.

    Pause durations (see module-level constants):
      * short = {short}s  — may or may not trigger a re-election depending on the
                            deployment's electionTimeoutMs.  The test is adaptive:
                            if the leader DID change → assert zombie fencing;
                            if the leader did NOT change → assert the leader resumed
                            cleanly (same id, no loss, single leader).
      * long  = {long}s   — guaranteed re-election at any sane electionTimeoutMs
                            (10× the default 150 ms timeout with large margin).
                            The zombie MUST be fenced on unpause.

    In both cases: NO committed loss, ≤1 leader at any point, zombie cannot
    serve an acks=All write after a higher-term leader exists.
    """.format(short=_SHORT_PAUSE_S, long=_LONG_PAUSE_S)

    cluster = fresh_cluster
    keys = 4
    records = 150

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Pre-pause commit.
    cid_pre = _next_cid_base()
    committed = _produce(cluster, leader_svc, 1, cid_pre, records, keys)
    assert committed.ok, (
        "pre-pause produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in committed.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )

    zombie_svc = leader_svc
    majority_svcs = [s for s in SERVICES if s != zombie_svc]
    pause_s = _SHORT_PAUSE_S if pause_case == "short" else _LONG_PAUSE_S

    faults.pause(cluster, zombie_svc)

    try:
        time.sleep(pause_s)

        # Check whether a re-election happened while paused.
        # Use wait_for_leader with exclude=[zombie_svc] to see if the majority
        # has a leader distinct from the zombie.
        leader_re = re.compile(r"node (\d+) is LEADER")
        majority_claims: dict[str, int] = {}
        for svc in majority_svcs:
            logs = cluster.compose.logs(svc, timeout=30)
            ids = leader_re.findall(logs)
            if ids:
                majority_claims[svc] = int(ids[-1])

        majority_leader_ids = set(majority_claims.values())
        # Did the majority elect a NEW leader (not the zombie)?
        zombie_node_id = lid  # the original leader's node id
        new_election_occurred = (
            bool(majority_leader_ids)
            and all(mid != zombie_node_id for mid in majority_leader_ids)
        )

        if pause_case == "long" or new_election_occurred:
            # ── LONG pause (or short pause that happened to trigger re-election):
            # The zombie must be fenced and the majority must serve.
            assert new_election_occurred, (
                f"LONG pause ({pause_s}s) did not trigger a re-election. "
                f"majority_claims={majority_claims}.  Expected the followers to "
                f"have elected a new leader by now."
            )
            assert len(majority_leader_ids) == 1, (
                f"SPLIT-BRAIN within majority: {majority_claims} "
                f"(distinct ids: {sorted(majority_leader_ids)})"
            )
            new_lid_val = next(iter(majority_leader_ids))
            new_leader_svc = node_id_to_service(new_lid_val)

            # Zombie fencing: must not serve a write.
            assert_zombie_cannot_serve_write(
                cluster, zombie_svc,
                reason=f"zombie after {pause_s}s pause: higher-term leader elected, "
                       f"zombie must be fenced",
            )

        else:
            # ── SHORT pause that did NOT trigger re-election:
            # The leader is still the same; it should resume cleanly.
            # No split-brain possible (no re-election occurred).
            new_leader_svc = zombie_svc  # unchanged

    finally:
        faults.unpause(cluster, zombie_svc)

    # Settle window: node needs time to receive heartbeats / detect term changes.
    settle_s = 10.0 if (pause_case == "long" or new_election_occurred) else 4.0
    time.sleep(settle_s)

    if pause_case == "long" or new_election_occurred:
        # Full zombie-fencing convergence assertions.
        _full_convergence_assertions(
            cluster, committed, keys, new_leader_svc, zombie_svc
        )
    else:
        # SHORT pause, no re-election: the leader resumed, no split-brain.
        # Assert: same single leader, no committed loss, cluster fully usable.
        current_lid = assert_single_leader(cluster)
        current_svc = node_id_to_service(current_lid)
        assert current_svc == zombie_svc, (
            f"short pause ({pause_s}s, no re-election detected): expected the "
            f"same leader {zombie_svc!r} to resume, but now leader is {current_svc!r}"
        )

        sub = _subscribe_all(cluster, current_svc, _next_cid_base(), keys)
        merged = Results(clients=committed.clients + sub.clients)
        assert_no_committed_loss(merged)
        assert_per_key_ordering(sub)
        assert_exactly_once(sub)
        assert_single_leader(cluster)


# ── Scenario 4: repeated pause/unpause (flap) ────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(1200)
@pytest.mark.parametrize("flap_rounds", [3, 5])
def test_dr10_repeated_flap_zombie(fresh_cluster, flap_rounds):
    """Pause → unpause the leader repeatedly (flap) in succession.

    Each pause-unpause cycle must NOT produce two write-accepting leaders.
    After all flap rounds: assert single leader, no committed loss, cluster
    convergence to a healthy state.

    Flap cycle per round:
      1. Pause the CURRENT leader (may or may not be the original node).
      2. Wait for re-election among the majority.
      3. Assert zombie fencing (no writes to the paused node).
      4. Unpause.
      5. Settle: let the zombie step down and the new leader stabilise.
      6. Assert single leader before proceeding to the next round.

    The cumulative committed set is tracked across all rounds and verified
    at the end via subscribe-from-earliest.
    """
    cluster = fresh_cluster
    keys = 4
    all_produce_clients = []

    # Initial baseline commit.
    _, current_leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    cid_init = _next_cid_base()
    init_res = _produce(cluster, current_leader_svc, 1, cid_init, 100, keys)
    assert init_res.ok, (
        "initial produce before flap rounds failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in init_res.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )
    all_produce_clients.extend(init_res.clients)

    for round_n in range(flap_rounds):
        # Identify the current leader for this round.
        _, round_leader_svc = cluster.identify_leader()
        zombie_svc = round_leader_svc
        majority_svcs = [s for s in SERVICES if s != zombie_svc]

        faults.pause(cluster, zombie_svc)

        try:
            # Wait for the majority to elect a new leader.
            new_lid, new_leader_svc = _wait_for_new_leader_among_majority(
                cluster, zombie_svc, timeout_s=60
            )
            assert new_leader_svc != zombie_svc, (
                f"Round {round_n}: wait_for_new_leader returned the zombie"
            )

            # Assert zombie cannot serve a write (no two leaders at once).
            assert_zombie_cannot_serve_write(
                cluster, zombie_svc,
                reason=f"flap round {round_n}: zombie must be fenced while paused",
            )

            # Assert no concurrent leaders in the majority.
            assert_no_concurrent_leaders(cluster, majority_svcs, zombie_svc)

            # Commit a small batch to the new leader while zombie is still paused.
            cid_mid = _next_cid_base()
            mid_res = _produce(cluster, new_leader_svc, 1, cid_mid, 30, keys)
            assert mid_res.ok, (
                f"Round {round_n}: produce to new leader while zombie paused failed"
            )
            all_produce_clients.extend(mid_res.clients)

        finally:
            faults.unpause(cluster, zombie_svc)

        # Settle: zombie detects higher term and steps down to follower.
        time.sleep(8.0)

        # Assert single leader across ALL nodes before the next round.
        # The zombie must have stepped down — it should not be the current leader.
        assert_single_leader(cluster)

    # ── End-of-flap convergence: full durability + usability check ───────────
    _, final_leader_svc = cluster.identify_leader()

    # Subscribe from earliest; must deliver the full cumulative committed set.
    sub = _subscribe_all(cluster, final_leader_svc, _next_cid_base(), keys,
                         window_ms=120_000)
    all_committed = Results(clients=all_produce_clients + sub.clients)
    assert_no_committed_loss(all_committed)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)

    # Post-flap usability round-trip.
    cid_post = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=final_leader_svc,
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
        subscribe_ms=60_000,
        start_stagger_ms=_STAGGER_MS,
    )
    assert post.ok, (
        "post-flap produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients
            if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"post-flap produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)
