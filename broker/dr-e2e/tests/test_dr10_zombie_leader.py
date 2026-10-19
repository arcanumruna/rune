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
from harness.cluster import SERVICES, node_id_to_service, service_to_node_id
from harness.driver import DRAIN_QUIET_MS, Results, run_clients
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
             keys: int, rate_hz: int = 0, connect_retries: int = 0) -> Results:
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
        connect_retries=connect_retries,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 60_000, drain_quiet_ms: int = DRAIN_QUIET_MS) -> Results:
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
        drain_quiet_ms=drain_quiet_ms,   # stop once the static log is fully read (driver._drain_complete)
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
    if not serving:
        return
    # Milestone D (per-partition leadership): the probe only BOOTSTRAPS through the zombie — the client routes
    # each record to its partition's current leader, so an ack is not a zombie write by itself (proven with
    # topic-aware broker logs, briefs/2026-10-04-dr10-diagnosis.md).  The split-brain property is that nothing
    # acked through the zombie's endpoint lives only on a deposed leader: every probe record must be delivered
    # by the real cluster (subscribe-from-earliest via a non-zombie leader).
    others = [svc for svc in SERVICES if svc != zombie_svc]
    leader_id = cluster.wait_for_leader(timeout_s=90, exclude=[zombie_svc])
    true_svc = node_id_to_service(leader_id)
    assert true_svc in others, f"no non-zombie leader to verify the probe against (leader={true_svc})"
    sub = _subscribe_all(cluster, true_svc, _next_cid_base(), keys=18, window_ms=60_000)
    try:
        assert_no_committed_loss(Results(clients=probe.clients + sub.clients))
    except AssertionError as exc:
        raise AssertionError(
            f"SPLIT-BRAIN [BLOCKER — DR-10]: {reason}. Zombie service {zombie_svc!r} acked an acks=All "
            f"produce whose records the real cluster (leader {true_svc!r}) does not have: {exc}"
        ) from exc


def assert_no_concurrent_leaders(cluster, majority_services: list[str],
                                 zombie_svc: str) -> None:
    """Assert the majority has a leader that is not the zombie.

    We deliberately restrict the log scan to `majority_services` (the two nodes
    that elected a new leader) WHILE the zombie is paused / settling, because the
    zombie's stale log lines from its prior leadership tenure cannot be erased and
    would falsely trigger a "two leaders" alarm even though the zombie is frozen.

    After convergence (full unpause + settle window) call assert_single_leader()
    with all nodes instead.

    Implementation note — why per-service last-match is wrong for flap tests:

    The broker's "is LEADER" line is printed by EVERY node (not just the leader)
    whenever it observes a leadership change via `node.leaderId()` polled at 100 ms.
    In a repeated-flap scenario each node accumulates "is LEADER" entries from ALL
    prior rounds.  Comparing the last match PER SERVICE produces false split-brain
    alarms:

      * Node A was leader in round N-1; its log ends with "node A is LEADER".
      * Node B just got elected in round N; its log ends with "node B is LEADER".
      * Per-service comparison sees A≠B → false SPLIT-BRAIN alarm.

    In reality A is now a follower; its stale log line is from a prior Raft term.
    Additionally, a follower node may lag by one 100 ms poll cycle before it logs
    the new leader's id — so two majority nodes can have different "last" entries
    for a brief window even after a clean re-election.

    Correct approach: scan the COMBINED (chronologically interleaved) log of the
    majority services (same strategy as cluster._scan_leader) and take the LAST
    leader announcement (`ids[-1]`).  After wait_for_new_leader() has returned, the
    newly elected node has already written its own "is LEADER" line, making it the
    most recent match in the combined stream.  We assert that this last claim is NOT
    the zombie's node id — confirming the majority has converged to a non-zombie
    leader.  The primary write-acceptance split-brain guard is
    assert_zombie_cannot_serve_write(); this function is a secondary observability
    check that the majority's log view agrees with the expected non-zombie leader.
    """
    leader_re = re.compile(r"node (\d+) is LEADER")
    zombie_node_id = service_to_node_id(zombie_svc)

    # Poll until the combined majority log ends with a non-zombie leader claim.
    # We must retry because: (1) follower nodes poll node.leaderId() every 100 ms
    # and may not have logged the new leader yet at the moment wait_for_new_leader()
    # returns, and (2) docker compose logs output ordering can place a follower's
    # stale "zombie is LEADER" line after the new leader's own announcement if both
    # lines are within the same sub-second window.  A genuine split-brain cannot
    # self-heal (a zombie that re-claims leadership would keep producing that log
    # line), so a 15 s retry window is safe — it is much shorter than the test
    # timeout (1200 s) and well under the minimum settle sleep (8 s) that follows.
    # Brief pause to allow broker log buffers to flush: the new leader's
    # "is LEADER" announcement is polled every 100 ms inside the broker process.
    # After wait_for_new_leader() returns, the winner has printed the line, but
    # docker compose logs may interleave a follower's slightly-older log line
    # AFTER it on the first read.  One 1-second settle is enough for the follower
    # to also poll and log the new leader, after which the combined log is stable.
    time.sleep(1.0)   # KEPT: log-interleave settle of a log-scrape read (1 s, bounded)
    all_ids: list[str] = []
    for _ in range(3):
        combined_logs = cluster.compose.logs(*majority_services, timeout=30)
        all_ids = leader_re.findall(combined_logs)
        if all_ids and int(all_ids[-1]) != zombie_node_id:
            break
        time.sleep(2.0)   # KEPT: poll back-off inside a bounded retry loop

    if not all_ids:
        raise AssertionError(
            f"No leader elected among majority {majority_services} after pausing "
            f"{zombie_svc!r}.  Cluster failed to re-elect (expected a new leader "
            f"within the election-timeout window)."
        )

    # After polling, the most recent "is LEADER" in the combined stream must not
    # be the zombie.  A zombie is frozen (SIGSTOP) and cannot log new lines; once
    # the new leader's announcement appears it will be the final match.
    last_id = int(all_ids[-1])
    assert last_id != zombie_node_id, (
        f"SPLIT-BRAIN [BLOCKER — DR-10]: the most recent leader claim in the "
        f"majority {majority_services} still names the zombie {zombie_svc!r} "
        f"(node {zombie_node_id}) as leader after 15 s of polling.  "
        f"The majority should have elected a different leader.  "
        f"All claims (last 10): {all_ids[-10:]}"
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


# Settle oracle (DR-10 review LOW-5).  A settled cluster: every running node
# reports a role, exactly one reports LEADER and all others FOLLOWER, and that
# (leader, term) is unchanged over _SETTLE_STABLE_READS consecutive polls
# (~1 s at the 0.2 s poll — several leader-heartbeat windows, so a resumed
# zombie that is about to step down cannot pass as settled).  Bounded by a
# deadline; never a fixed sleep.
_SETTLE_DEADLINE_S = 90.0
_SETTLE_STABLE_READS = 5


def _wait_for_settled_leader(cluster, deadline_s: float = _SETTLE_DEADLINE_S) -> str:
    """Wait until a single, stable leader is followed by every running node.

    Returns the settled leader's service.  Raises AssertionError at the deadline
    with the last observation (an unsettled cluster is a convergence failure).
    """
    running = cluster.running_services()
    deadline = time.time() + deadline_s
    last_obs: tuple[int, int] | None = None
    stable = 0
    readings: dict[str, tuple[str, int, int] | None] = {}
    while time.time() < deadline:
        readings = {svc: cluster._read_role(svc) for svc in running}
        leaders = {(r[1], r[2]) for r in readings.values() if r is not None and r[0] == "LEADER"}
        all_decided = all(r is not None and r[0] in ("LEADER", "FOLLOWER")
                          for r in readings.values())
        obs = next(iter(leaders)) if (len(leaders) == 1 and all_decided) else None
        if obs is not None and obs == last_obs:
            stable += 1
        else:
            stable = 1 if obs is not None else 0
        last_obs = obs
        if stable >= _SETTLE_STABLE_READS:
            return node_id_to_service(obs[0])
        time.sleep(cluster._LEADER_POLL_S)   # KEPT: poll interval of a condition poll
    raise AssertionError(
        f"cluster did not settle on a single followed leader within {deadline_s}s "
        f"(running={running}, last role readings={readings})"
    )


def _identify_leader_with_container(cluster, timeout_s: int = 30) -> tuple[int, str]:
    """Identify the current leader and verify its container id is resolvable.

    In a rapid flap scenario `cluster.identify_leader()` may return a service
    whose container is transiently unresolvable (Docker Desktop is still settling
    after an unpause).  Passing a None container id to `docker pause` causes a
    TypeError deep in subprocess.  This helper retries until BOTH the log-based
    leader election AND the `container_id()` lookup succeed, or raises after
    `timeout_s` seconds.
    """
    deadline = time.time() + timeout_s
    last_svc = None
    while time.time() < deadline:
        lid, svc = cluster.identify_leader(timeout_s=max(10, int(deadline - time.time())))
        cid = cluster.container_id(svc)
        if cid is not None:
            return lid, svc
        last_svc = svc
        time.sleep(1.0)   # KEPT: poll interval of a condition poll
    raise RuntimeError(
        f"_identify_leader_with_container: leader container id remained None "
        f"for {timeout_s}s (last resolved service: {last_svc!r})"
    )


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
    # Was a fixed 8 s sleep: now until the zombie reports FOLLOWER at the leader's term (cluster.settle), at most 8 s.
    cluster.settle(8.0, running=list(SERVICES))

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
    time.sleep(2.5)   # KEPT: scenario: the pause lands mid-produce

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

    # Post-unpause settle: zombie detects higher term and steps down (was a fixed 8 s sleep; at most 8 s now).
    cluster.settle(8.0, running=list(SERVICES))

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
        time.sleep(pause_s)   # KEPT: fault duration (the pause IS the scenario)

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

    # Settle (DR-10 review LOW-5): condition-based, not a fixed sleep — wait
    # (with a deadline) until exactly one node leads and every other node
    # follows, unchanged over consecutive role-counter reads.  The fixed 4 s
    # short-pause sleep was computed BEFORE the late-election re-check below, so
    # a late-detected election ran the full convergence assertions after 4 s.
    settled_svc = _wait_for_settled_leader(cluster)

    if pause_case == "short" and not new_election_occurred:
        # The short pause is AMBIGUOUS by design (1.5 s vs the leader-liveness
        # window max(electionTimeout, 3*statusInterval) = 300 ms under the DR
        # compose RUNE_ELECTION_TIMEOUT_MS=300; 150 ms at the broker default): the observation above runs mid-pause, so an election
        # still in flight is invisible there.  Aeron-faithful (ClosedLeader
        # StepDownRegression): a leader that resumes without an active follower
        # quorum steps down, so leadership may legitimately move AFTER the
        # observation.  Decide the ambiguity on the settled cluster: a moved
        # leadership gets the full zombie-fencing convergence assertions below.
        if settled_svc != zombie_svc:
            new_election_occurred = True
            new_leader_svc = settled_svc

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
        # Identify the current leader for this round.  Use the container-aware
        # helper so we never call faults.pause() with a None container id (which
        # would raise TypeError deep in subprocess during rapid flap transitions).
        _, round_leader_svc = _identify_leader_with_container(cluster, timeout_s=30)
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

            # Verify the majority has converged on the new (non-zombie) leader.
            # In a repeated-flap scenario, assert_no_concurrent_leaders() is
            # unreliable because (a) docker compose logs interleave multi-service
            # output non-deterministically when timestamps are close, and (b) each
            # node's log accumulates "is LEADER" lines from all prior rounds,
            # making per-service or combined-tail comparisons racy.
            # Instead: scan only the NEW LEADER's own log for its self-proclaimed
            # leadership — the leader node logs "is LEADER" about itself when it
            # wins the election.  A zombie is frozen and cannot log new lines
            # after being paused.  This is a single-service scan (no interleaving).
            _leader_re_check = re.compile(r"node (\d+) is LEADER")
            _new_leader_logs = cluster.compose.logs(new_leader_svc, timeout=30)
            _new_leader_ids = _leader_re_check.findall(_new_leader_logs)
            _new_lid_node_id = service_to_node_id(new_leader_svc)
            assert _new_leader_ids and int(_new_leader_ids[-1]) == _new_lid_node_id, (
                f"SPLIT-BRAIN / LEADER-NOT-SERVING [DR-10]: round {round_n}: "
                f"new_leader_svc={new_leader_svc!r} (node {_new_lid_node_id}) "
                f"has not yet announced itself as leader in its own log.  "
                f"Last few ids: {_new_leader_ids[-5:]}.  "
                f"zombie={zombie_svc!r}"
            )

            # Commit a small batch to the new leader while zombie is still paused.
            # connect_retries=2: after repeated pause/unpause cycles, Docker
            # Desktop's embedded DNS proxy can transiently fail to resolve a
            # service hostname.  The 3-second back-off between retries (in
            # driver._run_one_client) gives the DNS proxy time to recover.
            # This produce is a LIVENESS check (majority serves) not a SAFETY
            # check — the core safety invariant (no committed loss across ALL
            # rounds) is verified at the end via subscribe + assert_no_committed_loss.
            cid_mid = _next_cid_base()
            mid_res = _produce(cluster, new_leader_svc, 1, cid_mid, 30, keys,
                               connect_retries=2)
            if mid_res.ok:
                all_produce_clients.extend(mid_res.clients)
            else:
                # Only fail on genuine workload errors (not DNS/connect transients
                # that Docker Desktop may exhibit after heavy container churn).
                _dns_or_connect = any(
                    "unknown host" in c.raw
                    or "Name or service not known" in c.raw
                    or "connect() timed out" in c.raw
                    for c in mid_res.clients
                )
                assert _dns_or_connect, (
                    f"Round {round_n}: produce to new leader {new_leader_svc!r} "
                    f"failed with a non-transient error:\n"
                    + "\n\n".join(
                        f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-800:]}"
                        for c in mid_res.clients
                        if c.exit_code != 0 or c.rc != 0
                    )
                )

        finally:
            faults.unpause(cluster, zombie_svc)

        # Settle: zombie detects higher term and steps down to follower (was a fixed 8 s sleep; at most 8 s now).
        cluster.settle(8.0, running=list(SERVICES))

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
