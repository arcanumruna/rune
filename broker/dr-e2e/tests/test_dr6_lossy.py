"""DR-6: packet loss + latency (netem loss/delay) scenarios.

Verifies that Aeron's reliability layer — combined with the broker's
push-on-append replication + fetch backstop — preserves end-to-end
correctness under sustained packet loss and added delay on broker links.

Hard invariants (asserted in every case):
  * **Committed-loss = 0**: every record that received an acks=All
    ProduceResponse MUST arrive at a subscriber (no committed loss).
    This is iron-clad: Milestone C guarantees majority-flush before ack.
  * **Per-key ordering**: records delivered to a subscriber arrive in
    strictly increasing seq order per (cid, key).
  * **Exactly-once**: no committed record is delivered more than once to
    the same subscriber.

Election-stability approach:
  * At LOW loss (≤ 10 %) on a follower link: committed-loss=0 + ordering +
    exactly-once are asserted, AND the leader must NOT change (no spurious
    re-election from moderate follower-link loss).
  * At HIGHER loss (20 %) on a follower link and at any loss level on the
    LEADER's link: committed-loss=0 + ordering + exactly-once remain the
    absolute requirement.  A spurious re-election at elevated loss is
    tolerated and documented (its occurrence is logged) — the hard invariant
    is that committed loss stays ZERO and there is exactly ONE leader at any
    time (single-leader-at-a-time, not leader-never-changes).
  * After heal_all + convergence sleep: assert_single_leader() is called
    unconditionally.

Parametrized matrix:
  target   × loss_pct × delay_ms  → total cases
  follower   5, 10, 20   0, 20    → 6 cases × rep[0,1] = 12 cases
  leader     5, 10        20, 50  → 4 cases × rep[0,1] =  8 cases
  ─────────────────────────────────────────────────────────────────
  Total: 20 parametrized cases (bounded runtime; ~150-200 records
  per case; subscribe windows generous for lossy delivery).

NOTE on record counts: netem delay slows Aeron's retransmission windows,
so per-case record counts are kept small (~150-200) to bound wall-clock
runtime while still exercising the loss + reorder space.

CID window: 600_000+ (disjoint from DR-1..5 which occupy < 600_000).
"""

from __future__ import annotations

import re
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
# DR-1: n*1000, DR-2: 100_000+, DR-3: 300_000+, DR-4: 400_000+, DR-5: 500_000+
# DR-6: 600_000+ — fully disjoint from all prior suites.
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 600_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + client-start stagger (Docker Desktop tail; see
# DR-BUG-2).  Under netem delay these budgets must remain generous because
# Aeron retransmit + the delayed channel inflate round-trip times.
_CONNECT_MS = 40000
_STAGGER_MS = 400


# ── Shared helpers (mirror DR-3 / DR-5 conventions) ──────────────────────────

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
                   window_ms: int = 90000, drain_quiet_ms: int = DRAIN_QUIET_MS) -> Results:
    """Subscribe-from-earliest and drain the whole partition set.

    window_ms is intentionally generous (default 90s) because netem loss +
    delay slows Aeron's retransmission and inflate delivery times.
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
        drain_quiet_ms=drain_quiet_ms,   # stop once the static log is fully read (driver._drain_complete)
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


def _leader_id_from_logs(cluster, services: list[str]) -> int | None:
    """Return the most-recently-announced leader id across ``services``, or None."""
    leader_re = re.compile(r"node (\d+) is LEADER")
    latest: list[int] = []
    for svc in services:
        logs = cluster.compose.logs(svc, timeout=30)
        ids = leader_re.findall(logs)
        if ids:
            latest.append(int(ids[-1]))
    return latest[-1] if latest else None


def _assert_delivery_invariants(committed: Results, sub: Results) -> None:
    """Assert committed-loss=0 + per-key ordering + exactly-once.

    These three are the HARD invariants for every DR-6 case, regardless of
    loss intensity, target, or whether a spurious re-election occurred.
    """
    merged = Results(clients=committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)


# ── Scenario 1: follower-link loss + delay ────────────────────────────────────
#
# Apply netem(follower, loss=L, delay=D) WHILE sustaining produce/subscribe to
# the leader.  The cluster still has 2/3 quorum (leader + one other healthy
# follower) so acks=All must continue to succeed.
#
# Election-stability contract:
#   LOW loss (≤ 10 %): assert leader id is unchanged (no spurious re-election).
#   HIGH loss (20 %):  log if a re-election occurs; do NOT fail on it, but still
#                      assert committed-loss=0, ordering, exactly-once, and
#                      single-leader-at-a-time after heal.
#
# Matrix: loss_pct in (5, 10, 20) × delay_ms in (0, 20) × rep in (0, 1)
#         = 12 cases.

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
@pytest.mark.parametrize("rep", [0, 1])
@pytest.mark.parametrize("delay_ms", [0, 20])
@pytest.mark.parametrize("loss_pct", [5, 10, 20])
def test_dr6_follower_lossy(fresh_cluster, netem_ready, loss_pct, delay_ms, rep):
    """Follower-link packet loss + optional delay during sustained produce/subscribe.

    Hard invariants:
      * committed-loss = 0 (Aeron reliability + fetch/push retransmit absorb loss)
      * per-key ordering preserved
      * exactly-once per subscriber
    Election-stability:
      * at loss ≤ 10 %: leader must NOT change (moderate follower-link loss must
        not trigger a spurious election).
      * at loss = 20 %: a re-election is tolerated and documented; committed-loss
        stays ZERO regardless.
    Heal + convergence: always heal_all; assert_single_leader across all nodes.
    """
    cluster = fresh_cluster
    keys = 4
    records = 200

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Identify one follower to degrade; the other follower + leader form quorum.
    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc, "follower_service() returned the leader"

    # The non-degraded third node (stays healthy, part of majority with leader).
    third_svc = next(s for s in SERVICES if s not in (leader_svc, follower_svc))

    # ── Pre-loss baseline: commit a small batch ───────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, 50, keys)
    assert pre.ok, (
        "pre-loss produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) > 0, "pre-loss produce returned 0 records"

    election_occurred = False
    try:
        # ── Inject netem on the follower ──────────────────────────────────────
        # loss + optional delay; clear_netem on this svc in finally.
        netem_kwargs: dict[str, object] = {"loss": loss_pct}
        if delay_ms > 0:
            netem_kwargs["delay"] = delay_ms
        faults.netem(cluster, follower_svc, **netem_kwargs)  # type: ignore[arg-type]

        # Brief settle — give Aeron's loss-detection a moment to observe the link.
        # KEPT fixed: the produce must run on an already-degraded link (part of the scenario).
        time.sleep(2.0)

        # ── Sustained produce during follower loss ────────────────────────────
        # Leader + healthy third form 2/3 quorum; acks=All must still succeed.
        cid_mid = _next_cid_base()
        committed = _produce(cluster, leader_svc, 1, cid_mid, records, keys)
        assert committed.ok, (
            f"mid-loss produce (follower loss={loss_pct}% delay={delay_ms}ms) failed — "
            "2/3 quorum must keep serving acks=All while one follower is lossy:\n"
            + "\n\n".join(
                f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                for c in committed.clients if c.exit_code != 0 or c.rc != 0
            )
        )
        assert len(committed.all_produced) == records, (
            f"produced {len(committed.all_produced)} != {records} "
            f"(loss={loss_pct}% delay={delay_ms}ms on {follower_svc})"
        )

        # ── Concurrent subscribe (from-earliest) while loss is active ─────────
        # Subscriber targets the leader; Aeron retransmits lost fetch responses.
        cid_sub = _next_cid_base()
        sub_live = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=120000)

        # ── Election-stability check ──────────────────────────────────────────
        lid_now = _leader_id_from_logs(cluster, [leader_svc, third_svc])
        if lid_now is not None and lid_now != lid_initial:
            election_occurred = True
            if loss_pct <= 10:
                # At low loss, the leader MUST NOT change — Aeron heartbeat loss
                # detection must not be triggered by follower-link loss alone.
                raise AssertionError(
                    f"SPURIOUS ELECTION at low follower loss={loss_pct}%: "
                    f"leader changed from node {lid_initial} → node {lid_now}. "
                    f"Aeron heartbeating must tolerate {loss_pct}% link loss "
                    f"without triggering a leader timeout."
                )
            # At loss > 10 %, tolerate and document.
            print(
                f"\n[DR-6 note] spurious election observed at follower "
                f"loss={loss_pct}% delay={delay_ms}ms "
                f"(node {lid_initial} → node {lid_now}); "
                f"quantifying loss threshold — hard invariant is committed-loss=0."
            )

    finally:
        # Always heal — belt-and-suspenders on top of fresh_cluster teardown.
        faults.heal_all(cluster)

    # ── Convergence: give the follower time to catch up ───────────────────────
    # After netem clear the follower will reconnect and fetch missed records (was a fixed 10 s sleep; the checks
    # below are unchanged and only require a single leader + delivery).
    cluster.settle(10.0, running=list(SERVICES))
    assert_single_leader(cluster)

    # ── Delivery invariants — the hard guarantees regardless of election ───────
    # Use the live subscribe result already collected above.  If a re-election
    # happened, the subscriber may have had its connection reset — that is
    # acceptable as long as every COMMITTED record is ultimately delivered.
    # The subscribe window above (120s) is generous enough for retransmits.
    _assert_delivery_invariants(committed, sub_live)

    # ── Post-heal full-durability subscribe from earliest ────────────────────
    # Confirm the pre-loss batch is also intact in the healed cluster.
    all_committed = Results(clients=pre.clients + committed.clients)
    # Determine the current leader (may have changed if election occurred).
    _, current_leader = cluster.identify_leader()
    cid_sub2 = _next_cid_base()
    sub_post = _subscribe_all(cluster, current_leader, cid_sub2, keys, window_ms=120000)
    merged = Results(clients=all_committed.clients + sub_post.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # ── Post-heal usability: fresh produce+subscribe round-trip ──────────────
    cid_post = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=current_leader,
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
        "post-heal produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"post-heal produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)

    # Document outcome for the test log.
    print(
        f"\n[DR-6 follower] loss={loss_pct}% delay={delay_ms}ms rep={rep}: "
        f"committed={len(committed.all_produced)} records, "
        f"election={'YES (documented)' if election_occurred else 'NO (stable)'}; "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )


# ── Scenario 2: leader-link loss + delay ─────────────────────────────────────
#
# Apply netem on the LEADER's own eth0 — this degrades both the leader→follower
# replication channel AND the leader↔client channel.  Loss is kept lower
# (5, 10 %) to avoid triggering election at the majority of cases; a 50ms delay
# is added as a separate axis.
#
# At LOW loss (≤ 10 % + delay ≤ 20ms): assert leader id is UNCHANGED.
# At HIGHER loss/delay: a re-election is tolerated; committed-loss MUST be 0.
#
# Matrix: loss_pct in (5, 10) × delay_ms in (20, 50) × rep in (0, 1)
#         = 8 cases.

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
@pytest.mark.parametrize("rep", [0, 1])
@pytest.mark.parametrize("delay_ms", [20, 50])
@pytest.mark.parametrize("loss_pct", [5, 10])
def test_dr6_leader_lossy(fresh_cluster, netem_ready, loss_pct, delay_ms, rep):
    """Leader-link packet loss + delay during sustained produce/subscribe.

    Degrading the leader's own link simultaneously affects:
      * the client→leader RPC channel (produce latency / retransmits),
      * the leader→follower push-on-append replication channel.
    Aeron's reliability guarantees retransmit for both.

    Hard invariants (always):
      * committed-loss = 0
      * per-key ordering
      * exactly-once

    Election-stability:
      * loss=5% + delay=20ms: leader MUST NOT change (low-intensity stability).
      * loss=10% or delay=50ms: a re-election is tolerated and documented; the
        hard invariant (committed-loss=0 + single-leader-at-a-time) stays.
    """
    cluster = fresh_cluster
    keys = 4
    records = 150    # lower than follower case: leader-link delay inflates RTT

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Pre-loss baseline ─────────────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, 30, keys)
    assert pre.ok, (
        "pre-loss produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) > 0, "pre-loss produce returned 0 records"

    election_occurred = False
    new_leader_svc = leader_svc
    try:
        # ── Inject netem on the LEADER's link ────────────────────────────────
        faults.netem(cluster, leader_svc, loss=loss_pct, delay=delay_ms)

        # Brief settle.  KEPT fixed: the produce must run on an already-degraded link (part of the scenario).
        time.sleep(2.0)

        # ── Sustained produce while leader link is lossy ──────────────────────
        # Aeron retransmits: the producer must still get acks=All for every
        # record it commits; it may be slower, but committed-loss must be 0.
        cid_mid = _next_cid_base()
        committed = _produce(cluster, leader_svc, 1, cid_mid, records, keys)
        # NOTE: if a re-election occurs mid-produce the client may bail with a
        # connection error (acceptable — only ACKED records are in the committed
        # set).  We do NOT assert committed.ok here for the high-delay/loss case
        # because the client may legitimately timeout on a degraded leader link.
        # We assert committed-loss=0 on whatever was acked.
        if not committed.ok:
            print(
                f"\n[DR-6 leader note] produce partially failed at "
                f"loss={loss_pct}% delay={delay_ms}ms (may include re-election): "
                f"acked={len(committed.all_produced)}, "
                f"clients_failed={sum(1 for c in committed.clients if c.exit_code != 0 or c.rc != 0)}"
            )

        # ── Concurrent subscribe during loss ──────────────────────────────────
        # If re-election happened, subscribe against the new leader.
        running = cluster.running_services()
        current_leader_for_sub = leader_svc if leader_svc in running else (
            node_id_to_service(cluster.wait_for_leader(timeout_s=60, exclude=[]))
        )
        cid_sub = _next_cid_base()
        sub_live = _subscribe_all(
            cluster, current_leader_for_sub, cid_sub, keys, window_ms=120000
        )

        # ── Election-stability check ──────────────────────────────────────────
        all_running = cluster.running_services()
        lid_now = _leader_id_from_logs(cluster, all_running)
        if lid_now is not None and lid_now != lid_initial:
            election_occurred = True
            new_leader_svc = node_id_to_service(lid_now)
            if loss_pct <= 5 and delay_ms <= 20:
                # Lowest-intensity case: must be stable (5% + 20ms).
                raise AssertionError(
                    f"SPURIOUS ELECTION at minimal leader loss={loss_pct}% "
                    f"delay={delay_ms}ms: leader changed node {lid_initial} → "
                    f"node {lid_now}. Aeron must tolerate this intensity "
                    f"without triggering a leader timeout."
                )
            # Higher-intensity case: tolerate but document.
            print(
                f"\n[DR-6 leader note] election at loss={loss_pct}% delay={delay_ms}ms: "
                f"node {lid_initial} → node {lid_now}. "
                f"Hard invariant: committed-loss=0 on acked set."
            )
        else:
            new_leader_svc = leader_svc

    finally:
        # Always heal.
        faults.heal_all(cluster)

    # ── Convergence after heal (was a fixed 10 s sleep) ─────────────────────────
    cluster.settle(10.0, running=list(SERVICES))
    assert_single_leader(cluster)

    # ── Delivery invariants — HARD guarantees ─────────────────────────────────
    _assert_delivery_invariants(committed, sub_live)

    # ── Full durability check: pre + mid committed sets survive ───────────────
    all_committed = Results(clients=pre.clients + committed.clients)
    _, current_leader = cluster.identify_leader()
    cid_sub2 = _next_cid_base()
    sub_post = _subscribe_all(cluster, current_leader, cid_sub2, keys, window_ms=120000)
    merged = Results(clients=all_committed.clients + sub_post.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # ── Post-heal usability round-trip ────────────────────────────────────────
    _, current_leader = cluster.identify_leader()
    cid_post = _next_cid_base()
    post = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=current_leader,
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
        "post-heal produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"post-heal produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)

    # Document outcome.
    print(
        f"\n[DR-6 leader] loss={loss_pct}% delay={delay_ms}ms rep={rep}: "
        f"acked={len(committed.all_produced)}/{records}, "
        f"election={'YES — ' + str(lid_initial) + '→' + str(lid_now if election_occurred else lid_initial) + ' (documented)' if election_occurred else 'NO (stable)'}; "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )
