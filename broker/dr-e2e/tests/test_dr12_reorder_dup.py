"""DR-12: Packet reorder + duplication + MTU / large-batch fragmentation.

Verifies that Aeron's reliability layer — combined with the broker's
push-on-append replication and fetch backstop — preserves end-to-end
correctness under wire-level packet reorder and duplication on broker links.

Hard invariants (asserted in every non-skipped case):
  * **Committed-loss = 0**: every record that received an acks=All
    ProduceResponse MUST arrive at a subscriber (no committed loss).
    Milestone C guarantees majority-flush before ack.
  * **Per-key ordering**: records delivered to a subscriber arrive in
    strictly increasing seq order per (cid, key).  Aeron's sequence-number
    layer RESTORES delivery order despite wire reorder — no app-level reorder
    must reach the subscriber.
  * **Exactly-once**: no committed record is delivered more than once to the
    same subscriber.  Aeron + the broker's idempotent message handling absorb
    wire-level duplicates — no app-level dup must reach the subscriber.
  * **Single-leader-at-a-time**: after heal_all + convergence,
    assert_single_leader() is called unconditionally.

Scenarios (parametrized):
  1. Reorder on a follower link (reorder=25/50 %, delay=20 ms).
     Sustained produce acks=All + subscribe; assert all delivery invariants.
     At moderate reorder (≤ 25 %) the leader MUST NOT change (Aeron heartbeat
     must tolerate wire reorder without triggering a spurious election).
  2. Duplication on a single link (dup=5/10 % on follower and/or leader).
     Assert NO app-level duplicate delivery; committed-loss=0; ordering.
  3. Reorder + duplicate combined on a follower link.
     Worst-case wire chaos; assert all delivery invariants hold together.
  4. Large multi-fragment batches (many records/keys producing sizeable
     RecordBatch payloads, exercising Aeron FragmentAssembler end-to-end).
     NOTE: dr_client.cpp does NOT expose a RUNE_RECORD_BYTES value-size knob;
     the payload is "cid=<c>;key=<k>;seq=<s>" (~20-30 bytes).  To exercise
     fragmentation we use a high record count (500 records × 18 keys = 9 KB+
     of payload spread over many Aeron frames).  A dedicated RUNE_RECORD_BYTES
     knob would strengthen this by controlling per-record size directly; the
     controller can add it.  This scenario asserts that large-volume produce +
     subscribe with reorder+dup active preserves all delivery invariants.
  5. MTU reduction (strict, Linux docker host; the DR-BUG-MTU regression).
     set_mtu() (harness/faults.py, the netem sidecar with NET_ADMIN) lowers eth0
     of a follower that LEADS a partition of `multi` to 1200 bytes (below the
     1472-byte Aeron MTU derived at boot) and reads it back.  500 acks=All records,
     then a live read: every acked partition — the reduced node's included — must
     deliver, and the reduced node must have logged the GSO path-MTU fallback
     (preconditions asserted, never skipped).  Docker Desktop: unreliable.

Parametrized matrix:
  Scenario 1 — follower reorder:  reorder_pct in (25, 50) × rep in (0, 1) = 4 cases
  Scenario 2a — follower dup:     dup_pct in (5, 10) × rep in (0, 1)       = 4 cases
  Scenario 2b — leader dup:       dup_pct in (5, 10) × rep in (0, 1)       = 4 cases
  Scenario 3 — combined:          param in ('light', 'heavy') × rep in (0,1)= 4 cases
  Scenario 4 — large batch:       rep in (0, 1)                             = 2 cases
  Scenario 5 — MTU reduction:     strict (Linux docker host)                = 1 case
  ─────────────────────────────────────────────────────────────────────────────
  Total: 19 parametrized cases (4+4+4+4+2+1).

NOTE on record counts: netem reorder/duplicate slows Aeron's retransmission
windows, so per-case record counts are kept modest (100-200 per case) to bound
wall-clock runtime, except for the large-batch scenario which uses 500 to
exercise fragmentation without hitting the u16 batch cap (~64 KB).

CID window: 1_000_000+ (disjoint from DR-1..11 which occupy < 1_000_000).
  DR-1: n*1000, DR-2: 100_000+, DR-3: 300_000+, DR-4: 400_000+,
  DR-5: 500_000+, DR-6: 600_000+, DR-7: 700_000+, DR-10: 800_000+,
  DR-11: 900_000+.  DR-12: 1_000_000+ — fully disjoint.
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
# DR-12: 1_000_000+ — fully disjoint from all prior suites.
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 1_000_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + client-start stagger (Docker Desktop tail; see
# DR-BUG-2).  Under netem reorder/dup these budgets must remain generous because
# Aeron retransmit windows + delayed channels inflate round-trip times.
_CONNECT_MS = 40000
_STAGGER_MS = 400


# ── Shared helpers (mirror DR-6 conventions) ──────────────────────────────────

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
        acks="all",                # majority-durable ack before counting committed
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 120000, drain_quiet_ms: int = DRAIN_QUIET_MS) -> Results:
    """Subscribe-from-earliest and drain the whole partition set.

    window_ms is intentionally generous (default 120s) because netem reorder +
    duplicate slows Aeron's retransmission and inflates delivery times.
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

    These three are the HARD invariants for every DR-12 case, regardless of
    fault type (reorder/dup/combined/large-batch) or whether a re-election
    occurred.

    Aeron's sequence-number reliability layer is the mechanism that must absorb
    wire-level reorder and duplicate before delivering to the app:
      * Wire reorder → Aeron reassembles in order → no app-level reorder.
      * Wire duplicate → Aeron deduplicates by NAK/seq → no app-level dup.
    The broker's own dedup/idempotent handling provides a second guard.
    """
    merged = Results(clients=committed.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)


# ── Scenario 1: Reorder on a follower link ────────────────────────────────────
#
# Apply netem(follower, reorder=<r>%, delay=20ms) while sustaining
# produce/subscribe to the leader.  reorder in tc-netem requires an explicit
# delay (the helper's _netem_spec already inserts a default 10ms delay if none
# is provided, but we pass 20ms explicitly for clarity).
#
# The cluster retains 2/3 quorum (leader + one other healthy follower), so
# acks=All must continue to succeed.
#
# Election-stability contract:
#   reorder=25%: assert leader id is UNCHANGED (moderate reorder must not
#                trigger a spurious election — Aeron heartbeat tolerates it).
#   reorder=50%: a re-election is tolerated and documented; committed-loss MUST
#                remain 0 and the single-leader invariant must hold after heal.
#
# Matrix: reorder_pct in (25, 50) × rep in (0, 1) = 4 cases.

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
@pytest.mark.parametrize("rep", [0, 1])
@pytest.mark.parametrize("reorder_pct", [25, 50])
def test_dr12_follower_reorder(fresh_cluster, netem_ready, reorder_pct, rep):
    """Follower-link packet reorder (+ 20ms delay) during sustained produce/subscribe.

    Aeron's sequence-number reliability restores delivery order despite wire
    reorder.  tc-netem reorder semantics: reorder_pct% of packets are sent
    immediately (out of order); the remainder are held for the delay (20ms)
    then sent in order.  The combination creates genuine out-of-order delivery
    on the wire that Aeron's re-assembly must correct.

    Hard invariants:
      * committed-loss = 0 (Aeron reliability + fetch/push retransmit absorb reorder)
      * per-key ordering preserved (Aeron restores order — no app-level reorder)
      * exactly-once per subscriber
    Election-stability:
      * reorder=25%: leader MUST NOT change.
      * reorder=50%: re-election tolerated and documented; committed-loss stays 0.
    """
    cluster = fresh_cluster
    keys = 4
    records = 150

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc, "follower_service() returned the leader"

    third_svc = next(s for s in SERVICES if s not in (leader_svc, follower_svc))

    # ── Pre-fault baseline ────────────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, 50, keys)
    assert pre.ok, (
        "pre-fault produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) > 0, "pre-fault produce returned 0 records"

    election_occurred = False
    committed: Results = Results()
    sub_live: Results = Results()
    try:
        # ── Inject reorder + delay on the follower link ───────────────────────
        # reorder requires a base delay in tc-netem; pass 20ms explicitly.
        # _netem_spec order: delay first, then reorder (tc-netem requirement).
        faults.netem(cluster, follower_svc, reorder=reorder_pct, delay=20)

        time.sleep(2.0)   # KEPT: the produce must start on an already-degraded link (scenario)

        # ── Sustained produce while follower link is reordered ────────────────
        # Leader + healthy third form 2/3 quorum; acks=All must still succeed.
        cid_mid = _next_cid_base()
        committed = _produce(cluster, leader_svc, 1, cid_mid, records, keys)
        assert committed.ok, (
            f"mid-fault produce (reorder={reorder_pct}% delay=20ms on {follower_svc}) "
            "failed — 2/3 quorum must keep serving acks=All while one follower is "
            "reordered:\n"
            + "\n\n".join(
                f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                for c in committed.clients if c.exit_code != 0 or c.rc != 0
            )
        )
        assert len(committed.all_produced) == records, (
            f"produced {len(committed.all_produced)} != {records} "
            f"(reorder={reorder_pct}% delay=20ms on {follower_svc})"
        )

        # ── Concurrent subscribe while reorder is active ──────────────────────
        cid_sub = _next_cid_base()
        sub_live = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=120000)

        # ── Election-stability check ──────────────────────────────────────────
        lid_now = _leader_id_from_logs(cluster, [leader_svc, third_svc])
        if lid_now is not None and lid_now != lid_initial:
            election_occurred = True
            if reorder_pct <= 25:
                raise AssertionError(
                    f"SPURIOUS ELECTION at moderate follower reorder={reorder_pct}%: "
                    f"leader changed from node {lid_initial} → node {lid_now}. "
                    f"Aeron heartbeating must tolerate {reorder_pct}% wire reorder "
                    f"without triggering a leader timeout."
                )
            print(
                f"\n[DR-12 note] spurious election at follower reorder={reorder_pct}% "
                f"delay=20ms (node {lid_initial} → node {lid_now}); "
                f"tolerated at high reorder — hard invariant is committed-loss=0."
            )

    finally:
        faults.heal_all(cluster)

    # ── Convergence: give the follower time to catch up ───────────────────────
    cluster.settle(10.0, running=list(SERVICES))   # was a fixed 10 s sleep before the positive checks below
    assert_single_leader(cluster)

    # ── Delivery invariants — HARD guarantees ─────────────────────────────────
    _assert_delivery_invariants(committed, sub_live)

    # ── Post-heal full-durability subscribe from earliest ────────────────────
    all_committed = Results(clients=pre.clients + committed.clients)
    _, current_leader = cluster.identify_leader()
    cid_sub2 = _next_cid_base()
    sub_post = _subscribe_all(cluster, current_leader, cid_sub2, keys, window_ms=120000)
    merged = Results(clients=all_committed.clients + sub_post.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # ── Post-heal usability: fresh produce+subscribe round-trip ──────────────
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

    print(
        f"\n[DR-12 follower-reorder] reorder={reorder_pct}% delay=20ms rep={rep}: "
        f"committed={len(committed.all_produced)} records, "
        f"election={'YES (documented)' if election_occurred else 'NO (stable)'}; "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )


# ── Scenario 2a: Duplication on a follower link ───────────────────────────────
#
# Apply netem(follower, duplicate=<d>%) while sustaining produce/subscribe to
# the leader.  Wire duplicates must NOT surface as app-level duplicate delivery:
# Aeron's sequence-number dedup absorbs them before the broker sees them.
#
# At dup=5%/10% on a follower link: the leader + healthy third form 2/3 quorum;
# acks=All must still succeed, and leader SHOULD remain stable (dup alone should
# not trigger a spurious election).
#
# Matrix: dup_pct in (5, 10) × rep in (0, 1) = 4 cases.

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
@pytest.mark.parametrize("rep", [0, 1])
@pytest.mark.parametrize("dup_pct", [5, 10])
def test_dr12_follower_dup(fresh_cluster, netem_ready, dup_pct, rep):
    """Follower-link packet duplication during sustained produce/subscribe.

    Wire duplicates are absorbed by Aeron's sequence-number dedup layer.
    The broker's idempotent message handling provides a secondary guard.
    No app-level duplicate delivery must reach the subscriber.

    Hard invariants:
      * committed-loss = 0
      * per-key ordering preserved
      * exactly-once per subscriber (NO app-level dup delivery)
    Election-stability: leader MUST NOT change (dup alone must not trigger
    a spurious election at 5-10% duplicate rate).
    """
    cluster = fresh_cluster
    keys = 4
    records = 150

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc, "follower_service() returned the leader"

    third_svc = next(s for s in SERVICES if s not in (leader_svc, follower_svc))

    # ── Pre-fault baseline ────────────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, 50, keys)
    assert pre.ok, (
        "pre-fault produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) > 0, "pre-fault produce returned 0 records"

    committed: Results = Results()
    sub_live: Results = Results()
    try:
        # ── Inject packet duplication on the follower link ────────────────────
        faults.netem(cluster, follower_svc, duplicate=dup_pct)

        time.sleep(2.0)   # KEPT: the produce must start on an already-degraded link (scenario)

        # ── Sustained produce while duplication is active ─────────────────────
        cid_mid = _next_cid_base()
        committed = _produce(cluster, leader_svc, 1, cid_mid, records, keys)
        assert committed.ok, (
            f"mid-fault produce (duplicate={dup_pct}% on {follower_svc}) failed — "
            "2/3 quorum must keep serving acks=All with follower-link duplication:\n"
            + "\n\n".join(
                f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                for c in committed.clients if c.exit_code != 0 or c.rc != 0
            )
        )
        assert len(committed.all_produced) == records, (
            f"produced {len(committed.all_produced)} != {records} "
            f"(duplicate={dup_pct}% on {follower_svc})"
        )

        # ── Concurrent subscribe while duplication is active ──────────────────
        cid_sub = _next_cid_base()
        sub_live = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=120000)

        # ── Election-stability check ──────────────────────────────────────────
        lid_now = _leader_id_from_logs(cluster, [leader_svc, third_svc])
        if lid_now is not None and lid_now != lid_initial:
            raise AssertionError(
                f"SPURIOUS ELECTION under follower duplicate={dup_pct}%: "
                f"leader changed from node {lid_initial} → node {lid_now}. "
                f"Wire duplication on a follower link must not trigger a spurious "
                f"election — Aeron heartbeat tolerates duplicate packets."
            )

    finally:
        faults.heal_all(cluster)

    # ── Convergence ───────────────────────────────────────────────────────────
    cluster.settle(10.0, running=list(SERVICES))   # was a fixed 10 s sleep before the positive checks below
    assert_single_leader(cluster)

    # ── Delivery invariants — HARD guarantees ─────────────────────────────────
    # exactly-once is the key assertion here: wire dups must NOT become app dups.
    _assert_delivery_invariants(committed, sub_live)

    # ── Post-heal full-durability check ──────────────────────────────────────
    all_committed = Results(clients=pre.clients + committed.clients)
    _, current_leader = cluster.identify_leader()
    cid_sub2 = _next_cid_base()
    sub_post = _subscribe_all(cluster, current_leader, cid_sub2, keys, window_ms=120000)
    merged = Results(clients=all_committed.clients + sub_post.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # ── Post-heal usability ───────────────────────────────────────────────────
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
    assert len(post.all_produced) == 30
    assert_single_leader(cluster)

    print(
        f"\n[DR-12 follower-dup] duplicate={dup_pct}% rep={rep}: "
        f"committed={len(committed.all_produced)} records; "
        f"committed-loss=0, ordering=OK, exactly-once=OK (no app-level dup)."
    )


# ── Scenario 2b: Duplication on the leader link ───────────────────────────────
#
# Apply netem(leader, duplicate=<d>%) — degrading both the client→leader RPC
# channel and the leader→follower replication channel simultaneously.
# At dup=5%/10%: assert no app-level duplicate delivery, committed-loss=0,
# ordering, and single-leader.  A spurious election at 10% dup on the leader
# link is tolerated and documented (the hard invariant is committed-loss=0).
#
# Matrix: dup_pct in (5, 10) × rep in (0, 1) = 4 cases.

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
@pytest.mark.parametrize("rep", [0, 1])
@pytest.mark.parametrize("dup_pct", [5, 10])
def test_dr12_leader_dup(fresh_cluster, netem_ready, dup_pct, rep):
    """Leader-link packet duplication during sustained produce/subscribe.

    Degrading the leader's own link simultaneously affects:
      * the client→leader RPC channel (produce/subscribe retransmits),
      * the leader→follower push-on-append replication channel.
    Aeron's reliability guarantees dedup for both.

    Hard invariants:
      * committed-loss = 0
      * per-key ordering preserved
      * exactly-once per subscriber (NO app-level dup from wire dups)
    Election-stability:
      * dup=5%: leader MUST NOT change (low-intensity stability).
      * dup=10%: a re-election is tolerated and documented.
    """
    cluster = fresh_cluster
    keys = 4
    records = 150

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Pre-fault baseline ────────────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, 30, keys)
    assert pre.ok, (
        "pre-fault produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) > 0, "pre-fault produce returned 0 records"

    election_occurred = False
    new_leader_svc = leader_svc
    committed: Results = Results()
    sub_live: Results = Results()
    try:
        # ── Inject packet duplication on the LEADER's link ────────────────────
        faults.netem(cluster, leader_svc, duplicate=dup_pct)

        time.sleep(2.0)   # KEPT: the produce must start on an already-degraded link (scenario)

        # ── Sustained produce while leader link is duplicating ────────────────
        cid_mid = _next_cid_base()
        committed = _produce(cluster, leader_svc, 1, cid_mid, records, keys)
        # NOTE: if a re-election occurs mid-produce the client may bail — only
        # ACKED records are in the committed set.  We do NOT fail on partial
        # produce here; we assert committed-loss=0 on whatever was acked.
        if not committed.ok:
            print(
                f"\n[DR-12 leader-dup note] produce partially failed at "
                f"duplicate={dup_pct}% (may include re-election): "
                f"acked={len(committed.all_produced)}, "
                f"clients_failed={sum(1 for c in committed.clients if c.exit_code != 0 or c.rc != 0)}"
            )

        # ── Concurrent subscribe during duplication ───────────────────────────
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
            if dup_pct <= 5:
                raise AssertionError(
                    f"SPURIOUS ELECTION at minimal leader duplicate={dup_pct}%: "
                    f"leader changed from node {lid_initial} → node {lid_now}. "
                    f"Aeron must tolerate {dup_pct}% wire duplication on the "
                    f"leader link without triggering a leader timeout."
                )
            print(
                f"\n[DR-12 leader-dup note] election at duplicate={dup_pct}%: "
                f"node {lid_initial} → node {lid_now}. "
                f"Hard invariant: committed-loss=0 on acked set."
            )
        else:
            new_leader_svc = leader_svc

    finally:
        faults.heal_all(cluster)

    # ── Convergence after heal ────────────────────────────────────────────────
    cluster.settle(10.0, running=list(SERVICES))   # was a fixed 10 s sleep before the positive checks below
    assert_single_leader(cluster)

    # ── Delivery invariants — HARD guarantees ─────────────────────────────────
    _assert_delivery_invariants(committed, sub_live)

    # ── Full durability check ─────────────────────────────────────────────────
    all_committed = Results(clients=pre.clients + committed.clients)
    _, current_leader = cluster.identify_leader()
    cid_sub2 = _next_cid_base()
    sub_post = _subscribe_all(cluster, current_leader, cid_sub2, keys, window_ms=120000)
    merged = Results(clients=all_committed.clients + sub_post.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # ── Post-heal usability ───────────────────────────────────────────────────
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
    assert len(post.all_produced) == 30
    assert_single_leader(cluster)

    print(
        f"\n[DR-12 leader-dup] duplicate={dup_pct}% rep={rep}: "
        f"acked={len(committed.all_produced)}/{records}, "
        f"election={'YES — ' + str(lid_initial) + '→' + str(lid_now if election_occurred else lid_initial) + ' (documented)' if election_occurred else 'NO (stable)'}; "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )


# ── Scenario 3: Reorder + duplicate combined on a follower link ───────────────
#
# The worst-case wire chaos: both reorder AND duplicate active simultaneously
# on the same follower link.  Parametrized by intensity:
#   'light': reorder=20%, dup=5%, delay=15ms
#   'heavy': reorder=40%, dup=10%, delay=20ms
#
# At both intensities: all delivery invariants must hold.  The leader SHOULD
# NOT change at 'light' intensity; at 'heavy' a re-election is tolerated.
#
# Matrix: param in ('light', 'heavy') × rep in (0, 1) = 4 cases.

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
@pytest.mark.parametrize("rep", [0, 1])
@pytest.mark.parametrize("param", ["light", "heavy"])
def test_dr12_follower_reorder_and_dup(fresh_cluster, netem_ready, param, rep):
    """Follower-link reorder + duplication combined during sustained produce/subscribe.

    Exercises the combined wire chaos of simultaneous reorder and duplication.
    Aeron's sequence-number layer must absorb both faults before delivery:
      * Wire reorder → reassembled in order → no app-level reorder.
      * Wire dup → deduplicated by sequence → no app-level dup.

    Hard invariants:
      * committed-loss = 0
      * per-key ordering preserved (no app-level reorder)
      * exactly-once per subscriber (no app-level dup)
    Election-stability:
      * 'light' (reorder=20%, dup=5%): leader MUST NOT change.
      * 'heavy' (reorder=40%, dup=10%): re-election tolerated; committed-loss 0.
    """
    # Intensity parameters.
    intensity = {
        "light": {"reorder": 20, "duplicate": 5,  "delay": 15},
        "heavy": {"reorder": 40, "duplicate": 10, "delay": 20},
    }[param]
    reorder_pct  = intensity["reorder"]
    dup_pct      = intensity["duplicate"]
    delay_ms     = intensity["delay"]

    cluster = fresh_cluster
    keys = 4
    records = 120   # slightly lower — combined chaos slows retransmits more

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc, "follower_service() returned the leader"

    third_svc = next(s for s in SERVICES if s not in (leader_svc, follower_svc))

    # ── Pre-fault baseline ────────────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, 40, keys)
    assert pre.ok, (
        "pre-fault produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) > 0, "pre-fault produce returned 0 records"

    election_occurred = False
    committed: Results = Results()
    sub_live: Results = Results()
    try:
        # ── Inject combined reorder + dup on the follower link ────────────────
        faults.netem(
            cluster, follower_svc,
            reorder=reorder_pct,
            duplicate=dup_pct,
            delay=delay_ms,
        )

        time.sleep(2.0)   # KEPT: the produce must start on an already-degraded link (scenario)

        # ── Sustained produce during combined chaos ───────────────────────────
        cid_mid = _next_cid_base()
        committed = _produce(cluster, leader_svc, 1, cid_mid, records, keys)
        assert committed.ok, (
            f"mid-fault produce (reorder={reorder_pct}% dup={dup_pct}% "
            f"delay={delay_ms}ms on {follower_svc}) failed — "
            "2/3 quorum must keep serving acks=All under combined wire chaos:\n"
            + "\n\n".join(
                f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                for c in committed.clients if c.exit_code != 0 or c.rc != 0
            )
        )
        assert len(committed.all_produced) == records, (
            f"produced {len(committed.all_produced)} != {records} "
            f"(reorder={reorder_pct}% dup={dup_pct}% delay={delay_ms}ms on {follower_svc})"
        )

        # ── Concurrent subscribe during combined chaos ────────────────────────
        cid_sub = _next_cid_base()
        sub_live = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=150000)

        # ── Election-stability check ──────────────────────────────────────────
        lid_now = _leader_id_from_logs(cluster, [leader_svc, third_svc])
        if lid_now is not None and lid_now != lid_initial:
            election_occurred = True
            if param == "light":
                raise AssertionError(
                    f"SPURIOUS ELECTION at 'light' combined chaos "
                    f"(reorder={reorder_pct}% dup={dup_pct}% delay={delay_ms}ms): "
                    f"leader changed from node {lid_initial} → node {lid_now}. "
                    f"Aeron must tolerate this intensity without triggering a "
                    f"leader timeout."
                )
            print(
                f"\n[DR-12 combined note] election at 'heavy' chaos "
                f"(reorder={reorder_pct}% dup={dup_pct}% delay={delay_ms}ms): "
                f"node {lid_initial} → node {lid_now}; tolerated."
            )

    finally:
        faults.heal_all(cluster)

    # ── Convergence ───────────────────────────────────────────────────────────
    cluster.settle(10.0, running=list(SERVICES))   # was a fixed 10 s sleep before the positive checks below
    assert_single_leader(cluster)

    # ── Delivery invariants — HARD guarantees ─────────────────────────────────
    _assert_delivery_invariants(committed, sub_live)

    # ── Post-heal full-durability check ──────────────────────────────────────
    all_committed = Results(clients=pre.clients + committed.clients)
    _, current_leader = cluster.identify_leader()
    cid_sub2 = _next_cid_base()
    sub_post = _subscribe_all(cluster, current_leader, cid_sub2, keys, window_ms=120000)
    merged = Results(clients=all_committed.clients + sub_post.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # ── Post-heal usability ───────────────────────────────────────────────────
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
    assert len(post.all_produced) == 30
    assert_single_leader(cluster)

    print(
        f"\n[DR-12 combined] param={param} reorder={reorder_pct}% "
        f"dup={dup_pct}% delay={delay_ms}ms rep={rep}: "
        f"committed={len(committed.all_produced)} records, "
        f"election={'YES (documented)' if election_occurred else 'NO (stable)'}; "
        f"committed-loss=0, ordering=OK, exactly-once=OK."
    )


# ── Scenario 4: Large multi-fragment batches ──────────────────────────────────
#
# Exercise Aeron FragmentAssembler end-to-end: produce a large number of
# records across many keys so the client→gateway and gateway→follower
# replication paths carry sizeable RecordBatch payloads that may span multiple
# Aeron fragments.  Apply combined reorder+dup on the follower link during
# production to maximise reassembly + dedup pressure.
#
# LIMITATION: dr_client.cpp does NOT expose a RUNE_RECORD_BYTES value-size
# knob; the value payload is fixed as "cid=<c>;key=<k>;seq=<s>" (~20-30 bytes).
# To generate large batches we use 500 records × 18 keys per run.  A dedicated
# RUNE_RECORD_BYTES environment variable would let us set e.g. 1024-byte
# payloads per record, bringing a 500-record batch to ~512 KB and forcing
# multiple Aeron fragment reassemblies per batch.  The controller can add this
# knob to dr_client.cpp; this scenario documents the gap.
#
# Even without large payloads, a 500-record × 3-partition batch exercises the
# batch serialisation/deserialisation path and is useful as a volume regression.
#
# Matrix: rep in (0, 1) = 2 cases.

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(1200)
@pytest.mark.parametrize("rep", [0, 1])
def test_dr12_large_batch_fragmentation(fresh_cluster, netem_ready, rep):
    """Large multi-record batches + follower reorder+dup: fragmentation integrity.

    Produces a high volume of records (500 × 18 keys across 3 partitions) while
    a follower link has combined reorder (30%) + duplicate (8%) + delay (20ms)
    active.  This exercises the client→gateway and gateway→follower replication
    fragmentation paths under Aeron FragmentAssembler.

    LIMITATION: dr_client.cpp payload is fixed as "cid=<c>;key=<k>;seq=<s>"
    (~20-30 bytes per record).  True multi-fragment reassembly requires larger
    per-record values; a RUNE_RECORD_BYTES knob (controller TODO) would
    strengthen this scenario by controlling per-record payload size directly.
    At 1024 bytes per record a 500-record batch would be ~512 KB, forcing
    multiple Aeron fragment reassemblies; at the current ~25 bytes/record the
    batch is ~12 KB — still a useful volume regression but less aggressive on
    the fragmentation path.

    Hard invariants:
      * committed-loss = 0 (all 500 acked records delivered)
      * per-key ordering preserved (fragmentation + reorder both handled)
      * exactly-once per subscriber (dup + reassembly both handled)
    """
    cluster = fresh_cluster
    keys = 18       # spread across all 3 partitions
    records = 500   # sizeable batch; keep within u16 RecordBatch cap (~64 KB at 25 bytes/record)

    lid_initial, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc, "follower_service() returned the leader"

    # ── Pre-fault baseline ────────────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, 50, keys)
    assert pre.ok, (
        "pre-fault produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) > 0, "pre-fault produce returned 0 records"

    committed: Results = Results()
    sub_live: Results = Results()
    try:
        # ── Inject combined reorder + dup + delay on the follower link ────────
        # Moderate intensity: exercises fragmentation path without excessive
        # retransmit churn that would make 500 records intractable under netem.
        faults.netem(cluster, follower_svc, reorder=30, duplicate=8, delay=20)

        time.sleep(2.0)   # KEPT: the produce must start on an already-degraded link (scenario)

        # ── Large-volume produce ──────────────────────────────────────────────
        # 500 records × 18 keys → sizeable RecordBatch payloads that exercise
        # the batch encoding/decoding path and may fragment across Aeron frames
        # at the current ~25-byte per-record payload.  With RUNE_RECORD_BYTES=1024
        # this would be ~512 KB per batch, forcing multiple fragment reassemblies.
        cid_mid = _next_cid_base()
        committed = _produce(cluster, leader_svc, 1, cid_mid, records, keys)
        assert committed.ok, (
            f"large-batch produce ({records} records × {keys} keys with "
            "reorder=30% dup=8% delay=20ms on follower) failed:\n"
            + "\n\n".join(
                f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                for c in committed.clients if c.exit_code != 0 or c.rc != 0
            )
        )
        assert len(committed.all_produced) == records, (
            f"produced {len(committed.all_produced)} != {records} (large-batch scenario)"
        )

        # ── Subscribe with a generous window (large volume + netem = slow) ─────
        cid_sub = _next_cid_base()
        sub_live = _subscribe_all(
            cluster, leader_svc, cid_sub, keys, window_ms=180000  # 3 min: 500 records + netem
        )

    finally:
        faults.heal_all(cluster)

    # ── Convergence ───────────────────────────────────────────────────────────
    cluster.settle(10.0, running=list(SERVICES))   # was a fixed 10 s sleep before the positive checks below
    assert_single_leader(cluster)

    # ── Delivery invariants — HARD guarantees ─────────────────────────────────
    _assert_delivery_invariants(committed, sub_live)

    # ── Post-heal full-durability check ──────────────────────────────────────
    all_committed = Results(clients=pre.clients + committed.clients)
    _, current_leader = cluster.identify_leader()
    cid_sub2 = _next_cid_base()
    sub_post = _subscribe_all(
        cluster, current_leader, cid_sub2, keys, window_ms=180000
    )
    merged = Results(clients=all_committed.clients + sub_post.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # ── Post-heal usability ───────────────────────────────────────────────────
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
    assert len(post.all_produced) == 30
    assert_single_leader(cluster)

    print(
        f"\n[DR-12 large-batch] records={records} keys={keys} rep={rep}: "
        f"committed={len(committed.all_produced)} records; "
        f"committed-loss=0, ordering=OK, exactly-once=OK. "
        f"NOTE: RUNE_RECORD_BYTES knob needed for true multi-fragment stress."
    )


# ── Scenario 5: MTU reduction + large batches ─────────────────────────────────
#
# faults.set_mtu() lowers the follower's eth0 MTU to 1200 bytes AFTER the broker derived its Aeron MTU from the
# 1500-byte interface (1472): every full-size replication datagram to / from that follower now exceeds the link
# MTU, combined with reorder + duplication on the same link.  The broker must either get the data through (IP
# fragmentation / Aeron retransmit) or keep the follower out of the ISR — never lose or duplicate a committed
# record.  Runs on the Linux docker host (veth MTU changes are honoured there).

@pytest.mark.fault
@pytest.mark.netem
@pytest.mark.timeout(900)
def test_dr12_mtu_reduction_large_batch(fresh_cluster, netem_ready):
    """Follower MTU 1200 (< the 1472-byte Aeron MTU the broker derived) + reorder 30 % + dup 8 % + 20 ms delay,
    while 500 acks=All records are produced.  Hard invariants: committed-loss = 0, per-key ordering,
    exactly-once, single leader; after restoring MTU 1500 and healing, the follower converges and the
    cluster serves a fresh round trip."""
    cluster = fresh_cluster
    keys, records = 18, 500
    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)
    # Review B-I4 precondition 1: the MTU-reduced node must LEAD a partition of `multi` (topic 1), else the
    # live-read check is vacuous.  Never a skip.
    leaders = cluster.partition_leaders(1)
    follower_svc = next((s for s in SERVICES if s != leader_svc and s in leaders.values()), None)
    if follower_svc is None:
        pytest.fail(f"no follower of the consensus leader {leader_svc} leads a partition of multi: {leaders}")
    reduced_parts = sorted(p for p, s in leaders.items() if s == follower_svc)

    pre = _produce(cluster, leader_svc, 1, _next_cid_base(), 50, keys)
    assert pre.ok and len(pre.all_produced) == 50, "pre-fault produce failed:\n" + pre.clients[0].raw[-1200:]

    committed: Results = Results()
    sub_live: Results = Results()
    try:
        faults.set_mtu(cluster, follower_svc, 1200)
        faults.netem(cluster, follower_svc, reorder=30, duplicate=8, delay=20)
        time.sleep(2.0)   # KEPT: the produce must start on an already-degraded link (scenario)
        committed = _produce(cluster, leader_svc, 1, _next_cid_base(), records, keys)
        assert committed.ok and len(committed.all_produced) == records, (
            "large-batch produce with follower MTU 1200 + reorder/dup failed (leader + healthy third node are a "
            "majority):\n" + "\n\n".join(c.raw[-1200:] for c in committed.clients))
        # 60 s: 500 records stream in seconds on a healthy path (the full 180 s window, measured, adds nothing).
        sub_live = _subscribe_all(cluster, leader_svc, _next_cid_base(), keys, window_ms=60000)
    finally:
        try:
            faults.set_mtu(cluster, follower_svc, 1500)
        finally:
            faults.heal_all(cluster)

    cluster.settle(10.0, running=list(SERVICES))
    assert_single_leader(cluster)

    # Durability first (after the MTU is restored), then availability during the fault: a failure names which
    # one broke - committed data LOST vs committed data not DELIVERED while the MTU was reduced.
    _, current_leader = cluster.identify_leader()
    sub_post = _subscribe_all(cluster, current_leader, _next_cid_base(), keys, window_ms=180000)
    assert_no_committed_loss(Results(clients=pre.clients + committed.clients + sub_post.clients))
    assert_per_key_ordering(sub_post)
    assert_exactly_once(sub_post)

    # DR-BUG-MTU regression (fixed: rune_gso_bindings falls back to IP fragmentation on a GSO EMSGSIZE): the live
    # read must deliver every acked partition, the one the MTU-reduced node leads included.
    live_parts = {m.partition for m in sub_live.all_delivered}
    acked_parts = {m.partition for m in committed.all_produced}
    # Review B-I4 precondition 2: records were acked on the reduced node's partition(s).
    assert set(reduced_parts) <= acked_parts, (
        f"no record acked on {follower_svc}'s partitions {reduced_parts} (acked on {sorted(acked_parts)}): the "
        f"live-read check would be vacuous")
    assert acked_parts <= live_parts, (
        f"DR-BUG-MTU: with {follower_svc} at MTU 1200 (Aeron MTU 1472) the live read delivered partitions "
        f"{sorted(live_parts)} but records were acked on {sorted(acked_parts)}: acked records of partitions "
        f"{sorted(acked_parts - live_parts)} were not delivered for the whole read (they ARE durable: the "
        f"post-restore read above delivered every one)")
    # Review B-I4 precondition 3 (checked AFTER the live read: a missing fallback must not mask the DR-BUG-MTU failure itself): the fallback path ran on the reduced node (product self-evidence).
    reduced_log = cluster.compose.logs(follower_svc, timeout=30)
    assert "exceeds the path MTU" in reduced_log, (
        f"{follower_svc} never logged the GSO path-MTU fallback: the scenario did not exercise the fix "
        f"(GSO off on this host / image?)")
    _assert_delivery_invariants(committed, sub_live)

    post = run_clients(
        1, network=cluster.compose.network_name(), target_service=current_leader, cid_base=_next_cid_base(),
        timeout_s=180, mode="produce_subscribe", topics="multi:1:3", sub_partitions=3, records=30, keys=18,
        expect=30, acks="all", expect_own=True, timeout_ms=_CONNECT_MS, connect_retries=0, subscribe_ms=60000,
        start_stagger_ms=_STAGGER_MS, **{"from": "earliest"})
    assert post.ok and len(post.all_produced) == 30, "post-heal round trip failed:\n" + "\n".join(
        c.raw[-1200:] for c in post.clients)
    assert_single_leader(cluster)
