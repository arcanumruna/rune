"""DR-15: Failover data-integrity deep-dive.

This suite is the authoritative acks=Leader-caveat quantification and the
no-phantom-read / no-divergence verification for the Rune broker.  It
exercises five tightly-scoped scenarios:

1. **acks=Leader tail-loss vs acks=All (headline deliverable)**
   Produce a batch at a steady rate with acks="leader" to the leader, then
   SIGKILL the leader IMMEDIATELY so that a small un-replicated tail is in
   flight.  After re-election, subscribe-from-earliest on the new leader and
   COUNT how many of the old leader's acked records are missing — that is the
   acks=Leader tail-loss window.  Assert it is BOUNDED (≤ a generous ceiling
   that reflects the ~10ms group-commit + replication lag; not zero by design).
   Repeat IDENTICALLY with acks="all" and assert ZERO committed loss.  The
   contrast is the deliverable: acks=Leader may lose a small tail; acks=All
   loses nothing.

2. **Subscriber never reads a truncated record (no phantom read)**
   Produce acks=Leader records right before killing the leader.  Some will be
   un-replicated / uncommitted and therefore truncated by the new leader on
   failover.  Run a subscriber (HW-bounded, read_committed) throughout / after
   the failover.  Assert that the subscriber NEVER delivered a record that is
   absent from the new leader's final log — i.e. no phantom-committed record
   was visible to any subscriber (GR-6 HW-bound guarantees this; this test
   confirms it empirically).  Concretely: delivered ⊆ post-failover-leader set.

3. **Log divergence guard**
   After leader crash + re-election + old-leader restart-as-follower, assert
   the old leader's log CONVERGES to the new leader's (same committed records
   reachable via subscribe, no phantom-committed record, no divergence).
   Reuse assert_no_committed_loss over a subscribe from the restarted old
   leader.

4. **Graceful (SIGTERM) vs ungraceful (SIGKILL) acks=All parity**
   One case each — both must lose ZERO acks=All committed records.  Lightweight
   complement to DR-7, scoped to the integrity focus of this suite.

5. **Simultaneous all-3-node crash → cold restart → consistent recovery (acks=All)**
   SIGKILL all 3, restart all 3, wait_for_leader, assert the acks=All
   committed set survives + is consistent (no divergence) + the cluster serves
   again.  Lightweight complement to DR-7's cold-restart scenario.

Hard invariants across ALL scenarios:
  * acks=All committed-loss = 0 (every single failover).
  * acks=Leader loss is BOUNDED and QUANTIFIED (not zero; that is the spec).
  * NO phantom read: subscriber-delivered ⊆ post-failover-leader log.
  * NO divergence: converged logs byte-identical on (cid, key, seq).

HARNESS UNCERTAINTIES (flagged for controller):
  * Scenario 2 concurrent subscriber: the subscriber is launched in a daemon
    thread while the kill races ahead.  If the harness's `run_clients` blocks
    until the container exits, the subscriber may finish BEFORE the kill races
    in.  The test tolerates this by also running a post-failover subscriber and
    unioning both delivery sets for the phantom check.
  * acks=Leader tail-loss bound: `_LEADER_TAIL_LOSS_CEILING` is set
    conservatively at 50 records (generous upper bound for a 10ms group-commit
    window at the test's ~40 Hz produce rate = ~0.4 records/ms × 10ms ≈ 4
    records; 50 is a 12× safety margin).  If the broker's group-commit window
    or replication lag changes, this ceiling may need adjustment.
  * Old-leader restart in scenario 3: `faults.restart` calls
    `cluster.compose.restart` which issues `docker compose restart`.  For
    SIGKILL'd containers (in 'exited' state), `cluster.compose.start` is the
    correct call.  The test uses `cluster.compose.start` directly following the
    pattern established in DR-7 scenario 4.
"""

from __future__ import annotations

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

# ── Per-run disjoint cid windows ─────────────────────────────────────────────
# DR-1: n*1_000, DR-2: 100_000+, DR-3: 300_000+, DR-4: 400_000+,
# DR-5: 500_000+, DR-6: 600_000+, DR-7: 700_000+, DR-8: 1_000_000+,
# DR-10: 800_000+, DR-11: 900_000+, DR-12: 1_000_000+, DR-13: 1_200_000+,
# DR-14: 1_300_000+
# DR-15: 1_400_000+ — fully disjoint from all prior suites.
_CID_STRIDE = 3000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 1_400_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget + client-start stagger — mirrors DR-3/DR-7 tuning.
_CONNECT_MS = 40000
_STAGGER_MS = 400

# Conservative upper bound on acks=Leader tail-loss records.
# At rate_hz=40 and a 10ms group-commit window ≈ 0.4 records/ms × 10ms ≈ 4
# records lost at most under ideal conditions; 50 gives a 12× safety margin
# for slower CI environments, replication lag, and OS scheduling jitter.
_LEADER_TAIL_LOSS_CEILING = 50


# ── Shared helpers ────────────────────────────────────────────────────────────

def _produce(cluster, target_svc: str, n: int, cid_base: int, records: int,
             keys: int, rate_hz: int = 0, acks: str = "all") -> Results:
    """Run `n` producers against target_svc.

    `acks` controls durability:
      "all"    — majority-durable; every PRODUCED line committed to ISR majority.
      "leader" — leader-page-cache-durable; a group-commit tail may be
                 un-replicated at the moment of a hard leader crash.
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
        keys=max(keys, 18),       # >= partitions so records span all 3 partitions
        rate_hz=rate_hz,
        acks=acks,
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 60000) -> Results:
    """Subscribe-from-earliest on `target_svc` and drain the whole partition set.

    Used to verify the committed set survived failover or to check divergence:
    a single subscriber reads every record on the (multi-partition) log from
    the beginning.
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
        expect=0,                 # drain window; don't require a fixed count
        from_="earliest",
        subscribe_ms=window_ms,
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


# ── Scenario 1: acks=Leader tail-loss vs acks=All (headline deliverable) ─────

@pytest.mark.fault
@pytest.mark.timeout(900)
@pytest.mark.parametrize("acks,expect_zero_loss", [
    ("leader", False),
    ("all",    True),
])
def test_dr15_tail_loss_quantification(fresh_cluster, acks, expect_zero_loss):
    """Quantify acks=Leader tail-loss vs acks=All across a leader SIGKILL.

    The scenario is identical for both acks modes so the contrast is clean:
      1. Produce at a steady rate (rate_hz=40) to the leader with the given
         acks mode.
      2. SIGKILL the leader IMMEDIATELY (minimal sleep) while production is
         running so a group-commit tail is in flight.
      3. Wait for re-election.
      4. Subscribe-from-earliest on the new leader; count how many of the
         old leader's acked records are absent.

    acks=Leader:
      Some acked records may be un-replicated at the moment of the crash →
      truncated by the new leader on failover.  The loss is BOUNDED (≤
      `_LEADER_TAIL_LOSS_CEILING`) and QUANTIFIED in the assertion message.
      We explicitly do NOT assert zero loss — that is the acks=Leader caveat
      and the reason acks=All exists.

    acks=All:
      Every acked record was majority-flushed before the ProduceResponse was
      sent (GR-9 / Milestone C).  ZERO committed loss must result.
    """
    cluster = fresh_cluster
    keys = 4
    records = 300       # enough records to span several group-commit windows
    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    holder: dict[str, Results] = {}

    def _run_produce():
        holder["r"] = _produce(
            cluster, leader_svc, 1, _next_cid_base(), records, keys,
            rate_hz=40, acks=acks,
        )

    t = threading.Thread(target=_run_produce, daemon=True)
    t.start()

    # Minimal delay: let the producer connect + send at least a few records,
    # then kill immediately to maximise the chance of an un-replicated tail.
    # 1.5 s at 40 Hz ≈ 60 records in flight before the crash.
    time.sleep(1.5)
    faults.kill_node(cluster, leader_svc, signal.SIGKILL)

    t.join(timeout=300)
    produced_result = holder.get("r", Results())

    # Re-election: wait for a DIFFERENT node to lead.
    new_lid = cluster.wait_for_new_leader(leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != leader_svc, (
        f"[acks={acks}] leader did not change after SIGKILL"
    )

    n_produced = len(produced_result.all_produced)
    print(f"\n[DR-15 acks={acks}] Producer acked {n_produced} records before/around crash")

    # Subscribe-from-earliest on the new leader to discover the surviving set.
    sub = _subscribe_all(cluster, new_leader_svc, _next_cid_base(), keys,
                         window_ms=90000)

    produced_ids = {(m.cid, m.key, m.seq) for m in produced_result.all_produced}
    delivered_ids = {(m.cid, m.key, m.seq) for m in sub.all_delivered}

    # Records the producer acked that are ABSENT from the new leader's log.
    lost = produced_ids - delivered_ids
    n_lost = len(lost)

    print(f"[DR-15 acks={acks}] Survived on new leader: {len(delivered_ids & produced_ids)} "
          f"/ {n_produced} acked.  Lost (truncated tail): {n_lost} records.")

    if expect_zero_loss:
        # acks=All: GR-9 durability contract — ZERO committed loss.
        assert n_lost == 0, (
            f"[acks=all] committed-loss violation: {n_lost} acked records absent "
            f"from new leader {new_leader_svc} after SIGKILL of {leader_svc}. "
            f"Missing (cid,key,seq): {sorted(lost)[:20]}"
        )
        print(f"[DR-15 acks=all] PASS: zero committed loss (as required by GR-9).")
    else:
        # acks=Leader: loss is permitted but MUST be bounded.
        assert n_lost <= _LEADER_TAIL_LOSS_CEILING, (
            f"[acks=leader] tail-loss EXCEEDED ceiling: {n_lost} records lost "
            f"(ceiling={_LEADER_TAIL_LOSS_CEILING}); this suggests a replication "
            f"lag or group-commit window far larger than expected (~10ms). "
            f"Missing (cid,key,seq): {sorted(lost)[:20]}"
        )
        # Also assert ordering + exactly-once for whatever DID survive.
        survived = Results(clients=produced_result.clients + sub.clients)
        # Ordering and exactly-once check for subscriber's own received stream.
        assert_per_key_ordering(sub)
        assert_exactly_once(sub)
        print(f"[DR-15 acks=leader] PASS: tail-loss={n_lost} records "
              f"(bounded ≤ {_LEADER_TAIL_LOSS_CEILING}; ordering+EOS preserved "
              f"for surviving {len(delivered_ids & produced_ids)} records).")
        _ = survived  # merged result used above for context


# ── Scenario 2: Subscriber never sees a truncated record (no phantom read) ───

@pytest.mark.fault
@pytest.mark.timeout(900)
def test_dr15_no_phantom_read(fresh_cluster):
    """GR-6 HW-bound empirical confirmation: subscriber-delivered ⊆ new-leader log.

    Produce acks=Leader records right before killing the leader.  Some will be
    un-replicated / uncommitted → truncated on the new leader.  A subscriber
    running HW-bounded / read_committed throughout and after the failover must
    NEVER deliver a record that the new leader subsequently truncated.

    Implementation:
      * Start a background subscriber against the leader from EARLIEST before
        the kill (it will see the truncation boundary when the leader dies).
      * Kill the leader SIGKILL.
      * Run a post-failover subscriber against the new leader from EARLIEST.
      * Build the "authoritative set" = records present on the new leader's log
        (post-failover subscriber delivered set).
      * Union the background subscriber's delivered set with the post-failover
        subscriber's delivered set.
      * Assert: the union of all subscriber deliveries ⊆ the authoritative set
        from the new leader.  Any phantom record (delivered but NOT on the new
        leader) violates GR-6.
    """
    cluster = fresh_cluster
    keys = 4
    records = 200
    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Pre-kill batch (acks=All) so the subscriber has something to start on ─
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, leader_svc, 1, cid_pre, records, keys,
                          acks="all")
    assert pre_result.ok, (
        "pre-kill produce (acks=all) failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )

    # ── Background subscriber: starts BEFORE the kill so it may see un-replicated
    #    records that get truncated on the new leader.  HW-bounded: the broker
    #    must not deliver records above the committed HW (GR-6).
    bg_holder: dict[str, Results] = {}

    def _bg_subscribe():
        bg_holder["r"] = _subscribe_all(
            cluster, leader_svc, _next_cid_base(), keys, window_ms=30000
        )

    bg_t = threading.Thread(target=_bg_subscribe, daemon=True)
    bg_t.start()

    # ── Produce acks=Leader while the background subscriber is running ────────
    # Some of these records will be un-replicated when the leader is killed.
    cid_leader_batch = _next_cid_base()
    leader_batch_holder: dict[str, Results] = {}

    def _run_leader_produce():
        leader_batch_holder["r"] = _produce(
            cluster, leader_svc, 1, cid_leader_batch, 150, keys,
            rate_hz=40, acks="leader",
        )

    prod_t = threading.Thread(target=_run_leader_produce, daemon=True)
    prod_t.start()

    # Let production get going, then kill immediately (small tail un-replicated).
    time.sleep(1.5)
    faults.kill_node(cluster, leader_svc, signal.SIGKILL)

    prod_t.join(timeout=300)
    bg_t.join(timeout=120)

    leader_batch = leader_batch_holder.get("r", Results())
    bg_sub = bg_holder.get("r", Results())

    # ── Wait for re-election ──────────────────────────────────────────────────
    new_lid = cluster.wait_for_new_leader(leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != leader_svc, (
        "leader did not change after SIGKILL (phantom-read test)"
    )

    # ── Post-failover subscriber: authoritative view of the new leader's log ──
    # This establishes the "truth set" — every record present on the new leader.
    post_sub = _subscribe_all(cluster, new_leader_svc, _next_cid_base(), keys,
                              window_ms=90000)

    # The authoritative set: all records the new leader serves.
    authoritative_ids = {(m.cid, m.key, m.seq) for m in post_sub.all_delivered}

    # The background subscriber's delivered set.
    bg_delivered_ids = {(m.cid, m.key, m.seq) for m in bg_sub.all_delivered}

    # Union of ALL subscriber deliveries.
    all_delivered_ids = bg_delivered_ids | authoritative_ids

    # Phantom records: delivered to any subscriber but NOT on the new leader.
    # A non-empty phantom set means a subscriber saw a record above the HW
    # that was subsequently truncated — a GR-6 violation.
    # NOTE: the post-failover subscriber IS the authority, so
    # authoritative_ids ⊆ authoritative_ids is trivially true.  The check that
    # matters is bg_delivered_ids ⊆ authoritative_ids.
    phantom_ids = bg_delivered_ids - authoritative_ids
    n_phantom = len(phantom_ids)

    print(f"\n[DR-15 phantom-read] background subscriber delivered: "
          f"{len(bg_delivered_ids)} records")
    print(f"[DR-15 phantom-read] new-leader authoritative set: "
          f"{len(authoritative_ids)} records")
    print(f"[DR-15 phantom-read] phantom records (bg-delivered ∉ new-leader): "
          f"{n_phantom}")

    assert n_phantom == 0, (
        f"GR-6 violated: {n_phantom} records delivered to background subscriber "
        f"but ABSENT from new leader {new_leader_svc} (truncated tail leaked "
        f"above HW).  Phantom (cid,key,seq): {sorted(phantom_ids)[:20]}"
    )

    # The acks=All pre-kill batch must be fully present on the new leader.
    merged_all = Results(clients=pre_result.clients + post_sub.clients)
    assert_no_committed_loss(merged_all)

    # Post-failover ordering + exactly-once for the authoritative subscriber.
    assert_per_key_ordering(post_sub)
    assert_exactly_once(post_sub)
    assert_single_leader(cluster)

    print("[DR-15 phantom-read] PASS: no phantom read — "
          "subscriber-delivered ⊆ post-failover-leader log (GR-6 confirmed).")

    _ = (all_delivered_ids, leader_batch)  # referenced for context; suppress linter


# ── Scenario 3: Log divergence guard ─────────────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(900)
def test_dr15_log_divergence_guard(fresh_cluster):
    """After leader crash + re-election + old-leader rejoin, logs must converge.

    After the old leader restarts as a follower and catches up, its log must
    be byte-identical to the new leader's for the committed prefix.  Concretely:
    a subscribe-from-earliest on the OLD leader must deliver the SAME committed
    records (by cid,key,seq) as a subscribe-from-earliest on the new leader.
    No divergence, no phantom-committed records, no stale tail entries.

    Uses assert_no_committed_loss over both subscribe results to confirm
    convergence: if the old leader's log diverged (extra phantom records), the
    old-leader subscriber would deliver records not in the committed set and the
    new-leader subscriber would miss them — the cross-check catches both.
    """
    cluster = fresh_cluster
    keys = 4
    records_pre = 200
    records_post = 100
    lid, old_leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Pre-kill batch: commit acks=All (majority-durable) ───────────────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, old_leader_svc, 1, cid_pre, records_pre, keys,
                          acks="all")
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

    new_lid = cluster.wait_for_new_leader(old_leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != old_leader_svc, (
        "leader did not change after SIGKILL (divergence test)"
    )

    # ── Produce more acks=All records to the new leader ──────────────────────
    cid_post = _next_cid_base()
    post_result = _produce(cluster, new_leader_svc, 1, cid_post, records_post, keys,
                           acks="all")
    assert post_result.ok, (
        "post-failover produce to new leader failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in post_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post_result.all_produced) == records_post, (
        f"post-failover: produced {len(post_result.all_produced)} != {records_post}"
    )

    # ── Restart the old leader as a follower + wait for catch-up ─────────────
    # Use compose.start (not compose.restart) since the container is in 'exited'
    # state after SIGKILL — mirrors DR-7 scenario 4.
    cluster.compose.start(old_leader_svc)
    # Generous catch-up window: the old leader must replicate pre-kill +
    # post-kill records from the new leader before re-entering ISR.
    time.sleep(15.0)

    running = cluster.running_services()
    assert old_leader_svc in running, (
        f"restarted old leader {old_leader_svc} not running after restart"
    )
    assert len(running) == 3, (
        f"expected 3 running nodes after restart, got {running}"
    )

    # The new leader must remain the single leader — old node as follower only.
    current_leader = assert_single_leader(cluster)
    assert node_id_to_service(current_leader) == new_leader_svc, (
        f"old leader {old_leader_svc} appears to have re-elected itself "
        f"(current leader: {node_id_to_service(current_leader)}, "
        f"expected {new_leader_svc})"
    )

    # ── Convergence check 1: new leader's log ────────────────────────────────
    cid_sub_new = _next_cid_base()
    sub_new = _subscribe_all(cluster, new_leader_svc, cid_sub_new, keys,
                             window_ms=90000)

    merged_new = Results(clients=pre_result.clients + post_result.clients + sub_new.clients)
    assert_no_committed_loss(merged_new)
    assert_per_key_ordering(sub_new)
    assert_exactly_once(sub_new)

    new_leader_delivered = {(m.cid, m.key, m.seq) for m in sub_new.all_delivered}

    # ── Convergence check 2: old leader's log (must match new leader's) ──────
    cid_sub_old = _next_cid_base()
    sub_old = _subscribe_all(cluster, old_leader_svc, cid_sub_old, keys,
                             window_ms=90000)

    merged_old = Results(clients=pre_result.clients + post_result.clients + sub_old.clients)
    assert_no_committed_loss(merged_old)
    assert_per_key_ordering(sub_old)
    assert_exactly_once(sub_old)

    old_leader_delivered = {(m.cid, m.key, m.seq) for m in sub_old.all_delivered}

    # ── Divergence check: committed prefix must be identical ─────────────────
    # Records on new leader but NOT on old leader (old leader missed some).
    missing_on_old = new_leader_delivered - old_leader_delivered
    # Records on old leader but NOT on new leader (phantom / stale tail on old).
    phantom_on_old = old_leader_delivered - new_leader_delivered

    print(f"\n[DR-15 divergence] new-leader delivered: {len(new_leader_delivered)}")
    print(f"[DR-15 divergence] old-leader delivered: {len(old_leader_delivered)}")
    print(f"[DR-15 divergence] missing on old leader: {len(missing_on_old)}")
    print(f"[DR-15 divergence] phantom on old leader: {len(phantom_on_old)}")

    assert not missing_on_old, (
        f"log divergence: {len(missing_on_old)} committed records present on new "
        f"leader {new_leader_svc} but ABSENT from restarted old leader "
        f"{old_leader_svc} — catch-up incomplete or truncation over-reached. "
        f"Missing (cid,key,seq): {sorted(missing_on_old)[:20]}"
    )
    assert not phantom_on_old, (
        f"log divergence: {len(phantom_on_old)} phantom records on restarted old "
        f"leader {old_leader_svc} NOT present on new leader {new_leader_svc} — "
        f"stale / un-truncated tail survived leader restart. "
        f"Phantom (cid,key,seq): {sorted(phantom_on_old)[:20]}"
    )

    assert_single_leader(cluster)
    print("[DR-15 divergence] PASS: logs converged — "
          "committed prefix byte-identical by (cid,key,seq).")


# ── Scenario 4: Graceful (SIGTERM) vs ungraceful (SIGKILL) acks=All parity ──

@pytest.mark.fault
@pytest.mark.timeout(720)
@pytest.mark.parametrize("sig,label", [
    (signal.SIGTERM, "graceful_sigterm"),
    (signal.SIGKILL, "ungraceful_sigkill"),
])
def test_dr15_sigterm_vs_sigkill_acks_all_parity(fresh_cluster, sig, label):
    """acks=All committed-loss = 0 for both SIGTERM and SIGKILL leader kills.

    Lightweight complement to DR-7 scenario 3, scoped to the integrity focus
    of this suite: a single acks=All batch committed to the leader, then the
    leader is stopped (SIGTERM) or crashed (SIGKILL); a fresh subscribe on the
    new leader must deliver every acked record with zero loss.
    """
    cluster = fresh_cluster
    keys = 4
    records = 150
    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Commit a batch acks=All ───────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, leader_svc, 1, cid_pre, records, keys,
                          acks="all")
    assert pre_result.ok, (
        f"[{label}] pre-kill produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre_result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre_result.all_produced) == records, (
        f"[{label}] pre-kill: produced {len(pre_result.all_produced)} != {records}"
    )

    # ── Send the signal ───────────────────────────────────────────────────────
    faults.kill_node(cluster, leader_svc, sig)

    # Wait for re-election.
    new_lid = cluster.wait_for_new_leader(leader_svc)
    new_leader_svc = node_id_to_service(new_lid)
    assert new_leader_svc != leader_svc, (
        f"[{label}] leader did not change after {label}"
    )

    # ── Subscribe-from-earliest: all acks=All records must survive ────────────
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, new_leader_svc, cid_sub, keys, window_ms=90000)

    merged = Results(clients=pre_result.clients + sub.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)

    n_produced = len(pre_result.all_produced)
    n_delivered = len({(m.cid, m.key, m.seq) for m in sub.all_delivered}
                      & {(m.cid, m.key, m.seq) for m in pre_result.all_produced})
    print(f"\n[DR-15 parity {label}] acks=all committed: {n_produced}, "
          f"survived on new leader: {n_delivered}.  Loss=0 (as required).")

    # Restart the killed leader so the cluster heals for teardown.
    try:
        cluster.compose.start(leader_svc)
        time.sleep(5.0)
    except Exception:
        pass   # best-effort; fresh_cluster teardown (down -v) handles cleanup


# ── Scenario 5: Simultaneous all-3-node crash → cold restart → consistent ────

@pytest.mark.fault
@pytest.mark.timeout(1200)
def test_dr15_all_nodes_crash_cold_restart_consistent(fresh_cluster):
    """SIGKILL all 3 nodes simultaneously; restart all 3; assert consistency.

    Complement to DR-7 scenario 4 with an added divergence focus:
      1. Commit an acks=All batch (majority-flushed, guaranteed durable).
      2. SIGKILL all 3 nodes.
      3. Restart all 3 via cluster.compose.start (per-node, staggered).
      4. Wait for a new leader (disk replay + Raft election on cold start).
      5. Subscribe-from-earliest: acks=All committed set SURVIVES + is consistent
         on EVERY node (no divergence: each node's log equals the leader's log).
      6. Fresh acks=All produce+subscribe confirms the cluster is fully serving.

    HARNESS NOTE: `cluster.compose.start(svc)` is called per-node (not as a
    batch) because the current harness has no `start_all` primitive.  The
    controller may wish to add `Compose.start_all(*services)` to reduce the
    cold-restart race window (see DR-7 module docstring).
    """
    cluster = fresh_cluster
    keys = 4
    records = 200
    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # ── Pre-crash batch: commit acks=All (majority-flushed) ──────────────────
    cid_pre = _next_cid_base()
    pre_result = _produce(cluster, leader_svc, 1, cid_pre, records, keys,
                          acks="all")
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

    # ── SIGKILL all 3 nodes (short inter-kill gap to avoid panic on last node) ─
    for svc in SERVICES:
        faults.kill_node(cluster, svc, signal.SIGKILL)
        time.sleep(0.5)

    # Verify all nodes are down.
    time.sleep(3.0)
    running = cluster.running_services()
    assert len(running) == 0, (
        f"expected 0 running nodes after killing all 3, got {running}"
    )

    # ── Restart all 3 nodes (staggered to avoid thundering-herd on Raft) ──────
    for svc in SERVICES:
        cluster.compose.start(svc)
        time.sleep(0.3)

    # Wait for election — generous: cold-start requires disk replay + Raft.
    new_lid = cluster.wait_for_leader(timeout_s=120)
    new_leader_svc = node_id_to_service(new_lid)
    assert_single_leader(cluster)

    running = cluster.running_services()
    assert len(running) == 3, (
        f"expected 3 running nodes after cold restart, got {running}"
    )

    # ── Durability: acks=All committed set survives full cold restart ──────────
    cid_sub_leader = _next_cid_base()
    sub_leader = _subscribe_all(cluster, new_leader_svc, cid_sub_leader, keys,
                                window_ms=120000)

    merged_leader = Results(clients=pre_result.clients + sub_leader.clients)
    assert_no_committed_loss(merged_leader)
    assert_per_key_ordering(sub_leader)
    assert_exactly_once(sub_leader)

    leader_delivered_ids = {(m.cid, m.key, m.seq) for m in sub_leader.all_delivered}

    # ── Convergence: each follower's log must also match the leader's ─────────
    follower_svcs = [s for s in SERVICES if s != new_leader_svc]
    for fsvc in follower_svcs:
        # Brief catch-up window before subscribing from a follower.
        time.sleep(5.0)
        cid_sub_f = _next_cid_base()
        sub_f = _subscribe_all(cluster, fsvc, cid_sub_f, keys, window_ms=120000)

        follower_delivered_ids = {(m.cid, m.key, m.seq) for m in sub_f.all_delivered}

        missing_on_follower = leader_delivered_ids - follower_delivered_ids
        phantom_on_follower = follower_delivered_ids - leader_delivered_ids

        print(f"\n[DR-15 cold-restart divergence] {fsvc}: "
              f"delivered={len(follower_delivered_ids)}, "
              f"missing={len(missing_on_follower)}, "
              f"phantom={len(phantom_on_follower)}")

        assert not missing_on_follower, (
            f"cold-restart divergence: {len(missing_on_follower)} committed records "
            f"present on new leader {new_leader_svc} but ABSENT from {fsvc}. "
            f"Missing (cid,key,seq): {sorted(missing_on_follower)[:20]}"
        )
        assert not phantom_on_follower, (
            f"cold-restart divergence: {len(phantom_on_follower)} phantom records "
            f"on {fsvc} NOT present on new leader {new_leader_svc}. "
            f"Phantom (cid,key,seq): {sorted(phantom_on_follower)[:20]}"
        )

        # Per-follower no-committed-loss and ordering checks.
        merged_f = Results(clients=pre_result.clients + sub_f.clients)
        assert_no_committed_loss(merged_f)
        assert_per_key_ordering(sub_f)
        assert_exactly_once(sub_f)

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

    print(f"\n[DR-15 cold-restart] PASS: acks=All committed set survived + "
          f"logs converged on all 3 nodes after simultaneous SIGKILL + cold restart.")
