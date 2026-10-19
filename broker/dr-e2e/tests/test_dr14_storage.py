"""DR-14: Storage / disk / shared-memory faults.

Tests the cluster's resilience to on-disk log corruption, truncated/torn write
tails, and shared-memory sizing.  Milestone C storage guarantees tested here:

  * acks=All majority-durable before ProduceResponse is sent (GR-9).
  * recover()/CRC detects torn tails on restart and either heals them from the
    leader's copy (follower case) or refuses to serve corrupt data (leader case).
  * No committed (acks=All) record is ever silently corrupted or lost — the
    only acceptable outcomes for a damaged log are:
      (a) Recover: node drops the torn/corrupt tail, re-fetches from the leader,
          rejoins the ISR, and serves CONSISTENT data.
      (b) Clean refusal: node exits with a non-zero exit code (detects corruption
          and refuses to start) rather than serving bad data.
    NEVER: silent corruption — serving a record whose bytes differ from what the
    producer committed.

Scenarios:
  1. TRUNCATED / TORN LOG TAIL → recovery (IMPLEMENTED)
     Commit acks=All; SIGKILL a follower; truncate/corrupt the last few bytes of
     its on-disk log while it is stopped; restart it.  Assert:
       * node comes back up (or exits non-zero — clean refusal);
       * IF it comes back: re-fetches the torn tail from the leader (replication
         heals it) and serves the committed set without loss;
       * the cluster delivers all committed records (follower's damage is healed
         by the leader copy) with ordering, exactly-once, single-leader.
     This is the key DR-14 case: validates recover()/CRC + replication healing.

  2. MID-LOG CORRUPTION → detection (IMPLEMENTED where feasible)
     Corrupt bytes in the MIDDLE of a stopped follower's log (not just the tail).
     Restart the node.  Assert:
       * node detects the corruption (CRC) and either truncates-from-corruption
         and re-fetches from the leader, or exits non-zero (clean refusal);
       * never serves the corrupt record;
       * if the node recovers, the cluster delivers all committed records.
     If mid-log recovery is NOT supported, assert exit != 0 (clean refusal).

  3. /dev/shm SIZING (LITE — IMPLEMENTED)
     Verify the cluster starts and serves correctly under the configured shm_size
     (256 MiB per node, per docker-compose.yml).  This is an existence check:
     confirms shm_size is sufficient for Aeron CnC + log buffers.
     shm-EXHAUSTION injection (filling /dev/shm to force alloc failure) is
     @pytest.mark.skip (needs a constrained shm mount; unreliable on Docker Desktop).

  4. FSYNC-FAILURE / DISK-FULL (SKIP — documented)
     @pytest.mark.skip placeholders with reasons.  These need FUSE/dm-error or a
     full tmpfs mount, unreliable on Docker Desktop LinuxKit / macOS Rosetta; enable
     on Linux CI with appropriate kernel/mount setup.

  5. LOST DATA DIR (ISR v3, IMPLEMENTED)
     A node whose disk is lost and replaced (SIGKILL, volume wiped, restarted with no pause) while producers run and
     DDLs have been committed.  The wiped node may lead a partition, lead __catalog, or be the controller.  It must
     come back UNCONFIRMED (holding its leader roles), be stripped and confirmed, and rejoin; no follower may ever
     truncate below its committed offset; no acked record is lost.
       * test_dr14_disk_loss_fast_restart_never_truncates_committed[target_role][fmt]
       * test_dr14_wiped_node_without_format_refuses_to_start (production formatting: exit 78 until `--join`)

CID window: 1_300_000+ — disjoint from all prior DR suites.
  DR-1: n*1000, DR-2: 100_000+, DR-3: 300_000+, DR-4: 400_000+,
  DR-5: 500_000+, DR-6: 600_000+, DR-7: 700_000+, DR-10: 800_000+,
  DR-11: 900_000+, DR-12/8: 1_000_000+, DR-13: 1_200_000+.
  DR-14: 1_300_000+ (stride 3_000).

RUNE_DATA_DIR: /var/lib/rune (baked into docker-compose.yml x-broker-env).
Volumes: rune-data-1 → rune-1:/var/lib/rune, etc.
Access: `docker exec <cid> ...` works on a RUNNING container; for a STOPPED
container the volume is still present on the host (named volume) but the
container's filesystem is accessible via `docker cp` or by starting a temporary
throwaway container mounted on the same volume.  The harness helpers in
faults.py (corrupt_log_tail, truncate_log_tail, corrupt_log_mid) exec into the
STOPPED container is NOT possible — instead they use `docker run --rm -v <vol>:/data`
to mount the named volume into a minimal container and apply the mutation.
This is documented in faults._exec_on_volume().

HARNESS UNCERTAINTIES (flagged for controller review):
  * Volume access via docker-run-on-volume: the named volumes (rune-data-1 etc.)
    are attached to the stopped container.  The compose volume name follows the
    pattern `<project>_rune-data-<n>` where project is "rune-dr-fault" (the
    fresh_cluster project name).  faults._volume_name() derives this from the
    compose project name and the service index (1-3).
  * Log file layout inside /var/lib/rune: the broker may write segment files as
    /var/lib/rune/<topic>/<partition>/<segment>.log (or a flat structure).  The
    helpers use `find /data -name '*.log' -o -name '*.seg'` to locate log files
    and pick the last segment (sort + tail -1).  If the broker uses a different
    extension the find command will need updating.
  * Torn-tail vs mid-log: the helpers assume the last-found file is the active
    segment tail.  Mid-log corruption targets the FIRST found file (oldest).
    These heuristics work for small test runs; for large runs with multiple
    segments, the file ordering may differ.
  * CRC detection: whether the broker actually CRC-checks on open (Phase-1 storage)
    vs deferring to read time is not known from this test.  The test asserts the
    end-to-end invariant (no served corruption) and is agnostic about WHEN the
    check fires.
"""

from __future__ import annotations

import hashlib
import re
import signal
import threading
import time

import pytest

from harness import faults
from harness.dockerutil import run
from harness.cluster import BOOT_LINE, SERVICES, node_id_to_service, slice_since_last_boot
from harness.driver import DRAIN_QUIET_MS, Results, run_clients
from harness.invariants import (
    assert_exactly_once,
    assert_no_committed_loss,
    assert_per_key_ordering,
    assert_single_leader,
)

# ── Per-run disjoint CID window ───────────────────────────────────────────────
# DR-14: 1_300_000+ — fully disjoint from all prior suites.
_CID_STRIDE = 3_000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 1_300_000 + _run_counter["n"] * _CID_STRIDE


# Generous connect budget (Docker Desktop Aeron UDP establishment tail).
_CONNECT_MS = 40_000
_STAGGER_MS = 400


# ── Shared produce/subscribe helpers (mirror DR-7 / DR-13 style) ─────────────

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
        acks="all",                # GR-9: majority-durable ack before response
        expect_own=True,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_all(cluster, target_svc: str, cid_base: int, keys: int,
                   window_ms: int = 90_000, drain_quiet_ms: int = DRAIN_QUIET_MS) -> Results:
    """Subscribe-from-earliest on target_svc and drain the whole partition set."""
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
        drain_quiet_ms=drain_quiet_ms,   # stop once the static log is fully read (driver._drain_complete)
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


def _assert_cluster_healthy(cluster, leader_svc: str, keys: int) -> None:
    """Assert ISR healed (3 nodes) and acks=All produce+subscribe works."""
    running = cluster.running_services()
    assert len(running) == 3, (
        f"cluster-healthy: expected 3 running nodes, got {running}"
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
        subscribe_ms=60_000,
        start_stagger_ms=_STAGGER_MS,
    )
    assert post.ok, (
        "cluster-healthy produce+subscribe failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in post.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(post.all_produced) == 30, (
        f"cluster-healthy: produced {len(post.all_produced)} != 30"
    )
    assert_single_leader(cluster)


# ── Scenario 1: Truncated / torn log tail → recovery ─────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(720)
@pytest.mark.parametrize("target_role", ["follower", "leader"])
def test_dr14_torn_log_tail_recovery(fresh_cluster, target_role):
    """SIGKILL a node; truncate/corrupt the TAIL of its on-disk log while down;
    restart it; assert:

      * No silent corruption: the node either recovers consistently (recover()/CRC
        drops the torn tail and re-fetches from the leader's copy) OR exits
        non-zero (clean refusal) — never serves a bad record.
      * For the follower case: if the node recovers, all committed acks=All
        records survive (leader holds the authoritative copy).
      * Cluster delivers the full committed set with ordering, exactly-once,
        single-leader throughout.

    ``target_role`` parametrizes which kind of node gets the torn tail:
      * "follower" — the simpler case: the torn tail is on a follower; the leader
        holds the intact committed log and heals the follower via replication.
      * "leader" — after SIGKILL the leader, a new leader is elected; we corrupt
        the old leader's log tail; restart it; assert it rejoins as a follower
        and the cluster (new leader's intact copy) delivers all committed records.

    This is the KEY DR-14 case validating recover()/CRC + replication healing.

    HARNESS UNCERTAINTY: faults.truncate_log_tail() accesses the node's named
    Docker volume via a temporary alpine container (`docker run --rm -v <vol>:/data`).
    The compose project name for fresh_cluster is "rune-dr-fault"; the volume name
    is therefore "rune-dr-fault_rune-data-<n>" (compose naming convention).
    If the volume does not exist (e.g., the container was never started) the helper
    raises RuntimeError.  The volume DOES exist while the container is stopped
    (Docker retains named volumes until `compose down -v`).
    """
    cluster = fresh_cluster
    keys = 4
    records = 150

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    # Pick the target node based on role.
    if target_role == "follower":
        target_svc = cluster.follower_service()
        assert target_svc != leader_svc
        produce_svc = leader_svc
    else:
        # target_role == "leader": kill the leader, produce to new leader,
        # corrupt the old leader's log, restart it as a follower.
        target_svc = leader_svc
        produce_svc = leader_svc  # updated to new leader after kill below

    # ── Pre-kill batch: commit acks=All ──────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, produce_svc, 1, cid_pre, records, keys)
    assert pre.ok, (
        f"[{target_role}] pre-kill produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) == records, (
        f"[{target_role}] pre-kill: produced {len(pre.all_produced)} != {records}"
    )

    # ── SIGKILL the target node ───────────────────────────────────────────────
    faults.kill_node(cluster, target_svc, signal.SIGKILL)
    cluster.wait_running([x for x in SERVICES if x != target_svc], 3.0)   # was a fixed 3 s sleep

    running = cluster.running_services()
    assert target_svc not in running, (
        f"[{target_role}] target {target_svc} still running after SIGKILL"
    )

    # For the leader case: wait for a new leader; produce more to new leader.
    post_kill_result: Results | None = None
    if target_role == "leader":
        new_lid = cluster.wait_for_new_leader(target_svc)
        new_leader_svc = node_id_to_service(new_lid)
        assert new_leader_svc != target_svc
        produce_svc = new_leader_svc

        cid_post_kill = _next_cid_base()
        post_kill_result = _produce(cluster, produce_svc, 1, cid_post_kill, 50, keys)
        assert post_kill_result.ok, (
            "[leader] post-kill produce to new leader failed:\n"
            + "\n\n".join(
                f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
                for c in post_kill_result.clients if c.exit_code != 0 or c.rc != 0
            )
        )
    else:
        # Follower down; ISR = leader + other follower; cluster still serves.
        assert_single_leader(cluster)

    # ── Corrupt the TAIL of the stopped node's on-disk log ───────────────────
    # faults.truncate_log_tail() mounts the named Docker volume into a temporary
    # container and uses dd/truncate to remove the last N bytes of the last log
    # segment, simulating a torn write or an incomplete fsync before crash.
    # The volume is identified by the compose project name ("rune-dr-fault") and
    # the service's node index.
    service_idx = SERVICES.index(target_svc) + 1  # 1-based: rune-1 → 1
    faults.truncate_log_tail(
        project=cluster.compose.project,
        service_idx=service_idx,
        tail_bytes=128,           # tear off the last 128 bytes — enough to cut a
                                  # record in half without destroying the whole log
    )

    # ── Restart the damaged node ──────────────────────────────────────────────
    # compose.start() restarts a stopped (exited) container.
    cluster.compose.start(target_svc)
    # KEPT fixed: the outcome below branches on recover-and-heal vs clean refusal - the damaged node needs the full
    # window to either crash on the corruption or rejoin, or the branch would be picked on a half-started node.
    time.sleep(12.0)  # generous: recovery + replication catch-up + ISR re-join

    # ── Outcome assertion: recover-and-heal OR clean-refusal ─────────────────
    running_after = cluster.running_services()

    if target_svc not in running_after:
        # ACCEPTABLE: the node detected the corruption and exited non-zero
        # (clean refusal — better than serving corrupt data).
        # Verify the cluster (surviving nodes) still serves the full committed set.
        surviving_svc = produce_svc if target_role == "leader" else leader_svc
        assert surviving_svc in running_after, (
            f"[{target_role}] surviving service {surviving_svc} not running "
            f"after damaged node refused to start"
        )
        # Drain from the leader/surviving node — all acks=All records must survive.
        cid_sub = _next_cid_base()
        sub = _subscribe_all(cluster, surviving_svc, cid_sub, keys, window_ms=90_000)
        all_clients = pre.clients + sub.clients
        if post_kill_result:
            all_clients = pre.clients + post_kill_result.clients + sub.clients
        assert_no_committed_loss(Results(clients=all_clients))
        assert_per_key_ordering(sub)
        assert_exactly_once(sub)
        assert_single_leader(cluster)
        return  # clean refusal + surviving cluster is a passing outcome

    # RECOVERS: node came back up — assert it serves CONSISTENT data (no corrupt
    # records) by subscribing from it and from the surviving leader, and asserting
    # the full committed set is intact.
    assert len(running_after) >= 2, (
        f"[{target_role}] fewer than 2 nodes running after restart: {running_after}"
    )

    # Wait for single leader in the post-recovery cluster.
    recover_leader_id = cluster.wait_for_leader(timeout_s=60)
    recover_leader_svc = node_id_to_service(recover_leader_id)
    assert_single_leader(cluster)

    # Subscribe from the leader (the authoritative copy) — full committed set.
    cid_sub_leader = _next_cid_base()
    sub_leader = _subscribe_all(cluster, recover_leader_svc, cid_sub_leader,
                                keys, window_ms=90_000)

    all_clients_leader = pre.clients + sub_leader.clients
    if post_kill_result:
        all_clients_leader = pre.clients + post_kill_result.clients + sub_leader.clients
    assert_no_committed_loss(Results(clients=all_clients_leader))
    assert_per_key_ordering(sub_leader)
    assert_exactly_once(sub_leader)

    # Subscribe from the RECOVERED node to assert it re-fetched the torn tail
    # from the leader (no corrupt records served from its own log).
    # We only do this if the recovered node is up.
    if target_svc in running_after:
        cid_sub_recovered = _next_cid_base()
        sub_recovered = _subscribe_all(cluster, target_svc, cid_sub_recovered,
                                       keys, window_ms=90_000)
        # No committed loss from the recovered node's perspective.
        ref_clients = pre.clients + sub_recovered.clients
        if post_kill_result:
            ref_clients = pre.clients + post_kill_result.clients + sub_recovered.clients
        assert_no_committed_loss(Results(clients=ref_clients))
        assert_per_key_ordering(sub_recovered)
        assert_exactly_once(sub_recovered)

    assert_single_leader(cluster)

    # ISR fully healed: acks=All must be satisfiable across 3 nodes.
    if len(running_after) == 3:
        _assert_cluster_healthy(cluster, recover_leader_svc, keys)


# ── Scenario 2: Mid-log corruption → detection ───────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(600)
def test_dr14_mid_log_corruption_detection(fresh_cluster):
    """Corrupt bytes in the MIDDLE of a stopped follower's log (not just the tail).

    Restart the node.  Assert:
      * The node detects the corruption (CRC) and either:
          (a) truncates from the corruption point + re-fetches from the leader, or
          (b) exits non-zero (clean refusal to serve corrupt data).
      * NEVER: node starts and serves a corrupt record.
      * If the node recovers, the cluster delivers the full committed set with no
        ordering violations, exactly-once, and single-leader.

    IMPORTANT: mid-log recovery (truncate-from-corruption + re-fetch) is more
    complex than tail recovery.  If the broker does not implement it, exit != 0
    (clean refusal) is the acceptable outcome — tested by asserting the surviving
    cluster still serves all committed records.

    HARNESS NOTE: faults.corrupt_log_mid() overwrites 16 bytes at a fixed offset
    inside the FIRST (oldest) log segment found in the volume.  For small test runs
    with a single segment file this is the same file as the tail, but the corruption
    is anchored near the BEGINNING of the file (offset 64) rather than the end,
    simulating a mid-log bit-flip.  If the broker writes multiple segment files,
    the FIRST file is the earliest segment — corrupting it exercises the full
    CRC scan from the beginning of the log.
    """
    cluster = fresh_cluster
    keys = 4
    records = 120

    lid, leader_svc = cluster.identify_leader()
    assert_single_leader(cluster)

    follower_svc = cluster.follower_service()
    assert follower_svc != leader_svc

    # ── Commit acks=All records ───────────────────────────────────────────────
    cid_pre = _next_cid_base()
    pre = _produce(cluster, leader_svc, 1, cid_pre, records, keys)
    assert pre.ok, (
        "pre-kill produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in pre.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre.all_produced) == records, (
        f"pre-kill: produced {len(pre.all_produced)} != {records}"
    )

    # ── SIGKILL the follower ──────────────────────────────────────────────────
    faults.kill_node(cluster, follower_svc, signal.SIGKILL)
    cluster.wait_running([x for x in SERVICES if x != follower_svc], 3.0)   # was a fixed 3 s sleep

    running = cluster.running_services()
    assert follower_svc not in running, (
        f"follower {follower_svc} still running after SIGKILL"
    )

    # ── Corrupt bytes in the MIDDLE of the follower's log ────────────────────
    service_idx = SERVICES.index(follower_svc) + 1
    faults.corrupt_log_mid(
        project=cluster.compose.project,
        service_idx=service_idx,
        corrupt_offset=64,        # skip the first 64 bytes (header), then flip
        corrupt_length=16,        # overwrite 16 bytes with 0xFF — breaks any CRC
    )

    # ── Restart the follower ──────────────────────────────────────────────────
    cluster.compose.start(follower_svc)
    time.sleep(12.0)   # KEPT fixed: same recover-or-refuse branch as above needs the full window

    # ── Outcome: recover + re-fetch OR clean refusal ──────────────────────────
    running_after = cluster.running_services()

    # Either way, the LEADER must still be up and serving.
    assert leader_svc in running_after, (
        f"leader {leader_svc} unexpectedly down after follower corruption scenario"
    )
    assert_single_leader(cluster)

    # The surviving cluster (leader + possibly other follower) must deliver the
    # full committed set — committed data is never lost due to a follower's damage.
    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=90_000)
    assert_no_committed_loss(Results(clients=pre.clients + sub.clients))
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)

    if follower_svc in running_after:
        # Node recovered: subscribe from it to confirm no corrupt records served.
        cid_sub_rec = _next_cid_base()
        sub_rec = _subscribe_all(cluster, follower_svc, cid_sub_rec,
                                 keys, window_ms=90_000)
        assert_no_committed_loss(Results(clients=pre.clients + sub_rec.clients))
        assert_per_key_ordering(sub_rec)
        assert_exactly_once(sub_rec)
        # ISR healed if all 3 back.
        if len(running_after) == 3:
            _assert_cluster_healthy(cluster, leader_svc, keys)
    else:
        # Clean refusal (exit != 0): acceptable.  The cluster must still serve
        # all committed records from the surviving nodes.
        # (already asserted via sub above — no additional check needed.)
        pass


# ── Scenario 3: /dev/shm sizing (lite) ───────────────────────────────────────

@pytest.mark.fault
@pytest.mark.timeout(300)
def test_dr14_shm_size_sufficient(fresh_cluster):
    """Verify the cluster starts and serves acks=All under the configured shm_size.

    The docker-compose.yml sets shm_size: "256m" per node, which must be large
    enough for Aeron CnC file + media driver log buffers.  This test is an
    existence / regression check: if shm_size is too small, Aeron cannot start
    and the broker will crash at startup (no leader elected → fresh_cluster.up()
    would time out).

    The fact that fresh_cluster.up() completed and a leader was elected is itself
    the main assertion.  We add a small produce+subscribe round-trip to confirm
    the data plane is operational under the configured shm allocation.

    shm-EXHAUSTION injection (filling /dev/shm to force CnC / log-buffer alloc
    failure) is tested as a skip below — it requires a constrained shm mount
    (e.g., tmpfs with a small size limit) that is unreliable on Docker Desktop's
    LinuxKit VM / macOS Rosetta.
    """
    cluster = fresh_cluster
    keys = 4
    records = 60

    # fresh_cluster.up() succeeded → leader elected → shm_size is sufficient.
    assert_single_leader(cluster)
    running = cluster.running_services()
    assert len(running) == 3, (
        f"shm-size check: expected 3 running nodes on startup, got {running}"
    )

    # Data-plane round-trip confirms Aeron media drivers are fully operational.
    lid, leader_svc = cluster.identify_leader()
    cid = _next_cid_base()
    result = _produce(cluster, leader_svc, 1, cid, records, keys)
    assert result.ok, (
        "shm-size produce failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1200:]}"
            for c in result.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(result.all_produced) == records, (
        f"shm-size: produced {len(result.all_produced)} != {records}"
    )

    cid_sub = _next_cid_base()
    sub = _subscribe_all(cluster, leader_svc, cid_sub, keys, window_ms=60_000)
    assert_no_committed_loss(Results(clients=result.clients + sub.clients))
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)


@pytest.mark.fault
@pytest.mark.timeout(300)
def test_dr14_shm_exhaustion_injection(tmp_path):
    """/dev/shm capped at 1 MiB on every broker: the cluster must still start, elect and serve acks=All.

    The original premise ("Aeron CnC / log buffers live in /dev/shm, so a tiny shm must fail startup cleanly")
    does not hold for this broker: its Aeron directory is RUNE_DATA_DIR/aeron on the data volume (rune_role
    reads it there), not /dev/shm - measured 2026-10-15 on the Linux VM, brokers with shm_size=1m start and
    elect normally.  So the injected fault is a 1 MiB /dev/shm, and the invariant is that the broker has NO
    hidden /dev/shm dependency: it forms a cluster and serves a committed, ordered, exactly-once round trip.
    (A 1 MiB shm could not hold even one 4 MiB Aeron term, so any shm-backed log buffer would fail here.)
    """
    from harness.cluster import Cluster
    override = tmp_path / "shm-1m.yml"
    override.write_text("services:\n" + "".join(f"  {s}:\n    shm_size: 1m\n" for s in SERVICES))
    c = Cluster(project="rune-dr-shm1m", override_files=[str(override)])
    try:
        c.up(leader_timeout=120)
        c.wait_data_plane_ready(max_attempts=5, timeout_s=60)
        for svc in SERVICES:
            cid = c.container_id(svc)
            size = run(["docker", "inspect", "-f", "{{.HostConfig.ShmSize}}", cid], timeout=15).stdout.strip()
            assert size == str(1024 * 1024), f"{svc}: shm_size override not applied (ShmSize={size})"
        assert_single_leader(c)
        _, leader_svc = c.identify_leader()
        keys, records = 4, 60
        result = _produce(c, leader_svc, 1, _next_cid_base(), records, keys)
        assert result.ok and len(result.all_produced) == records, (
            "produce with a 1 MiB /dev/shm failed:\n" + "\n".join(x.raw[-1200:] for x in result.clients))
        sub = _subscribe_all(c, leader_svc, _next_cid_base(), keys, window_ms=60_000)
        assert_no_committed_loss(Results(clients=result.clients + sub.clients))
        assert_per_key_ordering(sub)
        assert_exactly_once(sub)
        assert_single_leader(c)
    finally:
        c.down()


# ── Scenario 4: fsync-failure / disk-full (SKIP — documented) ────────────────

@pytest.mark.skip(
    reason=(
        "fsync-failure injection needs a block device that fails writes (device-mapper dm-error / dm-flakey, "
        "or a FUSE filesystem returning EIO) under the broker's data dir; both are host-kernel setup (dmsetup / "
        "loop devices / /dev/fuse on the docker HOST).  The suite's host is a shared k3s node whose rules allow "
        "only containers and images the suite creates - no host device setup - so this cannot run there.  "
        "Previously stated: not available on Docker Desktop / macOS LinuxKit. "
        "Enable on Linux CI with a FUSE error-injector (e.g., `erofs` or a "
        "custom FUSE fs that returns EIO after N successful writes). "
        "Expected behaviour: the broker must either (a) propagate the fsync error "
        "upward and reject the Produce (return error to the client, no false ack) "
        "or (b) crash with a non-zero exit code — NEVER silently return a "
        "ProduceResponse OK for a record that was not actually fsynced to disk. "
        "This validates the durability contract: an acks=All ProduceResponse is "
        "only sent AFTER a successful fsync on a majority of the ISR."
    )
)
@pytest.mark.fault
def test_dr14_fsync_failure_graceful_degradation(fresh_cluster):  # pragma: no cover
    """[SKIP] Inject fsync failure; assert no false acks and graceful degradation.

    What this would test (Linux CI only):
      * Mount the broker's RUNE_DATA_DIR on a tmpfs/FUSE layer that returns EIO
        on fsync after writing N bytes.
      * Produce acks=All; assert the ProduceResponse is NOT OK (error returned to
        client) OR the broker crashes non-zero (fail-stop) — never a silent OK ack
        for a non-durable record.
      * After the fault is removed, assert the cluster recovers (re-election if
        needed) and accepts new acks=All produces correctly.
    """
    pass  # body never executes (skipped)


@pytest.mark.skip(
    reason=(
        "disk-full injection cannot isolate the log-append ENOSPC path: the broker keeps its Aeron directory "
        "(CnC + every publication's 3 x 4 MiB term buffers) INSIDE RUNE_DATA_DIR, so a capped data dir fills "
        "with Aeron buffers first - measured 2026-10-15 on the Linux VM: a 64 MiB tmpfs data dir makes all "
        "three brokers exit 1 while the cluster forms ('insufficient storage space' for peer publications, then "
        "'refusing shard mode - aeron dir has 5165056 bytes free but ... need 12648448'), before any record; an uncapped volume lives "
        "on the host disk (no per-container cap without host loop devices, not allowed on the shared k3s "
        "host).  Needs a broker option that puts the log directory on its own mount. "
        "Enable on Linux CI with: `--mount type=tmpfs,target=/var/lib/rune,"
        "tmpfs-size=16777216` in the compose file (override) and a produce loop "
        "that writes until ENOSPC. "
        "Expected behaviour: the broker must surface ENOSPC as a ProduceResponse "
        "error (or crash non-zero) rather than silently dropping records. "
        "After the fault is cleared (tmpfs re-mounted with more space or old logs "
        "compacted), the cluster must recover and serve the previously-committed "
        "(durable) records without loss."
    )
)
@pytest.mark.fault
def test_dr14_disk_full_graceful_degradation(fresh_cluster):  # pragma: no cover
    """[SKIP] Fill the data volume to 100%; assert no silent record loss.

    What this would test (Linux CI only):
      * Constrain RUNE_DATA_DIR to a small tmpfs (16 MiB).
      * Produce acks=All until ENOSPC; assert the broker surfaces the error
        rather than silently discarding records.
      * Free space (e.g., delete old segments); assert the broker resumes
        accepting produces and committed records are not lost.
      * Validate that the broker does not enter a state where it claims acks=All
        durability for records it could not persist (false-durable ack).
    """
    pass  # body never executes (skipped)


# ── Scenario 5: lost data dir (ISR v3) ───────────────────────────────────────

_TOPIC_MULTI = 1          # DEFAULT_TOPICS: events = 0, multi = 1, cg = 2
_TOPIC_CATALOG = -6       # the replicated __catalog partition
_COMMITTED_BELOW = "below our known committed offset"


def _volume_of(cluster, svc: str) -> str:
    return faults._volume_name(cluster.project, SERVICES.index(svc) + 1)


def _wipe_volume(cluster, svc: str) -> None:
    """Delete everything inside the (stopped) node's data volume: a replaced disk."""
    res = faults._exec_on_volume(_volume_of(cluster, svc), "rm -rf /data/* /data/.[!.]* /data/..?*")
    assert res.ok, f"wiping {svc}'s volume failed:\n{res.stdout}{res.stderr}"


def _pick_target(cluster, target_role: str) -> str:
    """The service playing `target_role`: the leader of a user partition, the __catalog leader, or the controller
    (the consensus / metadata leader)."""
    _, controller_svc = cluster.identify_leader()
    if target_role == "controller":
        return controller_svc
    topic = _TOPIC_MULTI if target_role == "partition_leader" else _TOPIC_CATALOG
    leaders = cluster.partition_leaders(topic)
    assert leaders, f"no becomeLeader line for topic {topic} in any broker log"
    return leaders[min(leaders)]


class _StoragePoller:
    """Polls a node's `rune_role` STORAGE line every 200 ms from a thread and keeps the readings in order."""

    def __init__(self, cluster, svc: str):
        self._cluster, self._svc = cluster, svc
        self.readings: list[dict] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        while not self._stop.is_set():
            r = self._cluster.read_storage(self._svc)
            if r is not None:
                self.readings.append(r)
            self._stop.wait(0.2)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=15)

    def first_index(self, state: str) -> int:
        return next((i for i, r in enumerate(self.readings) if r["state"] == state), -1)

    def wait_confirmed(self, timeout_s: float) -> dict | None:
        """The first reading with state CONFIRMED and outOfSync == 0, within `timeout_s`."""
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            for r in list(self.readings):
                if r["state"] == "CONFIRMED" and r["out_of_sync"] == 0:
                    return r
            time.sleep(0.2)
        return None


_HISTORY_ROW_RE = re.compile(r"^\s*\d+\s")


def _history_rows_digest(out: str) -> str:
    """sha256 of the history ROW lines only (a line that starts with an offset number): a header, footer or banner can
    never make two equal histories differ."""
    rows = [ln.rstrip() for ln in (out or "").splitlines() if _HISTORY_ROW_RE.match(ln)]
    return hashlib.sha256("\n".join(rows).encode()).hexdigest()


def _catalog_digest(cluster, svc: str) -> str:
    res = cluster.ql("show history;", seed_service=svc, timeout=90)
    assert res.ok, f"show history through {svc} failed:\n{res.stdout}{res.stderr}"
    return _history_rows_digest(res.stdout)


def _await_equal_catalog_digests(cluster, timeout_s: float = 60) -> dict:
    """Poll until every node's catalog history digest agrees (a follower's catalog HW may lag), then return them."""
    deadline = time.time() + timeout_s
    digests: dict = {}
    while time.time() < deadline:
        digests = {svc: _catalog_digest(cluster, svc) for svc in SERVICES}
        if len(set(digests.values())) == 1:
            return digests
        time.sleep(2.0)
    return digests


# The order of the gate's own log lines is read from the RESTARTED process's log (sampling a counter every 200 ms can
# miss a state that lasts under a second).
_UNCONFIRMED_LINE = "storage UNCONFIRMED (id="
_CONFIRMED_LINE = "storage CONFIRMED (id="
_HOLD_LINE = "REFUSED \u2014 storage unconfirmed"          # armLeadershipBump_'s warn for a held leader role
_GUARD_LINE = "REFUSED to truncate"                        # the truncation guard (a follower refused a lineage)


def _assert_recovered_after_disk_loss(cluster, wiped_svc: str, poller: "_StoragePoller", producers: list,
                                      subscribe_svc: str, keys: int, *, expect_role_hold: bool,
                                      restarted_at: float, boot_log: str | None = None) -> None:
    """The recovery assertions shared by the disk-loss cases (see the module docstring, scenario 5)."""
    # Within 120 s of the restart call: CONFIRMED and in sync again (the poller supplies outOfSync / pendingNodes).
    final = poller.wait_confirmed(max(1.0, restarted_at + 120 - time.time()))
    assert final is not None, f"{wiped_svc} not CONFIRMED with outOfSync=0 within 120 s; last: {poller.readings[-3:]}"
    # The gate's own lines from the restarted process: UNCONFIRMED before CONFIRMED (and the hold, if it led a role).
    log = boot_log if boot_log is not None else cluster.logs_since_last_boot(wiped_svc)
    u, c = log.find(_UNCONFIRMED_LINE), log.find(_CONFIRMED_LINE)
    assert u >= 0, f"{wiped_svc}'s restarted process never logged '{_UNCONFIRMED_LINE}':\n{log[-2500:]}"
    assert c > u, f"{wiped_svc} logged CONFIRMED before UNCONFIRMED (or never confirmed):\n{log[-2500:]}"
    if expect_role_hold:
        assert _HOLD_LINE in log, (
            f"{wiped_svc} led a role at the kill but its restarted process never held it ('{_HOLD_LINE}'):\n{log[-2500:]}")
    # Every node's controller view has no pending storage left.
    deadline = time.time() + 60
    pending = {}
    while time.time() < deadline:
        pending = {svc: (cluster.read_storage(svc) or {}).get("pending_nodes", -1) for svc in SERVICES}
        if all(v == 0 for v in pending.values()):
            break
        time.sleep(0.5)
    assert all(v == 0 for v in pending.values()), f"pendingNodes != 0 at the end: {pending}"
    # With the gate working the truncation guard must NEVER fire: a refusal here is a gate bug the guard masked.
    for svc in SERVICES:
        logs = cluster.compose.logs(svc, timeout=60)
        assert _COMMITTED_BELOW not in logs, f"{svc} logged '{_COMMITTED_BELOW}' - a follower truncated committed data"
        assert _GUARD_LINE not in logs, f"{svc}: the truncation guard fired ('{_GUARD_LINE}') - the gate let a wiped leader lead"
        reading = cluster.read_storage(svc)
        assert reading is not None and reading["lost_committed_reports"] <= 0, (
            f"{svc}: lostCommittedReports={None if reading is None else reading['lost_committed_reports']} (a below-committed report was made)")
    # Committed set intact: ordering, exactly-once, no loss.
    sub = _subscribe_all(cluster, subscribe_svc, _next_cid_base(), keys, window_ms=90_000)
    assert_no_committed_loss(Results(clients=[c for r in producers for c in r.clients] + sub.clients))
    assert_per_key_ordering(sub)
    assert_exactly_once(sub)
    assert_single_leader(cluster)
    # The catalog history is identical through every node (polled: a follower's catalog may lag a moment).
    digests = _await_equal_catalog_digests(cluster)
    assert len(set(digests.values())) == 1, f"catalog history differs between nodes after 60 s: {digests}"
    # A post-recovery round trip.
    _, leader_svc = cluster.identify_leader()
    _assert_cluster_healthy(cluster, leader_svc, keys)


@pytest.mark.fault
@pytest.mark.timeout(1200)
@pytest.mark.parametrize("fmt", ["auto", "join"])
@pytest.mark.parametrize("target_role", ["partition_leader", "catalog_leader", "controller"])
def test_dr14_disk_loss_fast_restart_never_truncates_committed(tmp_path, target_role, fmt):
    """SIGKILL a node, wipe its data volume and restart it at once - while rate-limited acks=All producers run and
    after 3 DDLs - for the node that leads a user partition, leads __catalog, or is the controller.

    The wiped node must come back UNCONFIRMED (and, if it led a role, hold it), be stripped from every ISR, confirmed
    and rejoin; the truncation guard must never fire (the gate keeps the wiped node from leading), no follower may
    truncate below its committed offset, and no acked record is lost - including records acked AFTER the restart.
    ``fmt``: ``auto`` (compose RUNE_STORAGE_FORMAT=auto formats the empty dir as a joiner) or ``join`` (the operator's
    `rune_broker storage format --join`).  The controller's broker-liveness timeout is raised to 60 s
    (RUNE_BROKER_LIVENESS_TIMEOUT_MS) so the wiped node deterministically boots, replicates the metadata and finds itself
    still named leader, instead of racing the failover.  Fixes: ISR v3 Steps 7-11.
    """
    from harness.cluster import Cluster
    override = tmp_path / "liveness.yml"
    override.write_text("services:\n" + "".join(
        f"  {s}:\n    environment:\n      RUNE_BROKER_LIVENESS_TIMEOUT_MS: \"60000\"\n" for s in SERVICES))
    cluster = Cluster(project="rune-dr-diskloss", override_files=[str(override)])
    keys = 4
    try:
        cluster.up(leader_timeout=120)
        cluster.wait_data_plane_ready(max_attempts=5, timeout_s=60)
        assert_single_leader(cluster)

        # Committed catalog history: 3 DDLs.
        for i in range(3):
            res = cluster.ql(f"topic dr14_disk_loss_{i} partitions 1;", timeout=90)
            assert res.ok, f"DDL {i} failed:\n{res.stdout}{res.stderr}"

        target_svc = _pick_target(cluster, target_role)
        led_roles = [svc for topic in (_TOPIC_MULTI, _TOPIC_CATALOG) for svc in cluster.partition_leaders(topic).values()]
        expect_role_hold = target_svc in led_roles    # the committed map names it leader at the kill
        survivors = [s for s in SERVICES if s != target_svc]

        # Rate-limited producers (they may bail on the failover - acceptable; only ACKED records are audited).
        holder: dict[str, Results] = {}

        def _run_producers():
            holder["r"] = _produce(cluster, survivors[0], 1, _next_cid_base(), 1200, keys, rate_hz=40)

        producer_thread = threading.Thread(target=_run_producers, daemon=True)
        producer_thread.start()
        time.sleep(3.0)    # let production get going

        faults.kill_node(cluster, target_svc, signal.SIGKILL)
        _wipe_volume(cluster, target_svc)
        if fmt == "join":
            faults.format_storage(cluster, target_svc, "join")
        with _StoragePoller(cluster, target_svc) as poller:
            restarted_at = time.time()
            faults.restart(cluster, target_svc)      # no pause: the committed assignment still names it
            # Records acked AFTER the restart call: a second producer on a survivor (the first may have bailed on the
            # failover; that is observed, never assumed).
            after = _produce(cluster, survivors[1], 1, _next_cid_base(), 150, keys, rate_hz=15)
            assert after.ok and len(after.all_produced) >= 100, (
                f"fewer than 100 records acked after the restart: {len(after.all_produced)}\n"
                + "\n".join(c.raw[-800:] for c in after.clients))
            producer_thread.join(timeout=300)
            produced = holder.get("r", Results())
            _assert_recovered_after_disk_loss(
                cluster, target_svc, poller, [produced, after], survivors[1], keys,
                expect_role_hold=expect_role_hold, restarted_at=restarted_at)
    finally:
        cluster.down()


@pytest.mark.fault
@pytest.mark.timeout(900)
def test_dr14_wiped_node_without_format_refuses_to_start(tmp_path):
    """Production formatting: with RUNE_STORAGE_FORMAT unset a wiped node refuses to start (exit 78) and says which
    command formats it; after `rune_broker storage format --join` it rejoins as a replaced disk and recovers as in
    the fast-restart cases.  The override unsets the variable for rune-2 only, AFTER the cluster formed (a fresh
    volume needs the compose default `auto` to form in the first place)."""
    from harness.cluster import Cluster
    c = Cluster(project="rune-dr-noformat")
    keys = 4
    try:
        c.up(leader_timeout=120)
        c.wait_data_plane_ready(max_attempts=5, timeout_s=60)
        assert_single_leader(c)
        pre = _produce(c, "rune-1", 1, _next_cid_base(), 120, keys)
        assert pre.ok and len(pre.all_produced) == 120, "pre-wipe produce failed"
        override = tmp_path / "no-format-rune-2.yml"
        override.write_text("services:\n  rune-2:\n    environment:\n      RUNE_STORAGE_FORMAT: \"\"\n")
        c.compose._override_files.append(str(override))

        faults.kill_node(c, "rune-2", signal.SIGKILL)
        _wipe_volume(c, "rune-2")
        c.compose.up("rune-2")                       # recreates the container with the unset format variable

        # Within 30 s: stopped with exit code 78 and the log names the command to run.
        name = c.container_name("rune-2")
        deadline = time.time() + 30
        code = None
        while time.time() < deadline:
            res = run(["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", name], timeout=15)
            status, _, exit_code = (res.stdout or "").strip().partition(" ")
            if status in ("exited", "dead"):
                code = int(exit_code)
                break
            time.sleep(0.5)
        assert code == 78, f"rune-2 did not refuse to start with exit 78 within 30 s (exit code {code})"
        logs = run(["docker", "logs", name], timeout=30)
        text = (logs.stdout or "") + (logs.stderr or "")
        assert "rune_broker storage format --join" in text, "the refusal does not name `rune_broker storage format --join`"
        assert BOOT_LINE not in text, "the process logged its storage boot line: it did not refuse before touching the dir"

        # The operator formats the disk as a replacement and starts the node again.
        faults.format_storage(c, "rune-2", "join")
        with _StoragePoller(c, "rune-2") as poller:
            restarted_at = time.time()
            c.compose.up("rune-2")
            # The container was recreated by the first `up`: its log holds only the refused start and this one; the
            # last boot line separates them.
            _assert_recovered_after_disk_loss(c, "rune-2", poller, [pre], "rune-1", keys,
                                              expect_role_hold=False, restarted_at=restarted_at)
    finally:
        c.down()


def test_dr14_storage_poller_orders_states_without_a_cluster():
    """No cluster needed: the poller keeps readings in order and reports the first index of a state / confirmation; the
    boot-log slicer separates the restarted process from the one it replaced (RED: slice at the FIRST boot line and the
    old process's CONFIRMED precedes the new process's UNCONFIRMED)."""
    class _Fake:
        def __init__(self):
            self.seq = [None,
                        {"state": "UNCONFIRMED", "held_leader_roles": 1, "out_of_sync": 2, "pending_nodes": 1},
                        {"state": "UNCONFIRMED", "held_leader_roles": 1, "out_of_sync": 1, "pending_nodes": 1},
                        {"state": "CONFIRMED", "held_leader_roles": 0, "out_of_sync": 0, "pending_nodes": 0}]
            self.i = 0

        def read_storage(self, svc):
            r = self.seq[min(self.i, len(self.seq) - 1)]
            self.i += 1
            return r

    with _StoragePoller(_Fake(), "rune-1") as p:
        final = p.wait_confirmed(10)
    assert final is not None and final["state"] == "CONFIRMED"
    assert 0 <= p.first_index("UNCONFIRMED") < p.first_index("CONFIRMED")
    assert p.first_index("UNDECIDED") == -1

    two_boots = (f"{BOOT_LINE}/d storageId=1 state=CONFIRMED\n{_CONFIRMED_LINE}1)\n"
                 f"{BOOT_LINE}/d storageId=2 state=CONFIRMED\n{_UNCONFIRMED_LINE}2)\n{_CONFIRMED_LINE}2)\n")
    last = slice_since_last_boot(two_boots)
    assert last.find(_UNCONFIRMED_LINE) >= 0 and last.find(_UNCONFIRMED_LINE) < last.find(_CONFIRMED_LINE)
    assert two_boots.find(_CONFIRMED_LINE) < two_boots.find(_UNCONFIRMED_LINE)   # the unsliced log would mislead
    assert slice_since_last_boot("no boot line here") == ""
    digest_a = _history_rows_digest("offset  time\n  12  t  define\n  15  t  alter\nfooter 1\n")
    digest_b = _history_rows_digest("HEADER\n  12  t  define\n  15  t  alter\nother footer\n")
    assert digest_a == digest_b != _history_rows_digest("  12  t  define\n")
