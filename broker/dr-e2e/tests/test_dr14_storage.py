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

import signal
import time

import pytest

from harness import faults
from harness.dockerutil import run
from harness.cluster import SERVICES, node_id_to_service
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
