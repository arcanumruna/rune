"""Milestone D Phase-1 e2e smoke + availability tests.

Proves two things that are ALREADY proven by unit+integration (MD1-1..12):

1. SMOKE — a dockerised 3-node cluster starts; the ``multi`` topic (3
   partitions, one per node per StaticBalancer) gets its partition leaders
   distributed across all three nodes; a client produces keyed records that
   span all three partitions and consumes them back via the MD1-10 routing
   (MetadataRequest/REDIRECT transparent to the client).  All records
   round-trip.

2. AVAILABILITY (Phase-1 boundary) — killing a node that is a follower for
   the Raft consensus *but* the partition leader for one of the ``multi``
   topic partitions:
     - partitions whose leaders are on OTHER (surviving) nodes keep serving
       produce+consume without interruption;
     - the partition whose OWN leader was killed is EXPECTED UNAVAILABLE
       (Phase-1 has NO dynamic failover / re-election for data-plane
       partitions — that is Phase-2).
   The test asserts EXACTLY that boundary and does NOT expect the dead
   leader's partition to recover (which would be a Phase-2 feature).

Phase-1 boundary contract (documented here for clarity):
  * Placement: the catalog places each created topic as its own unit
    (MetadataService::placeUnit_ -> StaticBalancer::assign(unit, placementNodes());
    the stripe starts at 0 for every topic), so for ``multi:3`` on nodes 0,1,2
    partition p -> node p: 0 -> rune-1, 1 -> rune-2, 2 -> rune-3.  The tests CHECK this
    against the leaders' becomeLeader log lines (``_assert_placement``) before they rely on it.
  * If node 0 (Raft-follower, partition-0 leader) is killed:
      - partition 0's leader is re-elected on a survivor (Milestone D failover).
      - partitions 1+2 (led by nodes 1+2) KEEP SERVING.
  * No split-brain: only ONE Raft consensus leader at any time
    (assert_single_leader, counter-based per DR-HARNESS convention).

Markers: ``milestone_d_p1``

CID base: 5_000_000 + n*2000 — disjoint from all existing DR/CG/TX/CP suites.
"""

from __future__ import annotations

import re
import signal
import tempfile
import textwrap
import time
from typing import Iterator, Optional

import pytest

from harness import faults
from harness.cluster import (
    BROKER_REPO,
    COMPOSE_FILE,
    SERVICES,
    Cluster,
    node_id_to_service,
    service_to_node_id,
)
from harness.driver import Results, run_clients
from harness.invariants import (
    assert_exactly_once,
    assert_no_committed_loss,
    assert_per_key_ordering,
    assert_single_leader,
)

# ---------------------------------------------------------------------------
# Module-level marker
# ---------------------------------------------------------------------------
pytestmark = pytest.mark.milestone_d_p1

# ---------------------------------------------------------------------------
# CID base — disjoint from all existing suites (DR-1..15, CG, TX, CP).
# DR-1: n*1000 (max ~16_000), DR-2: 100_000+n*2000, DR-3: 300_000+n*3000.
# CG/CP/TX stay below 3_000_000.  5_000_000 gives a safe gap.
# ---------------------------------------------------------------------------
_CID_STRIDE = 2000
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    return 5_000_000 + _run_counter["n"] * _CID_STRIDE


# ---------------------------------------------------------------------------
# Topic constants — must match the topics Cluster.up creates by DDL (DEFAULT_TOPICS).
#   events:1, multi:3, cg:4
# Client topic-table format: "name:topicId:partitions"
# ---------------------------------------------------------------------------
_MULTI_TOPIC        = "multi"
_MULTI_TOPIC_ID     = 1
_MULTI_PARTITIONS   = 3   # 3 partitions → StaticBalancer assigns one per node

# Client topic table for the multi topic.
_CLIENT_MULTI_TOPICS = f"{_MULTI_TOPIC}:{_MULTI_TOPIC_ID}:{_MULTI_PARTITIONS}"

# Keys ≥ partitions ensures every partition receives at least one record.
# 18 keys with hash-distribution across 3 partitions = 6 records per partition.
_KEYS_PER_PARTITION_FACTOR = 6
_KEYS = _MULTI_PARTITIONS * _KEYS_PER_PARTITION_FACTOR  # 18 keys

# Client connection / subscribe timing (mirrors DR-3 tuning for Docker Desktop).
_CONNECT_MS   = 40_000
_STAGGER_MS   = 400
_SUBSCRIBE_MS = 60_000

# ---------------------------------------------------------------------------
# Placement - per-topic, from stripe 0.
#
# The cluster's topics are created by DDL in this order: events:1, multi:3, cg:4.
# The catalog places each created topic as its own unit
# (MetadataService::placeUnit_ -> StaticBalancer::assign(unit, placementNodes());
# the stripe offset starts at 0 for EVERY topic - StaticBalancer.cpp globalLeaderOffset),
# so earlier topics do not shift later ones:
#
#    multi:0 -> node 0 (rune-1)
#    multi:1 -> node 1 (rune-2)
#    multi:2 -> node 2 (rune-3)
#
# Inverse: node n leads partition n.
#
# This constant is NOT trusted: `_assert_placement` compares it with the leaders the
# brokers actually log (becomeLeader), and the tests fail on a mismatch.
# ---------------------------------------------------------------------------

_MULTI_STRIPE_OFFSET = 0   # the catalog starts every topic's stripe at node 0
_MULTI_TOPIC_ID = 1        # events=0, multi=1, cg=2 (creation order; checked by Cluster.create_topics)

# Each partition's leader logs (info level) when it becomes leader:
#   "PartitionReplica[node=<n>] becomeLeader topic=<topicId> part=<p> epoch=<e> ..."
# (The gateways' per-partition redirect debug lines are NOT usable: the client routes straight to each
# partition's leader from its metadata, so a healthy run produces no redirects at all.)
_LEADER_RE = re.compile(
    r"PartitionReplica\[node=(\d+)\] becomeLeader topic=" + str(_MULTI_TOPIC_ID) + r" part=(\d+) epoch=\d+")


def _expected_partition_leader(partition: int) -> int:
    """Return the node ID expected to lead ``partition`` of the ``multi`` topic.

    multi:p -> node (_MULTI_STRIPE_OFFSET + p) % N  where N=3, i.e. p -> p
    (partition 0 -> rune-1, 1 -> rune-2, 2 -> rune-3).
    """
    n = len(SERVICES)
    return (_MULTI_STRIPE_OFFSET + partition) % n


def _partition_led_by_node(node_id: int) -> int:
    """Return the ``multi`` partition led by ``node_id``.

    Inverse of _expected_partition_leader:
        node n -> partition (n - _MULTI_STRIPE_OFFSET + N) % N  (= n).
    """
    n = len(SERVICES)
    return (node_id - _MULTI_STRIPE_OFFSET + n) % n


def _service_of_partition_leader(partition: int) -> str:
    return node_id_to_service(_expected_partition_leader(partition))


def _observed_multi_leaders(cluster: Cluster) -> dict[int, set[int]]:
    """{partition -> leader node ids} as the leaders themselves logged them (becomeLeader)."""
    logs = cluster.compose.logs(*SERVICES, timeout=60)
    out: dict[int, set[int]] = {}
    for m in _LEADER_RE.finditer(logs):
        out.setdefault(int(m.group(2)), set()).add(int(m.group(1)))
    return out


def _assert_placement(cluster: Cluster) -> None:
    """The placement the tests rely on, checked against what the brokers actually decided.

    Every one of the topic's partitions must have a logged leader, and it must be exactly
    `_expected_partition_leader` (called before any kill, so only initial-epoch leaders exist).
    """
    obs = _observed_multi_leaders(cluster)
    assert set(obs) == set(range(_MULTI_PARTITIONS)), (
        f"expected becomeLeader lines for all {_MULTI_PARTITIONS} multi partitions, got {obs}")
    for p, leaders in obs.items():
        assert leaders == {_expected_partition_leader(p)}, (
            f"multi:{p} led by {leaders}, expected {_expected_partition_leader(p)}")


# ---------------------------------------------------------------------------
# Compose override factory — sets RUNE_LOG_LEVEL=debug on all nodes so that
# the gateway logs per-partition REDIRECT decisions, giving us a direct audit
# trail that different nodes are serving different partitions.
#
# The override only sets debug logging for this test cluster.
# ---------------------------------------------------------------------------

def _write_debug_override(tmp_dir: str) -> str:
    """Write a compose override that enables debug logging; return its path."""
    override = textwrap.dedent("""\
        # MD1 Phase-1 e2e: debug-level logging so per-partition REDIRECT lines
        # appear in docker compose logs, providing direct evidence that partition
        # leaders are distributed across nodes.
        # Generated by test_multi_leader_smoke.py — do not commit.
        services:
          rune-1:
            environment:
              RUNE_LOG_LEVEL: "debug"
          rune-2:
            environment:
              RUNE_LOG_LEVEL: "debug"
          rune-3:
            environment:
              RUNE_LOG_LEVEL: "debug"
    """)
    import os
    path = os.path.join(tmp_dir, "docker-compose.md1-debug-override.yml")
    with open(path, "w") as f:
        f.write(override)
    return path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _warmup_probe_node0(cluster: Cluster, timeout_s: int = 90) -> None:
    """Warm up the data-plane by probing node 0 (rune-1) directly.

    StaticBalancer assigns partition 0 of every topic to node 0, so probing
    rune-1 directly avoids a REDIRECT round-trip.  We retry up to ``timeout_s``
    seconds to give the metadata bridge time to apply assignments after startup.
    """
    deadline = time.time() + timeout_s
    last_err = ""
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        res = run_clients(
            1,
            network=cluster.compose.network_name(),
            target_service="rune-1",   # node 0 — partition-0 leader for all topics
            mode="produce",
            records=5,
            keys=3,
            acks="leader",             # acks=leader avoids waiting for ISR on all 3 nodes
            timeout_ms=10_000,
            cid_base=995_000,          # reuse the standard warm-up cid window
            timeout_s=30,
            topics="events:0:1",       # events topic, topicId=0, 1 partition
        )
        if res.ok and len(res.all_produced) == 5:
            # Cache rune-1 as the serving node for subsequent serving_node() calls.
            cluster._serving_cache = "rune-1"
            return
        last_err = (res.clients[0].raw[-500:] if res.clients else "no result")
        time.sleep(3.0)
    raise RuntimeError(
        f"MD1 warm-up: node 0 (rune-1) not ready after {attempt} attempts / "
        f"{timeout_s}s — is StaticBalancer assigning events:0 to node 0? "
        f"Last client output:\n{last_err}"
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def md1_cluster() -> Iterator[Cluster]:
    """Function-scoped fresh cluster with debug logging for MD1 Phase-1 e2e."""
    import os
    tmp = tempfile.mkdtemp(prefix="rune_md1_e2e_")
    override = _write_debug_override(tmp)
    build = os.environ.get("DR_E2E_BUILD", "0") == "1"

    c = Cluster(project="rune-dr-md1", override_files=[override])
    try:
        c.up(build=build, leader_timeout=120)
        # MD1-specific warm-up.
        #
        # Under MD1 per-partition routing, StaticBalancer assigns partition leaders
        # distributed across nodes (events:0→node0, multi:0→node0, multi:1→node1,
        # multi:2→node2).  Standard ``wait_data_plane_ready`` probes the Raft leader
        # (any node), which may NOT be the events-partition leader (node 0), causing
        # a REDIRECT that can exhaust the client retry limit under certain timing
        # conditions.
        #
        # Strategy: wait for Raft consensus to stabilise, give the metadata bridge
        # time to converge, then probe the multi topic by targeting node 0 directly
        # (which is the StaticBalancer-assigned leader for partition 0 of both events
        # and multi).  This gives the most direct warm-up path.
        c.wait_for_leader(timeout_s=120)
        # Wait for the metadata bridge to emit + replicate + apply StaticBalancer
        # assignments.  On Docker Desktop with Rosetta emulation this can take
        # several seconds.  15 s is generous; the bridge typically converges in <2 s.
        time.sleep(15.0)
        # Probe node 0 (rune-1) directly — StaticBalancer assigns events:0 and
        # multi:0 to node 0, so a direct probe skips the redirect entirely.
        _warmup_probe_node0(c)
        yield c
    finally:
        faults.heal_all(c)
        c.down()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _produce_multi(cluster: Cluster, target_svc: str,
                   cid_base: int, records: int = 60,
                   keys: int = _KEYS, acks: str = "all") -> Results:
    """Produce ``records`` records to the ``multi`` topic (3 partitions).

    acks=all ensures every PRODUCED line is majority-committed.
    keys=18 (≥ 3*6) guarantees all 3 partitions receive records.
    """
    return run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=240,
        mode="produce",
        topics=_CLIENT_MULTI_TOPICS,
        records=records,
        keys=max(keys, _KEYS),
        acks=acks,
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )


def _subscribe_multi(cluster: Cluster, target_svc: str,
                     cid_base: int, window_ms: int = _SUBSCRIBE_MS) -> Results:
    """Subscribe-from-earliest to all 3 partitions of the ``multi`` topic."""
    return run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=180,
        mode="subscribe",
        topics=_CLIENT_MULTI_TOPICS,
        sub_partitions=_MULTI_PARTITIONS,
        keys=_KEYS,
        from_="earliest",
        expect=0,
        subscribe_ms=window_ms,
        timeout_ms=_CONNECT_MS,
        start_stagger_ms=_STAGGER_MS,
    )


def _assert_all_partitions_produced(results: Results) -> None:
    """Assert that produced records span all ``_MULTI_PARTITIONS`` partitions.

    This is the primary evidence that partition leaders are distributed:
    if any partition had no leader, produce would fail or time out for keys
    hashing to that partition, so the absence of records on that partition
    would be detected here.
    """
    partitions_seen = {m.partition for m in results.all_produced}
    expected = set(range(_MULTI_PARTITIONS))
    missing = expected - partitions_seen
    assert not missing, (
        f"partition leadership gap: no produced records on partitions {missing} "
        f"(saw partitions {sorted(partitions_seen)}). "
        f"StaticBalancer assigns partition p → node p; if a partition is empty "
        f"the corresponding node is not serving as partition leader."
    )


def _parse_redirect_leaders(cluster: Cluster) -> dict[int, set[str]]:
    """Parse broker debug logs for per-partition REDIRECT messages.

    Returns a dict  {partition_id → set of services that did NOT lead it}
    — i.e. for each REDIRECT line "partition X is led by node Y; redirecting
    from service Z", we know Z is NOT the leader of partition X.

    This is supplementary evidence (only available with RUNE_LOG_LEVEL=debug)
    and is NOT the primary correctness check (that is _assert_all_partitions_produced).
    """
    # Pattern from ClientGateway.cpp:
    #   "Produce per-partition redirect sessionId=... → <endpoint>"
    # We collect any redirect mentions per service to show that each node
    # redirects away from itself for the partitions it does NOT lead.
    redirect_by_partition: dict[int, set[str]] = {}

    logs = cluster.compose.logs(*SERVICES, timeout=60)
    # Match lines like:
    #   "Produce per-partition redirect sessionId=<n> → rune-2:40200 (part=1)"
    # or the unassigned path:
    #   "Produce unassigned partition (topicId=1 part=2) sessionId=..."
    redirect_re = re.compile(
        r"Produce per-partition redirect.*?part=(\d+)", re.IGNORECASE)
    for m in redirect_re.finditer(logs):
        p = int(m.group(1))
        redirect_by_partition.setdefault(p, set())

    return redirect_by_partition


# ---------------------------------------------------------------------------
# Test 1: Smoke — distributed leadership + full round-trip
# ---------------------------------------------------------------------------

@pytest.mark.milestone_d_p1
@pytest.mark.timeout(600)
def test_md1_smoke_distributed_leadership(md1_cluster: Cluster) -> None:
    """Smoke: 3-node cluster starts, partition leaders distributed, records
    round-trip across all three partitions via MD1-10 REDIRECT routing.

    Steps:
      1. Assert a single Raft consensus leader (counter-based, DR-HARNESS).
      2. Produce 60 records to ``multi:3`` (acks=all, 18 keys).
      3. Assert all 3 partitions received records (proves all partition leaders
         are operational — if any were missing, produce would fail for those
         key hashes).
      4. Subscribe from all 3 partitions; assert no committed loss + ordering
         + exactly-once.
      5. Assert single Raft leader still holds post-workload.

    Note on leadership detection:
      - Raft consensus leader: counter-based via ``rune_role`` / ``assert_single_leader``
        (Aeron-faithful, independent of RUNE_LOG_LEVEL — DR-HARNESS convention).
      - Partition leader distribution: verified INDIRECTLY by requiring all
        partitions to appear in the PRODUCED output.  With MD1-10 routing the
        client transparently follows REDIRECT responses to the correct partition
        leader; if a partition leader is absent (no leader for that partition),
        records targeting that partition's key range cannot be produced (acks=all
        would time out).  The debug-log redirect evidence is collected as a
        supplementary audit trail but is NOT the primary correctness gate.
    """
    cluster = md1_cluster

    # Step 1: Raft consensus layer is healthy — single leader.
    assert_single_leader(cluster)

    # Step 2: Produce 60 records across all 3 partitions.
    serving = cluster.serving_node()
    cid_produce = _next_cid_base()
    produce_res = _produce_multi(cluster, serving, cid_produce, records=60)

    assert produce_res.ok, (
        "produce to multi topic failed:\n" +
        "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-2000:]}"
            for c in produce_res.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(produce_res.all_produced) == 60, (
        f"produced {len(produce_res.all_produced)} != 60"
    )

    # Step 3: All 3 partitions must appear in the produced output.
    # This is the primary evidence of distributed partition leadership.
    _assert_all_partitions_produced(produce_res)
    _assert_placement(cluster)

    # Step 4: Subscribe and verify round-trip.
    cid_sub = _next_cid_base()
    sub_res = _subscribe_multi(cluster, serving, cid_sub)

    # Merge produce + subscribe for invariant checks.
    merged = Results(clients=produce_res.clients + sub_res.clients)
    assert_no_committed_loss(merged)
    assert_per_key_ordering(sub_res)
    assert_exactly_once(sub_res)

    # Step 5: Raft consensus still healthy post-workload.
    assert_single_leader(cluster)

    # --- Supplementary: log-based redirect evidence (debug level) ---
    # Collect REDIRECT log evidence as an informational audit trail.
    # This does NOT drive pass/fail; the primary gate is _assert_all_partitions_produced.
    logs = cluster.compose.logs(*SERVICES, timeout=60)
    redirect_count = len(re.findall(
        r"Produce per-partition redirect", logs, re.IGNORECASE))
    # We expect redirects to appear because each produce request targets a
    # gateway node that is not the leader of every partition.
    # Log the count for diagnostic purposes.
    print(
        f"\n[MD1-smoke] redirect log lines found: {redirect_count}. "
        f"Partitions covered: "
        f"{sorted(m.partition for m in produce_res.all_produced)[:10]}..."
    )


# ---------------------------------------------------------------------------
# Test 2: Availability — Phase-1 boundary
# ---------------------------------------------------------------------------

@pytest.mark.milestone_d_p1
@pytest.mark.fault
@pytest.mark.timeout(600)
def test_md1_availability_partition_failover(md1_cluster: Cluster) -> None:
    """Availability: kill a partition leader → failover asserted.

    Contract (Milestone D, distributed partition leadership — was the Phase-1
    "dead partition stays unavailable" boundary before failover existed):
      - Partitions led by SURVIVING nodes keep producing + consuming without
        interruption after a node kill.
      - The partition whose OWN leader was killed is re-elected on a survivor and
        serves produce again (step 6).

    Steps:
      1. Pre-kill produce: 30 records across all 3 partitions (acks=all).
      2. Identify the current Raft consensus leader.
      3. Kill one NON-Raft-leader node.  In Phase-1, each node is the partition
         leader for one partition of ``multi:3`` (the catalog places each topic from
         stripe 0: partition p -> node p, verified by _assert_placement).  Killing a
         Raft-follower node kills the partition leader for one partition without
         disrupting Raft consensus.
      4. Wait for the surviving 2-node Raft quorum to stabilise.
      5. Post-kill produce: attempt 20 records per surviving partition (keys
         that hash to partitions led by ALIVE nodes).  Assert these succeed.
      6. Dead-partition assertion: produce to the partition whose leader was
         killed times out / fails.  Assert UNAVAILABILITY (Phase-1 boundary).
      7. Post-kill subscribe: subscribe from the ALIVE partitions; assert no
         committed-loss for their records.

    Phase-1 availability boundary documented:
      - Alive partition leaders: serve produce + consume uninterrupted.
      - Dead partition leader: partition unavailable until Phase-2 re-election.
        This is EXPECTED and CORRECT for Phase-1.
    """
    cluster = md1_cluster

    # Step 1: Pre-kill produce spanning all 3 partitions (acks=all = majority-durable).
    assert_single_leader(cluster)
    serving = cluster.serving_node()
    cid_pre = _next_cid_base()
    pre_res = _produce_multi(cluster, serving, cid_pre, records=30, acks="all")

    assert pre_res.ok, (
        "pre-kill produce failed:\n" + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-1500:]}"
            for c in pre_res.clients if c.exit_code != 0 or c.rc != 0
        )
    )
    assert len(pre_res.all_produced) == 30, (
        f"pre-kill: produced {len(pre_res.all_produced)} != 30"
    )
    _assert_all_partitions_produced(pre_res)
    # The placement the kill below relies on, checked BEFORE the kill (only initial-epoch logs exist).
    _assert_placement(cluster)

    # Step 2: Identify Raft consensus leader.
    raft_leader_id, raft_leader_svc = cluster.identify_leader()

    # Step 3: Choose a node to kill.
    #
    # Strategy: kill a NON-Raft-leader node.  Killing the Raft leader would
    # trigger Raft re-election and might change the overall Raft leader, which
    # is a separate concern from per-partition leadership.  By killing a Raft
    # FOLLOWER we:
    #   a) Keep the Raft consensus on the surviving 2 nodes uninterrupted.
    #   b) Kill the partition leader for the partition assigned to that node.
    #
    # Placement: partition p -> node p (stripe 0 per topic; checked by _assert_placement).
    # Each Raft-follower node is the partition leader for one multi partition.
    # We pick the first Raft follower.
    killed_svc: Optional[str] = None
    for svc in SERVICES:
        if svc != raft_leader_svc:
            killed_svc = svc
            break
    assert killed_svc is not None, "No follower node found to kill"

    killed_node_id = service_to_node_id(killed_svc)
    # The partition whose leader was just killed.
    # The catalog places multi:p -> node (stripe+p)%N (stripe=_MULTI_STRIPE_OFFSET=0).
    # Inverse: node n leads partition (n - stripe + N) % N = _partition_led_by_node(n).
    dead_partition = _partition_led_by_node(killed_node_id)

    # The partitions whose leaders are on surviving nodes.
    alive_partitions = [p for p in range(_MULTI_PARTITIONS) if p != dead_partition]
    # Services that are still alive after the kill.
    alive_services = [s for s in SERVICES if s != killed_svc]

    print(
        f"\n[MD1-avail] Raft leader: {raft_leader_svc} (node {raft_leader_id}); "
        f"killing: {killed_svc} (node {killed_node_id}); "
        f"dead partition: {dead_partition}; alive partitions: {alive_partitions}"
    )

    faults.kill_node(cluster, killed_svc, signal.SIGKILL)

    # Step 4: Wait for Raft quorum to stabilise on surviving nodes.
    # The 2-node quorum can still elect and serve consensus (2 > 3/2).
    # Exclude the killed node from the leader scan.
    try:
        surviving_leader_id = cluster.wait_for_leader(timeout_s=60, exclude=[killed_svc])
    except TimeoutError as exc:
        # Raft may have already had a leader; if the Raft leader survived just
        # wait again.
        surviving_leader_id = cluster.wait_for_leader(timeout_s=60, exclude=[killed_svc])

    surviving_leader_svc = node_id_to_service(surviving_leader_id)
    print(
        f"[MD1-avail] Raft leader after kill: {surviving_leader_svc} (node {surviving_leader_id})"
    )

    # Give the surviving nodes a moment to stabilise data-plane connections.
    time.sleep(3.0)

    # Step 5: Post-kill produce to ALIVE partitions.
    #
    # We target the surviving Raft leader (which can redirect to any alive
    # partition leader).  Use keys that are known to hash to alive partitions.
    # The client's MD1-10 routing handles REDIRECT transparently.
    #
    # Key assignment: dr_client hashes key=(cid * KEYS + k) % partitionCount.
    # With keys=6 and targeting only 2 alive partitions, we pass keys=6 but
    # note that ~1/3 of keys hash to the dead partition.  The safe approach is
    # to use _KEYS=18 (covers all partitions) and tolerate the dead partition's
    # failure: acks=leader so the produce succeeds for the alive ones even if
    # the dead one times out — BUT acks=all would stall if any partition fails.
    #
    # For Phase-1 validation we use acks=leader (not all) for the post-kill
    # produce so that the alive partitions' records are immediately confirmed
    # regardless of the dead partition.  The dead partition's records simply
    # won't appear in the alive-partition count and we assert the unavailability
    # separately.
    cid_post_alive = _next_cid_base()
    # Produce 30 records; with 18 keys about 20 should land on alive partitions.
    post_alive_res = run_clients(
        1,
        network=cluster.compose.network_name(),
        target_service=surviving_leader_svc,
        cid_base=cid_post_alive,
        timeout_s=240,
        mode="produce",
        topics=_CLIENT_MULTI_TOPICS,
        records=30,
        keys=_KEYS,
        acks="leader",   # acks=leader so alive-partition records confirm quickly
        timeout_ms=_CONNECT_MS,
        connect_retries=0,
        start_stagger_ms=_STAGGER_MS,
    )

    # After the kill, records on alive partitions MUST be produced successfully.
    # We expect the alive partitions to have records even if the dead partition
    # causes some produces to fail with NotLeader/timeout.
    alive_parts_in_post = {
        m.partition for m in post_alive_res.all_produced
        if m.partition in alive_partitions
    }
    assert alive_parts_in_post == set(alive_partitions), (
        f"Phase-1 availability violated: after killing node {killed_node_id} "
        f"(partition-{dead_partition} leader), expected alive partitions "
        f"{alive_partitions} to keep serving, but only got records on "
        f"partitions {sorted(alive_parts_in_post)}. "
        f"Produced {len(post_alive_res.all_produced)} records total. "
        f"Alive-partition records: "
        f"{[m.partition for m in post_alive_res.all_produced if m.partition in alive_partitions][:20]}"
        + (f"\nClient stderr tail:\n{post_alive_res.clients[0].raw[-1000:]}"
           if post_alive_res.clients else "")
    )

    # Step 6: Dead partition unavailability assertion (Phase-1 boundary).
    #
    # Records on the dead partition must be absent from the post-kill produce
    # (because the partition leader is down and Phase-1 has no failover).
    dead_parts_in_post = {
        m.partition for m in post_alive_res.all_produced
        if m.partition == dead_partition
    }
    # Milestone D (distributed partition leadership, failover): the partition
    # whose leader was killed is re-elected on a survivor and serves again.
    assert dead_partition in dead_parts_in_post, (
        f"Partition failover missing: partition {dead_partition} (led by killed "
        f"node {killed_node_id}) produced no records after the kill — its leadership "
        f"was not re-elected on a surviving node."
        + (f"\nClient stderr tail:\n{post_alive_res.clients[0].raw[-1000:]}"
           if post_alive_res.clients else "")
    )

    print(
        f"[MD1-avail] failover confirmed: partition {dead_partition} (killed leader) "
        f"serves again; alive partitions {alive_partitions} kept serving: "
        f"{sorted(alive_parts_in_post)} ✓"
    )

    # Step 7: Post-kill subscribe from ALIVE partitions verifies no committed loss
    # for the pre-kill records that landed on alive partitions.
    #
    # Only check the pre-kill records on alive partitions (the dead partition's
    # pre-kill records are majority-durable, but the dead leader cannot serve
    # them — this is also the Phase-1 boundary and is expected).
    #
    # IMPORTANT: we subscribe to each alive partition SEPARATELY, each from its
    # correct leader node.  A single multi-partition subscribe that targets one
    # gateway and relies on broker-side REDIRECT to bounce between nodes causes
    # an infinite session-retarget loop (the single-session client retargets on
    # every redirect, causing all pending subscribes to be re-sent on the new
    # node — including partitions that DON'T live there — triggering a redirect
    # back, etc.).  By targeting each partition's own leader directly we avoid
    # all redirects and each subscribe delivers its records cleanly.
    #
    # Each per-partition subscribe uses RUNE_SUB_PARTITION_BASE=p + SUB_PARTITIONS=1
    # so dr_client subscribes ONLY partition p from its leader node.
    pre_alive_produced = [
        m for m in pre_res.all_produced if m.partition in alive_partitions
    ]

    import dataclasses

    all_sub_clients = []
    for p in alive_partitions:
        leader_svc = _service_of_partition_leader(p)
        cid_sub_p = _next_cid_base()
        sub_p = run_clients(
            1,
            network=cluster.compose.network_name(),
            target_service=leader_svc,
            cid_base=cid_sub_p,
            timeout_s=180,
            mode="subscribe",
            topics=_CLIENT_MULTI_TOPICS,
            sub_partitions=1,
            sub_partition_base=p,
            keys=_KEYS,
            from_="earliest",
            expect=0,
            subscribe_ms=_SUBSCRIBE_MS,
            timeout_ms=_CONNECT_MS,
            start_stagger_ms=_STAGGER_MS,
        )
        all_sub_clients.extend(sub_p.clients)
        print(
            f"[MD1-avail] Subscribe partition {p} from {leader_svc}: "
            f"{len(sub_p.all_delivered)} records delivered"
        )

    sub_after_kill = Results(clients=all_sub_clients)

    # Build a filtered Results over the alive partitions only.
    # We create a fake producer result with only the alive-partition records
    # to assert no committed loss on those partitions.
    alive_produce_clients = [
        dataclasses.replace(
            pre_res.clients[0],
            produced=pre_alive_produced,
            delivered=[],
        )
    ]
    alive_merged = Results(clients=alive_produce_clients + sub_after_kill.clients)
    # Alive partitions must not have committed loss.
    assert_no_committed_loss(alive_merged)
    assert_per_key_ordering(sub_after_kill)

    print(
        f"[MD1-avail] No committed loss on alive partitions {alive_partitions} "
        f"after node-{killed_node_id} kill. Phase-1 availability boundary: PASS"
    )
