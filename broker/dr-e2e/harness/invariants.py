"""Invariant assertions over driver Results + cluster state.

Each assertion raises AssertionError with the offending records / nodes on
failure, and is a no-op on success.  These are the correctness contracts the
DR matrix checks after every fault scenario.

Terminology:
  * "produced" = records the producer got a Leader ack for (durable/committed).
  * "delivered" = records a subscriber actually received.
  * A record is identified across produce/subscribe by (cid, key, seq): the
    dr_client encodes that triple into the record VALUE payload.

CG-6 group-consumer invariants:
  * "single_owner" — each partition is consumed by at most ONE member per
    generation (no simultaneous delivery to two members).
  * "group_no_committed_loss" — the UNION of all group members' records covers
    every produced record (no committed record missed by the group).
  * "bounded_duplicates" — duplicates only within the uncommitted window; after
    a rebalance / failover, redelivery starts at the last committed offset, not
    from 0 (i.e. duplicated offsets must all be >= the member's own min-offset,
    not far below the committed frontier).
  * "offset_resume" — after coordinator failover, members resume from the
    committed offset (not 0, not skipping ahead).
"""

from __future__ import annotations
import time
from collections import defaultdict
from typing import TYPE_CHECKING

from .driver import GroupResults, Msg, Results
from .timing import timed

if TYPE_CHECKING:
    from .driver import TxnResult


def _rid(m: Msg) -> tuple[int, int, int]:
    return (m.cid, m.key, m.seq)


def assert_no_committed_loss(results: Results) -> None:
    """Every acked (committed) record must be delivered to at least one
    subscriber (the union of all subscriber views covers the committed set).

    Matching is by (cid, key, seq) identity encoded in the record value
    payload — partition-agnostic and correct across multi-partition topics
    where the same log offset can exist in multiple partitions.
    """
    produced_ids: set[tuple[int, int, int]] = {_rid(m) for m in results.all_produced
                                               if m.cid >= 0}
    delivered_ids: set[tuple[int, int, int]] = {_rid(m) for m in results.all_delivered
                                                if m.cid >= 0}
    missing = produced_ids - delivered_ids
    assert not missing, (
        f"committed-loss: {len(missing)} acked records never delivered "
        f"(by cid,key,seq): {sorted(missing)[:20]}{' ...' if len(missing) > 20 else ''}"
    )


def assert_per_key_ordering(results: Results) -> None:
    """For each (subscribing client, source producer key), delivered records
    must arrive in strictly increasing seq order (no per-key reordering).

    Ordering is a PER-SUBSCRIBER property: every subscriber reads the shared
    partition log independently, so we check each subscribing client's own
    delivered stream, grouped by the source producer key (cid, key).

    Only records from THIS run are checked (m.cid >= 0 skips foreign records
    whose value did not match the schema).  Per-run cid disjointness via
    _next_cid_base() keeps each run's records distinct from earlier runs in
    the same session without needing offset-based filtering.
    """
    run_rids: set[tuple[int, int, int]] = {_rid(m) for m in results.all_produced
                                           if m.cid >= 0}

    for sub in results.clients:
        by_key: dict[tuple[int, int], list[Msg]] = {}
        for m in sub.delivered:
            if m.cid < 0:  # foreign record — skip
                continue
            if _rid(m) not in run_rids:  # not produced in this run — skip
                continue
            by_key.setdefault((m.cid, m.key), []).append(m)

        for (cid, key), msgs in by_key.items():
            last_seq = None
            for m in msgs:
                if last_seq is not None and m.seq < last_seq:
                    raise AssertionError(
                        f"per-key-ordering: subscriber={sub.cid} src cid={cid} "
                        f"key={key} seq {m.seq} < previous {last_seq} (reordered)"
                    )
                last_seq = m.seq


def assert_exactly_once(results: Results) -> None:
    """No committed record delivered more than once TO THE SAME SUBSCRIBER.

    Exactly-once is per-subscriber: distinct subscribers each legitimately see
    every record on the shared partition, so duplicate detection is scoped to a
    single subscribing client's stream.

    Only records from THIS run are checked (cid >= 0 + run_rids membership).
    Per-run cid disjointness via _next_cid_base() keeps records from separate
    runs distinct without offset-based filtering.
    """
    run_rids: set[tuple[int, int, int]] = {_rid(m) for m in results.all_produced
                                           if m.cid >= 0}

    for sub in results.clients:
        seen: dict[tuple[int, int, int], int] = {}  # (cid,key,seq) → count
        for m in sub.delivered:
            if m.cid < 0:  # foreign record — skip
                continue
            rid = _rid(m)
            if rid not in run_rids:  # not from this run — skip
                continue
            seen[rid] = seen.get(rid, 0) + 1
        dups = {rid: n for rid, n in seen.items() if n > 1}
        assert not dups, (
            f"exactly-once violated for subscriber={sub.cid}: {len(dups)} records "
            f"delivered >1 time (by cid,key,seq): {list(dups.items())[:20]}"
        )


@timed("assert.single_leader")
def assert_single_leader(cluster, timeout_s: int = 30) -> int:
    """At most one node claims leadership at the current (highest) term.

    Counter-based, term-aware algorithm (Aeron-faithful; replaces log scraping):
      1. Read each RUNNING node's Aeron role + leadership-term counters via
         rune_role (cluster._read_role → docker exec).  The role counter mirrors
         Aeron Cluster's node-role counter (LEADER/FOLLOWER/CANDIDATE/NONE); the
         term counter carries that node's observed leadership term.
      2. Determine the highest term T_max among nodes reporting role=LEADER.
      3. Assert at most ONE distinct node reports LEADER at T_max.

    Rationale (DR-5): after a leader-isolation + heal cycle an ex-leader may
    briefly still report LEADER at an OLD term while the new leader reports
    LEADER at a HIGHER term.  Keying on T_max ignores the stale ex-leader — the
    term tie-break is exactly the old log oracle's discipline, but read from the
    authoritative counter instead of log text (so it is log-level independent).
    A REAL split-brain (two nodes reporting LEADER at the SAME highest term)
    still fails.

    Returns the agreed leader id at the highest term observed.
    """
    running = cluster.running_services()

    # Poll briefly: counters may lag a re-election by a poll tick.  Collect the
    # per-node (role, term) readings and evaluate the split-brain condition.
    deadline = time.time() + timeout_s
    leader_claims: dict[int, int] = {}  # node_id -> term (LEADER claims only)
    any_reading = False
    while True:
        leader_claims = {}
        any_reading = False
        for reading in cluster.read_roles(running):
            if reading is None:
                continue
            any_reading = True
            role, node_id, term = reading
            if role == "LEADER":
                # Keep the highest term seen for this node.
                leader_claims[node_id] = max(leader_claims.get(node_id, -1), term)
        if leader_claims or time.time() >= deadline:
            break
        # Counter is republished every ~100 ms; poll at 200 ms (matches
        # Cluster._LEADER_POLL_S) so a re-election is observed within ~one tick.
        time.sleep(0.2)

    assert any_reading, (
        f"no role counter readable among running nodes {running} "
        f"(is rune_role present in the image? are the nodes up?)"
    )
    assert leader_claims, (
        f"no node reports role=LEADER among running nodes {running}"
    )

    # Highest term across LEADER claims, then check for split-brain at that term.
    t_max = max(leader_claims.values())
    leaders_at_tmax = sorted(n for n, t in leader_claims.items() if t == t_max)
    assert len(leaders_at_tmax) == 1, (
        f"split-brain: {len(leaders_at_tmax)} distinct leaders at term {t_max}: "
        f"{leaders_at_tmax} (all LEADER claims node->term: {leader_claims})"
    )
    return leaders_at_tmax[0]


def assert_resume_from_offset(before: Results, after: Results, resume_offset: int) -> None:
    """A subscriber that resumed From::offset(resume_offset) must have delivered
    only records at offset >= resume_offset (no re-delivery of the prefix)."""
    bad = [m for m in after.all_delivered if m.offset < resume_offset]
    assert not bad, (
        f"resume-from-offset: {len(bad)} records delivered below resume offset "
        f"{resume_offset}: {[(m.cid, m.key, m.seq, m.offset) for m in bad[:20]]}"
    )
    _ = before


def assert_durable_after_restart(before: Results, after: Results) -> None:
    """Records acked BEFORE a restart must still be delivered AFTER it.

    `before` holds the produce results (committed set); `after` holds a
    fresh subscribe-from-earliest run performed once the node/cluster is back.
    """
    committed = {_rid(m) for m in before.all_produced}
    redelivered = {_rid(m) for m in after.all_delivered}
    missing = committed - redelivered
    assert not missing, (
        f"durable-after-restart: {len(missing)} pre-restart committed records "
        f"missing after restart: {sorted(missing)[:20]}"
    )


# ---------------------------------------------------------------------------
# CG-6: Consumer-group invariants
# ---------------------------------------------------------------------------

def assert_group_single_owner(group_results: GroupResults) -> None:
    """Every partition is owned by at most ONE member PER GENERATION.

    single_owner is a *same-generation* property: at any instant (i.e. within one
    assignment generation) a partition may be delivered to at most one member.
    We therefore key deliveries by (generation, partition, offset): if two
    distinct members report a GROUPREC for the same (partition, offset) IN THE
    SAME generation, that is a true simultaneous split-ownership and MUST fail.

    Cross-generation re-delivery is legitimate and NOT flagged:
      * During a rebalance a partition is reassigned from an old owner (gen N) to
        a new owner (gen N+1).  The new owner seeds at the committed offset and
        may re-read records the old owner already delivered.  Because the two
        deliveries carry DIFFERENT generations, they are not a split — they are
        bounded cross-generation duplication (the seam is checked by
        assert_group_bounded_duplicates / no_committed_loss instead).

    Duplicates within the SAME member are also allowed (redelivery after rebalance
    at the committed-offset boundary) and checked by bounded_duplicates.

    Backward compatibility: records from a pre-gen client image carry
    generation=-1; with a single sentinel bucket the check degrades to the old
    partition,offset-across-all behaviour, so it is never weaker for old images.
    """
    # Map (generation, partition, offset) -> set of member cids that delivered it.
    delivery_map: dict[tuple[int, int, int], set[int]] = defaultdict(set)
    for rec in group_results.all_group_records:
        delivery_map[(rec.generation, rec.partition, rec.offset)].add(rec.member_cid)

    violations = {
        k: sorted(v) for k, v in delivery_map.items() if len(v) > 1
    }
    assert not violations, (
        f"group-single-owner: {len(violations)} (generation,partition,offset) tuples "
        f"delivered to multiple members IN THE SAME GENERATION "
        f"(true split-ownership): {list(violations.items())[:10]}"
    )


def assert_group_no_committed_loss(group_results: GroupResults) -> None:
    """The UNION of all group members' records covers every produced record.

    A produced record is identified by (rec_cid, rec_key, rec_seq) — the triple
    encoded in the value payload.  The union of all group members' GROUPREC lines
    must include every triple the producer got an ack for.

    This is the group-level "at-least-once" invariant: no committed record is
    missed by the group as a whole.
    """
    # Produced set: (rec_cid, rec_key, rec_seq) for all acked records.
    produced_rids: set[tuple[int, int, int]] = {
        (m.cid, m.key, m.seq)
        for m in group_results.all_produced
        if m.cid >= 0
    }
    # Consumed union across all members.
    consumed_rids: set[tuple[int, int, int]] = {
        (r.rec_cid, r.rec_key, r.rec_seq)
        for r in group_results.all_group_records
        if r.rec_cid >= 0
    }
    missing = produced_rids - consumed_rids
    assert not missing, (
        f"group-no-committed-loss: {len(missing)} produced records never consumed "
        f"by any group member (by rec_cid,rec_key,rec_seq): "
        f"{sorted(missing)[:20]}{' ...' if len(missing) > 20 else ''}"
    )


def assert_group_bounded_duplicates(
    group_results: GroupResults,
    max_dup_fraction: float = 0.5,
) -> None:
    """Duplicates are bounded to the uncommitted window.

    After a rebalance or failover, a member may redeliver records starting from
    the last committed offset (not from 0).  This means duplicates — i.e. the
    same (partition, offset) appearing more than once for a SINGLE member — are
    expected in bounded amounts after faults.

    This assertion verifies:
      1. Per-member duplicates do not exceed max_dup_fraction × total_consumed
         (a gross-sanity cap that would catch "started from 0" regressions).
      2. Cross-member duplicates are zero (covered by assert_group_single_owner;
         this is a belt-and-suspenders check).

    The default max_dup_fraction=0.5 (50%) is a smoke gate, not a tight bound.
    It is intentionally loose for two reasons:
      1. Small tests (e.g. 80 records / 4 partitions, 2 members) where the
         uncommitted window before a rebalance can be up to ~autocommit_ms worth
         of records — with a 1 s autocommit interval and fast producers this can
         be a significant fraction of the total.
      2. The important correctness property is that re-delivery starts at the
         committed offset (not from 0).  A 50% cap is generous enough that it
         would catch a "restarted from 0" regression (which would yield ~100%
         duplicates for the pre-existing records) without requiring us to derive a
         tight per-test bound from the autocommit interval and produce rate.
    If you need a tighter bound, pass an explicit max_dup_fraction argument.
    """
    for member in group_results.member_results:
        seen: dict[tuple[int, int], int] = {}  # (partition, offset) -> count
        for r in member.records:
            k = (r.partition, r.offset)
            seen[k] = seen.get(k, 0) + 1
        dups = {k: n for k, n in seen.items() if n > 1}
        if not dups:
            continue
        total = len(member.records)
        dup_count = sum(n - 1 for n in dups.values())
        fraction = dup_count / max(total, 1)
        assert fraction <= max_dup_fraction, (
            f"group-bounded-duplicates: member cid={member.cid} has {dup_count} "
            f"duplicate deliveries out of {total} total "
            f"({fraction:.1%} > {max_dup_fraction:.1%} cap). "
            f"First 10 dups: {list(dups.items())[:10]}"
        )


def assert_group_offset_resume(
    group_results_before: GroupResults,
    group_results_after: GroupResults,
) -> None:
    """After a coordinator failover, members resume from the committed offset.

    Compares two GroupResults:
      - ``group_results_before``: produced+consumed before the failover.
      - ``group_results_after``: produced+consumed after the failover.

    The ``after`` members must NOT re-deliver records from offset 0 (i.e. the
    minimum offset seen in the ``after`` run must be >= the minimum committed
    offset from the ``before`` run minus the uncommitted window tolerance).

    Concretely: for each partition, the earliest offset consumed in the AFTER
    run must be within the autocommit window of the latest committed offset in
    the BEFORE run — not at 0 unless the partition was genuinely empty.

    This is a relaxed check: it only fires if ``after`` members start from an
    offset that is suspiciously close to 0 while ``before`` consumed records
    well past 0.
    """
    # For each partition: what was the maximum offset consumed in the before run?
    before_max_offset: dict[int, int] = {}
    for r in group_results_before.all_group_records:
        before_max_offset[r.partition] = max(
            before_max_offset.get(r.partition, -1), r.offset
        )

    # Minimum offset in the after run per partition.
    after_min_offset: dict[int, int] = {}
    for r in group_results_after.all_group_records:
        after_min_offset[r.partition] = min(
            after_min_offset.get(r.partition, 2**63 - 1), r.offset
        )

    violations: list[str] = []
    for part, before_max in before_max_offset.items():
        if before_max <= 0:
            continue  # partition had nothing or only offset-0 records before
        after_min = after_min_offset.get(part)
        if after_min is None:
            continue  # partition not consumed after failover — skip
        # If after_min is 0 and before_max is > 10, it almost certainly means
        # the member reset to offset 0 instead of resuming from the committed offset.
        if after_min == 0 and before_max > 10:
            violations.append(
                f"partition {part}: after-failover min_offset={after_min} "
                f"but before had max_offset={before_max} "
                f"(suspected restart from 0)"
            )
    assert not violations, (
        "group-offset-resume: members appear to have restarted from offset 0 "
        f"instead of committed offset after failover:\n" +
        "\n".join(violations)
    )


# ---------------------------------------------------------------------------
# TX-6: Exactly-once / transactional invariants
# ---------------------------------------------------------------------------

# Warm-up / probe records injected by the harness itself must be excluded from
# EOS/isolation reasoning.  cluster.wait_data_plane_ready() produces warm-up
# records with cid_base=995000 and serving_node() probes with cid_base=990000.
# These non-txn records land in the shared "events" topic log and are read by
# read_committed consumers; they are NOT part of any transaction under test, so
# every txn invariant skips records with cid >= _WARMUP_CID_MIN (same treatment
# as the existing m.cid < 0 foreign-record skip).
_WARMUP_CID_MIN = 990000


def _txn_relevant(m: "Msg") -> bool:
    """True if a delivered record belongs to a transaction under test.

    Excludes foreign records (cid < 0, value payload did not match schema) and
    harness warm-up/probe records (cid >= _WARMUP_CID_MIN).
    """
    return 0 <= m.cid < _WARMUP_CID_MIN


def assert_txn_committed(txn_result: "TxnResult") -> None:
    """The transaction must have committed (outcome == 'committed')."""
    assert txn_result.outcome == "committed", (
        f"txn outcome={txn_result.outcome!r}, expected 'committed'; "
        f"txn_id={txn_result.txn_id} raw tail:\n{txn_result.raw[-500:]}"
    )


def assert_txn_aborted(txn_result: "TxnResult") -> None:
    """The transaction must have aborted (outcome == 'aborted')."""
    assert txn_result.outcome == "aborted", (
        f"txn outcome={txn_result.outcome!r}, expected 'aborted'; "
        f"txn_id={txn_result.txn_id}"
    )


def assert_eos_no_dup(txn_result: "TxnResult") -> None:
    """Exactly-once IN THE LOG: each committed (cid,key,seq) occupies exactly
    ONE distinct (partition, offset).

    Exactly-once in Rune/Kafka is a property of the COMMITTED LOG — the
    idempotent producer deduplicates by (producerId, partition, baseSequence),
    so each distinct record appears at exactly one log position.  It is NOT a
    property of consumer delivery: a group consumer that joins an empty group
    and consumes over a window legitimately re-reads records across rebalance
    generations (join→sync→stable, autocommit timing).  Re-reading the SAME
    (partition, offset) is at-least-once at the consumer and is EXPECTED — see
    assert_group_single_owner (keys on (generation, partition, offset)) and the
    "bounded cross-generation duplication" notes above.

    So the correct EOS check dedups by log position, not by delivery count:

      * Group delivered records by (cid, key, seq).
      * Assert each distinct (cid, key, seq) maps to exactly ONE distinct
        (partition, offset).

    A (cid,key,seq) appearing at TWO different (partition, offset) positions is a
    real producer-dedup FAILURE (the record was written to the log twice) and
    fails.  Multiple deliveries of the SAME (partition, offset) [consumer re-read
    across generations] are ALLOWED.

    Warm-up/probe records (cid >= _WARMUP_CID_MIN) and foreign records (cid < 0)
    are skipped.
    """
    positions: dict[tuple[int, int, int], set[tuple[int, int]]] = defaultdict(set)
    for m in txn_result.delivered:
        if not _txn_relevant(m):
            continue
        rid = (m.cid, m.key, m.seq)
        positions[rid].add((m.partition, m.offset))
    dups = {rid: sorted(pos) for rid, pos in positions.items() if len(pos) > 1}
    assert not dups, (
        f"eos-no-dup (log-level): {len(dups)} records written to >1 distinct "
        f"(partition, offset) — producer-dedup failure: "
        f"{list(dups.items())[:20]}"
    )


def assert_eos_no_loss(produced: list, delivered: "TxnResult") -> None:
    """No committed record missed: every produced (cid,key,seq) must appear
    in the delivered set.

    `produced` is a list of TxnMsg (from txn_result.sent) that were sent in
    a committed transaction.
    """
    prod_ids = {(m.cid, m.key, m.seq) for m in produced}
    del_ids = {(m.cid, m.key, m.seq) for m in delivered.delivered
               if _txn_relevant(m)}
    missing = prod_ids - del_ids
    assert not missing, (
        f"eos-no-loss: {len(missing)} committed records never delivered: "
        f"{sorted(missing)[:20]}"
    )


def assert_read_committed_no_aborted(consumer_result: "TxnResult") -> None:
    """A read_committed consumer must NOT have seen records from aborted transactions.

    This invariant is verified by comparing the read_committed consumer against
    the read_uncommitted consumer:  records seen ONLY by the uncommitted reader
    (i.e., in aborted/open transactions) must NOT appear in the committed reader's
    output.

    Usage: pass the aborted producer's sent records and the read_committed consumer
    result; none of the aborted records should appear.
    """
    assert consumer_result.isolation == "read_committed", (
        f"expected isolation=read_committed but got {consumer_result.isolation!r}"
    )


def assert_read_committed_excludes(
    committed_consumer: "TxnResult",
    aborted_msgs: list,
) -> None:
    """None of the aborted_msgs should appear in the committed_consumer's delivered set.

    aborted_msgs: list of TxnMsg from an aborted (or open) transaction.
    committed_consumer: TxnResult from a txn_consume with isolation=read_committed.
    """
    aborted_ids = {(m.cid, m.key, m.seq) for m in aborted_msgs}
    delivered_ids = {(m.cid, m.key, m.seq) for m in committed_consumer.delivered
                     if _txn_relevant(m)}
    leaked = aborted_ids & delivered_ids
    assert not leaked, (
        f"read-committed-isolation: {len(leaked)} aborted/open records leaked into "
        f"read_committed consumer: {sorted(leaked)[:20]}"
    )


def assert_read_uncommitted_includes(
    uncommitted_consumer: "TxnResult",
    expected_msgs: list,
) -> None:
    """A read_uncommitted consumer should include records that are >= HW.

    We only assert it found AT LEAST the committed ones (if txn was committed).
    """
    assert uncommitted_consumer.isolation in ("read_uncommitted", ""), (
        f"expected isolation=read_uncommitted but got {uncommitted_consumer.isolation!r}"
    )
    exp_ids = {(m.cid, m.key, m.seq) for m in expected_msgs}
    del_ids = {(m.cid, m.key, m.seq) for m in uncommitted_consumer.delivered
               if _txn_relevant(m)}
    missing = exp_ids - del_ids
    assert not missing, (
        f"read-uncommitted: {len(missing)} expected records not delivered: "
        f"{sorted(missing)[:20]}"
    )


# ---- per-worker MDC egress is really used (DR re-check I6) ------------------------------------------------
EGRESS_CONNECTED_LINE = "per-worker MDC egress connected"
# Logged once at start by a broker whose subscriptions go out on per-worker egress (legacy per-worker egress,
# or shard mode, where every shard owns its egress).
_EGRESS_ACTIVE_LINES = ("per-worker EGRESS ACTIVE", "worker shard(s) attached")


def assert_per_worker_egress_connected(cluster, topic_id: int, partitions, require_active: bool) -> list[str]:
    """Every service that leads a subscribed partition (`partitions` of `topic_id`) and runs per-worker egress
    logged that a consumer connected to its MDC egress — else the clients fell back to legacy egress (B-I3).
    `require_active`: the run's mode has per-worker egress (the shard overlay), so a broker that never reports
    it active fails too.  Returns the services checked; [] means no subscribed leader runs per-worker egress."""
    leaders = cluster.partition_leaders(topic_id)
    missing = [p for p in partitions if p not in leaders]
    assert not missing, f"no becomeLeader logged for topic {topic_id} partition(s) {missing}"
    checked = []
    for svc in sorted({leaders[p] for p in partitions}):
        logs = cluster.compose.logs(svc, timeout=30)
        active = any(line in logs for line in _EGRESS_ACTIVE_LINES)
        if not active:
            assert not require_active, (
                f"{svc} leads a subscribed partition of topic {topic_id} but never reported per-worker egress "
                f"active in a per-worker-egress run")
            continue
        assert EGRESS_CONNECTED_LINE in logs, (
            f"{svc} leads a subscribed partition of topic {topic_id} with per-worker egress active, but no consumer "
            f"ever connected to its MDC egress: clients fell back to legacy egress (B-I3)")
        checked.append(svc)
    return checked
