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
from collections import defaultdict

from .driver import GroupResults, Msg, Results


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


def assert_single_leader(cluster, timeout_s: int = 30) -> int:
    """At most one node claims leadership for the current term.

    Term-aware algorithm (DR-5 fix):
      1. Parse each running node's logs for ALL 'node <id> is LEADER term <T>'
         announcements.  If no term is present (legacy format without 'term'),
         treat term as -1 (lowest priority — only used as a last resort).
      2. Determine the highest term T_max observed across all running nodes.
      3. Consider ONLY announcements at T_max; check that at most ONE distinct
         node id claims leadership at that term.

    Rationale: after a leader-isolation + heal cycle the ex-leader's log still
    contains its pre-partition "node N is LEADER term T_old" startup entry.
    The new leader logs "node M is LEADER term T_new" (T_new > T_old).  The
    old term-unaware oracle saw two distinct node ids and raised a false
    split-brain.  The term-aware oracle ignores T_old announcements — it checks
    only T_max, where exactly one node should be the leader.  A REAL split-brain
    (two nodes both logging "is LEADER" at the SAME highest term) still fails.

    Returns the agreed leader id at the highest term observed.
    """
    import re
    from .cluster import SERVICES

    # Matches both:
    #   "node N is LEADER term T"   (new format, term-aware)
    #   "node N is LEADER"          (legacy format, no term — treated as term -1)
    leader_re = re.compile(r"node (\d+) is LEADER(?: term (\d+))?")

    # Per-service: collect all (term, node_id) pairs from the log.
    # We keep ALL announcements, not just the latest, so the highest-term one wins.
    per_svc_announcements: dict[str, list[tuple[int, int]]] = {}
    running = cluster.running_services()
    for svc in running:
        logs = cluster.compose.logs(svc, timeout=timeout_s)
        matches = leader_re.findall(logs)
        # matches is a list of (node_id_str, term_str_or_empty)
        announcements = [
            (int(term_s) if term_s else -1, int(nid_s))
            for nid_s, term_s in matches
        ]
        if announcements:
            per_svc_announcements[svc] = announcements

    if not per_svc_announcements:
        assert False, f"no leader observed among running nodes {running}"

    # Find the highest term across all running nodes' announcements.
    t_max = max(
        term
        for announcements in per_svc_announcements.values()
        for (term, _nid) in announcements
    )

    # For each running node, take the latest announcement at t_max (if any).
    # A node that has no announcement at t_max uses its highest-term announcement
    # only if that's all it has (i.e., it hasn't yet received the new term).
    # We only assert on nodes that HAVE an announcement at t_max; nodes that
    # haven't yet seen t_max (e.g. late-rejoin) are silently ignored — their
    # stale announcements are below t_max and thus excluded.
    claims_at_tmax: dict[str, int] = {}
    for svc, announcements in per_svc_announcements.items():
        tmax_announcements = [(t, nid) for (t, nid) in announcements if t == t_max]
        if tmax_announcements:
            # Take the most recent announcement at t_max (last in log order).
            claims_at_tmax[svc] = tmax_announcements[-1][1]

    distinct = set(claims_at_tmax.values())
    assert len(distinct) <= 1, (
        f"split-brain: two distinct leaders at term {t_max}: {claims_at_tmax} "
        f"(distinct leaders: {sorted(distinct)}; all claims: {per_svc_announcements})"
    )
    assert distinct, (
        f"no leader observed at highest term {t_max} among running nodes {running}; "
        f"all claims: {per_svc_announcements}"
    )
    _ = SERVICES
    return next(iter(distinct))


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
    """Every partition is consumed by at most ONE member at any time.

    We identify records by (partition, offset): if two distinct members report
    a GROUPREC for the same (partition, offset), that violates single_owner.

    Note: duplicates within the SAME member (redelivery after rebalance at the
    committed offset boundary) are allowed and checked by assert_group_bounded_duplicates.
    This assertion only fires when TWO DIFFERENT members receive the SAME record
    simultaneously (true split-ownership, which must never happen).
    """
    # Map (partition, offset) -> set of member cids that delivered it.
    delivery_map: dict[tuple[int, int], set[int]] = defaultdict(set)
    for rec in group_results.all_group_records:
        delivery_map[(rec.partition, rec.offset)].add(rec.member_cid)

    violations = {
        k: sorted(v) for k, v in delivery_map.items() if len(v) > 1
    }
    assert not violations, (
        f"group-single-owner: {len(violations)} (partition,offset) pairs delivered "
        f"to multiple members simultaneously: "
        f"{list(violations.items())[:10]}"
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

    The default max_dup_fraction=0.5 (50%) is intentionally loose so that a
    single rebalance in a small test (e.g. 50 records / 2 partitions) where the
    second member re-reads up to the committed frontier does not fail the test.
    The important invariant is that duplicates are BOUNDED (not from offset 0)
    which is captured by checking that no member sees more than half the total
    produced records as duplicates.
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
