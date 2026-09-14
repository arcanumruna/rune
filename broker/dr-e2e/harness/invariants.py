"""Invariant assertions over driver Results + cluster state.

Each assertion raises AssertionError with the offending records / nodes on
failure, and is a no-op on success.  These are the correctness contracts the
DR matrix checks after every fault scenario.

Terminology:
  * "produced" = records the producer got a Leader ack for (durable/committed).
  * "delivered" = records a subscriber actually received.
  * A record is identified across produce/subscribe by (cid, key, seq): the
    dr_client encodes that triple into the record VALUE payload.
"""

from __future__ import annotations

from .driver import Msg, Results


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

    We parse each running node's logs for its LATEST 'node <id> is LEADER'
    announcement; every node must agree on the SAME leader id.  Two distinct
    write-accepting leaders would show divergent latest announcements.
    Returns the agreed leader id.
    """
    import re
    from .cluster import SERVICES

    leader_re = re.compile(r"node (\d+) is LEADER")
    claims: dict[str, int] = {}
    running = cluster.running_services()
    for svc in running:
        logs = cluster.compose.logs(svc, timeout=timeout_s)
        ids = leader_re.findall(logs)
        if ids:
            claims[svc] = int(ids[-1])
    distinct = set(claims.values())
    assert len(distinct) <= 1, (
        f"split-brain: nodes disagree on leader: {claims} "
        f"(distinct leaders: {sorted(distinct)})"
    )
    assert distinct, f"no leader observed among running nodes {running}"
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
