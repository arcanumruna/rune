"""DR-2: multi-client concurrent baseline — correctness floor (no faults).

K concurrent clients produce to many keys across topic partitions and subscribe;
asserts per-key ordering, exactly-once, no-committed-loss, and single-leader
before+after.  This is the correctness floor the fault tickets build on.

Matrix design
─────────────
Quick matrix (48 cases, ~25 min wall-clock):
  clients      × {1, 3, 5}
  data_state   × {(1, 100), (4, 500)}        — light and medium tiers
  partitions   × {1, 3}
  target       × {leader, follower}
  repetition   × 2                            — catch flakiness

Soak matrix (4 cases, @pytest.mark.soak, ~8 min additional):
  clients      × {3, 5}
  data_state   × {(16, 1000)}                — heavy soak tier
  partitions   × {1, 3}
  target       × {leader}                    — follower path already in quick
  repetition   × 1

Total: 52 cases.  Run without --soak: 48 cases (quick only, ~25 min).

Per-case record counts (aggregate = n_clients * records):
  (1 client,  100 recs)  =   100   records — trivial
  (3 clients, 100 recs)  =   300   records
  (5 clients, 100 recs)  =   500   records
  (1 client,  500 recs)  =   500   records
  (3 clients, 500 recs)  = 1 500   records — medium
  (5 clients, 500 recs)  = 2 500   records
  (3 clients, 1000 recs) = 3 000   records — soak
  (5 clients, 1000 recs) = 5 000   records — soak

Topic wiring
────────────
partitions=1: topic "events" (topicId 0, 1 partition) — same as DR-1 baseline.
              client RUNE_TOPICS = "events:0:1"
partitions=3: topic "multi"  (topicId 1, 3 partitions) — added in DR-2 compose.
              Broker RUNE_TOPICS = "events:1,multi:3"
              client RUNE_TOPICS = "multi:1:3"
              RUNE_SUB_PARTITIONS = 3 so every subscriber covers partitions 0-2.

Follower / redirect-follow (req. C)
────────────────────────────────────
target=leader  → client targets the actual consensus leader (no fallback).
target=follower → client targets an ACTUAL follower; the broker returns a
                  Redirect and the client follows it to the leader.  DR-BUG-1
                  (e56a527) fixed the SIGSEGV race; redirect-follow is now safe
                  to test directly.

Driver mask removal (req. B)
─────────────────────────────
driver.py no longer retries exit-139 (SIGSEGV) — a resurfaced crash fails
loudly.  connect_retries=2 allows only Aeron UDP connect-timeout retries
under concurrent load (rc==2 + nothing produced); crashes (exit 139) still
fail immediately.  results.ok checks exit_code==0 AND rc==0 per client.
"""

from __future__ import annotations

import pytest

from harness import matrix
from harness.driver import run_clients
from harness.invariants import (
    assert_exactly_once,
    assert_no_committed_loss,
    assert_per_key_ordering,
    assert_single_leader,
)

# ── Helpers ──────────────────────────────────────────────────────────────────

# Monotonic per-run client-id base.  The cluster's partition log PERSISTS across
# the session-scoped cluster, so each case gets a disjoint cid window (stride
# large enough for the max client count per case = 5) so old runs' records
# never confuse per-key ordering / exactly-once for the current run.
_CID_STRIDE = 2000   # wider than DR-1's 1000 to avoid cid overlap with it
_run_counter = {"n": 0}


def _next_cid_base() -> int:
    _run_counter["n"] += 1
    # Offset well above DR-1's range (DR-1 uses n*1000, max ~16*1000=16000)
    return 100_000 + _run_counter["n"] * _CID_STRIDE


def _n_clients(client_state: str) -> int:
    return int(client_state.split("=")[1])


def _topic_cfg(partitions: int) -> dict:
    """Return driver cfg keys for the topic shape (1 or 3 partitions)."""
    if partitions == 1:
        return {
            "topics": "events:0:1",
            "sub_partitions": 1,
        }
    else:
        # "multi" topic: topicId=1, 3 partitions.  Subscribe to all 3 so
        # assert_no_committed_loss is meaningful across every partition.
        return {
            "topics": "multi:1:3",
            "sub_partitions": 3,
        }


def _resolve_target(cluster, target: str) -> str:
    """Return the actual broker service hostname for the given role.

    DR-BUG-1 is fixed; we target the REAL leader or an ACTUAL follower with
    NO serving_node() fallback.  A follower-targeted client exercises the
    redirect-follow path: follower returns Redirect → client retargets to the
    leader → produce/subscribe succeed.
    """
    if target == "leader":
        _, svc = cluster.identify_leader()
        return svc
    elif target == "follower":
        return cluster.follower_service()
    else:
        from harness.cluster import node_id_to_service
        return node_id_to_service(int(target))


# ── Quick matrix ─────────────────────────────────────────────────────────────
# 3 × 2 × 2 × 2 × 2 = 48 cases.

_QUICK = matrix.matrix(
    client_state=["clients=1", "clients=3", "clients=5"],
    data_state=[(1, 100), (4, 500)],
    extra=[
        (("partitions", 1), ("target", "leader")),
        (("partitions", 1), ("target", "follower")),
        (("partitions", 3), ("target", "leader")),
        (("partitions", 3), ("target", "follower")),
    ],
    repetition=2,
)


@pytest.mark.baseline
@pytest.mark.timeout(600)
@pytest.mark.parametrize("case", _QUICK, ids=[c.id() for c in _QUICK])
def test_dr2_multiclient(cluster, case):
    """Correctness floor: concurrent clients, no faults."""
    n = _n_clients(case.client_state)
    keys, records = case.keys, case.records
    extra = dict(case.extra)
    partitions = int(extra["partitions"])
    target = extra["target"]
    target_svc = _resolve_target(cluster, target)
    cid_base = _next_cid_base()
    topic_cfg = _topic_cfg(partitions)

    # No split-brain before the workload.
    assert_single_leader(cluster)

    cfg = dict(
        mode="produce_subscribe",
        records=records,
        keys=keys,
        **topic_cfg,
        from_="earliest",
        expect=records,
        expect_own=True,
        timeout_ms=15000,
        subscribe_ms=60000,
        # connect_retries=2: allow Aeron UDP connect-timeout retries under load
        # (5 concurrent clients hitting the same broker node can cause transient
        # >15s connect delays on a loaded cluster, NOT a broker fault).  Retries
        # are strictly scoped: only on rc==2 + "connect() timed out" with zero
        # produced records — crashes (exit 139) still fail immediately.
        connect_retries=2,
    )
    cfg["from"] = cfg.pop("from_")

    results = run_clients(
        n,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=240,
        **cfg,
    )

    # Every client must exit cleanly on the FIRST try (no retry mask).
    assert results.ok, (
        "one or more dr_client containers failed (exit!=0 or rc!=0):\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-2000:]}"
            for c in results.clients if c.exit_code != 0 or c.rc != 0
        )
    )

    # All n clients must have produced their full record counts.
    total_expected = n * records
    assert len(results.all_produced) == total_expected, (
        f"produced {len(results.all_produced)} != expected {total_expected} "
        f"(n={n}, records={records})"
    )

    # Correctness invariants.
    assert_per_key_ordering(results)
    assert_exactly_once(results)
    assert_no_committed_loss(results)

    # No split-brain after the workload.
    assert_single_leader(cluster)


# ── Soak matrix ──────────────────────────────────────────────────────────────
# 2 × 1 × 2 × 1 × 1 = 4 cases.  Heavy tier; skip by default with -m "not soak".

_SOAK = matrix.matrix(
    client_state=["clients=3", "clients=5"],
    data_state=[(16, 1000)],
    extra=[
        (("partitions", 1), ("target", "leader")),
        (("partitions", 3), ("target", "leader")),
    ],
    repetition=1,
)


@pytest.mark.soak
@pytest.mark.timeout(600)
@pytest.mark.parametrize("case", _SOAK, ids=[c.id() for c in _SOAK])
def test_dr2_multiclient_soak(cluster, case):
    """Heavy soak: 16 keys × 1000 records × 3-5 clients.  Run with -m soak."""
    n = _n_clients(case.client_state)
    keys, records = case.keys, case.records
    extra = dict(case.extra)
    partitions = int(extra["partitions"])
    target = extra["target"]
    target_svc = _resolve_target(cluster, target)
    cid_base = _next_cid_base()
    topic_cfg = _topic_cfg(partitions)

    assert_single_leader(cluster)

    cfg = dict(
        mode="produce_subscribe",
        records=records,
        keys=keys,
        **topic_cfg,
        from_="earliest",
        expect=records,
        expect_own=True,
        timeout_ms=20000,
        subscribe_ms=120000,
        connect_retries=2,
    )
    cfg["from"] = cfg.pop("from_")

    results = run_clients(
        n,
        network=cluster.compose.network_name(),
        target_service=target_svc,
        cid_base=cid_base,
        timeout_s=420,
        **cfg,
    )

    assert results.ok, (
        "soak: one or more dr_client containers failed:\n"
        + "\n\n".join(
            f"[cid={c.cid} exit={c.exit_code} rc={c.rc}]\n{c.raw[-2000:]}"
            for c in results.clients if c.exit_code != 0 or c.rc != 0
        )
    )

    total_expected = n * records
    assert len(results.all_produced) == total_expected, (
        f"soak: produced {len(results.all_produced)} != expected {total_expected}"
    )

    assert_per_key_ordering(results)
    assert_exactly_once(results)
    assert_no_committed_loss(results)
    assert_single_leader(cluster)
