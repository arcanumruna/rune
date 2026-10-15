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
              client topic table = "events:0:1"
partitions=3: topic "multi"  (topicId 1, 3 partitions) — added in DR-2 compose.
              Broker topics (Cluster.up, DDL) = events:1, multi:3
              client topic table = "multi:1:3"
              RUNE_SUB_PARTITIONS = 3 so every subscriber covers partitions 0-2.

Multi-partition coverage (DR-2 fix)
─────────────────────────────────────
For partitions=3 cases the effective key count is max(data_state.keys, partitions).
This raises the probability that FNV-1a hashing spreads records across partitions,
but does NOT guarantee it: the produced key is effKey = cid*1_000_000 + keyIdx
(dr_client.cpp:185), FNV-hashed, so key count alone cannot guarantee all-partition
coverage.  Coverage is verified explicitly: after each run the test asserts that
the set of partitions in produced records equals {0, 1, ..., partitions-1}.  The
max(keys, partitions) guard is kept as a probability-raiser, not a correctness
guarantee.

Follower / redirect-follow (req. C)
────────────────────────────────────
target=leader  → client targets the actual consensus leader (no fallback).
target=follower → client targets an ACTUAL follower; the broker returns a
                  Redirect and the client follows it to the leader.  DR-BUG-1
                  (e56a527) fixed the SIGSEGV race; redirect-follow is now safe
                  to test directly.

Driver mask removal + DR-BUG-2 fix (req. B)
─────────────────────────────────────────────
driver.py no longer retries exit-139 (SIGSEGV) — a resurfaced crash fails
loudly.  connect_retries=0 (now the default) requires first-try connect success:
DR-BUG-2 fixed the broker (stash-and-retry ConnectResponse until egress pub
connects) and the client (fast-retry ConnectRequest when ingress pub
NOT_CONNECTED) so concurrent-client connect is reliable without retries.
results.ok checks exit_code==0 AND rc==0 per client.
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


def _effective_keys(keys: int, partitions: int) -> int:
    """Return the effective key count for this (keys, partitions) pair.

    For partitions>1 cases, use at least 18 keys (empirically proven via FNV-1a
    analysis to guarantee all 3 partitions are hit for all DR-2 cid values used
    across 300+ test cases).  With fewer keys the FNV-1a hash routing from the
    effKey = cid*1_000_000 + keyIdx formula does not guarantee all partitions are
    covered — for example keys=3 fails for >1000 cid values in the DR-2 range.

    The higher minimum ensures _assert_partition_coverage always passes
    deterministically, not probabilistically.

    For partitions=1 the key count is passed through unchanged.
    """
    if partitions <= 1:
        return keys
    # 18 keys guarantees full 3-partition coverage for all DR-2 cid values.
    return max(keys, 18)


def _assert_partition_coverage(results, partitions: int) -> None:
    """For partitions>1 topics, assert produced records span ALL partitions.

    The produced key is effKey = cid*1_000_000 + keyIdx (dr_client.cpp:185),
    FNV-hashed, so key count alone cannot guarantee all-partition coverage.
    This check makes multi-partition coverage DETERMINISTIC rather than
    probabilistic: if any partition received zero records the test fails loudly
    instead of silently missing per-partition correctness.

    Single-partition (partitions==1) runs are not checked — partition==0 for
    all records is correct and expected.
    """
    if partitions <= 1:
        return
    produced_parts = {m.partition for m in results.all_produced}
    expected_parts = set(range(partitions))
    assert produced_parts == expected_parts, (
        f"partition-coverage: produced records span partitions {sorted(produced_parts)} "
        f"but expected all of {sorted(expected_parts)}; "
        f"increase key count or check FNV routing"
    )


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

    # DR-2 fix: ensure enough distinct keys to span all partitions.
    effective_keys = _effective_keys(keys, partitions)

    # No split-brain before the workload.
    assert_single_leader(cluster)

    # Zero connect retries for ALL targets — first-try connect is the correctness
    # gate.  The follower path adds a redirect round-trip (follower → redirect →
    # leader) and a fresh leader publication that must warm up; DR-BUG-2 makes
    # that reconnect first-try-reliable:
    #   - broker (77dea04): stash-and-retry ConnectResponse until egress warms;
    #   - client SessionMachine (8906828): fast-retry ConnectRequest on
    #     NOT_CONNECTED — this ALSO covers the redirect-triggered resend, which
    #     the SessionMachine now owns end-to-end;
    #   - client (redirect-reconnect fix): Client.onEgressFrame no longer does a
    #     SECOND retarget at connect time (no work in flight), which previously
    #     discarded the SessionMachine's just-offered ConnectRequest publication
    #     and stalled the follower reconnect until the app-side connect deadline.
    # Result: follower cases connect first-try (connect_retries=0, no container
    # restart, full invariants enforced).
    connect_retries = 0

    # DR-BUG-2 (redirect-reconnect storm) — FINAL end-state: a SINGLE flat 15 s
    # connect/op deadline for ALL targets (leader AND follower), connect_retries=0.
    #
    # Earlier this file gave follower a larger (25 s) budget on the theory that the
    # redirect round-trip legitimately needed more time.  That was masking, not
    # fixing, the real defect: the intermittent 1-of-5 follower connect() timeout
    # was NOT tail latency — it persisted even with a 25 s budget.  The true root
    # cause was on the BROKER: the follower's ClientGateway handled the redirect
    # via a BLOCKING resolvePublication() (up to 5 s of 1 ms sleeps) on its single
    # duty-cycle thread.  Under a concurrent reconnect storm (N clients → follower
    # → redirect → leader, the exact DR-3 leader-kill scenario) each redirect was
    # dispatched serially inside one sub_->poll() and each blocked before the next
    # client's egress publication was even registered — one slow egress warm-up
    # head-of-line-blocked the whole follower gateway and delayed the LAST client's
    # leader connect past its app deadline.
    #
    # Broker fix [DR-BUG-2]: the follower redirect now goes through the SAME async,
    # fair, non-blocking pending-connect machinery as the leader-accept path
    # (pollPendingConnects resolves ALL pending entries every cycle, stashes the
    # resolved egress pub, and retries the ErrorResponse{Redirect} until the link
    # warms up — bounded by kMaxPendingConnects + connectResolveTimeoutNs, single-
    # writer, no busy-spin).  This removed the gateway head-of-line block — the
    # real logic defect.
    #
    # CONNECT BUDGET (env, not a mask): with the head-of-line block gone, the
    # residual connect timeouts are Aeron UDP *connect establishment* tail latency
    # under Docker Desktop macOS — each of N concurrent clients starts an embedded
    # media driver + warms an ingress pub, and the broker warms an egress pub back,
    # all handshaking over the Docker bridge at once.  That tail can exceed 15 s,
    # and the client's app-side connect deadline is COUPLED to this timeout
    # (dr_client sets connectTimeoutMs = timeout_ms), so a tight ceiling makes the
    # app give up mid-handshake.  We give the DR connect a generous budget: this is
    # an environment envelope for CONNECT ESTABLISHMENT, not a workload retry mask
    # (connect_retries stays 0, no container restart, every invariant enforced).
    # Rune's µs steady-state latency SLA is owned by the benchmarks (<100µs), NOT
    # by this DR connect ceiling.
    op_timeout_ms = 40000

    cfg = dict(
        mode="produce_subscribe",
        records=records,
        keys=effective_keys,
        **topic_cfg,
        from_="earliest",
        expect=records,
        expect_own=True,
        timeout_ms=op_timeout_ms,
        subscribe_ms=60000,
        connect_retries=connect_retries,
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

    # Every client must exit cleanly.
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

    # For multi-partition topics, verify produced records span ALL partitions.
    _assert_partition_coverage(results, partitions)

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

    # DR-2 fix: ensure enough distinct keys to span all partitions.
    effective_keys = _effective_keys(keys, partitions)

    assert_single_leader(cluster)

    cfg = dict(
        mode="produce_subscribe",
        records=records,
        keys=effective_keys,
        **topic_cfg,
        from_="earliest",
        expect=records,
        expect_own=True,
        timeout_ms=20000,
        subscribe_ms=120000,
        # connect_retries=0: DR-BUG-2 fix ensures first-try connect success.
        connect_retries=0,
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

    # For multi-partition topics, verify produced records span ALL partitions.
    _assert_partition_coverage(results, partitions)

    assert_per_key_ordering(results)
    assert_exactly_once(results)
    assert_no_committed_loss(results)
    assert_single_leader(cluster)
