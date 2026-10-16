"""assert_per_worker_egress_connected (DR re-check I6) over canned broker logs — no docker."""
import pytest

from harness.invariants import assert_per_worker_egress_connected

_LEAD = "PartitionReplica[node={n}] becomeLeader topic=0 part=0 epoch={e}\n"
_ACTIVE = "BrokerNode[0]: 2 worker shard(s) attached (own driver + client + plane + ingress + egress)\n"
_CONNECTED = "WorkerEgress[w1] per-worker MDC egress connected (control 0.0.0.0:40701)\n"


class _Compose:
    def __init__(self, logs): self._logs = logs
    def logs(self, svc, timeout=30): return self._logs.get(svc, "")


class _Cluster:
    from harness.cluster import Cluster as _C
    _BECOME_LEADER_RE = _C._BECOME_LEADER_RE
    def __init__(self, logs): self.compose = _Compose(logs)
    def partition_leaders(self, topic_id):
        from harness.cluster import Cluster
        return Cluster.partition_leaders(self, topic_id)


def test_the_leader_with_a_connected_egress_passes():
    c = _Cluster({"rune-1": _LEAD.format(n=0, e=1), "rune-2": _LEAD.format(n=1, e=2) + _ACTIVE + _CONNECTED})
    assert assert_per_worker_egress_connected(c, 0, [0], require_active=True) == ["rune-2"]


def test_a_leader_whose_clients_fell_back_to_legacy_egress_fails_regression():
    c = _Cluster({"rune-2": _LEAD.format(n=1, e=2) + _ACTIVE})
    with pytest.raises(AssertionError, match="fell back to legacy egress"):
        assert_per_worker_egress_connected(c, 0, [0], require_active=True)


def test_a_line_on_a_non_leader_does_not_count_regression():
    c = _Cluster({"rune-1": _ACTIVE + _CONNECTED, "rune-2": _LEAD.format(n=1, e=2) + _ACTIVE})
    with pytest.raises(AssertionError, match="rune-2"):
        assert_per_worker_egress_connected(c, 0, [0], require_active=True)


def test_inactive_egress_is_skipped_only_when_the_mode_has_none_regression():
    c = _Cluster({"rune-2": _LEAD.format(n=1, e=2)})
    assert assert_per_worker_egress_connected(c, 0, [0], require_active=False) == []
    with pytest.raises(AssertionError, match="never reported per-worker egress"):
        assert_per_worker_egress_connected(c, 0, [0], require_active=True)
