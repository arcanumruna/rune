"""Cluster topic specs -> Rune QL DDL (the brokers take no topic list; topics are created by DDL)."""
import pytest

from harness.cluster import DEFAULT_TOPICS, _request_id, topic_statement


def test_default_topics_are_the_former_compose_topics_in_id_order():
    assert [topic_statement(s) for s in DEFAULT_TOPICS] == [
        "topic events partitions 1;",
        "topic multi partitions 3;",
        "topic cg partitions 4;",
    ]


def test_transient_and_options_map_to_ddl():
    assert topic_statement("presence_failover:1:persistence=transient") == "transient topic presence_failover partitions 1;"
    assert topic_statement("t:2:rf=2:tier=leader:group=acct") == (
        "topic t partitions 2 replicas 2 with (tier = leader, copartition.group = 'acct');")


def test_positional_compaction_options_map_in_order_and_skip_empty_fields():
    assert topic_statement("c:1:compact:4096::-1:-1:1:1:0.0") == (
        "topic c partitions 1 with (cleanup.policy = compact, segment.bytes = 4096, retention.ms = -1, "
        "retention.bytes = -1, retention.check.ms = 1, file.delete.delay.ms = 1, min.cleanable.dirty.ratio = 0.0);")


@pytest.mark.parametrize("bad", ["noparts", "x:abc", "x:1:nosuchpolicy", "x:1:bogus=1", "x:1:persistence=weird",
                                 "x:1:compact:1:2:3:4:5:6:0.5:7:8", "bad name:1", "x:1:compact:abc"])
def test_an_option_without_a_ddl_equivalent_is_refused_never_dropped(bad):
    with pytest.raises(ValueError):
        topic_statement(bad)


def test_request_id_is_stable_nonzero_32_hex_with_room_for_statements():
    a = _request_id("topic a partitions 1;\n")
    assert a == _request_id("topic a partitions 1;\n") and a != _request_id("topic b partitions 1;\n")
    assert len(a) == 32 and int(a, 16) != 0 and a.endswith("0000")


# ---- Cluster.create_topics control flow (no Docker: a scripted fake `run` and a fake clock) ------------------------
import types

import harness.cluster as hc
from harness.dockerutil import CmdResult

OK3 = ("created topic 'events' topicId=0 version=1\n"
       "created topic 'multi' topicId=1 version=2\n"
       "created topic 'cg' topicId=2 version=3\n")
NOT_LEADER = "NotLeader: node rune-2 is not the catalog leader"


class _Script:
    """Scripted `docker run` outcomes (a CmdResult, or an exception instance to raise); +10 s per docker run."""

    def __init__(self, monkeypatch, outcomes):
        self.outcomes = list(outcomes)
        self.calls: list[list[str]] = []
        self.now = [0.0]
        monkeypatch.setenv("DR_QL_IMAGE", "rune-ql-probe:test")
        monkeypatch.setattr(hc, "run", self._run)
        monkeypatch.setattr(hc, "time", types.SimpleNamespace(time=lambda: self.now[0], sleep=lambda s: None))

    def _run(self, args, timeout=None, **kw):
        self.calls.append(list(args))
        if args[:3] == ["docker", "image", "inspect"] or args[0] == "bash":
            return CmdResult(args, 0, "", "")
        assert args[:2] == ["docker", "run"], args
        self.now[0] += 10
        out = self.outcomes.pop(0)
        if isinstance(out, BaseException):
            raise out
        return out

    def docker_runs(self):
        return [c for c in self.calls if c[:2] == ["docker", "run"]]


def _res(rc, text):
    return CmdResult([], rc, text, "")


def test_create_topics_retries_not_leader_then_succeeds_with_one_request_id(monkeypatch):
    s = _Script(monkeypatch, [_res(1, NOT_LEADER), _res(1, NOT_LEADER), _res(0, OK3)])
    c = hc.Cluster(project="t-ct")
    assert c.create_topics(hc.DEFAULT_TOPICS) == [("events", 0), ("multi", 1), ("cg", 2)]
    runs = s.docker_runs()
    assert len(runs) == 3
    rid = hc._request_id("\n".join(hc.topic_statement(t) for t in hc.DEFAULT_TOPICS) + "\n")
    assert all(f"--request-id {rid}" in r[-1] for r in runs)


def test_create_topics_non_retriable_error_fails_at_once_naming_the_statement(monkeypatch):
    s = _Script(monkeypatch, [_res(1, "statement 2: TopicExists (25)")])
    with pytest.raises(hc.TopicCreationError, match=r"topic multi partitions 3;"):
        hc.Cluster(project="t-ct").create_topics(hc.DEFAULT_TOPICS)
    assert len(s.docker_runs()) == 1


def test_create_topics_deadline_expires_with_an_explicit_error(monkeypatch):
    s = _Script(monkeypatch, [_res(1, "connect to rune-1:40200 timed out")] * 50)
    with pytest.raises(hc.TopicCreationError, match=r"not created within 30s"):
        hc.Cluster(project="t-ct").create_topics(hc.DEFAULT_TOPICS, deadline_s=30)
    assert len(s.docker_runs()) <= 4


def test_create_topics_retries_a_per_attempt_timeout(monkeypatch):
    _Script(monkeypatch, [TimeoutError("x"), _res(0, OK3)])
    assert hc.Cluster(project="t-ct").create_topics(hc.DEFAULT_TOPICS) == [("events", 0), ("multi", 1), ("cg", 2)]


def test_create_topics_refuses_a_surprise_topic_id(monkeypatch):
    _Script(monkeypatch, [_res(0, OK3.replace("'multi' topicId=1", "'multi' topicId=2")
                                  .replace("'cg' topicId=2", "'cg' topicId=1"))])
    with pytest.raises(hc.TopicCreationError, match=r"expected 1"):
        hc.Cluster(project="t-ct").create_topics(hc.DEFAULT_TOPICS)


def test_create_topics_refuses_a_missing_created_line(monkeypatch):
    _Script(monkeypatch, [_res(0, "created topic 'events' topicId=0 version=1\ncreated topic 'multi' topicId=1 version=2\n")])
    with pytest.raises(hc.TopicCreationError, match=r"2 of 3"):
        hc.Cluster(project="t-ct").create_topics(hc.DEFAULT_TOPICS)


def test_create_topics_always_rebuilds_the_cli_image(monkeypatch):
    s = _Script(monkeypatch, [_res(0, OK3)])
    hc.Cluster(project="t-ct").create_topics(hc.DEFAULT_TOPICS)
    builds = [c for c in s.calls if c[0] == "bash" and "--build-image" in c]
    assert len(builds) == 1, "the CLI image is rebuilt on every create_topics (a stale sha-tagged image must never run)"


def test_a_str_topics_argument_is_a_type_error_not_a_tuple_of_characters():
    with pytest.raises(TypeError):
        hc.Cluster(project="t", topics="events:1")
