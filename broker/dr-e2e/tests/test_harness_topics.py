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
