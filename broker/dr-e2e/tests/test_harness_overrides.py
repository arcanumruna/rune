"""RUNE_DR_COMPOSE_OVERRIDES appends compose overlays (shard-mode DR matrix)."""
import os

from harness.cluster import Cluster


def test_env_overlays_go_before_test_overrides_so_tests_win(monkeypatch):
    monkeypatch.setenv("RUNE_DR_COMPOSE_OVERRIDES", os.pathsep.join(["/x/a.yml", "/x/b.yml"]))
    c = Cluster(project="t-ovr", override_files=["/x/c.yml"])
    assert c.compose._override_files == ["/x/a.yml", "/x/b.yml", "/x/c.yml"]


def test_default_config_baseline_opts_out_of_global_overlays(monkeypatch):
    # Regression: the shard overlay silently turned default-OFF baseline arms
    # (test_per_worker_ingress default_cluster) into non-default runs.
    monkeypatch.setenv("RUNE_DR_COMPOSE_OVERRIDES", "/x/shard.yml")
    c = Cluster(project="t-ovr3", global_overlays=False)
    assert c.compose._override_files == []


def test_no_env_means_no_extra_overrides(monkeypatch):
    monkeypatch.delenv("RUNE_DR_COMPOSE_OVERRIDES", raising=False)
    c = Cluster(project="t-ovr2")
    assert c.compose._override_files == []
