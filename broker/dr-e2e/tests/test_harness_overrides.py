"""RUNE_DR_COMPOSE_OVERRIDES appends compose overlays (shard-mode DR matrix)."""
import os

from harness.cluster import Cluster


def test_env_overrides_are_appended_after_explicit_ones(monkeypatch):
    monkeypatch.setenv("RUNE_DR_COMPOSE_OVERRIDES", os.pathsep.join(["/x/a.yml", "/x/b.yml"]))
    c = Cluster(project="t-ovr", override_files=["/x/c.yml"])
    assert c.compose._override_files == ["/x/c.yml", "/x/a.yml", "/x/b.yml"]


def test_no_env_means_no_extra_overrides(monkeypatch):
    monkeypatch.delenv("RUNE_DR_COMPOSE_OVERRIDES", raising=False)
    c = Cluster(project="t-ovr2")
    assert c.compose._override_files == []
