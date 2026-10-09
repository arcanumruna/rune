"""Unit tests of harness.dockerutil's container cleanup — no Docker needed.

D10-L (D2-10): a client container whose `docker run` exceeds its timeout must be
removed (killing the `docker run` CLI does not stop the container — it kept
producing into the next test), named or not.  And a container left behind by a
harness process that died without cleanup (pytest-timeout's thread method ends
the process with os._exit: no `finally`, no TimeoutExpired) is swept by the next
session.  `subprocess.run` is faked: every docker CLI call is recorded.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from harness import dockerutil


class FakeDocker:
    """Records docker CLI calls; `docker run` times out, `docker ps` lists `ps_lines`."""

    def __init__(self, ps_lines: list[str]):
        self.calls: list[list[str]] = []
        self.ps_lines = ps_lines

    def __call__(self, args, **kwargs):
        self.calls.append(list(args))
        if args[:2] == ["docker", "run"]:
            raise subprocess.TimeoutExpired(args, kwargs.get("timeout", 0), output=b"partial", stderr=b"")
        out = "\n".join(self.ps_lines) + "\n" if args[:2] == ["docker", "ps"] else ""
        return subprocess.CompletedProcess(args, 0, stdout=out, stderr="")

    def removed(self) -> list[str]:
        return [c[3] for c in self.calls if c[:3] == ["docker", "rm", "-f"]]

    def run_label(self) -> str:
        run = next(c for c in self.calls if c[:2] == ["docker", "run"])
        return next(a.split("=", 1)[1] for a in run if a.startswith(dockerutil.LABEL_RUN + "="))


@pytest.mark.parametrize("named", [True, False])
def test_timed_out_docker_run_removes_its_container_regression(monkeypatch, named):
    fake = FakeDocker(ps_lines=["c0ffee"])
    monkeypatch.setattr(dockerutil.subprocess, "run", fake)
    args = ["docker", "run", "--rm"] + (["--name", "dr-client-1-abc"] if named else []) + ["img"]
    with pytest.raises(TimeoutError):
        dockerutil.run(args, timeout=1)
    run_call = fake.calls[0]
    assert run_call[-1] == "img" and "--rm" in run_call, "the command itself is unchanged"
    assert f"{dockerutil.LABEL_OWNER}={os.getpid()}" in run_call, "owned by this harness process"
    ps = next(c for c in fake.calls if c[:2] == ["docker", "ps"])
    assert f"label={dockerutil.LABEL_RUN}={fake.run_label()}" in ps, "found by ITS invocation label"
    assert "c0ffee" in fake.removed(), "the timed-out container is removed, named or not"


def test_other_commands_are_not_labelled(monkeypatch):
    seen: list[list[str]] = []

    def ok(args, **kwargs):
        seen.append(list(args))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(dockerutil.subprocess, "run", ok)
    dockerutil.run(["docker", "compose", "-f", "x.yml", "ps"])
    assert seen == [["docker", "compose", "-f", "x.yml", "ps"]]


def test_sweep_removes_only_containers_of_dead_harness_processes_regression(monkeypatch):
    dead = 0x7FFFFFF0                                   # no such pid
    fake = FakeDocker(ps_lines=[f"aaa {dead}", f"bbb {os.getpid()}", "ccc notapid", ""])
    monkeypatch.setattr(dockerutil.subprocess, "run", fake)
    assert dockerutil.sweep_orphaned_containers() == ["aaa", "ccc"]
    assert fake.removed() == ["aaa", "ccc"], "a live harness's containers are never touched"


def test_sweep_tolerates_docker_being_down(monkeypatch):
    def down(args, **kwargs):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(dockerutil.subprocess, "run", down)
    assert dockerutil.sweep_orphaned_containers() == []


@pytest.mark.parametrize("args,name", [
    (["docker", "run", "--name", "n1", "img"], "n1"),
    (["docker", "run", "--name=n2", "img"], "n2"),
    (["docker", "run", "img"], None),
    (["docker", "compose", "run", "--name", "x"], None),
])
def test_run_container_name(args, name):
    assert dockerutil._run_container_name(args) == name
