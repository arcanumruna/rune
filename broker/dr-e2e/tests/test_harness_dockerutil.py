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


# ---- ensure_dr_client_image: the dr client image is tied to the client checkout (no Docker) -------------------------
import importlib

from harness import driver
from harness.dockerutil import CmdResult

_SHA = "def4567"


@pytest.fixture
def drv(monkeypatch):
    """driver reloaded under a controlled environment and a fixed client sha; reloaded again afterwards."""
    monkeypatch.setattr(dockerutil, "_git_sha", lambda repo: _SHA)

    def load(**env):
        for k in ("DR_CLIENT_IMAGE", "DR_ALLOW_STALE_CLIENT_IMAGE", "DR_IMAGES_PREPARED"):
            monkeypatch.delenv(k, raising=False)
        for k, v in env.items():
            monkeypatch.setenv(k, v)
        return importlib.reload(driver)

    yield load
    monkeypatch.undo()
    importlib.reload(driver)


class _Runs:
    def __init__(self, label=None, build_rc=0):
        self.label, self.build_rc, self.calls, self.kw = label, build_rc, [], []

    def __call__(self, args, *, timeout=120, check=False, env=None):
        self.calls.append(list(args))
        self.kw.append({"check": check, "timeout": timeout})
        if args[:3] == ["docker", "image", "inspect"]:
            return CmdResult(args, 0 if self.label is not None else 1, (self.label or "") + "\n", "")
        res = CmdResult(args, self.build_rc, "", "boom")
        if check and not res.ok:
            raise RuntimeError(f"command failed (rc={res.returncode})")
        return res

    def builds(self):
        return [c for c in self.calls if c[:2] == ["docker", "build"]]


def test_a_prebuilt_client_image_from_another_sha_is_refused(drv, monkeypatch):
    d = drv(DR_CLIENT_IMAGE="my-dr:x")
    r = _Runs(label="abc1234")
    monkeypatch.setattr(d, "run", r)
    with pytest.raises(RuntimeError, match="stale dr client image"):
        d.ensure_dr_client_image()
    assert not r.builds()


def test_a_prebuilt_client_image_with_the_current_sha_is_accepted_without_a_build(drv, monkeypatch):
    d = drv(DR_CLIENT_IMAGE="my-dr:x")
    r = _Runs(label=_SHA)
    monkeypatch.setattr(d, "run", r)
    d.ensure_dr_client_image()
    assert not r.builds()


def test_a_stale_prebuilt_image_is_accepted_only_by_explicit_opt_in(drv, monkeypatch, capsys):
    d = drv(DR_CLIENT_IMAGE="my-dr:x", DR_ALLOW_STALE_CLIENT_IMAGE="1")
    monkeypatch.setattr(d, "run", _Runs(label=""))
    d.ensure_dr_client_image()
    assert "DR_ALLOW_STALE_CLIENT_IMAGE=1" in capsys.readouterr().out


def test_a_missing_prebuilt_image_is_an_error(drv, monkeypatch):
    d = drv(DR_CLIENT_IMAGE="my-dr:x")
    monkeypatch.setattr(d, "run", _Runs(label=None))
    with pytest.raises(RuntimeError, match="does not exist"):
        d.ensure_dr_client_image()


def test_without_an_image_override_the_harness_builds_it_with_all_five_contexts(drv, monkeypatch):
    d = drv()
    r = _Runs()
    monkeypatch.setattr(d, "run", r)
    d.ensure_dr_client_image()
    (build,) = r.builds()
    sib = os.path.dirname(d.CLIENT_REPO)
    for name in ("rune-protocol", "flight-recorder", "rune-codec", "rune-ql", "rune-crypto"):
        i = build.index(f"{name}={os.path.join(sib, name)}")
        assert build[i - 1] == "--build-context"
    assert ["--build-arg", f"RUNE_CLIENT_SHA={_SHA}"] == build[build.index("--build-arg"):build.index("--build-arg") + 2]
    assert build[build.index("-t") + 1] == f"rune-dr-client:{_SHA}" == d.DR_IMAGE
    assert r.kw[r.calls.index(build)]["check"] is True


def test_a_failed_client_image_build_raises(drv, monkeypatch):
    d = drv()
    monkeypatch.setattr(d, "run", _Runs(build_rc=1))
    with pytest.raises(RuntimeError):
        d.ensure_dr_client_image()


def test_the_client_image_is_built_once_per_process_not_per_cluster_up(drv, monkeypatch):
    d = drv()
    r = _Runs()
    monkeypatch.setattr(d, "run", r)
    d.ensure_dr_client_image()
    d.ensure_dr_client_image()
    assert len(r.builds()) == 1


def test_the_client_image_build_is_skipped_when_the_session_prepared_it(drv, monkeypatch):
    d = drv(DR_IMAGES_PREPARED="1")
    r = _Runs()
    monkeypatch.setattr(d, "run", r)
    d.ensure_dr_client_image()
    assert not r.calls


# ---- drain stop: a verification read of a static log ends once it is complete ---------------------------------

def test_drain_stops_only_after_every_partition_delivered_and_the_quiet_period():
    stop = driver._drain_complete(5.0, 0, 3)
    lines = ["CLIENT: connected\n"]
    assert not stop(lines, 0.0)
    assert not stop(lines, 100.0), "nothing delivered: the full window is kept"
    lines += ["DELIVERED cid=1 key=1 seq=0 part=0 offset=0\n", "DELIVERED cid=1 key=2 seq=0 part=1 offset=0\n"]
    assert not stop(lines, 101.0)
    assert not stop(lines, 200.0), "partition 2 never delivered: the full window is kept"
    lines += ["DELIVERED cid=1 key=3 seq=0 part=2 offset=0\n"]
    assert not stop(lines, 201.0)
    assert not stop(lines, 205.9)
    assert stop(lines, 206.0)


def test_drain_quiet_period_restarts_on_every_record():
    stop = driver._drain_complete(5.0, 0, 1)
    lines = ["DELIVERED cid=1 key=1 seq=0 part=0 offset=0\n"]
    assert not stop(lines, 0.0)
    lines.append("DELIVERED cid=1 key=1 seq=1 part=0 offset=1\n")
    assert not stop(lines, 4.0)
    assert not stop(lines, 8.9)
    assert stop(lines, 9.0)


def test_run_until_stops_the_container_and_reports_it(monkeypatch):
    removed = []
    monkeypatch.setattr(dockerutil, "_remove_labelled", lambda label, timeout=60: removed.append(label) or [])
    seen = []

    def stop_when(lines, now):
        seen.extend(lines[len(seen):])
        return any("tick 3" in l for l in seen)

    res = dockerutil.run_until(["bash", "-c", "for i in 1 2 3 4 5 6 7 8 9; do echo tick $i; sleep 1; done"],
                               timeout=30, stop_when=stop_when)
    assert "tick 3" in res.stdout and "tick 9" not in res.stdout
    assert res.ok   # bash is not a `docker run`: no label to remove, but the process is stopped


# ---- a hung client keeps its acked records (never an empty, vacuously-passing result) -------------------------

def test_a_hung_client_returns_its_partial_acks_as_a_failed_result(monkeypatch):
    out = ("PRODUCED cid=7 key=1 seq=0 part=0 offset=0\nPRODUCED cid=7 key=1 seq=1 part=0 offset=1\n")

    def hang(args, *, timeout=120, check=False, env=None):
        raise dockerutil.CommandTimeout("timed out", stdout=out, stderr="", timeout=timeout)

    monkeypatch.setattr(driver, "run", hang)
    monkeypatch.setattr(driver, "run_until", lambda args, *, timeout, stop_when, **kw: hang(args, timeout=timeout))
    with pytest.warns(driver.ClientHangWarning):
        res = driver.run_clients(1, network="n", target_service="rune-1", cid_base=7, mode="produce", timeout_s=5)
    assert not res.ok
    assert [m.seq for m in res.all_produced] == [0, 1]
    assert res.clients[0].exit_code == 124 and "HANG" in res.clients[0].raw


def test_run_raises_command_timeout_with_the_partial_output():
    with pytest.raises(dockerutil.CommandTimeout) as ei:
        dockerutil.run(["bash", "-c", "echo before; sleep 5"], timeout=1)
    assert "before" in ei.value.stdout
    assert isinstance(ei.value, TimeoutError)


def test_producer_stall_needs_two_deadlines_without_an_ack():
    stop = driver._producer_stalled(90.0)
    lines = ["CLIENT: connecting...\n"]
    assert not stop(lines, 0.0)
    assert not stop(lines, 89.0)
    lines.append("PRODUCED cid=1 key=0 seq=0 part=0 offset=0\n")
    assert not stop(lines, 89.5), "an ack restarts the stall clock"
    lines.append("CLIENT: FAIL produce cid=1 seq=1 TIMEOUT\n")
    assert not stop(lines, 150.0), "a failure line is not progress, but the clock started at the last ack"
    assert stop(lines, 179.5)


def test_a_stalled_producer_is_a_failed_result_with_its_acks(monkeypatch):
    out = "PRODUCED cid=3 key=0 seq=0 part=0 offset=0\nCLIENT: FAIL produce cid=3 seq=1 TIMEOUT\n"
    monkeypatch.setattr(driver, "run_until", lambda args, *, timeout, stop_when, **kw: dockerutil.StreamResult(
        args=args, returncode=0, stdout=out, stderr="", stopped_by_harness=True))
    with pytest.warns(driver.ClientHangWarning):
        res = driver.run_clients(1, network="n", target_service="rune-1", cid_base=3, mode="produce", timeout_s=5)
    assert not res.ok and res.clients[0].exit_code == 124 and "STALL" in res.clients[0].raw
    assert [m.seq for m in res.all_produced] == [0]


def test_a_rate_limited_producer_gets_a_rate_derived_envelope():
    env = {"RUNE_RATE_HZ": "40", "RUNE_RECORDS": "200", "RUNE_TIMEOUT_MS": "40000"}
    assert driver._producer_envelope(env, 240) == int(200 / 40 + 40 + driver.RATE_ENVELOPE_GRACE_S) + 1
    assert driver._producer_envelope(env, 60) == 61, "the caller's timeout_s still caps it"
    assert driver._producer_envelope({**env, "RUNE_RATE_HZ": "0"}, 240) == 240, "unlimited producers keep timeout_s"
