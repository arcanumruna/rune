"""Review B-M6 (DR-bugs fix round 1): the harness never labels an image with a sha its code is not.  No docker."""
from __future__ import annotations

import subprocess

import pytest

from harness import driver
from harness.dockerutil import _git_sha


def _repo(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q",
                    "--allow-empty", "-m", "init"], check=True)
    (tmp_path / "f.txt").write_text("a\n")
    subprocess.run(["git", "-C", str(tmp_path), "add", "f.txt"], check=True)
    subprocess.run(["git", "-C", str(tmp_path), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q",
                    "-m", "f"], check=True)
    return tmp_path


def test_git_sha_marks_a_dirty_tree(tmp_path):
    repo = _repo(tmp_path)
    clean = _git_sha(str(repo))
    assert clean and not clean.endswith("-dirty")
    (repo / "f.txt").write_text("b\n")
    assert _git_sha(str(repo)) == clean + "-dirty"
    (repo / "untracked.txt").write_text("x\n")   # untracked files never make it dirty
    (repo / "f.txt").write_text("a\n")
    assert _git_sha(str(repo)) == clean


def test_dirty_client_tree_refuses_the_image_build(tmp_path, monkeypatch):
    repo = _repo(tmp_path)
    (repo / "f.txt").write_text("b\n")
    monkeypatch.setattr(driver, "CLIENT_REPO", str(repo))
    monkeypatch.setattr(driver, "_CLIENT_SHA", _git_sha(str(repo)))
    monkeypatch.delenv("DR_CLIENT_IMAGE", raising=False)
    monkeypatch.delenv("DR_ALLOW_DIRTY_CLIENT", raising=False)
    calls = []
    monkeypatch.setattr(driver, "run", lambda *a, **k: calls.append(a))
    with pytest.raises(RuntimeError, match="uncommitted changes"):
        driver._ensure_dr_client_image()
    assert not calls, "no docker build may start from a dirty tree"
