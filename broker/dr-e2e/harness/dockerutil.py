"""Thin, bounded wrappers around the `docker` / `docker compose` CLIs.

Everything the harness does to Docker funnels through here so we get:
  * a single place that enforces hard timeouts (no unbounded subprocess waits),
  * consistent logging of the exact command run,
  * uniform error surfacing.

We deliberately shell out to the CLI (rather than the docker SDK) to match the
existing docker/e2e*.sh scripts and to keep the dependency surface tiny.

IMAGE TAG POLICY
----------------
The harness sets RUNE_IMAGE_TAG to the short git HEAD SHA before every
``docker compose build``.  The docker-compose.yml uses that variable to tag
the broker image as ``rune-broker-node:<sha>`` (falling back to ``latest``
when the variable is absent, e.g. manual interactive use).  A commit-specific
tag means:
  * Each commit's layers live under a unique name in the containerd snapshot
    store — no cross-commit layer collision.
  * Parallel test runs from different commits never stomp each other's images.
  * Layer sharing still works within a single commit (all three services
    reference the same ``rune-broker-node:<sha>`` image).
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass


def _git_sha(repo_root: str) -> str:
    """Return the short HEAD SHA of the repo at *repo_root* (7 hex chars).

    Falls back to ``"local"`` if git is unavailable or the directory is not a
    git repo — so the harness still works in bare-binary CI environments.
    """
    try:
        result = subprocess.run(
            ["git", "-C", repo_root, "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=10,
        )
        sha = result.stdout.strip()
        return sha if sha else "local"
    except Exception:
        return "local"


DEFAULT_TIMEOUT = 120  # seconds — generous but hard cap for any single CLI call


@dataclass
class CmdResult:
    args: list[str]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def _run_container_name(args: list[str]) -> str | None:
    """The --name of a `docker run ...` command line, else None."""
    if len(args) < 2 or args[0] != "docker" or args[1] != "run":
        return None
    for i, a in enumerate(args):
        if a == "--name" and i + 1 < len(args):
            return args[i + 1]
        if a.startswith("--name="):
            return a.split("=", 1)[1]
    return None


def run(args: list[str], *, timeout: int = DEFAULT_TIMEOUT, check: bool = False,
        env: dict[str, str] | None = None) -> CmdResult:
    """Run a command with a hard timeout. Never blocks forever.

    ``env``: if supplied, the subprocess inherits this environment instead of
    the current process environment.  Pass ``os.environ.copy()`` with mutations
    to extend the current environment rather than replace it.
    """
    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as exc:  # pragma: no cover - defensive
        # Killing the `docker run` CLI does not stop its container: remove a
        # named one so it cannot keep producing into the next test (DR-15 phantoms).
        name = _run_container_name(args)
        log_dir = os.environ.get("DR_CLIENT_LOG_DIR")
        if name and log_dir:  # keep the full partial output (the exception text gets truncated)
            os.makedirs(log_dir, exist_ok=True)
            with open(os.path.join(log_dir, f"TIMEOUT-{name}.log"), "w") as fh:
                for part in (exc.stdout, exc.stderr):
                    if part:
                        fh.write(part.decode(errors="replace") if isinstance(part, bytes) else part)
        if name:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, text=True, timeout=60)
        raise TimeoutError(
            f"command timed out after {timeout}s: {' '.join(args)}\n"
            f"stdout so far:\n{exc.stdout}\nstderr so far:\n{exc.stderr}"
        ) from exc

    res = CmdResult(args=args, returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)
    if check and not res.ok:
        raise RuntimeError(
            f"command failed (rc={res.returncode}): {' '.join(args)}\n"
            f"stdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        )
    return res


class Compose:
    """Bound to a specific docker-compose.yml + project name.

    An optional list of override compose files can be supplied via
    ``override_files``.  Each override is appended as a second ``-f`` argument
    so Docker Compose merges them in order (later files override earlier ones).
    This is used by the compaction/retention e2e tests, which need to inject
    RUNE_TOPICS with compact/delete policy and small segment sizes via a
    compose override — without touching the canonical docker-compose.yml.

    ``image_tag``: injected as ``RUNE_IMAGE_TAG`` into every ``build``/``up``
    call so the broker image is tagged ``rune-broker-node:<tag>`` rather than
    the project-scoped ``<project>-rune-{1,2,3}:latest``.  When ``None`` the
    current process environment is passed unchanged (backward-compat / manual
    use where the caller exported ``RUNE_IMAGE_TAG`` already).
    """

    def __init__(self, compose_file: str, project: str,
                 override_files: list[str] | None = None,
                 image_tag: str | None = None):
        self.compose_file = compose_file
        self.project = project
        self._override_files: list[str] = list(override_files or [])
        self._image_tag: str | None = image_tag

    def _base(self) -> list[str]:
        args = ["docker", "compose", "-f", self.compose_file]
        for ov in self._override_files:
            args += ["-f", ov]
        args += ["-p", self.project]
        return args

    def _env(self) -> dict[str, str] | None:
        """Return an env dict with RUNE_IMAGE_TAG injected, or None to inherit."""
        if self._image_tag is None:
            return None
        env = os.environ.copy()
        env["RUNE_IMAGE_TAG"] = self._image_tag
        return env

    def up(self, *services: str, timeout: int = 180) -> CmdResult:
        return run(self._base() + ["up", "-d", *services],
                   timeout=timeout, check=True, env=self._env())

    def build(self, *services: str, timeout: int = 600) -> CmdResult:
        return run(self._base() + ["build", *services],
                   timeout=timeout, check=True, env=self._env())

    def down(self, timeout: int = 120, keep_volumes: bool = False) -> CmdResult:
        # -v removes named volumes (fresh state), --remove-orphans cleans stragglers.
        # keep_volumes: the containers go, the logs stay (DR_STALE_DATA restarts on them).
        args = ["down", "--remove-orphans"] if keep_volumes else ["down", "-v", "--remove-orphans"]
        return run(self._base() + args, timeout=timeout)

    def stop(self, service: str, timeout: int = 60) -> CmdResult:
        return run(self._base() + ["stop", service], timeout=timeout)

    def start(self, service: str, timeout: int = 60) -> CmdResult:
        return run(self._base() + ["start", service], timeout=timeout, check=True)

    def restart(self, service: str, timeout: int = 90) -> CmdResult:
        return run(self._base() + ["restart", service], timeout=timeout, check=True)

    def exec(self, service: str, *cmd: str, timeout: int = 30) -> CmdResult:
        """Run `cmd` inside a running service container (docker compose exec -T).

        -T disables TTY allocation so the call is non-interactive and safe to
        capture.  Returns the CmdResult (returncode + stdout + stderr); the
        caller decides how to interpret a non-zero rc (e.g. rune_role exits 1/2
        when the CnC counter isn't ready yet).
        """
        args = self._base() + ["exec", "-T", service, *cmd]
        return run(args, timeout=timeout)

    def logs(self, *services: str, tail: int | None = None, timeout: int = 30) -> str:
        args = self._base() + ["logs", "--no-log-prefix"]
        if tail is not None:
            args += ["--tail", str(tail)]
        args += list(services)
        return run(args, timeout=timeout).stdout

    def ps(self, timeout: int = 30) -> str:
        return run(self._base() + ["ps"], timeout=timeout).stdout

    def container_id(self, service: str, timeout: int = 30) -> str | None:
        res = run(self._base() + ["ps", "-q", service], timeout=timeout)
        cid = res.stdout.strip().splitlines()
        return cid[0] if cid else None

    def network_name(self) -> str:
        # Compose's default network is "<project>_default".
        return f"{self.project}_default"
