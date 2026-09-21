"""Thin, bounded wrappers around the `docker` / `docker compose` CLIs.

Everything the harness does to Docker funnels through here so we get:
  * a single place that enforces hard timeouts (no unbounded subprocess waits),
  * consistent logging of the exact command run,
  * uniform error surfacing.

We deliberately shell out to the CLI (rather than the docker SDK) to match the
existing docker/e2e*.sh scripts and to keep the dependency surface tiny.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass


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


def run(args: list[str], *, timeout: int = DEFAULT_TIMEOUT, check: bool = False) -> CmdResult:
    """Run a command with a hard timeout. Never blocks forever."""
    try:
        proc = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:  # pragma: no cover - defensive
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
    """Bound to a specific docker-compose.yml + project name."""

    def __init__(self, compose_file: str, project: str):
        self.compose_file = compose_file
        self.project = project

    def _base(self) -> list[str]:
        return ["docker", "compose", "-f", self.compose_file, "-p", self.project]

    def up(self, *services: str, timeout: int = 180) -> CmdResult:
        return run(self._base() + ["up", "-d", *services], timeout=timeout, check=True)

    def build(self, *services: str, timeout: int = 600) -> CmdResult:
        return run(self._base() + ["build", *services], timeout=timeout, check=True)

    def down(self, timeout: int = 120) -> CmdResult:
        # -v removes named volumes (fresh state), --remove-orphans cleans stragglers.
        return run(self._base() + ["down", "-v", "--remove-orphans"], timeout=timeout)

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
