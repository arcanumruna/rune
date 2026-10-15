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
import uuid
from dataclasses import dataclass

from . import timing


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

# D10-L (D2-10): every container the harness starts with `docker run` is labelled
#   rune.dr.owner=<harness pid>   — which harness process owns it (session sweep);
#   rune.dr.run=<uuid>            — this one invocation (timeout cleanup, named or not).
LABEL_OWNER = "rune.dr.owner"
LABEL_RUN = "rune.dr.run"


def _with_labels(args: list[str]) -> tuple[list[str], str | None]:
    """A `docker run` command with the owner + invocation labels; other commands unchanged."""
    if len(args) < 2 or args[0] != "docker" or args[1] != "run":
        return args, None
    tag = uuid.uuid4().hex
    return [args[0], args[1], "--label", f"{LABEL_OWNER}={os.getpid()}",
            "--label", f"{LABEL_RUN}={tag}", *args[2:]], tag


def _remove_labelled(label: str, timeout: int = 60) -> list[str]:
    """`docker rm -f` every container (running or not) carrying `label`; returns their ids."""
    ps = subprocess.run(["docker", "ps", "-aq", "--filter", f"label={label}"],
                        capture_output=True, text=True, timeout=timeout)
    ids = [c for c in ps.stdout.split() if c]
    for cid in ids:
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True, text=True, timeout=timeout)
    return ids


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True                    # exists, owned by someone else
    return True


def sweep_orphaned_containers(timeout: int = 60) -> list[str]:
    """Remove harness containers whose owning harness process is gone.

    pytest-timeout (timeout_method = thread) ends a hung session with os._exit:
    no `finally`, no TimeoutExpired — its client containers keep running (and
    producing) into the next session.  Called at session start (conftest).
    Containers of a LIVE harness process (a concurrent run) are never touched.
    Docker unavailable → nothing to do.  Returns the removed container ids.
    """
    try:
        ps = subprocess.run(["docker", "ps", "-a", "--filter", f"label={LABEL_OWNER}",
                             "--format", '{{.ID}} {{.Label "' + LABEL_OWNER + '"}}'],
                            capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError):
        return []
    removed: list[str] = []
    for line in ps.stdout.splitlines():
        parts = line.split()
        if not parts:
            continue
        owner = parts[1] if len(parts) > 1 else ""
        if owner.isdigit() and _pid_alive(int(owner)):
            continue
        subprocess.run(["docker", "rm", "-f", parts[0]], capture_output=True, text=True, timeout=timeout)
        removed.append(parts[0])
    return removed


class CommandTimeout(TimeoutError):
    """A command ran past its hard timeout (its container is removed); carries the output it printed so far."""

    def __init__(self, msg: str, *, stdout: str, stderr: str, timeout: float):
        super().__init__(msg)
        self.stdout, self.stderr, self.timeout = stdout, stderr, timeout


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


def _classify(args: list[str]) -> tuple[str, str]:
    """(timing kind, detail) of a command line — every docker call is timed under one kind."""
    if len(args) >= 2 and args[0] == "bash":
        return ("image.ql" if "--build-image" in args else "bash"), os.path.basename(args[1])
    if not args or args[0] != "docker":
        return "cmd", args[0] if args else ""
    if len(args) >= 2 and args[1] == "compose":
        verbs = ("up", "down", "exec", "restart", "stop", "start", "logs", "ps", "build", "kill", "pause", "unpause")
        verb = next((a for a in args[2:] if a in verbs), "other")
        tail = args[args.index(verb) + 1:] if verb in args else []
        return f"compose.{verb}", " ".join(tail[:4])
    if len(args) >= 2 and args[1] == "run":
        image = ""
        skip_next = False
        rest = args[2:]
        opts_with_arg = {"--name", "--network", "--hostname", "-e", "-v", "--label", "--platform",
                         "--security-opt", "--shm-size", "--entrypoint", "-w", "--user", "--cap-add"}
        for a in rest:
            if skip_next:
                skip_next = False
                continue
            if a in opts_with_arg:
                skip_next = True
                continue
            if a.startswith("-"):
                continue
            image = a
            break
        repo = image.split(":")[0]
        kind = {"rune-dr-client": "container.client", "rune-ql-probe": "container.ql",
                "rune-netem": "container.netem", "alpine": "container.volume"}.get(repo, "container.other")
        return kind, image
    if len(args) >= 2 and args[1] == "build":
        tag = args[args.index("-t") + 1] if "-t" in args else ""
        return "image.build", tag
    return f"docker.{args[1] if len(args) > 1 else ''}", " ".join(args[2:4])


def run(args: list[str], *, timeout: int = DEFAULT_TIMEOUT, check: bool = False,
        env: dict[str, str] | None = None) -> CmdResult:
    """Run a command with a hard timeout. Never blocks forever.

    ``env``: if supplied, the subprocess inherits this environment instead of
    the current process environment.  Pass ``os.environ.copy()`` with mutations
    to extend the current environment rather than replace it.
    """
    kind, detail = _classify(args)
    with timing.phase(kind, detail) as ph:
        res = _run(args, timeout=timeout, check=check, env=env)
        ph.exit = f"rc={res.returncode}"
        return res


def _run(args: list[str], *, timeout: int, check: bool, env: dict[str, str] | None) -> CmdResult:
    args, run_tag = _with_labels(args)
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
        # D10-L (D2-10): found by its invocation label — named or not (netem / volume sidecars are unnamed).
        if run_tag:
            try:
                _remove_labelled(f"{LABEL_RUN}={run_tag}")
            except (OSError, subprocess.SubprocessError):
                pass                   # best effort; the next session's sweep catches it
        def _text(part):
            return (part.decode(errors="replace") if isinstance(part, bytes) else part) or ""
        raise CommandTimeout(
            f"command timed out after {timeout}s: {' '.join(args)}\n"
            f"stdout so far:\n{exc.stdout}\nstderr so far:\n{exc.stderr}",
            stdout=_text(exc.stdout), stderr=_text(exc.stderr), timeout=timeout,
        ) from exc

    res = CmdResult(args=args, returncode=proc.returncode, stdout=proc.stdout, stderr=proc.stderr)
    if check and not res.ok:
        raise RuntimeError(
            f"command failed (rc={res.returncode}): {' '.join(args)}\n"
            f"stdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        )
    return res


@dataclass
class StreamResult(CmdResult):
    stopped_by_harness: bool = False   # the stop predicate fired and the container was removed


def run_until(args: list[str], *, timeout: int, stop_when, tick_s: float = 0.25) -> StreamResult:
    """`docker run ...` with its output streamed: `stop_when(lines, now)` is called on every new
    line and every `tick_s`; when it returns True the container is removed (`docker rm -f`) and
    the result has stopped_by_harness=True with every line read so far.  The command exiting on
    its own is an ordinary result; running past `timeout` raises TimeoutError exactly like
    `run()` (the container is removed first).  stdout and stderr are merged."""
    import threading
    import time as _time

    kind, detail = _classify(args)
    with timing.phase(kind, detail) as ph:
        args, run_tag = _with_labels(args)
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        lines: list[str] = []
        new_line = threading.Event()

        def _reader():
            assert proc.stdout is not None
            for line in proc.stdout:
                lines.append(line)
                new_line.set()

        reader = threading.Thread(target=_reader, daemon=True)
        reader.start()
        deadline = _time.time() + timeout
        stopped = False
        while proc.poll() is None:
            now = _time.time()
            if now >= deadline:
                if run_tag:
                    try:
                        _remove_labelled(f"{LABEL_RUN}={run_tag}")
                    except (OSError, subprocess.SubprocessError):
                        pass
                proc.kill()
                ph.exit = "timeout"
                raise CommandTimeout(f"command timed out after {timeout}s: {' '.join(args)}\n"
                                     f"output so far:\n{''.join(lines)}",
                                     stdout="".join(lines), stderr="", timeout=timeout)
            if stop_when(lines, now):
                stopped = True
                if run_tag:
                    _remove_labelled(f"{LABEL_RUN}={run_tag}")   # the container first: killing the CLI does not stop it
                proc.kill()
                break
            new_line.wait(tick_s)
            new_line.clear()
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=30)
        reader.join(timeout=30)
        ph.exit = "stopped" if stopped else f"rc={proc.returncode}"
        return StreamResult(args=args, returncode=0 if stopped else proc.returncode,
                            stdout="".join(lines), stderr="", stopped_by_harness=stopped)


class Compose:
    """Bound to a specific docker-compose.yml + project name.

    An optional list of override compose files can be supplied via
    ``override_files``.  Each override is appended as a second ``-f`` argument
    so Docker Compose merges them in order (later files override earlier ones).
    This is used by the compaction/retention e2e tests, which need to inject
    broker environment overrides via a compose override — without touching the
    canonical docker-compose.yml.

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
        # -t 1 when the volumes go too: nothing of a discarded cluster needs a graceful 10 s stop (it cost every
        # test ~5 s of teardown); kept volumes (DR_STALE_DATA) keep the default graceful stop.
        args = ["down", "--remove-orphans"] if keep_volumes else ["down", "-v", "--remove-orphans", "-t", "1"]
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
