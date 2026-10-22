"""Authentication for the dr-e2e harness (authentication Task 20).

Every broker now runs with authentication ON (spec §9, D31): a cluster refuses to start without a cluster key file,
and every client needs credentials.  This module owns the per-process secrets of the harness:

    state()          a private temp dir: secrets/cluster.key (RUNE_SECRETS_DIR, the compose mount) and work/ (passwords)
    ensure_key()     the cluster key (`rune security gen-cluster-key`, once per process), the broker TLS certificate
                     (tls.pem / tls.key) and its ca.pem (docker/lib/gen-tls.sh - issue #10: an authenticating broker
                     serves only TLS sessions), and RUNE_SECRETS_DIR in the environment, so every `docker compose` call
                     mounts them
    bootstrap(net)   on a fresh cluster with a leader: bootstrap the first admin, change its one-time password, create
                     the `e2e` role + user (READ/WRITE/CREATE/ALTER/DROP on every topic, READ/CREATE/ALTER/DROP on
                     every view, READ on every group, WRITE on every transactional id, DESCRIBE ON CLUSTER)
    docker_args()    `-e RUNE_USER ... -e RUNE_PASSWORD_FILE ... -e RUNE_TLS_CA_FILE ... -v <work>:/work:ro` for a client
                     container (credentials travel only over TLS, issue #14)
    cli_env()        the same as a dict (RUNE_USER / RUNE_PASSWORD_FILE / RUNE_TLS_CA_FILE) for a container that already
                     mounts /work

One key and one set of passwords serve every cluster of the process: each cluster is fresh (`down -v` between tests)
and is bootstrapped with the same files.  Nothing is written outside the temp dir; it is removed at exit.
"""

from __future__ import annotations

import atexit
import os
import re
import secrets
import shutil
import string
import tempfile
import time
from dataclasses import dataclass

from .dockerutil import run

ADMIN = "admin"
E2E_USER = "e2e"
_RETRIABLE_RE = re.compile(r"NotLeader|Redirect|timed out|connection refused|not connected|unreachable|not ready|"
                           r"no leader|Unavailable", re.I)
_ALREADY_RE = re.compile(r"already bootstrapped|already has a security admin", re.I)
_BOOTSTRAP_DEADLINE_S = float(os.environ.get("DR_BOOTSTRAP_DEADLINE_S", "150"))
_CLI_TIMEOUT_S = 120


@dataclass(frozen=True)
class AuthState:
    root: str
    secrets_dir: str
    work_dir: str


_STATE: AuthState | None = None
_BOOTSTRAPPED: set[str] = set()


def _password() -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(24))


def _write_private(path: str, text: str) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(text)


def state() -> AuthState:
    """The per-process secrets directory (created on first use, removed at exit)."""
    global _STATE
    if _STATE is None:
        root = tempfile.mkdtemp(prefix="rune-dr-auth-")
        os.chmod(root, 0o755)   # the containers (root) read it; the files themselves are 0600
        secrets_dir = os.path.join(root, "secrets")
        work_dir = os.path.join(root, "work")
        os.makedirs(secrets_dir)
        os.makedirs(work_dir)
        for name in ("otp", "admin.pw", "e2e.pw"):
            _write_private(os.path.join(work_dir, name), _password() + "\n")
        _STATE = AuthState(root, secrets_dir, work_dir)
        atexit.register(shutil.rmtree, root, True)
    return _STATE


def _cli_base(network: str | None) -> list[str]:
    from .cluster import ql_image
    st = state()
    args = ["docker", "run", "--rm", "--shm-size=256m", "--security-opt", "seccomp=unconfined",
            "--platform", "linux/amd64", "-v", f"{st.work_dir}:/work", "-v", f"{st.secrets_dir}:/run/secrets:ro",
            "-e", "RUNE_TLS_CA_FILE=/run/secrets/ca.pem"]
    if network:
        args += ["--network", network]
    return args + [ql_image()]


def ensure_key() -> str:
    """Generate the cluster key once per process (`rune security gen-cluster-key`) and export RUNE_SECRETS_DIR."""
    from .cluster import ensure_ql_image, ql_image
    st = state()
    key = os.path.join(st.secrets_dir, "cluster.key")
    if not os.path.exists(key):
        ensure_ql_image()
        res = run(["docker", "run", "--rm", "--platform", "linux/amd64", "-v", f"{st.secrets_dir}:/out", ql_image(),
                   "rune", "security", "gen-cluster-key", "--out", "/out/cluster.key"], timeout=120)
        if not res.ok or not os.path.exists(key):
            raise RuntimeError(f"rune security gen-cluster-key failed:\n{res.stdout}{res.stderr}")
    if not os.path.exists(os.path.join(st.secrets_dir, "tls.pem")):
        gen = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "docker", "lib", "gen-tls.sh")
        res = run(["bash", gen, st.secrets_dir], timeout=60)
        if not res.ok:
            raise RuntimeError(f"gen-tls.sh failed:\n{res.stdout}{res.stderr}")
        shutil.copyfile(os.path.join(st.secrets_dir, "ca.pem"), os.path.join(st.work_dir, "ca.pem"))
    os.environ["RUNE_SECRETS_DIR"] = st.secrets_dir
    return key


def _cli(network: str, *args: str, timeout: int = _CLI_TIMEOUT_S):
    return run(_cli_base(network) + ["rune", *args], timeout=timeout)


def _as(user: str, pwfile: str, network: str, *args: str):
    return _cli(network, "ql", "--user", user, "--password-file", pwfile, *args)


def docker_args() -> list[str]:
    """`docker run` arguments that log a client container in as the e2e user (empty-safe: the state always exists)."""
    st = state()
    return ["-e", f"RUNE_USER={E2E_USER}", "-e", "RUNE_PASSWORD_FILE=/work/e2e.pw", "-e", "RUNE_TLS_CA_FILE=/work/ca.pem",
            "-v", f"{st.work_dir}:/work:ro"]


def cli_env() -> dict[str, str]:
    return {"RUNE_USER": E2E_USER, "RUNE_PASSWORD_FILE": "/work/e2e.pw", "RUNE_TLS_CA_FILE": "/work/ca.pem"}


def bootstrap(network: str, seed: str, *, deadline_s: float | None = None) -> None:
    """Bootstrap a fresh cluster (idempotent per network): first admin, its password change, the e2e role + user."""
    if network in _BOOTSTRAPPED:
        return
    st = state()
    broker = seed
    deadline = time.time() + (deadline_s if deadline_s is not None else _BOOTSTRAP_DEADLINE_S)
    last = "(no attempt ran)"
    while True:
        res = _cli(network, "security", "bootstrap", "--admin", ADMIN, "--password-file", "/work/otp",
                   "--cluster-key-file", "/run/secrets/cluster.key", "--broker", broker)
        out = (res.stdout or "") + (res.stderr or "")
        if res.ok:
            break
        if _ALREADY_RE.search(out):
            raise RuntimeError(f"bootstrap of {network}: the cluster is already bootstrapped:\n{out}")
        last = out.strip()[-600:]
        if time.time() >= deadline or not _RETRIABLE_RE.search(out):
            raise RuntimeError(f"bootstrap of {network} failed (rc={res.returncode}):\n{last}")
        time.sleep(1.0)

    with open(os.path.join(st.work_dir, "admin.pw")) as f:
        admin_pw = f.read().strip()
    with open(os.path.join(st.work_dir, "e2e.pw")) as f:
        e2e_pw = f.read().strip()

    def script(name: str, text: str) -> str:
        path = os.path.join(st.work_dir, name)
        _write_private(path, text)
        return f"/work/{name}"

    try:
        # The password literal travels in a file: the CLI refuses one on the command line.
        alter = script("alter-admin.rql", f"ALTER USER {ADMIN} SET PASSWORD '{admin_pw}';\n")
        res = _as(ADMIN, "/work/otp", network, "-f", alter, "--broker", broker)
        if not res.ok:
            raise RuntimeError(f"ALTER USER {ADMIN} SET PASSWORD failed on {network}:\n{res.stdout}{res.stderr}")
        lines = ["CREATE ROLE e2e;"]
        lines += [f"GRANT {op} ON TOPIC PREFIX '' TO ROLE e2e;" for op in ("READ", "WRITE", "CREATE", "ALTER", "DROP")]
        lines += [f"GRANT {op} ON VIEW PREFIX '' TO ROLE e2e;" for op in ("READ", "CREATE", "ALTER", "DROP")]
        lines += ["GRANT READ ON GROUP PREFIX '' TO ROLE e2e;",
                  "GRANT WRITE ON TRANSACTIONAL_ID PREFIX '' TO ROLE e2e;",
                  "GRANT DESCRIBE ON CLUSTER TO ROLE e2e;",
                  f"CREATE USER {E2E_USER} WITH PASSWORD '{e2e_pw}';",
                  f"GRANT ROLE e2e TO USER {E2E_USER};"]
        create = script("create-e2e-user.rql", "\n".join(lines) + "\n")
        res = _as(ADMIN, "/work/admin.pw", network, "-f", create, "--broker", broker)
        if not res.ok:
            raise RuntimeError(f"creating the e2e role/user failed on {network}:\n{res.stdout}{res.stderr}")
    finally:
        for name in ("alter-admin.rql", "create-e2e-user.rql"):
            try:
                os.unlink(os.path.join(st.work_dir, name))
            except FileNotFoundError:
                pass
    _BOOTSTRAPPED.add(network)


def forget(network: str) -> None:
    """The cluster on `network` was torn down: the next one under the same name needs a bootstrap again."""
    _BOOTSTRAPPED.discard(network)
