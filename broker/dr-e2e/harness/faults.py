"""Fault-injection helpers for the DR harness.

Two classes of fault:

  * Process/lifecycle faults (kill / pause / restart) — driven straight through
    the Docker CLI against the broker containers.

  * Network faults (netem loss/delay/reorder/duplicate, partition/heal) — applied
    with `tc qdisc` / `iptables` INSIDE the broker's network namespace.

The network faults use a **privileged sidecar** technique instead of modifying
or rebuilding the broker image:

    docker run --rm --net=container:<broker_cid> --cap-add=NET_ADMIN \
        <netem-image> tc qdisc add dev eth0 root netem loss 30%

Because the sidecar shares the target container's network namespace
(`--net=container:...`) with NET_ADMIN, the qdisc it installs on `eth0` affects
the broker's traffic and PERSISTS after the sidecar exits.  This keeps the
broker image untouched and works on Docker Desktop (proven: see README + the
netem smoke test).  The netem image is a tiny alpine + iproute2 built on demand.

Docker-Desktop / macOS caveat: NET_ADMIN + tc netem qdisc-add works under the
LinuxKit VM that backs Docker Desktop (verified live).  What is NOT reliable on
Docker Desktop is `iptables` inside `--net=container:` sidecars (the LinuxKit
kernel may lack the nf modules loaded); therefore `partition()` is implemented
with netem 100% loss (portable) rather than iptables DROP.  On Linux CI both
work; the netem-loss partition is the portable default here.
"""

from __future__ import annotations

import signal

from .dockerutil import run

NETEM_IMAGE = "rune-netem:dev"
_NETEM_BUILT = False


def ensure_netem_image() -> None:
    """Build the tiny alpine+iproute2 sidecar image once per session."""
    global _NETEM_BUILT
    if _NETEM_BUILT:
        return
    # Idempotent: skip build if the image already exists.
    exists = run(["docker", "image", "inspect", NETEM_IMAGE], timeout=30)
    if exists.ok:
        _NETEM_BUILT = True
        return
    dockerfile = "FROM alpine:latest\nRUN apk add --no-cache iproute2 iptables\n"
    _build_from_stdin(dockerfile)
    _NETEM_BUILT = True


def _build_from_stdin(dockerfile: str) -> None:
    import subprocess

    proc = subprocess.run(
        ["docker", "build", "-t", NETEM_IMAGE, "-"],
        input=dockerfile,
        text=True,
        capture_output=True,
        timeout=180,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"failed building netem image:\n{proc.stdout}\n{proc.stderr}")


def _netns_exec(broker_cid: str, cmd: list[str], timeout: int = 30):
    """Run a tc/iptables command inside the broker's netns via a sidecar."""
    ensure_netem_image()
    args = [
        "docker", "run", "--rm",
        f"--net=container:{broker_cid}",
        "--cap-add=NET_ADMIN",
        NETEM_IMAGE,
        *cmd,
    ]
    return run(args, timeout=timeout)


# ---------------------------------------------------------------------------
# Process / lifecycle faults
# ---------------------------------------------------------------------------

def kill_node(cluster, service: str, sig=signal.SIGKILL) -> None:
    """Kill a broker container with SIGKILL (crash) or SIGTERM (graceful)."""
    signame = {signal.SIGKILL: "KILL", signal.SIGTERM: "TERM"}.get(sig, str(int(sig)))
    cid = cluster.container_id(service)
    if not cid:
        raise RuntimeError(f"no container for {service}")
    run(["docker", "kill", "--signal", signame, cid], timeout=30, check=True)


def pause(cluster, service: str) -> None:
    """Freeze the process (docker pause -> SIGSTOP-equivalent cgroup freeze)."""
    cid = cluster.container_id(service)
    run(["docker", "pause", cid], timeout=30, check=True)


def unpause(cluster, service: str) -> None:
    cid = cluster.container_id(service)
    run(["docker", "unpause", cid], timeout=30, check=True)


def restart(cluster, service: str) -> None:
    """Stop+start the container (returns after start; leader re-election is the
    caller's concern via cluster.wait_for_leader)."""
    cluster.compose.restart(service)


# ---------------------------------------------------------------------------
# Network faults (netem via sidecar in the broker netns)
# ---------------------------------------------------------------------------

def _netem_spec(loss=None, delay=None, reorder=None, duplicate=None, iface="eth0") -> list[str]:
    spec = ["tc", "qdisc", "replace", "dev", iface, "root", "netem"]
    if delay is not None:
        spec += ["delay", f"{delay}ms"]
    if loss is not None:
        spec += ["loss", f"{loss}%"]
    if duplicate is not None:
        spec += ["duplicate", f"{duplicate}%"]
    if reorder is not None:
        # reorder requires a delay to have any meaning; default 10ms if unset.
        if delay is None:
            spec += ["delay", "10ms"]
        spec += ["reorder", f"{reorder}%"]
    return spec


def netem(cluster, service: str, *, loss=None, delay=None, reorder=None,
          duplicate=None, iface="eth0") -> None:
    """Install a netem qdisc on the broker's eth0. Reversible via clear_netem()."""
    cid = cluster.container_id(service)
    if not cid:
        raise RuntimeError(f"no container for {service}")
    spec = _netem_spec(loss=loss, delay=delay, reorder=reorder,
                       duplicate=duplicate, iface=iface)
    res = _netns_exec(cid, spec)
    if not res.ok:
        raise RuntimeError(f"netem failed on {service}: {res.stderr or res.stdout}")


def clear_netem(cluster, service: str, iface="eth0") -> None:
    """Remove any netem qdisc from the broker's eth0 (best-effort, reversible)."""
    cid = cluster.container_id(service)
    if not cid:
        return
    _netns_exec(cid, ["tc", "qdisc", "del", "dev", iface, "root"])


def show_netem(cluster, service: str, iface="eth0") -> str:
    cid = cluster.container_id(service)
    if not cid:
        return ""
    return _netns_exec(cid, ["tc", "qdisc", "show", "dev", iface]).stdout


# ---------------------------------------------------------------------------
# Partition / heal
#
# A partition drops all traffic on the target's interface (netem 100% loss).
# This isolates the node from the whole cluster.  For a *directional* / pairwise
# partition, a later ticket can add iptables rules keyed on peer IPs (Linux-CI
# only — see the Docker-Desktop caveat in the module docstring).
# ---------------------------------------------------------------------------

def partition(cluster, service: str) -> None:
    """Isolate `service` from the cluster via netem 100% loss on eth0."""
    netem(cluster, service, loss=100)


def heal(cluster, service: str) -> None:
    """Reverse partition()/netem() on `service`."""
    clear_netem(cluster, service)


def heal_all(cluster) -> None:
    """Best-effort clear of every broker's netem (teardown safety)."""
    from .cluster import SERVICES
    for s in SERVICES:
        try:
            clear_netem(cluster, s)
        except Exception:
            pass
