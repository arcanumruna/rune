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
    if not cid:
        raise RuntimeError(f"pause: no container id for {service!r} (container not running?)")
    run(["docker", "pause", cid], timeout=30, check=True)


def unpause(cluster, service: str) -> None:
    cid = cluster.container_id(service)
    if not cid:
        raise RuntimeError(f"unpause: no container id for {service!r} (container not running?)")
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


# ---------------------------------------------------------------------------
# MTU control (sidecar ip link set — NET_ADMIN)
#
# Reduces (or restores) the interface MTU inside the broker's network namespace.
# Mirrors _netns_exec with `ip link set dev <iface> mtu <mtu>`.
#
# Docker Desktop caveat: MTU changes on veth pairs inside the LinuxKit VM may
# be silently ignored or cause Aeron media driver stalls.  Use on Linux CI only.
# The container restart performed by fresh_cluster teardown (c.down()) destroys
# and recreates the container, which resets the veth MTU automatically — no
# explicit MTU restore is needed in the test's finally block.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Client-container faults (DR-13)
#
# dr_client containers are named `dr-client-<cid>-<hex8>-a<attempt>` (see
# driver._run_one_client).  kill_client_container(name_substr) finds the
# first running container whose name contains `name_substr` and sends it
# SIGKILL via `docker kill`.
# ---------------------------------------------------------------------------

def kill_client_container(name_substr: str, sig: str = "KILL") -> str:
    """Find a running dr-client container matching ``name_substr`` and kill it.

    Uses `docker ps --filter name=<name_substr>` to locate the container, then
    `docker kill --signal <sig>` to send the signal.  Returns the container id
    that was killed.  Raises RuntimeError if no matching container is found.

    This is used in DR-13 fault scenarios to crash a client mid-produce so the
    broker's behaviour under a sudden client disconnect can be observed.
    """
    # docker ps -q --filter name=<substr> returns container ids whose NAME
    # contains the substring (anchored at the start by Docker's filter, so we
    # use the full name prefix where possible).
    res = run(
        ["docker", "ps", "-q", "--filter", f"name={name_substr}"],
        timeout=30,
    )
    ids = res.stdout.strip().splitlines()
    if not ids:
        raise RuntimeError(
            f"kill_client_container: no running container matching {name_substr!r}; "
            f"stdout={res.stdout!r} stderr={res.stderr!r}"
        )
    target = ids[0]
    run(["docker", "kill", "--signal", sig, target], timeout=30, check=True)
    return target


def set_mtu(cluster, service: str, mtu: int, iface: str = "eth0") -> None:
    """Set the interface MTU inside the broker's network namespace.

    Uses the netem sidecar with NET_ADMIN (mirrors _netns_exec).  On Linux CI
    this forces IP-level fragmentation when combined with large Aeron frames,
    exercising the two-level fragmentation path (Aeron + IP).

    On Docker Desktop (macOS LinuxKit VM) this may have no effect or cause
    media driver stalls — use only on Linux CI where veth MTU changes are
    reliable.  The MTU is automatically restored when the container is
    recreated by the fresh_cluster teardown (c.down()).
    """
    cid = cluster.container_id(service)
    if not cid:
        raise RuntimeError(f"no container for {service}")
    res = _netns_exec(cid, ["ip", "link", "set", "dev", iface, "mtu", str(mtu)])
    if not res.ok:
        raise RuntimeError(
            f"set_mtu({mtu}) failed on {service} ({iface}): "
            f"{res.stderr or res.stdout}"
        )


# ---------------------------------------------------------------------------
# Storage / disk / log-corruption faults (DR-14)
#
# These helpers reach into a broker node's named Docker volume via a temporary
# alpine container.  The container is STOPPED (exited) when these helpers run —
# the volume persists on the Docker host and is accessible by mounting it into a
# throwaway container with `docker run --rm -v <vol>:/data alpine ...`.
#
# Volume naming: docker compose uses "<project>_<volume-name>" for named volumes.
# The compose volumes are declared as `rune-data-1`, `rune-data-2`, `rune-data-3`.
# With project="rune-dr-fault" (fresh_cluster) the volume names are:
#   rune-dr-fault_rune-data-1, rune-dr-fault_rune-data-2, rune-dr-fault_rune-data-3
#
# RUNE_DATA_DIR is /var/lib/rune (baked into x-broker-env in docker-compose.yml).
# The helpers mount the volume at /data inside the throwaway container, so paths
# inside the volume are /data/<topic>/<partition>/<segment>.log (or similar).
#
# Log file discovery: `find /data -type f \( -name "*.log" -o -name "*.seg" \)
# | sort` locates all log/segment files.  The LAST file (sort order) is treated
# as the active tail; the FIRST file is the oldest segment.  For small test runs
# (few hundred records) there will typically be a single segment file per
# partition, so tail ≡ first ≡ only.
#
# Docker Desktop / macOS caveat: named volume mounts via `docker run -v` work
# on Docker Desktop (the volume lives in the LinuxKit VM).  Shell commands
# (find, dd, truncate) work inside the alpine container.  The approach is
# portable across platforms.
# ---------------------------------------------------------------------------

_ALPINE_IMAGE = "alpine:latest"


def _volume_name(project: str, service_idx: int) -> str:
    """Return the Docker volume name for a broker node.

    compose naming: "<project>_rune-data-<service_idx>" where service_idx is
    1-based (rune-1 → 1, rune-2 → 2, rune-3 → 3).
    """
    return f"{project}_rune-data-{service_idx}"


def _exec_on_volume(volume: str, shell_cmd: str, timeout: int = 60):
    """Run a shell command inside a throwaway alpine container with the broker
    volume mounted at /data.  Returns CmdResult.

    The container is removed after the command exits (--rm).  The volume must
    exist (i.e., the broker container must have been started at least once so
    compose has initialised the named volume).
    """
    return run(
        [
            "docker", "run", "--rm",
            "-v", f"{volume}:/data",
            _ALPINE_IMAGE,
            "sh", "-c", shell_cmd,
        ],
        timeout=timeout,
    )


def _find_log_files_cmd() -> str:
    """Shell fragment that lists all broker log/segment files under /data, sorted."""
    return r"find /data -type f \( -name '*.log' -o -name '*.seg' \) | sort"


def truncate_log_tail(project: str, service_idx: int, tail_bytes: int = 128) -> None:
    """Remove the last `tail_bytes` bytes from the active log segment of the
    broker node identified by `service_idx` (1-based).

    The node's container MUST be stopped (exited) before calling this.  The
    helper mounts the named Docker volume into a throwaway alpine container,
    finds the last log/segment file (sorted), and uses `truncate` to remove
    the tail, simulating a torn write or incomplete fsync before crash.

    Raises RuntimeError if no log files are found (volume empty / wrong path)
    or if the truncate command fails.

    Args:
        project:     Compose project name (e.g. "rune-dr-fault").
        service_idx: 1-based node index matching the volume suffix (1, 2, or 3).
        tail_bytes:  Number of bytes to remove from the end of the last segment.
    """
    vol = _volume_name(project, service_idx)

    # Find the last (active) log segment and truncate its tail.
    # The shell script:
    #   1. Finds all .log/.seg files under /data.
    #   2. Takes the LAST one (sort, tail -1) as the active tail segment.
    #   3. Gets its current size via `wc -c`.
    #   4. Truncates to max(0, size - tail_bytes) bytes.
    shell_cmd = (
        f"set -e; "
        f"FILES=$({_find_log_files_cmd()}); "
        f"if [ -z \"$FILES\" ]; then echo 'ERROR: no log files found in /data' >&2; exit 1; fi; "
        f"LAST=$(echo \"$FILES\" | tail -1); "
        f"echo \"Truncating tail of: $LAST\"; "
        f"SIZE=$(wc -c < \"$LAST\"); "
        f"NEW_SIZE=$(( SIZE > {tail_bytes} ? SIZE - {tail_bytes} : 0 )); "
        f"truncate -s \"$NEW_SIZE\" \"$LAST\"; "
        f"echo \"Truncated $LAST from $SIZE → $NEW_SIZE bytes\""
    )

    res = _exec_on_volume(vol, shell_cmd)
    if not res.ok:
        raise RuntimeError(
            f"truncate_log_tail failed for volume {vol}:\n"
            f"stdout: {res.stdout}\nstderr: {res.stderr}"
        )


def corrupt_log_tail(project: str, service_idx: int,
                     corrupt_offset: int = 0, corrupt_length: int = 16) -> None:
    """Overwrite `corrupt_length` bytes at `corrupt_offset` from the END of the
    last log segment with 0xFF, simulating bit-flips in the tail (torn write).

    The node's container MUST be stopped before calling this.  Unlike
    truncate_log_tail (which removes the tail), this OVERWRITES bytes, which
    exercises the CRC-detection path without changing the file size.

    Args:
        project:        Compose project name.
        service_idx:    1-based node index.
        corrupt_offset: Byte offset from the END of the file to start overwriting.
                        0 = the very last `corrupt_length` bytes.
        corrupt_length: Number of bytes to overwrite with 0xFF.
    """
    vol = _volume_name(project, service_idx)

    shell_cmd = (
        f"set -e; "
        f"FILES=$({_find_log_files_cmd()}); "
        f"if [ -z \"$FILES\" ]; then echo 'ERROR: no log files found in /data' >&2; exit 1; fi; "
        f"LAST=$(echo \"$FILES\" | tail -1); "
        f"echo \"Corrupting tail of: $LAST\"; "
        f"SIZE=$(wc -c < \"$LAST\"); "
        f"WRITE_POS=$(( SIZE - {corrupt_offset} - {corrupt_length} )); "
        f"if [ $WRITE_POS -lt 0 ]; then WRITE_POS=0; fi; "
        f"dd if=/dev/zero bs=1 count={corrupt_length} 2>/dev/null | tr '\\000' '\\377' | "
        f"dd of=\"$LAST\" bs=1 seek=$WRITE_POS count={corrupt_length} conv=notrunc 2>/dev/null; "
        f"echo \"Overwrote {corrupt_length} bytes at offset $WRITE_POS in $LAST\""
    )

    res = _exec_on_volume(vol, shell_cmd)
    if not res.ok:
        raise RuntimeError(
            f"corrupt_log_tail failed for volume {vol}:\n"
            f"stdout: {res.stdout}\nstderr: {res.stderr}"
        )


def corrupt_log_mid(project: str, service_idx: int,
                    corrupt_offset: int = 64, corrupt_length: int = 16) -> None:
    """Overwrite `corrupt_length` bytes at `corrupt_offset` bytes from the
    START of the FIRST (oldest) log segment with 0xFF, simulating a mid-log
    bit-flip or hardware error in the middle of the committed log.

    The node's container MUST be stopped before calling this.  Targeting the
    FIRST segment (oldest) means the corruption is not at the tail — the broker
    must scan through the committed log from the beginning to detect it.

    Args:
        project:        Compose project name.
        service_idx:    1-based node index.
        corrupt_offset: Byte offset from the START of the first segment.
                        Default 64: skip a typical header before overwriting.
        corrupt_length: Number of bytes to overwrite with 0xFF.
    """
    vol = _volume_name(project, service_idx)

    shell_cmd = (
        f"set -e; "
        f"FILES=$({_find_log_files_cmd()}); "
        f"if [ -z \"$FILES\" ]; then echo 'ERROR: no log files found in /data' >&2; exit 1; fi; "
        f"FIRST=$(echo \"$FILES\" | head -1); "
        f"echo \"Corrupting mid-log of: $FIRST at offset {corrupt_offset}\"; "
        f"SIZE=$(wc -c < \"$FIRST\"); "
        f"if [ $SIZE -le {corrupt_offset} ]; then "
        f"  echo \"WARN: file too small ($SIZE <= {corrupt_offset}), using offset 0\"; "
        f"  WRITE_POS=0; "
        f"else "
        f"  WRITE_POS={corrupt_offset}; "
        f"fi; "
        f"dd if=/dev/zero bs=1 count={corrupt_length} 2>/dev/null | tr '\\000' '\\377' | "
        f"dd of=\"$FIRST\" bs=1 seek=$WRITE_POS count={corrupt_length} conv=notrunc 2>/dev/null; "
        f"echo \"Overwrote {corrupt_length} bytes at offset $WRITE_POS in $FIRST\""
    )

    res = _exec_on_volume(vol, shell_cmd)
    if not res.ok:
        raise RuntimeError(
            f"corrupt_log_mid failed for volume {vol}:\n"
            f"stdout: {res.stdout}\nstderr: {res.stderr}"
        )
