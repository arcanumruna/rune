"""Cluster lifecycle + leader identification for the 3-node Rune cluster.

Wraps the rune-broker docker-compose.yml.  A `Cluster` instance owns the compose
project: it can bring the 3 nodes up, wait for a leader, identify which service
is the leader, and tear everything down (even on failure).

Node id <-> service mapping is fixed by the compose file:
    node 0 -> rune-1, node 1 -> rune-2, node 2 -> rune-3
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import os
import re
import time
from typing import Sequence

from .dockerutil import Compose, _git_sha, run
from . import timing
from .timing import timed
from . import auth
from .driver import ensure_dr_client_image

# Repo layout: dr-e2e/ lives inside broker/ (monorepo), compose is at ../docker/.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DR_E2E_DIR = os.path.dirname(_THIS_DIR)
BROKER_REPO = os.path.dirname(_DR_E2E_DIR)
COMPOSE_FILE = os.path.join(BROKER_REPO, "docker", "docker-compose.yml")

SERVICES = ["rune-1", "rune-2", "rune-3"]
CLIENT_PORT = 40200

# The topics the brokers used to be started with (compose env, now removed): ids follow creation
# order, so events = 0, multi = 1, cg = 2 — the ids the dr_client topic tables hard-code.
DEFAULT_TOPICS = ("events:1", "multi:3", "cg:4")

_SIBLINGS = os.path.dirname(BROKER_REPO)
_CREATE_TOPICS_SH = os.path.join(BROKER_REPO, "docker", "create-topics.sh")
_QL_ATTEMPT_CAP_S = 45          # one `rune ql` container run (connect + DDL budget)
_QL_RETRY_PAUSE_S = 0.5
_QL_RETRIABLE_RE = re.compile(r"NotLeader|Redirect|timed out|connection refused|not connected|unreachable", re.I)
_QL_CREATED_RE = re.compile(r"created topic '([^']*)' topicId=(\d+)")
_QL_STATEMENT_RE = re.compile(r"^statement (\d+):", re.M)


def ql_image() -> str:
    image = os.environ.get("DR_QL_IMAGE")
    if image:
        return image
    client_repo = os.path.join(_SIBLINGS, "client-cpp")
    return f"rune-ql-probe:{_git_sha(client_repo)}"


_QL_IMAGE_READY: set[str] = set()


def ensure_ql_image() -> None:
    """Build the rune CLI image once per process (session start: harness.images.prepare_all).

    Built from the working tree (the layer cache makes an unchanged tree cheap), so a dirty client tree never runs
    on a stale image that happens to carry the HEAD sha tag - but no longer once per Cluster.up(): that rebuild-check
    of the multi-GB builder image cost every test seconds inside its timeout.  DR_QL_IMAGE only names the tag."""
    from .images import prepared
    image = ql_image()
    if image in _QL_IMAGE_READY or prepared():
        return
    res = run(["bash", _CREATE_TOPICS_SH, "--build-image"], timeout=1800, env={**os.environ, "QL_IMAGE": image})
    if not res.ok:
        raise RuntimeError(f"cannot build the rune CLI image {image}:\n{res.stdout}{res.stderr}")
    _QL_IMAGE_READY.add(image)


class TopicCreationError(RuntimeError):
    """A topic DDL did not succeed; the message names the statement and the CLI's last output."""


# Old `name:partitions[:policy[:segmentRollBytes[:deleteRetentionMs[:retentionMs[:retentionBytes[
#  :retentionCheckMs[:fileDeleteDelayMs[:minCleanableDirtyRatio[:maxCompactionLagMs]]]]]]]]][:key=value...]`
# positional fields -> the DDL `with (...)` setting each one became.
_POSITIONAL_SETTINGS = (
    "cleanup.policy", "segment.bytes", "delete.retention.ms", "retention.ms", "retention.bytes",
    "retention.check.ms", "file.delete.delay.ms", "min.cleanable.dirty.ratio", "max.compaction.lag.ms",
)
_POLICIES = {"delete": "delete", "compact": "compact", "compact_delete": "compact_delete", "compactdelete": "compact_delete"}
_TOKEN_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")


def topic_statement(spec: str) -> str:
    """One Rune QL `topic ...;` statement for an old-style `name:partitions[:options]` spec.

    Mapping (every option has a DDL equivalent or the spec is refused - nothing is dropped silently):

      name:partitions                    topic NAME partitions P
      positional policy                  with (cleanup.policy = delete|compact|compact_delete)
      positional 2..9, in order          segment.bytes, delete.retention.ms, retention.ms, retention.bytes,
                                         retention.check.ms, file.delete.delay.ms, min.cleanable.dirty.ratio,
                                         max.compaction.lag.ms   (an empty field is skipped)
      rf=N                               replicas N
      persistence=transient|memory       transient topic NAME ...   (disk = the default)
      durability=majority|all-in-sync    with (durability = ...)
      tier=leader|majority|strict        with (tier = ...)
      group=G                            with (copartition.group = 'G')
      key=K                              with (partition.key = 'K')

    Anything else (an unknown option, a 10th positional field, a bad value) raises ValueError.
    """
    fields = spec.split(":")
    if len(fields) < 2 or not _TOKEN_RE.match(fields[0]) or not fields[1].isdigit():
        raise ValueError(f"topic spec {spec!r}: want name:partitions[:options]")
    name, parts = fields[0], int(fields[1])
    settings: list[str] = []
    replicas = None
    transient = False
    positional: list[str] = []
    for f in fields[2:]:
        if "=" in f:
            k, v = (x.strip() for x in f.split("=", 1))
            k = k.lower()
            if not _TOKEN_RE.match(v or "!"):
                raise ValueError(f"topic spec {spec!r}: bad value for {k}")
            if k == "rf":
                replicas = int(v)
            elif k == "persistence":
                if v.lower() in ("transient", "memory"):
                    transient = True
                elif v.lower() != "disk":
                    raise ValueError(f"topic spec {spec!r}: persistence must be disk or transient")
            elif k in ("durability", "tier"):
                settings.append(f"{k} = {v.lower()}")
            elif k == "group":
                settings.append(f"copartition.group = '{v}'")
            elif k == "key":
                settings.append(f"partition.key = '{v}'")
            else:
                raise ValueError(f"topic spec {spec!r}: unknown option {k!r} (no DDL equivalent)")
        else:
            positional.append(f.strip())
    if len(positional) > len(_POSITIONAL_SETTINGS):
        raise ValueError(f"topic spec {spec!r}: more than {len(_POSITIONAL_SETTINGS)} positional options")
    for key, val in zip(_POSITIONAL_SETTINGS, positional):
        if not val:
            continue
        if key == "cleanup.policy":
            if val.lower() not in _POLICIES:
                raise ValueError(f"topic spec {spec!r}: policy must be delete, compact or compact_delete")
            val = _POLICIES[val.lower()]
        elif not re.fullmatch(r"-?\d+(\.\d+)?", val):
            raise ValueError(f"topic spec {spec!r}: {key} must be a number")
        settings.append(f"{key} = {val}")
    stmt = ("transient " if transient else "") + f"topic {name} partitions {parts}"
    if replicas is not None:
        stmt += f" replicas {replicas}"
    if settings:
        stmt += " with (" + ", ".join(settings) + ")"
    return stmt + ";"


def _request_id(script: str) -> str:
    """A stable non-zero 32-hex request id for a script: a re-run returns the original results.
    The low 16 bits are zero so statement N (id lo + N - 1) never overflows."""
    return hashlib.sha256(script.encode()).hexdigest()[:28] + "0000"

# Legacy log-scrape pattern — kept ONLY as a fallback for images that predate
# the Aeron role counter (see _read_role below).  Primary detection is
# counter-based and does NOT depend on RUNE_LOG_LEVEL.
_LEADER_RE = re.compile(r"node (\d+) is LEADER")

# rune_role prints:  ROLE nodeId=<n> role=<LEADER|FOLLOWER|CANDIDATE|NONE> term=<t>
_ROLE_RE = re.compile(r"ROLE nodeId=(-?\d+) role=(\w+) term=(-?\d+)")

# ... and (ISR v3) a second line:
#   STORAGE nodeId=<n> state=<CONFIRMED|UNCONFIRMED|UNDECIDED|UNKNOWN> heldLeaderRoles=<n> pendingNodes=<n> \
#           outOfSync=<n> lostCommittedReports=<n>        (-1 = that counter is absent)
_STORAGE_RE = re.compile(
    r"STORAGE nodeId=(-?\d+) state=(\w+) heldLeaderRoles=(-?\d+) pendingNodes=(-?\d+) "
    r"outOfSync=(-?\d+) lostCommittedReports=(-?\d+)")


def parse_rune_role_storage(out: str) -> dict | None:
    """Parse rune_role's STORAGE line (None when the line is absent: an image that predates the storage gate)."""
    m = _STORAGE_RE.search(out or "")
    if not m:
        return None
    return {
        "node_id": int(m.group(1)),
        "state": m.group(2),
        "held_leader_roles": int(m.group(3)),
        "pending_nodes": int(m.group(4)),
        "out_of_sync": int(m.group(5)),
        "lost_committed_reports": int(m.group(6)),
    }


BOOT_LINE = "storage: dataDir="   # logged once per broker process, before anything else touches the data dir


def slice_since_last_boot(text: str) -> str:
    """The part of a (possibly multi-process) container log written by the LAST broker process: from the last boot line
    on.  A restarted container keeps its old process's lines, so an order check on the whole log would see them."""
    i = (text or "").rfind(BOOT_LINE)
    return text[i:] if i >= 0 else ""


def node_id_to_service(node_id: int) -> str:
    if node_id not in (0, 1, 2):
        raise ValueError(f"node id {node_id!r} out of range (expected 0-2)")
    return SERVICES[node_id]


def service_to_node_id(service: str) -> int:
    return SERVICES.index(service)


class Cluster:
    def __init__(self, project: str = "rune-dr", topics: Sequence[str] | None = None,
                 override_files: list[str] | None = None,
                 global_overlays: bool = True):
        """Initialise a Cluster.

        Args:
            project:        Docker Compose project name.
            topics:         The topics `up()` creates by DDL once a leader exists
                            (`name:partitions[:options]` specs, see `topic_statement`;
                            ids follow list order).  Default: DEFAULT_TOPICS.
            global_overlays: Apply RUNE_DR_COMPOSE_OVERRIDES (e.g. the shard
                            overlay).  Default-config baseline arms pass False so
                            they keep measuring the true default config.
            override_files: Optional list of additional docker-compose YAML
                            files to merge on top of the canonical compose file
                            (broker environment overrides).
        """
        if isinstance(topics, str):
            raise TypeError("topics is a sequence of 'name:partitions[:options]' specs, not a str")
        # pytest-xdist: every worker gets its own compose projects (containers, network, volumes), so parallel
        # workers never share a cluster.  Nothing is published on the host, so there are no port clashes.
        worker = os.environ.get("PYTEST_XDIST_WORKER")
        if worker and not project.endswith(f"-{worker}"):
            project = f"{project}-{worker}"
        self.project = project
        self.topics: tuple[str, ...] = tuple(DEFAULT_TOPICS if topics is None else topics)
        # RUNE_IMAGE_TAG: use the git HEAD SHA so every commit's broker image is
        # tagged rune-broker-node:<sha> instead of <project>-rune-{1,2,3}:latest.
        # This prevents layer-store collisions between builds from different commits
        # (the root cause of "parent snapshot does not exist" errors when Docker's
        # containerd snapshot store has stale layers from a previous build).
        # _git_sha falls back to "local" in bare-binary CI environments.
        # An explicit RUNE_IMAGE_TAG (e.g. a diagnostic build of this SHA) wins.
        _env_tag = os.environ.get("RUNE_IMAGE_TAG")
        _sha = _env_tag or _git_sha(BROKER_REPO)   # review B-M6: "<sha>-dirty" for a dirty tree (never HEAD's tag)
        print(f"[harness] broker image tag {_sha} ({'RUNE_IMAGE_TAG' if _env_tag else 'git HEAD'})", flush=True)
        self._image_sha = _sha   # broker_image(): the image `storage format` runs from (faults.format_storage)
        # RUNE_DR_COMPOSE_OVERRIDES: global overlays (e.g. docker/docker-compose.shard.yml)
        # that run the whole matrix in a mode.  They go FIRST so a test's own
        # override_files still win, and default-config baseline arms opt out entirely
        # (global_overlays=False) so they keep measuring the real default.
        extra = ([p for p in os.environ.get("RUNE_DR_COMPOSE_OVERRIDES", "").split(os.pathsep) if p]
                 if global_overlays else [])
        self.compose = Compose(COMPOSE_FILE, project,
                               override_files=extra + list(override_files or []),
                               image_tag=_sha)
        self._up = False
        self._serving_cache: str | None = None

    def broker_image(self) -> str:
        """The broker image this cluster runs (``rune-broker-node:<sha>``, the compose image_tag)."""
        return f"rune-broker-node:{self._image_sha}"

    # ---- lifecycle --------------------------------------------------------

    @timed("cluster.up")
    def up(self, build: bool = False, leader_timeout: int = 90,
           clean: bool = True) -> "Cluster":
        # Every e2e test brings a Cluster up before any dr client runs: refuse (or build) a stale client image here.
        ensure_dr_client_image()
        # Authentication is ON: the brokers refuse to start without the cluster key file (RUNE_SECRETS_DIR is
        # what docker-compose.yml mounts it from); the harness generates one per process.
        auth.ensure_key()
        # The brokers boot with an empty catalog: once a leader exists the topics are
        # created by DDL (create_topics below).
        #
        # HERMETIC START: wipe any named data volumes from a PRIOR run of this
        # project name BEFORE bringing the cluster up.  The fixture teardown calls
        # `down -v`, but if a prior run was interrupted (crash, SIGKILL, pytest
        # cancelled, CI timeout) its teardown never ran and its committed records
        # persist in `<project>_rune-data-N`.  A subsequent `up` would then mount
        # those DIRTY volumes and serve the prior run's records — surfacing as
        # bogus "phantom"/resurrection failures (records not in THIS run's produced
        # set).  A pre-up `down -v` makes every cluster start deterministic and
        # independent of how the previous run ended.  clean=False opts out.
        if clean and self._project_has_leftovers():
            self.compose.down()  # `down -v --remove-orphans` — wipes stale volumes
        from .images import prepared
        if build and not prepared():   # DR_E2E_BUILD: built once at session start when the session hook ran
            self.compose.build(*SERVICES)
        self.compose.up(*SERVICES, timeout=180)
        self._up = True
        self.wait_for_leader(leader_timeout)
        # A fresh cluster: bootstrap the first admin and create the e2e user the harness's clients log in as.
        auth.bootstrap(self.compose.network_name(), f"{SERVICES[0]}:{CLIENT_PORT}")
        if self.topics:
            self.create_topics(self.topics)
        return self

    def _project_has_leftovers(self) -> bool:
        """Any container, volume or network of this compose project left (by an interrupted run)?  The pre-up
        `down -v` is only needed then; on a clean host it cost every bring-up ~1 s of compose start-up."""
        label = f"label=com.docker.compose.project={self.project}"
        for cmd in (["docker", "ps", "-aq", "--filter", label], ["docker", "volume", "ls", "-q", "--filter", label],
                    ["docker", "network", "ls", "-q", "--filter", label]):
            res = run(cmd, timeout=30)
            if not res.ok or res.stdout.strip():
                return True       # leftovers, or docker could not tell: clean as before
        return False

    # ---- topics (DDL) -----------------------------------------------------

    def _ql_image(self) -> str:
        return ql_image()

    @timed("topics.create")
    def create_topics(self, specs: Sequence[str], *, deadline_s: float | None = None) -> list[tuple[str, int]]:
        """Create topics by DDL through the `rune ql -f` CLI container on the cluster network.

        `specs` are old-style `name:partitions[:options]` strings (see `topic_statement` for the
        option mapping); topic ids follow list order (checked: a surprise id raises).  One script,
        one fixed request id (derived from the script), so a retry after a partial run - or a
        second call with the same specs - returns the original results and never duplicates.

        Retries (every 0.5 s) while the cluster forms: connect timed out / refused, NotLeader,
        Redirect, a per-attempt timeout.  Any other failure (parse error, TopicExists with other
        options, ...) raises at once.  Total budget DR_CREATE_TOPICS_DEADLINE_S (default 60).
        Raises TopicCreationError naming the failing statement and the last CLI output.
        Returns [(name, topicId)].
        """
        statements = [topic_statement(s) for s in specs]
        script = "\n".join(statements) + "\n"
        request_id = _request_id(script)
        if deadline_s is None:
            deadline_s = float(os.environ.get("DR_CREATE_TOPICS_DEADLINE_S", "60"))
        image = ql_image()
        try:
            ensure_ql_image()
        except RuntimeError as e:
            raise TopicCreationError(str(e)) from e
        network = self.compose.network_name()
        seed = f"{SERVICES[0]}:{CLIENT_PORT}"
        deadline = time.time() + deadline_s
        last = "(no attempt ran)"
        attempt = 0
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            attempt += 1
            cap = int(min(remaining, _QL_ATTEMPT_CAP_S)) + 1
            try:
                res = run(
                    ["docker", "run", "--rm", "--network", network, "--shm-size=256m",
                     "--security-opt", "seccomp=unconfined", "--platform", "linux/amd64",
                     *auth.docker_args(),
                     "-e", f"RQL={script}", image,
                     "bash", "-c",
                     f'printf "%s" "$RQL" > /tmp/topics.rql && '
                     f'exec rune ql -f /tmp/topics.rql --request-id {request_id} --broker {seed}'],
                    timeout=cap)
            except TimeoutError:
                last = f"attempt {attempt} ran longer than {cap}s and was stopped"
                time.sleep(_QL_RETRY_PAUSE_S)
                continue
            out = (res.stdout or "") + (res.stderr or "")
            if res.ok:
                created = _QL_CREATED_RE.findall(out)
                if len(created) != len(statements):
                    raise TopicCreationError(
                        f"{len(created)} of {len(statements)} topics reported created; output:\n{out}")
                for want, (name, tid) in enumerate(created):
                    if int(tid) != want:
                        raise TopicCreationError(
                            f"topic {name!r} got topicId={tid}, expected {want} (is the cluster fresh?)")
                return [(n, int(t)) for n, t in created]
            last = out.strip()[-800:]
            if not _QL_RETRIABLE_RE.search(out):
                m = _QL_STATEMENT_RE.search(out)
                idx = int(m.group(1)) - 1 if m else -1
                stmt = statements[idx] if 0 <= idx < len(statements) else "(unknown statement)"
                raise TopicCreationError(f"not retriable: {stmt}\n{last}")
            time.sleep(_QL_RETRY_PAUSE_S)
        raise TopicCreationError(
            f"topics not created within {deadline_s:.0f}s ({attempt} attempts): {statements}\nlast error: {last}")

    @timed("cluster.down")
    def down(self) -> None:
        # Idempotent, best-effort, bounded. Safe to call multiple times.
        self.compose.down()
        auth.forget(self.compose.network_name())
        self._up = False

    # ---- leadership -------------------------------------------------------

    def _read_role(self, service: str) -> tuple[str, int, int] | None:
        """Read (role, nodeId, term) from a service's Aeron role counter.

        Runs `rune_role` inside the container (docker compose exec), which reads
        the machine-readable node-role + leadership-term counters out of the
        driver's CnC file — the Aeron-faithful leadership signal, independent of
        RUNE_LOG_LEVEL and of any log line.

        Returns None if the container is not running, rune_role isn't present, or
        the counter isn't allocated yet (rune_role exits non-zero) — callers poll.

        A tight exec timeout (5 s) keeps the detection poll responsive: an exec
        against a just-killed/unreachable node returns quickly as "no reading"
        rather than stalling the loop.  rune_role itself is a fast point read.
        """
        try:
            # `docker exec` on the compose container name: the same exec as `docker compose exec -T`, without the
            # compose CLI's ~0.15 s of project parsing per call (a role read is the harness's hottest call).
            res = run(["docker", "exec", self.container_name(service), "rune_role"], timeout=5)
        except Exception:
            return None
        m = _ROLE_RE.search(res.stdout or "")
        if not m:
            return None
        role = m.group(2)
        node_id = int(m.group(1))
        term = int(m.group(3))
        return role, node_id, term

    def read_storage(self, service: str) -> dict | None:
        """The storage gate of one node (``rune_role``'s STORAGE line, see `parse_rune_role_storage`), or None when the
        container is down, rune_role is absent, or the counters are not allocated yet (callers poll)."""
        try:
            res = run(["docker", "exec", self.container_name(service), "rune_role"], timeout=5)
        except Exception:
            return None
        return parse_rune_role_storage(res.stdout or "")

    def ql(self, script: str, *, seed_service: str | None = None, request_id: str | None = None,
           timeout: int = 60) -> "CmdResult":
        """Run a Rune QL script (DDL or a read such as ``show history;``) through the `rune ql` CLI container on the
        cluster network against ``seed_service`` (default rune-1).  One shot, no retry: the caller asserts on
        ``.ok`` / ``.stdout``.  ``request_id`` (32 hex) makes a DDL script idempotent across a re-run."""
        ensure_ql_image()
        seed = f"{seed_service or SERVICES[0]}:{CLIENT_PORT}"
        rid = f" --request-id {request_id}" if request_id else ""
        return run(
            ["docker", "run", "--rm", "--network", self.compose.network_name(), "--shm-size=256m",
             "--security-opt", "seccomp=unconfined", "--platform", "linux/amd64",
             *auth.docker_args(),
             "-e", f"RQL={script}", ql_image(),
             "bash", "-c",
             f'printf "%s" "$RQL" > /tmp/s.rql && exec rune ql -f /tmp/s.rql{rid} --broker {seed}'],
            timeout=timeout)

    def logs_since_last_boot(self, service: str) -> str:
        """The container's log from the last broker process's boot line on (see `slice_since_last_boot`)."""
        return slice_since_last_boot(self.compose.logs(service, timeout=60))

    def container_name(self, service: str) -> str:
        """Compose v2's container name for a service of this project (no container_name/scale in the compose file)."""
        return f"{self.project}-{service}-1"

    def read_roles(self, services: list[str]) -> list[tuple[str, int, int] | None]:
        """`_read_role` of every service at once (concurrent execs): one poll costs one exec, not three."""
        if len(services) <= 1:
            return [self._read_role(s) for s in services]
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(services)) as pool:
            return list(pool.map(self._read_role, services))

    def _scan_leader(self, services: list[str]) -> int | None:
        """Return the current leader node id across `services`, or None.

        Counter-based: query each running service's role counter via rune_role
        and pick the node reporting role=LEADER at the HIGHEST term (the term tie-
        break discards a stale ex-leader that hasn't yet stepped down, exactly
        like the old term-aware log oracle — DR-5).  Falls back to log scraping
        only if NO service produced a usable counter reading (pre-counter image).
        """
        best_leader: int | None = None
        best_term = -1
        any_reading = False
        for reading in self.read_roles(services):
            if reading is None:
                continue
            any_reading = True
            role, node_id, term = reading
            if role == "LEADER" and term >= best_term:
                best_term = term
                best_leader = node_id
        if any_reading:
            return best_leader
        # Fallback: legacy log scrape (only reached on pre-counter images).
        logs = self.compose.logs(*services, timeout=30)
        ids = _LEADER_RE.findall(logs)
        if not ids:
            return None
        return int(ids[-1])

    # Poll interval for leadership detection.  The broker republishes its role
    # counter every 100 ms (main loop tick) and _read_role is a cheap point read
    # (docker exec rune_role), so a 200 ms poll observes a role change within ~1
    # tick without hammering the counter.  (Election SPEED is unchanged — this is
    # only how fast the harness OBSERVES the outcome.)
    _LEADER_POLL_S = 0.2

    @timed("leader.wait")
    def wait_for_leader(self, timeout_s: int = 90, exclude: list[str] | None = None) -> int:
        """Poll the role counters until a leader appears. Returns the leader node id.

        `exclude` drops services (e.g. a just-killed node) from the scan so a
        stale reading cannot re-select a dead node.
        """
        services = [s for s in SERVICES if not exclude or s not in exclude]
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            leader = self._scan_leader(services)
            if leader is not None and (not exclude or node_id_to_service(leader) not in exclude):
                return leader
            time.sleep(self._LEADER_POLL_S)
        raise TimeoutError(
            f"no leader within {timeout_s}s (scanned {services}); "
            f"last logs:\n{self.compose.logs(*services, tail=40)}"
        )

    @timed("leader.wait_new")
    def wait_for_new_leader(self, old_leader_svc: str, timeout_s: int = 90) -> int:
        """After killing `old_leader_svc`, wait for a DIFFERENT node to lead.

        Polls the role counters of the surviving nodes directly (no nested
        wait_for_leader) so detection reacts within ~one broker publish tick.
        """
        exclude = [old_leader_svc]
        services = [s for s in SERVICES if s not in exclude]
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            leader = self._scan_leader(services)
            if leader is not None and node_id_to_service(leader) != old_leader_svc:
                return leader
            time.sleep(self._LEADER_POLL_S)
        raise TimeoutError(
            f"no new leader (!= {old_leader_svc}) within {timeout_s}s"
        )

    def identify_leader(self, timeout_s: int = 90) -> tuple[int, str]:
        """Return (leader_node_id, leader_service)."""
        lid = self.wait_for_leader(timeout_s)
        return lid, node_id_to_service(lid)

    _BECOME_LEADER_RE = re.compile(r"PartitionReplica\[node=(\d+)\] becomeLeader topic=(-?\d+) part=(\d+) epoch=(\d+)")

    def partition_leaders(self, topic_id: int) -> dict[int, str]:
        """{partition: leader service} for `topic_id`, from the brokers' own logs (review B-I4): every
        becomeLeader is logged at info; the highest epoch per partition wins.  `multi` is topic 1 (DEFAULT_TOPICS)."""
        best: dict[int, tuple[int, int]] = {}
        for svc in SERVICES:
            for m in self._BECOME_LEADER_RE.finditer(self.compose.logs(svc, timeout=30)):
                node, topic, part, epoch = (int(x) for x in m.groups())
                if topic != topic_id:
                    continue
                if part not in best or epoch >= best[part][0]:
                    best[part] = (epoch, node)
        return {p: node_id_to_service(n) for p, (_, n) in best.items() if n in (0, 1, 2)}

    def follower_service(self, timeout_s: int = 90) -> str:
        """Return one service that is NOT the current leader."""
        _, leader_svc = self.identify_leader(timeout_s)
        for s in SERVICES:
            if s != leader_svc:
                return s
        raise RuntimeError("no follower found")  # pragma: no cover

    # ---- settling ----------------------------------------------------------

    def converged(self, running: list[str] | None = None) -> int | None:
        """The leader id when the cluster has settled, else None.

        Settled = every running node reports a role, exactly ONE reports LEADER, and every node reports the leader's
        term (the followers learned it; a zombie / partitioned ex-leader has stepped down).  `running` (a service
        list) also requires exactly those services to be running."""
        actual = self.running_services()
        if running is not None and sorted(actual) != sorted(running):
            return None
        readings = self.read_roles(actual)
        if not readings or any(r is None for r in readings):
            return None
        leaders = [(nid, term) for role, nid, term in readings if role == "LEADER"]
        if len(leaders) != 1:
            return None
        lid, lterm = leaders[0]
        if any(term != lterm for _, _, term in readings):
            return None
        return lid

    def settle(self, max_s: float, *, running: list[str] | None = None, stable_reads: int = 2) -> int | None:
        """Wait until `converged()` holds for `stable_reads` consecutive polls, at most `max_s` seconds.

        Replaces a fixed `time.sleep(max_s)` that only gave the cluster time to settle before POSITIVE checks (a
        produce / subscribe must succeed, a single leader must be found): the checks that follow are unchanged, and
        when the cluster does not converge in time this returns after exactly the old sleep, so the worst case is the
        old behaviour.  Never use it for an observation window (a sleep during which a fault must NOT happen) - those
        stay fixed sleeps.  Returns the converged leader id, or None after `max_s`."""
        with timing.phase("settle", f"max={max_s:g}s") as ph:
            deadline = time.time() + max_s
            streak, last = 0, None
            while True:
                lid = self.converged(running)
                streak = streak + 1 if (lid is not None and lid == last) else (1 if lid is not None else 0)
                last = lid
                if streak >= stable_reads:
                    ph.exit = "converged"
                    return lid
                remaining = deadline - time.time()
                if remaining <= 0:
                    ph.exit = "max"
                    return None
                time.sleep(min(self._LEADER_POLL_S, remaining))

    def wait_running(self, services: list[str], max_s: float) -> bool:
        """Poll until exactly `services` are running (at most `max_s`; replaces a fixed sleep before a
        running-set assertion, which still runs after this).  True when reached."""
        with timing.phase("wait_running", ",".join(services) or "none") as ph:
            deadline = time.time() + max_s
            while True:
                if sorted(self.running_services()) == sorted(services):
                    ph.exit = "reached"
                    return True
                if time.time() >= deadline:
                    ph.exit = "max"
                    return False
                time.sleep(self._LEADER_POLL_S)

    # ---- data-plane readiness --------------------------------------------

    @timed("dataplane.ready")
    def wait_data_plane_ready(self, max_attempts: int = 5, timeout_s: int = 60,
                              topics: str = "events:0:1") -> str:
        """Block until the data plane (Aeron media driver + broker ingress) is
        fully warm on the current leader and return the leader service name.

        Leader-elected (consensus) does not guarantee the Aeron data path is
        ready: the media driver and broker ingress publication need a few
        additional seconds to complete their start-up handshake.  Without this
        gate the FIRST workload produce of a test can connect before the data
        plane is ready, causing a spurious "pre-fault produce failed" (DR-BUG-8).

        Strategy: attempt a tiny acks=All warm-up produce (5 records) against the
        current leader up to ``max_attempts`` times, with short back-off between
        attempts, within ``timeout_s`` seconds total.  On success the leader
        service is cached as the known-serving node so ``serving_node()`` need not
        re-probe.  Uses cid_base=995000 — disjoint from test cids and from the
        990000 window used by ``serving_node()``.

        The ``topics`` parameter controls which topic/topicId is used for the
        warm-up probe.  Callers with a non-standard topic layout (e.g. the
        compaction cluster where topicId=0 is "compact_log", not "events")
        MUST pass the appropriate topic string so that the warm-up records do
        NOT land on a test topic and skew invariant checks.

        Raises RuntimeError if no warm-up succeeds within the budget.
        """
        # Imported lazily to avoid a cycle (driver imports nothing from cluster).
        from .driver import run_clients

        deadline = time.time() + timeout_s
        last_err = ""
        attempt = 0
        while attempt < max_attempts and time.time() < deadline:
            attempt += 1
            try:
                leader_id = self.wait_for_leader(timeout_s=30)
                leader_svc = node_id_to_service(leader_id)
            except TimeoutError as exc:
                last_err = str(exc)
                time.sleep(2.0)
                continue

            res = run_clients(
                1,
                network=self.compose.network_name(),
                target_service=leader_svc,
                mode="produce",
                records=5,
                keys=3,
                acks="all",
                timeout_ms=12000,
                cid_base=995000,  # warm-up cid window; disjoint from test cids
                timeout_s=45,
                topics=topics,
            )
            if res.ok and len(res.all_produced) == 5:
                # Cache so serving_node() does not re-probe for this Cluster.
                self._serving_cache = leader_svc
                return leader_svc
            last_err = (res.clients[0].raw[-500:] if res.clients else "no result")
            time.sleep(3.0)

        raise RuntimeError(
            f"data-plane not ready after {attempt} attempts / {timeout_s}s; "
            f"last output:\n{last_err}"
        )

    @timed("serving_node")
    def serving_node(self, timeout_s: int = 60) -> str:
        """Return a node that actually SERVES produce (data-plane ready).

        Leader-elected (consensus) does not guarantee the client can produce
        through a given node yet, and — see the DR-1 report — the current
        rune-client-cpp build deterministically SIGSEGVs when producing through
        some nodes while another serves fine.  This probe runs a tiny produce
        against each running node and returns the first that succeeds, so the
        harness workload targets a known-good gateway.  Raises if none serve.
        """
        if self._serving_cache is not None:
            return self._serving_cache
        # Imported lazily to avoid a cycle (driver imports nothing from cluster).
        from .driver import run_clients

        deadline = time.time() + timeout_s
        last_err = ""
        while time.time() < deadline:
            for svc in self.running_services():
                res = run_clients(
                    1,
                    network=self.compose.network_name(),
                    target_service=svc,
                    mode="produce",
                    records=1,
                    keys=1,
                    timeout_ms=6000,
                    cid_base=990000,  # probe cid window, far from test cids
                    timeout_s=40,
                )
                if res.ok and len(res.all_produced) == 1:
                    self._serving_cache = svc
                    return svc
                last_err = (res.clients[0].raw[-300:] if res.clients else "no result")
            time.sleep(2.0)
        raise RuntimeError(
            f"no node served a probe produce within {timeout_s}s; last:\n{last_err}"
        )

    # ---- introspection ----------------------------------------------------

    def running_services(self) -> list[str]:
        # The compose labels directly (what `docker compose ps --services --filter status=running` reads), without
        # the compose CLI start-up per call.
        out = run(
            ["docker", "ps", "--filter", f"label=com.docker.compose.project={self.project}",
             "--filter", "status=running", "--format", '{{.Label "com.docker.compose.service"}}'],
            timeout=30,
        ).stdout
        return sorted(s for s in set(out.split()) if s in SERVICES)

    def container_id(self, service: str) -> str | None:
        return self.compose.container_id(service)

    def container_ip(self, service: str, network: str | None = None) -> str | None:
        """Return the container's IP on the compose default network, or None."""
        cid = self.container_id(service)
        if not cid:
            return None
        net = network or self.compose.network_name()
        res = run(
            ["docker", "inspect", "--format",
             # `index`: a network name with '-' (every compose project here) is not a valid template field name
             # - `.Networks.<net>.IPAddress` failed to parse and this returned None for every project.
             f'{{{{(index .NetworkSettings.Networks "{net}").IPAddress}}}}', cid],
            timeout=15,
        )
        ip = res.stdout.strip()
        return ip if ip else None
