"""Cluster lifecycle + leader identification for the 3-node Rune cluster.

Wraps the rune-broker docker-compose.yml.  A `Cluster` instance owns the compose
project: it can bring the 3 nodes up, wait for a leader, identify which service
is the leader, and tear everything down (even on failure).

Node id <-> service mapping is fixed by the compose file:
    node 0 -> rune-1, node 1 -> rune-2, node 2 -> rune-3
"""

from __future__ import annotations

import os
import re
import time

from .dockerutil import Compose, run

# Repo layout: dr-e2e/ lives inside rune-broker/, compose is at ../docker/.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_DR_E2E_DIR = os.path.dirname(_THIS_DIR)
BROKER_REPO = os.path.dirname(_DR_E2E_DIR)
COMPOSE_FILE = os.path.join(BROKER_REPO, "docker", "docker-compose.yml")

SERVICES = ["rune-1", "rune-2", "rune-3"]
CLIENT_PORT = 40200

_LEADER_RE = re.compile(r"node (\d+) is LEADER")


def node_id_to_service(node_id: int) -> str:
    if node_id not in (0, 1, 2):
        raise ValueError(f"node id {node_id!r} out of range (expected 0-2)")
    return SERVICES[node_id]


def service_to_node_id(service: str) -> int:
    return SERVICES.index(service)


class Cluster:
    def __init__(self, project: str = "rune-dr", topics: str = "events:1"):
        self.project = project
        self.topics = topics
        self.compose = Compose(COMPOSE_FILE, project)
        self._up = False
        self._serving_cache: str | None = None

    # ---- lifecycle --------------------------------------------------------

    def up(self, build: bool = False, leader_timeout: int = 90) -> "Cluster":
        env_topics = os.environ.get("RUNE_TOPICS")
        # RUNE_TOPICS is baked into the compose file's x-broker-env; we honour any
        # override the caller exported, otherwise the compose default applies.
        if build:
            self.compose.build(*SERVICES)
        self.compose.up(*SERVICES, timeout=180)
        self._up = True
        self.wait_for_leader(leader_timeout)
        _ = env_topics  # documented pass-through; compose owns the actual value
        return self

    def down(self) -> None:
        # Idempotent, best-effort, bounded. Safe to call multiple times.
        self.compose.down()
        self._up = False

    # ---- leadership -------------------------------------------------------

    def _scan_leader(self, services: list[str]) -> int | None:
        """Return the most-recently-announced leader id across `services`, or None."""
        logs = self.compose.logs(*services, timeout=30)
        ids = _LEADER_RE.findall(logs)
        if not ids:
            return None
        # tail: the newest announcement reflects the current term after re-election.
        return int(ids[-1])

    def wait_for_leader(self, timeout_s: int = 90, exclude: list[str] | None = None) -> int:
        """Poll node logs until a leader id appears. Returns the leader node id.

        `exclude` drops services (e.g. a just-killed node) from the scan so a
        stale "is LEADER" line cannot re-select a dead node.
        """
        services = [s for s in SERVICES if not exclude or s not in exclude]
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            leader = self._scan_leader(services)
            if leader is not None and (not exclude or node_id_to_service(leader) not in exclude):
                return leader
            time.sleep(1.0)
        raise TimeoutError(
            f"no leader within {timeout_s}s (scanned {services}); "
            f"last logs:\n{self.compose.logs(*services, tail=40)}"
        )

    def wait_for_new_leader(self, old_leader_svc: str, timeout_s: int = 90) -> int:
        """After killing `old_leader_svc`, wait for a DIFFERENT node to lead."""
        exclude = [old_leader_svc]
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                leader = self.wait_for_leader(timeout_s=10, exclude=exclude)
            except TimeoutError:
                leader = None
            if leader is not None and node_id_to_service(leader) != old_leader_svc:
                return leader
            time.sleep(1.0)
        raise TimeoutError(
            f"no new leader (!= {old_leader_svc}) within {timeout_s}s"
        )

    def identify_leader(self, timeout_s: int = 90) -> tuple[int, str]:
        """Return (leader_node_id, leader_service)."""
        lid = self.wait_for_leader(timeout_s)
        return lid, node_id_to_service(lid)

    def follower_service(self, timeout_s: int = 90) -> str:
        """Return one service that is NOT the current leader."""
        _, leader_svc = self.identify_leader(timeout_s)
        for s in SERVICES:
            if s != leader_svc:
                return s
        raise RuntimeError("no follower found")  # pragma: no cover

    # ---- data-plane readiness --------------------------------------------

    def wait_data_plane_ready(self, max_attempts: int = 5, timeout_s: int = 60) -> str:
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
        out = run(
            ["docker", "compose", "-f", COMPOSE_FILE, "-p", self.project, "ps",
             "--services", "--filter", "status=running"],
            timeout=30,
        ).stdout
        return [s for s in out.split() if s in SERVICES]

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
             f"{{{{.NetworkSettings.Networks.{net}.IPAddress}}}}", cid],
            timeout=15,
        )
        ip = res.stdout.strip()
        return ip if ip else None
