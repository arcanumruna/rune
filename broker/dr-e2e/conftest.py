"""Pytest fixtures for the Rune DR E2E harness.

Fixture strategy:
  * `cluster` (session-scoped): brings the 3-node compose cluster up ONCE for the
    baseline read/produce/subscribe matrix (fast — no per-case cluster churn),
    waits for a leader, and tears down (down -v --remove-orphans) at session end,
    even on failure.  Fault tests that need a pristine cluster per case should
    request `fresh_cluster` (function-scoped) instead.
  * `fresh_cluster` (function-scoped): a full up/leader/down cycle per test — for
    destructive fault cases (DR-2..15) that mutate cluster membership/state.

Both guarantee teardown via try/finally + a netem heal-all sweep so a leaked
qdisc never bleeds into the next case.
"""

from __future__ import annotations

import os

import pytest

from harness.cluster import Cluster
from harness import dockerutil, faults


@pytest.fixture(scope="session", autouse=True)
def _sweep_orphaned_containers():
    """D10-L (D2-10): containers left by a harness process that died without cleanup
    (pytest-timeout's thread method os._exit()s a hung session) are removed before
    this session starts — they would keep producing into its tests."""
    dockerutil.sweep_orphaned_containers()
    yield


def _build_flag() -> bool:
    # Default: assume images are already built (CI builds once up front).
    # Set DR_E2E_BUILD=1 to force `compose build` on cluster bring-up.
    return os.environ.get("DR_E2E_BUILD", "0") == "1"


@pytest.fixture(scope="session")
def cluster():
    c = Cluster(project="rune-dr")
    try:
        c.up(build=_build_flag(), leader_timeout=120)
        c.wait_data_plane_ready(max_attempts=5, timeout_s=60)
        yield c
    finally:
        faults.heal_all(c)
        c.down()


@pytest.fixture
def fresh_cluster():
    c = Cluster(project="rune-dr-fault")
    try:
        # DR_STALE_DATA: start on the previous test's logs (never wiped) — the broker
        # must be correct WITH stale/divergent data on disk (production never gets a
        # clean slate).  DR_KEEP_CLUSTER: leave a failed run's cluster up to inspect.
        stale = bool(os.environ.get("DR_STALE_DATA"))
        c.up(build=_build_flag(), leader_timeout=120, clean=not stale)
        c.wait_data_plane_ready(max_attempts=5, timeout_s=60)
        yield c
    finally:
        faults.heal_all(c)
        if os.environ.get("DR_STALE_DATA"):
            c.compose.down(keep_volumes=True)
        elif not os.environ.get("DR_KEEP_CLUSTER"):
            c.down()


@pytest.fixture(scope="session")
def netem_ready():
    """Ensure the netem sidecar image exists (for network-fault tests)."""
    faults.ensure_netem_image()
    return True
