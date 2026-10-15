"""Pytest fixtures for the Rune DR E2E harness.

Fixture strategy:
  * `cluster` (module-scoped): brings the 3-node compose cluster up ONCE per module
    for the baseline read/produce/subscribe matrices (fast — no per-case cluster
    churn), waits for a leader, and tears down (down -v --remove-orphans) when the
    module ends, even on failure.  Module (not session) scope: an idle 3-broker
    cluster holds ~1.4 GB and ~0.75 core, so it must not stay up underneath every
    later fault test's own cluster.  Fault tests that need a pristine cluster per case should
    request `fresh_cluster` (function-scoped) instead.
  * `fresh_cluster` (function-scoped): a full up/leader/down cycle per test — for
    destructive fault cases (DR-2..15) that mutate cluster membership/state.

Both guarantee teardown via try/finally + a netem heal-all sweep so a leaked
qdisc never bleeds into the next case.
"""

from __future__ import annotations

import os
import threading

import pytest

from harness.cluster import Cluster
from harness import dockerutil, faults, timing

timing.install_sleep_probe()


def _only_harness_unit_tests(config) -> bool:
    """The docker-free harness self-tests (test_harness_*) need no images."""
    args = [a for a in config.args if not a.startswith("-")]
    return bool(args) and all(os.path.basename(a.split("::")[0]).startswith("test_harness_") for a in args)


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session):
    """Prepare EVERY image once, before any test and outside every per-test timeout (harness.images).

    Runs in the single process, or in the xdist controller BEFORE it spawns the workers (tryfirst), which then
    inherit DR_IMAGES_PREPARED=1.  A failed build/verification ends the session at once with the image named."""
    config = session.config
    if hasattr(config, "workerinput") or os.environ.get("DR_SKIP_IMAGE_PREP") == "1":
        return
    if config.getoption("collectonly", False) or _only_harness_unit_tests(config):
        return
    # The host-wide test lock the xdist workers share (see _host_test_lock); created before the workers spawn.
    os.environ.setdefault("DR_HOST_LOCK", os.path.join(str(config.rootpath), f".dr-host-{os.getpid()}.lock"))
    from harness import images
    try:
        images.prepare_all()
    except images.ImagePreparationError as e:
        pytest.exit(f"dr-e2e image preparation failed - no test was run:\n{e}", returncode=3)


def _timed_runtest(item, when):
    timing.current["test"] = item.nodeid
    timing.current["when"] = when
    with timing.phase(f"test.{when}", item.nodeid):
        yield
    timing.current["test"] = "session"
    timing.current["when"] = "session"


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_setup(item):
    yield from _timed_runtest(item, "setup")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item):
    yield from _timed_runtest(item, "call")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_teardown(item, nextitem):
    yield from _timed_runtest(item, "teardown")


_FAILED_NOW = {"nodeid": None}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    if rep.failed and rep.when in ("setup", "call"):
        _FAILED_NOW["nodeid"] = item.nodeid


def _save_broker_logs_on_failure(c: Cluster, request) -> None:
    """A failed test's broker logs go to DR_CLIENT_LOG_DIR/brokers/<test>/ before the cluster is removed, so a
    failure can be classified (harness vs broker vs client) after the fact."""
    log_dir = os.environ.get("DR_CLIENT_LOG_DIR")
    if not log_dir or _FAILED_NOW["nodeid"] != request.node.nodeid:
        return
    import re as _re
    d = os.path.join(log_dir, "brokers", _re.sub(r"[^A-Za-z0-9_.-]+", "_", request.node.nodeid)[-150:])
    os.makedirs(d, exist_ok=True)
    from harness.cluster import SERVICES
    for svc in SERVICES:
        try:
            with open(os.path.join(d, f"{svc}.log"), "w") as fh:
                fh.write(c.compose.logs(svc, timeout=60))
        except Exception as e:  # noqa: BLE001 - diagnostics only
            print(f"[dr-e2e] could not save {svc} logs: {e}")


@pytest.fixture(scope="session", autouse=True)
def _sweep_orphaned_containers():
    """D10-L (D2-10): containers left by a harness process that died without cleanup
    (pytest-timeout's thread method os._exit()s a hung session) are removed before
    this session starts — they would keep producing into its tests."""
    dockerutil.sweep_orphaned_containers()
    yield


def _build_flag() -> bool:
    # Default: the broker image must already exist (checked once at session start).
    # DR_E2E_BUILD=1 builds it once at session start (harness.images), never per bring-up.
    return os.environ.get("DR_E2E_BUILD", "0") == "1"


@pytest.fixture(scope="module")
def cluster(request):
    c = Cluster(project="rune-dr")
    try:
        c.up(build=_build_flag(), leader_timeout=120)
        c.wait_data_plane_ready(max_attempts=5, timeout_s=60)
        yield c
    finally:
        faults.heal_all(c)
        c.down()


@pytest.fixture(autouse=True)
def _module_cluster_logs_on_failure(request):
    """The module-scoped `cluster` outlives a failed test: save its logs right after the failing test."""
    yield
    if "cluster" in request.fixturenames:
        _save_broker_logs_on_failure(request.getfixturevalue("cluster"), request)


# fresh_cluster teardown runs `down -v` in the background while the next test brings ITS cluster up: consecutive
# fresh clusters alternate between two project names, so a teardown never races the next bring-up of its own
# project (each bring-up first waits for the previous teardown of the same project).  Non-daemon threads, joined
# at session end: no cluster outlives the session.
_BG_DOWNS: dict[str, threading.Thread] = {}
_FRESH_SLOT = {"n": 0}


def _await_down(project: str) -> None:
    t = _BG_DOWNS.pop(project, None)
    if t is not None:
        t.join(timeout=300)


def _background_down(c: Cluster) -> None:
    _await_down(c.project)
    t = threading.Thread(target=c.down, name=f"down-{c.project}")
    t.start()
    _BG_DOWNS[c.project] = t


def pytest_sessionfinish(session, exitstatus):
    for project in list(_BG_DOWNS):
        _await_down(project)
    lock = os.environ.get("DR_HOST_LOCK")
    if lock and not hasattr(session.config, "workerinput") and str(os.getpid()) in lock:
        for f in (lock, lock + ".gate"):
            try:
                os.remove(f)
            except OSError:
                pass


@pytest.fixture
def fresh_cluster(request):
    stale = bool(os.environ.get("DR_STALE_DATA"))
    keep = bool(os.environ.get("DR_KEEP_CLUSTER"))
    if stale or keep:
        name = "rune-dr-fault"        # DR_STALE_DATA restarts on the previous test's volumes: one fixed project
    else:
        name = f"rune-dr-fault-{'ab'[_FRESH_SLOT['n'] % 2]}"
        _FRESH_SLOT["n"] += 1
    c = Cluster(project=name)
    _await_down(c.project)
    try:
        # DR_STALE_DATA: start on the previous test's logs (never wiped) — the broker
        # must be correct WITH stale/divergent data on disk (production never gets a
        # clean slate).  DR_KEEP_CLUSTER: leave a failed run's cluster up to inspect.
        c.up(build=_build_flag(), leader_timeout=120, clean=not stale)
        c.wait_data_plane_ready(max_attempts=5, timeout_s=60)
        yield c
    finally:
        _save_broker_logs_on_failure(c, request)
        # A container that is removed takes its qdisc with it: heal only a cluster that stays up / is restarted on.
        if stale or keep:
            faults.heal_all(c)
        if stale:
            c.compose.down(keep_volumes=True)
        elif not keep:
            # DR_BG_TEARDOWN=0: synchronous teardown - no second cluster per worker, ever (memory-tight hosts / -n 3).
            if os.environ.get("DR_BG_TEARDOWN", "1") == "1":
                _background_down(c)
            else:
                c.down()


@pytest.fixture(scope="session")
def netem_ready():
    """Ensure the netem sidecar image exists (for network-fault tests)."""
    faults.ensure_netem_image()
    return True


# Longest modules first (measured 2026-10-15, timing_report): with xdist --dist load the last tests handed out decide
# the wall time, so the multi-minute fault scenarios must not be the tail.  A stable sort: the order inside a module
# is unchanged (module-scoped `cluster` users stay contiguous).
_LONG_FIRST = ("test_dr8_", "test_dr13_", "test_dr15_", "test_dr3_", "test_compaction_", "test_cg_", "test_dr11_",
               "test_dr10_", "test_dr4_", "test_dr7_", "test_dr14_")


def pytest_collection_modifyitems(session, config, items):
    def rank(item):
        name = os.path.basename(str(item.fspath))
        for i, prefix in enumerate(_LONG_FIRST):
            if name.startswith(prefix):
                return i
        return len(_LONG_FIRST)
    items.sort(key=rank)


@pytest.fixture(autouse=True)
def _host_test_lock(request):
    """xdist isolation for CPU-heavy scenarios.  Every test holds a SHARED flock on DR_HOST_LOCK; a test marked
    `exclusive` holds it EXCLUSIVELY, so it never shares the 4-core host with another worker's cluster (measured
    2026-10-15: under -n 3 the 15-client reconnect storm saw no re-election within 90 s at load ~28; alone it
    passes).  A gate lock gives the exclusive taker priority: it holds the gate while it waits, and shared takers
    pass the gate first, so a stream of short tests cannot starve it.  Autouse + function scope: taken before the
    test's own cluster comes up.  A no-op outside xdist."""
    path = os.environ.get("DR_HOST_LOCK")
    if not path or not os.environ.get("PYTEST_XDIST_WORKER"):
        yield
        return
    import fcntl
    exclusive = request.node.get_closest_marker("exclusive") is not None
    gate = open(path + ".gate", "a")
    main = open(path, "a")
    try:
        with timing.phase("host_lock.wait", "exclusive" if exclusive else "shared"):
            fcntl.flock(gate, fcntl.LOCK_EX)
            if exclusive:
                fcntl.flock(main, fcntl.LOCK_EX)
            else:
                fcntl.flock(main, fcntl.LOCK_SH)
            fcntl.flock(gate, fcntl.LOCK_UN)
        yield
    finally:
        fcntl.flock(main, fcntl.LOCK_UN)
        main.close()
        gate.close()
