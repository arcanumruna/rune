"""Per-phase timing log for the dr-e2e harness (DR_TIMING_LOG=<path>).

Every harness phase (image preparation, compose up/down, leader waits, topic DDL, client
container runs, fault inject/heal, sleeps, poll loops) is wrapped in `phase(kind, detail)`.
With DR_TIMING_LOG set, each finished phase appends one JSON line:

    {"test": <nodeid>, "when": setup|call|teardown|session, "kind": ..., "detail": ...,
     "t0": <epoch s>, "dur": <s>, "depth": <nesting depth on this thread>, "main": <bool>,
     "exit": <exit reason, for polls>}

`depth`/`main` let `tools/timing_report.py` compute EXCLUSIVE time per kind (a phase minus its
direct children) on the main thread, so nested phases are never double-counted.  Unset
DR_TIMING_LOG = no file I/O; the phase stack is still kept (it is cheap and thread-local to this
module's caller only).
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
import time

_LOG = os.environ.get("DR_TIMING_LOG")
_lock = threading.Lock()          # serialises appends from the driver's client threads
_local = threading.local()

# The pytest item currently running (set by conftest hooks); "session" outside any test.
current = {"test": "session", "when": "session"}


def _depth() -> int:
    return getattr(_local, "depth", 0)


def emit(kind: str, detail: str, t0: float, dur: float, depth: int, exit_reason: str | None = None) -> None:
    if not _LOG:
        return
    rec = {"test": current["test"], "when": current["when"], "kind": kind, "detail": detail,
           "t0": round(t0, 3), "dur": round(dur, 3), "depth": depth,
           "main": threading.current_thread() is threading.main_thread(),
           "worker": os.environ.get("PYTEST_XDIST_WORKER", "")}
    if exit_reason is not None:
        rec["exit"] = exit_reason
    line = json.dumps(rec)
    with _lock:
        with open(_LOG, "a") as fh:
            fh.write(line + "\n")


class _Phase:
    __slots__ = ("exit",)

    def __init__(self) -> None:
        self.exit: str | None = None


@contextlib.contextmanager
def phase(kind: str, detail: str = ""):
    """Time a block.  `with phase("leader.wait", svc) as p: ...; p.exit = "found"` records why a
    poll ended (an exception records `exc:<Type>`)."""
    d = _depth()
    _local.depth = d + 1
    p = _Phase()
    t0 = time.time()
    try:
        yield p
    except BaseException as e:  # noqa: BLE001 - recorded, re-raised
        if p.exit is None:
            p.exit = f"exc:{type(e).__name__}"
        raise
    finally:
        _local.depth = d
        emit(kind, detail, t0, time.time() - t0, d, p.exit)


def timed(kind: str, detail_arg: int | None = None):
    """Decorator form of `phase`: `detail_arg` = index of a positional arg used as the detail."""
    import functools

    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            detail = ""
            if detail_arg is not None and len(args) > detail_arg:
                detail = str(args[detail_arg])
            with phase(kind, detail):
                return fn(*args, **kwargs)
        return wrapper
    return deco


_real_sleep = time.sleep


def _timed_sleep(seconds: float) -> None:
    """time.sleep replacement installed by conftest: every main-thread sleep >= 0.5 s is logged with
    its caller (file:line) so fixed sleeps show up by location."""
    if not _LOG or seconds < 0.5 or threading.current_thread() is not threading.main_thread():
        _real_sleep(seconds)
        return
    import sys
    f = sys._getframe(1)
    where = f"{os.path.basename(f.f_code.co_filename)}:{f.f_lineno}"
    with phase("sleep", f"{where} {seconds:g}s"):
        _real_sleep(seconds)


def install_sleep_probe() -> None:
    if _LOG:
        time.sleep = _timed_sleep
