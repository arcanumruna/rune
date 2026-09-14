"""THE parametrization framework.

The whole DR suite (DR-2..15) rests on one idea: **a disaster is never one test
— it is a MATRIX** across many dimensions.  This module turns a declaration of
dimension values into a ready-to-use `pytest.mark.parametrize` cross-product so
a later ticket can express hundreds of cases in a single line.

Dimension vocabulary (the axes a DR scenario sweeps):

    target      — which node the fault hits / clients target:
                  "leader" | "follower" | "any" | 0 | 1 | 2
    count       — how many nodes are affected (1 for a 3-node quorum-safe fault,
                  2 = quorum loss).
    timing      — WHEN the fault fires relative to the workload:
                  "before" | "mid_produce" | "mid_subscribe" | "after" | "idle"
    intensity   — fault magnitude: for netem a % (loss/dup) or ms (delay);
                  for kill a signal; for pause a duration ms.
    client_state— what the clients are doing: "producing" | "subscribing" |
                  "both" | "idle" | "slow_subscriber" | "reconnect_storm"
    data_state  — the data shape: (keys, records) e.g. (1, 20) or (16, 5000).
    repetition  — repeat index, to catch flakiness / non-determinism.
    recovery    — how the fault is undone: "restart" | "heal" | "unpause" |
                  "none" (permanent).

`matrix(**dimensions)` returns a list of `Case` objects (frozen dataclasses) and
`parametrize(**dimensions)` returns the decorator you stack on a test.  Only the
dimensions you pass are swept; the rest keep their defaults, so small matrices
stay small and large ones are one edit away.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field, fields

import pytest


# The canonical dimension vocabulary + defaults.  A Case is a fully-resolved
# point in this space.
@dataclass(frozen=True)
class Case:
    target: object = "leader"
    count: int = 1
    timing: str = "before"
    intensity: object = None
    client_state: str = "both"
    data_state: tuple = (1, 20)   # (keys, records)
    repetition: int = 0
    recovery: str = "none"
    # free-form extra knobs a specific ticket wants swept without touching Case.
    extra: tuple = field(default_factory=tuple)

    @property
    def keys(self) -> int:
        return self.data_state[0]

    @property
    def records(self) -> int:
        return self.data_state[1]

    def id(self) -> str:
        """Compact, unique pytest test id."""
        parts = [
            f"t={self.target}",
            f"n={self.count}",
            f"when={self.timing}",
            f"i={self.intensity}",
            f"cs={self.client_state}",
            f"keys={self.keys}",
            f"recs={self.records}",
            f"rec={self.recovery}",
            f"r{self.repetition}",
        ]
        if self.extra:
            parts.append("x=" + ",".join(f"{k}:{v}" for k, v in self.extra))
        return "|".join(parts)


_DIM_NAMES = {f.name for f in fields(Case)} - {"extra"}


def _as_list(v):
    # A list is already a set of values to sweep; anything else is one value.
    return list(v) if isinstance(v, list) else [v]


def matrix(**dimensions) -> list[Case]:
    """Cross-product of the supplied dimension value-lists into Cases.

    Any dimension not supplied stays at its Case default.  `repetition` may be
    given as an int N (shorthand for range(N)) or an explicit list.

    Example:
        matrix(target=["leader", "follower"],
               data_state=[(1, 20), (4, 20)],
               timing=["before", "mid_produce"])
        -> 2 * 2 * 2 = 8 Cases.
    """
    axes: dict[str, list] = {}
    for name, val in dimensions.items():
        if name not in _DIM_NAMES and name != "extra":
            raise ValueError(
                f"unknown dimension {name!r}; known: {sorted(_DIM_NAMES)} (+extra)"
            )
        if name == "repetition" and isinstance(val, int):
            axes[name] = list(range(val))
        else:
            axes[name] = _as_list(val)

    if not axes:
        return [Case()]

    names = list(axes.keys())
    combos = itertools.product(*(axes[n] for n in names))
    cases = [Case(**dict(zip(names, combo))) for combo in combos]
    return cases


def parametrize(argname: str = "case", **dimensions):
    """Return a pytest.mark.parametrize decorator over the matrix cross-product.

    One-liner a later ticket uses:

        @matrix.parametrize(target=["leader","follower"],
                            data_state=[(1,20),(16,2000)],
                            timing=["mid_produce","mid_subscribe"],
                            intensity=[10, 50], repetition=3)
        def test_partition(case, cluster): ...
        # -> 2*2*2*2*3 = 48 parametrized cases, ids auto-generated.
    """
    cases = matrix(**dimensions)
    return pytest.mark.parametrize(argname, cases, ids=[c.id() for c in cases])
