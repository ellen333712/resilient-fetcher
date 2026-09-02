"""Time source abstraction.

Every timing component takes a `Clock` so behaviour is deterministic under
test: a `FakeClock` lets you advance time by hand and assert exactly when a
token refills or a circuit half-opens, with no sleeping and no flakiness.
"""
from __future__ import annotations

import time
from abc import ABC, abstractmethod


class Clock(ABC):
    @abstractmethod
    def now(self) -> float:  # monotonic seconds
        ...


class MonotonicClock:
    """Wall-clock-immune monotonic time, as it should be for timeouts/backoff."""

    def now(self) -> float:
        return time.monotonic()


class FakeClock:
    """Manually-advanced clock for tests. `advance` is the only way time moves."""

    def __init__(self, start: float = 0.0) -> None:
        self._t = float(start)

    def now(self) -> float:
        return self._t

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("time cannot go backward")
        self._t += seconds
        return self._t
