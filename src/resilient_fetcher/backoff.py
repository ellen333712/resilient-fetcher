"""Backoff schedules.

Exponential growth capped at `max_delay`, then one of two jitter policies:
  * full    — uniform in [0, capped]        (AWS "full jitter")
  * decorrelated — sleep = min(cap, rand(base, prev*3))  (AWS article)
Randomness is injected so tests are deterministic.
"""
from __future__ import annotations

import random
from typing import Callable, Optional


class ExponentialBackoff:
    def __init__(
        self,
        base: float = 0.5,
        factor: float = 2.0,
        max_delay: float = 30.0,
        jitter: str = "full",
        rng: Optional[Callable[[], float]] = None,
    ) -> None:
        if base <= 0:
            raise ValueError("base must be > 0")
        if factor < 1:
            raise ValueError("factor must be >= 1")
        if jitter not in ("none", "full", "equal", "decorrelated"):
            raise ValueError(f"unknown jitter policy {jitter!r}")
        self.base = base
        self.factor = factor
        self.max_delay = max_delay
        self.jitter = jitter
        self._rng: Callable[[], float] = rng or random.random
        self._prev = base

    def _rand(self, lo: float, hi: float) -> float:
        return lo + (hi - lo) * self._rng()

    def delay(self, attempt: int) -> float:
        """Delay before retry number `attempt` (attempt=1 is the first retry)."""
        if attempt < 1:
            raise ValueError("attempt starts at 1")
        capped = min(self.max_delay, self.base * (self.factor ** (attempt - 1)))
        if self.jitter == "none":
            return capped
        if self.jitter == "full":
            return capped * self._rng()
        if self.jitter == "equal":
            return capped / 2.0 + capped * self._rng() / 2.0
        # decorrelated
        d = min(self.max_delay, self._rand(self.base, self._prev * 3.0))
        self._prev = d
        return d
