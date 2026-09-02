"""Token-bucket rate limiting.

Classic token bucket: capacity tokens, refilled continuously at `refill_rate`
tokens/sec. A call either takes a token immediately or reports how long to
wait. `acquire_async` parks the coroutine via an injectable `sleep`, so in
tests you advance a FakeClock instead of actually waiting.
"""
from __future__ import annotations

from typing import Callable, Optional

from .clock import Clock


class TokenBucket:
    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        clock: Clock,
    ) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if refill_rate <= 0:
            raise ValueError("refill_rate must be > 0")
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._clock = clock
        self._tokens = float(capacity)
        self._last = clock.now()

    def _refill(self) -> None:
        now = self._clock.now()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
            self._last = now

    @property
    def available(self) -> float:
        self._refill()
        return self._tokens

    def try_acquire(self, tokens: int = 1) -> bool:
        """Take `tokens` if available now; never blocks. Returns success."""
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    def wait_time(self, tokens: int = 1) -> float:
        """Seconds until `tokens` will be available (0.0 if available now)."""
        self._refill()
        deficit = tokens - self._tokens
        if deficit <= 0:
            return 0.0
        return deficit / self.refill_rate

    async def acquire(
        self,
        tokens: int = 1,
        sleep: Optional[Callable[[float], "object"]] = None,
    ) -> float:
        """Wait until `tokens` are granted; return seconds actually waited."""
        import asyncio as _asyncio

        _sleep = sleep or _asyncio.sleep
        waited = 0.0
        while True:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return waited
            dt = self.wait_time(tokens)
            await _sleep(dt)  # type: ignore[operator]
            # advance internal clock baseline is handled by _refill via real clock
            self._refill()
            waited += dt
