"""Deterministic primitives shared by the tests."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import List

from resilient_fetcher.clock import FakeClock
from resilient_fetcher.transport import Result


def run(coro):
    return asyncio.run(coro)


class FakeSleep:
    """Awaitable sleep that advances the FakeClock instead of waiting."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.calls: List[float] = []

    async def __call__(self, dt: float) -> None:
        self.calls.append(dt)
        self.clock.advance(dt)


class ScriptedTransport:
    """Returns pre-scripted Results per URL, cycling on exhaustion. Counts
    calls and tracks max concurrency observed."""

    def __init__(self, script: dict | None = None, delay: float = 0.0) -> None:
        self.script = {u: list(v) for u, v in (script or {}).items()}
        self.calls: List[str] = []
        self.max_concurrent = 0
        self._active = 0
        self._delay = delay

    async def get(self, url: str, timeout: float) -> Result:
        self._active += 1
        self.max_concurrent = max(self.max_concurrent, self._active)
        try:
            if self._delay:
                await asyncio.sleep(self._delay)
            self.calls.append(url)
            seq = self.script.get(url)
            if seq is None:
                return Result(status=200, body=b"ok")
            item = seq.pop(0) if seq else self.script[f"{url}:last"]
            if isinstance(item, Callable):  # allow raising to simulate exceptions
                item = item()
            return item
        finally:
            self._active -= 1


def ok(body: bytes = b"ok") -> Result:
    return Result(status=200, body=body)


def fail(status: int = 500) -> Result:
    """A real HTTP error answer: status carries it, no transport error."""
    return Result(status=status)


def net_err(msg: str = "URLError: timed out") -> Result:
    return Result(status=0, error=msg)
