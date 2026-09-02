"""The resilient fetcher: retry loop + rate limiting + circuit breaker.

Policy, in the order each attempt passes through it:
  1. circuit.check()       — downstream on fire? don't even try (fast fail)
  2. bucket.acquire()      — respect the configured request rate
  3. transport.get()       — the one thing that can't be unit-tested here
  4. record outcome        — feed the breaker
  5. if retryable: sleep(backoff(attempt)) — with jitter to avoid thundering herd

Retries are additionally bounded by a *retry budget* (max total extra
attempts across a batch), not just per-request — because a fleet of
"3 retries each" is how you take down the service you're rescuing.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, List, Optional

from .backoff import ExponentialBackoff
from .circuit import CircuitBreaker, CircuitOpenError
from .ratelimit import TokenBucket
from .transport import Result, Transport


@dataclass
class FetchPolicy:
    max_attempts: int = 4
    timeout: float = 10.0
    retry_budget: Optional[int] = None  # per-batch cap on retries


@dataclass
class FetchOutcome:
    url: str
    result: Result
    attempts: int
    retries_used: int
    aborted_by_circuit: bool = False

    @property
    def ok(self) -> bool:
        return self.result.ok


class RetryBudgetExhausted(RuntimeError):
    pass


class RetryBudget:
    """Token-style budget: retries cost 1; it refills every `window` seconds
    to `capacity` (Google SRE style: rate-based, not just a global cap)."""

    def __init__(self, capacity: int, refill_per_sec: float, clock) -> None:
        self._bucket = TokenBucket(capacity=max(1, capacity), refill_rate=refill_per_sec, clock=clock)

    def consume(self) -> bool:
        return self._bucket.try_acquire()


class ResilientFetcher:
    def __init__(
        self,
        transport: Transport,
        bucket: TokenBucket,
        breaker: CircuitBreaker,
        backoff: ExponentialBackoff,
        policy: Optional[FetchPolicy] = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        budget: Optional[RetryBudget] = None,
    ) -> None:
        self.transport = transport
        self.bucket = bucket
        self.breaker = breaker
        self.backoff = backoff
        self.policy = policy or FetchPolicy()
        self._sleep = sleep
        self.budget = budget

    async def fetch(self, url: str) -> FetchOutcome:
        retries_used = 0
        attempt = 0
        last: Optional[Result] = None
        while attempt < self.policy.max_attempts:
            attempt += 1
            try:
                self.breaker.check()
            except CircuitOpenError as e:
                if attempt == 1:
                    return FetchOutcome(url, Result(status=0, error=str(e)), attempt, retries_used, aborted_by_circuit=True)
                # a circuit that opened mid-retry: stop hammering
                assert last is not None
                return FetchOutcome(url, last, attempt - 1, retries_used, aborted_by_circuit=True)

            await self.bucket.acquire(1, sleep=self._sleep)

            last = await self.transport.get(url, timeout=self.policy.timeout)
            if last.ok:
                self.breaker.record_success()
                return FetchOutcome(url, last, attempt, retries_used)

            self.breaker.record_failure()
            if not last.retryable:
                return FetchOutcome(url, last, attempt, retries_used)  # 4xx (other than 408/429): retry won't help

            if attempt >= self.policy.max_attempts:
                break  # exhausted per-request attempts

            if self.budget is not None and not self.budget.consume():
                raise RetryBudgetExhausted(
                    f"retry budget exhausted while fetching {url} after {attempt} attempts"
                )
            retries_used += 1
            await self._sleep(self.backoff.delay(attempt))

        assert last is not None
        return FetchOutcome(url, last, attempt, retries_used)

    async def fetch_many(self, urls: List[str], concurrency: int = 8) -> List[FetchOutcome]:
        """Bounded fan-out: at most `concurrency` in flight — resilience also
        means not becoming the flood yourself."""
        sem = asyncio.Semaphore(concurrency)

        async def _one(u: str) -> FetchOutcome:
            async with sem:
                return await self.fetch(u)

        return await asyncio.gather(*(_one(u) for u in urls))
