"""resilient-fetcher: concurrency resilience primitives, stdlib only.

Public API:
  ResilientFetcher   — retry loop around a Transport, gated by TokenBucket
                       and CircuitBreaker, delayed by ExponentialBackoff
  FetchPolicy        — attempts/timeout/budget knobs
  RetryBudget        — batch-level rate-limited retry allowance
  TokenBucket / CircuitBreaker / ExponentialBackoff / UrllibTransport
                       — usable standalone; the whole point is that these
                       compose but do not depend on each other
"""
from .backoff import ExponentialBackoff
from .circuit import CircuitBreaker, CircuitOpenError, State
from .clock import Clock, FakeClock, MonotonicClock
from .fetcher import FetchOutcome, FetchPolicy, ResilientFetcher, RetryBudget, RetryBudgetExhausted
from .ratelimit import TokenBucket
from .transport import Result, Transport, UrllibTransport

__all__ = [
    "CircuitBreaker",
    "CircuitOpenError",
    "Clock",
    "ExponentialBackoff",
    "FakeClock",
    "FetchOutcome",
    "FetchPolicy",
    "MonotonicClock",
    "ResilientFetcher",
    "Result",
    "RetryBudget",
    "RetryBudgetExhausted",
    "State",
    "TokenBucket",
    "Transport",
    "UrllibTransport",
]

__version__ = "0.1.0"
