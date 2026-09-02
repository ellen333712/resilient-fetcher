"""Circuit breaker.

CLOSED → (failure_threshold consecutive failures) → OPEN →
(recovery_timeout elapses) → HALF_OPEN → (success_threshold successes) → CLOSED
A single failure in HALF_OPEN re-opens immediately. OPEN rejects *before* the
network call — that's the point: give the downstream service room to recover.
"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from .clock import Clock


class State(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is rejected because the circuit is OPEN."""

    def __init__(self, retry_after: float) -> None:
        super().__init__(f"circuit is open; retry after {retry_after:.2f}s")
        self.retry_after = retry_after


class CircuitBreaker:
    def __init__(
        self,
        clock: Clock,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
        name: str = "circuit",
    ) -> None:
        if failure_threshold < 1 or success_threshold < 1:
            raise ValueError("thresholds must be >= 1")
        self._clock = clock
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.name = name
        self._state = State.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at: Optional[float] = None

    @property
    def state(self) -> State:
        if self._state is State.OPEN and self._opened_at is not None:
            if self._clock.now() - self._opened_at >= self.recovery_timeout:
                self._transition(State.HALF_OPEN)
        return self._state

    def _transition(self, to: State) -> None:
        self._state = to
        if to is State.OPEN:
            self._opened_at = self._clock.now()
            self._failures = 0
        elif to is State.HALF_OPEN:
            self._successes = 0
        elif to is State.CLOSED:
            self._failures = 0
            self._successes = 0

    # ── gating API ────────────────────────────────────────────────
    def check(self) -> None:
        """Raise CircuitOpenError if calls must not be made right now."""
        state = self.state  # property may auto-transition OPEN→HALF_OPEN
        if state is State.OPEN:
            assert self._opened_at is not None
            remaining = self.recovery_timeout - (self._clock.now() - self._opened_at)
            raise CircuitOpenError(max(0.0, remaining))

    # ── outcome reporting ─────────────────────────────────────────
    def record_success(self) -> None:
        state = self.state
        if state is State.HALF_OPEN:
            self._successes += 1
            if self._successes >= self.success_threshold:
                self._transition(State.CLOSED)
        elif state is State.CLOSED:
            self._failures = 0

    def record_failure(self) -> None:
        state = self.state
        if state is State.HALF_OPEN:
            self._transition(State.OPEN)
        elif state is State.CLOSED:
            self._failures += 1
            if self._failures >= self.failure_threshold:
                self._transition(State.OPEN)
