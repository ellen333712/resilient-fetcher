"""Unit tests for the standalone primitives: bucket, backoff, breaker, Result."""
from __future__ import annotations

import unittest

from resilient_fetcher.backoff import ExponentialBackoff
from resilient_fetcher.circuit import CircuitBreaker, CircuitOpenError, State
from resilient_fetcher.clock import FakeClock
from resilient_fetcher.ratelimit import TokenBucket
from resilient_fetcher.transport import Result


class TokenBucketTest(unittest.TestCase):
    def test_burst_then_deny(self):
        clock = FakeClock()
        b = TokenBucket(capacity=3, refill_rate=1.0, clock=clock)
        self.assertTrue(b.try_acquire())
        self.assertTrue(b.try_acquire())
        self.assertTrue(b.try_acquire())
        self.assertFalse(b.try_acquire())  # empty

    def test_continuous_refill(self):
        clock = FakeClock()
        b = TokenBucket(capacity=2, refill_rate=0.5, clock=clock)  # 1 token / 2s
        b.try_acquire(2)
        clock.advance(1.0)
        self.assertFalse(b.try_acquire())  # only 0.5 tokens back
        clock.advance(1.5)
        self.assertTrue(b.try_acquire())

    def test_capacity_clamps_refill(self):
        clock = FakeClock()
        b = TokenBucket(capacity=2, refill_rate=100.0, clock=clock)
        b.try_acquire(2)
        clock.advance(10)  # a huge elapsed time must not overfill
        self.assertAlmostEqual(b.available, 2.0)

    def test_wait_time_reports_exact_gap(self):
        clock = FakeClock()
        b = TokenBucket(capacity=1, refill_rate=0.25, clock=clock)  # 4s/token
        b.try_acquire()
        self.assertAlmostEqual(b.wait_time(), 4.0)

    def test_invalid_config(self):
        clock = FakeClock()
        with self.assertRaises(ValueError):
            TokenBucket(capacity=0, refill_rate=1, clock=clock)
        with self.assertRaises(ValueError):
            TokenBucket(capacity=1, refill_rate=0, clock=clock)


class BackoffTest(unittest.TestCase):
    def test_exponential_growth_capped(self):
        bo = ExponentialBackoff(base=1, factor=2, max_delay=10, jitter="none")
        self.assertEqual([bo.delay(i) for i in range(1, 6)], [1, 2, 4, 8, 10])

    def test_full_jitter_scales_between_zero_and_capped(self):
        bo = ExponentialBackoff(base=1, factor=2, max_delay=10, jitter="full", rng=lambda: 0.0)
        self.assertEqual(bo.delay(3), 0.0)
        bo1 = ExponentialBackoff(base=1, factor=2, max_delay=10, jitter="full", rng=lambda: 1.0)
        self.assertEqual(bo1.delay(3), 4.0)  # deterministic with rng pinned

    def test_equal_jitter_bisects(self):
        bo = ExponentialBackoff(base=2, factor=2, max_delay=100, jitter="equal", rng=lambda: 0.0)
        self.assertEqual(bo.delay(2), 2.0)  # capped=4, low half fixed at cap/2

    def test_decorrelated_never_exceeds_cap(self):
        bo = ExponentialBackoff(base=1, factor=2, max_delay=5, jitter="decorrelated", rng=lambda: 0.999)
        for attempt in range(1, 20):
            self.assertLessEqual(bo.delay(attempt), 5.0)

    def test_bad_attempt_rejected(self):
        bo = ExponentialBackoff()
        with self.assertRaises(ValueError):
            bo.delay(0)


class CircuitBreakerTest(unittest.TestCase):
    def make(self, threshold=3, recovery=10.0, success=2):
        self.clock = FakeClock()
        return CircuitBreaker(
            self.clock,
            failure_threshold=threshold,
            recovery_timeout=recovery,
            success_threshold=success,
        )

    def test_opens_after_consecutive_failures(self):
        cb = self.make(threshold=3)
        cb.record_failure()
        cb.record_failure()
        cb.record_success()          # success resets the consecutive streak
        cb.record_failure()
        cb.record_failure()
        self.assertIs(cb.state, State.CLOSED)
        cb.record_failure()
        self.assertIs(cb.state, State.OPEN)
        with self.assertRaises(CircuitOpenError) as ctx:
            cb.check()
        self.assertAlmostEqual(ctx.exception.retry_after, 10.0)

    def test_half_open_probe_then_close(self):
        cb = self.make(threshold=1, recovery=5.0, success=2)
        cb.record_failure()
        self.assertIs(cb.state, State.OPEN)
        self.clock.advance(5.0)
        self.assertIs(cb.state, State.HALF_OPEN)  # transition observed via state
        cb.check()                                 # probes allowed
        cb.record_success()
        self.assertIs(cb.state, State.HALF_OPEN)   # need 2 successes
        cb.record_success()
        self.assertIs(cb.state, State.CLOSED)

    def test_half_open_failure_reopens_immediately(self):
        cb = self.make(threshold=1, recovery=5.0)
        cb.record_failure()
        self.clock.advance(5.0)
        self.assertIs(cb.state, State.HALF_OPEN)
        cb.record_failure()
        self.assertIs(cb.state, State.OPEN)
        self.clock.advance(4.9)
        self.assertIs(cb.state, State.OPEN)        # fresh recovery window
        self.clock.advance(0.2)
        self.assertIs(cb.state, State.HALF_OPEN)


class ResultTest(unittest.TestCase):
    def test_ok_matrix(self):
        self.assertTrue(Result(status=200).ok)
        self.assertTrue(Result(status=204).ok)
        self.assertFalse(Result(status=200, error="URLError").ok)
        self.assertFalse(Result(status=500).ok)

    def test_retryable_matrix(self):
        self.assertTrue(Result(status=503).retryable)
        self.assertTrue(Result(status=429).retryable)
        self.assertTrue(Result(status=408).retryable)
        self.assertTrue(Result(status=0, error="timeout").retryable)
        self.assertFalse(Result(status=404).retryable)   # a retry won't fix a 404
        self.assertFalse(Result(status=400).retryable)


if __name__ == "__main__":
    unittest.main()
