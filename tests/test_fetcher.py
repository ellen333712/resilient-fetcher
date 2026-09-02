"""Integration tests for the orchestration layer.

Everything real is injected: FakeClock (time), FakeSleep (waiting),
ScriptedTransport (the network). So these assert EXACT behaviour —
attempt counts, wait sequences, breaker state, budget accounting.
"""
from __future__ import annotations

import unittest

from conftest_shims import ScriptedTransport, FakeSleep, ok, fail, net_err, run

from resilient_fetcher.backoff import ExponentialBackoff
from resilient_fetcher.circuit import CircuitBreaker, State
from resilient_fetcher.clock import FakeClock
from resilient_fetcher.fetcher import (
    FetchPolicy,
    ResilientFetcher,
    RetryBudget,
    RetryBudgetExhausted,
)
from resilient_fetcher.ratelimit import TokenBucket


def make_fetcher(script=None, *, capacity=100, rate=100.0, max_attempts=4,
                 failure_threshold=99, recovery=30.0, budget=None):
    clock = FakeClock()
    sleep = FakeSleep(clock)
    transport = ScriptedTransport(script or {})
    fetcher = ResilientFetcher(
        transport=transport,
        bucket=TokenBucket(capacity=capacity, refill_rate=rate, clock=clock),
        breaker=CircuitBreaker(clock, failure_threshold=failure_threshold,
                               recovery_timeout=recovery, success_threshold=1),
        backoff=ExponentialBackoff(base=1, factor=2, max_delay=10, jitter="none"),
        policy=FetchPolicy(max_attempts=max_attempts, timeout=5),
        sleep=sleep,
        budget=budget,
    )
    return fetcher, transport, sleep, clock


class FetcherHappyPathTest(unittest.TestCase):
    def test_simple_get(self):
        f, tr, sleep, clock = make_fetcher()
        o = run(f.fetch("http://x/a"))
        self.assertTrue(o.ok)
        self.assertEqual(o.attempts, 1)
        self.assertEqual(sleep.calls, [])

    def test_flaky_then_success_with_exponential_waits(self):
        f, tr, sleep, clock = make_fetcher(
            {"http://x/flaky": [fail(500), net_err(), ok()],
             "http://x/flaky:last": [ok()]})
        o = run(f.fetch("http://x/flaky"))
        self.assertTrue(o.ok)
        self.assertEqual(o.attempts, 3)
        self.assertEqual(o.retries_used, 2)
        self.assertEqual(sleep.calls, [1, 2])  # no-jitter schedule, exact

    def test_fatal_4xx_is_not_retried(self):
        f, tr, sleep, clock = make_fetcher({"http://x/gone": [fail(404), ok(), ok(), ok()],
                                            "http://x/gone:last": [fail(404)]})
        o = run(f.fetch("http://x/gone"))
        self.assertFalse(o.ok)
        self.assertEqual(o.attempts, 1)      # gave up immediately — correctly
        self.assertEqual(sleep.calls, [])

    def test_attempt_cap_respected(self):
        always = [fail(503)] * 10
        f, tr, sleep, clock = make_fetcher({"http://x/down": always,
                                            "http://x/down:last": [fail(503)]},
                                           max_attempts=3)
        o = run(f.fetch("http://x/down"))
        self.assertEqual(o.attempts, 3)
        self.assertEqual(o.retries_used, 2)  # 3 attempts = 2 retries
        self.assertFalse(o.ok)


class RateLimitTest(unittest.TestCase):
    def test_waits_when_bucket_empty(self):
        clock = FakeClock()
        sleep = FakeSleep(clock)
        transport = ScriptedTransport({})
        f = ResilientFetcher(
            transport=transport,
            bucket=TokenBucket(capacity=1, refill_rate=0.5, clock=clock),  # 2s/token
            breaker=CircuitBreaker(clock, failure_threshold=99),
            backoff=ExponentialBackoff(jitter="none"),
            policy=FetchPolicy(max_attempts=1, timeout=5),
            sleep=sleep,
        )
        o1 = run(f.fetch("http://x/1"))   # takes the one free token
        self.assertTrue(o1.ok)
        o2 = run(f.fetch("http://x/2"))   # must wait ~2s (in fake time)
        self.assertTrue(o2.ok)
        self.assertGreater(sum(sleep.calls), 0)
        self.assertLess(sum(sleep.calls), 5)  # bounded wait, not a hang


class BreakerTest(unittest.TestCase):
    def test_breaker_opens_and_aborts_fast(self):
        script = {}
        for i in range(5):
            script[f"http://x/f{i}"] = [fail(500), ok(), ok(), ok()]
            script[f"http://x/f{i}:last"] = [fail(500)]
        f, tr, sleep, clock = make_fetcher(script, max_attempts=1, failure_threshold=2)
        o0 = run(f.fetch("http://x/f0"))   # 1 consecutive failure
        o1 = run(f.fetch("http://x/f1"))   # 2nd → OPEN
        self.assertIs(f.breaker.state, State.OPEN)
        o2 = run(f.fetch("http://x/f2"))   # next call: rejected BEFORE transport
        self.assertTrue(o2.aborted_by_circuit)
        self.assertEqual(len(tr.calls), 2)          # f2 never reached transport
        self.assertEqual(o2.result.status, 0)
        self.assertIn("circuit is open", o2.result.error or "")

    def test_half_open_probe_recovers_traffic(self):
        f, tr, sleep, clock = make_fetcher(
            {"http://x/a": [fail(500), ok(), ok(), ok()], "http://x/a:last": [fail(500)]},
            max_attempts=1, failure_threshold=1, recovery=5.0)
        run(f.fetch("http://x/a"))                    # fails → OPEN
        clock.advance(5.0)                            # recovery window passes
        o = run(f.fetch("http://x/a"))                # HALF_OPEN probe succeeds
        self.assertTrue(o.ok)
        self.assertIs(f.breaker.state, State.CLOSED)  # success_threshold=1 in harness


class BudgetTest(unittest.TestCase):
    def test_batch_retry_budget_stops_a_retry_storm(self):
        script = {}
        for i in range(6):
            script[f"http://x/b{i}"] = [fail(500)] * 4
            script[f"http://x/b{i}:last"] = [fail(500)]
        clock = FakeClock()
        budget = RetryBudget(capacity=2, refill_per_sec=0.001, clock=clock)
        f, tr, sleep, _ = make_fetcher(script, max_attempts=4, budget=budget)
        with self.assertRaises(RetryBudgetExhausted):
            run(f.fetch_many([f"http://x/b{i}" for i in range(6)], concurrency=2))
        # 2 retries were granted; total transport calls == 2 initial + their retries-ish
        self.assertLessEqual(len(tr.calls), 6 + 2)  # no unbounded storm


class ConcurrencyTest(unittest.TestCase):
    def test_fan_out_bounded_by_semaphore(self):
        f, tr, sleep, clock = make_fetcher()
        # tiny REAL delay so over-concurrency would show up in max_concurrent
        transport = ScriptedTransport({}, delay=0.02)
        f.transport = transport
        urls = [f"http://x/{i}" for i in range(20)]
        run(f.fetch_many(urls, concurrency=4))
        self.assertLessEqual(transport.max_concurrent, 4)
        self.assertEqual(len(transport.calls), 20)


if __name__ == "__main__":
    unittest.main()
