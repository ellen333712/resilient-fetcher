# resilient-fetcher

> Concurrency resilience as it should be taught: a **token-bucket rate
> limiter**, **exponential backoff with jitter**, a **circuit breaker**, and
> a batch-level **retry budget**, composed around one pluggable transport —
> in pure stdlib Python, with deterministic tests that never touch a socket
> or wait a real second.

![python](https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue)
![tests](https://img.shields.io/badge/tests-27%20passing-brightgreen)
![deps](https://img.shields.io/badge/runtime%20dependencies-0-yellowgreen)
[![CI](https://github.com/<owner>/resilient-fetcher/actions/workflows/ci.yml/badge.svg)](https://github.com/<owner>/resilient-fetcher/actions)

## Why a wheel reinvention?

Because half of production incidents are people *misusing* these wheels:
retries without budgets (the retry storm that kills the recovering service),
backoff without jitter (thundering herd), circuit breakers that open for
fatal 4xx (which a retry can never fix). Implementing the primitives
yourself, small and testable, is the fastest way to internalize *why* each
piece exists — and you get a clean, dependency-free library out of it.

## The four primitives

```
 caller ──▶ ResilientFetcher.fetch(url)
              │
              ├─ 1. CircuitBreaker.check()      downstream on fire? fail fast
              ├─ 2. TokenBucket.acquire()       obey the rate; wait politely
              ├─ 3. Transport.get(url)          the single seam to the network
              ├─ 4. breaker.record(outcome)     feed the state machine
              └─ 5. if retryable:               408/429/5xx/net — never 404
                     RetryBudget.consume()      batch-wide cap on retries
                     await sleep(ExponentialBackoff.delay(attempt))
```

Each component is **standalone, injectable, and clock-agnostic** — the
breaker, the bucket, and the backoff all accept a `Clock` (and the backoff a
rng), so the test suite asserts exact behaviour with a `FakeClock`, zero
sleeps, and a scripted transport.

### Design decisions worth arguing about in an interview

- **`status` and `error` never mix.** A `Result` carries an HTTP status *or*
  a transport error. Conflating them makes a 404 look like a blip and gets
  it retried to death. (Caught by a test.)
- **Retries are budgeted per batch, not just per request.** `max_retries=3`
  × 10 000 URLs is still a DDOS. `RetryBudget` is a token bucket *for
  retries* (Google SRE style).
- **The breaker counts *consecutive* failures**, and any success resets the
  streak — otherwise one 500 per minute eventually opens the circuit
  forever.
- **OPEN rejects before the socket**, not after: protecting the downstream is
  the whole point.
- **Half-open probes** are allowed exactly through the state machine, with a
  configurable success threshold to close again; one probe failure re-opens
  instantly with a fresh recovery window.
- **Bounded fan-out** in `fetch_many` via a semaphore: resilience also means
  not becoming the flood yourself.

## Install & use

```bash
pip install -e .          # or just put src/ on your PYTHONPATH — zero deps
```

```python
import asyncio
from resilient_fetcher import (
    ResilientFetcher, TokenBucket, CircuitBreaker, ExponentialBackoff,
    UrllibTransport, FetchPolicy, RetryBudget, MonotonicClock,
)

async def main():
    clock = MonotonicClock()
    fetcher = ResilientFetcher(
        transport=UrllibTransport(),
        bucket=TokenBucket(capacity=10, refill_rate=10, clock=clock),   # 10 rps, burst 10
        breaker=CircuitBreaker(clock, failure_threshold=5, recovery_timeout=30),
        backoff=ExponentialBackoff(base=0.5, max_delay=15, jitter="full"),
        policy=FetchPolicy(max_attempts=4, timeout=10),
        budget=RetryBudget(capacity=20, refill_per_sec=5, clock=clock),
    )
    outcomes = await fetcher.fetch_many(urls, concurrency=16)
    for o in outcomes:
        print(o.url, o.ok, f"{o.attempts} attempts, {o.retries_used} retries")

asyncio.run(main())
```

## CLI

```console
$ resilient-fetch 'https://httpbin.org/status/503' 'https://httpbin.org/status/404' \
                  'https://httpbin.org/status/200' -a 2
✗ [503] 2att 1ret        0B  https://httpbin.org/status/503
✗ [404] 1att 0ret        0B  https://httpbin.org/status/404   ← fatal, never retried
✓ [200] 1att 0ret        0B  https://httpbin.org/status/200
— 1/3 ok, 1 retries, 0 circuit-aborted
```

Flags: `-c` concurrency, `-r` rate, `-b` burst, `-a` max attempts, `-t`
timeout, `--retry-budget`, `--failure-threshold`, `--recovery-timeout`,
`--json`, `-o report.json`. `--from-stdin` reads a URL list.

## Tests

```bash
PYTHONPATH=src:tests python -m unittest discover -s tests
# or: pip install -e . && pytest
```

27 tests / 4 suites:

| Suite | Proves |
|---|---|
| `test_primitives` | burst→deny, continuous refill & clamping, exact wait times; growth/cap of `none/full/equal/decorrelated` jitter; breaker lifecycle `CLOSED→OPEN→HALF_OPEN→CLOSED` and instant re-open; the retryable matrix (`408/429/5xx/net = yes`, `404 = no`) |
| `test_fetcher` | exact attempt & wait schedules (`[1, 2]` — deterministic, no sleeps), fatal 4xx gives up at attempt 1, attempt caps respected, mid-retry circuit opening aborts *before* the transport, half-open probe recovery, **retry budget actually stops a storm**, `fetch_many` respects the concurrency bound under a real event loop |
| `test_cli` | the argparse surface builds a wired fetcher; report math |

## Layout

```
src/resilient_fetcher/
  clock.py       Clock / MonotonicClock / FakeClock      — injectable time
  ratelimit.py   TokenBucket                             — continuous refill
  backoff.py     ExponentialBackoff                      — none/full/equal/decorrelated
  circuit.py     CircuitBreaker                          — three-state machine
  transport.py   Result + Transport contract + Urllib    — the only seam to the net
  fetcher.py     ResilientFetcher + RetryBudget          — orchestration
  __main__.py    CLI                                     — JSON report out
```

MIT licensed. Contributions welcome — especially a `httpx` transport and a
hedged-request policy as additional, opt-in primitives.
